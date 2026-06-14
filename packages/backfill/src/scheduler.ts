/** Claim/scheduling loop: concurrency limits, the claimable scan and --did mode. */

import { createReadStream } from 'node:fs';
import { createInterface } from 'node:readline';
import { parseArgs } from 'node:util';

import pLimit, { type LimitFunction } from 'p-limit';

import { GLOBAL_CONCURRENCY, MAX_ATTEMPTS } from './config.js';
import type { HostHealth } from './host-health.js';
import { hostCapFor, type HostPressure } from './host-pressure.js';
import logger from './logger.js';
import type { CrawlControl, CrawlStats } from './run-state.js';
import type { Ledger, RepoRow } from './types.js';

export interface CliFlags {
  /** Stop claiming after this many repos have been claimed this run. */
  limit: number;
  /** Process exactly these DIDs (must already be in the ledger), skipping the claimable scan. */
  dids: string[];
  /** Stream additional exact-DID work from a file, one DID per line. */
  didFile?: string;
  /** Stream exact-DID work from a tab/space-separated DID + PDS host file. */
  didHostFile?: string;
  /** Final sweep: zero the attempts budget on parked unreachable rows so waves resume. */
  finalSweep: boolean;
  /**
   * Revive these hosts: drop each from the dead-host registry and reset its
   * parked 'unreachable' rows to claimable. The targeted exit-ramp for a host
   * that recovered (or was deliberately skipped, e.g. brid.gy) — unlike
   * --final-sweep it does not re-arm genuinely-dead DNS/legal hosts.
   */
  reviveHosts: string[];
}

export function parseFlags(): CliFlags {
  const { values } = parseArgs({
    options: {
      limit: { type: 'string' },
      did: { type: 'string', multiple: true },
      // Re-fetch a DID list from a file, one per line — the at-scale companion to
      // verify --emit-loose (a shell-expanded `--did $(cat)` breaks: --did is
      // repeatable so extras land as rejected positionals, and millions blow past
      // ARG_MAX). Merged into dids; same exact-DID path, no claimable scan.
      'did-file': { type: 'string' },
      // For v1-metadata recrawls, lets a worker use a lightweight local ledger
      // populated from source-ledger DID+host rows instead of copying the source
      // SQLite DB to every recrawl worker.
      'did-host-file': { type: 'string' },
      // Explicit, not automatic: resetting budgets on every restart would let a
      // crash loop hammer dead hosts forever; a sweep is an operator decision.
      'final-sweep': { type: 'boolean', default: false },
      // Repeatable: --revive-host a.example --revive-host b.example
      'revive-host': { type: 'string', multiple: true },
    },
  });

  let limit = Number.POSITIVE_INFINITY;
  if (values.limit !== undefined) {
    limit = Number(values.limit);
    if (!Number.isInteger(limit) || limit <= 0) {
      throw new Error(
        `--limit must be a positive integer, got "${values.limit}"`,
      );
    }
  }
  const dids = [...new Set(values.did ?? [])];
  return {
    limit,
    dids,
    didFile: values['did-file'],
    didHostFile: values['did-host-file'],
    finalSweep: values['final-sweep'] ?? false,
    reviveHosts: values['revive-host'] ?? [],
  };
}

export interface Scheduler {
  /** Claims until the limit/idle policy says stop, then drains in-flight repos. */
  run(flags: CliFlags): Promise<void>;
  /** Claimed repos whose pipeline has not settled yet. */
  inFlight(): number;
  /** Downloads currently holding a global slot. */
  fetching(): number;
  /** Rows sitting in the in-memory claim backlog (scan amortization buffer). */
  backlog(): number;
  /** The three deepest per-host queues, for the stats line. */
  topHosts(): Array<{ host: string; depth: number }>;
}

export interface SchedulerDeps {
  ledger: Ledger;
  stats: CrawlStats;
  control: CrawlControl;
  hostPressure: HostPressure;
  hostHealth: HostHealth;
  processRepo: (repo: RepoRow) => Promise<void>;
}

const CLAIM_SCAN_MULTIPLIER = 16;
const CLAIM_SCAN_MIN = 250_000;
const CLAIM_SCAN_MAX = 250_000;
// Refill in slot-batches well below the full pool: waiting for an entire
// GLOBAL_CONCURRENCY of free slots (the old threshold) drained the pool to
// half before every refill — a sawtooth where the fetch pool spent its tail
// half-empty while the scheduler sat in Promise.race.
const CLAIM_REFILL_MIN = Math.max(64, GLOBAL_CONCURRENCY >> 4);
// Yield by TIME, not row count. v2 yielded every 1,000 rows; with hundreds
// of fetches in flight each setImmediate parks the pass behind the entire
// I/O backlog (seconds per yield under load), so a 250k-row walk — micro-
// seconds of work per row — took minutes and the claim loop starved itself.
// Now the pass checks the clock every 1,000 rows and yields only after
// 50ms of continuous walking: same event-loop friendliness, ~100× fewer
// involuntary parks.
const CLAIM_LOOP_YIELD_CHECK_EVERY = 1_000;
const CLAIM_LOOP_YIELD_AFTER_MS = 50;
/** Hard floor between ledger scans — each one is synchronous main-thread work. */
const CLAIM_SCAN_RETRY_MS = 2_000;
const CLAIM_RETAINED_DROP_MIN = CLAIM_SCAN_MIN >> 1;
const CLAIM_RETAINED_DROP_RATIO = CLAIM_SCAN_MULTIPLIER * 4;
/** Rows per dead-host parking UPDATE; the pause adapts to chunk duration. */
const DEAD_HOST_PARK_CHUNK = 10_000;
/** Re-park dead hosts this often: catches enumeration trickle + restart leftovers. */
const DEAD_HOST_SWEEP_MS = 300_000;

export function shouldDropRetainedBacklog(
  scheduled: number,
  retained: number,
  capacity: number,
): boolean {
  return (
    retained >= CLAIM_RETAINED_DROP_MIN &&
    scheduled < Math.min(CLAIM_REFILL_MIN, capacity) &&
    retained > Math.max(1, scheduled) * CLAIM_RETAINED_DROP_RATIO
  );
}

export function nextClaimWakeDelay(
  wake: number | undefined,
  now: number = Date.now(),
): number {
  return wake === undefined
    ? 1_000
    : Math.min(Math.max(wake - now, 250), 5_000);
}

export function shouldExcludeHostFromClaimScan(
  queued: number,
  effectiveCap: number,
  backoffMs: number,
  isDead: boolean,
): boolean {
  return isDead || queued >= effectiveCap || backoffMs > 0;
}

export function shouldWaitForUnreachableRetry(
  attempts: number,
  pdsHost: string,
  isDeadHost: (host: string) => boolean,
): boolean {
  return attempts < MAX_ATTEMPTS && !isDeadHost(pdsHost);
}

const yieldToTimers = async (): Promise<void> => {
  await new Promise<void>((resolve) => {
    setImmediate(resolve);
  });
};

interface ExactRepo {
  did: string;
  pdsHost?: string;
}

async function* exactRepos(flags: CliFlags): AsyncGenerator<ExactRepo> {
  const seenInline = new Set<string>();
  for (const did of flags.dids) {
    const trimmed = did.trim();
    if (trimmed.length === 0 || seenInline.has(trimmed)) continue;
    seenInline.add(trimmed);
    yield { did: trimmed };
  }
  if (flags.didFile !== undefined) {
    const lines = createInterface({
      input: createReadStream(flags.didFile, { encoding: 'utf8' }),
      crlfDelay: Infinity,
    });
    for await (const line of lines) {
      const did = line.trim();
      if (did.length > 0) yield { did };
    }
  }
  if (flags.didHostFile !== undefined) {
    const lines = createInterface({
      input: createReadStream(flags.didHostFile, { encoding: 'utf8' }),
      crlfDelay: Infinity,
    });
    for await (const line of lines) {
      const trimmed = line.trim();
      if (trimmed.length === 0) continue;
      const [did, pdsHost] = trimmed.split(/\s+/, 2);
      if (did === undefined || pdsHost === undefined || pdsHost.length === 0) {
        logger.warn(
          { line: trimmed },
          'skipping malformed --did-host-file row',
        );
        continue;
      }
      yield { did, pdsHost };
    }
  }
}

export function createScheduler(deps: SchedulerDeps): Scheduler {
  const { ledger, stats, control, hostPressure, hostHealth, processRepo } =
    deps;

  const globalLimit = pLimit(GLOBAL_CONCURRENCY);
  // Keyed by the ledger's pds_host string verbatim — including the
  // 'http://host' form the rare http PDS stores (fetcher.pdsHostFromEndpoint).
  // Each row carries exactly one canonical form, so partitions stay coherent,
  // and endsWith below is scheme-tolerant by construction.
  const hostLimits = new Map<string, LimitFunction>();
  const hostLimitFor = (host: string): LimitFunction => {
    let limit = hostLimits.get(host);
    if (limit === undefined) {
      limit = pLimit(hostCapFor(host));
      hostLimits.set(host, limit);
    }
    return limit;
  };
  const hostQueued = (host: string): number => {
    const limit = hostLimitFor(host);
    return limit.activeCount + limit.pendingCount;
  };
  const unavailableHosts = (): string[] => [
    ...new Set([
      ...[...hostLimits.keys()].filter((host) =>
        shouldExcludeHostFromClaimScan(
          hostQueued(host),
          hostPressure.effectiveCap(host),
          hostPressure.backoffMs(host),
          false,
        ),
      ),
      // Dead hosts are excluded at the SQL level too: their millions of
      // pending rows would otherwise dominate every scan window while the
      // chunked parking is still draining them.
      ...hostHealth.deadHosts(),
    ]),
  ];

  const active = new Set<Promise<void>>();
  let claimBacklog: RepoRow[] = [];

  const waitForHostPace = async (repo: RepoRow): Promise<void> => {
    for (;;) {
      const coolMs =
        repo.rateLimitReserved === true
          ? hostPressure.backoffMs(repo.pdsHost)
          : hostPressure.coolingMs(repo.pdsHost);
      if (coolMs > 0) {
        await new Promise((resolve) => {
          setTimeout(resolve, coolMs);
        });
        continue;
      }
      if (repo.rateLimitReserved === true || hostPressure.reserve(repo.pdsHost))
        break;
      await new Promise((resolve) => {
        setTimeout(resolve, Math.max(hostPressure.coolingMs(repo.pdsHost), 1));
      });
    }
    return globalLimit(() => processRepo(repo));
  };

  // Per-host limiter outermost so a slow host's queue never pins global slots;
  // the global limiter inside is what actually caps simultaneous downloads.
  // Pacing happens inside the host limiter, before the global slot is taken:
  // queued work for one host cannot pin fleet-wide download capacity, and the
  // scheduler can keep a small ready queue for rate-limited tail hosts.
  const trackRepo = (repo: RepoRow): void => {
    const task = hostLimitFor(repo.pdsHost)(() => {
      return waitForHostPace(repo);
    });
    const tracked: Promise<void> = task
      .catch((err: unknown) => {
        logger.error(
          {
            did: repo.did,
            err: err instanceof Error ? err.message : String(err),
          },
          'unexpected pipeline error',
        );
      })
      .finally(() => {
        active.delete(tracked);
      });
    active.add(tracked);
  };

  const sleep = async (ms: number): Promise<void> => {
    const deadline = Date.now() + ms;
    while (!control.stopClaiming && Date.now() < deadline) {
      await new Promise((resolve) => {
        setTimeout(resolve, Math.min(500, deadline - Date.now()));
      });
    }
  };
  // Parking runs BESIDE the claim loop, not inside it: a 1.9M-row host is
  // hundreds of chunks of synchronous sqlite, and v1 awaited that from the
  // scheduler loop — claims froze for the whole park. The pause between
  // chunks adapts to the measured chunk duration (≥3× pause) so the park
  // never takes more than ~25% of the main thread. Chunks are atomic; rows
  // a crash leaves unparked are excluded from scans this run, persisted as
  // dead via ledger meta for the next run, and swept again periodically —
  // the sweep also catches rows enumeration parked late or missed inside
  // its 60s dead-list refresh window.
  let parkChain: Promise<void> = Promise.resolve();
  let lastDeadSweepAt = Date.now();
  const enqueuePark = (
    host: string,
    reason: string,
    // The unreachable-arm park is a SINGLE sqlite statement that walks the
    // host's entire (pds_host, 'unreachable') index range — for a 17.9M-row
    // host that is ~4 minutes of fully blocked event loop, unyieldable.
    // It runs ONCE at trip time, where it is one-off and load-bearing.
    // Periodic sweeps must never include it: the observed failure mode was
    // a 200-250s whole-process stall every DEAD_HOST_SWEEP_MS, sawing fleet
    // throughput and freezing telemetry on a metronome. The sweep's pending
    // arm stays — it is an index probe on an empty range when there is
    // nothing to do.
    includeUnreachableArm: boolean,
  ): void => {
    parkChain = parkChain
      .then(async () => {
        const error = `host dead: ${host} (${reason})`;
        let parked = 0;
        for (;;) {
          const chunkStart = Date.now();
          const changes = ledger.parkDeadHostChunk(
            host,
            error,
            DEAD_HOST_PARK_CHUNK,
          );
          parked += changes;
          if (changes < DEAD_HOST_PARK_CHUNK) break;
          const chunkMs = Date.now() - chunkStart;
          await new Promise((resolve) => {
            setTimeout(resolve, Math.max(250, chunkMs * 3));
          });
        }
        if (includeUnreachableArm)
          parked += ledger.parkDeadHostUnreachable(host, error);
        if (parked > 0)
          logger.warn(
            { host, parked, reason },
            'dead host: claimable repos parked as unreachable (final-sweep list)',
          );
        return undefined;
      })
      // A failed chunk (e.g. SQLITE_BUSY past the timeout) must not kill the
      // chain for later hosts or surface as an unhandled rejection — the
      // host stays excluded from scans either way.
      .catch((err: unknown) => {
        logger.error(
          { host, err: err instanceof Error ? err.message : String(err) },
          'dead host parking failed; rows remain excluded via scan filter',
        );
      });
  };
  const parkDeadHosts = (): void => {
    for (const host of hostHealth.takeNewlyTripped()) {
      // Persist the verdict so enumeration diverts the host's future rows
      // straight to parked and the next crawl run seeds it as dead.
      ledger.addDeadHost(host);
      enqueuePark(host, 'tripped', true);
    }
    if (Date.now() - lastDeadSweepAt >= DEAD_HOST_SWEEP_MS) {
      lastDeadSweepAt = Date.now();
      for (const host of hostHealth.deadHosts())
        enqueuePark(host, 'sweep', false);
    }
  };
  // Verdicts persisted by previous runs apply immediately — no 30-failure
  // re-trip, no scan pollution while it re-learns. The cheap pending-arm
  // park also runs at startup: rows left pending by a mid-park crash (or
  // enumerated into 'pending' before the dead list existed) would otherwise
  // sit excluded-but-pending until the first 5-minute sweep, paying the
  // NOT-IN row-filter cost on every claim scan in between.
  for (const host of ledger.getDeadHosts()) {
    hostHealth.markDead(host);
    enqueuePark(host, 'startup', false);
  }

  // Idle policy: pending rows are always claimable, so an idle ledger means only
  // unreachable repos remain. Retry waves continue while any of them still has
  // attempts budget; once all are past MAX_ATTEMPTS the run ends and they stay
  // parked as the explicit unreachable list for the final sweep.
  const idleWait = (): number | null => {
    const counts = ledger.statusCounts();
    if ((counts.pending ?? 0) > 0 || (counts.fetching ?? 0) > 0) return 1_000;
    let withinBudget = false;
    let nextDueMs = Number.POSITIVE_INFINITY;
    for (const row of ledger.iterateByStatus('unreachable')) {
      if (
        !shouldWaitForUnreachableRetry(row.attempts, row.pdsHost, (host) =>
          hostHealth.isDead(host),
        )
      )
        continue;
      withinBudget = true;
      nextDueMs = Math.min(nextDueMs, row.retryAfter ?? 0);
    }
    if (!withinBudget) return null;
    return Math.min(Math.max(nextDueMs - Date.now(), 1_000), 30_000);
  };

  async function run(flags: CliFlags): Promise<void> {
    if (
      flags.dids.length > 0 ||
      flags.didFile !== undefined ||
      flags.didHostFile !== undefined
    ) {
      const maxOutstanding = GLOBAL_CONCURRENCY * 2;
      let exactCount = 0;
      for await (const exact of exactRepos(flags)) {
        const { did, pdsHost } = exact;
        if (control.stopClaiming || stats.claimed >= flags.limit) break;
        while (active.size >= maxOutstanding) await Promise.race(active);
        if (pdsHost !== undefined) ledger.upsertPending(did, pdsHost);
        const repo = ledger.getRepo(did);
        if (repo === undefined) {
          logger.error(
            { did },
            'did not present in ledger; run enumerate first or use --did-host-file',
          );
          continue;
        }
        const preserveExisting =
          repo.status === 'loaded' || repo.status === 'verified';
        if (preserveExisting) {
          logger.warn(
            { did, status: repo.status },
            'reprocessing loaded/verified repo without downgrading existing ledger state on failure',
          );
          repo.preserveExisting = true;
        } else if (!ledger.markFetching(did)) {
          logger.warn(
            { did, status: repo.status },
            'forcing reprocess (--did) from a non-claimable status',
          );
        }
        stats.claimed += 1;
        exactCount += 1;
        trackRepo(repo);
      }
      if (exactCount === 0) {
        throw new Error(
          `exact recrawl input contained no DIDs; refusing to fall through to a full claimable crawl`,
        );
      }
    } else {
      // Repos stuck in 'fetching' from a killed run are otherwise unclaimable;
      // requeue them through markRetry(0) so they re-fetch immediately.
      const stale = [...ledger.iterateByStatus('fetching')];
      for (const row of stale)
        ledger.markRetry(row.did, 'stale fetching state at crawl startup', 0);
      if (stale.length > 0)
        logger.warn(
          { stale: stale.length },
          'requeued repos stuck in fetching from a previous run',
        );

      const maxOutstanding = GLOBAL_CONCURRENCY * 2;
      let nextScanAllowedAt = 0;
      while (!control.stopClaiming && stats.claimed < flags.limit) {
        parkDeadHosts();
        if (active.size >= maxOutstanding) {
          await Promise.race(active);
          continue;
        }
        const available = maxOutstanding - active.size;
        const remainingLimit = flags.limit - stats.claimed;
        if (
          active.size > 0 &&
          available < Math.min(CLAIM_REFILL_MIN, remainingLimit)
        ) {
          await Promise.race(active);
          continue;
        }
        const claimCapacity = Math.min(available, remainingLimit);
        // Rescan whenever the backlog can no longer fill the open capacity,
        // not only when it is empty: a residue of busy/cooling rows that
        // trickles out one claim per completion would otherwise pin the
        // backlog non-empty forever and starve claims at the completion
        // rate. The fresh scan REPLACES the backlog — retained rows are
        // still 'pending' in the ledger, so the new did-ordered window
        // re-includes them; nothing is lost. The time floor is the only
        // thing between this and the old melt-the-main-thread regime.
        if (
          claimBacklog.length < claimCapacity &&
          Date.now() >= nextScanAllowedAt
        ) {
          nextScanAllowedAt = Date.now() + CLAIM_SCAN_RETRY_MS;
          const scanStart = Date.now();
          claimBacklog = ledger.listClaimable(
            Math.min(
              CLAIM_SCAN_MAX,
              Math.max(
                claimCapacity,
                claimCapacity * CLAIM_SCAN_MULTIPLIER,
                CLAIM_SCAN_MIN,
              ),
            ),
            unavailableHosts(),
          );
          logger.debug(
            { rows: claimBacklog.length, ms: Date.now() - scanStart },
            'claim scan',
          );
        }
        if (claimBacklog.length === 0) {
          if (active.size > 0) {
            await Promise.race(active);
            continue;
          }
          const wait = idleWait();
          if (wait === null) break;
          await sleep(Math.max(wait, nextScanAllowedAt - Date.now()));
          continue;
        }
        let scheduled = 0;
        let read = 0;
        const passStart = Date.now();
        let lastYieldAt = passStart;
        // Rows whose host is merely busy/cooling RIGHT NOW go back into the
        // backlog: a deep host (morel) must be re-offered as its queue drains
        // and its cooldowns lapse, not once per full ledger scan.
        const retained: RepoRow[] = [];
        for (; read < claimBacklog.length; read += 1) {
          if (
            read > 0 &&
            read % CLAIM_LOOP_YIELD_CHECK_EVERY === 0 &&
            Date.now() - lastYieldAt >= CLAIM_LOOP_YIELD_AFTER_MS
          ) {
            await yieldToTimers();
            lastYieldAt = Date.now();
          }
          const repo = claimBacklog[read];
          if (repo === undefined) break;
          if (scheduled >= claimCapacity) break;
          if (stats.claimed >= flags.limit) break;
          if (hostHealth.isDead(repo.pdsHost)) {
            stats.skipped += 1;
            continue;
          }
          // Backed-off hosts are skipped without claiming: the rows stay
          // pending and re-offer once the cooldown lapses, and the in-flight
          // pool only ever holds repos that can actually fetch right now.
          // Short header-derived pacing is handled inside the per-host limiter;
          // claiming a bounded host queue lets the crawler stay close to the
          // advertised request rate without burning global slots.
          if (
            hostPressure.backoffMs(repo.pdsHost) > 0 ||
            hostQueued(repo.pdsHost) >= hostPressure.effectiveCap(repo.pdsHost)
          ) {
            retained.push(repo);
            stats.skipped += 1;
            continue;
          }
          let claimedRow: boolean;
          try {
            claimedRow = ledger.markFetching(repo.did);
          } catch (err) {
            // SQLITE_BUSY past the timeout (an operator tool holding the
            // write lock) must not kill the run — the row stays pending and
            // a later scan re-offers it. Anything else is a real DB fault
            // and still crashes loudly (and now actually exits).
            if ((err as { code?: string }).code !== 'SQLITE_BUSY') throw err;
            stats.skipped += 1;
            continue;
          }
          if (!claimedRow) {
            stats.skipped += 1;
            continue;
          }
          stats.claimed += 1;
          scheduled += 1;
          trackRepo(repo);
        }
        // The unconsumed tail survives partial refills. v2 discarded it
        // whenever the host caps cut a refill short — and the very next
        // iteration re-ran the full synchronous 250k-row scan, back to back,
        // forever: the main thread spent whole minutes inside better-sqlite3
        // while sockets starved (observed as rowsPerSec=0 with "skipped"
        // exploding, and as ClickHouse "socket hang up" — the server closing
        // inserts the frozen event loop never serviced).
        logger.debug(
          {
            read,
            scheduled,
            retained: retained.length,
            capacity: claimCapacity,
            ms: Date.now() - passStart,
          },
          'claim pass',
        );
        const nextBacklog = retained.concat(claimBacklog.slice(read));
        // A backlog that is ONLY busy/cooling rows is dropped: the next
        // (time-floored) scan re-derives it with fresh host exclusions
        // instead of this loop re-walking the same parked rows on every
        // completion wake-up.
        if (
          (scheduled === 0 && retained.length === nextBacklog.length) ||
          shouldDropRetainedBacklog(
            scheduled,
            nextBacklog.length,
            claimCapacity,
          )
        ) {
          logger.debug(
            {
              scheduled,
              retained: nextBacklog.length,
              capacity: claimCapacity,
            },
            'dropping retained claim backlog for fresh scan',
          );
          claimBacklog = [];
        } else {
          claimBacklog = nextBacklog;
        }
        if (scheduled < claimCapacity) {
          const wake = hostPressure.nextWake();
          if (active.size > 0) {
            if (wake === undefined) {
              await Promise.race(active);
            } else {
              await Promise.race([
                Promise.race(active),
                sleep(nextClaimWakeDelay(wake)),
              ]);
            }
            continue;
          }
          await sleep(nextClaimWakeDelay(wake));
        }
      }
    }

    while (active.size > 0) {
      await Promise.race(active);
    }
    // Let an in-flight dead-host park finish before the run returns: chunks
    // are atomic, but a clean exit should leave the ledger fully parked.
    await parkChain;
  }

  return {
    run,
    inFlight: () => active.size,
    fetching: () => globalLimit.activeCount,
    backlog: () => claimBacklog.length,
    topHosts: () =>
      [...hostLimits.entries()]
        .map(([host, limit]) => ({
          host,
          depth: limit.activeCount + limit.pendingCount,
        }))
        .filter((entry) => entry.depth > 0)
        .toSorted((a, b) => b.depth - a.depth)
        .slice(0, 3),
  };
}
