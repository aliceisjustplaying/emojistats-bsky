/** Claim/scheduling loop: concurrency limits, the claimable scan and --did mode. */

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
  /** Final sweep: zero the attempts budget on parked unreachable rows so waves resume. */
  finalSweep: boolean;
}

export function parseFlags(): CliFlags {
  const { values } = parseArgs({
    options: {
      limit: { type: 'string' },
      did: { type: 'string', multiple: true },
      // Explicit, not automatic: resetting budgets on every restart would let a
      // crash loop hammer dead hosts forever; a sweep is an operator decision.
      'final-sweep': { type: 'boolean', default: false },
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
  return {
    limit,
    dids: values.did ?? [],
    finalSweep: values['final-sweep'] ?? false,
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
/** Rows per dead-host parking UPDATE; the pause adapts to chunk duration. */
const DEAD_HOST_PARK_CHUNK = 10_000;
/** Re-park dead hosts this often: catches enumeration trickle + restart leftovers. */
const DEAD_HOST_SWEEP_MS = 300_000;

const yieldToTimers = async (): Promise<void> => {
  await new Promise<void>((resolve) => {
    setImmediate(resolve);
  });
};

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
      ...[...hostLimits.keys()].filter(
        (host) =>
          hostQueued(host) >= hostPressure.effectiveCap(host) ||
          hostPressure.coolingMs(host) > 0,
      ),
      // Dead hosts are excluded at the SQL level too: their millions of
      // pending rows would otherwise dominate every scan window while the
      // chunked parking is still draining them.
      ...hostHealth.deadHosts(),
    ]),
  ];

  const active = new Set<Promise<void>>();
  let claimBacklog: RepoRow[] = [];

  // Per-host limiter outermost so a slow host's queue never pins global slots;
  // the global limiter inside is what actually caps simultaneous downloads.
  // A host that started cooling after this repo was claimed gets requeued
  // immediately — never slept on: sleeping here held the whole in-flight pool
  // hostage to the deepest host's cooldown (observed fetching: 2 of 3,072).
  const trackRepo = (repo: RepoRow): void => {
    const task = hostLimitFor(repo.pdsHost)(() => {
      const coolMs = hostPressure.coolingMs(repo.pdsHost);
      if (coolMs > 0) {
        ledger.markThrottled(repo.did, `host cooling: ${repo.pdsHost}`, coolMs);
        stats.retried += 1;
        return Promise.resolve();
      }
      return globalLimit(() => processRepo(repo));
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
    if ((counts.unreachable ?? 0) === 0) return null;
    let withinBudget = false;
    let nextDueMs = Number.POSITIVE_INFINITY;
    for (const row of ledger.iterateByStatus('unreachable')) {
      if (row.attempts < MAX_ATTEMPTS) withinBudget = true;
      nextDueMs = Math.min(nextDueMs, row.retryAfter ?? 0);
    }
    if (!withinBudget) return null;
    return Math.min(Math.max(nextDueMs - Date.now(), 1_000), 30_000);
  };

  async function run(flags: CliFlags): Promise<void> {
    if (flags.dids.length > 0) {
      for (const did of flags.dids) {
        const repo = ledger.getRepo(did);
        if (repo === undefined) {
          logger.error(
            { did },
            'did not present in ledger; run enumerate first',
          );
          continue;
        }
        if (!ledger.markFetching(did)) {
          logger.warn(
            { did, status: repo.status },
            'forcing reprocess (--did) from a non-claimable status',
          );
        }
        stats.claimed += 1;
        trackRepo(repo);
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
          // Cooling hosts are skipped without claiming: the rows stay
          // pending and re-offer once the cooldown lapses, and the in-flight
          // pool only ever holds repos that can actually fetch right now.
          if (
            hostPressure.coolingMs(repo.pdsHost) > 0 ||
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
        claimBacklog = retained.concat(claimBacklog.slice(read));
        // A backlog that is ONLY busy/cooling rows is dropped: the next
        // (time-floored) scan re-derives it with fresh host exclusions
        // instead of this loop re-walking the same parked rows on every
        // completion wake-up.
        if (scheduled === 0 && retained.length === claimBacklog.length)
          claimBacklog = [];
        if (scheduled < claimCapacity) {
          const wake = hostPressure.nextWake();
          if (active.size > 0) {
            await Promise.race(active);
            continue;
          }
          await sleep(
            wake === undefined
              ? 1_000
              : Math.min(Math.max(wake - Date.now(), 250), 5_000),
          );
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
