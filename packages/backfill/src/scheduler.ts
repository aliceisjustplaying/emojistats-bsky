/** Claim/scheduling loop: concurrency limits, the claimable scan and --did mode. */

import { parseArgs } from 'node:util';

import pLimit, { type LimitFunction } from 'p-limit';

import {
  GLOBAL_CONCURRENCY,
  MAX_ATTEMPTS,
  PER_HOST_CONCURRENCY,
  PER_HOST_CONCURRENCY_BSKY,
} from './config.js';
import type { HostPressure } from './host-pressure.js';
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
  /** The three deepest per-host queues, for the stats line. */
  topHosts(): Array<{ host: string; depth: number }>;
}

export interface SchedulerDeps {
  ledger: Ledger;
  stats: CrawlStats;
  control: CrawlControl;
  hostPressure: HostPressure;
  processRepo: (repo: RepoRow) => Promise<void>;
}

// bsky.social is the entryway fronting every mushroom: millions of early
// accounts' PLC tails still point there (the fetcher follows the DID doc on
// claim). With the third-party cap of 2 it became the whole fleet's
// bottleneck — two slots gating a 168-deep queue per box on launch night.
const isBskyInfra = (host: string): boolean =>
  host.endsWith('.bsky.network') ||
  host === 'bsky.social' ||
  host.endsWith('//bsky.social');
const hostCapFor = (host: string): number =>
  isBskyInfra(host) ? PER_HOST_CONCURRENCY_BSKY : PER_HOST_CONCURRENCY;

const CLAIM_SCAN_MULTIPLIER = 16;
const CLAIM_SCAN_MAX = 50_000;

export function createScheduler(deps: SchedulerDeps): Scheduler {
  const { ledger, stats, control, hostPressure, processRepo } = deps;

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

  const active = new Set<Promise<void>>();

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
      while (!control.stopClaiming && stats.claimed < flags.limit) {
        if (active.size >= maxOutstanding) {
          await Promise.race(active);
          continue;
        }
        const claimCapacity = Math.min(
          maxOutstanding - active.size,
          flags.limit - stats.claimed,
        );
        const batch = ledger.listClaimable(
          Math.min(
            CLAIM_SCAN_MAX,
            Math.max(claimCapacity, claimCapacity * CLAIM_SCAN_MULTIPLIER),
          ),
        );
        if (batch.length === 0) {
          if (active.size > 0) {
            await Promise.race(active);
            continue;
          }
          const wait = idleWait();
          if (wait === null) break;
          await sleep(wait);
          continue;
        }
        let scheduled = 0;
        for (const repo of batch) {
          if (scheduled >= claimCapacity) break;
          if (stats.claimed >= flags.limit) break;
          // Cooling hosts are skipped without claiming: the rows stay
          // pending and re-offer once the cooldown lapses, and the in-flight
          // pool only ever holds repos that can actually fetch right now.
          if (hostPressure.coolingMs(repo.pdsHost) > 0) {
            stats.skipped += 1;
            continue;
          }
          if (hostQueued(repo.pdsHost) >= hostCapFor(repo.pdsHost)) continue;
          if (!ledger.markFetching(repo.did)) {
            stats.skipped += 1;
            continue;
          }
          stats.claimed += 1;
          scheduled += 1;
          trackRepo(repo);
        }
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
  }

  return {
    run,
    inFlight: () => active.size,
    fetching: () => globalLimit.activeCount,
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
