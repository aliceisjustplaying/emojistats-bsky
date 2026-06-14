/**
 * Crawl entrypoint: wires the modules together and owns nothing else.
 *   scheduler.ts  — claim/scheduling loop (--limit / --did)
 *   pipeline.ts   — per-repo fetch → parse → extract → archive → load
 *   retry.ts      — retry/host-refresh policy + post-crash reconciliation
 *   lifecycle.ts  — signals, stats logging, telemetry + sink wiring, shutdown
 */

import { resolveStoragePolicy } from 'archive/policy';

import { createClickHouseClient, pingClickHouse } from './clickhouse.js';
import {
  ARCHIVE_ENABLED,
  CRASH_RECONCILE_ON_STARTUP,
  CRAWL_SHARD_INDEX,
  CRAWL_SHARDS,
  LEDGER_DB_PATH,
  TEXT_IN_CLICKHOUSE,
  assertBackfillRunIdConfigured,
} from './config.js';
import { createHostHealth } from './host-health.js';
import { createHostPressure } from './host-pressure.js';
import { createLedgerStats } from './ledger-stats.js';
import { SqliteLedger } from './ledger.js';
import {
  createTelemetry,
  installSignalHandlers,
  openArchiveSink,
  shutdown,
  startStatsLogging,
  startTelemetry,
} from './lifecycle.js';
import { ClickHouseRepoLoader } from './loader.js';
import logger from './logger.js';
import { createParsePool } from './parse-pool.js';
import { createRepoPipeline } from './pipeline.js';
import { createRetryPolicy, reconcileRecentLoads } from './retry.js';
import { createCrawlStats, type CrawlControl } from './run-state.js';
import { createScheduler, parseFlags } from './scheduler.js';
import type { Ledger, RepoLoader } from './types.js';

async function main(): Promise<void> {
  assertBackfillRunIdConfigured();
  const flags = parseFlags();
  // Resolved exactly once, fail-fast; every behavior read goes through the
  // policy object (archive/policy), never the raw config values.
  const policy = resolveStoragePolicy({
    textInClickhouse: TEXT_IN_CLICKHOUSE,
    archiveEnabled: ARCHIVE_ENABLED,
  });

  const ledger: Ledger = new SqliteLedger(LEDGER_DB_PATH, {
    shards: CRAWL_SHARDS,
    shardIndex: CRAWL_SHARD_INDEX,
    // Generous: operator tools (listrepos-diff --apply) legitimately hold
    // the write lock in bursts; the crawler waiting beats the crawler dying.
    busyTimeoutMs: 30_000,
  });
  const chClient = createClickHouseClient();
  await pingClickHouse(chClient);
  const loader: RepoLoader = new ClickHouseRepoLoader(chClient, {
    // Self-heal a poisoned socket pool: a stale keepalive socket makes every
    // insert ECONNRESET/time-out and jams the loader until restart, so the
    // retry path rebuilds the client on connection-level failures.
    recreateClient: () => createClickHouseClient(),
  });
  const { telemetry } = createTelemetry();
  await telemetry.assertEventColumns();

  const archiveSink = await openArchiveSink(policy);

  if (CRASH_RECONCILE_ON_STARTUP && ledger.getMeta('crawl_dirty') === '1') {
    await reconcileRecentLoads(ledger, chClient);
  } else if (ledger.getMeta('crawl_dirty') === '1') {
    logger.warn(
      'unclean shutdown detected: skipping loaded-row reconciliation; stale fetching rows will requeue',
    );
  }
  ledger.setMeta('crawl_dirty', '1');

  // Re-crawl accounting for the 2026-06-13 archive widening: repos with
  // loaded_at BEFORE this timestamp were archived text-only (facets/reply/
  // embed/labels missing) and need a re-fetch to recover those fields.
  // Set once, on the first run of widened code; never moved.
  if (ledger.getMeta('archive_extras_since') === undefined) {
    const since = new Date().toISOString();
    ledger.setMeta('archive_extras_since', since);
    logger.info(
      { since },
      'archive metadata widening active: repos loaded before this need re-crawl for extras',
    );
  }

  if (flags.finalSweep) {
    const deadHosts = ledger.getDeadHosts();
    const reset = ledger.resetUnreachableAttempts(deadHosts);
    logger.warn(
      { reset, deadHosts: deadHosts.length },
      'final sweep: non-dead unreachable attempt budgets zeroed, retry waves resume',
    );
  }

  // Revive must run BEFORE createScheduler: the scheduler seeds host-health and
  // the scan-exclusion set from ledger.getDeadHosts() at construction, so the
  // verdict has to be gone from the registry first or the host is re-excluded
  // and its just-reset rows never get claimed (the final-sweep dead-host gap).
  //
  // Operational notes (codex review):
  //   - Per-box: each shard owns its own ledger, so revive on every box whose
  //     shard holds rows for the host. Pass the EXACT canonical pds_host string
  //     stored in the ledger (e.g. 'atproto.brid.gy', no scheme for https).
  //   - resetUnreachableForHost is a single unchunked UPDATE — fine here (runs
  //     once at startup before the scheduler/telemetry loops), sub-second for a
  //     ~100k-row host; a multi-million-row revive would block startup briefly.
  //   - If enumeration runs CONCURRENTLY (it does not on the crawl boxes today —
  //     no enumerate service/timer), its ≤60s dead-host cache could re-park rows
  //     freshly enumerated in that window. upsertParked only clobbers 'pending',
  //     never an already-revived 'unreachable' row, so the bulk is safe; re-run
  //     revive afterward to catch any stragglers, or revive while it is idle.
  for (const host of flags.reviveHosts) {
    ledger.removeDeadHost(host);
    const revived = ledger.resetUnreachableForHost(host);
    logger.warn(
      { host, revived },
      'host revived: dropped from dead registry, unreachable rows reset to claimable',
    );
  }

  const stats = createCrawlStats();
  const control: CrawlControl = { stopClaiming: false };

  const hostPressure = createHostPressure();
  const hostHealth = createHostHealth();
  const retry = createRetryPolicy({
    ledger,
    telemetry,
    stats,
    hostPressure,
    hostHealth,
  });
  const parsePool = createParsePool();
  const processRepo = createRepoPipeline({
    ledger,
    loader,
    parsePool,
    archiveSink,
    policy,
    telemetry,
    retry,
    stats,
    control,
    hostPressure,
    hostHealth,
  });
  const scheduler = createScheduler({
    ledger,
    stats,
    control,
    hostPressure,
    hostHealth,
    processRepo,
  });

  const ledgerStats = createLedgerStats({
    dbPath: LEDGER_DB_PATH,
    shards: CRAWL_SHARDS,
    shardIndex: CRAWL_SHARD_INDEX,
  });
  // First snapshot must land before telemetry emits: an empty initial row
  // would argMax-zero this shard's counts on the dashboard for a tick.
  await ledgerStats.ready;
  startTelemetry(telemetry, {
    ledgerStats,
    stats,
    inFlight: () => scheduler.inFlight(),
  });
  installSignalHandlers(control, () => scheduler.inFlight());
  const stopStatsLogging = startStatsLogging(stats, scheduler);

  await scheduler.run(flags);

  stopStatsLogging();
  logger.info(
    {
      statuses: ledger.statusCounts(),
      totalPostsLoaded: ledger.totalPostsLoaded(),
    },
    'crawl run finished',
  );
  await parsePool.close();
  await ledgerStats.close();
  // Closes only a client the loader REBUILT after a poisoned pool; the injected
  // chClient is still closed by shutdown() below.
  await loader.close();
  await shutdown({
    telemetry,
    archiveSink,
    ledger,
    chClient,
  });
}

main().catch((err: unknown) => {
  logger.fatal(
    { err: err instanceof Error ? (err.stack ?? err.message) : String(err) },
    'crawl crashed',
  );
  // HARD exit, not process.exitCode: live timers (stats, telemetry, worker
  // threads) keep the event loop alive after main() dies, leaving a zombie
  // that ticks telemetry, claims nothing, and never lets systemd restart
  // it. Observed 2026-06-12 19:56-21:12 on crawl1: a SQLITE_BUSY escaping
  // the claim loop froze the box for 76 minutes while every dashboard
  // showed it "alive". The ledger is crash-safe by design; exiting is the
  // recovery path.
  process.exit(1);
});
