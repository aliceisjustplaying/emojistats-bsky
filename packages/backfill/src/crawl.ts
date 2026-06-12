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
  const loader: RepoLoader = new ClickHouseRepoLoader(chClient);

  const archiveSink = await openArchiveSink(policy);

  if (CRASH_RECONCILE_ON_STARTUP && ledger.getMeta('crawl_dirty') === '1') {
    await reconcileRecentLoads(ledger, chClient);
  } else if (ledger.getMeta('crawl_dirty') === '1') {
    logger.warn(
      'unclean shutdown detected: skipping loaded-row reconciliation; stale fetching rows will requeue',
    );
  }
  ledger.setMeta('crawl_dirty', '1');

  if (flags.finalSweep) {
    const reset = ledger.resetUnreachableAttempts();
    logger.warn(
      { reset },
      'final sweep: unreachable attempt budgets zeroed, retry waves resume',
    );
  }

  const stats = createCrawlStats();
  const control: CrawlControl = { stopClaiming: false };

  const { telemetry, clients: telemetryClients } = createTelemetry();
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
  await shutdown({
    telemetry,
    archiveSink,
    ledger,
    chClient,
    telemetryClients,
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
