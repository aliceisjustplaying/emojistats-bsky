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
  CRAWL_SHARD_INDEX,
  CRAWL_SHARDS,
  LEDGER_DB_PATH,
  TEXT_IN_CLICKHOUSE,
} from './config.js';
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
  });
  const chClient = createClickHouseClient();
  await pingClickHouse(chClient);
  const loader: RepoLoader = new ClickHouseRepoLoader(chClient);

  const archiveSink = await openArchiveSink(policy);

  if (ledger.getMeta('crawl_dirty') === '1') {
    await reconcileRecentLoads(ledger, chClient);
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

  const telemetry = createTelemetry(chClient);
  const retry = createRetryPolicy({ ledger, telemetry, stats });
  const processRepo = createRepoPipeline({
    ledger,
    loader,
    archiveSink,
    policy,
    telemetry,
    retry,
    stats,
    control,
  });
  const scheduler = createScheduler({ ledger, stats, control, processRepo });

  startTelemetry(telemetry, {
    ledger,
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
  await shutdown({ telemetry, archiveSink, ledger, chClient });
}

main().catch((err: unknown) => {
  logger.fatal(
    { err: err instanceof Error ? (err.stack ?? err.message) : String(err) },
    'crawl crashed',
  );
  process.exitCode = 1;
});
