/** Run lifecycle: signals, stats logging, telemetry + sink wiring, shutdown ordering. */

import type { ClickHouseClient } from '@clickhouse/client';
import { createArchiveSink } from 'archive';
import type { StoragePolicy } from 'archive/policy';
import type { ArchiveSink } from 'archive/types';

import {
  ARCHIVE_DIR,
  ARCHIVE_MAX_FILE_AGE_MS,
  ARCHIVE_MAX_ROWS_PER_FILE,
  ARCHIVE_SYNC_COMMAND,
  BACKFILL_RUN_ID,
  SHARD_LABEL,
  STATS_LOG_INTERVAL_MS,
  TELEMETRY_INTERVAL_MS,
} from './config.js';
import logger from './logger.js';
import type { CrawlControl, CrawlStats } from './run-state.js';
import type { Scheduler } from './scheduler.js';
import { CrawlTelemetry } from './telemetry.js';
import type { Ledger } from './types.js';

// The archive is the ONLY durable home of full post text (plan 0001 storage
// revision) — when enabled, append failures are fatal, never dropped.
export async function openArchiveSink(
  policy: StoragePolicy,
): Promise<ArchiveSink | null> {
  return policy.archiveEnabled
    ? createArchiveSink({
        dir: ARCHIVE_DIR,
        prefix: `backfill-${SHARD_LABEL}`,
        maxRowsPerFile: ARCHIVE_MAX_ROWS_PER_FILE,
        maxFileAgeMs: ARCHIVE_MAX_FILE_AGE_MS,
        syncCommand: ARCHIVE_SYNC_COMMAND,
      })
    : null;
}

export function createTelemetry(chClient: ClickHouseClient): CrawlTelemetry {
  return new CrawlTelemetry(chClient, {
    runId: BACKFILL_RUN_ID,
    shard: SHARD_LABEL,
    intervalMs: TELEMETRY_INTERVAL_MS,
  });
}

export interface TelemetryWiring {
  ledger: Ledger;
  stats: CrawlStats;
  inFlight: () => number;
}

export function startTelemetry(
  telemetry: CrawlTelemetry,
  deps: TelemetryWiring,
): void {
  // Warn-only: a missing backfill_progress column means dropped ticks, not a crash.
  void telemetry.assertProgressColumns();
  let telemetryRate = { postRows: 0, at: Date.now() };
  telemetry.start(() => {
    const now = Date.now();
    const elapsedSec = (now - telemetryRate.at) / 1000;
    const rowsPerSec =
      elapsedSec > 0
        ? (deps.stats.postRows - telemetryRate.postRows) / elapsedSec
        : 0;
    telemetryRate = { postRows: deps.stats.postRows, at: now };
    return {
      // Shard-scoped: a sharded ledger instance reports only its own slice,
      // so the dashboard can SUM statusCounts/postsLoaded across shards and
      // boxes and get exact fleet totals. bytesDownloaded / rowsPerSec /
      // inFlight stay per-process as before.
      statusCounts: deps.ledger.statusCounts(),
      postsLoaded: deps.ledger.totalPostsLoaded(),
      bytesDownloaded: deps.stats.bytes,
      rowsPerSec,
      inFlight: deps.inFlight(),
    };
  });
}

export function installSignalHandlers(
  control: CrawlControl,
  inFlight: () => number,
): void {
  let signalCount = 0;
  const onSignal = (signal: string): void => {
    signalCount += 1;
    if (signalCount > 1) process.exit(130);
    control.stopClaiming = true;
    logger.warn(
      { signal, inFlight: inFlight() },
      'shutdown requested: claiming stopped, draining in-flight repos (signal again to force-quit)',
    );
  };
  process.on('SIGINT', () => onSignal('SIGINT'));
  process.on('SIGTERM', () => onSignal('SIGTERM'));
}

/** Periodic stats line; the returned stop function logs one final snapshot. */
export function startStatsLogging(
  stats: CrawlStats,
  scheduler: Scheduler,
): () => void {
  let last = { postRows: 0, at: Date.now() };
  const logStats = (): void => {
    const now = Date.now();
    const elapsedSec = (now - last.at) / 1000;
    const rowsPerSec =
      elapsedSec > 0
        ? Math.round((stats.postRows - last.postRows) / elapsedSec)
        : 0;
    last = { postRows: stats.postRows, at: now };
    logger.info(
      {
        ...stats,
        rowsPerSec,
        fetching: scheduler.fetching(),
        inFlight: scheduler.inFlight(),
        topHosts: scheduler.topHosts(),
      },
      'crawl stats',
    );
  };
  const timer = setInterval(logStats, STATS_LOG_INTERVAL_MS);
  return (): void => {
    clearInterval(timer);
    logStats();
  };
}

export interface ShutdownDeps {
  telemetry: CrawlTelemetry;
  archiveSink: ArchiveSink | null;
  ledger: Ledger;
  chClient: ClickHouseClient;
}

// Telemetry first (final tick + buffered events), then the sink — close()
// finalizes the open parquet file and runs the sync hook; if that throws, the
// run exits non-zero with the dirty flag still set, which is the loud failure
// the archive doctrine demands.
export async function shutdown(deps: ShutdownDeps): Promise<void> {
  await deps.telemetry.stop();
  if (deps.archiveSink !== null) await deps.archiveSink.close();
  deps.ledger.setMeta('crawl_dirty', '0');
  deps.ledger.close();
  await deps.chClient.close();
  // pino-pretty flushes in a worker thread; if some handle still pins the event
  // loop after that, force the promised exit (keeping a non-zero code from an
  // archive trip).
  setTimeout(() => process.exit(process.exitCode ?? 0), 500).unref();
}
