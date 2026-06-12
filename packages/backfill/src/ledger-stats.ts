/** Main-thread cache over the ledger-stats worker (see ledger-stats-worker.ts). */
import { Worker } from 'node:worker_threads';

import { TELEMETRY_INTERVAL_MS } from './config.js';
import type { LedgerStatsSnapshot } from './ledger-stats-worker.js';
import logger from './logger.js';

export interface LedgerStats {
  /** Resolves once the first snapshot has landed (telemetry must never emit zeros). */
  ready: Promise<void>;
  latest(): LedgerStatsSnapshot;
  close(): Promise<void>;
}

export function createLedgerStats(options: {
  dbPath: string;
  shards: number;
  shardIndex: number;
}): LedgerStats {
  let snapshot: LedgerStatsSnapshot = {
    statusCounts: {},
    postsLoaded: 0,
    computedAt: 0,
  };
  // The service runs under tsx; workers need the loader spelled out or the
  // .ts entry fails to resolve inside the thread.
  const worker = new Worker(
    new URL('./ledger-stats-worker.ts', import.meta.url),
    {
      execArgv: ['--import', 'tsx'],
      workerData: { ...options, intervalMs: TELEMETRY_INTERVAL_MS },
    },
  );
  worker.unref();
  const ready = new Promise<void>((resolve) => {
    worker.once('message', resolve);
  });
  worker.on('message', (next: LedgerStatsSnapshot) => {
    snapshot = next;
  });
  worker.on('error', (err) => {
    // Stats are reporting, not accounting: a dead stats thread must never
    // take the crawl down. The cached snapshot just goes stale (and the
    // dashboard's per-shard freshness shows it).
    logger.error({ err: err.message }, 'ledger stats worker died');
  });
  return {
    ready,
    latest: () => snapshot,
    close: async () => {
      await worker.terminate();
    },
  };
}
