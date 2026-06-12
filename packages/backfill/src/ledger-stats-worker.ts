/**
 * Off-thread ledger aggregates (worker side of ledger-stats.ts).
 *
 * statusCounts + totalPostsLoaded each walk millions of index entries on a
 * 67M-row ledger. Run synchronously on the crawl main thread by the 10s
 * telemetry tick (as they were until 2026-06-12), they grew with enumeration
 * into ~10s event-loop freezes per tick — bottleneck #11, the one that
 * starved sockets, claim passes and ClickHouse inserts fleet-wide. The
 * profile read: 91.5% totalPostsLoaded, 8.5% statusCounts, both under
 * Statement.get/all inside the telemetry tick. WAL mode makes concurrent
 * readonly readers free, so the aggregates live on this thread now and the
 * main thread only ever touches a cached copy.
 */
import { parentPort, workerData } from 'node:worker_threads';

import Database from 'better-sqlite3';

import type { RepoStatus } from './types.js';

export interface LedgerStatsSnapshot {
  statusCounts: Partial<Record<RepoStatus, number>>;
  postsLoaded: number;
  computedAt: number;
}

const { dbPath, shards, shardIndex, intervalMs } = workerData as {
  dbPath: string;
  shards: number;
  shardIndex: number;
  intervalMs: number;
};

const db = new Database(dbPath, { readonly: true });
const shardWhere = shards > 1 ? `WHERE bucket = ${shardIndex}` : '';
const stmtCounts = db.prepare(
  `SELECT status, COUNT(*) AS n FROM repos ${shardWhere} GROUP BY status`,
);
const stmtPosts = db.prepare(
  `SELECT COALESCE(SUM(posts_total), 0) AS n FROM repos ${shardWhere}`,
);

function snapshot(): LedgerStatsSnapshot {
  const statusCounts: Partial<Record<RepoStatus, number>> = {};
  for (const row of stmtCounts.all() as Array<{ status: string; n: number }>)
    statusCounts[row.status as RepoStatus] = row.n;
  return {
    statusCounts,
    postsLoaded: (stmtPosts.get() as { n: number }).n,
    computedAt: Date.now(),
  };
}

function tick(): void {
  try {
    parentPort!.postMessage(snapshot());
  } catch {
    // Transient read hiccup (e.g. WAL checkpoint): keep the previous
    // snapshot current rather than crash the stats thread.
  }
  setTimeout(tick, intervalMs);
}
tick();
