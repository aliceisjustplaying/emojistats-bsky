import path from 'node:path';
import { fileURLToPath } from 'node:url';

import dotenv from 'dotenv';

const PACKAGE_ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '..',
);
export const REPO_ROOT = path.resolve(PACKAGE_ROOT, '../..');

dotenv.config({ path: path.join(PACKAGE_ROOT, '.env') });

function num(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value))
    throw new Error(`Invalid numeric env var ${name}: ${raw}`);
  return value;
}

function bool(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  if (raw === 'true') return true;
  if (raw === 'false') return false;
  throw new Error(`Invalid boolean env var ${name}: ${raw}`);
}

export const CLICKHOUSE_URL =
  process.env.CLICKHOUSE_URL ?? 'http://localhost:8123';
export const CLICKHOUSE_DATABASE =
  process.env.CLICKHOUSE_DATABASE ?? 'emojistats';
export const CLICKHOUSE_USER = process.env.CLICKHOUSE_USER ?? 'emojistats';
export const CLICKHOUSE_PASSWORD =
  process.env.CLICKHOUSE_PASSWORD ?? 'emojistats';
export const CLICKHOUSE_REQUEST_TIMEOUT_MS = num(
  'CLICKHOUSE_REQUEST_TIMEOUT_MS',
  180_000,
);

export const LEDGER_DB_PATH =
  process.env.LEDGER_DB_PATH ??
  path.join(PACKAGE_ROOT, 'data', 'ledger.sqlite');

export const PLC_DIRECTORY_URL =
  process.env.PLC_DIRECTORY_URL ?? 'https://plc.directory';
export const RELAY_URL =
  process.env.RELAY_URL ?? 'https://relay1.us-east.bsky.network';

// Politeness: the per-host download concurrency is the lever that matters;
// getRepo is one request per repo but a potentially huge body.
export const GLOBAL_CONCURRENCY = num('GLOBAL_CONCURRENCY', 32);
export const PER_HOST_CONCURRENCY = num('PER_HOST_CONCURRENCY', 2);
// Bluesky's mushroom fleet (*.host.bsky.network) tolerates serious parallelism
// (operator experience: Alice). The protocol signals are the real governor —
// 429/Retry-After always wins, so err high and let the fleet push back.
export const PER_HOST_CONCURRENCY_BSKY = num('PER_HOST_CONCURRENCY_BSKY', 16);
export const REPO_FETCH_TIMEOUT_MS = num('REPO_FETCH_TIMEOUT_MS', 300_000);
// Max time a getRepo may make NO forward progress — no response headers, or no
// body bytes since the last chunk — before the socket is declared dead. This is
// the wedge cure: AbortSignal.timeout does not reliably interrupt a half-open
// socket (no FIN/RST, read() hangs forever), so a stalled fetch would hold its
// GLOBAL_CONCURRENCY slot indefinitely; enough leaks freeze the scheduler. A
// self-driven progress timer rejects the hung await regardless of whether the
// abort reaches the socket, so every fetch settles within this window and the
// slot is always freed. Inactivity-based (reset per chunk), so a slow-but-alive
// host streaming steadily is never killed; only true silence trips it. Sits
// well under the 180s wedge-watchdog threshold so stalls self-heal without a
// restart. REPO_FETCH_TIMEOUT_MS remains the absolute wall-clock cap.
export const REPO_FETCH_STALL_MS = num('REPO_FETCH_STALL_MS', 60_000);
// Safety valve only. Real Bluesky repos can exceed 1 GiB, so the default must
// stay well above observed production CARs. Set CAR_MAX_BYTES lower only for
// targeted tests or emergency memory containment.
export const CAR_MAX_BYTES = num('CAR_MAX_BYTES', 64 * 1024 * 1024 * 1024);
// Parse worker threads (0 = auto: availableParallelism - 2, min 1). CAR
// parsing is pure CPU; on the main thread it starves every socket and timer.
export const PARSE_WORKERS = num('PARSE_WORKERS', 0);

export const RETRY_BASE_MS = num('RETRY_BASE_MS', 60_000);
export const RETRY_MAX_MS = num('RETRY_MAX_MS', 3_600_000);
export const MAX_ATTEMPTS = num('MAX_ATTEMPTS', 5);

// Cross-repo insert batching (see loader.ts): flush the shared buffer at
// LOADER_BATCH_ROWS rows or when the oldest buffered row is LOADER_FLUSH_MS
// old, whichever comes first. Sized so each box inserts ~4×/min instead of
// ~4×/s — the per-month partition fan-out made tiny inserts a parts storm.
export const LOADER_BATCH_ROWS = num('LOADER_BATCH_ROWS', 200_000);
// 5s, not 15s: every repo's pipeline slot is held until its rows' flush
// lands (finish() barrier), so flush latency is slot occupancy. ClickHouse
// p95 insert latency is sub-second; the parts-storm risk that motivated 15s
// scales with insert COUNT, and cross-repo batching already collapsed that.
export const LOADER_FLUSH_MS = num('LOADER_FLUSH_MS', 5_000);
export const CRASH_RECONCILE_ON_STARTUP = bool(
  'CRASH_RECONCILE_ON_STARTUP',
  false,
);

export const USER_AGENT =
  process.env.BACKFILL_USER_AGENT ??
  'emojistats-backfill/0.1 (+https://github.com/aliceisjustplaying/emojistats-bsky)';

export const STATS_LOG_INTERVAL_MS = num('STATS_LOG_INTERVAL_MS', 10_000);
export const LOG_LEVEL = process.env.LOG_LEVEL ?? 'info';

// Cost-revised storage (plan 0001): CH keeps text for emoji posts only; the
// Parquet archive holds ALL text and is a hard prerequisite of the real crawl.
export const TEXT_IN_CLICKHOUSE = (process.env.TEXT_IN_CLICKHOUSE ??
  'emoji') as 'emoji' | 'all';
export const ARCHIVE_ENABLED =
  (process.env.ARCHIVE_ENABLED ?? 'true') === 'true';
export const ARCHIVE_DIR =
  process.env.ARCHIVE_DIR ?? path.join(PACKAGE_ROOT, 'data', 'archive');
export const ARCHIVE_MAX_ROWS_PER_FILE = num(
  'ARCHIVE_MAX_ROWS_PER_FILE',
  1_000_000,
);
export const ARCHIVE_MAX_FILE_AGE_MS = num(
  'ARCHIVE_MAX_FILE_AGE_MS',
  3_600_000,
);
export const ARCHIVE_SYNC_COMMAND = process.env.ARCHIVE_SYNC_COMMAND;

// Telemetry to ClickHouse (backfill_progress / backfill_repo_events): the
// dashboard's data source, shared across crawl processes and boxes.
export const TELEMETRY_INTERVAL_MS = num('TELEMETRY_INTERVAL_MS', 10_000);
export const TELEMETRY_EVENT_BATCH_ROWS = num(
  'TELEMETRY_EVENT_BATCH_ROWS',
  1_000,
);
export const BACKFILL_RUN_ID = process.env.BACKFILL_RUN_ID ?? 'dev';
export const CRAWL_SHARDS = num('CRAWL_SHARDS', 1);
export const CRAWL_SHARD_INDEX = num('CRAWL_SHARD_INDEX', 0);
export const SHARD_LABEL =
  process.env.SHARD_LABEL ?? `shard${CRAWL_SHARD_INDEX}`;
