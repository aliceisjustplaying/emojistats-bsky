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

export const CLICKHOUSE_URL =
  process.env.CLICKHOUSE_URL ?? 'http://localhost:8123';
export const CLICKHOUSE_DATABASE =
  process.env.CLICKHOUSE_DATABASE ?? 'emojistats';
export const CLICKHOUSE_USER = process.env.CLICKHOUSE_USER ?? 'emojistats';
export const CLICKHOUSE_PASSWORD =
  process.env.CLICKHOUSE_PASSWORD ?? 'emojistats';

export const JETSTREAM_ENDPOINT =
  process.env.JETSTREAM_ENDPOINT ??
  'wss://jetstream2.us-east.bsky.network/subscribe';

// 1s flush is a product requirement (plan 0001): part pressure gets solved server-side, never by slowing this.
export const FLUSH_INTERVAL_MS = num('FLUSH_INTERVAL_MS', 1000);
// Fail-loud cap: if ClickHouse is down long enough to hit this, crash rather than silently drop rows.
export const WRITER_MAX_BUFFER_ROWS = num('WRITER_MAX_BUFFER_ROWS', 500_000);

export const DEDUPE_DB_PATH =
  process.env.DEDUPE_DB_PATH ?? path.join(PACKAGE_ROOT, 'data', 'seen.sqlite');
export const DEDUPE_RETENTION_HOURS = num('DEDUPE_RETENTION_HOURS', 72);
export const DEDUPE_CLEANUP_INTERVAL_MS = num(
  'DEDUPE_CLEANUP_INTERVAL_MS',
  3_600_000,
);

export const CURSOR_FILE_PATH =
  process.env.CURSOR_FILE_PATH ?? path.join(PACKAGE_ROOT, 'data', 'cursor.txt');
export const CURSOR_OVERRIDE_PATH =
  process.env.CURSOR_OVERRIDE_PATH ??
  path.join(REPO_ROOT, 'CURSOR_OVERRIDE.TXT');
export const CURSOR_SAVE_INTERVAL_MS = num('CURSOR_SAVE_INTERVAL_MS', 10_000);
// Generous rewind because ClickHouse acks inserts before fsync: a power cut can
// evaporate rows the cursor already advanced past. Replaying 2 minutes costs
// nothing (the seen-set rejects duplicates); losing posts costs forever.
export const CURSOR_REWIND_US = num('CURSOR_REWIND_US', 120_000_000);

export const STATS_LOG_INTERVAL_MS = num('STATS_LOG_INTERVAL_MS', 10_000);

// Bluesky's limit is 300 graphemes, so a lexicon-valid post can never exceed 300 matches;
// the cap only truncates over-limit records from non-validating PDSes.
export const EMOJI_MAX_PER_POST = num('EMOJI_MAX_PER_POST', 300);

// Cost-revised storage (plan 0001): ClickHouse keeps text for emoji posts only;
// the Parquet archive is the durable home of ALL text. 'all' restores the
// original everything-in-CH behavior. Raw string on purpose: validation and all
// behavior decisions live in archive/policy (resolveStoragePolicy at startup).
export const TEXT_IN_CLICKHOUSE = process.env.TEXT_IN_CLICKHOUSE ?? 'emoji';

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
export const ARCHIVE_SYNC_COMMAND = process.env.ARCHIVE_SYNC_COMMAND; // deploy-time rclone/scp hook

// createdAt sanity window: clients lie about timestamps; outside this window we fall back (TID, then receive time).
export const MIN_CREATED_AT_MS = Date.UTC(2022, 10, 16); // 2022-11-16, around the first sandbox posts
export const CREATED_AT_FUTURE_SLACK_MS = 48 * 3_600_000;

export const LOG_LEVEL = process.env.LOG_LEVEL ?? 'info';
