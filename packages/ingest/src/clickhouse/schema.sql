-- emojistats ClickHouse schema (plan 0001): tables only.
-- Applied by migrate.ts: full-line comments are stripped, statements split on a
-- semicolon at end of line. Keep one statement per block and semicolons only there.
-- The materialized views feeding the Summing tables are NOT defined here — their
-- SELECTs live in aggregates.ts (single source shared with rebuild.ts) and
-- migrate.ts creates them right after this file is applied.
--
-- Doctrine: `posts` is the only truth. Every other table is a disposable cache
-- maintained by materialized views and rebuilt from `posts` whenever in doubt.
-- Duplicate raw inserts are expected (backfill/live overlap, retries) and collapse
-- via ReplacingMergeTree; aggregates therefore over-count duplicates until rebuilt.
-- Always query Summing tables with sum() + GROUP BY — merges settle lazily.

CREATE TABLE IF NOT EXISTS posts (
  did         String CODEC(ZSTD(1)),
  rkey        String CODEC(ZSTD(1)),
  created_at  DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
  text        String CODEC(ZSTD(6)),
  langs       Array(LowCardinality(String)),
  emojis      Array(LowCardinality(String)),
  src         LowCardinality(String),
  -- Microsecond version column: with second-resolution DateTime, live and
  -- backfill copies of one post landing in the same second tie, and a tied
  -- ReplacingMergeTree version keeps an arbitrary row — "later ingest wins"
  -- must be deterministic for the rkey-reuse semantics documented below.
  ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(created_at)
-- Key soundness (atproto record-key spec): (did, rkey) alone is NOT unique —
-- only (did, collection, rkey) is. This key is safe because every ingest path
-- is hard-filtered to the single collection app.bsky.feed.post (Jetstream
-- wantedCollections; the CAR walker's POST_COLLECTION), and within one
-- collection the repo MST is a key/value map: one record per rkey at a time.
-- Across time an rkey CAN be deleted and re-created (or a record updated);
-- the later ingest then wins this merge and the earlier text is dropped from
-- truth, while aggregates keep both raw arrivals until a rebuild re-derives
-- from survivors. Pathological for posts (TID reuse must be forced), bounded,
-- and verify surfaces it as a loose-tier digest mismatch rather than a pass.
ORDER BY (did, rkey)
-- Makes insert_deduplication_token effective (default 0 = inert on non-replicated
-- tables): immediate re-inserts of the same chunk don't double-fire the MVs.
-- Long-horizon re-loads still over-count aggregates until rebuild — by design.
SETTINGS non_replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS emoji_hourly (
  hour        DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
  emoji       LowCardinality(String),
  occurrences UInt64,
  posts       UInt64
) ENGINE = SummingMergeTree((occurrences, posts))
PARTITION BY toYear(hour)
ORDER BY (emoji, hour);

CREATE TABLE IF NOT EXISTS emoji_hourly_by_lang (
  lang        LowCardinality(String),
  hour        DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
  emoji       LowCardinality(String),
  occurrences UInt64,
  posts       UInt64
) ENGINE = SummingMergeTree((occurrences, posts))
PARTITION BY toYear(hour)
ORDER BY (lang, emoji, hour);

CREATE TABLE IF NOT EXISTS posts_hourly (
  hour              DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
  posts             UInt64,
  posts_with_emojis UInt64,
  emoji_occurrences UInt64
) ENGINE = SummingMergeTree((posts, posts_with_emojis, emoji_occurrences))
PARTITION BY toYear(hour)
ORDER BY hour;

CREATE TABLE IF NOT EXISTS emoji_total (
  emoji       LowCardinality(String),
  occurrences UInt64,
  posts       UInt64
) ENGINE = SummingMergeTree((occurrences, posts))
ORDER BY emoji;

CREATE TABLE IF NOT EXISTS emoji_total_by_lang (
  lang        LowCardinality(String),
  emoji       LowCardinality(String),
  occurrences UInt64,
  posts       UInt64
) ENGINE = SummingMergeTree((occurrences, posts))
ORDER BY (lang, emoji);

CREATE TABLE IF NOT EXISTS lang_total (
  lang        LowCardinality(String),
  occurrences UInt64,
  posts       UInt64
) ENGINE = SummingMergeTree((occurrences, posts))
ORDER BY lang;

-- Crawl telemetry: written by the backfill crawler (any number of processes or
-- boxes), read by the dashboard. ClickHouse is the shared bus so the dashboard
-- works across machines and the throughput history survives every restart.
CREATE TABLE IF NOT EXISTS backfill_progress (
  ts               DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
  run_id           LowCardinality(String),
  shard            LowCardinality(String),
  pending          UInt64,
  fetching         UInt64,
  loaded           UInt64,
  verified         UInt64,
  empty            UInt64,
  tombstoned       UInt64,
  deactivated      UInt64,
  takendown        UInt64,
  unreachable      UInt64,
  quarantined      UInt64,
  failed           UInt64,
  posts_loaded     UInt64,
  bytes_downloaded UInt64,
  rows_per_sec     Float32,
  in_flight        UInt16
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (run_id, shard, ts)
TTL ts + INTERVAL 6 MONTH DELETE;

CREATE TABLE IF NOT EXISTS backfill_repo_events (
  ts        DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
  did       String CODEC(ZSTD(1)),
  pds_host  LowCardinality(String),
  event     LowCardinality(String),
  posts     UInt32,
  records   UInt32,
  car_bytes UInt64,
  error     String CODEC(ZSTD(3))
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY ts
TTL ts + INTERVAL 6 MONTH DELETE;

CREATE TABLE IF NOT EXISTS backfill_verify_progress (
  ts              DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
  run_id          LowCardinality(String),
  shard           LowCardinality(String),
  ledger_path     String CODEC(ZSTD(1)),
  phase           LowCardinality(String),
  repos_total     UInt64,
  repos_checked   UInt64,
  exact           UInt64,
  loose           UInt64,
  mismatches      UInt64,
  loose_emitted   UInt64,
  sample_checked  UInt64,
  sample_failures UInt64,
  done            UInt8,
  error           String CODEC(ZSTD(3))
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (run_id, shard, ts)
TTL ts + INTERVAL 6 MONTH DELETE;

CREATE TABLE IF NOT EXISTS backfill_status_reason_counts (
  ts          DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
  snapshot_id LowCardinality(String),
  shard       LowCardinality(String),
  status      LowCardinality(String),
  reason      LowCardinality(String),
  count       UInt64
) ENGINE = ReplacingMergeTree(ts)
PARTITION BY toYYYYMM(ts)
ORDER BY (snapshot_id, shard, status, reason)
TTL ts + INTERVAL 6 MONTH DELETE;
