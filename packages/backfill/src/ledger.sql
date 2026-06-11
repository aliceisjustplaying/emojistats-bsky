-- Crawl ledger (plan 0001). One row per DID; the only checkpoint the crawl has.
-- Timestamps are epoch milliseconds. Applied idempotently by ledger.ts.

CREATE TABLE IF NOT EXISTS repos (
  did               TEXT PRIMARY KEY,
  pds_host          TEXT NOT NULL,
  status            TEXT NOT NULL DEFAULT 'pending',
  rev               TEXT,
  car_bytes         INTEGER,
  records_total     INTEGER,
  posts_total       INTEGER,
  posts_with_emojis INTEGER,
  emoji_occurrences INTEGER,
  -- 64-bit XOR fold of the loaded rkey set (RepoCounts.rkeyDigest), fixed-width
  -- lowercase hex. Exists on fresh ledgers only: ledger.ts runs CREATE TABLE IF
  -- NOT EXISTS and nothing else — no ALTER shim, no backwards compatibility.
  -- Pre-digest ledgers are unsupported; delete and re-enumerate.
  rkey_digest       TEXT,
  attempts          INTEGER NOT NULL DEFAULT 0,
  error             TEXT,
  enumerated_at     INTEGER NOT NULL,
  fetched_at        INTEGER,
  loaded_at         INTEGER,
  retry_after       INTEGER
);

CREATE INDEX IF NOT EXISTS idx_repos_status ON repos (status, retry_after);

CREATE INDEX IF NOT EXISTS idx_repos_host_status ON repos (pds_host, status);

CREATE INDEX IF NOT EXISTS idx_repos_loaded_at ON repos (loaded_at) WHERE loaded_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
