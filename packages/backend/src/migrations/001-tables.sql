CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS posts (
  id BIGSERIAL,
  did TEXT NOT NULL,
  rkey TEXT NOT NULL,
  text TEXT,
  has_emojis BOOLEAN NOT NULL DEFAULT FALSE,
  langs TEXT[] NOT NULL DEFAULT '{}',
  emojis TEXT[] NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (id, created_at)
);

CREATE UNIQUE INDEX idx_posts_id_created_at ON posts (id, created_at);

SELECT
  create_hypertable (
    'posts',
    'created_at',
    if_not_exists => TRUE,
    migrate_data => TRUE,
    chunk_time_interval => INTERVAL '1 hour'
  );

CREATE TABLE IF NOT EXISTS emojis (
  id BIGSERIAL,
  did TEXT NOT NULL,
  rkey TEXT NOT NULL,
  emoji TEXT NOT NULL,
  lang TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX idx_emojis_id_created_at ON emojis (id, created_at);

SELECT
  create_hypertable (
    'emojis',
    'created_at',
    if_not_exists => TRUE,
    migrate_data => TRUE,
    chunk_time_interval => INTERVAL '1 hour'
  );
