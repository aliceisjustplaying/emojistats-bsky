import 'dotenv/config';
import pg from 'pg';

const { Client } = pg;

const client = new Client({
  connectionString: process.env.DATABASE_URL!,
});

await client.connect();

export async function createTables() {
  await client.query(`
    CREATE TABLE IF NOT EXISTS posts (
      did TEXT NOT NULL, -- ~32 characters
      rkey TEXT NOT NULL, -- ~13 characters
      text TEXT,
      has_emojis BOOLEAN NOT NULL DEFAULT FALSE,
      langs TEXT[] NOT NULL DEFAULT '{}',
      emojis TEXT[] NOT NULL DEFAULT '{}',
      created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (now() at time zone 'utc'),
      UNIQUE (did, rkey)
    );

    CREATE TABLE IF NOT EXISTS emojis (
      did TEXT NOT NULL, -- ~32 characters
      rkey TEXT NOT NULL, -- ~13 characters
      emoji TEXT NOT NULL,
      lang TEXT NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (now() at time zone 'utc')
    );
  `);

  await client.query(`
    SELECT create_hypertable(
      'emojis',
      'created_at',
      if_not_exists => TRUE,
      migrate_data => TRUE,
      chunk_time_interval => INTERVAL '1 hour'
    );
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_emojis_did ON emojis(did);
    CREATE INDEX IF NOT EXISTS idx_emojis_rkey ON emojis(rkey);
    CREATE INDEX IF NOT EXISTS idx_emojis_created_at ON emojis(created_at DESC);
  `);

  await client.query(`
    -- Create a continuous aggregate for overall emoji stats
    CREATE MATERIALIZED VIEW IF NOT EXISTS emoji_stats_overall
    WITH (timescaledb.continuous) AS
    SELECT
      time_bucket('1 second', created_at) AS bucket,
      emoji,
      COUNT(*) AS count
    FROM emojis
    GROUP BY bucket, emoji;
  `);

  await client.query(`
    -- Create a continuous aggregate for per-language emoji stats
    CREATE MATERIALIZED VIEW IF NOT EXISTS emoji_stats_per_language
    WITH (timescaledb.continuous) AS
    SELECT
      time_bucket('1 second', created_at) AS bucket,
      lang,
      emoji,
      COUNT(*) AS count
    FROM emojis
    GROUP BY bucket, lang, emoji;
  `);

  await client.query(`
    -- Create a continuous aggregate for language stats
    CREATE MATERIALIZED VIEW IF NOT EXISTS language_stats
    WITH (timescaledb.continuous) AS
    SELECT
      time_bucket('1 second', created_at) AS bucket,
      lang,
      COUNT(*) AS count
    FROM emojis
      GROUP BY bucket, lang;
  `);

  await client.query(`
    -- Set policies to refresh continuous aggregates every second
    SELECT add_continuous_aggregate_policy('emoji_stats_overall',
      if_not_exists => TRUE,
      start_offset => INTERVAL '1 hour',
      end_offset => INTERVAL '0 second',
      schedule_interval => INTERVAL '1 second');

    SELECT add_continuous_aggregate_policy('emoji_stats_per_language',
      if_not_exists => TRUE,
      start_offset => INTERVAL '1 hour',
      end_offset => INTERVAL '0 second',
      schedule_interval => INTERVAL '1 second');

    SELECT add_continuous_aggregate_policy('language_stats',
      if_not_exists => TRUE,
      start_offset => INTERVAL '1 hour',
      end_offset => INTERVAL '0 second',
      schedule_interval => INTERVAL '1 second');
`);

  await client.query(`
    ALTER MATERIALIZED VIEW emoji_stats_overall SET (timescaledb.materialized_only = FALSE);
    ALTER MATERIALIZED VIEW emoji_stats_per_language SET (timescaledb.materialized_only = FALSE);
    ALTER MATERIALIZED VIEW language_stats SET (timescaledb.materialized_only = FALSE);
  `);
}

createTables()
  .catch((e: unknown) => {
    console.error(e);
  })
  .finally(() => void client.end());
