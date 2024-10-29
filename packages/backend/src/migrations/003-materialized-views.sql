CREATE MATERIALIZED VIEW IF NOT EXISTS emoji_stats_realtime
WITH
  (
    timescaledb.continuous,
    timescaledb.materialized_only = FALSE
  ) AS
SELECT
  time_bucket ('1 week', created_at) AS bucket,
  emoji,
  COUNT(*) AS count
FROM
  emojis_new
GROUP BY
  bucket,
  emoji;

CREATE MATERIALIZED VIEW IF NOT EXISTS emoji_stats_hourly
WITH
  (
    timescaledb.continuous,
    timescaledb.materialized_only = FALSE
  ) AS
SELECT
  time_bucket ('1 hour', created_at) AS bucket,
  emoji,
  COUNT(*) AS count
FROM
  emojis_new
GROUP BY
  bucket,
  emoji;

CREATE MATERIALIZED VIEW IF NOT EXISTS emoji_stats_per_language_realtime
WITH
  (
    timescaledb.continuous,
    timescaledb.materialized_only = FALSE
  ) AS
SELECT
  time_bucket ('1 week', created_at) AS bucket,
  lang,
  emoji,
  COUNT(*) AS count
FROM
  emojis
GROUP BY
  bucket,
  lang,
  emoji;

CREATE MATERIALIZED VIEW IF NOT EXISTS emoji_stats_per_language_hourly
WITH
  (
    timescaledb.continuous,
    timescaledb.materialized_only = FALSE
  ) AS
SELECT
  time_bucket ('1 hour', created_at) AS bucket,
  lang,
  emoji,
  COUNT(*) AS count
FROM
  emojis
GROUP BY
  bucket,
  lang,
  emoji;

CREATE MATERIALIZED VIEW IF NOT EXISTS language_stats_realtime
WITH
  (
    timescaledb.continuous,
    timescaledb.materialized_only = FALSE
  ) AS
SELECT
  time_bucket ('1 week', created_at) AS bucket,
  lang,
  COUNT(*) AS count
FROM
  emojis
GROUP BY
  bucket,
  lang;

CREATE MATERIALIZED VIEW IF NOT EXISTS language_stats_hourly
WITH
  (
    timescaledb.continuous,
    timescaledb.materialized_only = FALSE
  ) AS
SELECT
  time_bucket ('1 hour', created_at) AS bucket,
  lang,
  COUNT(*) AS count
FROM
  emojis
GROUP BY
  bucket,
  lang;
