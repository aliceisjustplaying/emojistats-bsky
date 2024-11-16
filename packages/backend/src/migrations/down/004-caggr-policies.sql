SELECT
  remove_continuous_aggregate_policy ('emoji_stats_realtime', if_exists => TRUE);

SELECT
  remove_continuous_aggregate_policy ('emoji_stats_hourly', if_exists => TRUE);

SELECT
  remove_continuous_aggregate_policy (
    'emoji_stats_per_language_realtime',
    if_exists => TRUE
  );

SELECT
  remove_continuous_aggregate_policy (
    'emoji_stats_per_language_hourly',
    if_exists => TRUE
  );

SELECT
  remove_continuous_aggregate_policy ('language_stats_realtime', if_exists => TRUE);

SELECT
  remove_continuous_aggregate_policy ('language_stats_hourly', if_exists => TRUE);
