SELECT
  add_continuous_aggregate_policy (
    'emoji_stats_realtime',
    if_not_exists => TRUE,
    start_offset => INTERVAL '4 weeks',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 day'
  );

SELECT
  add_continuous_aggregate_policy (
    'emoji_stats_hourly',
    if_not_exists => TRUE,
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '6 hours'
  );

SELECT
  add_continuous_aggregate_policy (
    'emoji_stats_per_language_realtime',
    if_not_exists => TRUE,
    start_offset => INTERVAL '4 weeks',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 day'
  );

SELECT
  add_continuous_aggregate_policy (
    'emoji_stats_per_language_hourly',
    if_not_exists => TRUE,
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '6 hours'
  );

SELECT
  add_continuous_aggregate_policy (
    'language_stats_realtime',
    if_not_exists => TRUE,
    start_offset => INTERVAL '4 weeks',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 day'
  );

SELECT
  add_continuous_aggregate_policy (
    'language_stats_hourly',
    if_not_exists => TRUE,
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '6 hours'
  );
