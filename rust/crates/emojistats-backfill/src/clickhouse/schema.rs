use super::ClickHouseSchemaError;

const AGGREGATE_REBUILD_MAX_MEMORY_USAGE: u64 = 8_589_934_592;
const AGGREGATE_REBUILD_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY: u64 = 1_073_741_824;
pub(super) const AGGREGATE_REBUILD_SHADOW_SUFFIX: &str = "__rebuild_shadow";
const AGGREGATE_REBUILD_TABLES: [AggregateRebuildTable; 4] = [
    AggregateRebuildTable::EmojiTotal,
    AggregateRebuildTable::EmojiTotalByLang,
    AggregateRebuildTable::LangTotal,
    AggregateRebuildTable::PostsHourly,
];

#[derive(Debug, Clone, Copy)]
enum AggregateRebuildTable {
    EmojiTotal,
    EmojiTotalByLang,
    LangTotal,
    PostsHourly,
}

impl AggregateRebuildTable {
    const fn clickhouse_table(self) -> ClickHouseTable {
        match self {
            Self::EmojiTotal => ClickHouseTable::EmojiTotal,
            Self::EmojiTotalByLang => ClickHouseTable::EmojiTotalByLang,
            Self::LangTotal => ClickHouseTable::LangTotal,
            Self::PostsHourly => ClickHouseTable::PostsHourly,
        }
    }
}

/// Fixed `ClickHouse` table names owned by the v2 derive lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClickHouseTable {
    /// Compact post serving projection derived from archive rows.
    PostServing,
    /// Per-manifest total-post counters that cannot be reconstructed from emoji rows.
    TotalPostCounter,
    /// Aggregate emoji totals rebuilt from compact post-serving rows.
    EmojiTotal,
    /// Aggregate emoji totals by language rebuilt from compact post-serving rows.
    EmojiTotalByLang,
    /// Aggregate language totals rebuilt from compact post-serving rows.
    LangTotal,
    /// Aggregate hourly post counters rebuilt from compact post-serving rows.
    PostsHourly,
}

impl ClickHouseTable {
    /// Return the unqualified table name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::PostServing => "v2_post_serving_r3",
            Self::TotalPostCounter => "v2_total_post_counters_r3",
            Self::EmojiTotal => "v2_emoji_total_r3",
            Self::EmojiTotalByLang => "v2_emoji_total_by_lang_r3",
            Self::LangTotal => "v2_lang_total_r3",
            Self::PostsHourly => "v2_posts_hourly_r3",
        }
    }

    /// Return the schema SQL for this table in the given database.
    ///
    /// # Errors
    ///
    /// Returns [`ClickHouseSchemaError`] if the database name is not a valid `ClickHouse`
    /// identifier.
    pub fn create_table_sql(self, database: &str) -> Result<String, ClickHouseSchemaError> {
        let database = ClickHouseIdentifier::new(database)?;
        Ok(match self {
            Self::PostServing => post_serving_table_sql(&database),
            Self::TotalPostCounter => total_post_counter_table_sql(&database),
            Self::EmojiTotal => emoji_total_table_sql(&database),
            Self::EmojiTotalByLang => emoji_total_by_lang_table_sql(&database),
            Self::LangTotal => lang_total_table_sql(&database),
            Self::PostsHourly => posts_hourly_table_sql(&database),
        })
    }
}

/// Return all v2 derive `ClickHouse` table definitions as executable SQL statements.
///
/// # Errors
///
/// Returns [`ClickHouseSchemaError`] if the database name is not a valid `ClickHouse` identifier.
pub fn create_schema_sql(database: &str) -> Result<String, ClickHouseSchemaError> {
    let database = ClickHouseIdentifier::new(database)?;
    let statements = [
        ClickHouseTable::PostServing.create_table_sql(database.0.as_str())?,
        ClickHouseTable::TotalPostCounter.create_table_sql(database.0.as_str())?,
        ClickHouseTable::EmojiTotal.create_table_sql(database.0.as_str())?,
        ClickHouseTable::EmojiTotalByLang.create_table_sql(database.0.as_str())?,
        ClickHouseTable::LangTotal.create_table_sql(database.0.as_str())?,
        ClickHouseTable::PostsHourly.create_table_sql(database.0.as_str())?,
        post_serving_dedupe_token_migration_sql(&database),
        total_post_counter_dedupe_token_migration_sql(&database),
    ];
    Ok(statements.join("\n\n"))
}

fn post_serving_dedupe_token_migration_sql(database: &ClickHouseIdentifier) -> String {
    format!(
        "ALTER TABLE {database}.v2_post_serving_r3 ADD COLUMN IF NOT EXISTS derive_dedupe_token String CODEC(ZSTD(1)) AFTER src;"
    )
}

fn total_post_counter_dedupe_token_migration_sql(database: &ClickHouseIdentifier) -> String {
    format!(
        "ALTER TABLE {database}.v2_total_post_counters_r3 ADD COLUMN IF NOT EXISTS derive_dedupe_token String CODEC(ZSTD(1)) AFTER src;"
    )
}

/// Return aggregate rebuild SQL statements that derive serving caches from compact post rows.
///
/// # Errors
///
/// Returns [`ClickHouseSchemaError`] if the database name is not a valid `ClickHouse` identifier.
pub fn aggregate_rebuild_sql(database: &str) -> Result<String, ClickHouseSchemaError> {
    Ok(aggregate_rebuild_statements(database)?.join("\n\n"))
}

/// Return aggregate rebuild SQL statements in execution order.
///
/// # Errors
///
/// Returns [`ClickHouseSchemaError`] if the database name is not a valid `ClickHouse` identifier.
pub fn aggregate_rebuild_statements(database: &str) -> Result<Vec<String>, ClickHouseSchemaError> {
    let database = ClickHouseIdentifier::new(database)?;
    let mut statements = Vec::new();

    for table in AGGREGATE_REBUILD_TABLES {
        statements.extend(aggregate_rebuild_table_statements(&database, table));
    }

    Ok(statements)
}

fn aggregate_rebuild_table_statements(
    database: &ClickHouseIdentifier,
    table: AggregateRebuildTable,
) -> Vec<String> {
    let shadow_table = aggregate_rebuild_shadow_table_name(table);
    let target_table = table.clickhouse_table();
    vec![
        drop_table_sql(database, shadow_table.as_str()),
        create_aggregate_shadow_table_sql(database, table, shadow_table.as_str()),
        rebuild_aggregate_sql(database, table, shadow_table.as_str()),
        exchange_tables_sql(database, target_table.name(), shadow_table.as_str()),
        drop_table_sql(database, shadow_table.as_str()),
    ]
}

fn aggregate_rebuild_shadow_table_name(table: AggregateRebuildTable) -> String {
    format!(
        "{}{AGGREGATE_REBUILD_SHADOW_SUFFIX}",
        table.clickhouse_table().name()
    )
}

fn drop_table_sql(database: &ClickHouseIdentifier, table_name: &str) -> String {
    format!("DROP TABLE IF EXISTS {database}.{table_name} SYNC;")
}

fn exchange_tables_sql(
    database: &ClickHouseIdentifier,
    target_table: &str,
    shadow_table: &str,
) -> String {
    format!("EXCHANGE TABLES {database}.{target_table} AND {database}.{shadow_table};")
}

fn create_aggregate_shadow_table_sql(
    database: &ClickHouseIdentifier,
    table: AggregateRebuildTable,
    shadow_table: &str,
) -> String {
    match table {
        AggregateRebuildTable::EmojiTotal => {
            emoji_total_table_sql_for_name(database, shadow_table, CreateTableMode::Create)
        }
        AggregateRebuildTable::EmojiTotalByLang => {
            emoji_total_by_lang_table_sql_for_name(database, shadow_table, CreateTableMode::Create)
        }
        AggregateRebuildTable::LangTotal => {
            lang_total_table_sql_for_name(database, shadow_table, CreateTableMode::Create)
        }
        AggregateRebuildTable::PostsHourly => {
            posts_hourly_table_sql_for_name(database, shadow_table, CreateTableMode::Create)
        }
    }
}

fn rebuild_aggregate_sql(
    database: &ClickHouseIdentifier,
    table: AggregateRebuildTable,
    shadow_table: &str,
) -> String {
    match table {
        AggregateRebuildTable::EmojiTotal => rebuild_emoji_total_sql(database, shadow_table),
        AggregateRebuildTable::EmojiTotalByLang => {
            rebuild_emoji_total_by_lang_sql(database, shadow_table)
        }
        AggregateRebuildTable::LangTotal => rebuild_lang_total_sql(database, shadow_table),
        AggregateRebuildTable::PostsHourly => rebuild_posts_hourly_sql(database, shadow_table),
    }
}

fn aggregate_rebuild_settings_sql() -> String {
    format!(
        "SETTINGS max_memory_usage = {AGGREGATE_REBUILD_MAX_MEMORY_USAGE}, max_bytes_before_external_group_by = {AGGREGATE_REBUILD_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY};"
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ClickHouseIdentifier(String);

impl ClickHouseIdentifier {
    pub(super) fn new(value: &str) -> Result<Self, ClickHouseSchemaError> {
        if is_clickhouse_identifier(value) {
            Ok(Self(value.to_owned()))
        } else {
            Err(ClickHouseSchemaError::InvalidIdentifier {
                value: value.to_owned(),
            })
        }
    }
}

impl std::fmt::Display for ClickHouseIdentifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn is_clickhouse_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn post_serving_table_sql(database: &ClickHouseIdentifier) -> String {
    format!(
        r"CREATE TABLE IF NOT EXISTS {database}.v2_post_serving_r3 (
  src LowCardinality(String),
  derive_dedupe_token String CODEC(ZSTD(1)),
  run_id LowCardinality(String),
  shard LowCardinality(String),
  file_sequence UInt64,
  dataset LowCardinality(String),
  fetch_method LowCardinality(String),
  completeness_class LowCardinality(String),
  receipt_hash String CODEC(ZSTD(1)),
  normalizer_name LowCardinality(String),
  normalizer_semver LowCardinality(String),
  normalizer_git_rev LowCardinality(String),
  normalizer_unicode_version LowCardinality(String),
  normalizer_emoji_data_version LowCardinality(String),
  did String CODEC(ZSTD(1)),
  rkey String CODEC(ZSTD(1)),
  created_at Nullable(DateTime64(6, 'UTC')) CODEC(Delta(8), ZSTD(1)),
  created_at_parse_status LowCardinality(String),
  langs Array(LowCardinality(String)),
  emojis Array(LowCardinality(String)),
  emoji_occurrences UInt64,
  observed_at DateTime64(6, 'UTC'),
  inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY cityHash64(did) % 256
ORDER BY (src, normalizer_git_rev, dataset, fetch_method, completeness_class, derive_dedupe_token, did, rkey)
SETTINGS non_replicated_deduplication_window = 10000;"
    )
}

fn total_post_counter_table_sql(database: &ClickHouseIdentifier) -> String {
    format!(
        r"CREATE TABLE IF NOT EXISTS {database}.v2_total_post_counters_r3 (
  src LowCardinality(String),
  derive_dedupe_token String CODEC(ZSTD(1)),
  run_id LowCardinality(String),
  shard LowCardinality(String),
  file_sequence UInt64,
  dataset LowCardinality(String),
  fetch_method LowCardinality(String),
  completeness_class LowCardinality(String),
  receipt_hash String CODEC(ZSTD(1)),
  normalizer_name LowCardinality(String),
  normalizer_semver LowCardinality(String),
  normalizer_git_rev LowCardinality(String),
  normalizer_unicode_version LowCardinality(String),
  normalizer_emoji_data_version LowCardinality(String),
  did String CODEC(ZSTD(1)),
  posts_processed UInt64,
  posts_with_emojis UInt64,
  emoji_occurrences UInt64,
  min_created_at Nullable(DateTime64(6, 'UTC')) CODEC(Delta(8), ZSTD(1)),
  max_created_at Nullable(DateTime64(6, 'UTC')) CODEC(Delta(8), ZSTD(1)),
  inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (src, normalizer_git_rev, dataset, fetch_method, completeness_class, derive_dedupe_token, run_id, shard, file_sequence, receipt_hash, did)
SETTINGS non_replicated_deduplication_window = 10000;"
    )
}

fn emoji_total_table_sql(database: &ClickHouseIdentifier) -> String {
    emoji_total_table_sql_for_name(
        database,
        ClickHouseTable::EmojiTotal.name(),
        CreateTableMode::CreateIfNotExists,
    )
}

fn emoji_total_table_sql_for_name(
    database: &ClickHouseIdentifier,
    table_name: &str,
    mode: CreateTableMode,
) -> String {
    format!(
        r"{mode} {database}.{table_name} (
  src LowCardinality(String),
  normalizer_git_rev LowCardinality(String),
  dataset LowCardinality(String),
  fetch_method LowCardinality(String),
  completeness_class LowCardinality(String),
  emoji LowCardinality(String),
  occurrences UInt64,
  posts UInt64
) ENGINE = SummingMergeTree((occurrences, posts))
ORDER BY (src, normalizer_git_rev, dataset, fetch_method, completeness_class, emoji);"
    )
}

fn emoji_total_by_lang_table_sql(database: &ClickHouseIdentifier) -> String {
    emoji_total_by_lang_table_sql_for_name(
        database,
        ClickHouseTable::EmojiTotalByLang.name(),
        CreateTableMode::CreateIfNotExists,
    )
}

fn emoji_total_by_lang_table_sql_for_name(
    database: &ClickHouseIdentifier,
    table_name: &str,
    mode: CreateTableMode,
) -> String {
    format!(
        r"{mode} {database}.{table_name} (
  src LowCardinality(String),
  normalizer_git_rev LowCardinality(String),
  dataset LowCardinality(String),
  fetch_method LowCardinality(String),
  completeness_class LowCardinality(String),
  lang LowCardinality(String),
  emoji LowCardinality(String),
  occurrences UInt64,
  posts UInt64
) ENGINE = SummingMergeTree((occurrences, posts))
ORDER BY (src, normalizer_git_rev, dataset, fetch_method, completeness_class, lang, emoji);"
    )
}

fn lang_total_table_sql(database: &ClickHouseIdentifier) -> String {
    lang_total_table_sql_for_name(
        database,
        ClickHouseTable::LangTotal.name(),
        CreateTableMode::CreateIfNotExists,
    )
}

fn lang_total_table_sql_for_name(
    database: &ClickHouseIdentifier,
    table_name: &str,
    mode: CreateTableMode,
) -> String {
    format!(
        r"{mode} {database}.{table_name} (
  src LowCardinality(String),
  normalizer_git_rev LowCardinality(String),
  dataset LowCardinality(String),
  fetch_method LowCardinality(String),
  completeness_class LowCardinality(String),
  lang LowCardinality(String),
  occurrences UInt64,
  posts UInt64
) ENGINE = SummingMergeTree((occurrences, posts))
ORDER BY (src, normalizer_git_rev, dataset, fetch_method, completeness_class, lang);"
    )
}

fn posts_hourly_table_sql(database: &ClickHouseIdentifier) -> String {
    posts_hourly_table_sql_for_name(
        database,
        ClickHouseTable::PostsHourly.name(),
        CreateTableMode::CreateIfNotExists,
    )
}

fn posts_hourly_table_sql_for_name(
    database: &ClickHouseIdentifier,
    table_name: &str,
    mode: CreateTableMode,
) -> String {
    format!(
        r"{mode} {database}.{table_name} (
  hour DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
  src LowCardinality(String),
  normalizer_git_rev LowCardinality(String),
  dataset LowCardinality(String),
  fetch_method LowCardinality(String),
  completeness_class LowCardinality(String),
  posts UInt64,
  posts_with_emojis UInt64,
  emoji_occurrences UInt64
) ENGINE = SummingMergeTree((posts, posts_with_emojis, emoji_occurrences))
PARTITION BY toYear(hour)
ORDER BY (src, normalizer_git_rev, dataset, fetch_method, completeness_class, hour);"
    )
}

#[derive(Debug, Clone, Copy)]
enum CreateTableMode {
    Create,
    CreateIfNotExists,
}

impl std::fmt::Display for CreateTableMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Create => "CREATE TABLE",
            Self::CreateIfNotExists => "CREATE TABLE IF NOT EXISTS",
        })
    }
}

fn rebuild_emoji_total_sql(database: &ClickHouseIdentifier, target_table: &str) -> String {
    let settings = aggregate_rebuild_settings_sql();
    format!(
        r"INSERT INTO {database}.{target_table}
SELECT
  src,
  normalizer_git_rev,
  dataset,
  fetch_method,
  completeness_class,
  emoji,
  count() AS occurrences,
  uniqExact(did, rkey) AS posts
FROM {database}.v2_post_serving_r3 FINAL
ARRAY JOIN emojis AS emoji
GROUP BY src, normalizer_git_rev, dataset, fetch_method, completeness_class, emoji
{settings}"
    )
}

fn rebuild_emoji_total_by_lang_sql(database: &ClickHouseIdentifier, target_table: &str) -> String {
    let settings = aggregate_rebuild_settings_sql();
    format!(
        r"INSERT INTO {database}.{target_table}
SELECT
  src,
  normalizer_git_rev,
  dataset,
  fetch_method,
  completeness_class,
  arrayJoin(langs) AS lang,
  arrayJoin(emojis) AS emoji,
  count() AS occurrences,
  uniqExact(did, rkey) AS posts
FROM {database}.v2_post_serving_r3 FINAL
WHERE notEmpty(langs) AND notEmpty(emojis)
GROUP BY src, normalizer_git_rev, dataset, fetch_method, completeness_class, lang, emoji
{settings}"
    )
}

fn rebuild_lang_total_sql(database: &ClickHouseIdentifier, target_table: &str) -> String {
    let settings = aggregate_rebuild_settings_sql();
    format!(
        r"INSERT INTO {database}.{target_table}
SELECT
  src,
  normalizer_git_rev,
  dataset,
  fetch_method,
  completeness_class,
  lang,
  sum(emoji_occurrences) AS occurrences,
  countIf(emoji_occurrences > 0) AS posts
FROM {database}.v2_post_serving_r3 FINAL
ARRAY JOIN langs AS lang
GROUP BY src, normalizer_git_rev, dataset, fetch_method, completeness_class, lang
{settings}"
    )
}

fn rebuild_posts_hourly_sql(database: &ClickHouseIdentifier, target_table: &str) -> String {
    let settings = aggregate_rebuild_settings_sql();
    format!(
        r"INSERT INTO {database}.{target_table}
SELECT
  toStartOfHour(coalesce(created_at, toDateTime64('1970-01-01 00:00:00', 6, 'UTC'))) AS hour,
  src,
  normalizer_git_rev,
  dataset,
  fetch_method,
  completeness_class,
  count() AS posts,
  countIf(emoji_occurrences > 0) AS posts_with_emojis,
  sum(emoji_occurrences) AS total_emoji_occurrences
FROM {database}.v2_post_serving_r3 FINAL
GROUP BY hour, src, normalizer_git_rev, dataset, fetch_method, completeness_class
{settings}"
    )
}
