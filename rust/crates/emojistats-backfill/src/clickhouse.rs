//! `ClickHouse` schema and insert-format skeleton for the v2 derive lane.

use std::time::Duration;

use reqwest::{
    Client, Method, Request, Url,
    header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT},
};
use serde::Serialize;

mod execute;

#[cfg(test)]
use execute::classify_insert_status;
pub use execute::execute_insert_payloads;

use crate::{
    archive::EmojiProjectionRow,
    derive::{
        BACKFILL_DERIVE_SOURCE, ClickHouseDeriveBatch, DeriveManifestIdentity,
        TotalPostCounterInput,
    },
};

/// `ClickHouse` HTTP insert format used by the derive lane.
pub const JSON_EACH_ROW_FORMAT: &str = "JSONEachRow";

const INSERT_DEDUPLICATE_SETTING: &str = "insert_deduplicate";
const INSERT_DEDUPLICATION_TOKEN_SETTING: &str = "insert_deduplication_token";
const DATE_TIME_INPUT_FORMAT_SETTING: &str = "date_time_input_format";
const CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES: usize = 4_096;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const DEFAULT_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(2);
const DEFAULT_MAX_INSERT_ATTEMPTS: u8 = 3;
const CLICKHOUSE_USER_HEADER: HeaderName = HeaderName::from_static("x-clickhouse-user");
const CLICKHOUSE_KEY_HEADER: HeaderName = HeaderName::from_static("x-clickhouse-key");
const CLICKHOUSE_DATABASE_HEADER: HeaderName = HeaderName::from_static("x-clickhouse-database");

/// Fixed `ClickHouse` table names owned by the v2 derive lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClickHouseTable {
    /// Compact emoji serving projection derived from archive rows.
    EmojiServing,
    /// Per-manifest total-post counters that cannot be reconstructed from emoji rows.
    TotalPostCounter,
}

impl ClickHouseTable {
    /// Return the unqualified table name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::EmojiServing => "v2_emoji_serving",
            Self::TotalPostCounter => "v2_total_post_counters",
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
            Self::EmojiServing => emoji_serving_table_sql(&database),
            Self::TotalPostCounter => total_post_counter_table_sql(&database),
        })
    }
}

/// Fully formatted HTTP insert body plus its target table and dedupe token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseInsertPayload {
    /// Target table for this payload.
    pub table: ClickHouseTable,
    /// `ClickHouse` insert format.
    pub format: &'static str,
    /// Newline-delimited insert body.
    pub body: String,
    /// Number of rows in the body.
    pub row_count: usize,
    /// Idempotent batch token from the derive output.
    pub dedupe_token: String,
}

/// Minimal `ClickHouse` HTTP client configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseClientConfig {
    /// Base HTTP endpoint, for example `http://localhost:8123`.
    pub url: Url,
    /// Target database.
    pub database: String,
    /// `ClickHouse` username.
    pub username: String,
    /// `ClickHouse` password.
    pub password: String,
    /// User-Agent/application marker visible in `ClickHouse` logs.
    pub application: String,
    /// Per-request timeout applied to inserts.
    pub request_timeout: Duration,
    /// Connect timeout for clients built from this config.
    pub connect_timeout: Duration,
    /// Initial retry delay for retryable insert failures.
    pub retry_initial_backoff: Duration,
    /// Maximum retry delay for retryable insert failures.
    pub retry_max_backoff: Duration,
    /// Maximum insert attempts, including the first try.
    pub max_insert_attempts: u8,
}

impl ClickHouseClientConfig {
    /// Build a client config after validating the endpoint URL and database identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ClickHouseSchemaError`] if the URL cannot be parsed or the database name is not
    /// a valid `ClickHouse` identifier.
    pub fn new(
        url: &str,
        database: &str,
        username: impl Into<String>,
        password: impl Into<String>,
        application: impl Into<String>,
    ) -> Result<Self, ClickHouseSchemaError> {
        let parsed_url =
            Url::parse(url).map_err(|error| ClickHouseSchemaError::Url(error.to_string()))?;
        ClickHouseIdentifier::new(database)?;
        Ok(Self {
            url: parsed_url,
            database: database.to_owned(),
            username: username.into(),
            password: password.into(),
            application: application.into(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            retry_initial_backoff: DEFAULT_RETRY_INITIAL_BACKOFF,
            retry_max_backoff: DEFAULT_RETRY_MAX_BACKOFF,
            max_insert_attempts: DEFAULT_MAX_INSERT_ATTEMPTS,
        })
    }

    /// Return a copy with an explicit timeout and retry policy.
    #[must_use]
    pub const fn with_timeout_and_retry_policy(
        mut self,
        request_timeout: Duration,
        connect_timeout: Duration,
        retry_initial_backoff: Duration,
        retry_max_backoff: Duration,
        max_insert_attempts: u8,
    ) -> Self {
        self.request_timeout = request_timeout;
        self.connect_timeout = connect_timeout;
        self.retry_initial_backoff = retry_initial_backoff;
        self.retry_max_backoff = retry_max_backoff;
        self.max_insert_attempts = max_insert_attempts;
        self
    }

    /// Build a `reqwest` client with this config's client-level timeouts.
    ///
    /// # Errors
    ///
    /// Returns [`ClickHouseSchemaError`] if the client cannot be built.
    pub fn http_client(&self) -> Result<Client, ClickHouseSchemaError> {
        Client::builder()
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout)
            .build()
            .map_err(ClickHouseSchemaError::Request)
    }

    /// Build a `reqwest` request for an insert payload without sending it.
    ///
    /// # Errors
    ///
    /// Returns [`ClickHouseSchemaError`] if a header value cannot be represented or request
    /// construction fails.
    pub fn insert_request(
        &self,
        payload: &ClickHouseInsertPayload,
    ) -> Result<Request, ClickHouseSchemaError> {
        let client = reqwest::Client::new();
        self.insert_request_with_client(&client, payload)
    }

    /// Build a `reqwest` request for an insert payload using the caller's client.
    ///
    /// # Errors
    ///
    /// Returns [`ClickHouseSchemaError`] if a header value cannot be represented or request
    /// construction fails.
    pub fn insert_request_with_client(
        &self,
        client: &Client,
        payload: &ClickHouseInsertPayload,
    ) -> Result<Request, ClickHouseSchemaError> {
        let query = format!(
            "INSERT INTO {} FORMAT {}",
            payload.table.name(),
            payload.format
        );
        let mut request = client
            .request(Method::POST, self.url.clone())
            .headers(self.headers()?)
            .query(&[
                ("database", self.database.as_str()),
                ("query", query.as_str()),
                (INSERT_DEDUPLICATE_SETTING, "1"),
                (
                    INSERT_DEDUPLICATION_TOKEN_SETTING,
                    payload.dedupe_token.as_str(),
                ),
                (DATE_TIME_INPUT_FORMAT_SETTING, "best_effort"),
            ])
            .timeout(self.request_timeout)
            .body(payload.body.clone())
            .build()
            .map_err(ClickHouseSchemaError::Request)?;
        request.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-ndjson; charset=utf-8"),
        );
        Ok(request)
    }

    /// Execute a batch of insert payloads in the order they were built.
    ///
    /// # Errors
    ///
    /// Returns [`ClickHouseInsertError`] when a request cannot be built, the HTTP transport fails,
    /// or `ClickHouse` returns a non-2xx status.
    pub async fn execute_insert_payloads(
        &self,
        client: &Client,
        payloads: &[ClickHouseInsertPayload],
    ) -> Result<Vec<ClickHouseInsertReceipt>, ClickHouseInsertError> {
        execute_insert_payloads(client, self, payloads).await
    }

    fn headers(&self) -> Result<HeaderMap, ClickHouseSchemaError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            CLICKHOUSE_USER_HEADER,
            HeaderValue::from_str(&self.username).map_err(ClickHouseSchemaError::Header)?,
        );
        headers.insert(
            CLICKHOUSE_KEY_HEADER,
            HeaderValue::from_str(&self.password).map_err(ClickHouseSchemaError::Header)?,
        );
        headers.insert(
            CLICKHOUSE_DATABASE_HEADER,
            HeaderValue::from_str(&self.database).map_err(ClickHouseSchemaError::Header)?,
        );
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&self.application).map_err(ClickHouseSchemaError::Header)?,
        );
        Ok(headers)
    }
}

/// Schema/request/payload formatting failures before any `ClickHouse` network call is made.
#[derive(Debug, thiserror::Error)]
pub enum ClickHouseSchemaError {
    /// Database identifier failed validation.
    #[error("invalid ClickHouse identifier {value:?}")]
    InvalidIdentifier {
        /// Rejected identifier value.
        value: String,
    },
    /// `JSONEachRow` serialization failed.
    #[error("failed to serialize ClickHouse JSONEachRow payload")]
    Json(#[from] serde_json::Error),
    /// URL parsing failed.
    #[error("invalid ClickHouse URL")]
    Url(String),
    /// Header formatting failed.
    #[error("invalid ClickHouse HTTP header")]
    Header(reqwest::header::InvalidHeaderValue),
    /// Request construction failed.
    #[error("failed to build ClickHouse HTTP request")]
    Request(reqwest::Error),
}

/// Insert metadata retained on success and failure so retries stay idempotent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseInsertContext {
    /// Target table for the attempted insert.
    pub table: ClickHouseTable,
    /// Number of rows attempted.
    pub row_count: usize,
    /// Idempotent batch token sent as `insert_deduplication_token`.
    pub dedupe_token: String,
    /// Whether the request enabled `insert_deduplicate`.
    pub insert_deduplicate: bool,
}

impl ClickHouseInsertContext {
    pub(super) fn from_payload(payload: &ClickHouseInsertPayload) -> Self {
        Self {
            table: payload.table,
            row_count: payload.row_count,
            dedupe_token: payload.dedupe_token.clone(),
            insert_deduplicate: true,
        }
    }
}

/// Successful `ClickHouse` insert receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseInsertReceipt {
    /// Insert metadata needed for replay/debugging.
    pub context: ClickHouseInsertContext,
    /// HTTP status returned by `ClickHouse`.
    pub status: u16,
    /// Capped response body snippet, if `ClickHouse` returned one.
    pub response_snippet: Option<String>,
}

/// Runtime failures from executing `ClickHouse` insert payloads.
#[derive(Debug, thiserror::Error)]
pub enum ClickHouseInsertError {
    /// Request construction failed before the HTTP call.
    #[error("failed to build ClickHouse insert request for {context:?}")]
    RequestBuild {
        /// Insert metadata needed for replay/debugging.
        context: ClickHouseInsertContext,
        /// Underlying request-build failure.
        #[source]
        source: ClickHouseSchemaError,
    },
    /// Transport failed while sending a request or reading a response body.
    #[error("ClickHouse insert transport failed for {context:?}")]
    Transport {
        /// Insert metadata needed for replay/debugging.
        context: ClickHouseInsertContext,
        /// Underlying transport failure.
        #[source]
        source: reqwest::Error,
    },
    /// `ClickHouse` returned a retryable non-2xx status.
    #[error("retryable ClickHouse insert status {status} for {context:?}: {response_snippet:?}")]
    RetryableStatus {
        /// Insert metadata needed for replay/debugging.
        context: ClickHouseInsertContext,
        /// HTTP status returned by `ClickHouse`.
        status: u16,
        /// Capped response body snippet.
        response_snippet: Option<String>,
    },
    /// `ClickHouse` returned a permanent non-2xx status.
    #[error("permanent ClickHouse insert status {status} for {context:?}: {response_snippet:?}")]
    PermanentStatus {
        /// Insert metadata needed for replay/debugging.
        context: ClickHouseInsertContext,
        /// HTTP status returned by `ClickHouse`.
        status: u16,
        /// Capped response body snippet.
        response_snippet: Option<String>,
    },
}

/// Return all v2 derive `ClickHouse` table definitions as executable SQL statements.
///
/// # Errors
///
/// Returns [`ClickHouseSchemaError`] if the database name is not a valid `ClickHouse` identifier.
pub fn create_schema_sql(database: &str) -> Result<String, ClickHouseSchemaError> {
    let statements = [
        ClickHouseTable::EmojiServing.create_table_sql(database)?,
        ClickHouseTable::TotalPostCounter.create_table_sql(database)?,
    ];
    Ok(statements.join("\n\n"))
}

/// Format the `ClickHouse` payloads needed to load one derive batch.
///
/// The total-post counter payload is always emitted. The emoji payload is omitted when the batch
/// contains no emoji rows.
///
/// # Errors
///
/// Returns [`ClickHouseSchemaError`] if `JSONEachRow` serialization fails.
pub fn derive_insert_payloads(
    batch: &ClickHouseDeriveBatch,
) -> Result<Vec<ClickHouseInsertPayload>, ClickHouseSchemaError> {
    let mut payloads = Vec::with_capacity(2);
    if !batch.emoji_rows.is_empty() {
        payloads.push(emoji_serving_insert_payload(batch)?);
    }
    payloads.push(total_post_counter_insert_payload(batch)?);
    Ok(payloads)
}

/// Format emoji serving rows as a `ClickHouse` `JSONEachRow` insert payload.
///
/// # Errors
///
/// Returns [`ClickHouseSchemaError`] if `JSONEachRow` serialization fails.
pub fn emoji_serving_insert_payload(
    batch: &ClickHouseDeriveBatch,
) -> Result<ClickHouseInsertPayload, ClickHouseSchemaError> {
    emoji_serving_rows_insert_payload(
        &batch.manifest_identity,
        &batch.emoji_rows,
        batch.dedupe_token.clone(),
    )
}

/// Format a bounded chunk of emoji serving rows as a `ClickHouse` `JSONEachRow` insert payload.
///
/// # Errors
///
/// Returns [`ClickHouseSchemaError`] if `JSONEachRow` serialization fails.
pub fn emoji_serving_rows_insert_payload(
    identity: &DeriveManifestIdentity,
    emoji_rows: &[EmojiProjectionRow],
    dedupe_token: String,
) -> Result<ClickHouseInsertPayload, ClickHouseSchemaError> {
    let rows = emoji_rows
        .iter()
        .map(|row| EmojiServingInsertRow::from_projection(row, identity))
        .collect::<Vec<_>>();
    Ok(ClickHouseInsertPayload {
        table: ClickHouseTable::EmojiServing,
        format: JSON_EACH_ROW_FORMAT,
        body: json_each_row(&rows)?,
        row_count: rows.len(),
        dedupe_token,
    })
}

/// Format the total-post counter as a `ClickHouse` `JSONEachRow` insert payload.
///
/// # Errors
///
/// Returns [`ClickHouseSchemaError`] if `JSONEachRow` serialization fails.
pub fn total_post_counter_insert_payload(
    batch: &ClickHouseDeriveBatch,
) -> Result<ClickHouseInsertPayload, ClickHouseSchemaError> {
    total_post_counter_insert_payload_for_counter(
        &batch.total_post_counter,
        batch.dedupe_token.clone(),
    )
}

/// Format one total-post counter as a `ClickHouse` `JSONEachRow` insert payload.
///
/// # Errors
///
/// Returns [`ClickHouseSchemaError`] if `JSONEachRow` serialization fails.
pub fn total_post_counter_insert_payload_for_counter(
    counter: &TotalPostCounterInput,
    dedupe_token: String,
) -> Result<ClickHouseInsertPayload, ClickHouseSchemaError> {
    let row = TotalPostCounterInsertRow::from_counter(counter);
    Ok(ClickHouseInsertPayload {
        table: ClickHouseTable::TotalPostCounter,
        format: JSON_EACH_ROW_FORMAT,
        body: json_each_row(&[row])?,
        row_count: 1,
        dedupe_token,
    })
}

fn emoji_serving_table_sql(database: &ClickHouseIdentifier) -> String {
    format!(
        r"CREATE TABLE IF NOT EXISTS {database}.v2_emoji_serving (
  src LowCardinality(String),
  run_id LowCardinality(String),
  shard LowCardinality(String),
  file_sequence UInt64,
  receipt_hash String CODEC(ZSTD(1)),
  did String CODEC(ZSTD(1)),
  rkey String CODEC(ZSTD(1)),
  created_at Nullable(DateTime64(6, 'UTC')) CODEC(Delta(8), ZSTD(1)),
  emoji LowCardinality(String),
  occurrences UInt64,
  langs Array(LowCardinality(String)),
  inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(coalesce(created_at, toDateTime64('1970-01-01 00:00:00', 6, 'UTC')))
ORDER BY (src, did, rkey, emoji)
SETTINGS non_replicated_deduplication_window = 10000;"
    )
}

fn total_post_counter_table_sql(database: &ClickHouseIdentifier) -> String {
    format!(
        r"CREATE TABLE IF NOT EXISTS {database}.v2_total_post_counters (
  src LowCardinality(String),
  run_id LowCardinality(String),
  shard LowCardinality(String),
  file_sequence UInt64,
  receipt_hash String CODEC(ZSTD(1)),
  posts_processed UInt64,
  posts_with_emojis UInt64,
  emoji_occurrences UInt64,
  min_created_at Nullable(DateTime64(6, 'UTC')) CODEC(Delta(8), ZSTD(1)),
  max_created_at Nullable(DateTime64(6, 'UTC')) CODEC(Delta(8), ZSTD(1)),
  inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (src, receipt_hash)
SETTINGS non_replicated_deduplication_window = 10000;"
    )
}

fn json_each_row<T: Serialize>(rows: &[T]) -> Result<String, serde_json::Error> {
    let mut output = String::new();
    for row in rows {
        output.push_str(&serde_json::to_string(row)?);
        output.push('\n');
    }
    Ok(output)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClickHouseIdentifier(String);

impl ClickHouseIdentifier {
    fn new(value: &str) -> Result<Self, ClickHouseSchemaError> {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct EmojiServingInsertRow<'a> {
    src: &'static str,
    run_id: &'a str,
    shard: &'a str,
    file_sequence: u64,
    receipt_hash: &'a str,
    did: &'a str,
    rkey: &'a str,
    created_at: Option<&'a str>,
    emoji: &'a str,
    occurrences: u64,
    langs: &'a [String],
}

impl<'a> EmojiServingInsertRow<'a> {
    fn from_projection(row: &'a EmojiProjectionRow, identity: &'a DeriveManifestIdentity) -> Self {
        Self {
            src: BACKFILL_DERIVE_SOURCE,
            run_id: identity.run_id.as_str(),
            shard: identity.shard.as_str(),
            file_sequence: identity.file_sequence,
            receipt_hash: identity.receipt_hash.as_str(),
            did: row.did.as_str(),
            rkey: row.rkey.as_str(),
            created_at: row.created_at_normalized.as_deref(),
            emoji: row.emoji.as_str(),
            occurrences: row.occurrences,
            langs: row.langs.as_slice(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct TotalPostCounterInsertRow<'a> {
    src: &'a str,
    run_id: &'a str,
    shard: &'a str,
    file_sequence: u64,
    receipt_hash: &'a str,
    posts_processed: u64,
    posts_with_emojis: u64,
    emoji_occurrences: u64,
    min_created_at: Option<&'a str>,
    max_created_at: Option<&'a str>,
}

impl<'a> TotalPostCounterInsertRow<'a> {
    fn from_counter(counter: &'a TotalPostCounterInput) -> Self {
        Self {
            src: counter.source.as_str(),
            run_id: counter.run_id.as_str(),
            shard: counter.shard.as_str(),
            file_sequence: counter.file_sequence,
            receipt_hash: counter.receipt_hash.as_str(),
            posts_processed: counter.posts_processed,
            posts_with_emojis: counter.posts_with_emojis,
            emoji_occurrences: counter.emoji_occurrences,
            min_created_at: counter.min_created_at_normalized.as_deref(),
            max_created_at: counter.max_created_at_normalized.as_deref(),
        }
    }
}

#[cfg(test)]
mod tests;
