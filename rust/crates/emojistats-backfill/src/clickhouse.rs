//! `ClickHouse` schema and insert-format skeleton for the v2 derive lane.

use std::{cmp, time::Duration};

use reqwest::{
    Client, Method, Request, Response, StatusCode, Url,
    header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT},
};
use serde::Serialize;

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
    fn from_payload(payload: &ClickHouseInsertPayload) -> Self {
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

/// Execute insert payloads in order through the provided HTTP client.
///
/// # Errors
///
/// Returns [`ClickHouseInsertError`] when a request cannot be built, the HTTP transport fails, or
/// `ClickHouse` returns a non-2xx status.
pub async fn execute_insert_payloads(
    client: &Client,
    config: &ClickHouseClientConfig,
    payloads: &[ClickHouseInsertPayload],
) -> Result<Vec<ClickHouseInsertReceipt>, ClickHouseInsertError> {
    let mut receipts = Vec::with_capacity(payloads.len());

    for payload in payloads {
        receipts.push(execute_insert_payload_with_retries(client, config, payload).await?);
    }

    Ok(receipts)
}

async fn execute_insert_payload_with_retries(
    client: &Client,
    config: &ClickHouseClientConfig,
    payload: &ClickHouseInsertPayload,
) -> Result<ClickHouseInsertReceipt, ClickHouseInsertError> {
    let context = ClickHouseInsertContext::from_payload(payload);
    let mut attempt = 1_u8;
    let mut backoff = config.retry_initial_backoff;
    let max_attempts = cmp::max(1, config.max_insert_attempts);

    loop {
        match execute_insert_payload_once(client, config, payload, &context).await {
            Ok(receipt) => return Ok(receipt),
            Err(error) if should_retry_insert_error(&error) && attempt < max_attempts => {
                tokio::time::sleep(backoff).await;
                attempt = attempt.checked_add(1).unwrap_or(max_attempts);
                backoff = cmp::min(backoff.saturating_mul(2), config.retry_max_backoff);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn execute_insert_payload_once(
    client: &Client,
    config: &ClickHouseClientConfig,
    payload: &ClickHouseInsertPayload,
    context: &ClickHouseInsertContext,
) -> Result<ClickHouseInsertReceipt, ClickHouseInsertError> {
    let request = config
        .insert_request_with_client(client, payload)
        .map_err(|source| ClickHouseInsertError::RequestBuild {
            context: context.clone(),
            source,
        })?;
    let response =
        client
            .execute(request)
            .await
            .map_err(|source| ClickHouseInsertError::Transport {
                context: context.clone(),
                source,
            })?;
    let status = response.status();
    let response_snippet =
        response_snippet(response)
            .await
            .map_err(|source| ClickHouseInsertError::Transport {
                context: context.clone(),
                source,
            })?;

    if !status.is_success() {
        return Err(classify_insert_status(
            context.clone(),
            status,
            response_snippet,
        ));
    }

    Ok(ClickHouseInsertReceipt {
        context: context.clone(),
        status: status.as_u16(),
        response_snippet,
    })
}

fn should_retry_insert_error(error: &ClickHouseInsertError) -> bool {
    match error {
        ClickHouseInsertError::Transport { source, .. } => {
            source.is_timeout() || source.is_connect() || source.is_body()
        }
        ClickHouseInsertError::RetryableStatus { .. } => true,
        ClickHouseInsertError::RequestBuild { .. }
        | ClickHouseInsertError::PermanentStatus { .. } => false,
    }
}

async fn response_snippet(mut response: Response) -> Result<Option<String>, reqwest::Error> {
    let mut bytes = Vec::with_capacity(CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES);
    while bytes.len() < CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES {
        let Some(chunk) = response.chunk().await? else {
            break;
        };
        let remaining = CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES.saturating_sub(bytes.len());
        let take = cmp::min(remaining, chunk.len());
        bytes.extend(chunk.iter().take(take).copied());
    }

    let snippet = String::from_utf8_lossy(&bytes).into_owned();
    if snippet.is_empty() {
        Ok(None)
    } else {
        Ok(Some(snippet))
    }
}

fn classify_insert_status(
    context: ClickHouseInsertContext,
    status: StatusCode,
    response_snippet: Option<String>,
) -> ClickHouseInsertError {
    if is_retryable_insert_status(status) {
        ClickHouseInsertError::RetryableStatus {
            context,
            status: status.as_u16(),
            response_snippet,
        }
    } else {
        ClickHouseInsertError::PermanentStatus {
            context,
            status: status.as_u16(),
            response_snippet,
        }
    }
}

fn is_retryable_insert_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::CONFLICT
        || status == StatusCode::TOO_EARLY
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
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
mod tests {
    #![allow(clippy::arithmetic_side_effects)]

    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        thread,
        time::Duration,
    };

    use reqwest::{Client, StatusCode};
    use serde_json::Value;

    use super::{
        CLICKHOUSE_DATABASE_HEADER, CLICKHOUSE_KEY_HEADER, CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES,
        CLICKHOUSE_USER_HEADER, ClickHouseClientConfig, ClickHouseInsertContext,
        ClickHouseInsertError, ClickHouseSchemaError, ClickHouseTable, JSON_EACH_ROW_FORMAT,
        classify_insert_status, create_schema_sql, derive_insert_payloads, execute_insert_payloads,
    };
    use crate::{
        archive::EmojiProjectionRow,
        derive::{ClickHouseDeriveBatch, DeriveManifestIdentity, TotalPostCounterInput},
    };

    fn batch() -> ClickHouseDeriveBatch {
        ClickHouseDeriveBatch {
            manifest_identity: DeriveManifestIdentity {
                run_id: "run-1".to_owned(),
                shard: "shard0".to_owned(),
                file_sequence: 42,
                dataset: "raw_archive_posts".to_owned(),
                content_hash: "content-hash".to_owned(),
                receipt_hash: "receipt-hash".to_owned(),
                schema_version: 1,
            },
            dedupe_token: "derive:test-token".to_owned(),
            emoji_rows: vec![
                EmojiProjectionRow {
                    did: "did:plc:abc".to_owned(),
                    rkey: "3kxyz".to_owned(),
                    created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
                    emoji: "✅".to_owned(),
                    occurrences: 2,
                    langs: vec!["en".to_owned(), "ja".to_owned()],
                },
                EmojiProjectionRow {
                    did: "did:plc:def".to_owned(),
                    rkey: "3kxyy".to_owned(),
                    created_at_normalized: None,
                    emoji: "🔥".to_owned(),
                    occurrences: 1,
                    langs: Vec::new(),
                },
            ],
            total_post_counter: TotalPostCounterInput {
                source: "backfill-v2-derive".to_owned(),
                run_id: "run-1".to_owned(),
                shard: "shard0".to_owned(),
                file_sequence: 42,
                receipt_hash: "receipt-hash".to_owned(),
                posts_processed: 3,
                posts_with_emojis: 2,
                emoji_occurrences: 3,
                min_created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
                max_created_at_normalized: None,
            },
        }
    }

    fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
        value.get(name).expect("field should exist")
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RecordedRequest {
        target: String,
        body: String,
    }

    fn spawn_http_server(
        responses: Vec<(u16, String)>,
    ) -> (String, thread::JoinHandle<Vec<RecordedRequest>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind test server");
        let addr = listener.local_addr().expect("server addr");
        let handle = thread::spawn(move || {
            responses
                .into_iter()
                .map(|(status, body)| {
                    let (mut stream, _addr) = listener.accept().expect("accept request");
                    let request = read_http_request(&mut stream);
                    write_http_response(&mut stream, status, &body);
                    request
                })
                .collect::<Vec<_>>()
        });

        (format!("http://{addr}"), handle)
    }

    fn read_http_request(stream: &mut TcpStream) -> RecordedRequest {
        let mut headers = Vec::new();
        let mut byte = [0_u8; 1];
        loop {
            let read = stream.read(&mut byte).expect("read request headers");
            if read == 0 {
                break;
            }
            headers.push(byte[0]);
            if headers.ends_with(b"\r\n\r\n") {
                break;
            }
        }

        let header_text = String::from_utf8(headers).expect("utf8 headers");
        let content_length = header_text
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().expect("content length"))
            })
            .unwrap_or(0);
        let mut body = vec![0_u8; content_length];
        stream.read_exact(&mut body).expect("read request body");
        let request_line = header_text.lines().next().expect("request line").to_owned();
        let target = request_line
            .split_whitespace()
            .nth(1)
            .expect("request target")
            .to_owned();

        RecordedRequest {
            target,
            body: String::from_utf8(body).expect("utf8 body"),
        }
    }

    fn write_http_response(stream: &mut TcpStream, status: u16, body: &str) {
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            503 => "Service Unavailable",
            _ => "Status",
        };
        write!(
            stream,
            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .expect("write response");
    }

    #[test]
    fn schema_sql_contains_typed_table_names_and_engines() {
        let sql = create_schema_sql("emojistats").expect("schema sql");

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS emojistats.v2_emoji_serving"));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS emojistats.v2_total_post_counters"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree(inserted_at)"));
        assert!(sql.contains("non_replicated_deduplication_window = 10000"));
        assert!(sql.contains("LowCardinality(String)"));
        assert!(sql.contains("ORDER BY (src, did, rkey, emoji)"));
        assert!(sql.contains("ORDER BY (src, receipt_hash)"));
    }

    #[test]
    fn schema_sql_rejects_untrusted_database_identifiers() {
        let error = create_schema_sql("emojistats; DROP TABLE posts").expect_err("bad database");

        assert!(matches!(
            error,
            ClickHouseSchemaError::InvalidIdentifier { .. }
        ));
    }

    #[test]
    fn json_each_row_payloads_include_derive_rows_and_total_counter() {
        let payloads = derive_insert_payloads(&batch()).expect("payloads");

        assert_eq!(payloads.len(), 2);
        let emoji_payload = payloads.first().expect("emoji payload should exist");
        assert_eq!(emoji_payload.table, ClickHouseTable::EmojiServing);
        assert_eq!(emoji_payload.format, JSON_EACH_ROW_FORMAT);
        assert_eq!(emoji_payload.row_count, 2);
        assert!(emoji_payload.body.ends_with('\n'));
        let emoji_lines = emoji_payload.body.lines().collect::<Vec<_>>();
        assert_eq!(emoji_lines.len(), 2);
        let first_line = emoji_lines.first().expect("first emoji line should exist");
        let first: Value = serde_json::from_str(first_line).expect("first emoji row json");
        assert_eq!(field(&first, "src"), "backfill-v2-derive");
        assert_eq!(field(&first, "did"), "did:plc:abc");
        assert_eq!(field(&first, "emoji"), "✅");
        assert_eq!(field(&first, "occurrences"), 2);
        let langs = field(&first, "langs")
            .as_array()
            .expect("langs should be an array");
        assert_eq!(langs.get(1), Some(&Value::String("ja".to_owned())));

        let counter_payload = payloads.get(1).expect("counter payload should exist");
        assert_eq!(counter_payload.table, ClickHouseTable::TotalPostCounter);
        assert_eq!(counter_payload.row_count, 1);
        let counter_line = counter_payload
            .body
            .lines()
            .next()
            .expect("counter row should exist");
        let counter: Value = serde_json::from_str(counter_line).expect("counter row json");
        assert_eq!(field(&counter, "posts_processed"), 3);
        assert_eq!(field(&counter, "posts_with_emojis"), 2);
        assert_eq!(field(&counter, "max_created_at"), &Value::Null);
    }

    #[test]
    fn request_builder_includes_auth_headers_and_dedupe_settings() {
        let payload = derive_insert_payloads(&batch())
            .expect("payloads")
            .remove(0);
        let config = ClickHouseClientConfig::new(
            "http://localhost:8123",
            "emojistats",
            "alice",
            "secret",
            "emojistats-backfill-test",
        )
        .expect("client config");

        let request = config.insert_request(&payload).expect("request");
        let url = request.url().as_str();

        assert_eq!(request.method(), "POST");
        assert_eq!(request.timeout(), Some(&Duration::from_secs(30)));
        assert!(url.contains("database=emojistats"));
        assert!(url.contains("query=INSERT"));
        assert!(url.contains("insert_deduplicate=1"));
        assert!(url.contains("insert_deduplication_token=derive%3Atest-token"));
        assert!(url.contains("date_time_input_format=best_effort"));
        assert_eq!(
            request.headers().get(&CLICKHOUSE_USER_HEADER),
            Some(&"alice".parse().expect("header value"))
        );
        assert_eq!(
            request.headers().get(&CLICKHOUSE_KEY_HEADER),
            Some(&"secret".parse().expect("header value"))
        );
        assert_eq!(
            request.headers().get(&CLICKHOUSE_DATABASE_HEADER),
            Some(&"emojistats".parse().expect("header value"))
        );
    }

    #[tokio::test]
    async fn execute_insert_payloads_sends_payloads_in_order() {
        let payloads = derive_insert_payloads(&batch()).expect("payloads");
        let (url, handle) = spawn_http_server(vec![
            (200, "emoji-ok".to_owned()),
            (200, "counter-ok".to_owned()),
        ]);
        let config = ClickHouseClientConfig::new(
            &url,
            "emojistats",
            "alice",
            "secret",
            "emojistats-backfill-test",
        )
        .expect("client config")
        .with_timeout_and_retry_policy(
            Duration::from_secs(30),
            Duration::from_secs(10),
            Duration::ZERO,
            Duration::ZERO,
            1,
        );
        let client = Client::new();

        let receipts = execute_insert_payloads(&client, &config, &payloads)
            .await
            .expect("insert payloads");
        let requests = handle.join().expect("server thread");

        assert_eq!(receipts.len(), 2);
        assert_eq!(
            receipts.first().expect("first receipt").context.table,
            ClickHouseTable::EmojiServing
        );
        assert_eq!(
            receipts.get(1).expect("second receipt").context.table,
            ClickHouseTable::TotalPostCounter
        );
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .first()
                .expect("first request")
                .target
                .contains("v2_emoji_serving")
        );
        assert!(
            requests
                .get(1)
                .expect("second request")
                .target
                .contains("v2_total_post_counters")
        );
        assert!(
            requests
                .first()
                .expect("first request")
                .target
                .contains("insert_deduplication_token=derive%3Atest-token")
        );
        assert_eq!(
            requests.first().expect("first request").body,
            payloads.first().expect("first payload").body
        );
        assert_eq!(
            requests.get(1).expect("second request").body,
            payloads.get(1).expect("second payload").body
        );
    }

    #[tokio::test]
    async fn execute_insert_payloads_classifies_retryable_status_with_snippet() {
        let payload = derive_insert_payloads(&batch())
            .expect("payloads")
            .remove(0);
        let response_body = format!(
            "too many parts {}",
            "x".repeat(CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES + 20)
        );
        let (url, handle) = spawn_http_server(vec![(503, response_body)]);
        let config = ClickHouseClientConfig::new(
            &url,
            "emojistats",
            "alice",
            "secret",
            "emojistats-backfill-test",
        )
        .expect("client config")
        .with_timeout_and_retry_policy(
            Duration::from_secs(30),
            Duration::from_secs(10),
            Duration::ZERO,
            Duration::ZERO,
            1,
        );
        let client = Client::new();

        let error = execute_insert_payloads(&client, &config, &[payload])
            .await
            .expect_err("retryable status");
        let requests = handle.join().expect("server thread");

        assert_eq!(requests.len(), 1);
        match error {
            ClickHouseInsertError::RetryableStatus {
                context,
                status,
                response_snippet,
            } => {
                assert_eq!(status, 503);
                assert_eq!(context.table, ClickHouseTable::EmojiServing);
                assert_eq!(context.dedupe_token, "derive:test-token");
                assert!(context.insert_deduplicate);
                assert_eq!(
                    response_snippet.expect("response snippet").len(),
                    CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_insert_payloads_retries_bounded_retryable_statuses() {
        let payload = derive_insert_payloads(&batch())
            .expect("payloads")
            .remove(0);
        let (url, handle) =
            spawn_http_server(vec![(503, "busy".to_owned()), (200, "emoji-ok".to_owned())]);
        let config = ClickHouseClientConfig::new(
            &url,
            "emojistats",
            "alice",
            "secret",
            "emojistats-backfill-test",
        )
        .expect("client config")
        .with_timeout_and_retry_policy(
            Duration::from_secs(30),
            Duration::from_secs(10),
            Duration::ZERO,
            Duration::ZERO,
            2,
        );
        let client = Client::new();

        let receipts = execute_insert_payloads(&client, &config, &[payload])
            .await
            .expect("retry should succeed");
        let requests = handle.join().expect("server thread");

        assert_eq!(receipts.len(), 1);
        assert_eq!(requests.len(), 2);
    }

    #[test]
    fn classify_insert_status_marks_client_errors_permanent() {
        let context = ClickHouseInsertContext {
            table: ClickHouseTable::TotalPostCounter,
            row_count: 1,
            dedupe_token: "derive:test-token".to_owned(),
            insert_deduplicate: true,
        };

        let error =
            classify_insert_status(context, StatusCode::BAD_REQUEST, Some("syntax".to_owned()));

        match error {
            ClickHouseInsertError::PermanentStatus {
                context,
                status,
                response_snippet,
            } => {
                assert_eq!(status, 400);
                assert_eq!(context.table, ClickHouseTable::TotalPostCounter);
                assert_eq!(response_snippet.as_deref(), Some("syntax"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn empty_emoji_batches_only_emit_total_counter_payload() {
        let mut batch = batch();
        batch.emoji_rows.clear();

        let payloads = derive_insert_payloads(&batch).expect("payloads");

        assert_eq!(payloads.len(), 1);
        let payload = payloads.first().expect("counter payload should exist");
        assert_eq!(payload.table, ClickHouseTable::TotalPostCounter);
    }
}
