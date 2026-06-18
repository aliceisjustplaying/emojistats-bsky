//! `ClickHouse` schema and insert-format skeleton for the v2 derive lane.

use std::time::Duration;

use reqwest::{
    Client, Method, Request, Url,
    header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT},
};
use serde::Serialize;

mod execute;
mod schema;

#[cfg(test)]
use execute::{classify_insert_status, classify_sql_status};
pub use execute::{execute_insert_payloads, execute_sql_statements};
use schema::ClickHouseIdentifier;
pub use schema::{
    ClickHouseTable, aggregate_rebuild_sql, aggregate_rebuild_statements, create_schema_sql,
};

use crate::derive::{
    BACKFILL_DERIVE_SOURCE, ClickHouseDeriveBatch, DeriveCheckpointKey, DeriveManifestIdentity,
    PostServingRow, TotalPostCounterInput,
};

/// `ClickHouse` HTTP insert format used by the derive lane.
pub const JSON_EACH_ROW_FORMAT: &str = "JSONEachRow";

const INSERT_DEDUPLICATE_SETTING: &str = "insert_deduplicate";
const INSERT_DEDUPLICATION_TOKEN_SETTING: &str = "insert_deduplication_token";
const DATE_TIME_INPUT_FORMAT_SETTING: &str = "date_time_input_format";
const CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES: usize = 4_096;
pub const DEFAULT_POST_SERVING_PAYLOAD_MAX_BYTES: usize = 8_388_608;
const POST_SERVING_ROW_SIZE_DEDUPE_TOKEN: &str =
    "derive:post:0000000000000000000000000000000000000000000000000000000000000000";
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const DEFAULT_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(2);
const DEFAULT_MAX_INSERT_ATTEMPTS: u8 = 3;
const CLICKHOUSE_USER_HEADER: HeaderName = HeaderName::from_static("x-clickhouse-user");
const CLICKHOUSE_KEY_HEADER: HeaderName = HeaderName::from_static("x-clickhouse-key");
const CLICKHOUSE_DATABASE_HEADER: HeaderName = HeaderName::from_static("x-clickhouse-database");

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
    /// Durable derive checkpoint key for this insert payload.
    pub checkpoint_key: DeriveCheckpointKey,
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
    /// Client-level request timeout applied to inserts.
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
        let client = self.http_client()?;
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

    /// Build a `ClickHouse` SQL request using the caller's client.
    ///
    /// # Errors
    ///
    /// Returns [`ClickHouseSchemaError`] if a header value cannot be represented or request
    /// construction fails.
    pub fn sql_request_with_client(
        &self,
        client: &Client,
        query: &str,
    ) -> Result<Request, ClickHouseSchemaError> {
        let mut request = client
            .request(Method::POST, self.url.clone())
            .headers(self.headers()?)
            .query(&[
                ("database", self.database.as_str()),
                ("query", query),
                (DATE_TIME_INPUT_FORMAT_SETTING, "best_effort"),
            ])
            .timeout(self.request_timeout)
            .body(String::new())
            .build()
            .map_err(ClickHouseSchemaError::Request)?;
        request
            .headers_mut()
            .insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
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

    /// Execute SQL statements in order through the configured `ClickHouse` client.
    ///
    /// # Errors
    ///
    /// Returns [`ClickHouseSqlError`] when request construction, transport, or `ClickHouse`
    /// execution fails.
    pub async fn execute_sql_statements(
        &self,
        client: &Client,
        statements: &[String],
    ) -> Result<Vec<ClickHouseSqlReceipt>, ClickHouseSqlError> {
        execute_sql_statements(client, self, statements).await
    }

    /// Return whether the target table already contains the rows for this derive payload.
    ///
    /// # Errors
    ///
    /// Returns [`ClickHouseSqlError`] when the probe request fails or returns an invalid count.
    pub async fn derive_payload_exists(
        &self,
        client: &Client,
        payload: &ClickHouseInsertPayload,
    ) -> Result<bool, ClickHouseSqlError> {
        derive_payload_exists(client, self, payload).await
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
    /// Resource counter overflowed before an insert request was built.
    #[error("ClickHouse resource counter overflow: {field}")]
    CountOverflow {
        /// Counter that overflowed.
        field: &'static str,
    },
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

/// SQL statement metadata retained on success and failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseSqlContext {
    /// Zero-based statement position in the executed command.
    pub statement_index: usize,
    /// SQL statement sent to `ClickHouse`.
    pub statement: String,
}

/// Successful `ClickHouse` SQL execution receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseSqlReceipt {
    /// Statement metadata needed for replay/debugging.
    pub context: ClickHouseSqlContext,
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

/// Runtime failures from executing `ClickHouse` SQL statements.
#[derive(Debug, thiserror::Error)]
pub enum ClickHouseSqlError {
    /// Request construction failed before the HTTP call.
    #[error("failed to build ClickHouse SQL request for statement {statement_index}")]
    RequestBuild {
        /// Statement position in the executed command.
        statement_index: usize,
        /// Underlying request-build failure.
        #[source]
        source: ClickHouseSchemaError,
    },
    /// Transport failed while sending a request or reading a response body.
    #[error("ClickHouse SQL transport failed for {context:?}")]
    Transport {
        /// Statement metadata needed for replay/debugging.
        context: ClickHouseSqlContext,
        /// Underlying transport failure.
        #[source]
        source: reqwest::Error,
    },
    /// `ClickHouse` returned a retryable non-2xx status.
    #[error("retryable ClickHouse SQL status {status} for {context:?}: {response_snippet:?}")]
    RetryableStatus {
        /// Statement metadata needed for replay/debugging.
        context: ClickHouseSqlContext,
        /// HTTP status returned by `ClickHouse`.
        status: u16,
        /// Capped response body snippet.
        response_snippet: Option<String>,
    },
    /// `ClickHouse` returned a permanent non-2xx status.
    #[error("permanent ClickHouse SQL status {status} for {context:?}: {response_snippet:?}")]
    PermanentStatus {
        /// Statement metadata needed for replay/debugging.
        context: ClickHouseSqlContext,
        /// HTTP status returned by `ClickHouse`.
        status: u16,
        /// Capped response body snippet.
        response_snippet: Option<String>,
    },
}

/// Format the `ClickHouse` payloads needed to load one derive batch.
///
/// The total-post counter payload is always emitted. The compact post payload is omitted only when
/// the batch contains no post rows.
///
/// # Errors
///
/// Returns [`ClickHouseSchemaError`] if `JSONEachRow` serialization fails.
pub fn derive_insert_payloads(
    batch: &ClickHouseDeriveBatch,
) -> Result<Vec<ClickHouseInsertPayload>, ClickHouseSchemaError> {
    let mut payloads = Vec::with_capacity(2);
    if !batch.post_rows.is_empty() {
        payloads.push(post_serving_insert_payload(batch)?);
    }
    payloads.push(total_post_counter_insert_payload(batch)?);
    Ok(payloads)
}

/// Format compact post-serving rows as a `ClickHouse` `JSONEachRow` insert payload.
///
/// # Errors
///
/// Returns [`ClickHouseSchemaError`] if `JSONEachRow` serialization fails.
pub fn post_serving_insert_payload(
    batch: &ClickHouseDeriveBatch,
) -> Result<ClickHouseInsertPayload, ClickHouseSchemaError> {
    post_serving_rows_insert_payload(
        &batch.manifest_identity,
        &batch.post_rows,
        batch.dedupe_token.clone(),
        DeriveCheckpointKey::post_serving(&batch.manifest_identity, 0),
    )
}

/// Format a bounded chunk of compact post-serving rows as a `ClickHouse` `JSONEachRow` insert
/// payload.
///
/// # Errors
///
/// Returns [`ClickHouseSchemaError`] if `JSONEachRow` serialization fails.
pub fn post_serving_rows_insert_payload(
    identity: &DeriveManifestIdentity,
    post_rows: &[PostServingRow],
    dedupe_token: String,
    checkpoint_key: DeriveCheckpointKey,
) -> Result<ClickHouseInsertPayload, ClickHouseSchemaError> {
    let rows = post_rows
        .iter()
        .map(|row| PostServingInsertRow::from_projection(row, identity, dedupe_token.as_str()))
        .collect::<Vec<_>>();
    Ok(ClickHouseInsertPayload {
        table: ClickHouseTable::PostServing,
        format: JSON_EACH_ROW_FORMAT,
        body: json_each_row(&rows)?,
        row_count: rows.len(),
        dedupe_token,
        checkpoint_key,
    })
}

/// Return the exact `JSONEachRow` byte cost for one compact post-serving row.
///
/// # Errors
///
/// Returns [`ClickHouseSchemaError`] if `JSONEachRow` serialization fails.
pub fn post_serving_row_body_bytes(
    identity: &DeriveManifestIdentity,
    row: &PostServingRow,
) -> Result<usize, ClickHouseSchemaError> {
    serde_json::to_string(&PostServingInsertRow::from_projection(
        row,
        identity,
        POST_SERVING_ROW_SIZE_DEDUPE_TOKEN,
    ))?
    .len()
    .checked_add(1)
    .ok_or(ClickHouseSchemaError::CountOverflow {
        field: "post_serving_row_body_bytes",
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
        DeriveCheckpointKey::total_post_counter(&batch.manifest_identity),
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
    checkpoint_key: DeriveCheckpointKey,
) -> Result<ClickHouseInsertPayload, ClickHouseSchemaError> {
    let row = TotalPostCounterInsertRow::from_counter(counter, dedupe_token.as_str());
    Ok(ClickHouseInsertPayload {
        table: ClickHouseTable::TotalPostCounter,
        format: JSON_EACH_ROW_FORMAT,
        body: json_each_row(&[row])?,
        row_count: 1,
        dedupe_token,
        checkpoint_key,
    })
}

fn json_each_row<T: Serialize>(rows: &[T]) -> Result<String, serde_json::Error> {
    let mut output = String::new();
    for row in rows {
        output.push_str(&serde_json::to_string(row)?);
        output.push('\n');
    }
    Ok(output)
}

/// Return whether the target table already contains the rows for this derive payload.
///
/// # Errors
///
/// Returns [`ClickHouseSqlError`] when the probe request fails or returns an invalid count.
pub async fn derive_payload_exists(
    client: &Client,
    config: &ClickHouseClientConfig,
    payload: &ClickHouseInsertPayload,
) -> Result<bool, ClickHouseSqlError> {
    let statement = format!(
        "SELECT count() FROM {} FINAL WHERE derive_dedupe_token = '{}' FORMAT TabSeparated",
        payload.table.name(),
        clickhouse_string_literal(payload.dedupe_token.as_str())
    );
    let mut receipts = execute_sql_statements(client, config, &[statement]).await?;
    let receipt = receipts
        .pop()
        .ok_or_else(|| ClickHouseSqlError::PermanentStatus {
            context: ClickHouseSqlContext {
                statement_index: 0,
                statement: "derive payload existence probe".to_owned(),
            },
            status: 200,
            response_snippet: Some("ClickHouse returned no receipt".to_owned()),
        })?;
    let count = receipt
        .response_snippet
        .as_deref()
        .unwrap_or_default()
        .trim()
        .parse::<usize>()
        .map_err(|error| ClickHouseSqlError::PermanentStatus {
            context: receipt.context.clone(),
            status: receipt.status,
            response_snippet: Some(format!("invalid derive payload count: {error}")),
        })?;
    Ok(count >= payload.row_count)
}

fn clickhouse_string_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct PostServingInsertRow<'a> {
    src: &'static str,
    derive_dedupe_token: &'a str,
    run_id: &'a str,
    shard: &'a str,
    file_sequence: u64,
    dataset: &'a str,
    fetch_method: &'a str,
    completeness_class: &'a str,
    receipt_hash: &'a str,
    normalizer_name: &'a str,
    normalizer_semver: &'a str,
    normalizer_git_rev: &'a str,
    normalizer_unicode_version: &'a str,
    normalizer_emoji_data_version: &'a str,
    did: &'a str,
    rkey: &'a str,
    created_at: Option<&'a str>,
    created_at_parse_status: &'a str,
    langs: &'a [String],
    emojis: &'a [String],
    emoji_occurrences: usize,
    observed_at: &'a str,
}

impl<'a> PostServingInsertRow<'a> {
    fn from_projection(
        row: &'a PostServingRow,
        identity: &'a DeriveManifestIdentity,
        derive_dedupe_token: &'a str,
    ) -> Self {
        Self {
            src: BACKFILL_DERIVE_SOURCE,
            derive_dedupe_token,
            run_id: identity.run_id.as_str(),
            shard: identity.shard.as_str(),
            file_sequence: identity.file_sequence,
            dataset: identity.dataset.as_str(),
            fetch_method: identity.fetch_method.as_str(),
            completeness_class: identity.completeness_class.as_str(),
            receipt_hash: identity.receipt_hash.as_str(),
            normalizer_name: identity.normalizer.name.as_str(),
            normalizer_semver: identity.normalizer.semver.as_str(),
            normalizer_git_rev: identity.normalizer.git_rev.as_str(),
            normalizer_unicode_version: identity.normalizer.unicode_version.as_str(),
            normalizer_emoji_data_version: identity.normalizer.emoji_data_version.as_str(),
            did: row.did.as_str(),
            rkey: row.rkey.as_str(),
            created_at: row.created_at_normalized.as_deref(),
            created_at_parse_status: row.created_at_parse_status.as_str(),
            langs: row.langs.as_slice(),
            emojis: row.emojis.as_slice(),
            emoji_occurrences: row.emojis.len(),
            observed_at: identity.observed_at.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct TotalPostCounterInsertRow<'a> {
    src: &'a str,
    derive_dedupe_token: &'a str,
    run_id: &'a str,
    shard: &'a str,
    file_sequence: u64,
    dataset: &'a str,
    fetch_method: &'a str,
    completeness_class: &'a str,
    receipt_hash: &'a str,
    normalizer_name: &'a str,
    normalizer_semver: &'a str,
    normalizer_git_rev: &'a str,
    normalizer_unicode_version: &'a str,
    normalizer_emoji_data_version: &'a str,
    did: &'a str,
    posts_processed: u64,
    posts_with_emojis: u64,
    emoji_occurrences: u64,
    min_created_at: Option<&'a str>,
    max_created_at: Option<&'a str>,
}

impl<'a> TotalPostCounterInsertRow<'a> {
    fn from_counter(counter: &'a TotalPostCounterInput, derive_dedupe_token: &'a str) -> Self {
        Self {
            src: counter.source.as_str(),
            derive_dedupe_token,
            run_id: counter.run_id.as_str(),
            shard: counter.shard.as_str(),
            file_sequence: counter.file_sequence,
            dataset: counter.dataset.as_str(),
            fetch_method: counter.fetch_method.as_str(),
            completeness_class: counter.completeness_class.as_str(),
            receipt_hash: counter.receipt_hash.as_str(),
            normalizer_name: counter.normalizer.name.as_str(),
            normalizer_semver: counter.normalizer.semver.as_str(),
            normalizer_git_rev: counter.normalizer.git_rev.as_str(),
            normalizer_unicode_version: counter.normalizer.unicode_version.as_str(),
            normalizer_emoji_data_version: counter.normalizer.emoji_data_version.as_str(),
            did: counter.did.as_str(),
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
