//! `com.atproto.repo.listRecords` fallback fetch and archive path.

use std::{
    collections::HashSet,
    fmt,
    path::Path,
    time::{Duration, Instant, SystemTime},
};

use futures_util::StreamExt as _;
use jacquard_common::{
    deps::fluent_uri::Uri,
    types::{did::Did, recordkey::Rkey},
};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use tokio::time;

use crate::{
    archive::{
        ArchiveArtifacts, ArchiveCommitContext, ArchiveError, ArchiveStorageConfig,
        CompletenessClass, FetchMethod, RepoReceipt, StreamingArchiveSink, StreamingReceiptInput,
        archive_row_from_post_observed_at,
    },
    parse::PostRecord,
    post_decode,
    scheduler::{HostPacer, SharedHostPacer},
    transport::{AccountState, RateLimitSnapshot},
};

const POST_COLLECTION: &str = "app.bsky.feed.post";
const LIST_RECORDS_XRPC: &str = "com.atproto.repo.listRecords";
const DEFAULT_PAGE_LIMIT: u16 = 100;
const DEFAULT_MAX_PAGES: u64 = 100_000;
const DEFAULT_MAX_RECORDS: u64 = 10_000_000;
const DEFAULT_MAX_DECODE_ERRORS: u64 = 100_000;
const DEFAULT_MAX_PAGE_BYTES: u64 = 8_388_608;
const DEFAULT_CHUNK_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(60);
#[allow(clippy::duration_suboptimal_units)]
const DEFAULT_PAGE_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const DEFAULT_MIN_PROGRESS_BYTES: u64 = 16_384;
const DEFAULT_MIN_PROGRESS_INTERVAL: Duration = Duration::from_secs(60);

/// Pagination and response-size caps for `listRecords`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListRecordsConfig {
    pub page_limit: u16,
    pub max_pages: u64,
    pub max_records: u64,
    pub max_decode_errors: u64,
    pub max_page_bytes: u64,
    pub response_header_timeout: Duration,
    pub chunk_idle_timeout: Duration,
    pub page_download_timeout: Duration,
    pub min_progress_bytes: u64,
    pub min_progress_interval: Duration,
}

impl Default for ListRecordsConfig {
    fn default() -> Self {
        Self {
            page_limit: DEFAULT_PAGE_LIMIT,
            max_pages: DEFAULT_MAX_PAGES,
            max_records: DEFAULT_MAX_RECORDS,
            max_decode_errors: DEFAULT_MAX_DECODE_ERRORS,
            max_page_bytes: DEFAULT_MAX_PAGE_BYTES,
            response_header_timeout: DEFAULT_RESPONSE_HEADER_TIMEOUT,
            chunk_idle_timeout: DEFAULT_CHUNK_IDLE_TIMEOUT,
            page_download_timeout: DEFAULT_PAGE_DOWNLOAD_TIMEOUT,
            min_progress_bytes: DEFAULT_MIN_PROGRESS_BYTES,
            min_progress_interval: DEFAULT_MIN_PROGRESS_INTERVAL,
        }
    }
}

/// One fetched `listRecords` page in the collection-paginated fallback lane.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListRecordsPage {
    #[serde(default)]
    pub records: Vec<ListRecordsRecord>,
    pub cursor: Option<String>,
}

/// One raw `listRecords` record entry.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ListRecordsRecord {
    pub uri: String,
    #[serde(default)]
    pub cid: Option<String>,
    pub value: serde_json::Value,
}

/// Archive output for a collection-paginated repo fetch.
#[derive(Debug)]
pub struct ListRecordsArchiveOutput {
    pub receipt: RepoReceipt,
    pub artifacts: ArchiveArtifacts,
    pub records: u64,
    pub archived_posts: u64,
    pub decode_errors: u64,
    pub archive_ms: u64,
    pub rate_limits: Vec<RateLimitSnapshot>,
}

/// Shared host pacing used before each `listRecords` page request.
#[derive(Debug, Clone, Copy)]
pub struct ListRecordsHostPacing<'a> {
    pub pacer: &'a SharedHostPacer,
    pub host: &'a str,
    pub min_interval: Option<Duration>,
}

impl<'a> ListRecordsHostPacing<'a> {
    #[must_use]
    pub const fn new(
        pacer: &'a SharedHostPacer,
        host: &'a str,
        min_interval: Option<Duration>,
    ) -> Self {
        Self {
            pacer,
            host,
            min_interval,
        }
    }

    async fn reserve_page_request(self) -> Result<(), ListRecordsError> {
        HostPacer::reserve_next_request(self.pacer, self.host, self.min_interval)
            .await
            .map_err(|err| {
                ListRecordsError::PreCommit(format!("host pacing for {}: {err}", self.host))
            })
    }
}

/// `listRecords` transport, protocol, cap, decode, and archive failures.
#[derive(Debug, thiserror::Error)]
pub enum ListRecordsError {
    #[error("account state {state} from listRecords HTTP {status}: {message:?}")]
    AccountState {
        state: AccountState,
        status: u16,
        message: Option<Box<str>>,
        rate_limit: Box<RateLimitSnapshot>,
    },
    #[error("listRecords HTTP status {status}: error={error_code:?}, message={message:?}")]
    HttpStatus {
        status: u16,
        error_code: Option<Box<str>>,
        message: Option<Box<str>>,
        rate_limit: Box<RateLimitSnapshot>,
    },
    #[error("listRecords transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("listRecords response headers did not arrive within {timeout:?}")]
    ResponseHeaderTimeout { timeout: Duration },
    #[error("no listRecords body chunk within {timeout:?}")]
    InactivityTimeout { timeout: Duration },
    #[error("listRecords page download exceeded {timeout:?} after {observed_bytes} bytes")]
    DownloadTimeout {
        timeout: Duration,
        observed_bytes: u64,
    },
    #[error(
        "listRecords page body made {observed_bytes} bytes progress in {interval:?}, below minimum {min_bytes}"
    )]
    ProgressTimeout {
        interval: Duration,
        min_bytes: u64,
        observed_bytes: u64,
    },
    #[error("listRecords page JSON failed to decode: {0}")]
    PageJson(#[source] serde_json::Error),
    #[error("listRecords archive failed: {0}")]
    Archive(#[from] ArchiveError),
    #[error("listRecords resource cap exceeded: {limit} observed {observed}, max {max}")]
    ResourceLimitExceeded {
        limit: &'static str,
        observed: u64,
        max: u64,
    },
    #[error("listRecords protocol error: {0}")]
    Protocol(String),
    #[error("listRecords pre-commit check failed: {0}")]
    PreCommit(String),
}

impl ListRecordsError {
    #[must_use]
    pub const fn rate_limit(&self) -> Option<&RateLimitSnapshot> {
        match self {
            Self::AccountState { rate_limit, .. } | Self::HttpStatus { rate_limit, .. } => {
                Some(rate_limit)
            }
            Self::Transport(_)
            | Self::ResponseHeaderTimeout { .. }
            | Self::InactivityTimeout { .. }
            | Self::DownloadTimeout { .. }
            | Self::ProgressTimeout { .. }
            | Self::PageJson(_)
            | Self::Archive(_)
            | Self::ResourceLimitExceeded { .. }
            | Self::Protocol(_)
            | Self::PreCommit(_) => None,
        }
    }

    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        match self {
            Self::AccountState { .. }
            | Self::PageJson(_)
            | Self::Archive(_)
            | Self::ResourceLimitExceeded { .. }
            | Self::Protocol(_) => false,
            Self::Transport(_)
            | Self::ResponseHeaderTimeout { .. }
            | Self::InactivityTimeout { .. }
            | Self::DownloadTimeout { .. }
            | Self::ProgressTimeout { .. }
            | Self::PreCommit(_) => true,
            Self::HttpStatus { status, .. } => *status >= 500 || *status == 429,
        }
    }
}

/// Fetch all `app.bsky.feed.post` records with `listRecords` and write archive artifacts.
///
/// # Errors
///
/// Returns [`ListRecordsError`] for transport, pagination cap, decode, or archive failures.
pub async fn fetch_and_archive_list_records(
    http: &Client,
    pds: &Uri<String>,
    did: &Did,
    did_str: &str,
    archive_dir: &Path,
    archive_context: ArchiveCommitContext,
    config: ListRecordsConfig,
) -> Result<ListRecordsArchiveOutput, ListRecordsError> {
    fetch_and_archive_list_records_with_rate_limit_observer(
        http,
        pds,
        did,
        did_str,
        archive_dir,
        archive_context,
        ArchiveStorageConfig::Local,
        config,
        |_rate_limit| {},
    )
    .await
}

/// Fetch all `app.bsky.feed.post` records and report each page's rate-limit headers as it arrives.
///
/// # Errors
///
/// Returns [`ListRecordsError`] for transport, pagination cap, decode, or archive failures.
#[allow(clippy::too_many_arguments)]
pub async fn fetch_and_archive_list_records_with_rate_limit_observer(
    http: &Client,
    pds: &Uri<String>,
    did: &Did,
    did_str: &str,
    archive_dir: &Path,
    archive_context: ArchiveCommitContext,
    archive_storage: ArchiveStorageConfig,
    config: ListRecordsConfig,
    observe_rate_limit: impl FnMut(&RateLimitSnapshot),
) -> Result<ListRecordsArchiveOutput, ListRecordsError> {
    fetch_and_archive_list_records_with_precommit_check(
        http,
        pds,
        did,
        did_str,
        archive_dir,
        archive_context,
        archive_storage,
        config,
        None,
        observe_rate_limit,
        || Ok(()),
    )
    .await
}

/// Fetch all post records and run a final check immediately before artifacts are committed.
///
/// # Errors
///
/// Returns [`ListRecordsError`] for transport, pagination cap, decode, pre-commit, or archive failures.
#[allow(clippy::too_many_arguments)]
pub async fn fetch_and_archive_list_records_with_precommit_check(
    http: &Client,
    pds: &Uri<String>,
    did: &Did,
    did_str: &str,
    archive_dir: &Path,
    archive_context: ArchiveCommitContext,
    archive_storage: ArchiveStorageConfig,
    config: ListRecordsConfig,
    host_pacing: Option<ListRecordsHostPacing<'_>>,
    mut observe_rate_limit: impl FnMut(&RateLimitSnapshot),
    before_commit: impl FnOnce() -> Result<(), String>,
) -> Result<ListRecordsArchiveOutput, ListRecordsError> {
    let mut archiver = ListRecordsArchiver::new(
        did_str,
        archive_dir,
        archive_context,
        archive_storage,
        config,
    )?;
    let mut rate_limits = Vec::new();
    let mut seen_cursors = HashSet::new();
    let mut cursor: Option<String> = None;

    loop {
        if let Some(host_pacing) = host_pacing {
            host_pacing.reserve_page_request().await?;
        }
        let fetched = fetch_list_records_page(http, pds, did, cursor.as_deref(), config).await?;
        let observed_at = SystemTime::now();
        observe_rate_limit(&fetched.rate_limit);
        rate_limits.push(fetched.rate_limit);
        let next_cursor = fetched.page.cursor.clone();
        if let Some(next_cursor_value) = next_cursor.as_deref() {
            if Some(next_cursor_value) == cursor.as_deref() {
                return Err(ListRecordsError::Protocol(
                    "PDS returned the same listRecords cursor twice".to_owned(),
                ));
            }
            if !seen_cursors.insert(next_cursor_value.to_owned()) {
                return Err(ListRecordsError::Protocol(
                    "PDS returned a repeated listRecords cursor".to_owned(),
                ));
            }
        }
        archiver.push_page(fetched.page)?;
        let Some(_next_cursor_value) = next_cursor.as_deref() else {
            break;
        };
        if host_pacing.is_none()
            && let Some(delay) = HostPacer::rate_limit_delay(
                rate_limits.last().ok_or_else(|| {
                    ListRecordsError::Protocol("missing rate-limit snapshot".to_owned())
                })?,
                observed_at,
            )
        {
            time::sleep(delay).await;
        }
        cursor = next_cursor;
    }

    let mut output = archiver.finish(before_commit)?;
    output.rate_limits = rate_limits;
    Ok(output)
}

/// Archive already-fetched pages. Used by tests and by the HTTP fetcher.
///
/// # Errors
///
/// Returns [`ListRecordsError`] when caps, record decode, or archive writes fail.
pub fn archive_list_records_pages<I>(
    did_str: &str,
    archive_dir: &Path,
    pages: I,
    config: ListRecordsConfig,
) -> Result<ListRecordsArchiveOutput, ListRecordsError>
where
    I: IntoIterator<Item = ListRecordsPage>,
{
    let mut archiver = ListRecordsArchiver::new(
        did_str,
        archive_dir,
        ArchiveCommitContext::fetch_one_local(),
        ArchiveStorageConfig::Local,
        config,
    )?;

    for page in pages {
        archiver.push_page(page)?;
    }

    archiver.finish(|| Ok(()))
}

struct ListRecordsArchiver<'a> {
    did_str: &'a str,
    sink: StreamingArchiveSink,
    config: ListRecordsConfig,
    records: u64,
    decode_errors: u64,
    pages_seen: u64,
    seen_rkeys: HashSet<String>,
}

impl<'a> ListRecordsArchiver<'a> {
    fn new(
        did_str: &'a str,
        archive_dir: &Path,
        archive_context: ArchiveCommitContext,
        archive_storage: ArchiveStorageConfig,
        config: ListRecordsConfig,
    ) -> Result<Self, ListRecordsError> {
        Ok(Self {
            did_str,
            sink: StreamingArchiveSink::new_with_storage(
                archive_dir,
                did_str,
                archive_context,
                archive_storage,
            )?,
            config,
            records: 0,
            decode_errors: 0,
            pages_seen: 0,
            seen_rkeys: HashSet::new(),
        })
    }

    fn push_page(&mut self, page: ListRecordsPage) -> Result<(), ListRecordsError> {
        self.pages_seen =
            self.pages_seen
                .checked_add(1)
                .ok_or(ListRecordsError::ResourceLimitExceeded {
                    limit: "max_pages",
                    observed: u64::MAX,
                    max: self.config.max_pages,
                })?;
        enforce_cap("max_pages", self.pages_seen, self.config.max_pages)?;
        for record in page.records {
            self.push_record(record)?;
        }
        Ok(())
    }

    fn push_record(&mut self, record: ListRecordsRecord) -> Result<(), ListRecordsError> {
        self.records =
            self.records
                .checked_add(1)
                .ok_or(ListRecordsError::ResourceLimitExceeded {
                    limit: "max_records",
                    observed: u64::MAX,
                    max: self.config.max_records,
                })?;
        enforce_cap("max_records", self.records, self.config.max_records)?;
        let rkey = rkey_from_uri(self.did_str, &record.uri).map(ToOwned::to_owned);
        if let Some(rkey) = &rkey
            && !self.seen_rkeys.insert(rkey.clone())
        {
            return Err(ListRecordsError::Protocol(format!(
                "PDS returned duplicate listRecords rkey {rkey}"
            )));
        }
        match post_record_from_list_record(self.did_str, record) {
            Ok(decoded) => {
                if decoded.typed_decode_failed {
                    self.increment_decode_errors()?;
                }
                let row = archive_row_from_post_observed_at(
                    self.did_str,
                    &decoded.post,
                    &self.sink.normalizer().clone(),
                    self.sink.observed_at(),
                )?;
                self.sink.push_row(row)?;
            }
            Err(_error) => {
                self.increment_decode_errors()?;
            }
        }
        Ok(())
    }

    fn increment_decode_errors(&mut self) -> Result<(), ListRecordsError> {
        self.decode_errors =
            self.decode_errors
                .checked_add(1)
                .ok_or(ListRecordsError::ResourceLimitExceeded {
                    limit: "decode_errors",
                    observed: u64::MAX,
                    max: self.config.max_decode_errors,
                })?;
        enforce_cap(
            "max_decode_errors",
            self.decode_errors,
            self.config.max_decode_errors,
        )?;
        Ok(())
    }

    fn finish(
        self,
        before_commit: impl FnOnce() -> Result<(), String>,
    ) -> Result<ListRecordsArchiveOutput, ListRecordsError> {
        let records = self.records;
        let decode_errors = self.decode_errors;
        let archive_started = Instant::now();
        before_commit().map_err(ListRecordsError::PreCommit)?;
        let (receipt, artifacts) = self.sink.finish(
            StreamingReceiptInput {
                fetch_method: FetchMethod::ListRecords,
                completeness_class: CompletenessClass::CollectionPaginated,
                reachable_records_count: records,
                reachable_post_records_count: records,
                post_decode_error_count: decode_errors,
                profile_row_hash: None,
                mst_root_cid: None,
                commit_cid: None,
            },
            None,
        )?;
        let archive_ms = u64::try_from(archive_started.elapsed().as_millis()).unwrap_or(u64::MAX);

        Ok(ListRecordsArchiveOutput {
            archived_posts: receipt.archived_post_rows_count,
            receipt,
            artifacts,
            records,
            decode_errors,
            archive_ms,
            rate_limits: Vec::new(),
        })
    }
}

#[derive(Debug)]
struct FetchedListRecordsPage {
    page: ListRecordsPage,
    rate_limit: RateLimitSnapshot,
}

async fn fetch_list_records_page(
    http: &Client,
    pds: &Uri<String>,
    did: &Did,
    cursor: Option<&str>,
    config: ListRecordsConfig,
) -> Result<FetchedListRecordsPage, ListRecordsError> {
    let url = format!(
        "{}/xrpc/{LIST_RECORDS_XRPC}",
        pds.as_str().trim_end_matches('/')
    );
    let mut query = vec![
        ("repo", did.as_str().to_owned()),
        ("collection", POST_COLLECTION.to_owned()),
        ("limit", config.page_limit.to_string()),
    ];
    if let Some(cursor) = cursor {
        query.push(("cursor", cursor.to_owned()));
    }

    let response = time::timeout(
        config.response_header_timeout,
        http.get(url).query(&query).send(),
    )
    .await
    .map_err(|_elapsed| ListRecordsError::ResponseHeaderTimeout {
        timeout: config.response_header_timeout,
    })??;
    let status = response.status();
    let rate_limit = RateLimitSnapshot::from_headers(response.headers());
    if let Some(content_length) = response.content_length() {
        enforce_cap("max_page_bytes", content_length, config.max_page_bytes)?;
    }
    let body = read_response_body_with_cap(response, &config).await?;

    if !status.is_success() {
        return Err(classify_error_status(status, &rate_limit, &body));
    }

    serde_json::from_slice::<ListRecordsPage>(&body)
        .map(|page| FetchedListRecordsPage { page, rate_limit })
        .map_err(ListRecordsError::PageJson)
}

async fn read_response_body_with_cap(
    response: reqwest::Response,
    config: &ListRecordsConfig,
) -> Result<Vec<u8>, ListRecordsError> {
    let mut body = Vec::new();
    let mut observed = 0_u64;
    let mut stream = response.bytes_stream();
    let started = time::Instant::now();
    let mut progress_window_started = started;
    let mut progress_window_bytes = 0_u64;

    while let Some(next_chunk) = time::timeout(
        next_page_chunk_timeout(started, config, observed)?,
        stream.next(),
    )
    .await
    .map_err(|_elapsed| list_records_timeout_error(started, config, observed))?
    {
        enforce_page_progress(
            &mut progress_window_started,
            &mut progress_window_bytes,
            config,
        )?;
        let chunk = next_chunk?;
        let chunk_len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        observed =
            observed
                .checked_add(chunk_len)
                .ok_or(ListRecordsError::ResourceLimitExceeded {
                    limit: "max_page_bytes",
                    observed: u64::MAX,
                    max: config.max_page_bytes,
                })?;
        enforce_cap("max_page_bytes", observed, config.max_page_bytes)?;
        body.extend_from_slice(&chunk);
        progress_window_bytes = progress_window_bytes.checked_add(chunk_len).ok_or(
            ListRecordsError::ResourceLimitExceeded {
                limit: "min_progress_bytes",
                observed: u64::MAX,
                max: config.min_progress_bytes,
            },
        )?;
    }
    Ok(body)
}

fn next_page_chunk_timeout(
    started: time::Instant,
    config: &ListRecordsConfig,
    observed: u64,
) -> Result<Duration, ListRecordsError> {
    let Some(remaining) = config.page_download_timeout.checked_sub(started.elapsed()) else {
        return Err(ListRecordsError::DownloadTimeout {
            timeout: config.page_download_timeout,
            observed_bytes: observed,
        });
    };
    Ok(remaining.min(config.chunk_idle_timeout))
}

fn list_records_timeout_error(
    started: time::Instant,
    config: &ListRecordsConfig,
    observed: u64,
) -> ListRecordsError {
    if started.elapsed() >= config.page_download_timeout {
        ListRecordsError::DownloadTimeout {
            timeout: config.page_download_timeout,
            observed_bytes: observed,
        }
    } else {
        ListRecordsError::InactivityTimeout {
            timeout: config.chunk_idle_timeout,
        }
    }
}

fn enforce_page_progress(
    window_started: &mut time::Instant,
    window_bytes: &mut u64,
    config: &ListRecordsConfig,
) -> Result<(), ListRecordsError> {
    if config.min_progress_bytes == 0 || config.min_progress_interval.is_zero() {
        return Ok(());
    }
    if window_started.elapsed() < config.min_progress_interval {
        return Ok(());
    }
    if *window_bytes < config.min_progress_bytes {
        return Err(ListRecordsError::ProgressTimeout {
            interval: config.min_progress_interval,
            min_bytes: config.min_progress_bytes,
            observed_bytes: *window_bytes,
        });
    }
    *window_started = time::Instant::now();
    *window_bytes = 0;
    Ok(())
}

fn classify_error_status(
    status: StatusCode,
    rate_limit: &RateLimitSnapshot,
    body: &[u8],
) -> ListRecordsError {
    let decoded = serde_json::from_slice::<XrpcError>(body).ok();
    let error_code = decoded
        .as_ref()
        .and_then(|body| body.error.as_ref())
        .map(|value| value.clone().into_boxed_str());
    let message = decoded
        .and_then(|body| body.message)
        .map(String::into_boxed_str);
    if let Some(state) = error_code.as_deref().and_then(parse_account_state) {
        return ListRecordsError::AccountState {
            state,
            status: status.as_u16(),
            message,
            rate_limit: Box::new(rate_limit.clone()),
        };
    }
    ListRecordsError::HttpStatus {
        status: status.as_u16(),
        error_code,
        message,
        rate_limit: Box::new(rate_limit.clone()),
    }
}

#[derive(Debug, Deserialize)]
struct XrpcError {
    error: Option<String>,
    message: Option<String>,
}

fn parse_account_state(value: &str) -> Option<AccountState> {
    match value {
        "RepoNotFound" => Some(AccountState::RepoNotFound),
        "RepoTakendown" => Some(AccountState::RepoTakendown),
        "RepoSuspended" => Some(AccountState::RepoSuspended),
        "RepoDeactivated" => Some(AccountState::RepoDeactivated),
        _other => None,
    }
}

#[derive(Debug)]
struct ListRecordDecodeError;

impl fmt::Display for ListRecordDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("listRecords post record failed to decode")
    }
}

struct DecodedListRecord {
    post: PostRecord,
    typed_decode_failed: bool,
}

fn post_record_from_list_record(
    did_str: &str,
    record: ListRecordsRecord,
) -> Result<DecodedListRecord, ListRecordDecodeError> {
    let rkey = rkey_from_uri(did_str, &record.uri).ok_or(ListRecordDecodeError)?;
    let cid = validated_record_cid(record.cid)?;
    post_decode::from_json_value(record.value).map_or(Err(ListRecordDecodeError), |decoded| {
        Ok(DecodedListRecord {
            post: PostRecord {
                rkey: rkey.to_owned(),
                cid,
                body: decoded.body,
            },
            typed_decode_failed: decoded.typed_decode_failed,
        })
    })
}

fn validated_record_cid(cid: Option<String>) -> Result<String, ListRecordDecodeError> {
    let Some(cid) = cid else {
        return Err(ListRecordDecodeError);
    };
    if cid.is_empty() {
        return Err(ListRecordDecodeError);
    }
    cid::Cid::try_from(cid.as_str()).map_err(|_error| ListRecordDecodeError)?;
    Ok(cid)
}

fn rkey_from_uri<'a>(did_str: &str, uri: &'a str) -> Option<&'a str> {
    let prefix = format!("at://{did_str}/{POST_COLLECTION}/");
    let rkey = uri.strip_prefix(&prefix)?;
    if rkey.is_empty() || rkey.contains('/') || Rkey::<&str>::new(rkey).is_err() {
        return None;
    }
    Some(rkey)
}

const fn enforce_cap(limit: &'static str, observed: u64, max: u64) -> Result<(), ListRecordsError> {
    if observed <= max {
        return Ok(());
    }
    Err(ListRecordsError::ResourceLimitExceeded {
        limit,
        observed,
        max,
    })
}

#[cfg(test)]
mod tests;
