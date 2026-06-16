//! `com.atproto.repo.listRecords` fallback fetch and archive path.

use std::{collections::HashSet, fmt, path::Path, time::Duration};

use futures_util::StreamExt as _;
use jacquard_common::{deps::fluent_uri::Uri, types::did::Did};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use tokio::time;

use crate::{
    archive::{
        ArchiveArtifacts, ArchiveCommitContext, ArchiveError, CompletenessClass, FetchMethod,
        RepoReceipt, StreamingArchiveSink, StreamingReceiptInput, archive_row_from_post,
    },
    parse::PostRecord,
    post_decode,
    transport::{AccountState, RateLimitSnapshot},
};

const POST_COLLECTION: &str = "app.bsky.feed.post";
const LIST_RECORDS_XRPC: &str = "com.atproto.repo.listRecords";
const DEFAULT_PAGE_LIMIT: u16 = 100;
const DEFAULT_MAX_PAGES: u64 = 100_000;
const DEFAULT_MAX_RECORDS: u64 = 10_000_000;
const DEFAULT_MAX_PAGE_BYTES: u64 = 8_388_608;
const DEFAULT_CHUNK_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Pagination and response-size caps for `listRecords`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListRecordsConfig {
    pub page_limit: u16,
    pub max_pages: u64,
    pub max_records: u64,
    pub max_page_bytes: u64,
    pub chunk_idle_timeout: Duration,
}

impl Default for ListRecordsConfig {
    fn default() -> Self {
        Self {
            page_limit: DEFAULT_PAGE_LIMIT,
            max_pages: DEFAULT_MAX_PAGES,
            max_records: DEFAULT_MAX_RECORDS,
            max_page_bytes: DEFAULT_MAX_PAGE_BYTES,
            chunk_idle_timeout: DEFAULT_CHUNK_IDLE_TIMEOUT,
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
    pub rate_limits: Vec<RateLimitSnapshot>,
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
    #[error("no listRecords body chunk within {timeout:?}")]
    InactivityTimeout { timeout: Duration },
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
}

impl ListRecordsError {
    #[must_use]
    pub const fn rate_limit(&self) -> Option<&RateLimitSnapshot> {
        match self {
            Self::AccountState { rate_limit, .. } | Self::HttpStatus { rate_limit, .. } => {
                Some(rate_limit)
            }
            Self::Transport(_)
            | Self::InactivityTimeout { .. }
            | Self::PageJson(_)
            | Self::Archive(_)
            | Self::ResourceLimitExceeded { .. }
            | Self::Protocol(_) => None,
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
            Self::Transport(_) | Self::InactivityTimeout { .. } => true,
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
    config: ListRecordsConfig,
    mut observe_rate_limit: impl FnMut(&RateLimitSnapshot),
) -> Result<ListRecordsArchiveOutput, ListRecordsError> {
    let mut archiver = ListRecordsArchiver::new(did_str, archive_dir, archive_context, config)?;
    let mut rate_limits = Vec::new();
    let mut seen_cursors = HashSet::new();
    let mut cursor: Option<String> = None;

    loop {
        let fetched = fetch_list_records_page(http, pds, did, cursor.as_deref(), config).await?;
        observe_rate_limit(&fetched.rate_limit);
        rate_limits.push(fetched.rate_limit);
        let next_cursor = fetched.page.cursor.clone();
        archiver.push_page(fetched.page)?;
        let Some(next_cursor_value) = next_cursor.as_deref() else {
            break;
        };
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
        cursor = next_cursor;
    }

    let mut output = archiver.finish()?;
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
        config,
    )?;

    for page in pages {
        archiver.push_page(page)?;
    }

    archiver.finish()
}

struct ListRecordsArchiver<'a> {
    did_str: &'a str,
    sink: StreamingArchiveSink,
    config: ListRecordsConfig,
    records: u64,
    decode_errors: u64,
    pages_seen: u64,
}

impl<'a> ListRecordsArchiver<'a> {
    fn new(
        did_str: &'a str,
        archive_dir: &Path,
        archive_context: ArchiveCommitContext,
        config: ListRecordsConfig,
    ) -> Result<Self, ListRecordsError> {
        Ok(Self {
            did_str,
            sink: StreamingArchiveSink::new(archive_dir, did_str, archive_context)?,
            config,
            records: 0,
            decode_errors: 0,
            pages_seen: 0,
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
        match post_record_from_list_record(self.did_str, record) {
            Ok(decoded) => {
                if decoded.typed_decode_failed {
                    self.increment_decode_errors()?;
                }
                let row = archive_row_from_post(
                    self.did_str,
                    &decoded.post,
                    &self.sink.normalizer().clone(),
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
                    max: u64::MAX,
                })?;
        Ok(())
    }

    fn finish(self) -> Result<ListRecordsArchiveOutput, ListRecordsError> {
        let records = self.records;
        let decode_errors = self.decode_errors;
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

        Ok(ListRecordsArchiveOutput {
            archived_posts: receipt.archived_post_rows_count,
            receipt,
            artifacts,
            records,
            decode_errors,
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

    let response = http.get(url).query(&query).send().await?;
    let status = response.status();
    let rate_limit = RateLimitSnapshot::from_headers(response.headers());
    if let Some(content_length) = response.content_length() {
        enforce_cap("max_page_bytes", content_length, config.max_page_bytes)?;
    }
    let body =
        read_response_body_with_cap(response, config.max_page_bytes, config.chunk_idle_timeout)
            .await?;

    if !status.is_success() {
        return Err(classify_error_status(status, &rate_limit, &body));
    }

    serde_json::from_slice::<ListRecordsPage>(&body)
        .map(|page| FetchedListRecordsPage { page, rate_limit })
        .map_err(ListRecordsError::PageJson)
}

async fn read_response_body_with_cap(
    response: reqwest::Response,
    max_page_bytes: u64,
    chunk_idle_timeout: Duration,
) -> Result<Vec<u8>, ListRecordsError> {
    let mut body = Vec::new();
    let mut observed = 0_u64;
    let mut stream = response.bytes_stream();
    while let Some(next_chunk) = time::timeout(chunk_idle_timeout, stream.next())
        .await
        .map_err(|_elapsed| ListRecordsError::InactivityTimeout {
            timeout: chunk_idle_timeout,
        })?
    {
        let chunk = next_chunk?;
        let chunk_len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        observed =
            observed
                .checked_add(chunk_len)
                .ok_or(ListRecordsError::ResourceLimitExceeded {
                    limit: "max_page_bytes",
                    observed: u64::MAX,
                    max: max_page_bytes,
                })?;
        enforce_cap("max_page_bytes", observed, max_page_bytes)?;
        body.extend_from_slice(&chunk);
    }
    Ok(body)
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
        return Ok(String::new());
    };
    if cid.is_empty() {
        return Ok(cid);
    }
    cid::Cid::try_from(cid.as_str()).map_err(|_error| ListRecordDecodeError)?;
    Ok(cid)
}

fn rkey_from_uri<'a>(did_str: &str, uri: &'a str) -> Option<&'a str> {
    let prefix = format!("at://{did_str}/{POST_COLLECTION}/");
    let rkey = uri.strip_prefix(&prefix)?;
    if rkey.is_empty() || rkey.contains('/') {
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
mod tests {
    use std::{
        fs,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        path::PathBuf,
        thread,
        time::{Duration, SystemTime},
    };

    use jacquard_common::deps::fluent_uri::Uri;
    use serde_json::json;

    use super::*;
    use crate::archive::{CompletenessClass, FetchMethod, read_archive_post_rows};

    const TEST_CID_A: &str = "bafyreihyrpejdc3l3wqlbm7vuzx7hhvx6r5eg44vqyqjna6u6kwtpoyqte";
    const TEST_CID_B: &str = "bafyreibqj2lhp4fpizc2zstcsl2mzo6fycjfnwc6kyz4xpr2lzyqlw6wxi";

    #[test]
    fn paginated_pages_archive_collection_paginated_receipt() {
        let archive_dir = temp_dir("list-records-pages");
        let did = "did:plc:testrepo";
        let pages = vec![
            ListRecordsPage {
                records: vec![post_record(did, "3kabc", TEST_CID_A, "hello")],
                cursor: Some("next".to_owned()),
            },
            ListRecordsPage {
                records: vec![post_record(did, "3kabd", TEST_CID_B, "second")],
                cursor: None,
            },
        ];

        let output =
            archive_list_records_pages(did, &archive_dir, pages, ListRecordsConfig::default())
                .expect("archive listRecords pages");

        assert_eq!(output.records, 2);
        assert_eq!(output.archived_posts, 2);
        assert_eq!(output.decode_errors, 0);
        assert_eq!(output.receipt.fetch_method, FetchMethod::ListRecords);
        assert_eq!(
            output.receipt.completeness_class,
            CompletenessClass::CollectionPaginated
        );
        assert_eq!(output.receipt.mst_root_cid, None);
        assert_eq!(output.receipt.commit_cid, None);
        let rows = read_archive_post_rows(&output.artifacts.parquet_path).expect("read parquet");
        assert_eq!(rows.len(), 2);
        let first = rows.first().expect("first row");
        let second = rows.get(1).expect("second row");
        assert_eq!(first.rkey, "3kabc");
        assert_eq!(second.cid, TEST_CID_B);

        fs::remove_dir_all(archive_dir).expect("remove archive dir");
    }

    #[test]
    fn invalid_record_is_counted_as_decode_error() {
        let archive_dir = temp_dir("list-records-decode-error");
        let did = "did:plc:testrepo";
        let pages = vec![ListRecordsPage {
            records: vec![ListRecordsRecord {
                uri: format!("at://{did}/{POST_COLLECTION}/3kabc"),
                cid: Some(TEST_CID_A.to_owned()),
                value: json!({"$type": POST_COLLECTION, "text": "missing createdAt"}),
            }],
            cursor: None,
        }];

        let output =
            archive_list_records_pages(did, &archive_dir, pages, ListRecordsConfig::default())
                .expect("archive listRecords pages");

        assert_eq!(output.records, 1);
        assert_eq!(output.archived_posts, 1);
        assert_eq!(output.decode_errors, 1);
        assert_eq!(output.receipt.post_decode_error_count, 1);
        let rows = read_archive_post_rows(&output.artifacts.parquet_path).expect("read parquet");
        assert_eq!(rows.len(), 1);
        let row = rows.first().expect("partial row");
        assert_eq!(row.record_status.as_deref(), Some("typed_decode_failed"));
        assert_eq!(
            row.created_at_parse_status,
            crate::archive::CreatedAtParseStatus::Missing
        );

        fs::remove_dir_all(archive_dir).expect("remove archive dir");
    }

    #[test]
    fn invalid_record_cid_is_counted_as_decode_error() {
        let archive_dir = temp_dir("list-records-invalid-cid");
        let did = "did:plc:testrepo";
        let pages = vec![ListRecordsPage {
            records: vec![ListRecordsRecord {
                uri: format!("at://{did}/{POST_COLLECTION}/3kabc"),
                cid: Some("not-a-cid".to_owned()),
                value: json!({
                    "$type": POST_COLLECTION,
                    "createdAt": "2026-06-16T00:00:00Z",
                    "text": "hello"
                }),
            }],
            cursor: None,
        }];

        let output =
            archive_list_records_pages(did, &archive_dir, pages, ListRecordsConfig::default())
                .expect("archive listRecords pages");

        assert_eq!(output.records, 1);
        assert_eq!(output.archived_posts, 0);
        assert_eq!(output.decode_errors, 1);
        fs::remove_dir_all(archive_dir).expect("remove archive dir");
    }

    #[test]
    fn missing_record_cid_archives_with_empty_cid() {
        let archive_dir = temp_dir("list-records-missing-cid");
        let did = "did:plc:testrepo";
        let pages = vec![ListRecordsPage {
            records: vec![ListRecordsRecord {
                uri: format!("at://{did}/{POST_COLLECTION}/3kabc"),
                cid: None,
                value: json!({
                    "$type": POST_COLLECTION,
                    "createdAt": "2026-06-16T00:00:00Z",
                    "text": "hello"
                }),
            }],
            cursor: None,
        }];

        let output =
            archive_list_records_pages(did, &archive_dir, pages, ListRecordsConfig::default())
                .expect("archive listRecords pages");

        assert_eq!(output.records, 1);
        assert_eq!(output.archived_posts, 1);
        let rows = read_archive_post_rows(&output.artifacts.parquet_path).expect("read parquet");
        assert_eq!(rows.first().expect("row").cid, "");
        fs::remove_dir_all(archive_dir).expect("remove archive dir");
    }

    #[tokio::test]
    async fn fetch_rejects_cursor_cycle_beyond_immediate_repeat() {
        let archive_dir = temp_dir("list-records-cursor-cycle");
        let did_str = "did:plc:testrepo";
        let (base_url, handle) = spawn_list_records_server(vec![
            TestResponse::json_page(None, Some("cursor-a"), Some(10), true),
            TestResponse::json_page(None, Some("cursor-b"), Some(9), true),
            TestResponse::json_page(None, Some("cursor-a"), Some(8), true),
        ]);
        let http = Client::new();
        let pds = Uri::parse(base_url).expect("parse pds").clone();
        let did = Did::new_owned(did_str).expect("parse did");

        let error = fetch_and_archive_list_records(
            &http,
            &pds,
            &did,
            did_str,
            &archive_dir,
            ArchiveCommitContext::fetch_one_local(),
            ListRecordsConfig::default(),
        )
        .await
        .expect_err("cursor cycle should fail");

        match error {
            ListRecordsError::Protocol(message) => {
                assert_eq!(message, "PDS returned a repeated listRecords cursor");
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(handle.join().expect("server thread").len(), 3);
        fs::remove_dir_all(archive_dir).expect("remove archive dir");
    }

    #[tokio::test]
    async fn fetch_enforces_page_byte_cap_without_content_length() {
        let did_str = "did:plc:testrepo";
        let body = json!({"records": [], "cursor": null}).to_string();
        let max_page_bytes = u64::try_from(body.len() - 1).expect("body length fits");
        let (base_url, handle) =
            spawn_list_records_server(vec![TestResponse::raw(body, None, false)]);
        let http = Client::new();
        let pds = Uri::parse(base_url).expect("parse pds").clone();
        let did = Did::new_owned(did_str).expect("parse did");

        let error = fetch_list_records_page(
            &http,
            &pds,
            &did,
            None,
            ListRecordsConfig {
                max_page_bytes,
                ..ListRecordsConfig::default()
            },
        )
        .await
        .expect_err("oversize page should fail");

        match error {
            ListRecordsError::ResourceLimitExceeded {
                limit,
                observed,
                max,
            } => {
                assert_eq!(limit, "max_page_bytes");
                assert!(observed > max);
                assert_eq!(max, max_page_bytes);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(handle.join().expect("server thread").len(), 1);
    }

    #[tokio::test]
    async fn fetch_enforces_chunk_idle_timeout() {
        let did_str = "did:plc:testrepo";
        let body = json!({"records": [], "cursor": null}).to_string();
        let (base_url, handle) = spawn_list_records_server(vec![TestResponse::raw_delayed(
            body,
            None,
            true,
            Duration::from_millis(100),
        )]);
        let http = Client::new();
        let pds = Uri::parse(base_url).expect("parse pds").clone();
        let did = Did::new_owned(did_str).expect("parse did");

        let error = fetch_list_records_page(
            &http,
            &pds,
            &did,
            None,
            ListRecordsConfig {
                chunk_idle_timeout: Duration::from_millis(20),
                ..ListRecordsConfig::default()
            },
        )
        .await
        .expect_err("idle page body should fail");

        assert!(matches!(error, ListRecordsError::InactivityTimeout { .. }));
        assert_eq!(handle.join().expect("server thread").len(), 1);
    }

    #[tokio::test]
    async fn rate_limit_observer_runs_after_each_fetched_page() {
        let archive_dir = temp_dir("list-records-rate-limit-observer");
        let did_str = "did:plc:testrepo";
        let first_record = post_record(did_str, "3kabc", TEST_CID_A, "hello");
        let second_record = post_record(did_str, "3kabd", TEST_CID_B, "second");
        let (base_url, handle) = spawn_list_records_server(vec![
            TestResponse::json_page(Some(&first_record), Some("next"), Some(4), true),
            TestResponse::json_page(Some(&second_record), None, Some(3), true),
        ]);
        let http = Client::new();
        let pds = Uri::parse(base_url).expect("parse pds").clone();
        let did = Did::new_owned(did_str).expect("parse did");
        let mut observed_remaining = Vec::new();

        let output = fetch_and_archive_list_records_with_rate_limit_observer(
            &http,
            &pds,
            &did,
            did_str,
            &archive_dir,
            ArchiveCommitContext::fetch_one_local(),
            ListRecordsConfig::default(),
            |rate_limit| observed_remaining.push(rate_limit.remaining),
        )
        .await
        .expect("fetch and archive listRecords");

        assert_eq!(observed_remaining, vec![Some(4), Some(3)]);
        assert_eq!(
            output
                .rate_limits
                .iter()
                .map(|rate_limit| rate_limit.remaining)
                .collect::<Vec<_>>(),
            vec![Some(4), Some(3)]
        );
        assert_eq!(output.archived_posts, 2);
        assert_eq!(handle.join().expect("server thread").len(), 2);
        fs::remove_dir_all(archive_dir).expect("remove archive dir");
    }

    fn post_record(did: &str, rkey: &str, cid: &str, text: &str) -> ListRecordsRecord {
        ListRecordsRecord {
            uri: format!("at://{did}/{POST_COLLECTION}/{rkey}"),
            cid: Some(cid.to_owned()),
            value: json!({
                "$type": POST_COLLECTION,
                "createdAt": "2026-06-16T00:00:00Z",
                "text": text
            }),
        }
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{prefix}-{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    struct TestResponse {
        body: String,
        remaining: Option<u64>,
        content_length: bool,
        body_delay: Option<Duration>,
    }

    impl TestResponse {
        fn json_page(
            record: Option<&ListRecordsRecord>,
            cursor: Option<&str>,
            remaining: Option<u64>,
            content_length: bool,
        ) -> Self {
            Self::raw(
                json!({
                    "records": record.into_iter().map(record_json).collect::<Vec<_>>(),
                    "cursor": cursor
                })
                .to_string(),
                remaining,
                content_length,
            )
        }

        fn raw(body: String, remaining: Option<u64>, content_length: bool) -> Self {
            Self {
                body,
                remaining,
                content_length,
                body_delay: None,
            }
        }

        fn raw_delayed(
            body: String,
            remaining: Option<u64>,
            content_length: bool,
            body_delay: Duration,
        ) -> Self {
            Self {
                body,
                remaining,
                content_length,
                body_delay: Some(body_delay),
            }
        }
    }

    fn record_json(record: &ListRecordsRecord) -> serde_json::Value {
        json!({
            "uri": record.uri,
            "cid": record.cid,
            "value": record.value,
        })
    }

    fn spawn_list_records_server(
        responses: Vec<TestResponse>,
    ) -> (String, thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind test server");
        let addr = listener.local_addr().expect("server addr");
        let handle = thread::spawn(move || {
            responses
                .into_iter()
                .map(|response| {
                    let (mut stream, _addr) = listener.accept().expect("accept request");
                    let target = read_request_target(&mut stream);
                    write_list_records_response(&mut stream, &response);
                    target
                })
                .collect::<Vec<_>>()
        });

        (format!("http://{addr}"), handle)
    }

    fn read_request_target(stream: &mut TcpStream) -> String {
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
        header_text
            .lines()
            .next()
            .expect("request line")
            .split_whitespace()
            .nth(1)
            .expect("request target")
            .to_owned()
    }

    fn write_list_records_response(stream: &mut TcpStream, response: &TestResponse) {
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n"
        )
        .expect("write status");
        if let Some(remaining) = response.remaining {
            write!(stream, "ratelimit-remaining: {remaining}\r\n").expect("write rate limit");
        }
        if response.content_length {
            write!(stream, "Content-Length: {}\r\n", response.body.len())
                .expect("write content length");
        }
        write!(stream, "\r\n").expect("write header terminator");
        if let Some(delay) = response.body_delay {
            thread::sleep(delay);
        }
        let _ = write!(stream, "{}", response.body);
    }
}
