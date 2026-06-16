//! `com.atproto.repo.listRecords` fallback fetch and archive path.

use std::{fmt, path::Path};

use jacquard_api::app_bsky::feed::post::Post;
use jacquard_common::{deps::fluent_uri::Uri, types::did::Did};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use smol_str::SmolStr;

use crate::{
    archive::{
        ArchiveArtifacts, ArchiveCommitContext, ArchiveError, CompletenessClass, FetchMethod,
        RepoReceipt, StreamingArchiveSink, StreamingReceiptInput, archive_row_from_post,
    },
    parse::{PostRecord, PostRecordBody, RawPartialPostRecord},
    transport::{AccountState, RateLimitSnapshot},
};

const POST_COLLECTION: &str = "app.bsky.feed.post";
const LIST_RECORDS_XRPC: &str = "com.atproto.repo.listRecords";
const DEFAULT_PAGE_LIMIT: u16 = 100;
const DEFAULT_MAX_PAGES: u64 = 100_000;
const DEFAULT_MAX_RECORDS: u64 = 10_000_000;
const DEFAULT_MAX_PAGE_BYTES: u64 = 8_388_608;

/// Pagination and response-size caps for `listRecords`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListRecordsConfig {
    pub page_limit: u16,
    pub max_pages: u64,
    pub max_records: u64,
    pub max_page_bytes: u64,
}

impl Default for ListRecordsConfig {
    fn default() -> Self {
        Self {
            page_limit: DEFAULT_PAGE_LIMIT,
            max_pages: DEFAULT_MAX_PAGES,
            max_records: DEFAULT_MAX_RECORDS,
            max_page_bytes: DEFAULT_MAX_PAGE_BYTES,
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
    pub cid: String,
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
            Self::Transport(_) => true,
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
    let mut archiver = ListRecordsArchiver::new(did_str, archive_dir, archive_context, config)?;
    let mut rate_limits = Vec::new();
    let mut cursor = None;

    loop {
        let fetched = fetch_list_records_page(http, pds, did, cursor.as_deref(), config).await?;
        rate_limits.push(fetched.rate_limit);
        let next_cursor = fetched.page.cursor.clone();
        archiver.push_page(fetched.page)?;
        if next_cursor.is_none() {
            break;
        }
        if next_cursor == cursor {
            return Err(ListRecordsError::Protocol(
                "PDS returned the same listRecords cursor twice".to_owned(),
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
    let body = response.bytes().await?;
    enforce_cap(
        "max_page_bytes",
        u64::try_from(body.len()).unwrap_or(u64::MAX),
        config.max_page_bytes,
    )?;

    if !status.is_success() {
        return Err(classify_error_status(status, &rate_limit, &body));
    }

    serde_json::from_slice::<ListRecordsPage>(&body)
        .map(|page| FetchedListRecordsPage { page, rate_limit })
        .map_err(ListRecordsError::PageJson)
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
    let mut value = record.value;
    if let serde_json::Value::Object(object) = &mut value {
        object
            .entry("$type")
            .or_insert_with(|| serde_json::Value::String(POST_COLLECTION.to_owned()));
    }
    match serde_json::from_value::<Post<SmolStr>>(value.clone()) {
        Ok(post) => Ok(DecodedListRecord {
            post: PostRecord {
                rkey: rkey.to_owned(),
                cid: record.cid,
                body: PostRecordBody::Typed(Box::new(post)),
            },
            typed_decode_failed: false,
        }),
        Err(_error) => {
            raw_partial_post_from_json_value(value).map_or(Err(ListRecordDecodeError), |post| {
                Ok(DecodedListRecord {
                    post: PostRecord {
                        rkey: rkey.to_owned(),
                        cid: record.cid,
                        body: PostRecordBody::RawPartial(post),
                    },
                    typed_decode_failed: true,
                })
            })
        }
    }
}

fn raw_partial_post_from_json_value(value: serde_json::Value) -> Option<RawPartialPostRecord> {
    let serde_json::Value::Object(mut fields) = value else {
        return None;
    };
    let created_at_raw = raw_created_at(fields.get("createdAt"));
    let text = fields
        .get("text")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let langs = raw_string_array(fields.get("langs"));
    for key in ["$type", "createdAt", "langs", "text"] {
        fields.remove(key);
    }
    Some(RawPartialPostRecord {
        typed_decode_failed: raw_post_typed_decode_failed(&fields),
        created_at_raw,
        text,
        langs,
        extras_json: serde_json::Value::Object(fields),
    })
}

fn raw_post_typed_decode_failed(fields: &serde_json::Map<String, serde_json::Value>) -> bool {
    if !matches!(fields.get("createdAt"), Some(serde_json::Value::String(_))) {
        return true;
    }
    if !matches!(fields.get("text"), Some(serde_json::Value::String(_))) {
        return true;
    }
    raw_langs_decode_failed(fields.get("langs"))
}

fn raw_langs_decode_failed(value: Option<&serde_json::Value>) -> bool {
    let Some(value) = value else {
        return false;
    };
    let serde_json::Value::Array(values) = value else {
        return true;
    };
    values
        .iter()
        .any(|value| !matches!(value, serde_json::Value::String(_)))
}

fn raw_created_at(value: Option<&serde_json::Value>) -> Option<String> {
    match value {
        None => None,
        Some(serde_json::Value::String(value)) => Some(value.clone()),
        Some(value) => Some(value.to_string()),
    }
}

fn raw_string_array(value: Option<&serde_json::Value>) -> Vec<String> {
    let Some(serde_json::Value::Array(values)) = value else {
        return Vec::new();
    };
    values
        .iter()
        .filter_map(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .collect()
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
    use std::{fs, path::PathBuf, time::SystemTime};

    use serde_json::json;

    use super::*;
    use crate::archive::{CompletenessClass, FetchMethod, read_archive_post_rows};

    #[test]
    fn paginated_pages_archive_collection_paginated_receipt() {
        let archive_dir = temp_dir("list-records-pages");
        let did = "did:plc:testrepo";
        let pages = vec![
            ListRecordsPage {
                records: vec![post_record(did, "3kabc", "bafyreia", "hello")],
                cursor: Some("next".to_owned()),
            },
            ListRecordsPage {
                records: vec![post_record(did, "3kabd", "bafyreib", "second")],
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
        assert_eq!(second.cid, "bafyreib");

        fs::remove_dir_all(archive_dir).expect("remove archive dir");
    }

    #[test]
    fn invalid_record_is_counted_as_decode_error() {
        let archive_dir = temp_dir("list-records-decode-error");
        let did = "did:plc:testrepo";
        let pages = vec![ListRecordsPage {
            records: vec![ListRecordsRecord {
                uri: format!("at://{did}/{POST_COLLECTION}/3kabc"),
                cid: "bafyreia".to_owned(),
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

    fn post_record(did: &str, rkey: &str, cid: &str, text: &str) -> ListRecordsRecord {
        ListRecordsRecord {
            uri: format!("at://{did}/{POST_COLLECTION}/{rkey}"),
            cid: cid.to_owned(),
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
}
