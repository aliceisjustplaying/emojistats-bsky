//! Archive receipt, `Parquet`, and manifest primitives for the `fetch-one` vertical slice.

use std::{
    borrow::Cow,
    error::Error,
    fmt, fs,
    fs::File,
    io::{self as std_io, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, builder::StringBuilder};
use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, SecondsFormat, Utc};
pub use emoji_normalizer::NormalizerVersion;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::{NamedTempFile, TempPath};

use crate::{
    commit::{LocalStore, ManifestMode, Metadata, Request},
    derive::{DeriveError, borrowed_emoji_projection_rows_for_post, derive_emoji_projection_rows},
    hash::hash_serialized_json,
    parse::{ParsedRepo, PostRecord, PostRecordBody, ProfileRecord, RawPartialPostRecord},
};

const POST_COLLECTION: &str = "app.bsky.feed.post";
const PARTIAL_RECORD_STATUS: &str = "typed_decode_failed";
const ARCHIVE_SCHEMA_VERSION: u16 = 1;
const PARQUET_BATCH_ROWS: usize = 65_536;

/// Data-model-lossless post row before `Parquet` encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchivePostRow {
    pub did: String,
    pub rkey: String,
    pub cid: String,
    pub normalizer: NormalizerVersion,
    pub account_status: Option<String>,
    pub record_status: Option<String>,
    pub public_content_label: Option<String>,
    pub created_at_raw: Option<String>,
    pub created_at_normalized: Option<String>,
    pub created_at_parse_status: CreatedAtParseStatus,
    pub text: String,
    pub langs: Vec<String>,
    pub emoji_sequence: Vec<String>,
    pub extras_json: serde_json::Value,
}

/// Incremental hasher for canonical archive post row content.
#[derive(Debug, Default)]
pub struct ArchivePostRowsHasher {
    hasher: Sha256,
}

impl ArchivePostRowsHasher {
    #[must_use]
    pub fn new() -> Self {
        Self {
            hasher: Sha256::new(),
        }
    }

    /// Add one archive row to the canonical post-row hash.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveError`] if row content cannot be framed for hashing.
    pub fn push_row(&mut self, row: &ArchivePostRow) -> Result<(), ArchiveError> {
        archive_io::hash_post_row_into(&mut self.hasher, row)
    }

    #[must_use]
    pub fn finish(self) -> String {
        hex::encode(self.hasher.finalize())
    }
}

/// Compact local serving projection row derived from an archive row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmojiProjectionRow {
    pub did: String,
    pub rkey: String,
    pub created_at_normalized: Option<String>,
    pub emoji: String,
    pub occurrences: u64,
    pub langs: Vec<String>,
}

/// Local profile sidecar row for one repo, when present.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProfileSidecarRow<'a> {
    pub rkey: &'a str,
    pub cid: &'a str,
    pub record: &'a jacquard_api::app_bsky::actor::profile::Profile<smol_str::SmolStr>,
}

/// Classification for author-supplied `createdAt`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CreatedAtParseStatus {
    Valid,
    Missing,
    Invalid,
    Future,
}

/// Receipt over the rows produced for one fetched repo.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoReceipt {
    pub observed_at: String,
    pub fetch_method: FetchMethod,
    pub completeness_class: CompletenessClass,
    pub reachable_records_count: u64,
    pub reachable_post_records_count: u64,
    pub archived_post_rows_count: u64,
    pub post_decode_error_count: u64,
    pub emoji_posts_count: u64,
    pub emoji_occurrences_count: u64,
    pub mst_root_cid: Option<String>,
    pub commit_cid: Option<String>,
    /// Schema v1 contains only post rows, so this intentionally equals `post_rows_hash`.
    pub archive_rows_hash: String,
    pub post_rows_hash: String,
    pub emoji_projection_hash: String,
    pub profile_row_hash: Option<String>,
    pub normalizer: NormalizerVersion,
    pub repo_commit_signature_verified: bool,
    pub identity_verified: bool,
}

/// Inputs required to build one repo receipt.
#[derive(Debug, Clone)]
pub struct RepoReceiptInput<'a> {
    pub rows: &'a [ArchivePostRow],
    pub reachable_records_count: u64,
    pub reachable_post_records_count: u64,
    pub post_decode_error_count: u64,
    pub profile_row_hash: Option<String>,
    pub mst_root_cid: Option<String>,
    pub commit_cid: Option<String>,
    pub normalizer: NormalizerVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchMethod {
    GetRepo,
    ListRecords,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletenessClass {
    ContentAddressedSnapshot,
    CollectionPaginated,
}

/// Local manifest entry before the Storage Box commit protocol exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalManifestEntry {
    pub run_id: String,
    pub shard: String,
    pub file_sequence: u64,
    pub dataset: String,
    pub local_path: PathBuf,
    pub row_count: u64,
    pub bytes: u64,
    pub content_hash: String,
    pub min_created_at_normalized: Option<String>,
    pub max_created_at_normalized: Option<String>,
    pub receipt_hash: String,
    pub schema_version: u16,
    pub normalizer: NormalizerVersion,
}

/// Stable identity for one archive commit attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveCommitContext {
    pub run_id: String,
    pub shard: String,
    pub file_sequence: u64,
    pub observed_at: DateTime<Utc>,
}

impl ArchiveCommitContext {
    #[must_use]
    pub fn new(run_id: impl Into<String>, shard: impl Into<String>, file_sequence: u64) -> Self {
        Self {
            run_id: run_id.into(),
            shard: shard.into(),
            file_sequence,
            observed_at: Utc::now(),
        }
    }

    #[must_use]
    pub fn fetch_one_local() -> Self {
        Self::new("fetch-one-local", "single", 1)
    }
}

/// Files produced by Stage D for one `fetch-one` run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveArtifacts {
    pub parquet_path: PathBuf,
    pub receipt_path: PathBuf,
    pub object_receipt_path: PathBuf,
    pub manifest_path: PathBuf,
    pub emoji_projection_path: PathBuf,
    pub profile_sidecar_path: Option<PathBuf>,
    pub profile_sidecar_receipt_path: Option<PathBuf>,
    pub profile_sidecar_manifest_path: Option<PathBuf>,
    pub manifest: LocalManifestEntry,
    pub emoji_rows: u64,
}

/// Stage D archive/derive failures.
#[derive(Debug)]
pub enum ArchiveError {
    Io(std_io::Error),
    Parquet(parquet::errors::ParquetError),
    Arrow(arrow_schema::ArrowError),
    Json(serde_json::Error),
    Commit(crate::commit::Error),
    CountOverflow {
        field: &'static str,
    },
    InvalidCompression(String),
    InvalidPath {
        path: PathBuf,
    },
    InvalidRecordJson,
    InvalidParquetColumn {
        column: &'static str,
    },
    InvalidParquetValue {
        column: &'static str,
        value: String,
    },
    UnexpectedParquetNull {
        column: &'static str,
    },
    FinalPathExists {
        path: PathBuf,
    },
    FinalHashMismatch {
        path: PathBuf,
        expected: String,
        observed: String,
    },
}

#[path = "archive/io.rs"]
mod archive_io;
mod write;

pub use archive_io::{
    archive_post_rows_from_record_batch, build_repo_receipt, hash_post_rows, hash_profile_record,
    read_all_archive_post_rows,
};
pub use write::{
    StreamingArchiveSink, StreamingReceiptInput, archive_row_from_owned_post,
    archive_row_from_owned_post_observed_at, archive_row_from_post,
    archive_row_from_post_observed_at, archive_rows_from_parsed_repo, current_normalizer,
    write_archive_artifacts,
};

/// Archive classification for an author-supplied `createdAt` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassifiedCreatedAt {
    pub raw: Option<String>,
    pub normalized: Option<String>,
    pub status: CreatedAtParseStatus,
}

/// Classify an author-supplied `createdAt` value for archive rows.
#[must_use]
pub fn classify_created_at(value: Option<&str>) -> ClassifiedCreatedAt {
    classify_created_at_observed_at(value, Utc::now())
}

/// Classify an author-supplied `createdAt` value against a fixed observation time.
#[must_use]
pub fn classify_created_at_observed_at(
    value: Option<&str>,
    observed_at: DateTime<Utc>,
) -> ClassifiedCreatedAt {
    value.map_or_else(
        || ClassifiedCreatedAt {
            raw: None,
            normalized: None,
            status: CreatedAtParseStatus::Missing,
        },
        |raw| classify_present_created_at(raw, observed_at),
    )
}

fn classify_present_created_at(raw: &str, observed_at: DateTime<Utc>) -> ClassifiedCreatedAt {
    match DateTime::parse_from_rfc3339(raw) {
        Ok(timestamp) if timestamp.with_timezone(&Utc) > observed_at => ClassifiedCreatedAt {
            raw: Some(raw.to_owned()),
            normalized: None,
            status: CreatedAtParseStatus::Future,
        },
        Ok(timestamp) => ClassifiedCreatedAt {
            raw: Some(raw.to_owned()),
            normalized: Some(
                timestamp
                    .with_timezone(&Utc)
                    .to_rfc3339_opts(SecondsFormat::Secs, true),
            ),
            status: CreatedAtParseStatus::Valid,
        },
        Err(_error) => ClassifiedCreatedAt {
            raw: Some(raw.to_owned()),
            normalized: None,
            status: CreatedAtParseStatus::Invalid,
        },
    }
}

fn format_observed_at(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn write_temp_idempotent<F>(path: &Path, write: F) -> Result<(), ArchiveError>
where
    F: FnOnce(&Path) -> Result<(), ArchiveError>,
{
    let parent = path.parent().ok_or_else(|| ArchiveError::InvalidPath {
        path: path.to_path_buf(),
    })?;
    let temp_file = NamedTempFile::new_in(parent).map_err(ArchiveError::Io)?;
    match write(temp_file.path()) {
        Ok(()) => {
            sync_file(temp_file.path())?;
            promote_named_temp_idempotent(temp_file, path)
        }
        Err(error) => Err(error),
    }
}

fn promote_named_temp_idempotent(
    temp_file: NamedTempFile,
    path: &Path,
) -> Result<(), ArchiveError> {
    let temp_digest = archive_io::hash_file_for_archive(temp_file.path())?;
    match temp_file.persist_noclobber(path) {
        Ok(_file) => sync_parent_dir(path),
        Err(error) if error.error.kind() == std_io::ErrorKind::AlreadyExists => {
            let final_digest = archive_io::hash_file_for_archive(path)?;
            if final_digest.sha256 != temp_digest.sha256 || final_digest.bytes != temp_digest.bytes
            {
                return Err(ArchiveError::FinalHashMismatch {
                    path: path.to_path_buf(),
                    expected: temp_digest.sha256,
                    observed: final_digest.sha256,
                });
            }
            Ok(())
        }
        Err(error) => Err(ArchiveError::Io(error.error)),
    }
}

fn promote_temp_idempotent(temp_path: &Path, path: &Path) -> Result<(), ArchiveError> {
    let temp_digest = archive_io::hash_file_for_archive(temp_path)?;
    match fs::hard_link(temp_path, path) {
        Ok(()) => {
            let _ignored = fs::remove_file(temp_path);
            sync_parent_dir(path)
        }
        Err(error) if error.kind() == std_io::ErrorKind::AlreadyExists => {
            let final_digest = archive_io::hash_file_for_archive(path)?;
            if final_digest.sha256 != temp_digest.sha256 || final_digest.bytes != temp_digest.bytes
            {
                return Err(ArchiveError::FinalHashMismatch {
                    path: path.to_path_buf(),
                    expected: temp_digest.sha256,
                    observed: final_digest.sha256,
                });
            }
            let _ignored = fs::remove_file(temp_path);
            Ok(())
        }
        Err(error) => Err(ArchiveError::Io(error)),
    }
}

fn sync_file(path: &Path) -> Result<(), ArchiveError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn sync_parent_dir(path: &Path) -> Result<(), ArchiveError> {
    let parent = path.parent().ok_or_else(|| ArchiveError::InvalidPath {
        path: path.to_path_buf(),
    })?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

impl CreatedAtParseStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Missing => "missing",
            Self::Invalid => "invalid",
            Self::Future => "future",
        }
    }
}

impl fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Parquet(error) => write!(f, "Parquet error: {error}"),
            Self::Arrow(error) => write!(f, "Arrow error: {error}"),
            Self::Json(error) => write!(f, "JSON error: {error}"),
            Self::Commit(error) => write!(f, "commit protocol error: {error}"),
            Self::CountOverflow { field } => write!(f, "count overflow for {field}"),
            Self::InvalidCompression(error) => write!(f, "invalid compression level: {error}"),
            Self::InvalidPath { path } => write!(f, "invalid archive path: {}", path.display()),
            Self::InvalidRecordJson => f.write_str("post record serialized to non-object JSON"),
            Self::InvalidParquetColumn { column } => {
                write!(f, "invalid archive Parquet column: {column}")
            }
            Self::InvalidParquetValue { column, value } => {
                write!(f, "invalid archive Parquet value for {column}: {value}")
            }
            Self::UnexpectedParquetNull { column } => {
                write!(f, "unexpected null in archive Parquet column: {column}")
            }
            Self::FinalPathExists { path } => {
                write!(f, "archive final path already exists: {}", path.display())
            }
            Self::FinalHashMismatch {
                path,
                expected,
                observed,
            } => write!(
                f,
                "archive final hash mismatch for {}: expected {expected}, observed {observed}",
                path.display()
            ),
        }
    }
}

impl Error for ArchiveError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Parquet(error) => Some(error),
            Self::Arrow(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Commit(error) => Some(error),
            Self::CountOverflow { .. }
            | Self::InvalidCompression(_)
            | Self::InvalidPath { .. }
            | Self::InvalidRecordJson
            | Self::InvalidParquetColumn { .. }
            | Self::InvalidParquetValue { .. }
            | Self::UnexpectedParquetNull { .. }
            | Self::FinalPathExists { .. }
            | Self::FinalHashMismatch { .. } => None,
        }
    }
}

impl From<std_io::Error> for ArchiveError {
    fn from(error: std_io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<parquet::errors::ParquetError> for ArchiveError {
    fn from(error: parquet::errors::ParquetError) -> Self {
        Self::Parquet(error)
    }
}

impl From<arrow_schema::ArrowError> for ArchiveError {
    fn from(error: arrow_schema::ArrowError) -> Self {
        Self::Arrow(error)
    }
}

impl From<serde_json::Error> for ArchiveError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<crate::commit::Error> for ArchiveError {
    fn from(error: crate::commit::Error) -> Self {
        Self::Commit(error)
    }
}

#[cfg(test)]
#[path = "archive/tests.rs"]
mod tests;
