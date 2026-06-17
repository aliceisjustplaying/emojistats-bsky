//! Archive receipt, `Parquet`, and manifest primitives for the `fetch-one` vertical slice.

use std::{
    borrow::Cow,
    fs,
    fs::File,
    io::{self as std_io, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use ::parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use arrow_array::{Array, ArrayRef, LargeStringArray, RecordBatch, builder::LargeStringBuilder};
use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, SecondsFormat, Utc};
pub use emoji_normalizer::NormalizerVersion;
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempPath};

use crate::{
    commit::{LocalStore, ManifestMode, Metadata, Request},
    hash::hash_serialized_json,
    parse::{ParsedRepo, PostRecord, PostRecordBody, ProfileRecord, RawPartialPostRecord},
};

const POST_COLLECTION: &str = "app.bsky.feed.post";
const PARTIAL_RECORD_STATUS: &str = "typed_decode_failed";
pub const RAW_ARCHIVE_POSTS_DATASET: &str = "raw_archive_posts";
pub const COLLECTION_PAGINATED_POSTS_DATASET: &str = "collection_paginated_posts";
pub const NONCANONICAL_POSTS_DATASET: &str = "noncanonical_posts";
pub const ARCHIVE_SCHEMA_VERSION: u16 = 3;
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

/// Compact local serving projection row derived from an archive row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmojiProjectionRow {
    pub did: String,
    pub rkey: String,
    pub cid: String,
    pub created_at_normalized: Option<String>,
    pub created_at_parse_status: CreatedAtParseStatus,
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
    pub did: String,
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
    pub observed_at: DateTime<Utc>,
    pub did: &'a str,
    pub fetch_method: FetchMethod,
    pub completeness_class: CompletenessClass,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostDataset {
    RawArchivePosts,
    CollectionPaginatedPosts,
}

impl PostDataset {
    #[must_use]
    pub const fn from_dataset(value: &str) -> Option<Self> {
        match value.as_bytes() {
            b"raw_archive_posts" => Some(Self::RawArchivePosts),
            b"collection_paginated_posts" => Some(Self::CollectionPaginatedPosts),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RawArchivePosts => RAW_ARCHIVE_POSTS_DATASET,
            Self::CollectionPaginatedPosts => COLLECTION_PAGINATED_POSTS_DATASET,
        }
    }

    #[must_use]
    pub const fn fetch_method(self) -> FetchMethod {
        match self {
            Self::RawArchivePosts => FetchMethod::GetRepo,
            Self::CollectionPaginatedPosts => FetchMethod::ListRecords,
        }
    }

    #[must_use]
    pub const fn completeness_class(self) -> CompletenessClass {
        match self {
            Self::RawArchivePosts => CompletenessClass::ContentAddressedSnapshot,
            Self::CollectionPaginatedPosts => CompletenessClass::CollectionPaginated,
        }
    }
}

/// Manifest entry projected into local paths for smoke/derive compatibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalManifestEntry {
    pub manifest_format_version: u16,
    pub run_id: String,
    pub shard: String,
    pub file_sequence: u64,
    pub did: String,
    pub dataset: String,
    pub local_path: PathBuf,
    pub row_count: u64,
    pub bytes: u64,
    pub content_hash: String,
    pub min_created_at_normalized: Option<String>,
    pub max_created_at_normalized: Option<String>,
    pub receipt_hash: String,
    pub repo_receipt_path: Option<PathBuf>,
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

/// Archive commit backend.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ArchiveStorageConfig {
    #[default]
    Local,
    StorageBoxSsh(StorageBoxArchiveConfig),
    StorageBoxRclone(StorageBoxRcloneArchiveConfig),
}

impl ArchiveStorageConfig {
    #[must_use]
    pub const fn backend_name(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::StorageBoxSsh(_) => "storage_box_ssh",
            Self::StorageBoxRclone(_) => "storage_box_rclone",
        }
    }
}

/// SSH Storage Box archive backend configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageBoxArchiveConfig {
    pub remote_root: String,
    pub ssh_remote: String,
    pub ssh_program: PathBuf,
    pub ssh_args: Vec<String>,
    pub command_timeout: Duration,
}

/// Rclone/SFTP Storage Box archive backend configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageBoxRcloneArchiveConfig {
    pub remote_root: String,
    pub remote_name: String,
    pub config_path: PathBuf,
    pub rclone_program: PathBuf,
    pub command_timeout: Duration,
}

impl StorageBoxArchiveConfig {
    #[must_use]
    pub fn new(remote_root: impl Into<String>, ssh_remote: impl Into<String>) -> Self {
        Self {
            remote_root: remote_root.into(),
            ssh_remote: ssh_remote.into(),
            ssh_program: PathBuf::from("ssh"),
            ssh_args: Vec::new(),
            command_timeout: Duration::from_secs(300),
        }
    }
}

impl StorageBoxRcloneArchiveConfig {
    #[must_use]
    pub fn new(
        remote_root: impl Into<String>,
        remote_name: impl Into<String>,
        config_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            remote_root: remote_root.into(),
            remote_name: remote_name.into(),
            config_path: config_path.into(),
            rclone_program: PathBuf::from("rclone"),
            command_timeout: Duration::from_secs(300),
        }
    }
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
#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("I/O error: {0}")]
    Io(#[from] std_io::Error),
    #[error("Parquet error: {0}")]
    Parquet(#[from] ::parquet::errors::ParquetError),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("commit protocol error: {0}")]
    Commit(#[from] crate::commit::Error),
    #[error("Storage Box commit error: {0}")]
    StorageBox(#[from] crate::storage_box::Error),
    #[error("count overflow for {field}")]
    CountOverflow { field: &'static str },
    #[error("invalid compression level: {0}")]
    InvalidCompression(String),
    #[error("invalid archive path: {}", path.display())]
    InvalidPath { path: PathBuf },
    #[error("post record serialized to non-object JSON")]
    InvalidRecordJson,
    #[error("invalid archive Parquet column: {column}")]
    InvalidParquetColumn { column: &'static str },
    #[error("invalid archive Parquet value for {column}: {value}")]
    InvalidParquetValue { column: &'static str, value: String },
    #[error("unexpected null in archive Parquet column: {column}")]
    UnexpectedParquetNull { column: &'static str },
    #[error("archive final path already exists: {}", path.display())]
    FinalPathExists { path: PathBuf },
    #[error("archive final hash mismatch for {}: expected {expected}, observed {observed}", path.display())]
    FinalHashMismatch {
        path: PathBuf,
        expected: String,
        observed: String,
    },
}

#[path = "archive/io.rs"]
mod archive_io;
#[path = "archive/commit_backend.rs"]
mod commit_backend;
#[path = "archive/full_write.rs"]
mod full_write;
#[path = "archive/hash.rs"]
mod hash;
#[path = "archive/json.rs"]
mod json;
#[path = "archive/naming.rs"]
mod naming;
#[path = "archive/parquet.rs"]
mod parquet;
#[path = "archive/projection.rs"]
mod projection;
#[path = "archive/projection_writer.rs"]
mod projection_writer;
#[path = "archive/row.rs"]
mod row;
mod write;

pub use archive_io::{build_repo_receipt, hash_profile_record};
pub use full_write::write_local_archive_artifacts;
pub use hash::{ArchivePostRowsHasher, hash_post_rows};
pub use parquet::{archive_post_rows_from_record_batch, read_all_archive_post_rows};
pub use projection::{
    BorrowedEmojiProjectionRow, borrowed_emoji_projection_rows_for_post,
    derive_emoji_projection_rows, emoji_projection_rows_for_post,
};
pub use row::{
    archive_row_from_owned_post, archive_row_from_owned_post_observed_at, archive_row_from_post,
    archive_row_from_post_observed_at, archive_rows_from_parsed_repo, current_normalizer,
};
pub use write::{StreamingArchiveSink, StreamingReceiptInput};

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
            normalized: Some(
                timestamp
                    .with_timezone(&Utc)
                    .to_rfc3339_opts(SecondsFormat::Micros, true),
            ),
            status: CreatedAtParseStatus::Future,
        },
        Ok(timestamp) => ClassifiedCreatedAt {
            raw: Some(raw.to_owned()),
            normalized: Some(
                timestamp
                    .with_timezone(&Utc)
                    .to_rfc3339_opts(SecondsFormat::Micros, true),
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
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
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
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Missing => "missing",
            Self::Invalid => "invalid",
            Self::Future => "future",
        }
    }
}

#[cfg(test)]
#[path = "archive/tests.rs"]
mod tests;
