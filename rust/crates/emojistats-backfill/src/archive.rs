//! Archive receipt, `Parquet`, and manifest primitives for the `fetch-one` vertical slice.

use std::{
    borrow::Cow,
    error::Error,
    fmt, fs,
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
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
        hash_post_row_into(&mut self.hasher, row)
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
    SnapshotComplete,
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
}

impl ArchiveCommitContext {
    #[must_use]
    pub fn new(run_id: impl Into<String>, shard: impl Into<String>, file_sequence: u64) -> Self {
        Self {
            run_id: run_id.into(),
            shard: shard.into(),
            file_sequence,
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
    Io(io::Error),
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

/// Convert parsed post records into the first archive-row shape.
///
/// # Errors
///
/// Returns [`ArchiveError`] if record extras cannot be serialized without loss.
pub fn archive_rows_from_parsed_repo(
    parsed: &ParsedRepo,
) -> Result<Vec<ArchivePostRow>, ArchiveError> {
    let normalizer = current_normalizer();
    parsed
        .posts
        .iter()
        .map(|post| archive_row_from_post(&parsed.commit.did, post, &normalizer))
        .collect()
}

/// Convert one parsed post into an archive row without retaining the whole repo.
///
/// # Errors
///
/// Returns [`ArchiveError`] if record extras cannot be serialized without loss.
pub fn archive_row_from_post(
    did: &str,
    post: &PostRecord,
    normalizer: &NormalizerVersion,
) -> Result<ArchivePostRow, ArchiveError> {
    match &post.body {
        PostRecordBody::Typed(record) => {
            archive_row_from_typed_post(did, &post.rkey, &post.cid, record, normalizer)
        }
        PostRecordBody::RawPartial(record) => Ok(archive_row_from_raw_partial_post(
            did, post, record, normalizer,
        )),
    }
}

/// Convert an owned parsed post into an archive row.
///
/// # Errors
///
/// Returns [`ArchiveError`] if record extras cannot be serialized without loss.
pub fn archive_row_from_owned_post(
    did: &str,
    post: PostRecord,
    normalizer: &NormalizerVersion,
) -> Result<ArchivePostRow, ArchiveError> {
    let PostRecord { rkey, cid, body } = post;
    match body {
        PostRecordBody::Typed(record) => {
            archive_row_from_typed_post(did, &rkey, &cid, &record, normalizer)
        }
        PostRecordBody::RawPartial(record) => Ok(archive_row_from_owned_raw_partial_post(
            did, rkey, cid, record, normalizer,
        )),
    }
}

fn archive_row_from_typed_post(
    did: &str,
    rkey: &str,
    cid: &str,
    record: &jacquard_api::app_bsky::feed::post::Post<smol_str::SmolStr>,
    normalizer: &NormalizerVersion,
) -> Result<ArchivePostRow, ArchiveError> {
    let created_at = record.created_at.as_str();
    let classified = classify_created_at(Some(created_at));
    Ok(ArchivePostRow {
        did: did.to_owned(),
        rkey: rkey.to_owned(),
        cid: cid.to_owned(),
        normalizer: normalizer.clone(),
        account_status: None,
        record_status: None,
        public_content_label: None,
        created_at_raw: classified.raw,
        created_at_normalized: classified.normalized,
        created_at_parse_status: classified.status,
        text: record.text.to_string(),
        langs: record.langs.as_ref().map_or_else(Vec::new, |langs| {
            langs.iter().map(ToString::to_string).collect()
        }),
        emoji_sequence: extract_emojis(record.text.as_str()),
        extras_json: record_extras_json(record)?,
    })
}

fn archive_row_from_raw_partial_post(
    did: &str,
    post: &PostRecord,
    partial: &RawPartialPostRecord,
    normalizer: &NormalizerVersion,
) -> ArchivePostRow {
    let classified = classify_created_at(partial.created_at_raw.as_deref());
    let text = partial.text.clone().unwrap_or_default();
    ArchivePostRow {
        did: did.to_owned(),
        rkey: post.rkey.clone(),
        cid: post.cid.clone(),
        normalizer: normalizer.clone(),
        account_status: None,
        record_status: partial
            .typed_decode_failed
            .then(|| PARTIAL_RECORD_STATUS.to_owned()),
        public_content_label: None,
        created_at_raw: classified.raw,
        created_at_normalized: classified.normalized,
        created_at_parse_status: classified.status,
        emoji_sequence: extract_emojis(&text),
        text,
        langs: partial.langs.clone(),
        extras_json: partial.extras_json.clone(),
    }
}

fn archive_row_from_owned_raw_partial_post(
    did: &str,
    rkey: String,
    cid: String,
    partial: RawPartialPostRecord,
    normalizer: &NormalizerVersion,
) -> ArchivePostRow {
    let classified = classify_created_at(partial.created_at_raw.as_deref());
    let text = partial.text.unwrap_or_default();
    ArchivePostRow {
        did: did.to_owned(),
        rkey,
        cid,
        normalizer: normalizer.clone(),
        account_status: None,
        record_status: partial
            .typed_decode_failed
            .then(|| PARTIAL_RECORD_STATUS.to_owned()),
        public_content_label: None,
        created_at_raw: classified.raw,
        created_at_normalized: classified.normalized,
        created_at_parse_status: classified.status,
        emoji_sequence: extract_emojis(&text),
        text,
        langs: partial.langs,
        extras_json: partial.extras_json,
    }
}

/// Current vertical-slice normalizer identity.
#[must_use]
pub fn current_normalizer() -> NormalizerVersion {
    emoji_normalizer::version()
}

/// Write local archive artifacts for one parsed repo.
///
/// # Errors
///
/// Returns [`ArchiveError`] if local filesystem, `Parquet`, `Arrow`, serialization, or
/// resource-count work fails.
pub fn write_archive_artifacts(
    output_dir: &Path,
    did: &str,
    commit_context: &ArchiveCommitContext,
    rows: &[ArchivePostRow],
    profile: Option<&ProfileRecord>,
    receipt: &RepoReceipt,
) -> Result<ArchiveArtifacts, ArchiveError> {
    fs::create_dir_all(output_dir)?;
    let artifact_stem = artifact_file_stem(did);
    let parquet_object_path = PathBuf::from(format!("{artifact_stem}.posts.parquet"));
    let receipt_path = output_dir.join(format!("{artifact_stem}.receipt.json"));
    let object_receipt_object_path = PathBuf::from(format!("{artifact_stem}.object-receipt.json"));
    let manifest_object_path = PathBuf::from(format!("{artifact_stem}.manifest.jsonl"));
    let emoji_projection_path = output_dir.join(format!("{artifact_stem}.emoji.jsonl"));
    let profile_sidecar_object_path = PathBuf::from(format!("{artifact_stem}.profile.json"));
    let profile_sidecar_receipt_object_path =
        PathBuf::from(format!("{artifact_stem}.profile.object-receipt.json"));
    let profile_sidecar_manifest_object_path =
        PathBuf::from(format!("{artifact_stem}.profile.manifest.jsonl"));

    write_temp_rename(&receipt_path, |path| write_json_pretty(path, receipt))?;
    let store = LocalStore::new(output_dir);
    let commit_request = Request {
        object_path: parquet_object_path,
        receipt_path: object_receipt_object_path,
        manifest_path: manifest_object_path,
        manifest_mode: ManifestMode::AppendJsonl,
        metadata: build_commit_metadata(rows, receipt, commit_context)?,
    };
    let committed = store.commit(&commit_request, |file| {
        write_posts_parquet_to_writer(file, rows)
            .map_err(|error| crate::commit::Error::writer(format!("write posts parquet: {error}")))
    })?;
    let emoji_projection_rows =
        derive_emoji_projection_rows(rows).map_err(archive_error_from_derive)?;
    let emoji_rows = u64::try_from(emoji_projection_rows.len()).map_err(|_error| {
        ArchiveError::CountOverflow {
            field: "emoji_rows",
        }
    })?;
    write_temp_rename(&emoji_projection_path, |path| {
        write_emoji_projection_jsonl(path, &emoji_projection_rows)
    })?;
    let committed_profile = profile
        .map(|profile| {
            commit_profile_sidecar(
                &store,
                profile_sidecar_object_path,
                profile_sidecar_receipt_object_path,
                profile_sidecar_manifest_object_path,
                profile,
                receipt,
                commit_context,
            )
        })
        .transpose()?;

    let manifest = local_manifest_from_committed(&committed, receipt);

    Ok(ArchiveArtifacts {
        parquet_path: committed.object_path,
        receipt_path,
        object_receipt_path: committed.receipt_path,
        manifest_path: committed.manifest_path,
        emoji_projection_path,
        profile_sidecar_path: committed_profile
            .as_ref()
            .map(|artifact| artifact.object_path.clone()),
        profile_sidecar_receipt_path: committed_profile
            .as_ref()
            .map(|artifact| artifact.receipt_path.clone()),
        profile_sidecar_manifest_path: committed_profile.map(|artifact| artifact.manifest_path),
        manifest,
        emoji_rows,
    })
}

/// Streaming writer for one repo's archive artifacts.
pub struct StreamingArchiveSink {
    output_dir: PathBuf,
    artifact_stem: String,
    parquet_temp_path: PathBuf,
    receipt_path: PathBuf,
    emoji_projection_path: PathBuf,
    emoji_projection_temp_path: PathBuf,
    writer: Option<ArrowWriter<File>>,
    schema: Arc<Schema>,
    batch: Vec<ArchivePostRow>,
    rows_hash: Sha256,
    emoji_projection_hash: Sha256,
    archived_post_rows_count: u64,
    emoji_posts_count: u64,
    emoji_occurrences_count: u64,
    emoji_rows: u64,
    min_created_at_normalized: Option<String>,
    max_created_at_normalized: Option<String>,
    normalizer: NormalizerVersion,
    commit_context: ArchiveCommitContext,
    did: String,
    hash_prefix: Vec<u8>,
    hash_after_cid: Vec<u8>,
    hash_public_none: Vec<u8>,
    emoji_file: File,
}

/// Summary fields needed to finish a streaming repo receipt.
#[derive(Debug, Clone)]
pub struct StreamingReceiptInput {
    pub fetch_method: FetchMethod,
    pub completeness_class: CompletenessClass,
    pub reachable_records_count: u64,
    pub reachable_post_records_count: u64,
    pub post_decode_error_count: u64,
    pub profile_row_hash: Option<String>,
    pub mst_root_cid: Option<String>,
    pub commit_cid: Option<String>,
}

impl StreamingArchiveSink {
    /// Create a streaming sink for one repo.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveError`] if local files or the `Parquet` writer cannot be opened.
    pub fn new(
        output_dir: &Path,
        did: &str,
        commit_context: ArchiveCommitContext,
    ) -> Result<Self, ArchiveError> {
        fs::create_dir_all(output_dir)?;
        let artifact_stem = artifact_file_stem(did);
        let parquet_path = output_dir.join(format!("{artifact_stem}.posts.parquet"));
        let parquet_temp_path = temp_path_for(&parquet_path)?;
        let receipt_path = output_dir.join(format!("{artifact_stem}.receipt.json"));
        let emoji_projection_path = output_dir.join(format!("{artifact_stem}.emoji.jsonl"));
        let emoji_projection_temp_path = temp_path_for(&emoji_projection_path)?;
        remove_if_exists(&parquet_temp_path)?;
        remove_if_exists(&emoji_projection_temp_path)?;
        let parquet_file = File::create(&parquet_temp_path)?;
        let emoji_file = File::create(&emoji_projection_temp_path)?;
        let schema = archive_schema();
        let normalizer = current_normalizer();
        let writer = ArrowWriter::try_new(
            parquet_file,
            Arc::clone(&schema),
            Some(parquet_writer_properties()?),
        )?;
        let hash_prefix = framed_fields([POST_COLLECTION, did])?;
        let mut hash_after_cid = Vec::new();
        append_normalizer_frames(&mut hash_after_cid, &normalizer)?;
        append_hash_field_frame(&mut hash_after_cid, "none")?;
        let hash_public_none = framed_fields(["none"])?;
        Ok(Self {
            output_dir: output_dir.to_path_buf(),
            artifact_stem,
            parquet_temp_path,
            receipt_path,
            emoji_projection_path,
            emoji_projection_temp_path,
            writer: Some(writer),
            schema,
            batch: Vec::with_capacity(PARQUET_BATCH_ROWS),
            rows_hash: Sha256::new(),
            emoji_projection_hash: Sha256::new(),
            archived_post_rows_count: 0,
            emoji_posts_count: 0,
            emoji_occurrences_count: 0,
            emoji_rows: 0,
            min_created_at_normalized: None,
            max_created_at_normalized: None,
            normalizer,
            commit_context,
            did: did.to_owned(),
            hash_prefix,
            hash_after_cid,
            hash_public_none,
            emoji_file,
        })
    }

    /// Normalizer version used by this sink.
    #[must_use]
    pub const fn normalizer(&self) -> &NormalizerVersion {
        &self.normalizer
    }

    /// Write one archive row into the streaming artifacts.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveError`] if hashing, JSONL writing, or `Parquet` batch writing fails.
    pub fn push_row(&mut self, row: ArchivePostRow) -> Result<(), ArchiveError> {
        self.hash_streaming_row(&row)?;
        self.archived_post_rows_count =
            self.archived_post_rows_count
                .checked_add(1)
                .ok_or(ArchiveError::CountOverflow {
                    field: "archived_post_rows_count",
                })?;
        if !row.emoji_sequence.is_empty() {
            self.emoji_posts_count =
                self.emoji_posts_count
                    .checked_add(1)
                    .ok_or(ArchiveError::CountOverflow {
                        field: "emoji_posts_count",
                    })?;
        }
        let row_occurrences = u64::try_from(row.emoji_sequence.len()).map_err(|_error| {
            ArchiveError::CountOverflow {
                field: "emoji_occurrences_count",
            }
        })?;
        self.emoji_occurrences_count = self
            .emoji_occurrences_count
            .checked_add(row_occurrences)
            .ok_or(ArchiveError::CountOverflow {
                field: "emoji_occurrences_count",
            })?;
        update_min_max_created_at(
            &mut self.min_created_at_normalized,
            &mut self.max_created_at_normalized,
            row.created_at_normalized.as_deref(),
        );
        if !row.emoji_sequence.is_empty() {
            self.write_emoji_projection_rows(&row)?;
        }
        self.batch.push(row);
        if self.batch.len() >= PARQUET_BATCH_ROWS {
            self.flush_batch()?;
        }
        Ok(())
    }

    fn hash_streaming_row(&mut self, row: &ArchivePostRow) -> Result<(), ArchiveError> {
        if row.did != self.did
            || row.normalizer != self.normalizer
            || row.account_status.is_some()
            || row.public_content_label.is_some()
        {
            return hash_post_row_into(&mut self.rows_hash, row);
        }
        self.rows_hash.update(&self.hash_prefix);
        hash_field(&mut self.rows_hash, &row.rkey)?;
        hash_field(&mut self.rows_hash, &row.cid)?;
        self.rows_hash.update(&self.hash_after_cid);
        hash_optional_field(&mut self.rows_hash, row.record_status.as_deref())?;
        self.rows_hash.update(&self.hash_public_none);
        hash_optional_field(&mut self.rows_hash, row.created_at_raw.as_deref())?;
        hash_optional_field(&mut self.rows_hash, row.created_at_normalized.as_deref())?;
        hash_field(&mut self.rows_hash, row.created_at_parse_status.as_str())?;
        hash_field(&mut self.rows_hash, &row.text)?;
        hash_string_slice(&mut self.rows_hash, &row.langs)?;
        hash_string_slice(&mut self.rows_hash, &row.emoji_sequence)?;
        hash_extras_json(&mut self.rows_hash, &row.extras_json)
    }

    fn write_emoji_projection_rows(&mut self, row: &ArchivePostRow) -> Result<(), ArchiveError> {
        for projection_row in
            borrowed_emoji_projection_rows_for_post(row).map_err(archive_error_from_derive)?
        {
            let json = json_bytes(&projection_row)?;
            hash_field_bytes(&mut self.emoji_projection_hash, &json)?;
            self.emoji_file.write_all(&json)?;
            self.emoji_file.write_all(b"\n")?;
            self.emoji_rows =
                self.emoji_rows
                    .checked_add(1)
                    .ok_or(ArchiveError::CountOverflow {
                        field: "emoji_rows",
                    })?;
        }

        Ok(())
    }

    /// Finish all artifacts and return the receipt plus artifact paths.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveError`] for filesystem, hash, JSON, or `Parquet` failures.
    pub fn finish(
        mut self,
        input: StreamingReceiptInput,
        profile: Option<&ProfileRecord>,
    ) -> Result<(RepoReceipt, ArchiveArtifacts), ArchiveError> {
        self.finish_stream_files()?;
        let receipt = self.build_streaming_receipt(input);
        write_temp_rename(&self.receipt_path, |path| write_json_pretty(path, &receipt))?;
        let receipt_hash = hash_serialized_json(&receipt)?;
        let committed_posts = self.commit_streaming_posts(&receipt_hash)?;
        let manifest = local_manifest_from_committed(&committed_posts, &receipt);
        let committed_profile = self.commit_profile(profile, &receipt)?;
        let artifacts = self.into_artifacts(manifest, committed_posts, committed_profile);
        Ok((receipt, artifacts))
    }

    fn finish_stream_files(&mut self) -> Result<(), ArchiveError> {
        self.flush_batch()?;
        self.writer
            .take()
            .ok_or(ArchiveError::CountOverflow {
                field: "streaming_parquet_writer_missing",
            })?
            .close()?;
        self.emoji_file.sync_all()?;
        promote_temp_no_overwrite(
            &self.emoji_projection_temp_path,
            &self.emoji_projection_path,
        )?;
        sync_parent_dir(&self.emoji_projection_path)
    }

    fn build_streaming_receipt(&self, input: StreamingReceiptInput) -> RepoReceipt {
        let post_rows_hash = hex::encode(self.rows_hash.clone().finalize());
        RepoReceipt {
            fetch_method: input.fetch_method,
            completeness_class: input.completeness_class,
            reachable_records_count: input.reachable_records_count,
            reachable_post_records_count: input.reachable_post_records_count,
            archived_post_rows_count: self.archived_post_rows_count,
            post_decode_error_count: input.post_decode_error_count,
            emoji_posts_count: self.emoji_posts_count,
            emoji_occurrences_count: self.emoji_occurrences_count,
            mst_root_cid: input.mst_root_cid,
            commit_cid: input.commit_cid,
            archive_rows_hash: post_rows_hash.clone(),
            post_rows_hash,
            emoji_projection_hash: hex::encode(self.emoji_projection_hash.clone().finalize()),
            profile_row_hash: input.profile_row_hash,
            normalizer: self.normalizer.clone(),
            repo_commit_signature_verified: false,
            identity_verified: false,
        }
    }

    fn commit_streaming_posts(
        &self,
        receipt_hash: &str,
    ) -> Result<crate::commit::Artifact, ArchiveError> {
        let store = LocalStore::new(&self.output_dir);
        let request = Request {
            object_path: PathBuf::from(format!("{}.posts.parquet", self.artifact_stem)),
            receipt_path: PathBuf::from(format!("{}.object-receipt.json", self.artifact_stem)),
            manifest_path: PathBuf::from(format!("{}.manifest.jsonl", self.artifact_stem)),
            manifest_mode: ManifestMode::AppendJsonl,
            metadata: self.streaming_posts_metadata(receipt_hash),
        };
        Ok(store.commit_prepared_temp(&request, &self.parquet_temp_path)?)
    }

    fn streaming_posts_metadata(&self, receipt_hash: &str) -> Metadata {
        Metadata {
            run_id: self.commit_context.run_id.clone(),
            shard: self.commit_context.shard.clone(),
            file_sequence: self.commit_context.file_sequence,
            dataset: "raw_archive_posts".to_owned(),
            row_count: self.archived_post_rows_count,
            min_created_at_normalized: self.min_created_at_normalized.clone(),
            max_created_at_normalized: self.max_created_at_normalized.clone(),
            receipt_hash: receipt_hash.to_owned(),
            normalizer: self.normalizer.clone(),
            schema_version: ARCHIVE_SCHEMA_VERSION,
        }
    }

    fn commit_profile(
        &self,
        profile: Option<&ProfileRecord>,
        receipt: &RepoReceipt,
    ) -> Result<Option<crate::commit::Artifact>, ArchiveError> {
        let store = LocalStore::new(&self.output_dir);
        profile
            .map(|profile| {
                commit_profile_sidecar(
                    &store,
                    PathBuf::from(format!("{}.profile.json", self.artifact_stem)),
                    PathBuf::from(format!(
                        "{}.profile.object-receipt.json",
                        self.artifact_stem
                    )),
                    PathBuf::from(format!("{}.profile.manifest.jsonl", self.artifact_stem)),
                    profile,
                    receipt,
                    &self.commit_context,
                )
            })
            .transpose()
    }

    fn into_artifacts(
        self,
        manifest: LocalManifestEntry,
        committed_posts: crate::commit::Artifact,
        committed_profile: Option<crate::commit::Artifact>,
    ) -> ArchiveArtifacts {
        ArchiveArtifacts {
            parquet_path: committed_posts.object_path,
            receipt_path: self.receipt_path.clone(),
            object_receipt_path: committed_posts.receipt_path,
            manifest_path: committed_posts.manifest_path,
            emoji_projection_path: self.emoji_projection_path.clone(),
            profile_sidecar_path: committed_profile
                .as_ref()
                .map(|artifact| artifact.object_path.clone()),
            profile_sidecar_receipt_path: committed_profile
                .as_ref()
                .map(|artifact| artifact.receipt_path.clone()),
            profile_sidecar_manifest_path: committed_profile.map(|artifact| artifact.manifest_path),
            manifest,
            emoji_rows: self.emoji_rows,
        }
    }

    fn flush_batch(&mut self) -> Result<(), ArchiveError> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let batch = post_record_batch(&self.schema, &self.batch)?;
        self.writer
            .as_mut()
            .ok_or(ArchiveError::CountOverflow {
                field: "streaming_parquet_writer_missing",
            })?
            .write(&batch)?;
        self.batch.clear();
        Ok(())
    }
}

impl Drop for StreamingArchiveSink {
    fn drop(&mut self) {
        self.writer.take();
        let _ignored = fs::remove_file(&self.parquet_temp_path);
        let _ignored = fs::remove_file(&self.emoji_projection_temp_path);
    }
}

/// Read raw archive post rows from the Stage D Parquet shape.
///
/// # Errors
///
/// Returns [`ArchiveError`] when the file cannot be read as the expected archive schema,
/// or when JSON-encoded row fields fail to decode.
pub fn read_archive_post_rows(path: &Path) -> Result<Vec<ArchivePostRow>, ArchiveError> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut rows = Vec::new();
    for batch in reader {
        append_archive_rows_from_batch(&mut rows, &batch?)?;
    }
    Ok(rows)
}

/// Decode one `Parquet` record batch into archive rows.
///
/// # Errors
///
/// Returns [`ArchiveError`] when the batch does not match the archive schema or JSON fields
/// cannot be decoded.
pub fn archive_post_rows_from_record_batch(
    batch: &RecordBatch,
) -> Result<Vec<ArchivePostRow>, ArchiveError> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    append_archive_rows_from_batch(&mut rows, batch)?;
    Ok(rows)
}

/// Build a content receipt from already-normalized post rows.
///
/// # Errors
///
/// Returns [`ArchiveError`] if any counter or hash length overflows the receipt schema.
pub fn build_repo_receipt(input: RepoReceiptInput<'_>) -> Result<RepoReceipt, ArchiveError> {
    let rows = input.rows;
    let post_rows_hash = hash_post_rows(rows)?;
    let emoji_projection_rows =
        derive_emoji_projection_rows(rows).map_err(archive_error_from_derive)?;
    let emoji_projection_hash = hash_emoji_projection_rows(&emoji_projection_rows)?;
    Ok(RepoReceipt {
        fetch_method: FetchMethod::GetRepo,
        completeness_class: CompletenessClass::SnapshotComplete,
        reachable_records_count: input.reachable_records_count,
        reachable_post_records_count: input.reachable_post_records_count,
        archived_post_rows_count: u64::try_from(rows.len()).map_err(|_error| {
            ArchiveError::CountOverflow {
                field: "archived_post_rows_count",
            }
        })?,
        post_decode_error_count: input.post_decode_error_count,
        emoji_posts_count: count_emoji_posts(rows)?,
        emoji_occurrences_count: count_emoji_occurrences(rows)?,
        mst_root_cid: input.mst_root_cid,
        commit_cid: input.commit_cid,
        archive_rows_hash: post_rows_hash.clone(),
        post_rows_hash,
        emoji_projection_hash,
        profile_row_hash: input.profile_row_hash,
        normalizer: input.normalizer,
        repo_commit_signature_verified: false,
        identity_verified: false,
    })
}

/// Hash the canonical row content named in `docs/backfill-v2-design.md`.
///
/// # Errors
///
/// Returns [`ArchiveError`] if any hashed string length cannot fit the stable hash framing.
pub fn hash_post_rows(rows: &[ArchivePostRow]) -> Result<String, ArchiveError> {
    let mut hasher = Sha256::new();
    for row in rows {
        hash_post_row_into(&mut hasher, row)?;
    }
    Ok(hex::encode(hasher.finalize()))
}

fn hash_post_row_into(hasher: &mut Sha256, row: &ArchivePostRow) -> Result<(), ArchiveError> {
    hash_field(hasher, POST_COLLECTION)?;
    hash_field(hasher, &row.did)?;
    hash_field(hasher, &row.rkey)?;
    hash_field(hasher, &row.cid)?;
    hash_normalizer(hasher, &row.normalizer)?;
    hash_optional_field(hasher, row.account_status.as_deref())?;
    hash_optional_field(hasher, row.record_status.as_deref())?;
    hash_optional_field(hasher, row.public_content_label.as_deref())?;
    hash_optional_field(hasher, row.created_at_raw.as_deref())?;
    hash_optional_field(hasher, row.created_at_normalized.as_deref())?;
    hash_field(hasher, row.created_at_parse_status.as_str())?;
    hash_field(hasher, &row.text)?;
    hash_string_slice(hasher, &row.langs)?;
    hash_string_slice(hasher, &row.emoji_sequence)?;
    hash_extras_json(hasher, &row.extras_json)
}

/// Hash a profile sidecar row when Stage C extracted one.
///
/// # Errors
///
/// Returns [`ArchiveError`] if the profile row cannot be serialized without loss.
pub fn hash_profile_record(
    profile: Option<&ProfileRecord>,
) -> Result<Option<String>, ArchiveError> {
    profile.map(hash_one_profile_record).transpose()
}

fn hash_one_profile_record(profile: &ProfileRecord) -> Result<String, ArchiveError> {
    let mut hasher = Sha256::new();
    hash_field_bytes(&mut hasher, &json_bytes(&profile_sidecar_row(profile))?)?;
    Ok(hex::encode(hasher.finalize()))
}

fn hash_emoji_projection_rows(rows: &[EmojiProjectionRow]) -> Result<String, ArchiveError> {
    let mut hasher = Sha256::new();
    for row in rows {
        hash_field_bytes(&mut hasher, &json_bytes(row)?)?;
    }
    Ok(hex::encode(hasher.finalize()))
}

fn write_posts_parquet_to_writer<W>(writer: W, rows: &[ArchivePostRow]) -> Result<(), ArchiveError>
where
    W: Write + Send,
{
    let schema = archive_schema();
    let mut writer = ArrowWriter::try_new(
        writer,
        Arc::clone(&schema),
        Some(parquet_writer_properties()?),
    )?;
    for chunk in rows.chunks(PARQUET_BATCH_ROWS) {
        let batch = post_record_batch(&schema, chunk)?;
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

fn archive_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("did", DataType::Utf8, false),
        Field::new("rkey", DataType::Utf8, false),
        Field::new("cid", DataType::Utf8, false),
        Field::new("normalizer_name", DataType::Utf8, false),
        Field::new("normalizer_semver", DataType::Utf8, false),
        Field::new("normalizer_git_rev", DataType::Utf8, false),
        Field::new("normalizer_unicode_version", DataType::Utf8, false),
        Field::new("normalizer_emoji_data_version", DataType::Utf8, false),
        Field::new("account_status", DataType::Utf8, true),
        Field::new("record_status", DataType::Utf8, true),
        Field::new("public_content_label", DataType::Utf8, true),
        Field::new("created_at_raw", DataType::Utf8, true),
        Field::new("created_at_normalized", DataType::Utf8, true),
        Field::new("created_at_parse_status", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("langs_json", DataType::Utf8, false),
        Field::new("emoji_sequence_json", DataType::Utf8, false),
        Field::new("extras_json", DataType::Utf8, false),
    ]))
}

fn parquet_writer_properties() -> Result<WriterProperties, ArchiveError> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(1)
                .map_err(|error| ArchiveError::InvalidCompression(error.to_string()))?,
        ))
        .build())
}

fn post_record_batch(
    schema: &Arc<Schema>,
    rows: &[ArchivePostRow],
) -> Result<RecordBatch, ArchiveError> {
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.did.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.rkey.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.cid.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.normalizer.name.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.normalizer.semver.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.normalizer.git_rev.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|row| row.normalizer.unicode_version.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|row| row.normalizer.emoji_data_version.as_str()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.account_status.as_deref()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.record_status.as_deref()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.public_content_label.as_deref()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.created_at_raw.as_deref()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.created_at_normalized.as_deref()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.created_at_parse_status.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.text.as_str()),
            )),
            json_string_array(rows.iter().map(|row| json_string_slice(&row.langs)))?,
            json_string_array(
                rows.iter()
                    .map(|row| json_string_slice(&row.emoji_sequence)),
            )?,
            json_string_array(rows.iter().map(|row| extras_json_string(&row.extras_json)))?,
        ],
    )?)
}

fn append_archive_rows_from_batch(
    rows: &mut Vec<ArchivePostRow>,
    batch: &RecordBatch,
) -> Result<(), ArchiveError> {
    let did = string_column(batch, "did")?;
    let rkey = string_column(batch, "rkey")?;
    let cid = string_column(batch, "cid")?;
    let normalizer_name = string_column(batch, "normalizer_name")?;
    let normalizer_semver = string_column(batch, "normalizer_semver")?;
    let normalizer_git_rev = string_column(batch, "normalizer_git_rev")?;
    let normalizer_unicode_version = string_column(batch, "normalizer_unicode_version")?;
    let normalizer_emoji_data_version = string_column(batch, "normalizer_emoji_data_version")?;
    let account_status = string_column(batch, "account_status")?;
    let record_status = string_column(batch, "record_status")?;
    let public_content_label = string_column(batch, "public_content_label")?;
    let created_at_raw = string_column(batch, "created_at_raw")?;
    let created_at_normalized = string_column(batch, "created_at_normalized")?;
    let created_at_parse_status = string_column(batch, "created_at_parse_status")?;
    let text = string_column(batch, "text")?;
    let langs_json = string_column(batch, "langs_json")?;
    let emoji_sequence_json = string_column(batch, "emoji_sequence_json")?;
    let extras_json = string_column(batch, "extras_json")?;

    for row_index in 0..batch.num_rows() {
        rows.push(ArchivePostRow {
            did: required_string(did, row_index, "did")?.to_owned(),
            rkey: required_string(rkey, row_index, "rkey")?.to_owned(),
            cid: required_string(cid, row_index, "cid")?.to_owned(),
            normalizer: NormalizerVersion {
                name: required_string(normalizer_name, row_index, "normalizer_name")?.to_owned(),
                semver: required_string(normalizer_semver, row_index, "normalizer_semver")?
                    .to_owned(),
                git_rev: required_string(normalizer_git_rev, row_index, "normalizer_git_rev")?
                    .to_owned(),
                unicode_version: required_string(
                    normalizer_unicode_version,
                    row_index,
                    "normalizer_unicode_version",
                )?
                .to_owned(),
                emoji_data_version: required_string(
                    normalizer_emoji_data_version,
                    row_index,
                    "normalizer_emoji_data_version",
                )?
                .to_owned(),
            },
            account_status: optional_string(account_status, row_index),
            record_status: optional_string(record_status, row_index),
            public_content_label: optional_string(public_content_label, row_index),
            created_at_raw: optional_string(created_at_raw, row_index),
            created_at_normalized: optional_string(created_at_normalized, row_index),
            created_at_parse_status: parse_created_at_parse_status(required_string(
                created_at_parse_status,
                row_index,
                "created_at_parse_status",
            )?)?,
            text: required_string(text, row_index, "text")?.to_owned(),
            langs: serde_json::from_str(required_string(langs_json, row_index, "langs_json")?)?,
            emoji_sequence: serde_json::from_str(required_string(
                emoji_sequence_json,
                row_index,
                "emoji_sequence_json",
            )?)?,
            extras_json: serde_json::from_str(required_string(
                extras_json,
                row_index,
                "extras_json",
            )?)?,
        });
    }

    Ok(())
}

fn string_column<'a>(
    batch: &'a RecordBatch,
    column: &'static str,
) -> Result<&'a StringArray, ArchiveError> {
    let index = batch
        .schema()
        .index_of(column)
        .map_err(|_error| ArchiveError::InvalidParquetColumn { column })?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(ArchiveError::InvalidParquetColumn { column })
}

fn required_string<'a>(
    array: &'a StringArray,
    row_index: usize,
    column: &'static str,
) -> Result<&'a str, ArchiveError> {
    if array.is_null(row_index) {
        Err(ArchiveError::UnexpectedParquetNull { column })
    } else {
        Ok(array.value(row_index))
    }
}

fn optional_string(array: &StringArray, row_index: usize) -> Option<String> {
    if array.is_null(row_index) {
        None
    } else {
        Some(array.value(row_index).to_owned())
    }
}

fn parse_created_at_parse_status(value: &str) -> Result<CreatedAtParseStatus, ArchiveError> {
    match value {
        "valid" => Ok(CreatedAtParseStatus::Valid),
        "missing" => Ok(CreatedAtParseStatus::Missing),
        "invalid" => Ok(CreatedAtParseStatus::Invalid),
        "future" => Ok(CreatedAtParseStatus::Future),
        _ => Err(ArchiveError::InvalidParquetValue {
            column: "created_at_parse_status",
            value: value.to_owned(),
        }),
    }
}

fn build_commit_metadata(
    rows: &[ArchivePostRow],
    receipt: &RepoReceipt,
    commit_context: &ArchiveCommitContext,
) -> Result<Metadata, ArchiveError> {
    Ok(Metadata {
        run_id: commit_context.run_id.clone(),
        shard: commit_context.shard.clone(),
        file_sequence: commit_context.file_sequence,
        dataset: "raw_archive_posts".to_owned(),
        row_count: u64::try_from(rows.len())
            .map_err(|_error| ArchiveError::CountOverflow { field: "row_count" })?,
        min_created_at_normalized: min_created_at(rows),
        max_created_at_normalized: max_created_at(rows),
        receipt_hash: hash_serialized_json(receipt)?,
        normalizer: receipt.normalizer.clone(),
        schema_version: ARCHIVE_SCHEMA_VERSION,
    })
}

fn build_profile_sidecar_metadata(
    receipt: &RepoReceipt,
    commit_context: &ArchiveCommitContext,
) -> Result<Metadata, ArchiveError> {
    Ok(Metadata {
        run_id: commit_context.run_id.clone(),
        shard: commit_context.shard.clone(),
        file_sequence: commit_context.file_sequence,
        dataset: "raw_profile_sidecar".to_owned(),
        row_count: 1,
        min_created_at_normalized: None,
        max_created_at_normalized: None,
        receipt_hash: hash_serialized_json(receipt)?,
        normalizer: receipt.normalizer.clone(),
        schema_version: ARCHIVE_SCHEMA_VERSION,
    })
}

fn commit_profile_sidecar(
    store: &LocalStore,
    object_path: PathBuf,
    receipt_path: PathBuf,
    manifest_path: PathBuf,
    profile: &ProfileRecord,
    receipt: &RepoReceipt,
    commit_context: &ArchiveCommitContext,
) -> Result<crate::commit::Artifact, ArchiveError> {
    let request = Request {
        object_path,
        receipt_path,
        manifest_path,
        manifest_mode: ManifestMode::AppendJsonl,
        metadata: build_profile_sidecar_metadata(receipt, commit_context)?,
    };
    Ok(store.commit(&request, |file| {
        write_profile_sidecar_json_to_writer(file, profile).map_err(|error| {
            crate::commit::Error::writer(format!("write profile sidecar JSON: {error}"))
        })
    })?)
}

fn local_manifest_from_committed(
    committed: &crate::commit::Artifact,
    receipt: &RepoReceipt,
) -> LocalManifestEntry {
    LocalManifestEntry {
        run_id: committed.entry.run_id.clone(),
        shard: committed.entry.shard.clone(),
        file_sequence: committed.entry.file_sequence,
        dataset: committed.entry.dataset.clone(),
        local_path: committed.object_path.clone(),
        row_count: committed.entry.row_count,
        bytes: committed.entry.bytes,
        content_hash: committed.entry.content_hash.clone(),
        min_created_at_normalized: committed.entry.min_created_at_normalized.clone(),
        max_created_at_normalized: committed.entry.max_created_at_normalized.clone(),
        receipt_hash: committed.entry.receipt_hash.clone(),
        schema_version: committed.entry.schema_version,
        normalizer: receipt.normalizer.clone(),
    }
}

fn write_emoji_projection_jsonl(
    path: &Path,
    rows: &[EmojiProjectionRow],
) -> Result<(), ArchiveError> {
    let mut file = File::create(path)?;
    for row in rows {
        serde_json::to_writer(&mut file, row)?;
        file.write_all(b"\n")?;
    }
    file.sync_all()?;
    Ok(())
}

fn write_profile_sidecar_json_to_writer<W>(
    mut writer: W,
    profile: &ProfileRecord,
) -> Result<(), ArchiveError>
where
    W: Write,
{
    serde_json::to_writer_pretty(&mut writer, &profile_sidecar_row(profile))?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn profile_sidecar_row(profile: &ProfileRecord) -> ProfileSidecarRow<'_> {
    ProfileSidecarRow {
        rkey: &profile.rkey,
        cid: &profile.cid,
        record: &profile.record,
    }
}

fn extract_emojis(text: &str) -> Vec<String> {
    emoji_normalizer::extract_emoji_sequence(text)
}

const fn archive_error_from_derive(error: DeriveError) -> ArchiveError {
    match error {
        DeriveError::CountOverflow { field } => ArchiveError::CountOverflow { field },
        DeriveError::RowCountMismatch { .. } => ArchiveError::CountOverflow {
            field: "derive_row_count_mismatch",
        },
    }
}

fn count_emoji_posts(rows: &[ArchivePostRow]) -> Result<u64, ArchiveError> {
    u64::try_from(
        rows.iter()
            .filter(|row| !row.emoji_sequence.is_empty())
            .count(),
    )
    .map_err(|_error| ArchiveError::CountOverflow {
        field: "emoji_posts_count",
    })
}

fn count_emoji_occurrences(rows: &[ArchivePostRow]) -> Result<u64, ArchiveError> {
    rows.iter().try_fold(0_u64, |accumulator, row| {
        let row_count = u64::try_from(row.emoji_sequence.len()).map_err(|_error| {
            ArchiveError::CountOverflow {
                field: "emoji_occurrences_count",
            }
        })?;
        accumulator
            .checked_add(row_count)
            .ok_or(ArchiveError::CountOverflow {
                field: "emoji_occurrences_count",
            })
    })
}

fn json_string_array(
    values: impl Iterator<Item = Result<Cow<'static, str>, ArchiveError>>,
) -> Result<ArrayRef, ArchiveError> {
    let mut builder = StringBuilder::new();
    for value in values {
        builder.append_value(value?.as_ref());
    }
    Ok(Arc::new(builder.finish()))
}

fn json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ArchiveError> {
    Ok(serde_json::to_vec(value)?)
}

fn json_string<T: Serialize>(value: &T) -> Result<String, ArchiveError> {
    Ok(serde_json::to_string(value)?)
}

fn json_string_slice(value: &[String]) -> Result<Cow<'static, str>, ArchiveError> {
    if value.is_empty() {
        return Ok(Cow::Borrowed("[]"));
    }
    Ok(Cow::Owned(json_string(&value)?))
}

fn extras_json_string(value: &serde_json::Value) -> Result<Cow<'static, str>, ArchiveError> {
    if matches!(value, serde_json::Value::Object(fields) if fields.is_empty()) {
        return Ok(Cow::Borrowed("{}"));
    }
    Ok(Cow::Owned(canonical_json(value)?))
}

fn write_json_pretty<T: Serialize>(path: &Path, value: &T) -> Result<(), ArchiveError> {
    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn remove_if_exists(path: &Path) -> Result<(), ArchiveError> {
    if path.try_exists()? {
        fs::remove_file(path)?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArchiveFileDigest {
    bytes: u64,
    sha256: String,
}

fn hash_file_for_archive(path: &Path) -> Result<ArchiveFileDigest, ArchiveError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; 65_536].into_boxed_slice();
    loop {
        let read = std::io::Read::read(&mut file, &mut buffer)?;
        if read == 0 {
            break;
        }
        let chunk = buffer.get(..read).ok_or(ArchiveError::CountOverflow {
            field: "archive_file_hash_chunk",
        })?;
        hasher.update(chunk);
        let read_u64 = u64::try_from(read).map_err(|_error| ArchiveError::CountOverflow {
            field: "archive_file_hash_bytes",
        })?;
        bytes = bytes
            .checked_add(read_u64)
            .ok_or(ArchiveError::CountOverflow {
                field: "archive_file_hash_bytes",
            })?;
    }
    Ok(ArchiveFileDigest {
        bytes,
        sha256: hex::encode(hasher.finalize()),
    })
}

fn min_created_at(rows: &[ArchivePostRow]) -> Option<String> {
    rows.iter()
        .filter_map(|row| row.created_at_normalized.as_deref())
        .min()
        .map(ToOwned::to_owned)
}

fn update_min_max_created_at(
    min_value: &mut Option<String>,
    max_value: &mut Option<String>,
    value: Option<&str>,
) {
    let Some(value) = value else {
        return;
    };
    if min_value.as_deref().is_none_or(|current| value < current) {
        *min_value = Some(value.to_owned());
    }
    if max_value.as_deref().is_none_or(|current| value > current) {
        *max_value = Some(value.to_owned());
    }
}

fn max_created_at(rows: &[ArchivePostRow]) -> Option<String> {
    rows.iter()
        .filter_map(|row| row.created_at_normalized.as_deref())
        .max()
        .map(ToOwned::to_owned)
}

fn artifact_file_stem(value: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!(
        "{}.{}.{}",
        safe_file_component(value),
        std::process::id(),
        timestamp
    )
}

fn safe_file_component(value: &str) -> String {
    let mut safe = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            safe.push(ch);
        } else {
            safe.push('_');
        }
    }
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    let digest = hex::encode(hasher.finalize());
    safe.push_str("__");
    safe.extend(digest.chars().take(16));
    safe
}

fn hash_string_slice(hasher: &mut Sha256, values: &[String]) -> Result<(), ArchiveError> {
    for value in values {
        hash_field(hasher, value)?;
    }
    hash_field(hasher, "")
}

fn hash_optional_field(hasher: &mut Sha256, value: Option<&str>) -> Result<(), ArchiveError> {
    match value {
        Some(value) => {
            hash_field(hasher, "some")?;
            hash_field(hasher, value)
        }
        None => hash_field(hasher, "none"),
    }
}

fn hash_normalizer(
    hasher: &mut Sha256,
    normalizer: &NormalizerVersion,
) -> Result<(), ArchiveError> {
    hash_field(hasher, &normalizer.name)?;
    hash_field(hasher, &normalizer.semver)?;
    hash_field(hasher, &normalizer.git_rev)?;
    hash_field(hasher, &normalizer.unicode_version)?;
    hash_field(hasher, &normalizer.emoji_data_version)
}

fn append_normalizer_frames(
    target: &mut Vec<u8>,
    normalizer: &NormalizerVersion,
) -> Result<(), ArchiveError> {
    append_hash_field_frame(target, &normalizer.name)?;
    append_hash_field_frame(target, &normalizer.semver)?;
    append_hash_field_frame(target, &normalizer.git_rev)?;
    append_hash_field_frame(target, &normalizer.unicode_version)?;
    append_hash_field_frame(target, &normalizer.emoji_data_version)
}

fn framed_fields<const N: usize>(values: [&str; N]) -> Result<Vec<u8>, ArchiveError> {
    let mut framed = Vec::new();
    for value in values {
        append_hash_field_frame(&mut framed, value)?;
    }
    Ok(framed)
}

fn append_hash_field_frame(target: &mut Vec<u8>, value: &str) -> Result<(), ArchiveError> {
    let len = u64::try_from(value.len()).map_err(|_error| ArchiveError::CountOverflow {
        field: "hash_field_length",
    })?;
    target.extend_from_slice(&len.to_be_bytes());
    target.extend_from_slice(value.as_bytes());
    Ok(())
}

fn hash_field(hasher: &mut Sha256, value: &str) -> Result<(), ArchiveError> {
    hash_field_bytes(hasher, value.as_bytes())
}

fn hash_field_bytes(hasher: &mut Sha256, value: &[u8]) -> Result<(), ArchiveError> {
    let len = u64::try_from(value.len()).map_err(|_error| ArchiveError::CountOverflow {
        field: "hash_field_length",
    })?;
    hasher.update(len.to_be_bytes());
    hasher.update(value);
    Ok(())
}

fn hash_extras_json(hasher: &mut Sha256, value: &serde_json::Value) -> Result<(), ArchiveError> {
    if matches!(value, serde_json::Value::Object(fields) if fields.is_empty()) {
        return hash_field(hasher, "{}");
    }
    hash_field_bytes(hasher, &json_bytes(value)?)
}

fn canonical_json(value: &serde_json::Value) -> Result<String, ArchiveError> {
    Ok(serde_json::to_string(value)?)
}

fn record_extras_json(
    record: &jacquard_api::app_bsky::feed::post::Post<smol_str::SmolStr>,
) -> Result<serde_json::Value, ArchiveError> {
    let mut extras = serde_json::Map::new();
    insert_optional_json(&mut extras, "embed", record.embed.as_ref())?;
    insert_optional_json(&mut extras, "facets", record.facets.as_ref())?;
    insert_optional_json(&mut extras, "labels", record.labels.as_ref())?;
    insert_optional_json(&mut extras, "reply", record.reply.as_ref())?;
    insert_optional_json(&mut extras, "tags", record.tags.as_ref())?;
    insert_extra_data_json(&mut extras, record.extra_data.as_ref())?;
    Ok(serde_json::Value::Object(extras))
}

fn insert_optional_json<T: Serialize>(
    target: &mut serde_json::Map<String, serde_json::Value>,
    key: &'static str,
    value: Option<&T>,
) -> Result<(), ArchiveError> {
    if let Some(value) = value {
        target.insert(key.to_owned(), serde_json::to_value(value)?);
    }
    Ok(())
}

fn insert_extra_data_json<T: Serialize>(
    target: &mut serde_json::Map<String, serde_json::Value>,
    value: Option<&std::collections::BTreeMap<smol_str::SmolStr, T>>,
) -> Result<(), ArchiveError> {
    let Some(value) = value else {
        return Ok(());
    };
    for (key, extra_value) in value {
        let key = key.to_string();
        if !target.contains_key(&key) {
            target.insert(key, serde_json::to_value(extra_value)?);
        }
    }
    Ok(())
}

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
    value.map_or_else(
        || ClassifiedCreatedAt {
            raw: None,
            normalized: None,
            status: CreatedAtParseStatus::Missing,
        },
        classify_present_created_at,
    )
}

fn classify_present_created_at(raw: &str) -> ClassifiedCreatedAt {
    match DateTime::parse_from_rfc3339(raw) {
        Ok(timestamp) if timestamp.with_timezone(&Utc) > current_utc() => ClassifiedCreatedAt {
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

fn current_utc() -> DateTime<Utc> {
    Utc::now()
}

fn write_temp_rename<F>(path: &Path, write: F) -> Result<(), ArchiveError>
where
    F: FnOnce(&Path) -> Result<(), ArchiveError>,
{
    let temp_path = temp_path_for(path)?;
    if temp_path.try_exists()? {
        fs::remove_file(&temp_path)?;
    }
    match write(&temp_path) {
        Ok(()) => {
            sync_file(&temp_path)?;
            promote_temp_no_overwrite(&temp_path, path)?;
            sync_parent_dir(path)?;
            Ok(())
        }
        Err(error) => {
            let _ignored = fs::remove_file(&temp_path);
            Err(error)
        }
    }
}

fn promote_temp_no_overwrite(temp_path: &Path, path: &Path) -> Result<(), ArchiveError> {
    let temp_digest = hash_file_for_archive(temp_path)?;
    fs::hard_link(temp_path, path).map_err(|source| {
        if source.kind() == io::ErrorKind::AlreadyExists {
            ArchiveError::FinalPathExists {
                path: path.to_path_buf(),
            }
        } else {
            ArchiveError::Io(source)
        }
    })?;
    let _ignored = fs::remove_file(temp_path);
    let final_digest = hash_file_for_archive(path)?;
    if final_digest.sha256 != temp_digest.sha256 || final_digest.bytes != temp_digest.bytes {
        return Err(ArchiveError::FinalHashMismatch {
            path: path.to_path_buf(),
            expected: temp_digest.sha256,
            observed: final_digest.sha256,
        });
    }
    Ok(())
}

fn temp_path_for(path: &Path) -> Result<PathBuf, ArchiveError> {
    let file_name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| ArchiveError::InvalidPath {
            path: path.to_path_buf(),
        })?;
    Ok(path.with_file_name(format!("{file_name}.tmp.{}", std::process::id())))
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

impl From<io::Error> for ArchiveError {
    fn from(error: io::Error) -> Self {
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
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{
        ArchiveCommitContext, ArchivePostRow, CompletenessClass, CreatedAtParseStatus, FetchMethod,
        NormalizerVersion, RepoReceiptInput, StreamingArchiveSink, StreamingReceiptInput,
        build_repo_receipt, classify_created_at, extract_emojis, hash_post_rows,
    };

    fn normalizer() -> NormalizerVersion {
        NormalizerVersion {
            name: "emoji-normalizer".to_owned(),
            semver: "0.1.0".to_owned(),
            git_rev: "test".to_owned(),
            unicode_version: "16.0".to_owned(),
            emoji_data_version: "16.0".to_owned(),
        }
    }

    fn row(text: &str, emojis: &[&str]) -> ArchivePostRow {
        ArchivePostRow {
            did: "did:plc:test".to_owned(),
            rkey: "abc".to_owned(),
            cid: "bafy-test".to_owned(),
            normalizer: normalizer(),
            account_status: None,
            record_status: None,
            public_content_label: None,
            created_at_raw: Some("2026-06-15T00:00:00Z".to_owned()),
            created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
            created_at_parse_status: CreatedAtParseStatus::Valid,
            text: text.to_owned(),
            langs: vec!["en".to_owned()],
            emoji_sequence: emojis.iter().map(|emoji| (*emoji).to_owned()).collect(),
            extras_json: serde_json::json!({"facets": []}),
        }
    }

    #[test]
    fn row_hash_changes_when_content_changes() {
        let first = hash_post_rows(&[row("hello", &["✅"])]).expect("first row hash");
        let second = hash_post_rows(&[row("hello!", &["✅"])]).expect("second row hash");
        assert_ne!(first, second);
    }

    #[test]
    fn receipt_counts_posts_and_emoji_occurrences() {
        let rows = [row("a", &["✅", "✅"]), row("b", &[])];
        let receipt = build_repo_receipt(RepoReceiptInput {
            rows: &rows,
            reachable_records_count: 3,
            reachable_post_records_count: 2,
            post_decode_error_count: 1,
            profile_row_hash: Some("profile-hash".to_owned()),
            mst_root_cid: Some("root".to_owned()),
            commit_cid: Some("commit".to_owned()),
            normalizer: normalizer(),
        });
        let receipt = receipt.expect("receipt should build");
        assert_eq!(receipt.reachable_records_count, 3);
        assert_eq!(receipt.reachable_post_records_count, 2);
        assert_eq!(receipt.archived_post_rows_count, 2);
        assert_eq!(receipt.post_decode_error_count, 1);
        assert_eq!(receipt.emoji_posts_count, 1);
        assert_eq!(receipt.emoji_occurrences_count, 2);
        assert_eq!(receipt.profile_row_hash, Some("profile-hash".to_owned()));
    }

    #[test]
    fn extracts_grapheme_emoji_sequences() {
        assert_eq!(extract_emojis("hi ✅ 👩‍💻"), vec!["✅", "👩‍💻"]);
    }

    #[test]
    fn classifies_created_at_statuses() {
        let missing = classify_created_at(None);
        assert_eq!(missing.status, CreatedAtParseStatus::Missing);
        assert_eq!(missing.normalized, None);

        let invalid = classify_created_at(Some("not-a-date"));
        assert_eq!(invalid.status, CreatedAtParseStatus::Invalid);
        assert_eq!(invalid.raw, Some("not-a-date".to_owned()));
        assert_eq!(invalid.normalized, None);

        let future = classify_created_at(Some("9999-12-31T23:59:59Z"));
        assert_eq!(future.status, CreatedAtParseStatus::Future);
        assert_eq!(future.normalized, None);

        let valid = classify_created_at(Some("2020-01-02T03:04:05Z"));
        assert_eq!(valid.status, CreatedAtParseStatus::Valid);
        assert_eq!(valid.normalized, Some("2020-01-02T03:04:05Z".to_owned()));
    }

    #[test]
    fn unfinished_streaming_sink_removes_temp_files_on_drop() {
        let output_dir = unique_test_dir("streaming-sink-drop");
        fs::create_dir_all(&output_dir).expect("create test archive dir");

        let sink = StreamingArchiveSink::new(
            &output_dir,
            "did:plc:cleanup",
            ArchiveCommitContext::fetch_one_local(),
        )
        .expect("create sink");
        let parquet_temp = sink.parquet_temp_path.clone();
        let emoji_temp = sink.emoji_projection_temp_path.clone();
        assert!(parquet_temp.exists(), "{}", parquet_temp.display());
        assert!(emoji_temp.exists(), "{}", emoji_temp.display());
        drop(sink);

        assert!(!parquet_temp.exists());
        assert!(!emoji_temp.exists());
        fs::remove_dir_all(output_dir).expect("remove test archive dir");
    }

    #[test]
    fn streaming_sink_writes_committed_manifest_entry() {
        let output_dir = unique_test_dir("streaming-sink-manifest");
        fs::create_dir_all(&output_dir).expect("create test archive dir");
        let mut sink = StreamingArchiveSink::new(
            &output_dir,
            "did:plc:manifest",
            ArchiveCommitContext::new("run-test", "shard7", 42),
        )
        .expect("create sink");
        sink.push_row(row("hello ✅", &["✅"])).expect("push row");
        let (_receipt, artifacts) = sink
            .finish(
                StreamingReceiptInput {
                    fetch_method: FetchMethod::GetRepo,
                    completeness_class: CompletenessClass::SnapshotComplete,
                    reachable_records_count: 1,
                    reachable_post_records_count: 1,
                    post_decode_error_count: 0,
                    profile_row_hash: None,
                    mst_root_cid: Some("root".to_owned()),
                    commit_cid: Some("commit".to_owned()),
                },
                None,
            )
            .expect("finish sink");

        let manifest_json = fs::read_to_string(&artifacts.manifest_path).expect("read manifest");
        let entry: crate::commit::ManifestEntry =
            serde_json::from_str(&manifest_json).expect("parse committed manifest");
        let object_receipt: crate::commit::Receipt = serde_json::from_slice(
            &fs::read(&artifacts.object_receipt_path).expect("read receipt"),
        )
        .expect("parse committed receipt");

        assert!(entry.object_path.starts_with("did_plc_manifest__"));
        assert!(entry.object_path.ends_with(".posts.parquet"));
        assert_eq!(entry.run_id, "run-test");
        assert_eq!(entry.shard, "shard7");
        assert_eq!(entry.file_sequence, 42);
        assert_eq!(entry.dataset, "raw_archive_posts");
        assert_eq!(entry.row_count, 1);
        assert_eq!(object_receipt.object_path, entry.object_path);
        assert_eq!(object_receipt.content_hash, entry.content_hash);
        assert_eq!(object_receipt.receipt_hash, entry.receipt_hash);
        fs::remove_dir_all(output_dir).expect("remove test archive dir");
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "emojistats-backfill-{name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
