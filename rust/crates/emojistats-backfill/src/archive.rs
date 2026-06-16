//! Archive receipt, `Parquet`, and manifest primitives for the `fetch-one` vertical slice.

use std::{
    error::Error,
    fmt, fs,
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
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
    parse::{ParsedRepo, PostRecord, ProfileRecord},
};

const POST_COLLECTION: &str = "app.bsky.feed.post";
const ARCHIVE_SCHEMA_VERSION: u16 = 1;
const PARQUET_BATCH_ROWS: usize = 1_024;

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
    CountOverflow { field: &'static str },
    InvalidCompression(String),
    InvalidPath { path: PathBuf },
    InvalidRecordJson,
    InvalidParquetColumn { column: &'static str },
    InvalidParquetValue { column: &'static str, value: String },
    UnexpectedParquetNull { column: &'static str },
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
    let created_at = post.record.created_at.as_str();
    let classified = classify_created_at(Some(created_at));
    Ok(ArchivePostRow {
        did: did.to_owned(),
        rkey: post.rkey.clone(),
        cid: post.cid.clone(),
        normalizer: normalizer.clone(),
        account_status: None,
        record_status: None,
        public_content_label: None,
        created_at_raw: classified.raw,
        created_at_normalized: classified.normalized,
        created_at_parse_status: classified.status,
        text: post.record.text.to_string(),
        langs: post.record.langs.as_ref().map_or_else(Vec::new, |langs| {
            langs.iter().map(ToString::to_string).collect()
        }),
        emoji_sequence: extract_emojis(post.record.text.as_str()),
        extras_json: record_extras_json(post)?,
    })
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
    rows: &[ArchivePostRow],
    profile: Option<&ProfileRecord>,
    receipt: &RepoReceipt,
) -> Result<ArchiveArtifacts, ArchiveError> {
    fs::create_dir_all(output_dir)?;
    let safe_did = safe_file_component(did);
    let parquet_object_path = PathBuf::from(format!("{safe_did}.posts.parquet"));
    let receipt_path = output_dir.join(format!("{safe_did}.receipt.json"));
    let object_receipt_object_path = PathBuf::from(format!("{safe_did}.object-receipt.json"));
    let manifest_object_path = PathBuf::from(format!("{safe_did}.manifest.jsonl"));
    let emoji_projection_path = output_dir.join(format!("{safe_did}.emoji.jsonl"));
    let profile_sidecar_object_path = PathBuf::from(format!("{safe_did}.profile.json"));
    let profile_sidecar_receipt_object_path =
        PathBuf::from(format!("{safe_did}.profile.object-receipt.json"));
    let profile_sidecar_manifest_object_path =
        PathBuf::from(format!("{safe_did}.profile.manifest.jsonl"));

    write_temp_rename(&receipt_path, |path| write_json_pretty(path, receipt))?;
    let store = LocalStore::new(output_dir);
    let commit_request = Request {
        object_path: parquet_object_path,
        receipt_path: object_receipt_object_path,
        manifest_path: manifest_object_path,
        manifest_mode: ManifestMode::AppendJsonl,
        metadata: build_commit_metadata(rows, receipt)?,
    };
    let committed = store.commit(&commit_request, |file| {
        write_posts_parquet_to_writer(file, rows)
            .map_err(|error| crate::commit::Error::writer(format!("write posts parquet: {error}")))
    })?;
    let emoji_projection_rows = derive_emoji_projection_rows(rows)?;
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
    safe_did: String,
    parquet_path: PathBuf,
    parquet_temp_path: PathBuf,
    receipt_path: PathBuf,
    object_receipt_path: PathBuf,
    manifest_path: PathBuf,
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
    emoji_file: File,
}

/// Summary fields needed to finish a streaming repo receipt.
#[derive(Debug, Clone)]
pub struct StreamingReceiptInput {
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
    pub fn new(output_dir: &Path, did: &str) -> Result<Self, ArchiveError> {
        fs::create_dir_all(output_dir)?;
        let safe_did = safe_file_component(did);
        let parquet_path = output_dir.join(format!("{safe_did}.posts.parquet"));
        let parquet_temp_path = temp_path_for(&parquet_path)?;
        let receipt_path = output_dir.join(format!("{safe_did}.receipt.json"));
        let object_receipt_path = output_dir.join(format!("{safe_did}.object-receipt.json"));
        let manifest_path = output_dir.join(format!("{safe_did}.manifest.jsonl"));
        let emoji_projection_path = output_dir.join(format!("{safe_did}.emoji.jsonl"));
        let emoji_projection_temp_path = temp_path_for(&emoji_projection_path)?;
        remove_if_exists(&parquet_temp_path)?;
        remove_if_exists(&emoji_projection_temp_path)?;
        let parquet_file = File::create(&parquet_temp_path)?;
        let emoji_file = File::create(&emoji_projection_temp_path)?;
        let schema = archive_schema();
        let writer = ArrowWriter::try_new(
            parquet_file,
            Arc::clone(&schema),
            Some(parquet_writer_properties()?),
        )?;
        Ok(Self {
            output_dir: output_dir.to_path_buf(),
            safe_did,
            parquet_path,
            parquet_temp_path,
            receipt_path,
            object_receipt_path,
            manifest_path,
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
            normalizer: current_normalizer(),
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
        hash_post_row_into(&mut self.rows_hash, &row)?;
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
        for projection_row in emoji_projection_rows(&row)? {
            hash_field(
                &mut self.emoji_projection_hash,
                &json_string(&projection_row)?,
            )?;
            serde_json::to_writer(&mut self.emoji_file, &projection_row)?;
            self.emoji_file.write_all(b"\n")?;
            self.emoji_rows =
                self.emoji_rows
                    .checked_add(1)
                    .ok_or(ArchiveError::CountOverflow {
                        field: "emoji_rows",
                    })?;
        }
        self.batch.push(row);
        if self.batch.len() >= PARQUET_BATCH_ROWS {
            self.flush_batch()?;
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
        let receipt_hash = hash_serialized(&receipt)?;
        let manifest = self.write_object_receipt_and_manifest(&receipt_hash)?;
        let committed_profile = self.commit_profile(profile, &receipt)?;
        let artifacts = self.into_artifacts(manifest, committed_profile);
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
        sync_file(&self.parquet_temp_path)?;
        fs::rename(&self.parquet_temp_path, &self.parquet_path)?;
        sync_parent_dir(&self.parquet_path)?;
        self.emoji_file.sync_all()?;
        fs::rename(
            &self.emoji_projection_temp_path,
            &self.emoji_projection_path,
        )?;
        sync_parent_dir(&self.emoji_projection_path)
    }

    fn build_streaming_receipt(&self, input: StreamingReceiptInput) -> RepoReceipt {
        let post_rows_hash = hex::encode(self.rows_hash.clone().finalize());
        RepoReceipt {
            fetch_method: FetchMethod::GetRepo,
            completeness_class: CompletenessClass::SnapshotComplete,
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

    fn write_object_receipt_and_manifest(
        &self,
        receipt_hash: &str,
    ) -> Result<LocalManifestEntry, ArchiveError> {
        let parquet_digest = hash_file_for_archive(&self.parquet_path)?;
        let manifest = self.local_streaming_manifest(parquet_digest, receipt_hash);
        let object_receipt = self.streaming_object_receipt(&manifest, receipt_hash);
        write_temp_rename(&self.object_receipt_path, |path| {
            write_json_pretty(path, &object_receipt)
        })?;
        write_temp_rename(&self.manifest_path, |path| {
            let mut file = File::create(path)?;
            let entry = crate::commit::ManifestEntry {
                run_id: manifest.run_id.clone(),
                shard: manifest.shard.clone(),
                file_sequence: manifest.file_sequence,
                dataset: manifest.dataset.clone(),
                object_path: format!("{}.posts.parquet", self.safe_did),
                row_count: manifest.row_count,
                bytes: manifest.bytes,
                content_hash: manifest.content_hash.clone(),
                min_created_at_normalized: manifest.min_created_at_normalized.clone(),
                max_created_at_normalized: manifest.max_created_at_normalized.clone(),
                receipt_hash: manifest.receipt_hash.clone(),
                normalizer: manifest.normalizer.clone(),
                schema_version: manifest.schema_version,
            };
            serde_json::to_writer(&mut file, &entry)?;
            file.write_all(b"\n")?;
            Ok(())
        })?;
        Ok(manifest)
    }

    fn local_streaming_manifest(
        &self,
        parquet_digest: ArchiveFileDigest,
        receipt_hash: &str,
    ) -> LocalManifestEntry {
        LocalManifestEntry {
            run_id: "fetch-one-local".to_owned(),
            shard: "single".to_owned(),
            file_sequence: 1,
            dataset: "raw_archive_posts".to_owned(),
            local_path: self.parquet_path.clone(),
            row_count: self.archived_post_rows_count,
            bytes: parquet_digest.bytes,
            content_hash: parquet_digest.sha256,
            min_created_at_normalized: self.min_created_at_normalized.clone(),
            max_created_at_normalized: self.max_created_at_normalized.clone(),
            receipt_hash: receipt_hash.to_owned(),
            schema_version: ARCHIVE_SCHEMA_VERSION,
            normalizer: self.normalizer.clone(),
        }
    }

    fn streaming_object_receipt(
        &self,
        manifest: &LocalManifestEntry,
        receipt_hash: &str,
    ) -> crate::commit::Receipt {
        crate::commit::Receipt {
            protocol_version: 1,
            run_id: manifest.run_id.clone(),
            shard: manifest.shard.clone(),
            file_sequence: manifest.file_sequence,
            dataset: manifest.dataset.clone(),
            object_path: format!("{}.posts.parquet", self.safe_did),
            row_count: manifest.row_count,
            bytes: manifest.bytes,
            content_hash: manifest.content_hash.clone(),
            receipt_hash: receipt_hash.to_owned(),
            schema_version: manifest.schema_version,
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
                    PathBuf::from(format!("{}.profile.json", self.safe_did)),
                    PathBuf::from(format!("{}.profile.object-receipt.json", self.safe_did)),
                    PathBuf::from(format!("{}.profile.manifest.jsonl", self.safe_did)),
                    profile,
                    receipt,
                )
            })
            .transpose()
    }

    fn into_artifacts(
        self,
        manifest: LocalManifestEntry,
        committed_profile: Option<crate::commit::Artifact>,
    ) -> ArchiveArtifacts {
        ArchiveArtifacts {
            parquet_path: self.parquet_path.clone(),
            receipt_path: self.receipt_path.clone(),
            object_receipt_path: self.object_receipt_path.clone(),
            manifest_path: self.manifest_path.clone(),
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
    let emoji_projection_rows = derive_emoji_projection_rows(rows)?;
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
    hash_field(hasher, &canonical_json(&row.extras_json)?)
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
    hash_field(&mut hasher, &json_string(&profile_sidecar_row(profile))?)?;
    Ok(hex::encode(hasher.finalize()))
}

fn hash_emoji_projection_rows(rows: &[EmojiProjectionRow]) -> Result<String, ArchiveError> {
    let mut hasher = Sha256::new();
    for row in rows {
        hash_field(&mut hasher, &json_string(row)?)?;
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
            ZstdLevel::try_new(3)
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
            string_array(rows.iter().map(|row| Some(row.did.as_str()))),
            string_array(rows.iter().map(|row| Some(row.rkey.as_str()))),
            string_array(rows.iter().map(|row| Some(row.cid.as_str()))),
            string_array(rows.iter().map(|row| Some(row.normalizer.name.as_str()))),
            string_array(rows.iter().map(|row| Some(row.normalizer.semver.as_str()))),
            string_array(rows.iter().map(|row| Some(row.normalizer.git_rev.as_str()))),
            string_array(
                rows.iter()
                    .map(|row| Some(row.normalizer.unicode_version.as_str())),
            ),
            string_array(
                rows.iter()
                    .map(|row| Some(row.normalizer.emoji_data_version.as_str())),
            ),
            string_array(rows.iter().map(|row| row.account_status.as_deref())),
            string_array(rows.iter().map(|row| row.record_status.as_deref())),
            string_array(rows.iter().map(|row| row.public_content_label.as_deref())),
            string_array(rows.iter().map(|row| row.created_at_raw.as_deref())),
            string_array(rows.iter().map(|row| row.created_at_normalized.as_deref())),
            string_array(
                rows.iter()
                    .map(|row| Some(row.created_at_parse_status.as_str())),
            ),
            string_array(rows.iter().map(|row| Some(row.text.as_str()))),
            owned_string_array(rows.iter().map(|row| json_string(&row.langs)))?,
            owned_string_array(rows.iter().map(|row| json_string(&row.emoji_sequence)))?,
            owned_string_array(rows.iter().map(|row| canonical_json(&row.extras_json)))?,
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
) -> Result<Metadata, ArchiveError> {
    Ok(Metadata {
        run_id: "fetch-one-local".to_owned(),
        shard: "single".to_owned(),
        file_sequence: 1,
        dataset: "raw_archive_posts".to_owned(),
        row_count: u64::try_from(rows.len())
            .map_err(|_error| ArchiveError::CountOverflow { field: "row_count" })?,
        min_created_at_normalized: min_created_at(rows),
        max_created_at_normalized: max_created_at(rows),
        receipt_hash: hash_serialized(receipt)?,
        normalizer: receipt.normalizer.clone(),
        schema_version: ARCHIVE_SCHEMA_VERSION,
    })
}

fn build_profile_sidecar_metadata(receipt: &RepoReceipt) -> Result<Metadata, ArchiveError> {
    Ok(Metadata {
        run_id: "fetch-one-local".to_owned(),
        shard: "single".to_owned(),
        file_sequence: 1,
        dataset: "raw_profile_sidecar".to_owned(),
        row_count: 1,
        min_created_at_normalized: None,
        max_created_at_normalized: None,
        receipt_hash: hash_serialized(receipt)?,
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
) -> Result<crate::commit::Artifact, ArchiveError> {
    let request = Request {
        object_path,
        receipt_path,
        manifest_path,
        manifest_mode: ManifestMode::AppendJsonl,
        metadata: build_profile_sidecar_metadata(receipt)?,
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

fn derive_emoji_projection_rows(
    rows: &[ArchivePostRow],
) -> Result<Vec<EmojiProjectionRow>, ArchiveError> {
    let mut projected = Vec::new();
    for row in rows {
        projected.extend(emoji_projection_rows(row)?);
    }
    Ok(projected)
}

fn emoji_projection_rows(row: &ArchivePostRow) -> Result<Vec<EmojiProjectionRow>, ArchiveError> {
    let mut rows = Vec::new();
    for emoji in &row.emoji_sequence {
        if let Some(existing) = rows
            .iter_mut()
            .find(|candidate: &&mut EmojiProjectionRow| candidate.emoji == *emoji)
        {
            existing.occurrences =
                existing
                    .occurrences
                    .checked_add(1)
                    .ok_or(ArchiveError::CountOverflow {
                        field: "emoji_occurrences",
                    })?;
        } else {
            rows.push(EmojiProjectionRow {
                did: row.did.clone(),
                rkey: row.rkey.clone(),
                created_at_normalized: row.created_at_normalized.clone(),
                emoji: emoji.clone(),
                occurrences: 1,
                langs: row.langs.clone(),
            });
        }
    }
    Ok(rows)
}

fn extract_emojis(text: &str) -> Vec<String> {
    emoji_normalizer::extract_emoji_sequence(text)
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

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from(values.collect::<Vec<_>>()))
}

fn owned_string_array(
    values: impl Iterator<Item = Result<String, ArchiveError>>,
) -> Result<ArrayRef, ArchiveError> {
    Ok(Arc::new(StringArray::from(
        values.collect::<Result<Vec<_>, _>>()?,
    )))
}

fn json_string<T: Serialize>(value: &T) -> Result<String, ArchiveError> {
    Ok(serde_json::to_string(value)?)
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

fn hash_serialized<T: Serialize>(value: &T) -> Result<String, ArchiveError> {
    let bytes = serde_json::to_vec(value)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(hex::encode(hasher.finalize()))
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

fn safe_file_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
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

fn hash_field(hasher: &mut Sha256, value: &str) -> Result<(), ArchiveError> {
    let len = u64::try_from(value.len()).map_err(|_error| ArchiveError::CountOverflow {
        field: "hash_field_length",
    })?;
    hasher.update(len.to_be_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

fn canonical_json(value: &serde_json::Value) -> Result<String, ArchiveError> {
    Ok(serde_json::to_string(value)?)
}

fn record_extras_json(post: &PostRecord) -> Result<serde_json::Value, ArchiveError> {
    let mut extras = serde_json::Map::new();
    insert_optional_json(&mut extras, "embed", post.record.embed.as_ref())?;
    insert_optional_json(&mut extras, "facets", post.record.facets.as_ref())?;
    insert_optional_json(&mut extras, "labels", post.record.labels.as_ref())?;
    insert_optional_json(&mut extras, "reply", post.record.reply.as_ref())?;
    insert_optional_json(&mut extras, "tags", post.record.tags.as_ref())?;
    insert_optional_json(&mut extras, "extra_data", post.record.extra_data.as_ref())?;
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
    let timestamp = parse_rfc3339_epoch_seconds(raw);
    let now = current_epoch_seconds();
    let status = match (timestamp, now) {
        (Some(timestamp), Some(now)) if timestamp > now => CreatedAtParseStatus::Future,
        (Some(_timestamp), _now) => CreatedAtParseStatus::Valid,
        (None, _now) => CreatedAtParseStatus::Invalid,
    };
    let normalized = if status == CreatedAtParseStatus::Valid {
        Some(raw.to_owned())
    } else {
        None
    };
    ClassifiedCreatedAt {
        raw: Some(raw.to_owned()),
        normalized,
        status,
    }
}

fn current_epoch_seconds() -> Option<i64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    i64::try_from(duration.as_secs()).ok()
}

fn parse_rfc3339_epoch_seconds(value: &str) -> Option<i64> {
    let mut chars = value.chars();
    let year = read_digits(&mut chars, 4)?;
    expect_char(&mut chars, '-')?;
    let month = read_digits(&mut chars, 2)?;
    expect_char(&mut chars, '-')?;
    let day = read_digits(&mut chars, 2)?;
    expect_char(&mut chars, 'T')?;
    let hour = read_digits(&mut chars, 2)?;
    expect_char(&mut chars, ':')?;
    let minute = read_digits(&mut chars, 2)?;
    expect_char(&mut chars, ':')?;
    let second = read_digits(&mut chars, 2)?;
    let timezone = read_timezone(&mut chars)?;
    validate_datetime_parts(year, month, day, hour, minute, second)?;
    let days = days_from_civil(year, month, day)?;
    let day_seconds = days.checked_mul(86_400)?;
    let hour_seconds = hour.checked_mul(3_600)?;
    let minute_seconds = minute.checked_mul(60)?;
    day_seconds
        .checked_add(hour_seconds)?
        .checked_add(minute_seconds)?
        .checked_add(second)?
        .checked_sub(timezone)
}

fn read_timezone(chars: &mut std::str::Chars<'_>) -> Option<i64> {
    let next = chars.next()?;
    match next {
        '.' => {
            read_fraction(chars)?;
            read_timezone(chars)
        }
        'Z' => {
            ensure_finished(chars)?;
            Some(0)
        }
        '+' | '-' => {
            let sign = if next == '+' { 1_i64 } else { -1_i64 };
            let hour = read_digits(chars, 2)?;
            expect_char(chars, ':')?;
            let minute = read_digits(chars, 2)?;
            ensure_finished(chars)?;
            validate_timezone(hour, minute)?;
            hour.checked_mul(3_600)?
                .checked_add(minute.checked_mul(60)?)?
                .checked_mul(sign)
        }
        _other => None,
    }
}

fn read_fraction(chars: &mut std::str::Chars<'_>) -> Option<()> {
    let mut saw_digit = false;
    loop {
        let mut clone = chars.clone();
        match clone.next() {
            Some(ch) if ch.is_ascii_digit() => {
                saw_digit = true;
                let _discarded = chars.next();
            }
            Some('Z' | '+' | '-') if saw_digit => return Some(()),
            _other => return None,
        }
    }
}

fn read_digits(chars: &mut std::str::Chars<'_>, count: usize) -> Option<i64> {
    let mut value = 0_i64;
    for _ in 0..count {
        let digit = chars.next()?.to_digit(10)?;
        value = value.checked_mul(10)?.checked_add(i64::from(digit))?;
    }
    Some(value)
}

fn expect_char(chars: &mut std::str::Chars<'_>, expected: char) -> Option<()> {
    (chars.next()? == expected).then_some(())
}

fn ensure_finished(chars: &mut std::str::Chars<'_>) -> Option<()> {
    chars.next().is_none().then_some(())
}

fn validate_datetime_parts(
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
) -> Option<()> {
    if !(1..=9999).contains(&year) {
        return None;
    }
    if !(1..=12).contains(&month) {
        return None;
    }
    if !(1..=days_in_month(year, month)?).contains(&day) {
        return None;
    }
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=60).contains(&second) {
        return None;
    }
    Some(())
}

fn validate_timezone(hour: i64, minute: i64) -> Option<()> {
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) {
        return None;
    }
    Some(())
}

fn days_in_month(year: i64, month: i64) -> Option<i64> {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => Some(31),
        4 | 6 | 9 | 11 => Some(30),
        2 if is_leap_year(year)? => Some(29),
        2 => Some(28),
        _other => None,
    }
}

fn is_leap_year(year: i64) -> Option<bool> {
    let by_four = year.checked_rem(4)? == 0;
    let by_hundred = year.checked_rem(100)? == 0;
    let by_four_hundred = year.checked_rem(400)? == 0;
    Some(by_four && (!by_hundred || by_four_hundred))
}

fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
    let adjusted_year = if month <= 2 {
        year.checked_sub(1)?
    } else {
        year
    };
    let era = adjusted_year.div_euclid(400);
    let era_years = era.checked_mul(400)?;
    let year_of_era = adjusted_year.checked_sub(era_years)?;
    let month_prime = if month > 2 {
        month.checked_sub(3)?
    } else {
        month.checked_add(9)?
    };
    let day_of_year = 153_i64
        .checked_mul(month_prime)?
        .checked_add(2)?
        .div_euclid(5)
        .checked_add(day)?
        .checked_sub(1)?;
    let year_days = year_of_era.checked_mul(365)?;
    let leap_days = year_of_era
        .div_euclid(4)
        .checked_sub(year_of_era.div_euclid(100))?;
    let day_of_era = year_days.checked_add(leap_days)?.checked_add(day_of_year)?;
    era.checked_mul(146_097)?
        .checked_add(day_of_era)?
        .checked_sub(719_468)
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
            fs::rename(&temp_path, path)?;
            sync_parent_dir(path)?;
            Ok(())
        }
        Err(error) => {
            let _ignored = fs::remove_file(&temp_path);
            Err(error)
        }
    }
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
            | Self::UnexpectedParquetNull { .. } => None,
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
        ArchivePostRow, CreatedAtParseStatus, NormalizerVersion, RepoReceiptInput,
        StreamingArchiveSink, StreamingReceiptInput, build_repo_receipt, classify_created_at,
        extract_emojis, hash_post_rows, temp_path_for,
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
        let parquet_temp =
            temp_path_for(&output_dir.join("did_plc_cleanup.posts.parquet")).expect("temp path");
        let emoji_temp =
            temp_path_for(&output_dir.join("did_plc_cleanup.emoji.jsonl")).expect("temp path");

        let sink = StreamingArchiveSink::new(&output_dir, "did:plc:cleanup").expect("create sink");
        assert!(parquet_temp.exists());
        assert!(emoji_temp.exists());
        drop(sink);

        assert!(!parquet_temp.exists());
        assert!(!emoji_temp.exists());
        fs::remove_dir_all(output_dir).expect("remove test archive dir");
    }

    #[test]
    fn streaming_sink_writes_committed_manifest_entry() {
        let output_dir = unique_test_dir("streaming-sink-manifest");
        fs::create_dir_all(&output_dir).expect("create test archive dir");
        let mut sink =
            StreamingArchiveSink::new(&output_dir, "did:plc:manifest").expect("create sink");
        sink.push_row(row("hello ✅", &["✅"])).expect("push row");
        let (_receipt, artifacts) = sink
            .finish(
                StreamingReceiptInput {
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

        assert_eq!(entry.object_path, "did_plc_manifest.posts.parquet");
        assert_eq!(entry.dataset, "raw_archive_posts");
        assert_eq!(entry.row_count, 1);
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
