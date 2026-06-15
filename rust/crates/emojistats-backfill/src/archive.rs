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

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use unicode_segmentation::UnicodeSegmentation;

use crate::{
    commit::{LocalStore, ManifestMode, Metadata, Request},
    parse::{ParsedRepo, PostRecord, ProfileRecord},
};

const POST_COLLECTION: &str = "app.bsky.feed.post";
const ARCHIVE_SCHEMA_VERSION: u16 = 1;
const PARQUET_BATCH_ROWS: usize = 1_024;

/// Version identity for emoji normalization outputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizerVersion {
    pub name: String,
    pub semver: String,
    pub git_rev: String,
    pub unicode_version: String,
    pub emoji_data_version: String,
}

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
        .map(|post| {
            let created_at = post.record.created_at.as_str();
            let classified = classify_created_at(Some(created_at));
            Ok(ArchivePostRow {
                did: parsed.commit.did.clone(),
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
        })
        .collect()
}

/// Current vertical-slice normalizer identity.
#[must_use]
pub fn current_normalizer() -> NormalizerVersion {
    NormalizerVersion {
        name: "emoji-normalizer-rust-minimal".to_owned(),
        semver: "0.1.0".to_owned(),
        git_rev: option_env!("GIT_REV").unwrap_or("unknown").to_owned(),
        unicode_version: "emoji-rs".to_owned(),
        emoji_data_version: "emoji-rs".to_owned(),
    }
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
    let profile_sidecar_path =
        profile.map(|_profile| output_dir.join(format!("{safe_did}.profile.json")));

    write_temp_rename(&receipt_path, |path| write_json_pretty(path, receipt))?;
    let commit_request = Request {
        object_path: parquet_object_path,
        receipt_path: object_receipt_object_path,
        manifest_path: manifest_object_path,
        manifest_mode: ManifestMode::AppendJsonl,
        metadata: build_commit_metadata(rows, receipt)?,
    };
    let committed = LocalStore::new(output_dir).commit(&commit_request, |file| {
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
    if let (Some(path), Some(profile)) = (&profile_sidecar_path, profile) {
        write_temp_rename(path, |path| write_profile_sidecar_json(path, profile))?;
    }

    let manifest = local_manifest_from_committed(&committed, receipt);

    Ok(ArchiveArtifacts {
        parquet_path: committed.object_path,
        receipt_path,
        object_receipt_path: committed.receipt_path,
        manifest_path: committed.manifest_path,
        emoji_projection_path,
        profile_sidecar_path,
        manifest,
        emoji_rows,
    })
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
        hash_field(&mut hasher, POST_COLLECTION)?;
        hash_field(&mut hasher, &row.did)?;
        hash_field(&mut hasher, &row.rkey)?;
        hash_field(&mut hasher, &row.cid)?;
        hash_normalizer(&mut hasher, &row.normalizer)?;
        hash_optional_field(&mut hasher, row.account_status.as_deref())?;
        hash_optional_field(&mut hasher, row.record_status.as_deref())?;
        hash_optional_field(&mut hasher, row.public_content_label.as_deref())?;
        hash_optional_field(&mut hasher, row.created_at_raw.as_deref())?;
        hash_optional_field(&mut hasher, row.created_at_normalized.as_deref())?;
        hash_field(&mut hasher, row.created_at_parse_status.as_str())?;
        hash_field(&mut hasher, &row.text)?;
        hash_string_slice(&mut hasher, &row.langs)?;
        hash_string_slice(&mut hasher, &row.emoji_sequence)?;
        hash_field(&mut hasher, &canonical_json(&row.extras_json)?)?;
    }
    Ok(hex::encode(hasher.finalize()))
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
    let schema = Arc::new(Schema::new(vec![
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
    ]));

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3)
                .map_err(|error| ArchiveError::InvalidCompression(error.to_string()))?,
        ))
        .build();
    let mut writer = ArrowWriter::try_new(writer, Arc::clone(&schema), Some(props))?;
    for chunk in rows.chunks(PARQUET_BATCH_ROWS) {
        let batch = post_record_batch(&schema, chunk)?;
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
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

fn write_profile_sidecar_json(path: &Path, profile: &ProfileRecord) -> Result<(), ArchiveError> {
    write_json_pretty(path, &profile_sidecar_row(profile))
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
    text.graphemes(true)
        .filter(|grapheme| emojis::get(grapheme).is_some())
        .map(ToOwned::to_owned)
        .collect()
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
            | Self::InvalidRecordJson => None,
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
    use super::{
        ArchivePostRow, CreatedAtParseStatus, NormalizerVersion, RepoReceiptInput,
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
}
