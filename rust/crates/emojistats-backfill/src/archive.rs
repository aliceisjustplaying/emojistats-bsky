//! Archive receipt, `Parquet`, and manifest primitives for the `fetch-one` vertical slice.

use std::{
    error::Error,
    fmt, fs,
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
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

use crate::parse::{ParsedRepo, PostRecord};

const POST_COLLECTION: &str = "app.bsky.feed.post";
const ARCHIVE_SCHEMA_VERSION: u16 = 1;

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
    pub all_records_count: u64,
    pub all_posts_count: u64,
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
}

/// Files produced by Stage D for one `fetch-one` run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveArtifacts {
    pub parquet_path: PathBuf,
    pub receipt_path: PathBuf,
    pub manifest_path: PathBuf,
    pub emoji_projection_path: PathBuf,
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
    CountOverflow { field: &'static str },
    InvalidCompression(String),
}

/// Convert parsed post records into the first archive-row shape.
#[must_use]
pub fn archive_rows_from_parsed_repo(parsed: &ParsedRepo) -> Vec<ArchivePostRow> {
    parsed
        .posts
        .iter()
        .map(|post| {
            let created_at = post.record.created_at.as_str().to_owned();
            ArchivePostRow {
                did: parsed.commit.did.clone(),
                rkey: post.rkey.clone(),
                cid: post.cid.clone(),
                created_at_raw: Some(created_at.clone()),
                created_at_normalized: Some(created_at),
                created_at_parse_status: CreatedAtParseStatus::Valid,
                text: post.record.text.to_string(),
                langs: post.record.langs.as_ref().map_or_else(Vec::new, |langs| {
                    langs.iter().map(ToString::to_string).collect()
                }),
                emoji_sequence: extract_emojis(post.record.text.as_str()),
                extras_json: record_json(post),
            }
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
    receipt: &RepoReceipt,
) -> Result<ArchiveArtifacts, ArchiveError> {
    fs::create_dir_all(output_dir)?;
    let safe_did = safe_file_component(did);
    let parquet_path = output_dir.join(format!("{safe_did}.posts.parquet"));
    let receipt_path = output_dir.join(format!("{safe_did}.receipt.json"));
    let manifest_path = output_dir.join(format!("{safe_did}.manifest.json"));
    let emoji_projection_path = output_dir.join(format!("{safe_did}.emoji.jsonl"));

    write_posts_parquet(&parquet_path, rows)?;
    write_json_pretty(&receipt_path, receipt)?;
    let emoji_rows = write_emoji_projection_jsonl(&emoji_projection_path, rows)?;

    let manifest = build_manifest(&parquet_path, rows, receipt)?;
    write_json_pretty(&manifest_path, &manifest)?;

    Ok(ArchiveArtifacts {
        parquet_path,
        receipt_path,
        manifest_path,
        emoji_projection_path,
        manifest,
        emoji_rows,
    })
}

/// Build a content receipt from already-normalized post rows.
#[must_use]
pub fn build_repo_receipt(
    rows: &[ArchivePostRow],
    all_records_count: u64,
    mst_root_cid: Option<String>,
    commit_cid: Option<String>,
    normalizer: NormalizerVersion,
) -> RepoReceipt {
    let post_rows_hash = hash_post_rows(rows);
    let emoji_projection_hash = hash_emoji_projection(rows);
    RepoReceipt {
        fetch_method: FetchMethod::GetRepo,
        completeness_class: CompletenessClass::SnapshotComplete,
        all_records_count,
        all_posts_count: u64::try_from(rows.len()).unwrap_or(u64::MAX),
        emoji_posts_count: count_emoji_posts(rows),
        emoji_occurrences_count: count_emoji_occurrences(rows),
        mst_root_cid,
        commit_cid,
        archive_rows_hash: post_rows_hash.clone(),
        post_rows_hash,
        emoji_projection_hash,
        profile_row_hash: None,
        normalizer,
        repo_commit_signature_verified: false,
        identity_verified: false,
    }
}

/// Hash the canonical row content named in `docs/backfill-v2-design.md`.
#[must_use]
pub fn hash_post_rows(rows: &[ArchivePostRow]) -> String {
    let mut hasher = Sha256::new();
    for row in rows {
        hash_field(&mut hasher, POST_COLLECTION);
        hash_field(&mut hasher, &row.did);
        hash_field(&mut hasher, &row.rkey);
        hash_field(&mut hasher, &row.cid);
        hash_optional_field(&mut hasher, row.created_at_raw.as_deref());
        hash_optional_field(&mut hasher, row.created_at_normalized.as_deref());
        hash_field(&mut hasher, row.created_at_parse_status.as_str());
        hash_field(&mut hasher, &row.text);
        hash_string_slice(&mut hasher, &row.langs);
        hash_string_slice(&mut hasher, &row.emoji_sequence);
        hash_field(&mut hasher, &canonical_json(&row.extras_json));
    }
    hex::encode(hasher.finalize())
}

fn hash_emoji_projection(rows: &[ArchivePostRow]) -> String {
    let mut hasher = Sha256::new();
    for row in rows {
        for emoji in &row.emoji_sequence {
            hash_field(&mut hasher, &row.did);
            hash_field(&mut hasher, &row.rkey);
            hash_field(&mut hasher, emoji);
        }
    }
    hex::encode(hasher.finalize())
}

fn write_posts_parquet(path: &Path, rows: &[ArchivePostRow]) -> Result<(), ArchiveError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("did", DataType::Utf8, false),
        Field::new("rkey", DataType::Utf8, false),
        Field::new("cid", DataType::Utf8, false),
        Field::new("created_at_raw", DataType::Utf8, true),
        Field::new("created_at_normalized", DataType::Utf8, true),
        Field::new("created_at_parse_status", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("langs_json", DataType::Utf8, false),
        Field::new("emoji_sequence_json", DataType::Utf8, false),
        Field::new("extras_json", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            string_array(rows.iter().map(|row| Some(row.did.as_str()))),
            string_array(rows.iter().map(|row| Some(row.rkey.as_str()))),
            string_array(rows.iter().map(|row| Some(row.cid.as_str()))),
            string_array(rows.iter().map(|row| row.created_at_raw.as_deref())),
            string_array(rows.iter().map(|row| row.created_at_normalized.as_deref())),
            string_array(
                rows.iter()
                    .map(|row| Some(row.created_at_parse_status.as_str())),
            ),
            string_array(rows.iter().map(|row| Some(row.text.as_str()))),
            owned_string_array(rows.iter().map(|row| json_string(&row.langs))),
            owned_string_array(rows.iter().map(|row| json_string(&row.emoji_sequence))),
            owned_string_array(rows.iter().map(|row| canonical_json(&row.extras_json))),
        ],
    )?;

    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3)
                .map_err(|error| ArchiveError::InvalidCompression(error.to_string()))?,
        ))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn build_manifest(
    parquet_path: &Path,
    rows: &[ArchivePostRow],
    receipt: &RepoReceipt,
) -> Result<LocalManifestEntry, ArchiveError> {
    let metadata = fs::metadata(parquet_path)?;
    Ok(LocalManifestEntry {
        run_id: "fetch-one-local".to_owned(),
        shard: "single".to_owned(),
        file_sequence: 1,
        dataset: "raw_archive_posts".to_owned(),
        local_path: parquet_path.to_path_buf(),
        row_count: u64::try_from(rows.len())
            .map_err(|_error| ArchiveError::CountOverflow { field: "row_count" })?,
        bytes: metadata.len(),
        content_hash: hash_file(parquet_path)?,
        min_created_at_normalized: min_created_at(rows),
        max_created_at_normalized: max_created_at(rows),
        receipt_hash: hash_serialized(receipt)?,
        schema_version: ARCHIVE_SCHEMA_VERSION,
    })
}

fn write_emoji_projection_jsonl(path: &Path, rows: &[ArchivePostRow]) -> Result<u64, ArchiveError> {
    let mut file = File::create(path)?;
    let mut count = 0_u64;
    for row in rows {
        for projection in emoji_projection_rows(row) {
            serde_json::to_writer(&mut file, &projection)?;
            file.write_all(b"\n")?;
            count = count.checked_add(1).ok_or(ArchiveError::CountOverflow {
                field: "emoji_rows",
            })?;
        }
    }
    file.sync_all()?;
    Ok(count)
}

fn emoji_projection_rows(row: &ArchivePostRow) -> Vec<EmojiProjectionRow> {
    let mut rows = Vec::new();
    for emoji in &row.emoji_sequence {
        if let Some(existing) = rows
            .iter_mut()
            .find(|candidate: &&mut EmojiProjectionRow| candidate.emoji == *emoji)
        {
            existing.occurrences = existing.occurrences.saturating_add(1);
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
    rows
}

fn extract_emojis(text: &str) -> Vec<String> {
    text.graphemes(true)
        .filter(|grapheme| emojis::get(grapheme).is_some())
        .map(ToOwned::to_owned)
        .collect()
}

fn count_emoji_posts(rows: &[ArchivePostRow]) -> u64 {
    rows.iter()
        .filter(|row| !row.emoji_sequence.is_empty())
        .count()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn count_emoji_occurrences(rows: &[ArchivePostRow]) -> u64 {
    rows.iter().fold(0_u64, |accumulator, row| {
        let row_count = u64::try_from(row.emoji_sequence.len()).unwrap_or(u64::MAX);
        accumulator.saturating_add(row_count)
    })
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from(values.collect::<Vec<_>>()))
}

fn owned_string_array(values: impl Iterator<Item = String>) -> ArrayRef {
    Arc::new(StringArray::from(values.collect::<Vec<_>>()))
}

fn json_string<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_else(|error| {
        serde_json::json!({
            "serialization_error": error.to_string(),
        })
        .to_string()
    })
}

fn write_json_pretty<T: Serialize>(path: &Path, value: &T) -> Result<(), ArchiveError> {
    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn hash_file(path: &Path) -> Result<String, ArchiveError> {
    let bytes = fs::read(path)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(hex::encode(hasher.finalize()))
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

fn hash_string_slice(hasher: &mut Sha256, values: &[String]) {
    for value in values {
        hash_field(hasher, value);
    }
    hash_field(hasher, "");
}

fn hash_optional_field(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            hash_field(hasher, "some");
            hash_field(hasher, value);
        }
        None => hash_field(hasher, "none"),
    }
}

fn hash_field(hasher: &mut Sha256, value: &str) {
    let len = u64::try_from(value.len()).unwrap_or(u64::MAX);
    hasher.update(len.to_be_bytes());
    hasher.update(value.as_bytes());
}

fn canonical_json(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|error| {
        serde_json::json!({
            "serialization_error": error.to_string(),
        })
        .to_string()
    })
}

fn record_json(post: &PostRecord) -> serde_json::Value {
    match serde_json::to_value(&post.record) {
        Ok(value) => value,
        Err(error) => serde_json::json!({
            "serialization_error": error.to_string(),
        }),
    }
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
            Self::CountOverflow { field } => write!(f, "count overflow for {field}"),
            Self::InvalidCompression(error) => write!(f, "invalid compression level: {error}"),
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
            Self::CountOverflow { .. } | Self::InvalidCompression(_) => None,
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

#[cfg(test)]
mod tests {
    use super::{
        ArchivePostRow, CreatedAtParseStatus, NormalizerVersion, build_repo_receipt,
        extract_emojis, hash_post_rows,
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
        let first = hash_post_rows(&[row("hello", &["✅"])]);
        let second = hash_post_rows(&[row("hello!", &["✅"])]);
        assert_ne!(first, second);
    }

    #[test]
    fn receipt_counts_posts_and_emoji_occurrences() {
        let receipt = build_repo_receipt(
            &[row("a", &["✅", "✅"]), row("b", &[])],
            3,
            Some("root".to_owned()),
            Some("commit".to_owned()),
            normalizer(),
        );
        assert_eq!(receipt.all_records_count, 3);
        assert_eq!(receipt.all_posts_count, 2);
        assert_eq!(receipt.emoji_posts_count, 1);
        assert_eq!(receipt.emoji_occurrences_count, 2);
    }

    #[test]
    fn extracts_grapheme_emoji_sequences() {
        assert_eq!(extract_emojis("hi ✅ 👩‍💻"), vec!["✅", "👩‍💻"]);
    }
}
