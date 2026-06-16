//! Foundations for deriving load inputs from committed archive manifests.

use std::{
    fs::{self, File},
    io::{self, BufRead},
    path::{Component, Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::{
    archive::{
        ArchiveError, LocalManifestEntry, RepoReceipt, hash_post_rows, read_archive_post_rows,
    },
    commit::{ManifestEntry, Receipt},
    derive::{
        ClickHouseDeriveBatch, DeriveBatchInput, DeriveError, DeriveManifestIdentity,
        derive_clickhouse_batch, manifest_identity,
    },
    hash::hash_serialized_json,
};

const RAW_ARCHIVE_POSTS_DATASET: &str = "raw_archive_posts";
const RAW_ARCHIVE_POSTS_SCHEMA_VERSION: u16 = 1;
const DEFAULT_MAX_FULL_DERIVE_ROWS: u64 = 50_000;
const DEFAULT_MAX_FULL_DERIVE_BYTES: u64 = 536_870_912;

/// A committed raw-archive manifest prepared for the derive loader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoaderInput {
    pub manifest: LocalManifestEntry,
    pub identity: DeriveManifestIdentity,
}

/// Verified committed archive input ready for streaming derive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedLoaderInput {
    pub manifest: LocalManifestEntry,
    pub identity: DeriveManifestIdentity,
    pub object_path: PathBuf,
    pub repo_receipt: Option<RepoReceipt>,
}

/// Explicit caps for helpers that materialize a whole archive object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FullLoadCaps {
    pub max_rows: u64,
    pub max_bytes: u64,
}

impl Default for FullLoadCaps {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_MAX_FULL_DERIVE_ROWS,
            max_bytes: DEFAULT_MAX_FULL_DERIVE_BYTES,
        }
    }
}

/// Result of reading a mixed committed manifest stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub inputs: Vec<LoaderInput>,
    pub skipped_entries: Vec<SkippedEntry>,
}

/// A well-formed committed manifest row that is not a raw archive post object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedEntry {
    pub line_number: usize,
    pub dataset: String,
    pub object_path: String,
}

/// Failure while reading derive inputs from a committed manifest.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("read committed manifest line {line_number}: {source}")]
    Io {
        line_number: usize,
        #[source]
        source: io::Error,
    },
    #[error("parse committed manifest line {line_number}: {source}")]
    Json {
        line_number: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("line number overflow while reading committed manifest")]
    LineNumberOverflow,
    #[error("committed manifest line {line_number} has an empty {field}")]
    EmptyField {
        line_number: usize,
        field: &'static str,
    },
    #[error(
        "committed raw archive manifest line {line_number} has schema_version {actual}, expected {expected}"
    )]
    UnsupportedSchemaVersion {
        line_number: usize,
        actual: u16,
        expected: u16,
    },
    #[error(
        "committed manifest line {line_number} object_path escapes archive root: {object_path}"
    )]
    ObjectPathEscapesRoot {
        line_number: usize,
        object_path: String,
    },
    #[error("manifest local_path escapes archive root: {}", path.display())]
    LocalPathEscapesRoot { path: PathBuf },
    #[error("committed artifact is missing: {}", path.display())]
    MissingArtifact { path: PathBuf },
    #[error("hash {} failed for {}: {source}", operation, path.display())]
    HashIo {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("byte count overflow while hashing {}", path.display())]
    ByteCountOverflow { path: PathBuf },
    #[error(
        "committed artifact byte mismatch for {}: expected {expected}, actual {actual}",
        path.display()
    )]
    ByteMismatch {
        path: PathBuf,
        expected: u64,
        actual: u64,
    },
    #[error(
        "committed artifact hash mismatch for {}: expected {expected}, actual {actual}",
        path.display()
    )]
    ContentHashMismatch {
        path: PathBuf,
        expected: String,
        actual: String,
    },
    #[error("read receipt {}: {source}", path.display())]
    ReceiptIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("parse receipt {}: {source}", path.display())]
    ReceiptJson {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "receipt field mismatch for {} field {field}: expected {expected}, actual {actual}",
        path.display()
    )]
    ReceiptFieldMismatch {
        path: PathBuf,
        field: &'static str,
        expected: String,
        actual: String,
    },
    #[error("archive load failed for {}: {source}", path.display())]
    Archive {
        path: PathBuf,
        #[source]
        source: ArchiveError,
    },
    #[error(
        "committed artifact row count {actual} exceeds full derive load cap {max} for {}",
        path.display()
    )]
    FullLoadRowCapExceeded {
        path: PathBuf,
        actual: u64,
        max: u64,
    },
    #[error(
        "committed artifact bytes {actual} exceeds full derive load cap {max} for {}",
        path.display()
    )]
    FullLoadByteCapExceeded {
        path: PathBuf,
        actual: u64,
        max: u64,
    },
    #[error("derive batch failed: {source}")]
    Derive {
        #[from]
        source: DeriveError,
    },
}

/// Read a committed JSONL manifest and prepare raw archive entries for derive loading.
///
/// Non-empty lines must deserialize as [`ManifestEntry`]. Entries for datasets other than
/// `raw_archive_posts` are reported as skips; raw archive entries are validated and mapped
/// into [`LocalManifestEntry`] plus the stable derive identity.
///
/// # Errors
///
/// Returns [`Error`] when a line cannot be read or parsed, or when a target raw archive
/// manifest entry has invalid schema or required fields.
pub fn read_committed_jsonl<R>(reader: R) -> Result<Plan, Error>
where
    R: BufRead,
{
    let mut inputs = Vec::new();
    let mut skipped_entries = Vec::new();

    for (line_index, line) in reader.lines().enumerate() {
        let line_number = line_index.checked_add(1).ok_or(Error::LineNumberOverflow)?;
        let line = line.map_err(|source| Error::Io {
            line_number,
            source,
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let entry: ManifestEntry = serde_json::from_str(&line).map_err(|source| Error::Json {
            line_number,
            source,
        })?;
        match loader_input_from_entry(entry, line_number)? {
            EntryDisposition::Load(input) => inputs.push(*input),
            EntryDisposition::Skip(skip) => skipped_entries.push(skip),
        }
    }

    Ok(Plan {
        inputs,
        skipped_entries,
    })
}

/// Verify a parsed committed manifest entry and build the `ClickHouse` derive batch it names.
///
/// The object path is resolved under `archive_root`. The object bytes and SHA-256 are always
/// verified against the manifest. Adjacent object receipts and repo receipts are validated when
/// their current local artifact filenames are present.
///
/// # Errors
///
/// Returns [`Error`] when the object is missing, any available receipt disagrees with the
/// manifest or recomputed row hashes, or the verified rows cannot form a derive batch.
pub fn load_verified_clickhouse_batch(
    archive_root: &Path,
    input: &LoaderInput,
) -> Result<ClickHouseDeriveBatch, Error> {
    load_verified_clickhouse_batch_with_caps(archive_root, input, FullLoadCaps::default())
}

/// Verify a parsed committed manifest entry and build its `ClickHouse` derive batch with
/// caller-supplied full-object load caps.
///
/// # Errors
///
/// Returns [`Error`] when the artifact exceeds a cap, is missing, any available receipt
/// disagrees, or the verified rows cannot form a derive batch.
pub fn load_verified_clickhouse_batch_with_caps(
    archive_root: &Path,
    input: &LoaderInput,
    caps: FullLoadCaps,
) -> Result<ClickHouseDeriveBatch, Error> {
    let object_path = resolve_local_path(archive_root, &input.manifest.local_path)?;
    validate_full_load_caps(&object_path, &input.manifest, caps)?;
    let digest = hash_file(&object_path)?;
    validate_object_digest(&object_path, &input.manifest, &digest)?;

    if let Some(receipt_path) = first_existing_path(object_receipt_candidates(&object_path)) {
        let receipt = read_receipt::<Receipt>(&receipt_path)?;
        validate_object_receipt(&receipt_path, &input.manifest, &receipt)?;
    }

    let archive_rows = read_archive_post_rows(&object_path).map_err(|source| Error::Archive {
        path: object_path.clone(),
        source,
    })?;

    if let Some(receipt_path) = first_existing_path(repo_receipt_candidates(&object_path)) {
        let receipt = read_receipt::<RepoReceipt>(&receipt_path)?;
        validate_repo_receipt(&receipt_path, &input.manifest, &archive_rows, &receipt)?;
    }

    Ok(derive_clickhouse_batch(DeriveBatchInput {
        manifest: &input.manifest,
        archive_rows: &archive_rows,
    })?)
}

/// Verify a committed manifest entry without materializing archive rows.
///
/// The object bytes and SHA-256 are verified against the manifest, adjacent object receipts are
/// checked when present, and the adjacent repo receipt is returned for streaming row validation.
///
/// # Errors
///
/// Returns [`Error`] when the object is missing, a digest/receipt disagrees, or the local path
/// escapes the archive root.
pub fn verify_loader_input_for_streaming(
    archive_root: &Path,
    input: &LoaderInput,
) -> Result<VerifiedLoaderInput, Error> {
    let object_path = resolve_local_path(archive_root, &input.manifest.local_path)?;
    let digest = hash_file(&object_path)?;
    validate_object_digest(&object_path, &input.manifest, &digest)?;

    if let Some(receipt_path) = first_existing_path(object_receipt_candidates(&object_path)) {
        let receipt = read_receipt::<Receipt>(&receipt_path)?;
        validate_object_receipt(&receipt_path, &input.manifest, &receipt)?;
    }

    let repo_receipt = first_existing_path(repo_receipt_candidates(&object_path))
        .map(|receipt_path| read_receipt::<RepoReceipt>(&receipt_path))
        .transpose()?;

    Ok(VerifiedLoaderInput {
        manifest: input.manifest.clone(),
        identity: input.identity.clone(),
        object_path,
        repo_receipt,
    })
}

/// Verify every loader input and build its `ClickHouse` derive batch.
///
/// # Errors
///
/// Returns [`Error`] on the first failed artifact verification or derive failure.
pub fn load_verified_clickhouse_batches(
    archive_root: &Path,
    inputs: &[LoaderInput],
) -> Result<Vec<ClickHouseDeriveBatch>, Error> {
    inputs
        .iter()
        .map(|input| load_verified_clickhouse_batch(archive_root, input))
        .collect()
}

enum EntryDisposition {
    Load(Box<LoaderInput>),
    Skip(SkippedEntry),
}

fn loader_input_from_entry(
    entry: ManifestEntry,
    line_number: usize,
) -> Result<EntryDisposition, Error> {
    validate_required_fields(&entry, line_number)?;
    if entry.dataset != RAW_ARCHIVE_POSTS_DATASET {
        return Ok(EntryDisposition::Skip(SkippedEntry {
            line_number,
            dataset: entry.dataset,
            object_path: entry.object_path,
        }));
    }

    validate_raw_archive_entry(&entry, line_number)?;
    let manifest = local_manifest_from_entry(entry);
    let identity = manifest_identity(&manifest);

    Ok(EntryDisposition::Load(Box::new(LoaderInput {
        manifest,
        identity,
    })))
}

fn validate_required_fields(entry: &ManifestEntry, line_number: usize) -> Result<(), Error> {
    validate_non_empty(&entry.dataset, "dataset", line_number)?;
    validate_non_empty(&entry.run_id, "run_id", line_number)?;
    validate_non_empty(&entry.shard, "shard", line_number)?;
    validate_non_empty(&entry.object_path, "object_path", line_number)?;
    validate_non_empty(&entry.content_hash, "content_hash", line_number)?;
    validate_non_empty(&entry.receipt_hash, "receipt_hash", line_number)?;
    validate_scoped_object_path(&entry.object_path, line_number)
}

const fn validate_raw_archive_entry(
    entry: &ManifestEntry,
    line_number: usize,
) -> Result<(), Error> {
    if entry.schema_version == RAW_ARCHIVE_POSTS_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(Error::UnsupportedSchemaVersion {
            line_number,
            actual: entry.schema_version,
            expected: RAW_ARCHIVE_POSTS_SCHEMA_VERSION,
        })
    }
}

const fn validate_non_empty(
    value: &str,
    field: &'static str,
    line_number: usize,
) -> Result<(), Error> {
    if value.is_empty() {
        Err(Error::EmptyField { line_number, field })
    } else {
        Ok(())
    }
}

fn validate_scoped_object_path(object_path: &str, line_number: usize) -> Result<(), Error> {
    let path = Path::new(object_path);
    for component in path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(Error::ObjectPathEscapesRoot {
                    line_number,
                    object_path: object_path.to_owned(),
                });
            }
        }
    }
    Ok(())
}

fn local_manifest_from_entry(entry: ManifestEntry) -> LocalManifestEntry {
    LocalManifestEntry {
        run_id: entry.run_id,
        shard: entry.shard,
        file_sequence: entry.file_sequence,
        dataset: entry.dataset,
        local_path: PathBuf::from(entry.object_path),
        row_count: entry.row_count,
        bytes: entry.bytes,
        content_hash: entry.content_hash,
        min_created_at_normalized: entry.min_created_at_normalized,
        max_created_at_normalized: entry.max_created_at_normalized,
        receipt_hash: entry.receipt_hash,
        schema_version: entry.schema_version,
        normalizer: entry.normalizer,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DigestResult {
    bytes: u64,
    sha256: String,
}

fn resolve_local_path(root: &Path, path: &Path) -> Result<PathBuf, Error> {
    let mut scoped = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => scoped.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(Error::LocalPathEscapesRoot {
                    path: path.to_path_buf(),
                });
            }
        }
    }
    Ok(root.join(scoped))
}

fn hash_file(path: &Path) -> Result<DigestResult, Error> {
    if !path.try_exists().map_err(|source| Error::HashIo {
        operation: "stat",
        path: path.to_path_buf(),
        source,
    })? {
        return Err(Error::MissingArtifact {
            path: path.to_path_buf(),
        });
    }

    let mut file = File::open(path).map_err(|source| Error::HashIo {
        operation: "open",
        path: path.to_path_buf(),
        source,
    })?;
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; 65_536].into_boxed_slice();

    loop {
        let read = io::Read::read(&mut file, &mut buffer).map_err(|source| Error::HashIo {
            operation: "read",
            path: path.to_path_buf(),
            source,
        })?;
        if read == 0 {
            break;
        }
        let read_u64 = u64::try_from(read).map_err(|_error| Error::ByteCountOverflow {
            path: path.to_path_buf(),
        })?;
        bytes = bytes
            .checked_add(read_u64)
            .ok_or_else(|| Error::ByteCountOverflow {
                path: path.to_path_buf(),
            })?;
        let Some(chunk) = buffer.get(..read) else {
            return Err(Error::ByteCountOverflow {
                path: path.to_path_buf(),
            });
        };
        hasher.update(chunk);
    }

    Ok(DigestResult {
        bytes,
        sha256: hex::encode(hasher.finalize()),
    })
}

fn validate_object_digest(
    path: &Path,
    manifest: &LocalManifestEntry,
    digest: &DigestResult,
) -> Result<(), Error> {
    if manifest.bytes != digest.bytes {
        return Err(Error::ByteMismatch {
            path: path.to_path_buf(),
            expected: manifest.bytes,
            actual: digest.bytes,
        });
    }
    if manifest.content_hash != digest.sha256 {
        return Err(Error::ContentHashMismatch {
            path: path.to_path_buf(),
            expected: manifest.content_hash.clone(),
            actual: digest.sha256.clone(),
        });
    }
    Ok(())
}

fn validate_full_load_caps(
    path: &Path,
    manifest: &LocalManifestEntry,
    caps: FullLoadCaps,
) -> Result<(), Error> {
    if manifest.row_count > caps.max_rows {
        return Err(Error::FullLoadRowCapExceeded {
            path: path.to_path_buf(),
            actual: manifest.row_count,
            max: caps.max_rows,
        });
    }
    if manifest.bytes > caps.max_bytes {
        return Err(Error::FullLoadByteCapExceeded {
            path: path.to_path_buf(),
            actual: manifest.bytes,
            max: caps.max_bytes,
        });
    }
    Ok(())
}

fn read_receipt<T>(path: &Path) -> Result<T, Error>
where
    T: serde::de::DeserializeOwned,
{
    let bytes = fs::read(path).map_err(|source| Error::ReceiptIo {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(|source| Error::ReceiptJson {
        path: path.to_path_buf(),
        source,
    })
}

fn validate_object_receipt(
    path: &Path,
    manifest: &LocalManifestEntry,
    receipt: &Receipt,
) -> Result<(), Error> {
    expect_receipt_field(path, "run_id", &manifest.run_id, &receipt.run_id)?;
    expect_receipt_field(path, "shard", &manifest.shard, &receipt.shard)?;
    expect_receipt_field(
        path,
        "file_sequence",
        &manifest.file_sequence.to_string(),
        &receipt.file_sequence.to_string(),
    )?;
    expect_receipt_field(path, "dataset", &manifest.dataset, &receipt.dataset)?;
    expect_receipt_field(
        path,
        "object_path",
        manifest.local_path.to_string_lossy().as_ref(),
        &receipt.object_path,
    )?;
    expect_receipt_field(
        path,
        "row_count",
        &manifest.row_count.to_string(),
        &receipt.row_count.to_string(),
    )?;
    expect_receipt_field(
        path,
        "bytes",
        &manifest.bytes.to_string(),
        &receipt.bytes.to_string(),
    )?;
    expect_receipt_field(
        path,
        "content_hash",
        &manifest.content_hash,
        &receipt.content_hash,
    )?;
    expect_receipt_field(
        path,
        "receipt_hash",
        &manifest.receipt_hash,
        &receipt.receipt_hash,
    )?;
    expect_receipt_field(
        path,
        "schema_version",
        &manifest.schema_version.to_string(),
        &receipt.schema_version.to_string(),
    )
}

fn validate_repo_receipt(
    path: &Path,
    manifest: &LocalManifestEntry,
    rows: &[crate::archive::ArchivePostRow],
    receipt: &RepoReceipt,
) -> Result<(), Error> {
    expect_receipt_field(
        path,
        "archived_post_rows_count",
        &manifest.row_count.to_string(),
        &receipt.archived_post_rows_count.to_string(),
    )?;
    expect_receipt_field(
        path,
        "normalizer",
        &serde_json::to_string(&manifest.normalizer).map_err(|source| Error::ReceiptJson {
            path: path.to_path_buf(),
            source,
        })?,
        &serde_json::to_string(&receipt.normalizer).map_err(|source| Error::ReceiptJson {
            path: path.to_path_buf(),
            source,
        })?,
    )?;

    let row_hash = hash_post_rows(rows).map_err(|source| Error::Archive {
        path: path.to_path_buf(),
        source,
    })?;
    expect_receipt_field(path, "post_rows_hash", &row_hash, &receipt.post_rows_hash)?;
    expect_receipt_field(
        path,
        "archive_rows_hash",
        &row_hash,
        &receipt.archive_rows_hash,
    )?;

    let receipt_hash = hash_serialized_json(receipt).map_err(|source| Error::ReceiptJson {
        path: path.to_path_buf(),
        source,
    })?;
    expect_receipt_field(path, "receipt_hash", &manifest.receipt_hash, &receipt_hash)
}

fn expect_receipt_field(
    path: &Path,
    field: &'static str,
    expected: &str,
    actual: &str,
) -> Result<(), Error> {
    if expected == actual {
        Ok(())
    } else {
        Err(Error::ReceiptFieldMismatch {
            path: path.to_path_buf(),
            field,
            expected: expected.to_owned(),
            actual: actual.to_owned(),
        })
    }
}

fn first_existing_path(paths: Vec<PathBuf>) -> Option<PathBuf> {
    paths.into_iter().find(|path| path.exists())
}

fn object_receipt_candidates(object_path: &Path) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    candidates.push(object_path.with_extension("receipt.json"));
    if let Some(path) = replace_file_suffix(object_path, ".posts.parquet", ".object-receipt.json") {
        candidates.push(path);
    }
    if let Some(path) = replace_file_suffix(object_path, ".parquet", ".object-receipt.json") {
        candidates.push(path);
    }
    candidates
}

fn repo_receipt_candidates(object_path: &Path) -> Vec<PathBuf> {
    replace_file_suffix(object_path, ".posts.parquet", ".receipt.json")
        .into_iter()
        .collect()
}

fn replace_file_suffix(path: &Path, suffix: &str, replacement: &str) -> Option<PathBuf> {
    let file_name = path.file_name()?.to_str()?;
    let prefix = file_name.strip_suffix(suffix)?;
    Some(path.with_file_name(format!("{prefix}{replacement}")))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]

    use std::{
        fs,
        io::{BufReader, Cursor},
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::{
        Error, FullLoadCaps, load_verified_clickhouse_batch,
        load_verified_clickhouse_batch_with_caps, read_committed_jsonl,
    };
    use crate::{
        archive::{
            ArchiveCommitContext, ArchivePostRow, CreatedAtParseStatus, NormalizerVersion,
            RepoReceipt, RepoReceiptInput, build_repo_receipt, current_normalizer,
            write_archive_artifacts,
        },
        commit::ManifestEntry,
    };

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    fn normalizer() -> NormalizerVersion {
        NormalizerVersion {
            name: "emoji-normalizer".to_owned(),
            semver: "0.1.0".to_owned(),
            git_rev: "test".to_owned(),
            unicode_version: "16.0".to_owned(),
            emoji_data_version: "16.0".to_owned(),
        }
    }

    fn entry(dataset: &str) -> ManifestEntry {
        ManifestEntry {
            run_id: "run-1".to_owned(),
            shard: "shard3".to_owned(),
            file_sequence: 42,
            dataset: dataset.to_owned(),
            object_path: format!("objects/{dataset}/part-000042.parquet"),
            row_count: 123,
            bytes: 456,
            content_hash: "content-hash".to_owned(),
            min_created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
            max_created_at_normalized: Some("2026-06-15T01:00:00Z".to_owned()),
            receipt_hash: "receipt-hash".to_owned(),
            normalizer: normalizer(),
            schema_version: 1,
        }
    }

    fn jsonl(entries: &[ManifestEntry]) -> String {
        let mut lines = String::new();
        for entry in entries {
            lines.push_str(&serde_json::to_string(entry).expect("serialize manifest entry"));
            lines.push('\n');
        }
        lines
    }

    fn archive_row(rkey: &str, text: &str, emojis: &[&str]) -> ArchivePostRow {
        ArchivePostRow {
            did: "did:plc:fixture123".to_owned(),
            rkey: rkey.to_owned(),
            cid: format!("bafy-{rkey}"),
            normalizer: current_normalizer(),
            account_status: None,
            record_status: None,
            public_content_label: None,
            created_at_raw: Some("2026-06-15T00:00:00Z".to_owned()),
            created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
            created_at_parse_status: CreatedAtParseStatus::Valid,
            text: text.to_owned(),
            langs: vec!["en".to_owned()],
            emoji_sequence: emojis.iter().map(|emoji| (*emoji).to_owned()).collect(),
            extras_json: serde_json::json!({}),
        }
    }

    fn repo_receipt(rows: &[ArchivePostRow]) -> RepoReceipt {
        build_repo_receipt(RepoReceiptInput {
            rows,
            reachable_records_count: u64::try_from(rows.len()).expect("row count should fit u64"),
            reachable_post_records_count: u64::try_from(rows.len())
                .expect("row count should fit u64"),
            post_decode_error_count: 0,
            profile_row_hash: None,
            mst_root_cid: Some("bafy-mst".to_owned()),
            commit_cid: Some("bafy-commit".to_owned()),
            normalizer: current_normalizer(),
        })
        .expect("receipt should build")
    }

    fn read_plan_from_path(path: &Path) -> super::Plan {
        let file = fs::File::open(path).expect("manifest should be readable");
        read_committed_jsonl(BufReader::new(file)).expect("manifest should parse")
    }

    #[test]
    fn parses_jsonl_and_builds_loader_inputs_for_raw_archive_posts() {
        let raw_entry = entry("raw_archive_posts");
        let profile_entry = entry("raw_profile_sidecar");
        let plan = read_committed_jsonl(Cursor::new(jsonl(&[profile_entry, raw_entry.clone()])))
            .expect("read manifest jsonl");

        assert_eq!(plan.inputs.len(), 1);
        assert_eq!(plan.skipped_entries.len(), 1);
        let input = plan.inputs.first().expect("one loader input");
        assert_eq!(input.manifest.run_id, raw_entry.run_id);
        assert_eq!(
            input.manifest.local_path,
            std::path::PathBuf::from(raw_entry.object_path)
        );
        assert_eq!(plan.skipped_entries[0].dataset, "raw_profile_sidecar");
    }

    #[test]
    fn skips_non_raw_dataset_and_rejects_bad_dataset_field() {
        let skipped = read_committed_jsonl(Cursor::new(jsonl(&[entry("raw_profile_sidecar")])))
            .expect("read skipped manifest jsonl");
        assert!(skipped.inputs.is_empty());
        assert_eq!(skipped.skipped_entries.len(), 1);

        let mut bad = entry("");
        bad.object_path = "objects/empty-dataset.parquet".to_owned();
        let error =
            read_committed_jsonl(Cursor::new(jsonl(&[bad]))).expect_err("empty dataset rejected");
        assert!(matches!(
            error,
            Error::EmptyField {
                line_number: 1,
                field: "dataset"
            }
        ));
    }

    #[test]
    fn rejects_raw_archive_schema_mismatch() {
        let mut bad = entry("raw_archive_posts");
        bad.schema_version = 2;

        let error =
            read_committed_jsonl(Cursor::new(jsonl(&[bad]))).expect_err("bad schema rejected");

        assert!(matches!(
            error,
            Error::UnsupportedSchemaVersion {
                line_number: 1,
                actual: 2,
                expected: 1
            }
        ));
    }

    #[test]
    fn stable_identity_fields_come_from_committed_manifest() {
        let mut raw_entry = entry("raw_archive_posts");
        raw_entry.object_path = "objects/raw_archive_posts/a.parquet".to_owned();
        raw_entry.bytes = 111;
        raw_entry.min_created_at_normalized = Some("2026-06-15T00:00:00Z".to_owned());
        let first = read_committed_jsonl(Cursor::new(jsonl(&[raw_entry.clone()])))
            .expect("read first manifest jsonl");

        raw_entry.object_path = "objects/raw_archive_posts/b.parquet".to_owned();
        raw_entry.bytes = 222;
        raw_entry.min_created_at_normalized = Some("2026-06-14T00:00:00Z".to_owned());
        let second = read_committed_jsonl(Cursor::new(jsonl(&[raw_entry])))
            .expect("read second manifest jsonl");

        let first_identity = &first.inputs.first().expect("first input").identity;
        let second_identity = &second.inputs.first().expect("second input").identity;
        assert_eq!(first_identity, second_identity);
        assert_eq!(first_identity.run_id, "run-1");
        assert_eq!(first_identity.shard, "shard3");
        assert_eq!(first_identity.file_sequence, 42);
        assert_eq!(first_identity.dataset, "raw_archive_posts");
        assert_eq!(first_identity.content_hash, "content-hash");
        assert_eq!(first_identity.receipt_hash, "receipt-hash");
        assert_eq!(first_identity.schema_version, 1);
    }

    #[test]
    fn verified_manifest_entry_loads_clickhouse_batch() {
        let temp = TempDir::new("valid");
        let output_dir = temp.path.join("archive");
        let rows = vec![
            archive_row("a", "hello ✅", &["✅"]),
            archive_row("b", "fire 🔥🔥", &["🔥", "🔥"]),
        ];
        let receipt = repo_receipt(&rows);
        let artifacts = write_archive_artifacts(
            &output_dir,
            "did:plc:fixture123",
            &ArchiveCommitContext::fetch_one_local(),
            &rows,
            None,
            &receipt,
        )
        .expect("archive artifacts should write");
        let plan = read_plan_from_path(&artifacts.manifest_path);
        let input = plan.inputs.first().expect("loader input");

        let batch =
            load_verified_clickhouse_batch(&output_dir, input).expect("verified batch should load");

        assert_eq!(batch.manifest_identity, input.identity);
        assert_eq!(batch.emoji_rows.len(), 2);
        assert_eq!(batch.total_post_counter.posts_processed, 2);
        assert_eq!(batch.total_post_counter.emoji_occurrences, 3);
    }

    #[test]
    fn verified_manifest_entry_rejects_missing_parquet() {
        let temp = TempDir::new("missing");
        let output_dir = temp.path.join("archive");
        let rows = vec![archive_row("a", "hello ✅", &["✅"])];
        let receipt = repo_receipt(&rows);
        let artifacts = write_archive_artifacts(
            &output_dir,
            "did:plc:fixture123",
            &ArchiveCommitContext::fetch_one_local(),
            &rows,
            None,
            &receipt,
        )
        .expect("archive artifacts should write");
        let plan = read_plan_from_path(&artifacts.manifest_path);
        fs::remove_file(&artifacts.parquet_path).expect("parquet should be removable");

        let error =
            load_verified_clickhouse_batch(&output_dir, plan.inputs.first().expect("loader input"))
                .expect_err("missing parquet should fail");

        assert!(matches!(error, Error::MissingArtifact { .. }));
    }

    #[test]
    fn verified_manifest_entry_rejects_parquet_hash_mismatch() {
        let temp = TempDir::new("hash-mismatch");
        let output_dir = temp.path.join("archive");
        let rows = vec![archive_row("a", "hello ✅", &["✅"])];
        let receipt = repo_receipt(&rows);
        let artifacts = write_archive_artifacts(
            &output_dir,
            "did:plc:fixture123",
            &ArchiveCommitContext::fetch_one_local(),
            &rows,
            None,
            &receipt,
        )
        .expect("archive artifacts should write");
        let plan = read_plan_from_path(&artifacts.manifest_path);
        fs::write(&artifacts.parquet_path, b"corrupt").expect("parquet should be mutable");

        let error =
            load_verified_clickhouse_batch(&output_dir, plan.inputs.first().expect("loader input"))
                .expect_err("hash mismatch should fail");

        assert!(matches!(
            error,
            Error::ByteMismatch { .. } | Error::ContentHashMismatch { .. }
        ));
    }

    #[test]
    fn full_batch_load_rejects_manifest_above_explicit_caps_before_reading_rows() {
        let temp = TempDir::new("row-cap");
        let output_dir = temp.path.join("archive");
        let rows = vec![archive_row("a", "hello ✅", &["✅"])];
        let receipt = repo_receipt(&rows);
        let artifacts = write_archive_artifacts(
            &output_dir,
            "did:plc:fixture123",
            &ArchiveCommitContext::fetch_one_local(),
            &rows,
            None,
            &receipt,
        )
        .expect("archive artifacts should write");
        let plan = read_plan_from_path(&artifacts.manifest_path);
        let input = plan.inputs.first().expect("loader input");

        let error = load_verified_clickhouse_batch_with_caps(
            &output_dir,
            input,
            FullLoadCaps {
                max_rows: 0,
                max_bytes: u64::MAX,
            },
        )
        .expect_err("row cap should fail");

        assert!(matches!(error, Error::FullLoadRowCapExceeded { .. }));
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "emojistats-manifest-derive-{name}-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("test temp directory should be created");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ignored = fs::remove_dir_all(&self.path);
        }
    }
}
