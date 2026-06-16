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
    pub repo_receipt: RepoReceipt,
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
    #[error("repo receipt is missing for committed artifact: {}", path.display())]
    MissingRepoReceipt { path: PathBuf },
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

    if let Some(receipt_path) =
        first_existing_path(repo_receipt_candidates(&object_path, &input.manifest))
    {
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

    let Some(repo_receipt_path) =
        first_existing_path(repo_receipt_candidates(&object_path, &input.manifest))
    else {
        return Err(Error::MissingRepoReceipt { path: object_path });
    };
    let repo_receipt = read_receipt::<RepoReceipt>(&repo_receipt_path)?;

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

fn repo_receipt_candidates(object_path: &Path, manifest: &LocalManifestEntry) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Some(path) = archive_stem_receipt_path(object_path, &manifest.receipt_hash) {
        candidates.push(path);
    }
    if let Some(path) = replace_file_suffix(object_path, ".posts.parquet", ".receipt.json") {
        candidates.push(path);
    }
    candidates
}

fn archive_stem_receipt_path(object_path: &Path, receipt_hash: &str) -> Option<PathBuf> {
    let file_name = object_path.file_name()?.to_str()?;
    let marker = format!(".{RAW_ARCHIVE_POSTS_DATASET}__");
    let marker_start = file_name.find(&marker)?;
    let artifact_stem = file_name.get(..marker_start)?;
    Some(object_path.with_file_name(format!("{artifact_stem}.{receipt_hash}.receipt.json")))
}

fn replace_file_suffix(path: &Path, suffix: &str, replacement: &str) -> Option<PathBuf> {
    let file_name = path.file_name()?.to_str()?;
    let prefix = file_name.strip_suffix(suffix)?;
    Some(path.with_file_name(format!("{prefix}{replacement}")))
}

#[cfg(test)]
#[path = "manifest_derive/tests.rs"]
mod tests;
