//! Local committed-artifact protocol for Storage Box-shaped archive outputs.

use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use crate::archive::NormalizerVersion;

const HASH_BUFFER_BYTES: usize = 65_536;
pub(crate) const PROTOCOL_VERSION: u16 = 1;

mod manifest;
use manifest::{write_manifest, write_manifest_if_missing};

/// Local filesystem implementation of the committed artifact protocol.
#[derive(Debug, Clone)]
pub struct LocalStore {
    root: PathBuf,
}

/// One artifact commit request.
#[derive(Debug, Clone)]
pub struct Request {
    /// Final object path relative to the store root.
    pub object_path: PathBuf,
    /// Receipt sidecar path relative to the store root.
    pub receipt_path: PathBuf,
    /// Manifest path relative to the store root.
    pub manifest_path: PathBuf,
    /// Manifest update strategy.
    pub manifest_mode: ManifestMode,
    /// Manifest metadata known before object bytes are written.
    pub metadata: Metadata,
}

/// Manifest metadata known before the final object digest exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    /// Backfill run identifier.
    pub run_id: String,
    /// Canonical shard label.
    pub shard: String,
    /// Logical file/attempt sequence recorded for operator traceability.
    ///
    /// Derive idempotency is keyed by content and receipt hashes, so this value is not a
    /// global per-shard ordering primitive.
    pub file_sequence: u64,
    /// DID that produced this committed repo artifact.
    pub did: String,
    /// Dataset name, such as `raw_archive_posts`.
    pub dataset: String,
    /// Number of rows in the committed object.
    pub row_count: u64,
    /// Minimum normalized timestamp in the object.
    pub min_created_at_normalized: Option<String>,
    /// Maximum normalized timestamp in the object.
    pub max_created_at_normalized: Option<String>,
    /// Hash of the row-content receipt that produced this object.
    pub receipt_hash: String,
    /// Optional repo-level receipt path advertised to derive consumers.
    pub repo_receipt_path: Option<String>,
    /// Normalizer version used to produce row content.
    pub normalizer: NormalizerVersion,
    /// Archive schema version.
    pub schema_version: u16,
}

/// Manifest update strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestMode {
    /// Do not expose a local manifest entry for this commit.
    ///
    /// Used when another selected backend owns manifest publication.
    Skip,
    /// Append a single JSON object plus newline.
    AppendJsonl,
    /// Replace the manifest file with a JSON array containing this entry.
    ReplaceJsonArray,
}

/// Completed local commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artifact {
    /// Final object path.
    pub object_path: PathBuf,
    /// Receipt sidecar path.
    pub receipt_path: PathBuf,
    /// Manifest path.
    pub manifest_path: PathBuf,
    /// Manifest entry written after final object promotion.
    pub entry: ManifestEntry,
    /// Receipt sidecar written after final object promotion.
    pub receipt: Receipt,
}

/// Commit-plan output shared by local and remote stores after final object bytes are known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitPlan {
    pub entry: ManifestEntry,
    pub receipt: Receipt,
}

/// Storage Box-shaped committed manifest entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub run_id: String,
    pub shard: String,
    pub file_sequence: u64,
    pub did: String,
    pub dataset: String,
    pub object_path: String,
    pub row_count: u64,
    pub bytes: u64,
    pub content_hash: String,
    pub min_created_at_normalized: Option<String>,
    pub max_created_at_normalized: Option<String>,
    pub receipt_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_receipt_path: Option<String>,
    pub normalizer: NormalizerVersion,
    pub schema_version: u16,
}

/// Sidecar receipt for the committed object itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    pub protocol_version: u16,
    pub run_id: String,
    pub shard: String,
    pub file_sequence: u64,
    pub dataset: String,
    pub object_path: String,
    pub row_count: u64,
    pub bytes: u64,
    pub content_hash: String,
    pub receipt_hash: String,
    pub schema_version: u16,
}

/// Commit protocol failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A path attempted to leave the configured store root.
    #[error("{kind} path escapes local commit root: {}", path.display())]
    PathEscapesRoot { kind: &'static str, path: PathBuf },
    /// A path had no usable file name.
    #[error("{kind} path has no file name: {}", path.display())]
    MissingFileName { kind: &'static str, path: PathBuf },
    /// A path component could not be encoded into the manifest.
    #[error("{kind} path is not valid UTF-8 for manifest encoding: {}", path.display())]
    NonUtf8Path { kind: &'static str, path: PathBuf },
    /// Filesystem operation failed.
    #[error("{operation} failed for {}: {source}", path.display())]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    /// JSON serialization failed.
    #[error("JSON write failed for {}: {source}", path.display())]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    /// JSON deserialization failed.
    #[error("JSON read failed for {}: {source}", path.display())]
    JsonRead {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    /// Byte count overflowed the manifest schema.
    #[error("byte count overflow while hashing {}", path.display())]
    ByteCountOverflow { path: PathBuf },
    /// The filesystem returned a read size outside the requested buffer.
    #[error("invalid read size while hashing {}", path.display())]
    InvalidReadSize { path: PathBuf },
    /// Caller-provided object writer failed before commit.
    #[error("{0}")]
    Writer(String),
    /// A final artifact path already existed before promotion.
    #[error("{kind} final path already exists: {}", path.display())]
    FinalPathExists { kind: &'static str, path: PathBuf },
    /// The final artifact hash did not match the temp hash after promotion.
    #[error("{kind} final hash mismatch for {}: expected {expected}, observed {observed}", path.display())]
    FinalHashMismatch {
        kind: &'static str,
        path: PathBuf,
        expected: String,
        observed: String,
    },
    /// A retry found a final receipt, but it did not match the repaired commit request.
    #[error("existing receipt does not match repaired commit request: {}", path.display())]
    ExistingReceiptMismatch { path: PathBuf },
}

impl LocalStore {
    /// Create a local committed-artifact store rooted at `root`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Commit one object through temp write, fsync, hard-link promotion, digest, sidecar, and manifest.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if path validation, object writing, fsync, promotion, digesting,
    /// receipt writing, or manifest writing fails.
    pub fn commit<F>(&self, request: &Request, write_object: F) -> Result<Artifact, Error>
    where
        F: FnOnce(&mut File) -> Result<(), Error>,
    {
        self.prepare_commit(request, |object| {
            write_temp_promote_file(object, "object", write_object)
        })
    }

    /// Commit an already-written temp object through fsync, hard-link promotion, digest, sidecar, and manifest.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if path validation, object promotion, digesting, receipt writing, or
    /// manifest writing fails.
    pub(crate) fn commit_prepared_temp(
        &self,
        request: &Request,
        temp_object_path: &Path,
    ) -> Result<Artifact, Error> {
        self.prepare_commit(request, |object| {
            promote_prepared_file(temp_object_path, object, "object")
        })
    }

    fn prepare_commit<F>(&self, request: &Request, promote_object: F) -> Result<Artifact, Error>
    where
        F: FnOnce(&Path) -> Result<ObjectCommitState, Error>,
    {
        self.prepare_root()?;
        let object = self.resolve_scoped("object", &request.object_path)?;
        let receipt = self.resolve_scoped("receipt", &request.receipt_path)?;
        let manifest = self.resolve_scoped("manifest", &request.manifest_path)?;
        prepare_parent(&object, "object")?;
        prepare_parent(&receipt, "receipt")?;
        prepare_parent(&manifest, "manifest")?;

        let object_state = promote_object(&object)?;
        let digest = object_state.digest();
        let object_path = manifest_path_string("object", &request.object_path)?;
        let plan = CommitPlan::from_digest(&request.metadata, object_path, &digest);

        let (entry, committed_receipt) = match object_state {
            ObjectCommitState::Promoted(_) => {
                write_json_temp_promote(&receipt, "receipt", &plan.receipt)?;
                if request.manifest_mode != ManifestMode::Skip {
                    write_manifest(&manifest, request.manifest_mode, &plan.entry)?;
                }
                (plan.entry, plan.receipt)
            }
            ObjectCommitState::AlreadyCommitted(_) => {
                let committed_receipt =
                    repair_or_validate_existing_receipt(&receipt, &plan.receipt)?;
                let entry = ManifestEntry::from_parts(&request.metadata, &committed_receipt);
                if request.manifest_mode != ManifestMode::Skip {
                    write_manifest_if_missing(&manifest, request.manifest_mode, &entry)?;
                }
                (entry, committed_receipt)
            }
        };

        Ok(Artifact {
            object_path: object,
            receipt_path: receipt,
            manifest_path: manifest,
            entry,
            receipt: committed_receipt,
        })
    }

    fn prepare_root(&self) -> Result<(), Error> {
        fs::create_dir_all(&self.root).map_err(|source| Error::Io {
            operation: "create root directory",
            path: self.root.clone(),
            source,
        })
    }

    fn resolve_scoped(&self, kind: &'static str, path: &Path) -> Result<PathBuf, Error> {
        let mut scoped = PathBuf::new();
        for component in path.components() {
            match component {
                Component::Normal(part) => scoped.push(part),
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(Error::PathEscapesRoot {
                        kind,
                        path: path.to_path_buf(),
                    });
                }
            }
        }
        if scoped.file_name().is_none() {
            return Err(Error::MissingFileName {
                kind,
                path: path.to_path_buf(),
            });
        }
        Ok(self.root.join(scoped))
    }
}

impl CommitPlan {
    #[must_use]
    pub(crate) fn from_digest(
        metadata: &Metadata,
        object_path: String,
        digest: &DigestResult,
    ) -> Self {
        let receipt = Receipt::from_parts(metadata, object_path, digest);
        let entry = ManifestEntry::from_parts(metadata, &receipt);
        Self { entry, receipt }
    }
}

impl Error {
    /// Build a caller-facing object writer error.
    #[must_use]
    pub fn writer(message: impl Into<String>) -> Self {
        Self::Writer(message.into())
    }
}

impl ManifestEntry {
    pub(crate) fn from_parts(metadata: &Metadata, receipt: &Receipt) -> Self {
        Self {
            run_id: receipt.run_id.clone(),
            shard: receipt.shard.clone(),
            file_sequence: receipt.file_sequence,
            did: metadata.did.clone(),
            dataset: receipt.dataset.clone(),
            object_path: receipt.object_path.clone(),
            row_count: receipt.row_count,
            bytes: receipt.bytes,
            content_hash: receipt.content_hash.clone(),
            min_created_at_normalized: metadata.min_created_at_normalized.clone(),
            max_created_at_normalized: metadata.max_created_at_normalized.clone(),
            receipt_hash: receipt.receipt_hash.clone(),
            repo_receipt_path: metadata.repo_receipt_path.clone(),
            normalizer: metadata.normalizer.clone(),
            schema_version: receipt.schema_version,
        }
    }
}

impl Receipt {
    pub(crate) fn from_parts(
        metadata: &Metadata,
        object_path: String,
        digest: &DigestResult,
    ) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            run_id: metadata.run_id.clone(),
            shard: metadata.shard.clone(),
            file_sequence: metadata.file_sequence,
            dataset: metadata.dataset.clone(),
            object_path,
            row_count: metadata.row_count,
            bytes: digest.bytes,
            content_hash: digest.sha256.clone(),
            receipt_hash: metadata.receipt_hash.clone(),
            schema_version: metadata.schema_version,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DigestResult {
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ObjectCommitState {
    Promoted(DigestResult),
    AlreadyCommitted(DigestResult),
}

impl ObjectCommitState {
    fn digest(&self) -> DigestResult {
        match self {
            Self::Promoted(digest) | Self::AlreadyCommitted(digest) => digest.clone(),
        }
    }
}

fn write_temp_promote_file<F>(
    path: &Path,
    kind: &'static str,
    write: F,
) -> Result<ObjectCommitState, Error>
where
    F: FnOnce(&mut File) -> Result<(), Error>,
{
    let parent = path.parent().ok_or_else(|| Error::MissingFileName {
        kind,
        path: path.to_path_buf(),
    })?;
    let mut temp_file = NamedTempFile::new_in(parent).map_err(|source| Error::Io {
        operation: "create temp file",
        path: parent.to_path_buf(),
        source,
    })?;
    write(temp_file.as_file_mut())?;
    promote_named_temp_file(temp_file, path, kind)
}

fn promote_named_temp_file(
    temp_file: NamedTempFile,
    path: &Path,
    kind: &'static str,
) -> Result<ObjectCommitState, Error> {
    temp_file.as_file().sync_all().map_err(|source| Error::Io {
        operation: "fsync temp file",
        path: temp_file.path().to_path_buf(),
        source,
    })?;
    let temp_digest = hash_file(temp_file.path())?;
    match temp_file.persist_noclobber(path) {
        Ok(_file) => {}
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
            let final_digest = hash_file(path)?;
            if final_digest.sha256 == temp_digest.sha256 && final_digest.bytes == temp_digest.bytes
            {
                return Ok(ObjectCommitState::AlreadyCommitted(final_digest));
            }
            return Err(Error::FinalHashMismatch {
                kind,
                path: path.to_path_buf(),
                expected: temp_digest.sha256,
                observed: final_digest.sha256,
            });
        }
        Err(error) => {
            return Err(Error::Io {
                operation: "promote temp file without overwrite",
                path: path.to_path_buf(),
                source: error.error,
            });
        }
    }
    let final_digest = hash_file(path)?;
    if final_digest.sha256 != temp_digest.sha256 || final_digest.bytes != temp_digest.bytes {
        return Err(Error::FinalHashMismatch {
            kind,
            path: path.to_path_buf(),
            expected: temp_digest.sha256,
            observed: final_digest.sha256,
        });
    }
    sync_parent_dir(path, kind)?;
    Ok(ObjectCommitState::Promoted(final_digest))
}

fn promote_prepared_file(
    temp_path: &Path,
    path: &Path,
    kind: &'static str,
) -> Result<ObjectCommitState, Error> {
    File::open(temp_path)
        .and_then(|file| file.sync_all())
        .map_err(|source| Error::Io {
            operation: "fsync temp file",
            path: temp_path.to_path_buf(),
            source,
        })?;
    let temp_digest = hash_file(temp_path)?;
    match promote_no_overwrite(temp_path, path, kind) {
        Ok(()) => {}
        Err(Error::FinalPathExists { .. }) => {
            let final_digest = hash_file(path)?;
            if final_digest.sha256 == temp_digest.sha256 && final_digest.bytes == temp_digest.bytes
            {
                let _ignored = fs::remove_file(temp_path);
                return Ok(ObjectCommitState::AlreadyCommitted(final_digest));
            }
            return Err(Error::FinalHashMismatch {
                kind,
                path: path.to_path_buf(),
                expected: temp_digest.sha256,
                observed: final_digest.sha256,
            });
        }
        Err(error) => return Err(error),
    }
    let final_digest = hash_file(path)?;
    if final_digest.sha256 != temp_digest.sha256 || final_digest.bytes != temp_digest.bytes {
        return Err(Error::FinalHashMismatch {
            kind,
            path: path.to_path_buf(),
            expected: temp_digest.sha256,
            observed: final_digest.sha256,
        });
    }
    let _ignored = fs::remove_file(temp_path);
    Ok(ObjectCommitState::Promoted(final_digest))
}

fn write_json_temp_promote<T>(path: &Path, kind: &'static str, value: &T) -> Result<(), Error>
where
    T: Serialize,
{
    write_temp_promote_file(path, kind, |file| {
        serde_json::to_writer_pretty(&mut *file, value).map_err(|source| Error::Json {
            path: path.to_path_buf(),
            source,
        })?;
        file.write_all(b"\n").map_err(|source| Error::Io {
            operation: "write JSON newline",
            path: path.to_path_buf(),
            source,
        })
    })
    .map(|_digest| ())
}

fn repair_or_validate_existing_receipt(path: &Path, expected: &Receipt) -> Result<Receipt, Error> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            write_json_temp_promote(path, "receipt", expected)?;
            return Ok(expected.clone());
        }
        Err(source) => {
            return Err(Error::Io {
                operation: "read existing receipt",
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let actual: Receipt = serde_json::from_slice(&bytes).map_err(|source| Error::JsonRead {
        path: path.to_path_buf(),
        source,
    })?;
    if actual == *expected || receipts_are_content_compatible(&actual, expected) {
        Ok(actual)
    } else {
        Err(Error::ExistingReceiptMismatch {
            path: path.to_path_buf(),
        })
    }
}

fn receipts_are_content_compatible(actual: &Receipt, expected: &Receipt) -> bool {
    actual.protocol_version == expected.protocol_version
        && actual.dataset == expected.dataset
        && actual.object_path == expected.object_path
        && actual.row_count == expected.row_count
        && actual.bytes == expected.bytes
        && actual.content_hash == expected.content_hash
        && actual.receipt_hash == expected.receipt_hash
        && actual.schema_version == expected.schema_version
}

fn promote_no_overwrite(temp_path: &Path, path: &Path, kind: &'static str) -> Result<(), Error> {
    fs::hard_link(temp_path, path).map_err(|source| {
        if source.kind() == io::ErrorKind::AlreadyExists {
            Error::FinalPathExists {
                kind,
                path: path.to_path_buf(),
            }
        } else {
            Error::Io {
                operation: "promote temp file without overwrite",
                path: path.to_path_buf(),
                source,
            }
        }
    })?;
    sync_parent_dir(path, kind)
}

fn hash_file(path: &Path) -> Result<DigestResult, Error> {
    let mut file = File::open(path).map_err(|source| Error::Io {
        operation: "open final object for hashing",
        path: path.to_path_buf(),
        source,
    })?;
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer).map_err(|source| Error::Io {
            operation: "read final object for hashing",
            path: path.to_path_buf(),
            source,
        })?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(
                u64::try_from(read).map_err(|_error| Error::ByteCountOverflow {
                    path: path.to_path_buf(),
                })?,
            )
            .ok_or_else(|| Error::ByteCountOverflow {
                path: path.to_path_buf(),
            })?;
        let chunk = buffer.get(..read).ok_or_else(|| Error::InvalidReadSize {
            path: path.to_path_buf(),
        })?;
        hasher.update(chunk);
    }
    Ok(DigestResult {
        bytes,
        sha256: hex::encode(hasher.finalize()),
    })
}

fn prepare_parent(path: &Path, kind: &'static str) -> Result<(), Error> {
    let parent = path.parent().ok_or_else(|| Error::MissingFileName {
        kind,
        path: path.to_path_buf(),
    })?;
    fs::create_dir_all(parent).map_err(|source| Error::Io {
        operation: "create parent directory",
        path: parent.to_path_buf(),
        source,
    })
}

fn sync_parent_dir(path: &Path, kind: &'static str) -> Result<(), Error> {
    let parent = path.parent().ok_or_else(|| Error::MissingFileName {
        kind,
        path: path.to_path_buf(),
    })?;
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|source| Error::Io {
            operation: "fsync parent directory",
            path: parent.to_path_buf(),
            source,
        })
}

fn manifest_path_string(kind: &'static str, path: &Path) -> Result<String, Error> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| Error::NonUtf8Path {
                    kind,
                    path: path.to_path_buf(),
                })?;
                parts.push(part);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(Error::PathEscapesRoot {
                    kind,
                    path: path.to_path_buf(),
                });
            }
        }
    }
    if parts.is_empty() {
        return Err(Error::MissingFileName {
            kind,
            path: path.to_path_buf(),
        });
    }
    Ok(parts.join("/"))
}

#[cfg(test)]
#[path = "commit/tests.rs"]
mod tests;
