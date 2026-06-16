//! Local committed-artifact protocol for Storage Box-shaped archive outputs.

use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::archive::NormalizerVersion;

const HASH_BUFFER_BYTES: usize = 65_536;
const PROTOCOL_VERSION: u16 = 1;

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
    /// Monotonic file sequence within the shard/dataset stream.
    pub file_sequence: u64,
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
    /// Normalizer version used to produce row content.
    pub normalizer: NormalizerVersion,
    /// Archive schema version.
    pub schema_version: u16,
}

/// Manifest update strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestMode {
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

/// Storage Box-shaped committed manifest entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub run_id: String,
    pub shard: String,
    pub file_sequence: u64,
    pub dataset: String,
    pub object_path: String,
    pub row_count: u64,
    pub bytes: u64,
    pub content_hash: String,
    pub min_created_at_normalized: Option<String>,
    pub max_created_at_normalized: Option<String>,
    pub receipt_hash: String,
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
}

impl LocalStore {
    /// Create a local committed-artifact store rooted at `root`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Commit one object through temp write, fsync, rename, digest, sidecar, and manifest.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if path validation, object writing, fsync, rename, digesting,
    /// receipt writing, or manifest writing fails.
    pub fn commit<F>(&self, request: &Request, write_object: F) -> Result<Artifact, Error>
    where
        F: FnOnce(&mut File) -> Result<(), Error>,
    {
        self.prepare_root()?;
        let object = self.resolve_scoped("object", &request.object_path)?;
        let receipt = self.resolve_scoped("receipt", &request.receipt_path)?;
        let manifest = self.resolve_scoped("manifest", &request.manifest_path)?;
        prepare_parent(&object, "object")?;
        prepare_parent(&receipt, "receipt")?;
        prepare_parent(&manifest, "manifest")?;

        let digest = write_temp_promote_file(&object, "object", write_object)?;
        let object_path = manifest_path_string("object", &request.object_path)?;
        let entry = ManifestEntry::from_parts(&request.metadata, object_path.clone(), &digest);
        let receipt_doc = Receipt::from_parts(&request.metadata, object_path, &digest);

        write_json_temp_promote(&receipt, "receipt", &receipt_doc)?;
        write_manifest(&manifest, request.manifest_mode, &entry)?;

        Ok(Artifact {
            object_path: object,
            receipt_path: receipt,
            manifest_path: manifest,
            entry,
            receipt: receipt_doc,
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

impl Error {
    /// Build a caller-facing object writer error.
    #[must_use]
    pub fn writer(message: impl Into<String>) -> Self {
        Self::Writer(message.into())
    }
}

impl ManifestEntry {
    fn from_parts(metadata: &Metadata, object_path: String, digest: &DigestResult) -> Self {
        Self {
            run_id: metadata.run_id.clone(),
            shard: metadata.shard.clone(),
            file_sequence: metadata.file_sequence,
            dataset: metadata.dataset.clone(),
            object_path,
            row_count: metadata.row_count,
            bytes: digest.bytes,
            content_hash: digest.sha256.clone(),
            min_created_at_normalized: metadata.min_created_at_normalized.clone(),
            max_created_at_normalized: metadata.max_created_at_normalized.clone(),
            receipt_hash: metadata.receipt_hash.clone(),
            normalizer: metadata.normalizer.clone(),
            schema_version: metadata.schema_version,
        }
    }
}

impl Receipt {
    fn from_parts(metadata: &Metadata, object_path: String, digest: &DigestResult) -> Self {
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
struct DigestResult {
    bytes: u64,
    sha256: String,
}

fn write_temp_promote_file<F>(
    path: &Path,
    kind: &'static str,
    write: F,
) -> Result<DigestResult, Error>
where
    F: FnOnce(&mut File) -> Result<(), Error>,
{
    let temp_path = temp_path_for(path, kind)?;
    remove_stale_temp(&temp_path)?;
    let result = (|| {
        let mut file = File::create(&temp_path).map_err(|source| Error::Io {
            operation: "create temp file",
            path: temp_path.clone(),
            source,
        })?;
        write(&mut file)?;
        file.sync_all().map_err(|source| Error::Io {
            operation: "fsync temp file",
            path: temp_path.clone(),
            source,
        })?;
        drop(file);
        let temp_digest = hash_file(&temp_path)?;
        promote_no_overwrite(&temp_path, path, kind)?;
        let final_digest = hash_file(path)?;
        if final_digest.sha256 != temp_digest.sha256 || final_digest.bytes != temp_digest.bytes {
            return Err(Error::FinalHashMismatch {
                kind,
                path: path.to_path_buf(),
                expected: temp_digest.sha256,
                observed: final_digest.sha256,
            });
        }
        Ok(final_digest)
    })();
    let _ignored = fs::remove_file(&temp_path);
    result
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

fn write_manifest(path: &Path, mode: ManifestMode, entry: &ManifestEntry) -> Result<(), Error> {
    match mode {
        ManifestMode::AppendJsonl => append_manifest_jsonl(path, entry),
        ManifestMode::ReplaceJsonArray => write_json_temp_promote(path, "manifest", &[entry]),
    }
}

fn append_manifest_jsonl(path: &Path, entry: &ManifestEntry) -> Result<(), Error> {
    let _lock = ManifestAppendLock::acquire(path)?;
    let mut line = serde_json::to_vec(entry).map_err(|source| Error::Json {
        path: path.to_path_buf(),
        source,
    })?;
    line.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| Error::Io {
            operation: "open manifest for append",
            path: path.to_path_buf(),
            source,
        })?;
    file.write_all(&line).map_err(|source| Error::Io {
        operation: "write manifest record",
        path: path.to_path_buf(),
        source,
    })?;
    file.sync_all().map_err(|source| Error::Io {
        operation: "fsync manifest",
        path: path.to_path_buf(),
        source,
    })?;
    drop(file);
    sync_parent_dir(path, "manifest")
}

struct ManifestAppendLock {
    path: PathBuf,
}

impl ManifestAppendLock {
    fn acquire(path: &Path) -> Result<Self, Error> {
        let lock_path = manifest_lock_path(path)?;
        let mut attempts = 0_u16;
        loop {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(mut file) => {
                    writeln!(file, "{}", std::process::id()).map_err(|source| Error::Io {
                        operation: "write manifest append lock",
                        path: lock_path.clone(),
                        source,
                    })?;
                    file.sync_all().map_err(|source| Error::Io {
                        operation: "fsync manifest append lock",
                        path: lock_path.clone(),
                        source,
                    })?;
                    return Ok(Self { path: lock_path });
                }
                Err(source) if source.kind() == io::ErrorKind::AlreadyExists && attempts < 200 => {
                    attempts = attempts.saturating_add(1);
                    thread::sleep(Duration::from_millis(10));
                }
                Err(source) => {
                    return Err(Error::Io {
                        operation: "acquire manifest append lock",
                        path: lock_path,
                        source,
                    });
                }
            }
        }
    }
}

impl Drop for ManifestAppendLock {
    fn drop(&mut self) {
        let _ignored = fs::remove_file(&self.path);
    }
}

fn manifest_lock_path(path: &Path) -> Result<PathBuf, Error> {
    let file_name =
        path.file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| Error::MissingFileName {
                kind: "manifest lock",
                path: path.to_path_buf(),
            })?;
    Ok(path.with_file_name(format!(".{file_name}.lock")))
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

fn remove_stale_temp(path: &Path) -> Result<(), Error> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::Io {
            operation: "remove stale temp file",
            path: path.to_path_buf(),
            source,
        }),
    }
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

fn temp_path_for(path: &Path, kind: &'static str) -> Result<PathBuf, Error> {
    let file_name =
        path.file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| Error::MissingFileName {
                kind,
                path: path.to_path_buf(),
            })?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    Ok(path.with_file_name(format!(
        "{file_name}.tmp.{}.{}",
        std::process::id(),
        timestamp
    )))
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
mod tests {
    use std::{
        fs,
        io::Write,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use sha2::{Digest, Sha256};

    use super::{Error, LocalStore, ManifestEntry, ManifestMode, Metadata, Receipt, Request};
    use crate::archive::NormalizerVersion;

    fn normalizer() -> NormalizerVersion {
        NormalizerVersion {
            name: "emoji-normalizer".to_owned(),
            semver: "0.1.0".to_owned(),
            git_rev: "test".to_owned(),
            unicode_version: "16.0".to_owned(),
            emoji_data_version: "16.0".to_owned(),
        }
    }

    fn metadata(file_sequence: u64) -> Metadata {
        Metadata {
            run_id: "run-1".to_owned(),
            shard: "shard0".to_owned(),
            file_sequence,
            dataset: "raw_archive_posts".to_owned(),
            row_count: 2,
            min_created_at_normalized: Some("2026-06-01T00:00:00Z".to_owned()),
            max_created_at_normalized: Some("2026-06-02T00:00:00Z".to_owned()),
            receipt_hash: "repo-receipt-hash".to_owned(),
            normalizer: normalizer(),
            schema_version: 1,
        }
    }

    fn request(file_sequence: u64, mode: ManifestMode) -> Request {
        Request {
            object_path: PathBuf::from(format!("objects/run-1/shard0/{file_sequence}.parquet")),
            receipt_path: PathBuf::from(format!(
                "objects/run-1/shard0/{file_sequence}.receipt.json"
            )),
            manifest_path: PathBuf::from("manifests/raw.jsonl"),
            manifest_mode: mode,
            metadata: metadata(file_sequence),
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock should be after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "emojistats-commit-{name}-{}-{stamp}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("test temp dir should be created");
        path
    }

    fn read_json<T>(path: &Path) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        let bytes = fs::read(path).expect("test JSON file should be readable");
        serde_json::from_slice(&bytes).expect("test JSON should decode")
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex::encode(hasher.finalize())
    }

    #[test]
    fn commits_object_receipt_and_jsonl_manifest() {
        let root = temp_dir("jsonl");
        let store = LocalStore::new(&root);
        let artifact = store
            .commit(&request(1, ManifestMode::AppendJsonl), |file| {
                file.write_all(b"abc").map_err(|source| Error::Io {
                    operation: "test write",
                    path: PathBuf::from("test"),
                    source,
                })
            })
            .expect("commit should succeed");

        assert_eq!(
            fs::read(&artifact.object_path).expect("object should be readable"),
            b"abc"
        );
        assert_eq!(artifact.entry.bytes, 3);
        assert_eq!(artifact.entry.content_hash, sha256_hex(b"abc"));
        assert_eq!(artifact.entry.object_path, "objects/run-1/shard0/1.parquet");

        let receipt: Receipt = read_json(&artifact.receipt_path);
        assert_eq!(receipt, artifact.receipt);
        assert_eq!(receipt.protocol_version, 1);

        let manifest =
            fs::read_to_string(&artifact.manifest_path).expect("manifest should be readable");
        let mut lines = manifest.lines();
        let first = lines.next().expect("manifest should contain one entry");
        assert!(lines.next().is_none());
        let manifest_entry: ManifestEntry =
            serde_json::from_str(first).expect("manifest entry should decode");
        assert_eq!(manifest_entry, artifact.entry);

        fs::remove_dir_all(root).expect("test temp dir should be removed");
    }

    #[test]
    fn replace_manifest_writes_json_array() {
        let root = temp_dir("array");
        let store = LocalStore::new(&root);
        let artifact = store
            .commit(&request(7, ManifestMode::ReplaceJsonArray), |file| {
                file.write_all(b"payload").map_err(|source| Error::Io {
                    operation: "test write",
                    path: PathBuf::from("test"),
                    source,
                })
            })
            .expect("commit should succeed");

        let entries: Vec<ManifestEntry> = read_json(&artifact.manifest_path);
        assert_eq!(entries, vec![artifact.entry]);
        fs::remove_dir_all(root).expect("test temp dir should be removed");
    }

    #[test]
    fn failed_object_write_leaves_no_committed_outputs() {
        let root = temp_dir("failure");
        let store = LocalStore::new(&root);
        let result = store.commit(&request(1, ManifestMode::AppendJsonl), |_file| {
            Err(Error::writer("boom"))
        });

        assert!(matches!(result, Err(Error::Writer(message)) if message == "boom"));
        assert!(!root.join("objects/run-1/shard0/1.parquet").exists());
        assert!(!root.join("objects/run-1/shard0/1.receipt.json").exists());
        assert!(!root.join("manifests/raw.jsonl").exists());
        let object_dir = root.join("objects/run-1/shard0");
        let temp_count = fs::read_dir(object_dir)
            .expect("object dir should exist")
            .count();
        assert_eq!(temp_count, 0);
        fs::remove_dir_all(root).expect("test temp dir should be removed");
    }

    #[test]
    fn rejects_paths_that_escape_store_root() {
        let root = temp_dir("escape");
        let store = LocalStore::new(&root);
        let mut escaping = request(1, ManifestMode::AppendJsonl);
        escaping.object_path = PathBuf::from("../escape.parquet");

        let result = store.commit(&escaping, |file| {
            file.write_all(b"abc").map_err(|source| Error::Io {
                operation: "test write",
                path: PathBuf::from("test"),
                source,
            })
        });

        assert!(matches!(
            result,
            Err(Error::PathEscapesRoot { kind: "object", .. })
        ));
        assert!(!root.join("../escape.parquet").exists());
        fs::remove_dir_all(root).expect("test temp dir should be removed");
    }
}
