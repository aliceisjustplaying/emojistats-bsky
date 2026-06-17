//! Storage Box-shaped remote commit protocol skeleton.

use std::{
    io::Read,
    path::{Path, PathBuf},
};

use serde::Serialize;

use crate::commit::{CommitPlan, ManifestEntry, ManifestMode, Receipt, Request};

mod paths;
mod source;
mod ssh;

use paths::{
    RemotePaths, join_remote, manifest_path_string, normalize_root, normalize_temp_directory,
    safe_component, temp_name_for,
};
use source::{UploadSource, cleanup_remote_temp, upload_verify_promote};

const DEFAULT_READBACK_BYTES: usize = 4_096;

/// Typed configuration for a bounded remote Storage Box write scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageBoxConfig {
    /// Absolute remote directory that bounds every write this backend performs.
    pub remote_root: String,
    /// Temp directory relative to `remote_root`.
    pub temp_directory: PathBuf,
    /// Maximum bytes read back from an uploaded file before rename.
    pub readback_bytes: usize,
}

/// Command boundary for the eventual Storage Box transport.
pub trait StorageBoxCommands {
    /// Upload bytes to a remote temp path.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError`] when the underlying remote upload command fails.
    fn upload(&mut self, remote_path: &str, bytes: &[u8]) -> Result<(), CommandError>;

    /// Upload bytes from a reader to a remote temp path.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError`] when local reading or the underlying remote upload command fails.
    fn upload_reader(
        &mut self,
        remote_path: &str,
        reader: &mut (dyn Read + Send),
    ) -> Result<(), CommandError>;

    /// Return the remote file length, or `None` when the path is absent.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError`] when the underlying remote stat command fails.
    fn stat_len(&mut self, remote_path: &str) -> Result<Option<u64>, CommandError>;

    /// Return the remote SHA-256 hash, or `None` when the path is absent.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError`] when the underlying remote hash command fails.
    fn sha256(&mut self, remote_path: &str) -> Result<Option<String>, CommandError>;

    /// Return up to `max_bytes` from the start of the remote file, or `None` when absent.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError`] when the underlying remote read command fails.
    fn read_prefix(
        &mut self,
        remote_path: &str,
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>, CommandError>;

    /// Remove a remote path if it exists.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError`] when the underlying remote remove command fails.
    fn remove(&mut self, remote_path: &str) -> Result<(), CommandError>;

    /// Atomically promote a verified temp file to its final path without overwriting a final file.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError`] when the underlying remote rename command fails.
    fn rename(&mut self, from: &str, to: &str) -> Result<(), CommandError>;

    /// Append a manifest record only if it is absent, with the check and append under one lock.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError`] when the underlying remote append command fails.
    fn append_manifest_record_if_missing(
        &mut self,
        remote_path: &str,
        record_without_newline: &[u8],
    ) -> Result<(), CommandError>;

    /// Return whether a manifest already contains an exact JSONL record.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError`] when the underlying remote read/search command fails.
    fn contains_manifest_record(
        &mut self,
        remote_path: &str,
        record_without_newline: &[u8],
    ) -> Result<bool, CommandError>;
}

/// Remote Storage Box backend using an injectable command executor.
#[derive(Debug, Clone)]
pub struct StorageBoxBackend<C> {
    config: StorageBoxConfig,
    commands: C,
}

/// SSH-based command configuration for a Hetzner Storage Box-compatible POSIX endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageBoxSshConfig {
    /// SSH binary to execute locally.
    pub ssh_program: PathBuf,
    /// SSH destination, for example `u123456@u123456.your-storagebox.de`.
    pub remote: String,
    /// Extra SSH arguments, such as `-p`, `23`, or `-i`, `/path/to/key`.
    pub ssh_args: Vec<String>,
    /// Maximum time one SSH command may run before it is killed.
    pub command_timeout: std::time::Duration,
}

/// Storage Box command executor backed by local `ssh` processes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshStorageBoxCommands {
    config: StorageBoxSshConfig,
}

/// Completed remote commit result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteArtifact {
    /// Final absolute remote object path.
    pub remote_object_path: String,
    /// Absolute remote object temp path used before rename.
    pub remote_temp_object_path: String,
    /// Final absolute remote receipt path.
    pub remote_receipt_path: String,
    /// Absolute remote receipt temp path used before rename.
    pub remote_temp_receipt_path: String,
    /// Absolute remote manifest path.
    pub remote_manifest_path: String,
    /// Manifest entry appended after final paths exist.
    pub entry: ManifestEntry,
    /// Receipt sidecar uploaded before manifest append.
    pub receipt: Receipt,
}

/// Command adapter error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandError {
    message: String,
}

/// Remote commit protocol failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The configured remote root is not an absolute bounded path.
    #[error("remote root must be an absolute path: {0}")]
    InvalidRemoteRoot(String),
    /// Remote temp directory path attempted to leave the configured root.
    #[error("temp directory escapes remote root: {}", path.display())]
    TempDirectoryEscapesRoot { path: PathBuf },
    /// A path attempted to leave the configured remote root.
    #[error("{kind} path escapes remote root: {}", path.display())]
    PathEscapesRoot { kind: &'static str, path: PathBuf },
    /// A path had no usable file name.
    #[error("{kind} path has no file name: {}", path.display())]
    MissingFileName { kind: &'static str, path: PathBuf },
    /// A path component could not be encoded for the remote shell boundary.
    #[error("{kind} path is not valid UTF-8: {}", path.display())]
    NonUtf8Path { kind: &'static str, path: PathBuf },
    /// A manifest mode cannot preserve append-only remote semantics.
    #[error("remote Storage Box manifests are append-only JSONL")]
    UnsupportedManifestMode,
    /// Local byte count overflowed the manifest schema.
    #[error("byte count overflow while preparing {kind}")]
    ByteCountOverflow { kind: &'static str },
    /// JSON serialization failed.
    #[error("JSON write failed for {kind}: {source}")]
    Json {
        kind: &'static str,
        #[source]
        source: serde_json::Error,
    },
    /// A local source file could not be read.
    #[error("local {operation} failed for {}: {source}", path.display())]
    LocalIo {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A remote command failed.
    #[error("{operation} failed for {path}: {source}")]
    Command {
        operation: &'static str,
        path: String,
        #[source]
        source: CommandError,
    },
    /// Uploaded remote file size did not match the local source.
    #[error("remote size mismatch for {path}: expected {expected}, actual {actual}")]
    VerifySizeMismatch {
        path: String,
        expected: u64,
        actual: u64,
    },
    /// Uploaded remote file was missing during verification.
    #[error("remote file missing during {operation}: {path}")]
    MissingRemoteFile {
        operation: &'static str,
        path: String,
    },
    /// Uploaded remote file hash did not match the local source.
    #[error("remote hash mismatch for {path}: expected {expected}, actual {actual}")]
    VerifyHashMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    /// A final path already exists with different content.
    #[error("remote final path already exists with different content for {path}: {reason}")]
    FinalExistsConflict { path: String, reason: String },
    /// Uploaded remote readback prefix did not match the local source.
    #[error("remote readback mismatch for {path}")]
    VerifyReadbackMismatch { path: String },
}

impl StorageBoxConfig {
    /// Build config for a Storage Box directory root.
    #[must_use]
    pub fn new(remote_root: impl Into<String>) -> Self {
        Self {
            remote_root: remote_root.into(),
            temp_directory: PathBuf::from(".tmp"),
            readback_bytes: DEFAULT_READBACK_BYTES,
        }
    }
}

impl StorageBoxSshConfig {
    /// Build an SSH command config for a remote Storage Box account.
    #[must_use]
    pub fn new(remote: impl Into<String>) -> Self {
        Self {
            ssh_program: PathBuf::from("ssh"),
            remote: remote.into(),
            ssh_args: Vec::new(),
            command_timeout: std::time::Duration::from_secs(300),
        }
    }

    /// Set the SSH binary path.
    #[must_use]
    pub fn with_ssh_program(mut self, ssh_program: impl Into<PathBuf>) -> Self {
        self.ssh_program = ssh_program.into();
        self
    }

    /// Add one SSH argument.
    #[must_use]
    pub fn with_ssh_arg(mut self, arg: impl Into<String>) -> Self {
        self.ssh_args.push(arg.into());
        self
    }

    /// Set the maximum runtime for one SSH command.
    #[must_use]
    pub const fn with_command_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.command_timeout = timeout;
        self
    }
}

impl SshStorageBoxCommands {
    /// Build an SSH-backed Storage Box command executor.
    #[must_use]
    pub const fn new(config: StorageBoxSshConfig) -> Self {
        Self { config }
    }
}

impl CommandError {
    /// Build a command error from a message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CommandError {}

impl<C> StorageBoxBackend<C> {
    /// Create a backend around a typed config and command executor.
    #[must_use]
    pub const fn new(config: StorageBoxConfig, commands: C) -> Self {
        Self { config, commands }
    }

    /// Return the command executor after tests or callers finish with the backend.
    #[must_use]
    pub fn into_commands(self) -> C {
        self.commands
    }
}

impl<C> StorageBoxBackend<C>
where
    C: StorageBoxCommands,
{
    /// Commit one object through remote temp upload, verification, rename, receipt, and manifest.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if path validation, upload, verification, rename, receipt serialization,
    /// or manifest append fails.
    pub fn commit_bytes(
        &mut self,
        request: &Request,
        object_bytes: &[u8],
    ) -> Result<RemoteArtifact, Error> {
        self.commit_source(request, UploadSource::Bytes(object_bytes))
    }

    /// Commit one local object file without buffering the object bytes in memory.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if local reading, path validation, upload, verification, rename,
    /// receipt serialization, or manifest append fails.
    pub fn commit_file(
        &mut self,
        request: &Request,
        object_path: &Path,
    ) -> Result<RemoteArtifact, Error> {
        self.commit_source(
            request,
            UploadSource::File {
                path: object_path,
                open_operation: "open object source",
            },
        )
    }

    fn commit_source(
        &mut self,
        request: &Request,
        object_source: UploadSource<'_>,
    ) -> Result<RemoteArtifact, Error> {
        if request.manifest_mode != ManifestMode::AppendJsonl {
            return Err(Error::UnsupportedManifestMode);
        }

        let paths = RemotePaths::for_request(&self.config, request)?;
        let result = commit_source_pipeline(
            &mut self.commands,
            request,
            &paths,
            object_source,
            self.config.readback_bytes,
        );
        cleanup_remote_temps_on_error(&mut self.commands, &paths, &result);
        result
    }

    /// Upload and promote an auxiliary file without appending a manifest entry.
    ///
    /// This is used for repo-level receipts that manifest entries refer to by hash.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the local source cannot be read, the remote upload cannot be
    /// verified, or the final remote path already exists with different content.
    pub fn commit_auxiliary_file(
        &mut self,
        request: &Request,
        final_path: &Path,
        source_path: &Path,
        artifact_kind: &'static str,
    ) -> Result<String, Error> {
        let root = normalize_root(&self.config.remote_root)?;
        let temp_directory = normalize_temp_directory(&self.config.temp_directory)?;
        let final_manifest_path = manifest_path_string(artifact_kind, final_path)?;
        let final_remote_path = join_remote(&root, &final_manifest_path);
        let temp_base = join_remote(&root, &temp_directory);
        let temp_run = join_remote(
            &temp_base,
            &safe_component("run id", &request.metadata.run_id)?,
        );
        let temp_shard = join_remote(
            &temp_run,
            &safe_component("shard", &request.metadata.shard)?,
        );
        let temp_path = join_remote(
            &temp_shard,
            &temp_name_for(artifact_kind, final_path, request.metadata.file_sequence)?,
        );

        let result = upload_verify_promote(
            &mut self.commands,
            &temp_path,
            &final_remote_path,
            artifact_kind,
            UploadSource::File {
                path: source_path,
                open_operation: "open auxiliary source",
            },
            self.config.readback_bytes,
        )
        .map(|_digest| final_remote_path.clone());
        if result.is_err() {
            let _ignored = self.commands.remove(&temp_path);
        }
        result
    }
}

fn cleanup_remote_temps_on_error<C>(
    commands: &mut C,
    paths: &RemotePaths,
    result: &Result<RemoteArtifact, Error>,
) where
    C: StorageBoxCommands,
{
    if result.is_ok() {
        return;
    }
    let _ignored = cleanup_remote_temp(commands, &paths.temp_object, "object");
    let _ignored = cleanup_remote_temp(commands, &paths.temp_receipt, "receipt");
}

fn commit_source_pipeline<C>(
    commands: &mut C,
    request: &Request,
    paths: &RemotePaths,
    object_source: UploadSource<'_>,
    readback_bytes: usize,
) -> Result<RemoteArtifact, Error>
where
    C: StorageBoxCommands,
{
    let object_digest = upload_verify_promote(
        commands,
        &paths.temp_object,
        &paths.object,
        "object",
        object_source,
        readback_bytes,
    )?;

    let plan = CommitPlan::from_digest(
        &request.metadata,
        paths.object_manifest_path.clone(),
        &object_digest,
    );
    let receipt_bytes = json_bytes("receipt", &plan.receipt)?;
    upload_verify_promote(
        commands,
        &paths.temp_receipt,
        &paths.receipt,
        "receipt",
        UploadSource::Bytes(&receipt_bytes),
        readback_bytes,
    )?;

    let manifest_line = jsonl_bytes("manifest", &plan.entry)?;
    append_manifest_if_missing(commands, &paths.manifest, &manifest_line)?;

    Ok(RemoteArtifact {
        remote_object_path: paths.object.clone(),
        remote_temp_object_path: paths.temp_object.clone(),
        remote_receipt_path: paths.receipt.clone(),
        remote_temp_receipt_path: paths.temp_receipt.clone(),
        remote_manifest_path: paths.manifest.clone(),
        entry: plan.entry,
        receipt: plan.receipt,
    })
}

fn append_manifest_if_missing<C>(
    commands: &mut C,
    manifest_path: &str,
    manifest_line: &[u8],
) -> Result<(), Error>
where
    C: StorageBoxCommands,
{
    let record = manifest_record_without_newline(manifest_line);
    commands
        .append_manifest_record_if_missing(manifest_path, record)
        .map_err(|source| Error::Command {
            operation: "append manifest if missing",
            path: manifest_path.to_owned(),
            source,
        })?;
    if commands
        .contains_manifest_record(manifest_path, record)
        .map_err(|source| Error::Command {
            operation: "verify manifest entry",
            path: manifest_path.to_owned(),
            source,
        })?
    {
        Ok(())
    } else {
        Err(Error::MissingRemoteFile {
            operation: "verify manifest entry",
            path: manifest_path.to_owned(),
        })
    }
}

fn manifest_record_without_newline(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\n").unwrap_or(line)
}

fn json_bytes<T>(kind: &'static str, value: &T) -> Result<Vec<u8>, Error>
where
    T: Serialize,
{
    let mut bytes =
        serde_json::to_vec_pretty(value).map_err(|source| Error::Json { kind, source })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn jsonl_bytes<T>(kind: &'static str, value: &T) -> Result<Vec<u8>, Error>
where
    T: Serialize,
{
    let mut bytes = serde_json::to_vec(value).map_err(|source| Error::Json { kind, source })?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests;
