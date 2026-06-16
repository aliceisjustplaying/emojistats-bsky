//! Storage Box-shaped remote commit protocol skeleton.

use std::{
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::commit::{DigestResult, ManifestEntry, ManifestMode, Receipt, Request};

mod ssh;

use ssh::{CommandSpec, remote_parent, run_command, shell_quote, validate_remote_path};

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

    /// Atomically promote a verified temp file to its final path without overwriting a final file.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError`] when the underlying remote rename command fails.
    fn rename(&mut self, from: &str, to: &str) -> Result<(), CommandError>;

    /// Append bytes to the manifest.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError`] when the underlying remote append command fails.
    fn append(&mut self, remote_path: &str, bytes: &[u8]) -> Result<(), CommandError>;
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
}

impl SshStorageBoxCommands {
    /// Build an SSH-backed Storage Box command executor.
    #[must_use]
    pub const fn new(config: StorageBoxSshConfig) -> Self {
        Self { config }
    }

    fn upload_command(&self, remote_path: &str) -> Result<CommandSpec, CommandError> {
        let parent = remote_parent(remote_path)?;
        let script = format!(
            "umask 077; mkdir -p -- {}; cat > {}",
            shell_quote(&parent),
            shell_quote(remote_path)
        );
        self.config.ssh_command("upload", script, true)
    }

    fn stat_len_command(&self, remote_path: &str) -> Result<CommandSpec, CommandError> {
        validate_remote_path(remote_path)?;
        let path = shell_quote(remote_path);
        let script = format!("if [ -e {path} ]; then wc -c < {path}; fi");
        self.config.ssh_command("stat", script, false)
    }

    fn sha256_command(&self, remote_path: &str) -> Result<CommandSpec, CommandError> {
        validate_remote_path(remote_path)?;
        let path = shell_quote(remote_path);
        let script = format!("if [ -e {path} ]; then sha256sum -- {path} | awk '{{print $1}}'; fi");
        self.config.ssh_command("sha256", script, false)
    }

    fn read_prefix_command(
        &self,
        remote_path: &str,
        max_bytes: usize,
    ) -> Result<CommandSpec, CommandError> {
        validate_remote_path(remote_path)?;
        let path = shell_quote(remote_path);
        let script = format!(
            "if [ -e {path} ]; then printf 'present\\n'; head -c {max_bytes} -- {path}; else printf 'absent\\n'; fi"
        );
        self.config.ssh_command("read_prefix", script, false)
    }

    fn rename_command(&self, from: &str, to: &str) -> Result<CommandSpec, CommandError> {
        validate_remote_path(from)?;
        let parent = remote_parent(to)?;
        let script = format!(
            "mkdir -p -- {}; if [ -e {} ]; then printf '%s\\n' {}; exit 17; fi; mv -n -- {} {}; if [ -e {} ]; then printf '%s\\n' {}; exit 17; fi",
            shell_quote(&parent),
            shell_quote(to),
            shell_quote(&format!("final path already exists: {to}")),
            shell_quote(from),
            shell_quote(to),
            shell_quote(from),
            shell_quote(&format!("final path appeared during promotion: {to}"))
        );
        self.config.ssh_command("rename", script, false)
    }

    fn append_command(&self, remote_path: &str) -> Result<CommandSpec, CommandError> {
        let parent = remote_parent(remote_path)?;
        let lock_path = format!("{remote_path}.lock");
        let script = format!(
            "umask 077; mkdir -p -- {}; touch -- {}; flock -- {} sh -c 'cat >> \"$1\"' sh {}",
            shell_quote(&parent),
            shell_quote(&lock_path),
            shell_quote(&lock_path),
            shell_quote(remote_path)
        );
        self.config.ssh_command("append", script, true)
    }
}

impl StorageBoxCommands for SshStorageBoxCommands {
    fn upload(&mut self, remote_path: &str, bytes: &[u8]) -> Result<(), CommandError> {
        let command = self.upload_command(remote_path)?;
        run_command(&command, Some(bytes)).map(|_stdout| ())
    }

    fn stat_len(&mut self, remote_path: &str) -> Result<Option<u64>, CommandError> {
        let command = self.stat_len_command(remote_path)?;
        let stdout = run_command(&command, None)?;
        let text = std::str::from_utf8(&stdout)
            .map_err(|error| CommandError::new(format!("stat output was not UTF-8: {error}")))?;
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        trimmed.parse::<u64>().map(Some).map_err(|error| {
            CommandError::new(format!("stat output was not a byte count: {error}"))
        })
    }

    fn sha256(&mut self, remote_path: &str) -> Result<Option<String>, CommandError> {
        let command = self.sha256_command(remote_path)?;
        let stdout = run_command(&command, None)?;
        let text = std::str::from_utf8(&stdout)
            .map_err(|error| CommandError::new(format!("sha256 output was not UTF-8: {error}")))?;
        let trimmed = text.trim();
        if trimmed.is_empty() {
            Ok(None)
        } else {
            Ok(Some(trimmed.to_owned()))
        }
    }

    fn read_prefix(
        &mut self,
        remote_path: &str,
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>, CommandError> {
        let command = self.read_prefix_command(remote_path, max_bytes)?;
        let stdout = run_command(&command, None)?;
        stdout.strip_prefix(b"present\n").map_or_else(
            || {
                if stdout == b"absent\n" {
                    Ok(None)
                } else {
                    Err(CommandError::new(
                        "read prefix output had no presence marker",
                    ))
                }
            },
            |prefix| Ok(Some(prefix.to_vec())),
        )
    }

    fn rename(&mut self, from: &str, to: &str) -> Result<(), CommandError> {
        let command = self.rename_command(from, to)?;
        run_command(&command, None).map(|_stdout| ())
    }

    fn append(&mut self, remote_path: &str, bytes: &[u8]) -> Result<(), CommandError> {
        let command = self.append_command(remote_path)?;
        run_command(&command, Some(bytes)).map(|_stdout| ())
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
        if request.manifest_mode != ManifestMode::AppendJsonl {
            return Err(Error::UnsupportedManifestMode);
        }

        let paths = RemotePaths::for_request(&self.config, request)?;
        let object_digest = digest_bytes("object", object_bytes)?;
        self.commands
            .upload(&paths.temp_object, object_bytes)
            .map_err(|source| Error::Command {
                operation: "upload object temp",
                path: paths.temp_object.clone(),
                source,
            })?;
        verify_remote_bytes(
            &mut self.commands,
            &paths.temp_object,
            object_bytes,
            &object_digest,
            self.config.readback_bytes,
        )?;
        promote_temp_to_final(
            &mut self.commands,
            &paths.temp_object,
            &paths.object,
            &object_digest,
            "object",
        )?;

        let entry = ManifestEntry::from_parts(
            &request.metadata,
            paths.object_manifest_path,
            &object_digest,
        );
        let receipt =
            Receipt::from_parts(&request.metadata, entry.object_path.clone(), &object_digest);
        let receipt_bytes = json_bytes("receipt", &receipt)?;
        let receipt_digest = digest_bytes("receipt", &receipt_bytes)?;
        self.commands
            .upload(&paths.temp_receipt, &receipt_bytes)
            .map_err(|source| Error::Command {
                operation: "upload receipt temp",
                path: paths.temp_receipt.clone(),
                source,
            })?;
        verify_remote_bytes(
            &mut self.commands,
            &paths.temp_receipt,
            &receipt_bytes,
            &receipt_digest,
            self.config.readback_bytes,
        )?;
        promote_temp_to_final(
            &mut self.commands,
            &paths.temp_receipt,
            &paths.receipt,
            &receipt_digest,
            "receipt",
        )?;

        let manifest_line = jsonl_bytes("manifest", &entry)?;
        self.commands
            .append(&paths.manifest, &manifest_line)
            .map_err(|source| Error::Command {
                operation: "append manifest",
                path: paths.manifest.clone(),
                source,
            })?;

        Ok(RemoteArtifact {
            remote_object_path: paths.object,
            remote_temp_object_path: paths.temp_object,
            remote_receipt_path: paths.receipt,
            remote_temp_receipt_path: paths.temp_receipt,
            remote_manifest_path: paths.manifest,
            entry,
            receipt,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemotePaths {
    object: String,
    temp_object: String,
    receipt: String,
    temp_receipt: String,
    manifest: String,
    object_manifest_path: String,
}

impl RemotePaths {
    fn for_request(config: &StorageBoxConfig, request: &Request) -> Result<Self, Error> {
        let root = normalize_root(&config.remote_root)?;
        let temp_directory = normalize_temp_directory(&config.temp_directory)?;
        let object_manifest_path = manifest_path_string("object", &request.object_path)?;
        let receipt_manifest_path = manifest_path_string("receipt", &request.receipt_path)?;
        let manifest_path = manifest_path_string("manifest", &request.manifest_path)?;
        let object = join_remote(&root, &object_manifest_path);
        let receipt = join_remote(&root, &receipt_manifest_path);
        let manifest = join_remote(&root, &manifest_path);
        let temp_base = join_remote(&root, &temp_directory);
        let temp_run = join_remote(
            &temp_base,
            &safe_component("run id", &request.metadata.run_id)?,
        );
        let temp_shard = join_remote(
            &temp_run,
            &safe_component("shard", &request.metadata.shard)?,
        );
        let temp_object = join_remote(
            &temp_shard,
            &temp_name_for(
                "object",
                &request.object_path,
                request.metadata.file_sequence,
            )?,
        );
        let temp_receipt = join_remote(
            &temp_shard,
            &temp_name_for(
                "receipt",
                &request.receipt_path,
                request.metadata.file_sequence,
            )?,
        );

        Ok(Self {
            object,
            temp_object,
            receipt,
            temp_receipt,
            manifest,
            object_manifest_path,
        })
    }
}

fn normalize_root(root: &str) -> Result<String, Error> {
    let trimmed = root.trim_end_matches('/');
    if trimmed.is_empty() || !trimmed.starts_with('/') {
        return Err(Error::InvalidRemoteRoot(root.to_owned()));
    }
    Ok(trimmed.to_owned())
}

fn normalize_temp_directory(path: &Path) -> Result<String, Error> {
    let normalized = relative_path_string("temp directory", path).map_err(|error| match error {
        Error::PathEscapesRoot { path, .. } => Error::TempDirectoryEscapesRoot { path },
        other => other,
    })?;
    Ok(normalized)
}

fn manifest_path_string(kind: &'static str, path: &Path) -> Result<String, Error> {
    relative_path_string(kind, path)
}

fn relative_path_string(kind: &'static str, path: &Path) -> Result<String, Error> {
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

fn safe_component(kind: &'static str, value: &str) -> Result<String, Error> {
    let path = PathBuf::from(value);
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(component)), None) => component
            .to_str()
            .map(ToOwned::to_owned)
            .ok_or(Error::NonUtf8Path { kind, path }),
        _ => Err(Error::PathEscapesRoot { kind, path }),
    }
}

fn temp_name_for(kind: &'static str, path: &Path, file_sequence: u64) -> Result<String, Error> {
    let file_name = path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .ok_or_else(|| Error::MissingFileName {
            kind,
            path: path.to_path_buf(),
        })?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    Ok(format!(
        "{file_name}.tmp.{file_sequence}.{}.{}",
        std::process::id(),
        timestamp
    ))
}

fn join_remote(root: &str, relative: &str) -> String {
    format!("{root}/{relative}")
}

fn digest_bytes(kind: &'static str, bytes: &[u8]) -> Result<DigestResult, Error> {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let byte_count =
        u64::try_from(bytes.len()).map_err(|_error| Error::ByteCountOverflow { kind })?;
    Ok(DigestResult {
        bytes: byte_count,
        sha256: hex::encode(hasher.finalize()),
    })
}

fn verify_remote_bytes<C>(
    commands: &mut C,
    remote_path: &str,
    expected_bytes: &[u8],
    expected_digest: &DigestResult,
    readback_bytes: usize,
) -> Result<(), Error>
where
    C: StorageBoxCommands,
{
    let actual_len = commands
        .stat_len(remote_path)
        .map_err(|source| Error::Command {
            operation: "stat uploaded file",
            path: remote_path.to_owned(),
            source,
        })?;
    match actual_len {
        Some(actual) if actual == expected_digest.bytes => {}
        Some(actual) => {
            return Err(Error::VerifySizeMismatch {
                path: remote_path.to_owned(),
                expected: expected_digest.bytes,
                actual,
            });
        }
        None => {
            return Err(Error::MissingRemoteFile {
                operation: "stat uploaded file",
                path: remote_path.to_owned(),
            });
        }
    }

    let actual_hash = commands
        .sha256(remote_path)
        .map_err(|source| Error::Command {
            operation: "hash uploaded file",
            path: remote_path.to_owned(),
            source,
        })?;
    match actual_hash {
        Some(actual) if actual == expected_digest.sha256 => {}
        Some(actual) => {
            return Err(Error::VerifyHashMismatch {
                path: remote_path.to_owned(),
                expected: expected_digest.sha256.clone(),
                actual,
            });
        }
        None => {
            return Err(Error::MissingRemoteFile {
                operation: "hash uploaded file",
                path: remote_path.to_owned(),
            });
        }
    }

    let expected_prefix_len = expected_bytes.len().min(readback_bytes);
    let expected_prefix =
        expected_bytes
            .get(..expected_prefix_len)
            .ok_or_else(|| Error::VerifyReadbackMismatch {
                path: remote_path.to_owned(),
            })?;
    let actual_prefix = commands
        .read_prefix(remote_path, readback_bytes)
        .map_err(|source| Error::Command {
            operation: "read uploaded file prefix",
            path: remote_path.to_owned(),
            source,
        })?;
    match actual_prefix {
        Some(actual) if actual.as_slice() == expected_prefix => Ok(()),
        Some(_) => Err(Error::VerifyReadbackMismatch {
            path: remote_path.to_owned(),
        }),
        None => Err(Error::MissingRemoteFile {
            operation: "read uploaded file prefix",
            path: remote_path.to_owned(),
        }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinalState {
    Absent,
    Exact,
}

fn promote_temp_to_final<C>(
    commands: &mut C,
    temp_path: &str,
    final_path: &str,
    expected_digest: &DigestResult,
    artifact_kind: &'static str,
) -> Result<(), Error>
where
    C: StorageBoxCommands,
{
    match check_final_state(commands, final_path, expected_digest)? {
        FinalState::Exact => return Ok(()),
        FinalState::Absent => {}
    }

    let rename_result = commands.rename(temp_path, final_path);
    match rename_result {
        Ok(()) => verify_remote_final(commands, final_path, expected_digest),
        Err(source) => match check_final_state(commands, final_path, expected_digest)? {
            FinalState::Exact => Ok(()),
            FinalState::Absent => Err(Error::Command {
                operation: match artifact_kind {
                    "object" => "promote object temp",
                    "receipt" => "promote receipt temp",
                    _ => "promote temp",
                },
                path: final_path.to_owned(),
                source,
            }),
        },
    }
}

fn check_final_state<C>(
    commands: &mut C,
    final_path: &str,
    expected_digest: &DigestResult,
) -> Result<FinalState, Error>
where
    C: StorageBoxCommands,
{
    let actual_len = commands
        .stat_len(final_path)
        .map_err(|source| Error::Command {
            operation: "stat final file",
            path: final_path.to_owned(),
            source,
        })?;
    match actual_len {
        None => Ok(FinalState::Absent),
        Some(actual) if actual != expected_digest.bytes => Err(Error::FinalExistsConflict {
            path: final_path.to_owned(),
            reason: format!(
                "expected {} bytes, found {actual} bytes",
                expected_digest.bytes
            ),
        }),
        Some(_) => {
            let actual_hash = commands
                .sha256(final_path)
                .map_err(|source| Error::Command {
                    operation: "hash final file",
                    path: final_path.to_owned(),
                    source,
                })?;
            match actual_hash {
                Some(actual) if actual == expected_digest.sha256 => Ok(FinalState::Exact),
                Some(actual) => Err(Error::FinalExistsConflict {
                    path: final_path.to_owned(),
                    reason: format!("expected sha256 {}, found {actual}", expected_digest.sha256),
                }),
                None => Err(Error::MissingRemoteFile {
                    operation: "hash final file",
                    path: final_path.to_owned(),
                }),
            }
        }
    }
}

fn verify_remote_final<C>(
    commands: &mut C,
    remote_path: &str,
    expected_digest: &DigestResult,
) -> Result<(), Error>
where
    C: StorageBoxCommands,
{
    let state = check_final_state(commands, remote_path, expected_digest)?;
    match state {
        FinalState::Exact => Ok(()),
        FinalState::Absent => Err(Error::MissingRemoteFile {
            operation: "stat final file",
            path: remote_path.to_owned(),
        }),
    }
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
