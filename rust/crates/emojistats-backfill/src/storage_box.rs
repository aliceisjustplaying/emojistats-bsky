//! Storage Box-shaped remote commit protocol skeleton.

use std::{
    io::Write,
    path::{Component, Path, PathBuf},
    process::{Command as ProcessCommand, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::commit::{ManifestEntry, ManifestMode, Metadata, Receipt, Request};

const DEFAULT_READBACK_BYTES: usize = 4_096;
const PROTOCOL_VERSION: u16 = 1;

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

    /// Atomically move a verified temp file to its final path.
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

    fn upload_command(&self, remote_path: &str, bytes: &[u8]) -> Result<CommandSpec, CommandError> {
        let parent = remote_parent(remote_path)?;
        let script = format!(
            "umask 077; mkdir -p -- {}; cat > {}",
            shell_quote(&parent),
            shell_quote(remote_path)
        );
        self.config
            .ssh_command("upload", script, Some(bytes.to_vec()))
    }

    fn stat_len_command(&self, remote_path: &str) -> Result<CommandSpec, CommandError> {
        validate_remote_path(remote_path)?;
        let path = shell_quote(remote_path);
        let script = format!("if [ -e {path} ]; then wc -c < {path}; fi");
        self.config.ssh_command("stat", script, None)
    }

    fn sha256_command(&self, remote_path: &str) -> Result<CommandSpec, CommandError> {
        validate_remote_path(remote_path)?;
        let path = shell_quote(remote_path);
        let script = format!("if [ -e {path} ]; then sha256sum -- {path} | awk '{{print $1}}'; fi");
        self.config.ssh_command("sha256", script, None)
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
        self.config.ssh_command("read_prefix", script, None)
    }

    fn rename_command(&self, from: &str, to: &str) -> Result<CommandSpec, CommandError> {
        validate_remote_path(from)?;
        let parent = remote_parent(to)?;
        let script = format!(
            "mkdir -p -- {}; mv -f -- {} {}",
            shell_quote(&parent),
            shell_quote(from),
            shell_quote(to)
        );
        self.config.ssh_command("rename", script, None)
    }

    fn append_command(&self, remote_path: &str, bytes: &[u8]) -> Result<CommandSpec, CommandError> {
        let parent = remote_parent(remote_path)?;
        let script = format!(
            "umask 077; mkdir -p -- {}; cat >> {}",
            shell_quote(&parent),
            shell_quote(remote_path)
        );
        self.config
            .ssh_command("append", script, Some(bytes.to_vec()))
    }
}

impl StorageBoxCommands for SshStorageBoxCommands {
    fn upload(&mut self, remote_path: &str, bytes: &[u8]) -> Result<(), CommandError> {
        run_command(self.upload_command(remote_path, bytes)?).map(|_stdout| ())
    }

    fn stat_len(&mut self, remote_path: &str) -> Result<Option<u64>, CommandError> {
        let stdout = run_command(self.stat_len_command(remote_path)?)?;
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
        let stdout = run_command(self.sha256_command(remote_path)?)?;
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
        let stdout = run_command(self.read_prefix_command(remote_path, max_bytes)?)?;
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
        run_command(self.rename_command(from, to)?).map(|_stdout| ())
    }

    fn append(&mut self, remote_path: &str, bytes: &[u8]) -> Result<(), CommandError> {
        run_command(self.append_command(remote_path, bytes)?).map(|_stdout| ())
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommandSpec {
    operation: &'static str,
    program: PathBuf,
    args: Vec<String>,
    stdin: Option<Vec<u8>>,
}

impl StorageBoxSshConfig {
    fn ssh_command(
        &self,
        operation: &'static str,
        script: String,
        stdin: Option<Vec<u8>>,
    ) -> Result<CommandSpec, CommandError> {
        validate_remote(&self.remote)?;
        let mut args = self.ssh_args.clone();
        args.push(self.remote.clone());
        args.push(script);
        Ok(CommandSpec {
            operation,
            program: self.ssh_program.clone(),
            args,
            stdin,
        })
    }
}

fn run_command(spec: CommandSpec) -> Result<Vec<u8>, CommandError> {
    let mut command = ProcessCommand::new(&spec.program);
    command.args(&spec.args);
    if spec.stdin.is_some() {
        command.stdin(Stdio::piped());
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|error| CommandError::new(format!("{} spawn failed: {error}", spec.operation)))?;
    if let Some(stdin_bytes) = spec.stdin {
        let mut stdin = child.stdin.take().ok_or_else(|| {
            CommandError::new(format!("{} stdin was not available", spec.operation))
        })?;
        stdin.write_all(&stdin_bytes).map_err(|error| {
            CommandError::new(format!("{} stdin write failed: {error}", spec.operation))
        })?;
    }
    let output = child
        .wait_with_output()
        .map_err(|error| CommandError::new(format!("{} wait failed: {error}", spec.operation)))?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(CommandError::new(format!(
            "{} exited with {}: {}",
            spec.operation,
            output.status,
            stderr.trim()
        )))
    }
}

fn validate_remote(remote: &str) -> Result<(), CommandError> {
    if remote.is_empty() {
        return Err(CommandError::new("ssh remote must not be empty"));
    }
    if remote.starts_with('-') {
        return Err(CommandError::new("ssh remote must not start with '-'"));
    }
    if remote.chars().any(char::is_control) {
        return Err(CommandError::new("ssh remote contains a control character"));
    }
    Ok(())
}

fn validate_remote_path(path: &str) -> Result<(), CommandError> {
    if !path.starts_with('/') {
        return Err(CommandError::new(format!(
            "remote path must be absolute: {path}"
        )));
    }
    if path.chars().any(char::is_control) {
        return Err(CommandError::new(format!(
            "remote path contains a control character: {path}"
        )));
    }
    if path
        .split('/')
        .any(|component| component == "." || component == "..")
    {
        return Err(CommandError::new(format!(
            "remote path contains an unsafe component: {path}"
        )));
    }
    Ok(())
}

fn remote_parent(path: &str) -> Result<String, CommandError> {
    validate_remote_path(path)?;
    let Some((parent, file_name)) = path.rsplit_once('/') else {
        return Err(CommandError::new(format!(
            "remote path has no parent: {path}"
        )));
    };
    if parent.is_empty() || file_name.is_empty() {
        return Err(CommandError::new(format!(
            "remote path has no file name: {path}"
        )));
    }
    Ok(parent.to_owned())
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

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
        self.commands
            .rename(&paths.temp_object, &paths.object)
            .map_err(|source| Error::Command {
                operation: "rename object",
                path: paths.object.clone(),
                source,
            })?;
        verify_remote_final(&mut self.commands, &paths.object, object_digest.bytes)?;

        let entry = manifest_entry_from_parts(
            &request.metadata,
            paths.object_manifest_path,
            &object_digest,
        );
        let receipt =
            receipt_from_parts(&request.metadata, entry.object_path.clone(), &object_digest);
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
        self.commands
            .rename(&paths.temp_receipt, &paths.receipt)
            .map_err(|source| Error::Command {
                operation: "rename receipt",
                path: paths.receipt.clone(),
                source,
            })?;
        verify_remote_final(&mut self.commands, &paths.receipt, receipt_digest.bytes)?;

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct DigestResult {
    bytes: u64,
    sha256: String,
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

    let expected_prefix: Vec<u8> = expected_bytes
        .iter()
        .copied()
        .take(readback_bytes)
        .collect();
    let actual_prefix = commands
        .read_prefix(remote_path, readback_bytes)
        .map_err(|source| Error::Command {
            operation: "read uploaded file prefix",
            path: remote_path.to_owned(),
            source,
        })?;
    match actual_prefix {
        Some(actual) if actual == expected_prefix => Ok(()),
        Some(_) => Err(Error::VerifyReadbackMismatch {
            path: remote_path.to_owned(),
        }),
        None => Err(Error::MissingRemoteFile {
            operation: "read uploaded file prefix",
            path: remote_path.to_owned(),
        }),
    }
}

fn verify_remote_final<C>(
    commands: &mut C,
    remote_path: &str,
    expected_bytes: u64,
) -> Result<(), Error>
where
    C: StorageBoxCommands,
{
    match commands
        .stat_len(remote_path)
        .map_err(|source| Error::Command {
            operation: "stat final file",
            path: remote_path.to_owned(),
            source,
        })? {
        Some(actual) if actual == expected_bytes => Ok(()),
        Some(actual) => Err(Error::VerifySizeMismatch {
            path: remote_path.to_owned(),
            expected: expected_bytes,
            actual,
        }),
        None => Err(Error::MissingRemoteFile {
            operation: "stat final file",
            path: remote_path.to_owned(),
        }),
    }
}

fn manifest_entry_from_parts(
    metadata: &Metadata,
    object_path: String,
    digest: &DigestResult,
) -> ManifestEntry {
    ManifestEntry {
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

fn receipt_from_parts(metadata: &Metadata, object_path: String, digest: &DigestResult) -> Receipt {
    Receipt {
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
mod tests {
    use std::{collections::BTreeMap, path::PathBuf};

    use sha2::{Digest, Sha256};

    use super::{
        CommandError, Error, SshStorageBoxCommands, StorageBoxBackend, StorageBoxCommands,
        StorageBoxConfig, StorageBoxSshConfig,
    };
    use crate::{
        archive::NormalizerVersion,
        commit::{ManifestEntry, ManifestMode, Metadata, Request},
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Operation {
        name: &'static str,
        path: String,
        target: Option<String>,
    }

    #[derive(Debug, Default)]
    struct FakeCommands {
        files: BTreeMap<String, Vec<u8>>,
        operations: Vec<Operation>,
        upload_limit: Option<usize>,
    }

    impl StorageBoxCommands for FakeCommands {
        fn upload(&mut self, remote_path: &str, bytes: &[u8]) -> Result<(), CommandError> {
            self.operations.push(Operation {
                name: "upload",
                path: remote_path.to_owned(),
                target: None,
            });
            let stored = self.upload_limit.map_or_else(
                || bytes.to_vec(),
                |limit| bytes.iter().copied().take(limit).collect(),
            );
            self.files.insert(remote_path.to_owned(), stored);
            Ok(())
        }

        fn stat_len(&mut self, remote_path: &str) -> Result<Option<u64>, CommandError> {
            self.operations.push(Operation {
                name: "stat",
                path: remote_path.to_owned(),
                target: None,
            });
            self.files
                .get(remote_path)
                .map(|bytes| u64::try_from(bytes.len()))
                .transpose()
                .map_err(|_error| CommandError::new("test file too large"))
        }

        fn sha256(&mut self, remote_path: &str) -> Result<Option<String>, CommandError> {
            self.operations.push(Operation {
                name: "sha256",
                path: remote_path.to_owned(),
                target: None,
            });
            Ok(self.files.get(remote_path).map(|bytes| {
                let mut hasher = Sha256::new();
                hasher.update(bytes);
                hex::encode(hasher.finalize())
            }))
        }

        fn read_prefix(
            &mut self,
            remote_path: &str,
            max_bytes: usize,
        ) -> Result<Option<Vec<u8>>, CommandError> {
            self.operations.push(Operation {
                name: "read_prefix",
                path: remote_path.to_owned(),
                target: None,
            });
            Ok(self
                .files
                .get(remote_path)
                .map(|bytes| bytes.iter().copied().take(max_bytes).collect::<Vec<u8>>()))
        }

        fn rename(&mut self, from: &str, to: &str) -> Result<(), CommandError> {
            self.operations.push(Operation {
                name: "rename",
                path: from.to_owned(),
                target: Some(to.to_owned()),
            });
            let bytes = self
                .files
                .remove(from)
                .ok_or_else(|| CommandError::new("missing rename source"))?;
            self.files.insert(to.to_owned(), bytes);
            Ok(())
        }

        fn append(&mut self, remote_path: &str, bytes: &[u8]) -> Result<(), CommandError> {
            self.operations.push(Operation {
                name: "append",
                path: remote_path.to_owned(),
                target: None,
            });
            self.files
                .entry(remote_path.to_owned())
                .or_default()
                .extend_from_slice(bytes);
            Ok(())
        }
    }

    fn normalizer() -> NormalizerVersion {
        NormalizerVersion {
            name: "emoji-normalizer".to_owned(),
            semver: "0.1.0".to_owned(),
            git_rev: "test".to_owned(),
            unicode_version: "16.0".to_owned(),
            emoji_data_version: "16.0".to_owned(),
        }
    }

    fn metadata() -> Metadata {
        Metadata {
            run_id: "run-1".to_owned(),
            shard: "shard0".to_owned(),
            file_sequence: 42,
            dataset: "raw_archive_posts".to_owned(),
            row_count: 2,
            min_created_at_normalized: Some("2026-06-01T00:00:00Z".to_owned()),
            max_created_at_normalized: Some("2026-06-02T00:00:00Z".to_owned()),
            receipt_hash: "repo-receipt-hash".to_owned(),
            normalizer: normalizer(),
            schema_version: 1,
        }
    }

    fn request() -> Request {
        Request {
            object_path: PathBuf::from("objects/run-1/shard0/42.parquet"),
            receipt_path: PathBuf::from("objects/run-1/shard0/42.receipt.json"),
            manifest_path: PathBuf::from("manifests/raw.jsonl"),
            manifest_mode: ManifestMode::AppendJsonl,
            metadata: metadata(),
        }
    }

    fn backend(commands: FakeCommands) -> StorageBoxBackend<FakeCommands> {
        let mut config = StorageBoxConfig::new("/storage-box/emojistats");
        config.readback_bytes = 8;
        StorageBoxBackend::new(config, commands)
    }

    fn ssh_commands() -> SshStorageBoxCommands {
        SshStorageBoxCommands::new(
            StorageBoxSshConfig::new("u123456@u123456.your-storagebox.de")
                .with_ssh_program("/usr/bin/ssh")
                .with_ssh_arg("-p")
                .with_ssh_arg("23"),
        )
    }

    #[test]
    fn commits_in_verified_remote_order_before_manifest_append() {
        let mut backend = backend(FakeCommands::default());
        let artifact = backend
            .commit_bytes(&request(), b"parquet bytes")
            .expect("remote commit should succeed");
        let commands = backend.into_commands();

        let operation_names: Vec<&str> = commands
            .operations
            .iter()
            .map(|operation| operation.name)
            .collect();
        assert_eq!(
            operation_names,
            vec![
                "upload",
                "stat",
                "sha256",
                "read_prefix",
                "rename",
                "stat",
                "upload",
                "stat",
                "sha256",
                "read_prefix",
                "rename",
                "stat",
                "append"
            ]
        );
        assert_eq!(
            commands
                .operations
                .get(4)
                .expect("object rename operation should exist")
                .target
                .as_deref(),
            Some(artifact.remote_object_path.as_str())
        );
        assert_eq!(
            commands
                .operations
                .get(12)
                .expect("manifest append operation should exist")
                .path,
            "/storage-box/emojistats/manifests/raw.jsonl"
        );
        assert!(commands.files.contains_key(&artifact.remote_object_path));
        assert!(commands.files.contains_key(&artifact.remote_receipt_path));

        let manifest_bytes = commands
            .files
            .get(&artifact.remote_manifest_path)
            .expect("manifest should be appended");
        let manifest_line = std::str::from_utf8(manifest_bytes)
            .expect("manifest should be UTF-8")
            .trim_end();
        let manifest_entry: ManifestEntry =
            serde_json::from_str(manifest_line).expect("manifest should decode");
        assert_eq!(manifest_entry, artifact.entry);
        assert_eq!(
            artifact.entry.object_path,
            "objects/run-1/shard0/42.parquet"
        );
    }

    #[test]
    fn partial_object_upload_fails_before_rename_or_manifest_append() {
        let mut backend = backend(FakeCommands {
            upload_limit: Some(4),
            ..FakeCommands::default()
        });
        let result = backend.commit_bytes(&request(), b"parquet bytes");
        assert!(matches!(
            result,
            Err(Error::VerifySizeMismatch {
                expected: 13,
                actual: 4,
                ..
            })
        ));
        let commands = backend.into_commands();
        assert!(
            !commands
                .operations
                .iter()
                .any(|operation| operation.name == "rename")
        );
        assert!(
            !commands
                .operations
                .iter()
                .any(|operation| operation.name == "append")
        );
        assert!(
            !commands
                .files
                .contains_key("/storage-box/emojistats/objects/run-1/shard0/42.parquet")
        );
        assert!(
            !commands
                .files
                .contains_key("/storage-box/emojistats/manifests/raw.jsonl")
        );
    }

    #[test]
    fn rejects_remote_paths_outside_write_scope() {
        let mut escaping = request();
        escaping.object_path = PathBuf::from("../outside.parquet");
        let mut backend = backend(FakeCommands::default());

        let result = backend.commit_bytes(&escaping, b"parquet bytes");

        assert!(matches!(result, Err(Error::PathEscapesRoot { .. })));
        assert!(backend.into_commands().operations.is_empty());
    }

    #[test]
    fn ssh_upload_command_keeps_remote_path_inside_script_argument() {
        let command = ssh_commands()
            .upload_command(
                "/storage-box/emojistats/objects/run 1/quote'$(touch bad);.parquet",
                b"parquet bytes",
            )
            .expect("upload command should build");

        assert_eq!(command.program, PathBuf::from("/usr/bin/ssh"));
        assert_eq!(
            command.args,
            vec![
                "-p",
                "23",
                "u123456@u123456.your-storagebox.de",
                "umask 077; mkdir -p -- '/storage-box/emojistats/objects/run 1'; cat > '/storage-box/emojistats/objects/run 1/quote'\\''$(touch bad);.parquet'"
            ]
        );
        assert_eq!(command.stdin.as_deref(), Some(b"parquet bytes".as_slice()));
    }

    #[test]
    fn ssh_commands_reject_unsafe_remote_paths() {
        let commands = ssh_commands();

        assert!(commands.upload_command("relative/path", b"bytes").is_err());
        assert!(
            commands
                .upload_command("/storage/../outside", b"bytes")
                .is_err()
        );
        assert!(
            commands
                .upload_command("/storage/newline\nname", b"bytes")
                .is_err()
        );
    }

    #[test]
    fn ssh_read_prefix_command_marks_absent_and_present_outputs() {
        let command = ssh_commands()
            .read_prefix_command("/storage-box/emojistats/objects/run-1/42.parquet", 8)
            .expect("read prefix command should build");

        assert_eq!(command.args.len(), 4);
        assert_eq!(
            command
                .args
                .last()
                .expect("ssh script should be the final argument"),
            "if [ -e '/storage-box/emojistats/objects/run-1/42.parquet' ]; then printf 'present\\n'; head -c 8 -- '/storage-box/emojistats/objects/run-1/42.parquet'; else printf 'absent\\n'; fi"
        );
    }
}
