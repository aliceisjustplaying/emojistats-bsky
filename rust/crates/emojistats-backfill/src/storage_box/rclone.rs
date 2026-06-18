use std::{
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde::Deserialize;
use tempfile::NamedTempFile;

use super::{CommandError, RcloneStorageBoxCommands, StorageBoxCommands, StorageBoxRcloneConfig};

const COMMAND_OUTPUT_MAX_BYTES: usize = 64 * 1024;
const COMMAND_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommandSpec {
    operation: &'static str,
    program: PathBuf,
    args: Vec<String>,
    timeout: Duration,
}

#[derive(Debug, Deserialize)]
struct StatItem {
    #[serde(rename = "Size")]
    size: Option<u64>,
}

impl StorageBoxRcloneConfig {
    fn command(&self, operation: &'static str, args: Vec<String>) -> CommandSpec {
        let mut command_args = vec![
            "--config".to_owned(),
            self.config_path.to_string_lossy().into_owned(),
        ];
        command_args.extend(args);
        CommandSpec {
            operation,
            program: self.rclone_program.clone(),
            args: command_args,
            timeout: self.command_timeout,
        }
    }

    fn remote_path(&self, remote_path: &str) -> Result<String, CommandError> {
        validate_remote_name(&self.remote_name)?;
        validate_remote_path(remote_path)?;
        let trimmed = remote_path.trim_start_matches('/');
        if trimmed.is_empty() {
            return Err(CommandError::new("rclone remote path was empty"));
        }
        Ok(format!("{}:{trimmed}", self.remote_name))
    }
}

impl RcloneStorageBoxCommands {
    fn copy_file_to_remote(&self, source: &Path, remote_path: &str) -> Result<(), CommandError> {
        let target = self.config.remote_path(remote_path)?;
        let command = self.config.command(
            "rclone copyto",
            vec![
                "copyto".to_owned(),
                "--sftp-concurrency".to_owned(),
                "1".to_owned(),
                "--retries".to_owned(),
                "1".to_owned(),
                source.to_string_lossy().into_owned(),
                target,
            ],
        );
        run_command(&command).map(|_stdout| ())
    }

    fn write_bytes_to_remote(&self, remote_path: &str, bytes: &[u8]) -> Result<(), CommandError> {
        let mut temp = NamedTempFile::new()
            .map_err(|error| CommandError::new(format!("create temp file failed: {error}")))?;
        temp.write_all(bytes)
            .map_err(|error| CommandError::new(format!("write temp file failed: {error}")))?;
        temp.flush()
            .map_err(|error| CommandError::new(format!("flush temp file failed: {error}")))?;
        self.copy_file_to_remote(temp.path(), remote_path)
    }

    fn read_all(&self, remote_path: &str) -> Result<Option<Vec<u8>>, CommandError> {
        let target = self.config.remote_path(remote_path)?;
        let command = self.config.command(
            "rclone cat",
            vec![
                "cat".to_owned(),
                "--retries".to_owned(),
                "1".to_owned(),
                target,
            ],
        );
        match run_command(&command) {
            Ok(stdout) => Ok(Some(stdout)),
            Err(error) if is_not_found_message(&error.to_string()) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

impl StorageBoxCommands for RcloneStorageBoxCommands {
    fn upload(&mut self, remote_path: &str, bytes: &[u8]) -> Result<(), CommandError> {
        self.write_bytes_to_remote(remote_path, bytes)
    }

    fn upload_reader(
        &mut self,
        remote_path: &str,
        reader: &mut (dyn Read + Send),
    ) -> Result<(), CommandError> {
        let mut temp = NamedTempFile::new()
            .map_err(|error| CommandError::new(format!("create temp file failed: {error}")))?;
        io::copy(reader, &mut temp)
            .map_err(|error| CommandError::new(format!("write temp file failed: {error}")))?;
        temp.flush()
            .map_err(|error| CommandError::new(format!("flush temp file failed: {error}")))?;
        self.copy_file_to_remote(temp.path(), remote_path)
    }

    fn stat_len(&mut self, remote_path: &str) -> Result<Option<u64>, CommandError> {
        let target = self.config.remote_path(remote_path)?;
        let command = self.config.command(
            "rclone lsjson stat",
            vec![
                "lsjson".to_owned(),
                "--stat".to_owned(),
                "--files-only".to_owned(),
                target,
            ],
        );
        match run_command(&command) {
            Ok(stdout) => {
                let item: Option<StatItem> = serde_json::from_slice(&stdout).map_err(|error| {
                    CommandError::new(format!("lsjson stat output was not JSON: {error}"))
                })?;
                Ok(item.and_then(|item| item.size))
            }
            Err(error) if is_not_found_message(&error.to_string()) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn sha256(&mut self, remote_path: &str) -> Result<Option<String>, CommandError> {
        let target = self.config.remote_path(remote_path)?;
        let command = self.config.command(
            "rclone sha256",
            vec![
                "hashsum".to_owned(),
                "SHA-256".to_owned(),
                "--download".to_owned(),
                "--retries".to_owned(),
                "1".to_owned(),
                target,
            ],
        );
        match run_command(&command) {
            Ok(stdout) => {
                let text = std::str::from_utf8(&stdout).map_err(|error| {
                    CommandError::new(format!("hashsum output was not UTF-8: {error}"))
                })?;
                let trimmed = text.trim();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    let Some((hash, _path)) = trimmed.split_once(' ') else {
                        return Err(CommandError::new("hashsum output had no path separator"));
                    };
                    Ok(Some(hash.to_owned()))
                }
            }
            Err(error) if is_not_found_message(&error.to_string()) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn read_prefix(
        &mut self,
        remote_path: &str,
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>, CommandError> {
        let target = self.config.remote_path(remote_path)?;
        let command = self.config.command(
            "rclone cat prefix",
            vec![
                "cat".to_owned(),
                "--count".to_owned(),
                max_bytes.to_string(),
                "--retries".to_owned(),
                "1".to_owned(),
                target,
            ],
        );
        match run_command(&command) {
            Ok(stdout) => Ok(Some(stdout)),
            Err(error) if is_not_found_message(&error.to_string()) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn remove(&mut self, remote_path: &str) -> Result<(), CommandError> {
        let target = self.config.remote_path(remote_path)?;
        let command = self.config.command(
            "rclone deletefile",
            vec![
                "deletefile".to_owned(),
                "--retries".to_owned(),
                "1".to_owned(),
                target,
            ],
        );
        match run_command(&command) {
            Ok(_stdout) => Ok(()),
            Err(error) if is_not_found_message(&error.to_string()) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn rename(&mut self, from: &str, to: &str) -> Result<(), CommandError> {
        if self.stat_len(to)?.is_some() {
            return Err(CommandError::new(format!(
                "final path already exists: {to}"
            )));
        }
        let source = self.config.remote_path(from)?;
        let target = self.config.remote_path(to)?;
        let command = self.config.command(
            "rclone moveto",
            vec![
                "moveto".to_owned(),
                "--immutable".to_owned(),
                "--sftp-concurrency".to_owned(),
                "1".to_owned(),
                "--retries".to_owned(),
                "1".to_owned(),
                source,
                target,
            ],
        );
        run_command(&command).map(|_stdout| ())
    }

    fn append_manifest_record_if_missing(
        &mut self,
        _remote_path: &str,
        _record_without_newline: &[u8],
    ) -> Result<(), CommandError> {
        Err(CommandError::new(
            "rclone backend cannot atomically append manifests; use SSH Storage Box for manifest publication",
        ))
    }

    fn contains_manifest_record(
        &mut self,
        remote_path: &str,
        record_without_newline: &[u8],
    ) -> Result<bool, CommandError> {
        Ok(self
            .read_all(remote_path)?
            .is_some_and(|current| contains_line(&current, record_without_newline)))
    }
}

fn contains_line(bytes: &[u8], needle: &[u8]) -> bool {
    bytes
        .split(|byte| *byte == b'\n')
        .any(|line| line == needle)
}

fn validate_remote_name(remote_name: &str) -> Result<(), CommandError> {
    if remote_name.is_empty()
        || remote_name.contains(':')
        || remote_name.contains('/')
        || remote_name.contains('\\')
    {
        return Err(CommandError::new("invalid rclone remote name"));
    }
    Ok(())
}

fn validate_remote_path(remote_path: &str) -> Result<(), CommandError> {
    if !remote_path.starts_with('/') || remote_path.contains('\0') {
        return Err(CommandError::new("invalid rclone remote path"));
    }
    Ok(())
}

fn is_not_found_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("not found") || lower.contains("doesn't exist") || lower.contains("no such file")
}

fn run_command(spec: &CommandSpec) -> Result<Vec<u8>, CommandError> {
    let mut child = ProcessCommand::new(&spec.program)
        .args(&spec.args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| CommandError::new(format!("{} spawn failed: {error}", spec.operation)))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| CommandError::new(format!("{} stdout was not available", spec.operation)))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| CommandError::new(format!("{} stderr was not available", spec.operation)))?;
    let stdout_reader = read_pipe(spec.operation, "stdout", stdout);
    let stderr_reader = read_pipe(spec.operation, "stderr", stderr);
    let status = wait_with_timeout(spec, &mut child);
    let stdout = join_pipe_reader(spec.operation, "stdout", stdout_reader)?;
    let stderr = join_pipe_reader(spec.operation, "stderr", stderr_reader)?;

    match status {
        Ok(CommandStatus::Exited(status)) if status.success() => {
            if stdout.truncated {
                Err(CommandError::new(format!(
                    "{} stdout exceeded {} bytes",
                    spec.operation, COMMAND_OUTPUT_MAX_BYTES
                )))
            } else {
                Ok(stdout.bytes)
            }
        }
        Ok(CommandStatus::Exited(status)) => Err(CommandError::new(format!(
            "{} exited with {}: stdout={} stderr={}",
            spec.operation,
            status,
            format_pipe_output(&stdout),
            format_pipe_output(&stderr)
        ))),
        Ok(CommandStatus::TimedOut) => Err(CommandError::new(format!(
            "{} timed out after {:?} and was killed: stdout={} stderr={}",
            spec.operation,
            spec.timeout,
            format_pipe_output(&stdout),
            format_pipe_output(&stderr)
        ))),
        Err(error) => Err(error),
    }
}

enum CommandStatus {
    Exited(std::process::ExitStatus),
    TimedOut,
}

fn wait_with_timeout(
    spec: &CommandSpec,
    child: &mut std::process::Child,
) -> Result<CommandStatus, CommandError> {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().map_err(|error| {
            CommandError::new(format!("{} wait failed: {error}", spec.operation))
        })? {
            return Ok(CommandStatus::Exited(status));
        }
        if started.elapsed() >= spec.timeout {
            child.kill().map_err(|error| {
                CommandError::new(format!(
                    "{} kill after timeout failed: {error}",
                    spec.operation
                ))
            })?;
            child.wait().map_err(|error| {
                CommandError::new(format!(
                    "{} wait after kill failed: {error}",
                    spec.operation
                ))
            })?;
            return Ok(CommandStatus::TimedOut);
        }
        thread::sleep(COMMAND_WAIT_POLL_INTERVAL.min(spec.timeout));
    }
}

#[derive(Debug)]
struct PipeOutput {
    bytes: Vec<u8>,
    truncated: bool,
}

fn read_pipe(
    operation: &'static str,
    name: &'static str,
    mut pipe: impl Read + Send + 'static,
) -> thread::JoinHandle<Result<PipeOutput, CommandError>> {
    thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut truncated = false;
        let mut buffer = vec![0_u8; 8_192].into_boxed_slice();
        loop {
            let read = pipe.read(&mut buffer).map_err(|error| {
                CommandError::new(format!("{operation} {name} read failed: {error}"))
            })?;
            if read == 0 {
                break;
            }
            let remaining = COMMAND_OUTPUT_MAX_BYTES.saturating_sub(bytes.len());
            if remaining == 0 {
                truncated = true;
                continue;
            }
            let take = remaining.min(read);
            let chunk = buffer.get(..take).ok_or_else(|| {
                CommandError::new(format!("{operation} {name} slice exceeded read buffer"))
            })?;
            bytes.extend_from_slice(chunk);
            if take < read {
                truncated = true;
            }
        }
        Ok(PipeOutput { bytes, truncated })
    })
}

fn join_pipe_reader(
    operation: &'static str,
    name: &'static str,
    handle: thread::JoinHandle<Result<PipeOutput, CommandError>>,
) -> Result<PipeOutput, CommandError> {
    handle
        .join()
        .unwrap_or_else(|_panic| Err(CommandError::new(format!("{operation} {name} panicked"))))
}

fn format_pipe_output(output: &PipeOutput) -> String {
    let mut text = String::from_utf8_lossy(&output.bytes).into_owned();
    if output.truncated {
        text.push_str("...[truncated]");
    }
    text
}
