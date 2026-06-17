use std::{
    io::{self, Cursor, Read},
    path::PathBuf,
    process::{Command as ProcessCommand, Stdio},
    thread,
    time::{Duration, Instant},
};

use super::{CommandError, SshStorageBoxCommands, StorageBoxCommands, StorageBoxSshConfig};

const COMMAND_OUTPUT_MAX_BYTES: usize = 64 * 1024;
const COMMAND_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CommandSpec {
    pub(super) operation: &'static str,
    pub(super) program: PathBuf,
    pub(super) args: Vec<String>,
    pub(super) stdin: bool,
    pub(super) timeout: Duration,
}

impl StorageBoxSshConfig {
    pub(super) fn ssh_command(
        &self,
        operation: &'static str,
        script: String,
        stdin: bool,
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
            timeout: self.command_timeout,
        })
    }
}

impl SshStorageBoxCommands {
    pub(super) fn upload_command(&self, remote_path: &str) -> Result<CommandSpec, CommandError> {
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

    pub(super) fn read_prefix_command(
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

    pub(super) fn remove_command(&self, remote_path: &str) -> Result<CommandSpec, CommandError> {
        validate_remote_path(remote_path)?;
        let script = format!("rm -f -- {}", shell_quote(remote_path));
        self.config.ssh_command("remove", script, false)
    }

    pub(super) fn rename_command(&self, from: &str, to: &str) -> Result<CommandSpec, CommandError> {
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

    pub(super) fn append_manifest_record_if_missing_command(
        &self,
        remote_path: &str,
    ) -> Result<CommandSpec, CommandError> {
        let parent = remote_parent(remote_path)?;
        let lock_path = format!("{remote_path}.lock");
        let script = format!(
            "umask 077; mkdir -p -- {}; touch -- {}; flock -- {} sh -c 'record=$(cat); if [ -e \"$1\" ] && grep -Fqx -- \"$record\" \"$1\"; then exit 0; fi; printf \"%s\\n\" \"$record\" >> \"$1\"' sh {}",
            shell_quote(&parent),
            shell_quote(&lock_path),
            shell_quote(&lock_path),
            shell_quote(remote_path)
        );
        self.config
            .ssh_command("append_manifest_record_if_missing", script, true)
    }

    fn contains_manifest_record_command(
        &self,
        remote_path: &str,
    ) -> Result<CommandSpec, CommandError> {
        validate_remote_path(remote_path)?;
        let path = shell_quote(remote_path);
        let script = format!(
            "record=$(cat); if [ -e {path} ] && grep -Fqx -- \"$record\" {path}; then printf 'present\\n'; else printf 'absent\\n'; fi"
        );
        self.config
            .ssh_command("contains_manifest_record", script, true)
    }
}

impl StorageBoxCommands for SshStorageBoxCommands {
    fn upload(&mut self, remote_path: &str, bytes: &[u8]) -> Result<(), CommandError> {
        let command = self.upload_command(remote_path)?;
        let mut reader = Cursor::new(bytes);
        run_command(&command, Some(&mut reader)).map(|_stdout| ())
    }

    fn upload_reader(
        &mut self,
        remote_path: &str,
        reader: &mut (dyn Read + Send),
    ) -> Result<(), CommandError> {
        let command = self.upload_command(remote_path)?;
        run_command(&command, Some(reader)).map(|_stdout| ())
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

    fn remove(&mut self, remote_path: &str) -> Result<(), CommandError> {
        let command = self.remove_command(remote_path)?;
        run_command(&command, None).map(|_stdout| ())
    }

    fn append_manifest_record_if_missing(
        &mut self,
        remote_path: &str,
        record_without_newline: &[u8],
    ) -> Result<(), CommandError> {
        let command = self.append_manifest_record_if_missing_command(remote_path)?;
        let mut reader = Cursor::new(record_without_newline);
        run_command(&command, Some(&mut reader)).map(|_stdout| ())
    }

    fn contains_manifest_record(
        &mut self,
        remote_path: &str,
        record_without_newline: &[u8],
    ) -> Result<bool, CommandError> {
        let command = self.contains_manifest_record_command(remote_path)?;
        let mut reader = Cursor::new(record_without_newline);
        let stdout = run_command(&command, Some(&mut reader))?;
        match stdout.as_slice() {
            b"present\n" => Ok(true),
            b"absent\n" => Ok(false),
            _other => Err(CommandError::new(
                "manifest contains output had no presence marker",
            )),
        }
    }
}

pub(super) fn run_command(
    spec: &CommandSpec,
    stdin_reader: Option<&mut (dyn Read + Send)>,
) -> Result<Vec<u8>, CommandError> {
    let mut command = ProcessCommand::new(&spec.program);
    command.args(&spec.args);
    if spec.stdin || stdin_reader.is_some() {
        command.stdin(Stdio::piped());
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = command
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

    thread::scope(|scope| {
        let stdin_writer = if let Some(reader) = stdin_reader {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                CommandError::new(format!("{} stdin was not available", spec.operation))
            })?;
            Some(scope.spawn(move || {
                let result = io::copy(reader, &mut stdin);
                drop(stdin);
                result.map(|_bytes| ()).map_err(|error| {
                    CommandError::new(format!("{} stdin write failed: {error}", spec.operation))
                })
            }))
        } else {
            None
        };

        let status = wait_with_timeout(spec, &mut child);
        let stdout = join_pipe_reader(spec.operation, "stdout", stdout_reader)?;
        let stderr = join_pipe_reader(spec.operation, "stderr", stderr_reader)?;
        let stdin_result = stdin_writer.map_or(Ok(()), |handle| {
            handle.join().unwrap_or_else(|_panic| {
                Err(CommandError::new(format!(
                    "{} stdin writer panicked",
                    spec.operation
                )))
            })
        });

        match status {
            Ok(CommandStatus::Exited(status)) if status.success() => {
                stdin_result?;
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
    })
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

fn format_pipe_output(output: &PipeOutput) -> String {
    let text = String::from_utf8_lossy(&output.bytes);
    if output.truncated {
        format!("{}...[truncated]", text.trim())
    } else {
        text.trim().to_owned()
    }
}

fn read_pipe<R>(
    operation: &'static str,
    stream_name: &'static str,
    mut reader: R,
) -> thread::JoinHandle<Result<PipeOutput, CommandError>>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut truncated = false;
        let mut buffer = [0_u8; 8 * 1024];
        loop {
            let read = reader.read(&mut buffer).map_err(|error| {
                CommandError::new(format!("{operation} {stream_name} read failed: {error}"))
            })?;
            if read == 0 {
                break;
            }
            let remaining = COMMAND_OUTPUT_MAX_BYTES.saturating_sub(bytes.len());
            if remaining > 0 {
                let keep = remaining.min(read);
                let chunk = buffer.get(..keep).ok_or_else(|| {
                    CommandError::new(format!(
                        "{operation} {stream_name} read buffer slice out of bounds"
                    ))
                })?;
                bytes.extend_from_slice(chunk);
            }
            if read > remaining {
                truncated = true;
            }
        }
        Ok(PipeOutput { bytes, truncated })
    })
}

fn join_pipe_reader(
    operation: &'static str,
    stream_name: &'static str,
    handle: thread::JoinHandle<Result<PipeOutput, CommandError>>,
) -> Result<PipeOutput, CommandError> {
    handle.join().unwrap_or_else(|_panic| {
        Err(CommandError::new(format!(
            "{operation} {stream_name} reader panicked"
        )))
    })
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

pub(super) fn validate_remote_path(path: &str) -> Result<(), CommandError> {
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

pub(super) fn remote_parent(path: &str) -> Result<String, CommandError> {
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

pub(super) fn shell_quote(value: &str) -> String {
    shell_words::quote(value).into_owned()
}
