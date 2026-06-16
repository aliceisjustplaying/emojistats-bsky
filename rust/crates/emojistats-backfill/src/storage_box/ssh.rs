use std::{
    io::{Read, Write},
    path::PathBuf,
    process::{Command as ProcessCommand, Stdio},
    thread,
};

use super::{CommandError, StorageBoxSshConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CommandSpec {
    pub(super) operation: &'static str,
    pub(super) program: PathBuf,
    pub(super) args: Vec<String>,
    pub(super) stdin: bool,
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
        })
    }
}

pub(super) fn run_command(
    spec: &CommandSpec,
    stdin_bytes: Option<&[u8]>,
) -> Result<Vec<u8>, CommandError> {
    let mut command = ProcessCommand::new(&spec.program);
    command.args(&spec.args);
    if spec.stdin || stdin_bytes.is_some() {
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

    let stdin_error = if let Some(stdin_bytes) = stdin_bytes {
        let mut stdin = child.stdin.take().ok_or_else(|| {
            CommandError::new(format!("{} stdin was not available", spec.operation))
        })?;
        let result = stdin.write_all(stdin_bytes);
        drop(stdin);
        result.err()
    } else {
        None
    };

    let status = child
        .wait()
        .map_err(|error| CommandError::new(format!("{} wait failed: {error}", spec.operation)))?;
    let stdout = join_pipe_reader(spec.operation, "stdout", stdout_reader)?;
    let stderr = join_pipe_reader(spec.operation, "stderr", stderr_reader)?;
    if status.success() {
        stdin_error.map_or(Ok(stdout), |error| {
            Err(CommandError::new(format!(
                "{} stdin write failed: {error}",
                spec.operation
            )))
        })
    } else {
        let stderr = String::from_utf8_lossy(&stderr);
        Err(CommandError::new(format!(
            "{} exited with {}: {}",
            spec.operation,
            status,
            stderr.trim()
        )))
    }
}

fn read_pipe<R>(
    operation: &'static str,
    stream_name: &'static str,
    mut reader: R,
) -> thread::JoinHandle<Result<Vec<u8>, CommandError>>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).map_err(|error| {
            CommandError::new(format!("{operation} {stream_name} read failed: {error}"))
        })?;
        Ok(bytes)
    })
}

fn join_pipe_reader(
    operation: &'static str,
    stream_name: &'static str,
    handle: thread::JoinHandle<Result<Vec<u8>, CommandError>>,
) -> Result<Vec<u8>, CommandError> {
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
    format!("'{}'", value.replace('\'', r"'\''"))
}
