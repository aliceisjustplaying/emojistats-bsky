use std::{
    io::{self, Read},
    path::PathBuf,
    process::{Command as ProcessCommand, Stdio},
    thread,
    time::{Duration, Instant},
};

use super::CommandError;

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

pub(super) fn run_command(
    spec: &CommandSpec,
    stdin_reader: Option<&mut (dyn Read + Send)>,
) -> Result<Vec<u8>, CommandError> {
    let mut command = ProcessCommand::new(&spec.program);
    command.args(&spec.args);
    if spec.stdin || stdin_reader.is_some() {
        command.stdin(Stdio::piped());
    } else {
        command.stdin(Stdio::null());
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
