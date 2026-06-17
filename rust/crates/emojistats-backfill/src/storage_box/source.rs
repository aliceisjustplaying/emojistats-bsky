use std::{fs::File, io::Read, path::Path};

use sha2::{Digest, Sha256};

use super::{Error, StorageBoxCommands};
use crate::commit::DigestResult;

#[derive(Debug, Clone, Copy)]
pub(super) enum UploadSource<'a> {
    Bytes(&'a [u8]),
    File {
        path: &'a Path,
        open_operation: &'static str,
    },
}

impl UploadSource<'_> {
    fn prepare(self, kind: &'static str, readback_bytes: usize) -> Result<PreparedSource, Error> {
        match self {
            Self::Bytes(bytes) => Ok(PreparedSource {
                digest: digest_bytes(kind, bytes)?,
                prefix: prefix_bytes(bytes, readback_bytes)?,
            }),
            Self::File { path, .. } => digest_file(kind, path, readback_bytes),
        }
    }

    fn upload<C>(
        self,
        commands: &mut C,
        remote_path: &str,
        upload_operation: &'static str,
    ) -> Result<(), Error>
    where
        C: StorageBoxCommands,
    {
        match self {
            Self::Bytes(bytes) => {
                commands
                    .upload(remote_path, bytes)
                    .map_err(|source| Error::Command {
                        operation: upload_operation,
                        path: remote_path.to_owned(),
                        source,
                    })
            }
            Self::File {
                path,
                open_operation,
            } => {
                let mut file = File::open(path).map_err(|source| Error::LocalIo {
                    operation: open_operation,
                    path: path.to_path_buf(),
                    source,
                })?;
                commands
                    .upload_reader(remote_path, &mut file)
                    .map_err(|source| Error::Command {
                        operation: upload_operation,
                        path: remote_path.to_owned(),
                        source,
                    })
            }
        }
    }
}

struct PreparedSource {
    digest: DigestResult,
    prefix: Vec<u8>,
}

fn digest_file(
    kind: &'static str,
    path: &Path,
    readback_bytes: usize,
) -> Result<PreparedSource, Error> {
    let mut file = File::open(path).map_err(|source| Error::LocalIo {
        operation: "open source",
        path: path.to_path_buf(),
        source,
    })?;
    let mut hasher = Sha256::new();
    let mut byte_count = 0_u64;
    let mut prefix = Vec::with_capacity(readback_bytes);
    let mut buffer = vec![0_u8; 65_536].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer).map_err(|source| Error::LocalIo {
            operation: "read source",
            path: path.to_path_buf(),
            source,
        })?;
        if read == 0 {
            break;
        }
        let chunk = buffer
            .get(..read)
            .ok_or(Error::ByteCountOverflow { kind })?;
        hasher.update(chunk);
        let read_u64 = u64::try_from(read).map_err(|_error| Error::ByteCountOverflow { kind })?;
        byte_count = byte_count
            .checked_add(read_u64)
            .ok_or(Error::ByteCountOverflow { kind })?;
        let remaining_prefix = readback_bytes.saturating_sub(prefix.len());
        if remaining_prefix > 0 {
            let prefix_len = remaining_prefix.min(chunk.len());
            let prefix_chunk = chunk
                .get(..prefix_len)
                .ok_or(Error::ByteCountOverflow { kind })?;
            prefix.extend_from_slice(prefix_chunk);
        }
    }
    Ok(PreparedSource {
        digest: DigestResult {
            bytes: byte_count,
            sha256: hex::encode(hasher.finalize()),
        },
        prefix,
    })
}

pub(super) fn upload_verify_promote<C>(
    commands: &mut C,
    temp_path: &str,
    final_path: &str,
    artifact_kind: &'static str,
    source: UploadSource<'_>,
    readback_bytes: usize,
) -> Result<DigestResult, Error>
where
    C: StorageBoxCommands,
{
    let prepared = source.prepare(artifact_kind, readback_bytes)?;
    source.upload(commands, temp_path, upload_operation(artifact_kind))?;
    verify_remote_uploaded(
        commands,
        temp_path,
        &prepared.digest,
        &prepared.prefix,
        readback_bytes,
    )?;
    promote_temp_to_final(
        commands,
        temp_path,
        final_path,
        &prepared.digest,
        artifact_kind,
    )?;
    Ok(prepared.digest)
}

fn verify_remote_uploaded<C>(
    commands: &mut C,
    remote_path: &str,
    expected_digest: &DigestResult,
    expected_prefix: &[u8],
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

pub(super) fn cleanup_remote_temp<C>(
    commands: &mut C,
    temp_path: &str,
    artifact_kind: &'static str,
) -> Result<(), Error>
where
    C: StorageBoxCommands,
{
    commands.remove(temp_path).map_err(|source| Error::Command {
        operation: match artifact_kind {
            "object" => "cleanup object temp",
            "receipt" => "cleanup receipt temp",
            _ => "cleanup temp",
        },
        path: temp_path.to_owned(),
        source,
    })
}

fn upload_operation(artifact_kind: &'static str) -> &'static str {
    match artifact_kind {
        "object" => "upload object temp",
        "receipt" => "upload receipt temp",
        _ => "upload temp",
    }
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

fn prefix_bytes(bytes: &[u8], readback_bytes: usize) -> Result<Vec<u8>, Error> {
    let expected_prefix_len = bytes.len().min(readback_bytes);
    let prefix = bytes
        .get(..expected_prefix_len)
        .ok_or(Error::ByteCountOverflow { kind: "prefix" })?;
    Ok(prefix.to_vec())
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
    let rename_result = commands.rename(temp_path, final_path);
    match rename_result {
        Ok(()) => {
            verify_remote_final(commands, final_path, expected_digest)?;
            cleanup_remote_temp(commands, temp_path, artifact_kind)
        }
        Err(source) => match check_final_state(commands, final_path, expected_digest)? {
            FinalState::Exact => cleanup_remote_temp(commands, temp_path, artifact_kind),
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
