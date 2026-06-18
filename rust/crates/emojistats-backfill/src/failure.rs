use std::{
    fs, io,
    time::{Duration, Instant, SystemTime},
};

use serde::Serialize;

use crate::{
    archive::ArchiveError, commit, ledger::AttemptOutcome, list_records::ListRecordsError,
    parse::ParseError, storage_box, transport::FetchError,
};

const BYTE_PRESSURE_RETRY_AFTER: Duration = Duration::from_secs(60);
const DEFAULT_RATE_LIMIT_RETRY_AFTER: Duration = Duration::from_secs(60);

#[derive(Serialize)]
pub struct SmokeTelemetry<'a> {
    pub event: &'static str,
    pub did: &'a str,
    pub host: Option<&'a str>,
    pub outcome: &'static str,
    pub stage: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pressure_state: Option<&'static str>,
    pub elapsed_ms: u64,
    pub fetch_ms: Option<u64>,
    pub parse_ms: Option<u64>,
    pub archive_ms: Option<u64>,
    pub bytes: Option<u64>,
    pub records: Option<u64>,
    pub archived_posts: Option<u64>,
    pub decode_errors: Option<u64>,
    pub emoji_rows: Option<u64>,
    pub rss_kb: Option<u64>,
    pub error: Option<String>,
}

pub fn emit_smoke_telemetry(telemetry: &SmokeTelemetry<'_>) {
    match serde_json::to_string(telemetry) {
        Ok(line) => println!("smoke_telemetry {line}"),
        Err(error) => eprintln!("failed to serialize smoke telemetry: {error}"),
    }
}

pub fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

pub fn current_rss_kb() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?.trim();
        let kb = value.split_whitespace().next()?;
        kb.parse::<u64>().ok()
    })
}

pub const fn outcome_name(outcome: &AttemptOutcome) -> &'static str {
    match outcome {
        AttemptOutcome::Succeeded => "succeeded",
        AttemptOutcome::AccountState(_) => "account_state",
        AttemptOutcome::RateLimited { .. } => "rate_limited",
        AttemptOutcome::OperatorDeferred { .. } => "operator_deferred",
        AttemptOutcome::RetryableFailure { .. } => "retryable_failure",
        AttemptOutcome::ResourceLimitExceeded { .. } => "resource_limited",
        AttemptOutcome::PermanentFailure { .. } => "permanent_failure",
    }
}

#[derive(Debug)]
pub struct FetchOneFailure {
    pub outcome: AttemptOutcome,
    pub error: anyhow::Error,
}

pub fn classify_fetch_error(did: &str, error: &FetchError) -> FetchOneFailure {
    let message = format!("fetch getRepo for {did}: {error}");
    let outcome = match &error {
        FetchError::AccountState { state, .. } => AttemptOutcome::AccountState(*state),
        FetchError::HttpStatus {
            status, rate_limit, ..
        } if *status == 429 => AttemptOutcome::RateLimited {
            retry_after: rate_limit
                .cooldown_delay(SystemTime::now())
                .unwrap_or(DEFAULT_RATE_LIMIT_RETRY_AFTER),
        },
        FetchError::HttpStatus { status, .. } if *status >= 500 => {
            AttemptOutcome::RetryableFailure {
                message: message.clone(),
            }
        }
        FetchError::Io { source } if is_operator_io_error(source) => {
            AttemptOutcome::OperatorDeferred {
                retry_after: None,
                message: message.clone(),
            }
        }
        FetchError::InactivityTimeout { .. }
        | FetchError::DownloadTimeout { .. }
        | FetchError::ResponseHeaderTimeout { .. }
        | FetchError::ProgressTimeout { .. }
        | FetchError::Transport { .. }
        | FetchError::Io { .. } => AttemptOutcome::RetryableFailure {
            message: message.clone(),
        },
        FetchError::InFlightBytesUnavailable { .. } => AttemptOutcome::OperatorDeferred {
            retry_after: Some(BYTE_PRESSURE_RETRY_AFTER),
            message: message.clone(),
        },
        FetchError::ErrorBodyTooLarge { .. } | FetchError::ByteBudgetPoisoned => {
            AttemptOutcome::OperatorDeferred {
                retry_after: None,
                message: message.clone(),
            }
        }
        FetchError::MaxBytesExceeded { .. } | FetchError::InFlightBytesExceeded { .. } => {
            AttemptOutcome::ResourceLimitExceeded {
                message: message.clone(),
            }
        }
        FetchError::HttpStatus { .. } | FetchError::PermanentTransport { .. } => {
            AttemptOutcome::PermanentFailure {
                message: message.clone(),
            }
        }
    };
    FetchOneFailure {
        outcome,
        error: anyhow::anyhow!(message),
    }
}

pub fn classify_list_records_error(did: &str, error: &ListRecordsError) -> FetchOneFailure {
    let message = format!("fetch listRecords for {did}: {error}");
    let outcome = match error {
        ListRecordsError::AccountState { state, .. } => AttemptOutcome::AccountState(*state),
        ListRecordsError::HttpStatus { status, .. } if *status == 429 => {
            AttemptOutcome::RateLimited {
                retry_after: error
                    .rate_limit()
                    .and_then(|limit| limit.cooldown_delay(SystemTime::now()))
                    .unwrap_or(DEFAULT_RATE_LIMIT_RETRY_AFTER),
            }
        }
        ListRecordsError::HttpStatus { .. } if error.is_retryable() => {
            AttemptOutcome::RetryableFailure {
                message: message.clone(),
            }
        }
        ListRecordsError::Transport(_)
        | ListRecordsError::ResponseHeaderTimeout { .. }
        | ListRecordsError::InactivityTimeout { .. }
        | ListRecordsError::DownloadTimeout { .. }
        | ListRecordsError::ProgressTimeout { .. } => AttemptOutcome::RetryableFailure {
            message: message.clone(),
        },
        ListRecordsError::PreCommit(_) => AttemptOutcome::OperatorDeferred {
            retry_after: None,
            message: message.clone(),
        },
        ListRecordsError::ResourceLimitExceeded { .. } => AttemptOutcome::ResourceLimitExceeded {
            message: message.clone(),
        },
        ListRecordsError::Archive(error) => {
            return classify_archive_error(&format!("archive listRecords for {did}"), error);
        }
        ListRecordsError::HttpStatus { .. }
        | ListRecordsError::PageJson(_)
        | ListRecordsError::Protocol(_) => AttemptOutcome::PermanentFailure {
            message: message.clone(),
        },
    };
    FetchOneFailure {
        outcome,
        error: anyhow::anyhow!(message),
    }
}

pub fn classify_parse_error(did: &str, error: &ParseError) -> FetchOneFailure {
    let message = format!("parse CAR for {did}: {error}");
    let outcome = match error {
        ParseError::ResourceLimitExceeded { .. } | ParseError::ResourceCountOverflow { .. } => {
            AttemptOutcome::ResourceLimitExceeded {
                message: message.clone(),
            }
        }
        ParseError::Io { .. } | ParseError::Runtime(_) | ParseError::ThreadSpawn(_) => {
            AttemptOutcome::RetryableFailure {
                message: message.clone(),
            }
        }
        ParseError::Repo(_)
        | ParseError::InvalidRoots(_)
        | ParseError::CidMismatch { .. }
        | ParseError::UnsupportedCodec { .. }
        | ParseError::CommitNotFound { .. }
        | ParseError::RootCommitDecode { .. }
        | ParseError::CommitDidMismatch { .. }
        | ParseError::MissingBlock { .. }
        | ParseError::RecordDecode { .. }
        | ParseError::Unsupported { .. }
        | ParseError::NotYetImplemented { .. }
        | ParseError::RuntimeThreadTerminated
        | ParseError::MalformedVarint
        | ParseError::CarLengthOverflow { .. }
        | ParseError::MalformedCar(_)
        | ParseError::CidRead(_) => AttemptOutcome::PermanentFailure {
            message: message.clone(),
        },
    };
    FetchOneFailure {
        outcome,
        error: anyhow::anyhow!(message),
    }
}

pub fn classify_archive_error(context: &str, error: &ArchiveError) -> FetchOneFailure {
    let message = format!("{context}: {error}");
    let outcome = match error {
        ArchiveError::Io(source) if is_operator_io_error(source) => {
            AttemptOutcome::OperatorDeferred {
                retry_after: None,
                message: message.clone(),
            }
        }
        ArchiveError::Commit(error) => classify_commit_error(error, message.clone()),
        ArchiveError::StorageBox(error) => classify_storage_box_error(error, message.clone()),
        ArchiveError::Io(_) => AttemptOutcome::RetryableFailure {
            message: message.clone(),
        },
        ArchiveError::CountOverflow { .. } => AttemptOutcome::ResourceLimitExceeded {
            message: message.clone(),
        },
        ArchiveError::Parquet(_)
        | ArchiveError::Arrow(_)
        | ArchiveError::Json(_)
        | ArchiveError::InvalidParquetColumn { .. }
        | ArchiveError::InvalidParquetValue { .. }
        | ArchiveError::UnexpectedParquetNull { .. }
        | ArchiveError::InvalidCompression(_)
        | ArchiveError::InvalidPath { .. }
        | ArchiveError::InvalidRecordJson
        | ArchiveError::FinalPathExists { .. }
        | ArchiveError::FinalHashMismatch { .. } => AttemptOutcome::PermanentFailure {
            message: message.clone(),
        },
    };
    FetchOneFailure {
        outcome,
        error: anyhow::anyhow!(message),
    }
}

fn classify_commit_error(error: &commit::Error, message: String) -> AttemptOutcome {
    match error {
        commit::Error::FinalPathExists { .. }
        | commit::Error::FinalHashMismatch { .. }
        | commit::Error::ExistingReceiptMismatch { .. }
        | commit::Error::PathEscapesRoot { .. }
        | commit::Error::MissingFileName { .. }
        | commit::Error::NonUtf8Path { .. }
        | commit::Error::JsonRead { .. } => AttemptOutcome::PermanentFailure { message },
        commit::Error::ByteCountOverflow { .. } | commit::Error::InvalidReadSize { .. } => {
            AttemptOutcome::ResourceLimitExceeded { message }
        }
        commit::Error::Io { source, .. } if is_operator_io_error(source) => {
            AttemptOutcome::OperatorDeferred {
                retry_after: None,
                message,
            }
        }
        commit::Error::Io { .. } | commit::Error::Json { .. } | commit::Error::Writer(_) => {
            AttemptOutcome::RetryableFailure { message }
        }
    }
}

fn classify_storage_box_error(error: &storage_box::Error, message: String) -> AttemptOutcome {
    match error {
        storage_box::Error::VerifySizeMismatch { .. }
        | storage_box::Error::VerifyHashMismatch { .. }
        | storage_box::Error::FinalExistsConflict { .. }
        | storage_box::Error::VerifyReadbackMismatch { .. }
        | storage_box::Error::InvalidRemoteRoot(_)
        | storage_box::Error::TempDirectoryEscapesRoot { .. }
        | storage_box::Error::PathEscapesRoot { .. }
        | storage_box::Error::MissingFileName { .. }
        | storage_box::Error::NonUtf8Path { .. }
        | storage_box::Error::UnsupportedManifestMode
        | storage_box::Error::MissingRemoteFile { .. }
        | storage_box::Error::Json { .. } => AttemptOutcome::PermanentFailure { message },
        storage_box::Error::ByteCountOverflow { .. } => {
            AttemptOutcome::ResourceLimitExceeded { message }
        }
        storage_box::Error::LocalIo { source, .. } if is_operator_io_error(source) => {
            AttemptOutcome::OperatorDeferred {
                retry_after: None,
                message,
            }
        }
        storage_box::Error::LocalIo { .. } | storage_box::Error::Command { .. } => {
            AttemptOutcome::RetryableFailure { message }
        }
    }
}

fn is_operator_io_error(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::PermissionDenied
            | io::ErrorKind::AlreadyExists
            | io::ErrorKind::NotFound
            | io::ErrorKind::InvalidInput
            | io::ErrorKind::InvalidData
            | io::ErrorKind::WriteZero
            | io::ErrorKind::UnexpectedEof
    ) {
        return true;
    }
    matches!(error.raw_os_error(), Some(28 | 30 | 122))
}

pub fn retryable_failure(message: String) -> FetchOneFailure {
    FetchOneFailure {
        outcome: AttemptOutcome::RetryableFailure {
            message: message.clone(),
        },
        error: anyhow::anyhow!(message),
    }
}

pub fn permanent_failure(message: String) -> FetchOneFailure {
    FetchOneFailure {
        outcome: AttemptOutcome::PermanentFailure {
            message: message.clone(),
        },
        error: anyhow::anyhow!(message),
    }
}

#[cfg(test)]
mod tests {
    use std::{io, path::PathBuf};

    use super::*;
    use crate::transport::FetchError;

    #[test]
    fn commit_integrity_conflict_is_permanent_failure() {
        let failure = classify_archive_error(
            "archive",
            &ArchiveError::Commit(commit::Error::FinalHashMismatch {
                kind: "object",
                path: PathBuf::from("objects/1.parquet"),
                expected: "expected".to_owned(),
                observed: "observed".to_owned(),
            }),
        );

        assert!(matches!(
            failure.outcome,
            AttemptOutcome::PermanentFailure { .. }
        ));
    }

    #[test]
    fn commit_transient_io_stays_retryable() {
        let failure = classify_archive_error(
            "archive",
            &ArchiveError::Commit(commit::Error::Io {
                operation: "read final object for hashing",
                path: PathBuf::from("objects/1.parquet"),
                source: io::Error::from(io::ErrorKind::Interrupted),
            }),
        );

        assert!(matches!(
            failure.outcome,
            AttemptOutcome::RetryableFailure { .. }
        ));
    }

    #[test]
    fn oversized_error_body_is_operator_deferred() {
        let failure = classify_fetch_error(
            "did:plc:test",
            &FetchError::ErrorBodyTooLarge {
                max_bytes: 65_536,
                observed_bytes: 65_537,
            },
        );

        assert!(matches!(
            failure.outcome,
            AttemptOutcome::OperatorDeferred {
                retry_after: None,
                ..
            }
        ));
    }

    #[test]
    fn in_flight_byte_pressure_is_operator_deferred() {
        let failure = classify_fetch_error(
            "did:plc:test",
            &FetchError::InFlightBytesUnavailable {
                max_bytes: 10,
                requested_bytes: 11,
            },
        );

        assert!(matches!(
            failure.outcome,
            AttemptOutcome::OperatorDeferred {
                retry_after: Some(BYTE_PRESSURE_RETRY_AFTER),
                ..
            }
        ));
    }

    #[test]
    fn permanent_transport_is_permanent_failure() {
        let failure = classify_fetch_error(
            "did:plc:test",
            &FetchError::PermanentTransport {
                message: "dns error: failed to lookup address information".to_owned(),
                observed_bytes: None,
                source: Box::new(io::Error::other(
                    "dns error: failed to lookup address information",
                )),
            },
        );

        assert!(matches!(
            failure.outcome,
            AttemptOutcome::PermanentFailure { .. }
        ));
    }
}
