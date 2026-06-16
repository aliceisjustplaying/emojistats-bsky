use std::{
    fs, io,
    time::{Duration, Instant},
};

use emojistats_backfill::{
    archive::ArchiveError, ledger::AttemptOutcome, list_records::ListRecordsError,
    parse::ParseError, transport::FetchError,
};
use serde::Serialize;

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
        AttemptOutcome::ResourceLimitExceeded { .. } => "resource_limit_exceeded",
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
        } if *status == 429 => rate_limit.retry_after.map_or_else(
            || AttemptOutcome::RetryableFailure {
                message: message.clone(),
            },
            |retry_after| AttemptOutcome::RateLimited { retry_after },
        ),
        FetchError::HttpStatus { status, .. } if *status >= 500 => {
            AttemptOutcome::RetryableFailure {
                message: message.clone(),
            }
        }
        FetchError::InactivityTimeout { .. }
        | FetchError::Transport { .. }
        | FetchError::Io { .. }
        | FetchError::ByteBudgetPoisoned => AttemptOutcome::RetryableFailure {
            message: message.clone(),
        },
        FetchError::MaxBytesExceeded { .. }
        | FetchError::ErrorBodyTooLarge { .. }
        | FetchError::InFlightBytesExceeded { .. } => AttemptOutcome::ResourceLimitExceeded {
            message: message.clone(),
        },
        FetchError::HttpStatus { .. } => AttemptOutcome::PermanentFailure {
            message: message.clone(),
        },
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
        ListRecordsError::HttpStatus { status, .. }
            if *status == 429
                && error
                    .rate_limit()
                    .and_then(|limit| limit.retry_after)
                    .is_some() =>
        {
            AttemptOutcome::RateLimited {
                retry_after: error
                    .rate_limit()
                    .and_then(|limit| limit.retry_after)
                    .unwrap_or(Duration::ZERO),
            }
        }
        ListRecordsError::HttpStatus { .. } if error.is_retryable() => {
            AttemptOutcome::RetryableFailure {
                message: message.clone(),
            }
        }
        ListRecordsError::Transport(_) | ListRecordsError::InactivityTimeout { .. } => {
            AttemptOutcome::RetryableFailure {
                message: message.clone(),
            }
        }
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
        | ParseError::MstRootMismatch { .. }
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
            AttemptOutcome::PermanentFailure {
                message: message.clone(),
            }
        }
        ArchiveError::Io(_) | ArchiveError::Commit(_) => AttemptOutcome::RetryableFailure {
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
