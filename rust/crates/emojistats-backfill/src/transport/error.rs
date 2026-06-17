use std::{error::Error, fmt, io, time::Duration};

use http::StatusCode;
use jacquard_api::com_atproto::sync::get_repo::GetRepoError;

use super::{AccountState, RateLimitSnapshot};

/// Stage B fetch failures, split into account-state, HTTP, timeout, cap, stream, and I/O buckets.
#[derive(Debug)]
pub enum FetchError {
    /// The PDS returned a terminal account-state error.
    AccountState {
        /// Account-state code from the XRPC body.
        state: AccountState,
        /// HTTP status returned by the PDS.
        status: u16,
        /// Optional XRPC error message.
        message: Option<Box<str>>,
        /// Rate-limit headers observed on the response.
        rate_limit: Box<RateLimitSnapshot>,
    },
    /// The PDS returned a non-success HTTP status that was not a terminal account state.
    HttpStatus {
        /// HTTP status returned by the PDS.
        status: u16,
        /// XRPC error code when the body decoded as one.
        error_code: Option<Box<str>>,
        /// Optional XRPC error message.
        message: Option<Box<str>>,
        /// Rate-limit headers observed on the response.
        rate_limit: Box<RateLimitSnapshot>,
    },
    /// No body chunk arrived inside the configured idle timeout.
    InactivityTimeout {
        /// Timeout used for each chunk read.
        timeout: Duration,
    },
    /// The body download exceeded the configured wall-clock timeout.
    DownloadTimeout {
        /// Timeout used for the whole body download.
        timeout: Duration,
        /// Bytes already written when the timeout fired.
        observed_bytes: u64,
    },
    /// The PDS did not return response headers inside the configured timeout.
    ResponseHeaderTimeout {
        /// Timeout used while waiting for response headers.
        timeout: Duration,
    },
    /// The body trickled below the configured progress floor.
    ProgressTimeout {
        /// Progress window.
        interval: Duration,
        /// Minimum bytes expected in the window.
        min_bytes: u64,
        /// Bytes observed in the last window.
        observed_bytes: u64,
    },
    /// The streamed body exceeded the configured single-repo byte cap.
    MaxBytesExceeded {
        /// Configured cap.
        max_bytes: u64,
        /// Bytes observed after accepting the chunk that crossed the cap.
        observed_bytes: u64,
    },
    /// The PDS response body used for error classification exceeded its safety cap.
    ErrorBodyTooLarge {
        /// Configured cap.
        max_bytes: u64,
        /// Bytes observed after accepting the chunk that crossed the cap.
        observed_bytes: u64,
    },
    /// The fleet-wide in-flight spool byte budget was exceeded.
    InFlightBytesExceeded {
        /// Configured cap.
        max_bytes: u64,
        /// Bytes observed after accepting the chunk that crossed the cap.
        observed_bytes: u64,
    },
    /// The fleet-wide in-flight spool byte budget is temporarily occupied by other downloads.
    InFlightBytesUnavailable {
        /// Configured cap.
        max_bytes: u64,
        /// Bytes this reservation needed to hold.
        requested_bytes: u64,
    },
    /// The in-flight byte budget lock was poisoned.
    ByteBudgetPoisoned,
    /// A streaming transport error occurred before or during body download.
    Transport {
        /// Transport error message.
        message: String,
        /// Bytes already written before the transport failed, when the body stream had started.
        observed_bytes: Option<u64>,
    },
    /// Local filesystem I/O failed.
    Io {
        /// Underlying I/O error.
        source: io::Error,
    },
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AccountState {
                state,
                status,
                message,
                rate_limit: _,
            } => write_fetch_message(
                f,
                &format_args!("account state {state}"),
                *status,
                message.as_deref(),
            ),
            Self::HttpStatus {
                status,
                error_code,
                message,
                rate_limit: _,
            } => match error_code {
                Some(code) => write_fetch_message(
                    f,
                    &format_args!("HTTP status {status} with XRPC error {code}"),
                    *status,
                    message.as_deref(),
                ),
                None => write!(f, "HTTP status {status}"),
            },
            Self::InactivityTimeout { timeout } => {
                write!(f, "no body chunk within {}", timeout.as_secs())
            }
            Self::DownloadTimeout {
                timeout,
                observed_bytes,
            } => write!(
                f,
                "body download exceeded {} seconds after {observed_bytes} bytes",
                timeout.as_secs()
            ),
            Self::ResponseHeaderTimeout { timeout } => {
                write!(
                    f,
                    "response headers did not arrive within {} seconds",
                    timeout.as_secs()
                )
            }
            Self::ProgressTimeout {
                interval,
                min_bytes,
                observed_bytes,
            } => write!(
                f,
                "body download made {observed_bytes} bytes progress in {} seconds, below minimum {min_bytes}",
                interval.as_secs()
            ),
            Self::MaxBytesExceeded {
                max_bytes,
                observed_bytes,
            } => write!(
                f,
                "spooled CAR exceeded max bytes: observed {observed_bytes}, max {max_bytes}"
            ),
            Self::ErrorBodyTooLarge {
                max_bytes,
                observed_bytes,
            } => write!(
                f,
                "error response body exceeded max bytes: observed {observed_bytes}, max {max_bytes}"
            ),
            Self::InFlightBytesExceeded {
                max_bytes,
                observed_bytes,
            } => write!(
                f,
                "in-flight spooled CAR bytes exceeded max bytes: observed {observed_bytes}, max {max_bytes}"
            ),
            Self::InFlightBytesUnavailable {
                max_bytes,
                requested_bytes,
            } => write!(
                f,
                "in-flight spooled CAR byte budget unavailable: requested {requested_bytes}, max {max_bytes}"
            ),
            Self::ByteBudgetPoisoned => f.write_str("in-flight spool byte budget lock poisoned"),
            Self::Transport {
                message,
                observed_bytes,
            } => match observed_bytes {
                Some(bytes) => write!(f, "transport error after {bytes} bytes: {message}"),
                None => write!(f, "transport error: {message}"),
            },
            Self::Io { source } => write!(f, "I/O error: {source}"),
        }
    }
}

impl FetchError {
    #[must_use]
    pub const fn rate_limit(&self) -> Option<&RateLimitSnapshot> {
        match self {
            Self::AccountState { rate_limit, .. } | Self::HttpStatus { rate_limit, .. } => {
                Some(rate_limit)
            }
            Self::InactivityTimeout { .. }
            | Self::DownloadTimeout { .. }
            | Self::ResponseHeaderTimeout { .. }
            | Self::ProgressTimeout { .. }
            | Self::MaxBytesExceeded { .. }
            | Self::ErrorBodyTooLarge { .. }
            | Self::InFlightBytesExceeded { .. }
            | Self::InFlightBytesUnavailable { .. }
            | Self::ByteBudgetPoisoned
            | Self::Transport { .. }
            | Self::Io { .. } => None,
        }
    }
}

impl Error for FetchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source } => Some(source),
            Self::AccountState { .. }
            | Self::HttpStatus { .. }
            | Self::InactivityTimeout { .. }
            | Self::DownloadTimeout { .. }
            | Self::ResponseHeaderTimeout { .. }
            | Self::ProgressTimeout { .. }
            | Self::MaxBytesExceeded { .. }
            | Self::ErrorBodyTooLarge { .. }
            | Self::InFlightBytesExceeded { .. }
            | Self::InFlightBytesUnavailable { .. }
            | Self::ByteBudgetPoisoned
            | Self::Transport { .. } => None,
        }
    }
}

impl From<io::Error> for FetchError {
    fn from(source: io::Error) -> Self {
        Self::Io { source }
    }
}

pub(super) fn classify_http_error(
    status: StatusCode,
    rate_limit: RateLimitSnapshot,
    body: &[u8],
) -> FetchError {
    match serde_json::from_slice::<GetRepoError>(body) {
        Ok(GetRepoError::RepoNotFound(message)) => FetchError::AccountState {
            state: AccountState::RepoNotFound,
            status: status.as_u16(),
            message: message.map(|value| value.to_string().into_boxed_str()),
            rate_limit: Box::new(rate_limit),
        },
        Ok(GetRepoError::RepoTakendown(message)) => FetchError::AccountState {
            state: AccountState::RepoTakendown,
            status: status.as_u16(),
            message: message.map(|value| value.to_string().into_boxed_str()),
            rate_limit: Box::new(rate_limit),
        },
        Ok(GetRepoError::RepoSuspended(message)) => FetchError::AccountState {
            state: AccountState::RepoSuspended,
            status: status.as_u16(),
            message: message.map(|value| value.to_string().into_boxed_str()),
            rate_limit: Box::new(rate_limit),
        },
        Ok(GetRepoError::RepoDeactivated(message)) => FetchError::AccountState {
            state: AccountState::RepoDeactivated,
            status: status.as_u16(),
            message: message.map(|value| value.to_string().into_boxed_str()),
            rate_limit: Box::new(rate_limit),
        },
        Ok(GetRepoError::Other { error, message }) => FetchError::HttpStatus {
            status: status.as_u16(),
            error_code: Some(error.to_string().into_boxed_str()),
            message: message.map(|value| value.to_string().into_boxed_str()),
            rate_limit: Box::new(rate_limit),
        },
        Err(_err) => FetchError::HttpStatus {
            status: status.as_u16(),
            error_code: None,
            message: String::from_utf8(body.to_vec())
                .ok()
                .map(String::into_boxed_str),
            rate_limit: Box::new(rate_limit),
        },
    }
}

fn write_fetch_message(
    f: &mut fmt::Formatter<'_>,
    prefix: &fmt::Arguments<'_>,
    status: u16,
    message: Option<&str>,
) -> fmt::Result {
    match message {
        Some(message) => write!(f, "{prefix} ({status}): {message}"),
        None => write!(f, "{prefix} ({status})"),
    }
}
