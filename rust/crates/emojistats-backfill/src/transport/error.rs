use std::{error::Error, io, time::Duration};

use http::StatusCode;
use jacquard_api::com_atproto::sync::get_repo::GetRepoError;

use super::{AccountState, RateLimitSnapshot};

/// Stage B fetch failures, split into account-state, HTTP, timeout, cap, stream, and I/O buckets.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// The PDS returned a terminal account-state error.
    #[error("account state {state} ({status}){message}", message = display_optional_message(message.as_deref()))]
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
    #[error("HTTP status {status}{error_code}{message}", error_code = display_optional_error_code(error_code.as_deref()), message = display_optional_message(message.as_deref()))]
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
    #[error("no body chunk within {timeout:?}")]
    InactivityTimeout {
        /// Timeout used for each chunk read.
        timeout: Duration,
    },
    /// The body download exceeded the configured wall-clock timeout.
    #[error("body download exceeded {timeout:?} after {observed_bytes} bytes")]
    DownloadTimeout {
        /// Timeout used for the whole body download.
        timeout: Duration,
        /// Bytes already written when the timeout fired.
        observed_bytes: u64,
    },
    /// The PDS did not return response headers inside the configured timeout.
    #[error("response headers did not arrive within {timeout:?}")]
    ResponseHeaderTimeout {
        /// Timeout used while waiting for response headers.
        timeout: Duration,
    },
    /// The body trickled below the configured progress floor.
    #[error(
        "body download made {observed_bytes} bytes progress in {interval:?}, below minimum {min_bytes}"
    )]
    ProgressTimeout {
        /// Progress window.
        interval: Duration,
        /// Minimum bytes expected in the window.
        min_bytes: u64,
        /// Bytes observed in the last window.
        observed_bytes: u64,
    },
    /// The streamed body exceeded the configured single-repo byte cap.
    #[error("spooled CAR exceeded max bytes: observed {observed_bytes}, max {max_bytes}")]
    MaxBytesExceeded {
        /// Configured cap.
        max_bytes: u64,
        /// Bytes observed after accepting the chunk that crossed the cap.
        observed_bytes: u64,
    },
    /// The PDS response body used for error classification exceeded its safety cap.
    #[error("error response body exceeded max bytes: observed {observed_bytes}, max {max_bytes}")]
    ErrorBodyTooLarge {
        /// Configured cap.
        max_bytes: u64,
        /// Bytes observed after accepting the chunk that crossed the cap.
        observed_bytes: u64,
    },
    /// The fleet-wide in-flight spool byte budget was exceeded.
    #[error(
        "in-flight spooled CAR bytes exceeded max bytes: observed {observed_bytes}, max {max_bytes}"
    )]
    InFlightBytesExceeded {
        /// Configured cap.
        max_bytes: u64,
        /// Bytes observed after accepting the chunk that crossed the cap.
        observed_bytes: u64,
    },
    /// The fleet-wide in-flight spool byte budget is temporarily occupied by other downloads.
    #[error(
        "in-flight spooled CAR byte budget unavailable: requested {requested_bytes}, max {max_bytes}"
    )]
    InFlightBytesUnavailable {
        /// Configured cap.
        max_bytes: u64,
        /// Bytes this reservation needed to hold.
        requested_bytes: u64,
    },
    /// The in-flight byte budget lock was poisoned.
    #[error("in-flight spool byte budget lock poisoned")]
    ByteBudgetPoisoned,
    /// A streaming transport error occurred before or during body download.
    #[error("transport error{observed}: {message}", observed = display_observed_bytes(*observed_bytes))]
    Transport {
        /// Transport error message.
        message: String,
        /// Bytes already written before the transport failed, when the body stream had started.
        observed_bytes: Option<u64>,
        /// Original transport error.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    /// A transport error that is unlikely to succeed on retry for the same host.
    #[error("permanent transport error{observed}: {message}", observed = display_observed_bytes(*observed_bytes))]
    PermanentTransport {
        /// Transport error message.
        message: String,
        /// Bytes already written before the transport failed, when the body stream had started.
        observed_bytes: Option<u64>,
        /// Original transport error.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    /// Local filesystem I/O failed.
    #[error("I/O error: {source}")]
    Io {
        /// Underlying I/O error.
        source: io::Error,
    },
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
            | Self::PermanentTransport { .. }
            | Self::Io { .. } => None,
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

fn display_optional_message(message: Option<&str>) -> String {
    message.map_or_else(String::new, |message| format!(": {message}"))
}

fn display_optional_error_code(error_code: Option<&str>) -> String {
    error_code.map_or_else(String::new, |error_code| {
        format!(" with XRPC error {error_code}")
    })
}

fn display_observed_bytes(observed_bytes: Option<u64>) -> String {
    observed_bytes.map_or_else(String::new, |bytes| format!(" after {bytes} bytes"))
}
