//! Stage B `getRepo` transport.

use std::{
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::StreamExt as _;
use http::{HeaderMap, StatusCode};
use jacquard_api::com_atproto::sync::get_repo::{GetRepo, GetRepoError};
use jacquard_common::{
    deps::fluent_uri::Uri,
    http_client::{HttpClient, HttpClientExt},
    stream::ByteStream,
    types::did::Did,
    xrpc::XrpcExt as _,
};
use serde::{Deserialize, Serialize};
use tokio::time;

const DEFAULT_CHUNK_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_BYTES: u64 = 2_147_483_648;
const ERROR_BODY_MAX_BYTES: u64 = 65_536;

/// Runtime limits and local storage path for Stage B repo transport.
#[derive(Debug, Clone)]
pub struct FetchConfig {
    /// Directory where the streamed `CAR` is written.
    pub spool_dir: PathBuf,
    /// Maximum silence while waiting for the next body chunk.
    pub chunk_idle_timeout: Duration,
    /// Loud single-repo byte cap for the spooled `CAR`.
    pub max_bytes: u64,
}

impl FetchConfig {
    /// Build a transport config with conservative defaults.
    #[must_use]
    pub fn new(spool_dir: impl Into<PathBuf>) -> Self {
        Self {
            spool_dir: spool_dir.into(),
            chunk_idle_timeout: DEFAULT_CHUNK_IDLE_TIMEOUT,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

/// A successfully spooled repo `CAR`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpooledRepo {
    /// Path to the local spooled `CAR`.
    pub car_path: PathBuf,
    /// HTTP status returned by `getRepo`.
    pub http_status: u16,
    /// Rate-limit headers observed on the response.
    pub rate_limit: RateLimitSnapshot,
    /// Bytes written to `car_path`.
    pub bytes: u64,
}

/// Parsed `ratelimit-*`, `x-ratelimit-*`, and `retry-after` headers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RateLimitSnapshot {
    /// Advertised request limit.
    pub limit: Option<u64>,
    /// Remaining requests in the current window.
    pub remaining: Option<u64>,
    /// Reset value as advertised by the host.
    pub reset: Option<u64>,
    /// Retry delay when the host provides a seconds-based `retry-after`.
    pub retry_after: Option<Duration>,
    /// Raw `ratelimit-policy` header.
    pub policy: Option<String>,
}

impl RateLimitSnapshot {
    /// Parse rate-limit headers from a response.
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Self {
        Self {
            limit: parse_u64_header(headers, "ratelimit-limit")
                .or_else(|| parse_u64_header(headers, "x-ratelimit-limit")),
            remaining: parse_u64_header(headers, "ratelimit-remaining")
                .or_else(|| parse_u64_header(headers, "x-ratelimit-remaining")),
            reset: parse_u64_header(headers, "ratelimit-reset")
                .or_else(|| parse_u64_header(headers, "x-ratelimit-reset")),
            retry_after: parse_u64_header(headers, "retry-after").map(Duration::from_secs),
            policy: parse_string_header(headers, "ratelimit-policy"),
        }
    }
}

/// Terminal account states returned by `com.atproto.sync.getRepo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum AccountState {
    /// The repo does not exist on this host.
    RepoNotFound,
    /// The repo is taken down.
    RepoTakendown,
    /// The repo is suspended.
    RepoSuspended,
    /// The repo is deactivated.
    RepoDeactivated,
}

impl fmt::Display for AccountState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::RepoNotFound => "RepoNotFound",
            Self::RepoTakendown => "RepoTakendown",
            Self::RepoSuspended => "RepoSuspended",
            Self::RepoDeactivated => "RepoDeactivated",
        };
        f.write_str(name)
    }
}

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
    /// A streaming transport error occurred before or during body download.
    Transport {
        /// Transport error message.
        message: String,
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
            Self::Transport { message } => write!(f, "transport error: {message}"),
            Self::Io { source } => write!(f, "I/O error: {source}"),
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
            | Self::MaxBytesExceeded { .. }
            | Self::ErrorBodyTooLarge { .. }
            | Self::Transport { .. } => None,
        }
    }
}

impl From<io::Error> for FetchError {
    fn from(source: io::Error) -> Self {
        Self::Io { source }
    }
}

/// Stream `com.atproto.sync.getRepo` from `pds` into a local spool file.
///
/// # Errors
///
/// Returns [`FetchError`] when the PDS reports an account state or HTTP error, the body
/// stalls, the loud byte cap is hit, the stream fails, or local filesystem I/O fails.
pub async fn fetch_repo<C>(
    http: &C,
    pds: &Uri<String>,
    did: &Did,
    config: &FetchConfig,
) -> Result<SpooledRepo, FetchError>
where
    C: HttpClient + HttpClientExt + Sync,
{
    fs::create_dir_all(&config.spool_dir)?;

    let request = GetRepo {
        did: did.clone(),
        since: None,
    };
    let response = http
        .xrpc(pds.borrow())
        .download(&request)
        .await
        .map_err(|err| FetchError::Transport {
            message: err.to_string(),
        })?;
    let status = response.status();
    let rate_limit = RateLimitSnapshot::from_headers(response.headers());
    let (_parts, body) = response.into_parts();

    if !status.is_success() {
        let body_bytes =
            collect_body_with_cap(body, config.chunk_idle_timeout, ERROR_BODY_MAX_BYTES).await?;
        return Err(classify_http_error(status, rate_limit, &body_bytes));
    }

    let car_path = spool_path(&config.spool_dir, did);
    let bytes =
        stream_to_file(body, &car_path, config.chunk_idle_timeout, config.max_bytes).await?;

    Ok(SpooledRepo {
        car_path,
        http_status: status.as_u16(),
        rate_limit,
        bytes,
    })
}

async fn stream_to_file(
    body: ByteStream,
    car_path: &Path,
    chunk_idle_timeout: Duration,
    max_bytes: u64,
) -> Result<u64, FetchError> {
    let temp_path = temp_spool_path(car_path)?;
    match stream_to_temp_file(body, &temp_path, chunk_idle_timeout, max_bytes).await {
        Ok(bytes) => {
            fs::rename(&temp_path, car_path)?;
            sync_parent_dir(car_path)?;
            Ok(bytes)
        }
        Err(error) => {
            let _ignored = fs::remove_file(&temp_path);
            Err(error)
        }
    }
}

async fn stream_to_temp_file(
    body: ByteStream,
    temp_path: &Path,
    chunk_idle_timeout: Duration,
    max_bytes: u64,
) -> Result<u64, FetchError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp_path)?;
    let mut bytes = 0_u64;
    let mut stream = body.into_inner();

    while let Some(next_chunk) = time::timeout(chunk_idle_timeout, stream.next())
        .await
        .map_err(|_elapsed| FetchError::InactivityTimeout {
            timeout: chunk_idle_timeout,
        })?
    {
        let chunk = next_chunk.map_err(|err| FetchError::Transport {
            message: err.to_string(),
        })?;
        let chunk_len =
            u64::try_from(chunk.len()).map_err(|_err| FetchError::MaxBytesExceeded {
                max_bytes,
                observed_bytes: u64::MAX,
            })?;
        let observed_bytes = bytes
            .checked_add(chunk_len)
            .ok_or(FetchError::MaxBytesExceeded {
                max_bytes,
                observed_bytes: u64::MAX,
            })?;
        if observed_bytes > max_bytes {
            return Err(FetchError::MaxBytesExceeded {
                max_bytes,
                observed_bytes,
            });
        }
        file.write_all(chunk.as_ref())?;
        bytes = observed_bytes;
    }

    file.sync_all()?;
    Ok(bytes)
}

fn temp_spool_path(car_path: &Path) -> Result<PathBuf, FetchError> {
    let file_name = car_path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "spool path has no file name")
        })?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| io::Error::other(format!("system clock before UNIX epoch: {error}")))?;
    let temp_name = format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        timestamp.as_nanos()
    );
    Ok(car_path.with_file_name(temp_name))
}

fn sync_parent_dir(path: &Path) -> Result<(), FetchError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)?.sync_all()?;
    Ok(())
}

async fn collect_body_with_cap(
    body: ByteStream,
    chunk_idle_timeout: Duration,
    max_bytes: u64,
) -> Result<Vec<u8>, FetchError> {
    let mut bytes = Vec::new();
    let mut observed = 0_u64;
    let mut stream = body.into_inner();

    while let Some(next_chunk) = time::timeout(chunk_idle_timeout, stream.next())
        .await
        .map_err(|_elapsed| FetchError::InactivityTimeout {
            timeout: chunk_idle_timeout,
        })?
    {
        let chunk = next_chunk.map_err(|err| FetchError::Transport {
            message: err.to_string(),
        })?;
        let chunk_len =
            u64::try_from(chunk.len()).map_err(|_err| FetchError::ErrorBodyTooLarge {
                max_bytes,
                observed_bytes: u64::MAX,
            })?;
        let next_observed =
            observed
                .checked_add(chunk_len)
                .ok_or(FetchError::ErrorBodyTooLarge {
                    max_bytes,
                    observed_bytes: u64::MAX,
                })?;
        if next_observed > max_bytes {
            return Err(FetchError::ErrorBodyTooLarge {
                max_bytes,
                observed_bytes: next_observed,
            });
        }
        bytes.extend_from_slice(chunk.as_ref());
        observed = next_observed;
    }

    Ok(bytes)
}

fn classify_http_error(
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

fn spool_path(spool_dir: &Path, did: &Did) -> PathBuf {
    let mut file_name = String::from("repo-");
    for ch in did.as_str().chars() {
        if ch.is_ascii_alphanumeric() {
            file_name.push(ch);
        } else {
            file_name.push('_');
        }
    }
    file_name.push_str(".car");
    spool_dir.join(file_name)
}

fn parse_u64_header(headers: &HeaderMap, name: &'static str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn parse_string_header(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn write_fetch_message(
    f: &mut fmt::Formatter<'_>,
    prefix: &fmt::Arguments<'_>,
    status: u16,
    message: Option<&str>,
) -> fmt::Result {
    match message {
        Some(message) => write!(f, "{prefix} at HTTP status {status}: {message}"),
        None => write!(f, "{prefix} at HTTP status {status}"),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]

    use std::{path::PathBuf, time::Duration};

    use http::{HeaderMap, StatusCode};
    use jacquard_common::types::did::Did;

    use super::{
        AccountState, FetchConfig, FetchError, RateLimitSnapshot, classify_http_error, spool_path,
    };

    #[test]
    fn parses_standard_rate_limit_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("ratelimit-limit", "3000".parse().unwrap());
        headers.insert("ratelimit-remaining", "2999".parse().unwrap());
        headers.insert("ratelimit-reset", "42".parse().unwrap());
        headers.insert("ratelimit-policy", "3000;w=300".parse().unwrap());
        headers.insert("retry-after", "5".parse().unwrap());

        let snapshot = RateLimitSnapshot::from_headers(&headers);

        assert_eq!(snapshot.limit, Some(3000));
        assert_eq!(snapshot.remaining, Some(2999));
        assert_eq!(snapshot.reset, Some(42));
        assert_eq!(snapshot.retry_after, Some(Duration::from_secs(5)));
        assert_eq!(snapshot.policy.as_deref(), Some("3000;w=300"));
    }

    #[test]
    fn falls_back_to_x_rate_limit_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-limit", "100".parse().unwrap());
        headers.insert("x-ratelimit-remaining", "7".parse().unwrap());
        headers.insert("x-ratelimit-reset", "99".parse().unwrap());

        let snapshot = RateLimitSnapshot::from_headers(&headers);

        assert_eq!(snapshot.limit, Some(100));
        assert_eq!(snapshot.remaining, Some(7));
        assert_eq!(snapshot.reset, Some(99));
    }

    #[test]
    fn classifies_repo_account_states() {
        let body = br#"{"error":"RepoSuspended","message":"nope"}"#;

        let err = classify_http_error(StatusCode::FORBIDDEN, RateLimitSnapshot::default(), body);

        match err {
            FetchError::AccountState {
                state,
                status,
                message,
                rate_limit: _,
            } => {
                assert_eq!(state, AccountState::RepoSuspended);
                assert_eq!(status, 403);
                assert_eq!(message.as_deref(), Some("nope"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn preserves_unknown_xrpc_error_code() {
        let body = br#"{"error":"HostThrottled","message":"slow down"}"#;

        let err = classify_http_error(
            StatusCode::TOO_MANY_REQUESTS,
            RateLimitSnapshot::default(),
            body,
        );

        match err {
            FetchError::HttpStatus {
                status,
                error_code,
                message,
                rate_limit: _,
            } => {
                assert_eq!(status, 429);
                assert_eq!(error_code.as_deref(), Some("HostThrottled"));
                assert_eq!(message.as_deref(), Some("slow down"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn default_config_sets_spool_dir_and_limits() {
        let config = FetchConfig::new(PathBuf::from("/tmp/spool"));

        assert_eq!(config.spool_dir, PathBuf::from("/tmp/spool"));
        assert_eq!(config.chunk_idle_timeout, Duration::from_secs(30));
        assert_eq!(config.max_bytes, 2_147_483_648);
    }

    #[test]
    fn spool_path_sanitizes_did() {
        let did = Did::new_owned("did:plc:abc123").unwrap();

        let path = spool_path(PathBuf::from("/tmp/spool").as_path(), &did);

        assert_eq!(path, PathBuf::from("/tmp/spool/repo-did_plc_abc123.car"));
    }
}
