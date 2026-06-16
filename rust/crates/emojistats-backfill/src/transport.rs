//! Stage B `getRepo` transport.

use std::{
    error::Error,
    fmt,
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
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
use tempfile::NamedTempFile;
use tokio::{sync::Notify, time};

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
    /// Optional fleet-wide byte budget for in-flight spooled `CAR` bytes.
    pub byte_budget: Option<FetchByteBudget>,
}

impl FetchConfig {
    /// Build a transport config with conservative defaults.
    #[must_use]
    pub fn new(spool_dir: impl Into<PathBuf>) -> Self {
        Self {
            spool_dir: spool_dir.into(),
            chunk_idle_timeout: DEFAULT_CHUNK_IDLE_TIMEOUT,
            max_bytes: DEFAULT_MAX_BYTES,
            byte_budget: None,
        }
    }
}

/// Shared cap for bytes currently held by in-flight streamed `CAR` files.
#[derive(Debug, Clone)]
pub struct FetchByteBudget {
    inner: Arc<FetchByteBudgetInner>,
}

#[derive(Debug)]
struct FetchByteBudgetInner {
    max_bytes: u64,
    charged_bytes: Mutex<u64>,
    notify: Notify,
}

impl FetchByteBudget {
    /// Build a shared in-flight byte budget.
    #[must_use]
    pub fn new(max_bytes: u64) -> Self {
        Self {
            inner: Arc::new(FetchByteBudgetInner {
                max_bytes,
                charged_bytes: Mutex::new(0),
                notify: Notify::new(),
            }),
        }
    }

    fn reservation(&self) -> FetchByteBudgetReservation {
        FetchByteBudgetReservation {
            budget: self.clone(),
            charged_bytes: 0,
        }
    }

    async fn reserve_charged_delta(&self, delta: u64) -> Result<(), FetchError> {
        if delta == 0 || self.inner.max_bytes == 0 {
            return Ok(());
        }
        loop {
            let notified = self.inner.notify.notified();
            {
                let mut charged = self
                    .inner
                    .charged_bytes
                    .lock()
                    .map_err(|_error| FetchError::ByteBudgetPoisoned)?;
                let next = charged
                    .checked_add(delta)
                    .ok_or(FetchError::InFlightBytesExceeded {
                        max_bytes: self.inner.max_bytes,
                        observed_bytes: u64::MAX,
                    })?;
                if next <= self.inner.max_bytes {
                    *charged = next;
                    drop(charged);
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    fn release_charged(&self, bytes: u64) {
        if bytes == 0 || self.inner.max_bytes == 0 {
            return;
        }
        if let Ok(mut charged) = self.inner.charged_bytes.lock() {
            *charged = charged.saturating_sub(bytes);
        }
        self.inner.notify.notify_waiters();
    }
}

/// Held with a spooled repo until parse/archive no longer needs the local `CAR`.
#[derive(Debug)]
pub struct FetchByteBudgetReservation {
    budget: FetchByteBudget,
    charged_bytes: u64,
}

impl FetchByteBudgetReservation {
    async fn reserve_capacity(&mut self, bytes: u64) -> Result<(), FetchError> {
        if bytes > self.budget.inner.max_bytes {
            return Err(FetchError::InFlightBytesExceeded {
                max_bytes: self.budget.inner.max_bytes,
                observed_bytes: bytes,
            });
        }
        if bytes <= self.charged_bytes {
            return Ok(());
        }
        let charged_target = bytes;
        let delta = charged_target.checked_sub(self.charged_bytes).ok_or(
            FetchError::InFlightBytesExceeded {
                max_bytes: self.budget.inner.max_bytes,
                observed_bytes: bytes,
            },
        )?;
        self.budget.reserve_charged_delta(delta).await?;
        self.charged_bytes = charged_target;
        Ok(())
    }

    #[cfg(test)]
    const fn charged_bytes(&self) -> u64 {
        self.charged_bytes
    }
}

impl Drop for FetchByteBudgetReservation {
    fn drop(&mut self) {
        self.budget.release_charged(self.charged_bytes);
    }
}

/// A successfully spooled repo `CAR`.
#[derive(Debug)]
pub struct SpooledRepo {
    /// Path to the local spooled `CAR`.
    pub car_path: PathBuf,
    /// HTTP status returned by `getRepo`.
    pub http_status: u16,
    /// Rate-limit headers observed on the response.
    pub rate_limit: RateLimitSnapshot,
    /// Bytes written to `car_path`.
    pub bytes: u64,
    _byte_budget_reservation: Option<FetchByteBudgetReservation>,
}

impl Drop for SpooledRepo {
    fn drop(&mut self) {
        let _ignored = fs::remove_file(&self.car_path);
    }
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
            retry_after: parse_retry_after_header(headers),
            policy: parse_string_header(headers, "ratelimit-policy"),
        }
    }

    /// Return the host cooldown implied by these headers, if any.
    #[must_use]
    pub fn cooldown_delay(&self, now: SystemTime) -> Option<Duration> {
        if let Some(retry_after) = self.retry_after {
            return Some(retry_after);
        }
        if self.remaining != Some(0) {
            return None;
        }
        self.reset.and_then(|reset| reset_delay(reset, now))
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
    /// The fleet-wide in-flight spool byte budget was exceeded.
    InFlightBytesExceeded {
        /// Configured cap.
        max_bytes: u64,
        /// Bytes observed after accepting the chunk that crossed the cap.
        observed_bytes: u64,
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
            | Self::MaxBytesExceeded { .. }
            | Self::ErrorBodyTooLarge { .. }
            | Self::InFlightBytesExceeded { .. }
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
            | Self::MaxBytesExceeded { .. }
            | Self::ErrorBodyTooLarge { .. }
            | Self::InFlightBytesExceeded { .. }
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
            observed_bytes: None,
        })?;
    let status = response.status();
    let rate_limit = RateLimitSnapshot::from_headers(response.headers());
    let expected_body_bytes = if status.is_success() {
        Some(expected_body_bytes(response.headers(), config.max_bytes)?)
    } else {
        None
    };
    let (_parts, body) = response.into_parts();

    if !status.is_success() {
        let body_bytes =
            collect_body_with_cap(body, config.chunk_idle_timeout, ERROR_BODY_MAX_BYTES).await?;
        return Err(classify_http_error(status, rate_limit, &body_bytes));
    }

    let car_path = spool_path(&config.spool_dir, did);
    let (bytes, byte_budget_reservation) = stream_to_file(
        body,
        &car_path,
        config.chunk_idle_timeout,
        config.max_bytes,
        expected_body_bytes.unwrap_or(config.max_bytes),
        config.byte_budget.as_ref(),
    )
    .await?;

    Ok(SpooledRepo {
        car_path,
        http_status: status.as_u16(),
        rate_limit,
        bytes,
        _byte_budget_reservation: byte_budget_reservation,
    })
}

async fn stream_to_file(
    body: ByteStream,
    car_path: &Path,
    chunk_idle_timeout: Duration,
    max_bytes: u64,
    expected_body_bytes: u64,
    byte_budget: Option<&FetchByteBudget>,
) -> Result<(u64, Option<FetchByteBudgetReservation>), FetchError> {
    let parent = car_path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "spool path has no parent"))?;
    let mut temp_file = NamedTempFile::new_in(parent)?;
    let mut reservation = byte_budget.map(FetchByteBudget::reservation);
    if let Some(reservation) = reservation.as_mut() {
        reservation.reserve_capacity(expected_body_bytes).await?;
    }
    match stream_to_temp_file(
        body,
        temp_file.as_file_mut(),
        chunk_idle_timeout,
        max_bytes,
        reservation.as_mut(),
    )
    .await
    {
        Ok(bytes) => {
            temp_file.persist_noclobber(car_path).map_err(|error| {
                io::Error::new(
                    error.error.kind(),
                    format!(
                        "persist spooled temp file without overwrite: {}",
                        error.error
                    ),
                )
            })?;
            sync_parent_dir(car_path)?;
            Ok((bytes, reservation))
        }
        Err(error) => Err(error),
    }
}

async fn stream_to_temp_file(
    body: ByteStream,
    file: &mut File,
    chunk_idle_timeout: Duration,
    max_bytes: u64,
    mut byte_budget_reservation: Option<&mut FetchByteBudgetReservation>,
) -> Result<u64, FetchError> {
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
            observed_bytes: Some(bytes),
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
        if let Some(reservation) = byte_budget_reservation.as_mut() {
            reservation.reserve_capacity(observed_bytes).await?;
        }
        file.write_all(chunk.as_ref())?;
        bytes = observed_bytes;
    }

    file.sync_all()?;
    Ok(bytes)
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
            observed_bytes: Some(observed),
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

fn expected_body_bytes(headers: &HeaderMap, max_bytes: u64) -> Result<u64, FetchError> {
    let Some(value) = headers.get(http::header::CONTENT_LENGTH) else {
        return Ok(max_bytes);
    };
    let Ok(value) = value.to_str() else {
        return Ok(max_bytes);
    };
    let Ok(bytes) = value.parse::<u64>() else {
        return Ok(max_bytes);
    };
    if bytes > max_bytes {
        return Err(FetchError::MaxBytesExceeded {
            max_bytes,
            observed_bytes: bytes,
        });
    }
    Ok(bytes)
}

fn spool_path(spool_dir: &Path, did: &Did) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let mut file_name = String::from("repo-");
    for ch in did.as_str().chars() {
        if ch.is_ascii_alphanumeric() {
            file_name.push(ch);
        } else {
            file_name.push('_');
        }
    }
    file_name.push('.');
    file_name.push_str(&std::process::id().to_string());
    file_name.push('.');
    file_name.push_str(&timestamp.to_string());
    file_name.push_str(".car");
    spool_dir.join(file_name)
}

fn parse_u64_header(headers: &HeaderMap, name: &'static str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn parse_retry_after_header(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get("retry-after")?.to_str().ok()?.trim();
    value
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| parse_http_date_retry_after(value, SystemTime::now()))
}

fn parse_http_date_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    httpdate::parse_http_date(value)
        .ok()?
        .duration_since(now)
        .ok()
}

fn reset_delay(reset: u64, now: SystemTime) -> Option<Duration> {
    let now = now.duration_since(UNIX_EPOCH).ok()?.as_secs();
    if reset > now {
        return reset.checked_sub(now).map(Duration::from_secs);
    }
    if (1..=86_400).contains(&reset) {
        return Some(Duration::from_secs(reset));
    }
    None
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

    use std::{
        path::PathBuf,
        time::{Duration, UNIX_EPOCH},
    };

    use bytes::Bytes;
    use http::{HeaderMap, StatusCode};
    use jacquard_common::{stream::ByteStream, types::did::Did};

    use super::{
        AccountState, FetchByteBudget, FetchConfig, FetchError, RateLimitSnapshot,
        classify_http_error, parse_http_date_retry_after, spool_path, stream_to_file,
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
    #[allow(clippy::duration_suboptimal_units)]
    fn parses_http_date_retry_after() {
        let delay = parse_http_date_retry_after(
            "Tue, 16 Jun 2026 00:00:10 GMT",
            UNIX_EPOCH + Duration::from_secs(1_781_568_000),
        );

        assert_eq!(delay, Some(Duration::from_secs(10)));
    }

    #[test]
    #[allow(clippy::duration_suboptimal_units)]
    fn cooldown_delay_uses_empty_remaining_reset() {
        let snapshot = RateLimitSnapshot {
            remaining: Some(0),
            reset: Some(1_781_568_030),
            ..RateLimitSnapshot::default()
        };

        assert_eq!(
            snapshot.cooldown_delay(UNIX_EPOCH + Duration::from_secs(1_781_568_000)),
            Some(Duration::from_secs(30))
        );
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
        assert!(config.byte_budget.is_none());
    }

    #[tokio::test]
    async fn byte_budget_blocks_until_prior_reservation_is_dropped() {
        let budget = FetchByteBudget::new(10);
        let mut first = budget.reservation();
        first.reserve_capacity(10).await.unwrap();
        let mut second = budget.reservation();

        let blocked =
            tokio::time::timeout(Duration::from_millis(10), second.reserve_capacity(1)).await;

        assert!(blocked.is_err());
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), second.reserve_capacity(1))
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn byte_budget_rejects_single_repo_larger_than_budget() {
        let budget = FetchByteBudget::new(10);
        let mut reservation = budget.reservation();

        let error = reservation.reserve_capacity(11).await.unwrap_err();

        match error {
            FetchError::InFlightBytesExceeded {
                max_bytes,
                observed_bytes,
            } => {
                assert_eq!(max_bytes, 10);
                assert_eq!(observed_bytes, 11);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(reservation.charged_bytes(), 0);
    }

    #[tokio::test]
    async fn byte_budget_blocks_before_streaming_file() {
        let budget = FetchByteBudget::new(10);
        let mut first = budget.reservation();
        first.reserve_capacity(8).await.unwrap();
        let path = std::env::temp_dir().join(format!(
            "emojistats-byte-budget-stream-{}-{}.car",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let body = ByteStream::new(futures_util::stream::iter([Ok(Bytes::from_static(b"abc"))]));
        let task_path = path.clone();
        let handle = tokio::spawn(async move {
            stream_to_file(
                body,
                &task_path,
                Duration::from_secs(1),
                3,
                3,
                Some(&budget),
            )
            .await
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!handle.is_finished());
        assert!(!path.exists() || std::fs::metadata(&path).unwrap().len() == 0);

        drop(first);
        let (result, reservation) = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(result, 3);
        assert_eq!(reservation.unwrap().charged_bytes(), 3);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn spool_path_sanitizes_did() {
        let did = Did::new_owned("did:plc:abc123").unwrap();

        let path = spool_path(PathBuf::from("/tmp/spool").as_path(), &did);

        let file_name = path.file_name().and_then(std::ffi::OsStr::to_str).unwrap();
        assert!(file_name.starts_with("repo-did_plc_abc123."));
        assert!(
            std::path::Path::new(file_name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("car"))
        );
    }
}
