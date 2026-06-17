//! Stage B `getRepo` transport.

use std::{
    error::Error,
    fmt,
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
use tempfile::NamedTempFile;
use tokio::{io::AsyncWriteExt as _, sync::Notify, time};

const DEFAULT_CHUNK_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(60);
#[allow(clippy::duration_suboptimal_units)]
const DEFAULT_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);
const DEFAULT_MAX_BYTES: u64 = 2_147_483_648;
const DEFAULT_UNKNOWN_BODY_RESERVATION_BYTES: u64 = 268_435_456;
const DEFAULT_MIN_PROGRESS_BYTES: u64 = 16_384;
const DEFAULT_MIN_PROGRESS_INTERVAL: Duration = Duration::from_secs(60);
const ERROR_BODY_MAX_BYTES: u64 = 65_536;

/// Runtime limits and local storage path for Stage B repo transport.
#[derive(Debug, Clone)]
pub struct FetchConfig {
    /// Directory where the streamed `CAR` is written.
    pub spool_dir: PathBuf,
    /// Maximum silence while waiting for the next body chunk.
    pub chunk_idle_timeout: Duration,
    /// Maximum wall time waiting for response headers.
    pub response_header_timeout: Duration,
    /// Maximum wall time for one successful body download.
    pub download_timeout: Duration,
    /// Minimum byte progress expected during a progress window.
    pub min_progress_bytes: u64,
    /// Progress watchdog window.
    pub min_progress_interval: Duration,
    /// Loud single-repo byte cap for the spooled `CAR`.
    pub max_bytes: u64,
    /// Admission reservation for successful bodies without a usable `Content-Length`.
    pub unknown_body_reservation_bytes: u64,
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
            response_header_timeout: DEFAULT_RESPONSE_HEADER_TIMEOUT,
            download_timeout: DEFAULT_DOWNLOAD_TIMEOUT,
            min_progress_bytes: DEFAULT_MIN_PROGRESS_BYTES,
            min_progress_interval: DEFAULT_MIN_PROGRESS_INTERVAL,
            max_bytes: DEFAULT_MAX_BYTES,
            unknown_body_reservation_bytes: DEFAULT_UNKNOWN_BODY_RESERVATION_BYTES,
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

    fn try_reserve_capacity(&mut self, bytes: u64) -> Result<(), FetchError> {
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
        if delta == 0 || self.budget.inner.max_bytes == 0 {
            self.charged_bytes = charged_target;
            return Ok(());
        }
        let mut charged = self
            .budget
            .inner
            .charged_bytes
            .lock()
            .map_err(|_error| FetchError::ByteBudgetPoisoned)?;
        let next = charged
            .checked_add(delta)
            .ok_or(FetchError::InFlightBytesExceeded {
                max_bytes: self.budget.inner.max_bytes,
                observed_bytes: u64::MAX,
            })?;
        if next > self.budget.inner.max_bytes {
            return Err(FetchError::InFlightBytesUnavailable {
                max_bytes: self.budget.inner.max_bytes,
                requested_bytes: charged_target,
            });
        }
        *charged = next;
        drop(charged);
        self.charged_bytes = charged_target;
        Ok(())
    }

    fn shrink_to_actual(&mut self, bytes: u64) -> Result<(), FetchError> {
        if bytes > self.charged_bytes {
            return Err(FetchError::InFlightBytesExceeded {
                max_bytes: self.budget.inner.max_bytes,
                observed_bytes: bytes,
            });
        }
        let delta =
            self.charged_bytes
                .checked_sub(bytes)
                .ok_or(FetchError::InFlightBytesExceeded {
                    max_bytes: self.budget.inner.max_bytes,
                    observed_bytes: bytes,
                })?;
        self.charged_bytes = bytes;
        self.budget.release_charged(delta);
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

mod rate_limit;
#[cfg(test)]
use rate_limit::parse_http_date_retry_after;
pub use rate_limit::{AccountState, RateLimitSnapshot};

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
    fetch_repo_with_rate_limit_observer(http, pds, did, config, |_rate_limit| {}).await
}

/// Stream `com.atproto.sync.getRepo` and report response rate-limit headers before body reads.
///
/// # Errors
///
/// Returns [`FetchError`] when response headers, body streaming, caps, or local I/O fail.
pub async fn fetch_repo_with_rate_limit_observer<C>(
    http: &C,
    pds: &Uri<String>,
    did: &Did,
    config: &FetchConfig,
    mut observe_rate_limit: impl FnMut(&RateLimitSnapshot),
) -> Result<SpooledRepo, FetchError>
where
    C: HttpClient + HttpClientExt + Sync,
{
    fs::create_dir_all(&config.spool_dir)?;

    let request = GetRepo {
        did: did.clone(),
        since: None,
    };
    let response = time::timeout(
        config.response_header_timeout,
        http.xrpc(pds.borrow()).download(&request),
    )
    .await
    .map_err(|_elapsed| FetchError::ResponseHeaderTimeout {
        timeout: config.response_header_timeout,
    })?
    .map_err(|err| FetchError::Transport {
        message: err.to_string(),
        observed_bytes: None,
    })?;
    let status = response.status();
    let rate_limit = RateLimitSnapshot::from_headers(response.headers());
    observe_rate_limit(&rate_limit);
    let admission_body_bytes = if status.is_success() {
        Some(admission_body_bytes(
            response.headers(),
            config.max_bytes,
            config.unknown_body_reservation_bytes,
        )?)
    } else {
        None
    };
    let (_parts, body) = response.into_parts();

    if !status.is_success() {
        let body_bytes = collect_body_with_cap(
            body,
            StreamLimits {
                chunk_idle_timeout: config.chunk_idle_timeout,
                download_timeout: config.download_timeout,
                min_progress_bytes: config.min_progress_bytes,
                min_progress_interval: config.min_progress_interval,
                max_bytes: ERROR_BODY_MAX_BYTES,
            },
        )
        .await?;
        return Err(classify_http_error(status, rate_limit, &body_bytes));
    }

    let car_path = spool_path(&config.spool_dir, did);
    let (bytes, byte_budget_reservation) = stream_to_file(
        body,
        &car_path,
        StreamLimits {
            chunk_idle_timeout: config.chunk_idle_timeout,
            download_timeout: config.download_timeout,
            min_progress_bytes: config.min_progress_bytes,
            min_progress_interval: config.min_progress_interval,
            max_bytes: config.max_bytes,
        },
        admission_body_bytes.unwrap_or(config.max_bytes),
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

#[derive(Debug, Clone, Copy)]
struct StreamLimits {
    chunk_idle_timeout: Duration,
    download_timeout: Duration,
    min_progress_bytes: u64,
    min_progress_interval: Duration,
    max_bytes: u64,
}

async fn stream_to_file(
    body: ByteStream,
    car_path: &Path,
    limits: StreamLimits,
    admission_body_bytes: u64,
    byte_budget: Option<&FetchByteBudget>,
) -> Result<(u64, Option<FetchByteBudgetReservation>), FetchError> {
    let parent = car_path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "spool path has no parent"))?;
    let temp_file = NamedTempFile::new_in(parent)?;
    let mut reservation = byte_budget.map(FetchByteBudget::reservation);
    if let Some(reservation) = reservation.as_mut() {
        reservation.reserve_capacity(admission_body_bytes).await?;
    }
    match stream_to_temp_file(body, temp_file.path(), limits, reservation.as_mut()).await {
        Ok(bytes) => {
            if let Some(reservation) = reservation.as_mut() {
                reservation.shrink_to_actual(bytes)?;
            }
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
    path: &Path,
    limits: StreamLimits,
    mut byte_budget_reservation: Option<&mut FetchByteBudgetReservation>,
) -> Result<u64, FetchError> {
    let mut bytes = 0_u64;
    let mut stream = body.into_inner();
    let mut file = tokio::fs::OpenOptions::new().write(true).open(path).await?;
    let started = Instant::now();
    let mut progress_window_started = started;
    let mut progress_window_bytes = 0_u64;

    while let Some(next_chunk) = time::timeout(
        next_chunk_timeout(
            started,
            limits.download_timeout,
            limits.chunk_idle_timeout,
            bytes,
        )?,
        stream.next(),
    )
    .await
    .map_err(|_elapsed| {
        timeout_error(
            started,
            limits.download_timeout,
            limits.chunk_idle_timeout,
            bytes,
        )
    })? {
        enforce_progress(
            &mut progress_window_started,
            &mut progress_window_bytes,
            limits.min_progress_interval,
            limits.min_progress_bytes,
        )?;
        let chunk = next_chunk.map_err(|err| FetchError::Transport {
            message: err.to_string(),
            observed_bytes: Some(bytes),
        })?;
        let chunk_len =
            u64::try_from(chunk.len()).map_err(|_err| FetchError::MaxBytesExceeded {
                max_bytes: limits.max_bytes,
                observed_bytes: u64::MAX,
            })?;
        let observed_bytes = bytes
            .checked_add(chunk_len)
            .ok_or(FetchError::MaxBytesExceeded {
                max_bytes: limits.max_bytes,
                observed_bytes: u64::MAX,
            })?;
        if observed_bytes > limits.max_bytes {
            return Err(FetchError::MaxBytesExceeded {
                max_bytes: limits.max_bytes,
                observed_bytes,
            });
        }
        if let Some(reservation) = byte_budget_reservation.as_mut() {
            reservation.try_reserve_capacity(observed_bytes)?;
        }
        file.write_all(chunk.as_ref()).await?;
        bytes = observed_bytes;
        progress_window_bytes =
            progress_window_bytes
                .checked_add(chunk_len)
                .ok_or(FetchError::ProgressTimeout {
                    interval: limits.min_progress_interval,
                    min_bytes: limits.min_progress_bytes,
                    observed_bytes: u64::MAX,
                })?;
    }

    file.sync_all().await?;
    Ok(bytes)
}

fn next_chunk_timeout(
    started: Instant,
    download_timeout: Duration,
    chunk_idle_timeout: Duration,
    bytes: u64,
) -> Result<Duration, FetchError> {
    let Some(remaining) = download_timeout.checked_sub(started.elapsed()) else {
        return Err(FetchError::DownloadTimeout {
            timeout: download_timeout,
            observed_bytes: bytes,
        });
    };
    Ok(remaining.min(chunk_idle_timeout))
}

fn timeout_error(
    started: Instant,
    download_timeout: Duration,
    chunk_idle_timeout: Duration,
    bytes: u64,
) -> FetchError {
    if started.elapsed() >= download_timeout {
        FetchError::DownloadTimeout {
            timeout: download_timeout,
            observed_bytes: bytes,
        }
    } else {
        FetchError::InactivityTimeout {
            timeout: chunk_idle_timeout,
        }
    }
}

fn enforce_progress(
    window_started: &mut Instant,
    window_bytes: &mut u64,
    min_progress_interval: Duration,
    min_progress_bytes: u64,
) -> Result<(), FetchError> {
    if min_progress_bytes == 0 || min_progress_interval.is_zero() {
        return Ok(());
    }
    if window_started.elapsed() < min_progress_interval {
        return Ok(());
    }
    if *window_bytes < min_progress_bytes {
        return Err(FetchError::ProgressTimeout {
            interval: min_progress_interval,
            min_bytes: min_progress_bytes,
            observed_bytes: *window_bytes,
        });
    }
    *window_started = Instant::now();
    *window_bytes = 0;
    Ok(())
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
    limits: StreamLimits,
) -> Result<Vec<u8>, FetchError> {
    let mut bytes = Vec::new();
    let mut observed = 0_u64;
    let mut stream = body.into_inner();
    let started = Instant::now();
    let mut progress_window_started = started;
    let mut progress_window_bytes = 0_u64;

    while let Some(next_chunk) = time::timeout(
        next_chunk_timeout(
            started,
            limits.download_timeout,
            limits.chunk_idle_timeout,
            observed,
        )?,
        stream.next(),
    )
    .await
    .map_err(|_elapsed| {
        timeout_error(
            started,
            limits.download_timeout,
            limits.chunk_idle_timeout,
            observed,
        )
    })? {
        enforce_progress(
            &mut progress_window_started,
            &mut progress_window_bytes,
            limits.min_progress_interval,
            limits.min_progress_bytes,
        )?;
        let chunk = next_chunk.map_err(|err| FetchError::Transport {
            message: err.to_string(),
            observed_bytes: Some(observed),
        })?;
        let chunk_len =
            u64::try_from(chunk.len()).map_err(|_err| FetchError::ErrorBodyTooLarge {
                max_bytes: limits.max_bytes,
                observed_bytes: u64::MAX,
            })?;
        let next_observed =
            observed
                .checked_add(chunk_len)
                .ok_or(FetchError::ErrorBodyTooLarge {
                    max_bytes: limits.max_bytes,
                    observed_bytes: u64::MAX,
                })?;
        if next_observed > limits.max_bytes {
            return Err(FetchError::ErrorBodyTooLarge {
                max_bytes: limits.max_bytes,
                observed_bytes: next_observed,
            });
        }
        bytes.extend_from_slice(chunk.as_ref());
        observed = next_observed;
        progress_window_bytes =
            progress_window_bytes
                .checked_add(chunk_len)
                .ok_or(FetchError::ProgressTimeout {
                    interval: limits.min_progress_interval,
                    min_bytes: limits.min_progress_bytes,
                    observed_bytes: u64::MAX,
                })?;
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

fn admission_body_bytes(
    headers: &HeaderMap,
    max_bytes: u64,
    unknown_body_reservation_bytes: u64,
) -> Result<u64, FetchError> {
    let unknown_body_reservation_bytes = unknown_body_reservation_bytes.min(max_bytes);
    let Some(value) = headers.get(http::header::CONTENT_LENGTH) else {
        return Ok(unknown_body_reservation_bytes);
    };
    let Ok(value) = value.to_str() else {
        return Ok(unknown_body_reservation_bytes);
    };
    let Ok(bytes) = value.parse::<u64>() else {
        return Ok(unknown_body_reservation_bytes);
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
mod tests;
