//! Stage B `getRepo` transport.

use std::{
    error::Error,
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use futures_util::StreamExt as _;
use http::HeaderMap;
use jacquard_api::com_atproto::sync::get_repo::GetRepo;
use jacquard_common::{
    deps::fluent_uri::Uri,
    http_client::{HttpClient, HttpClientExt},
    stream::ByteStream,
    types::did::Did,
    xrpc::XrpcExt as _,
};
use tempfile::NamedTempFile;
use tokio::{io::AsyncWriteExt as _, time};

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

mod rate_limit;
#[cfg(test)]
use rate_limit::parse_http_date_retry_after;
pub use rate_limit::{AccountState, RateLimitSnapshot};
mod error;
pub use error::FetchError;
use error::classify_http_error;
mod spool;
pub use spool::{FetchByteBudget, SpooledRepo};
use spool::{FetchByteBudgetReservation, spool_path, sync_parent_dir};

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
    .map_err(|err| transport_error(err, None))?;
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
        let chunk = next_chunk.map_err(|err| transport_error(err, Some(bytes)))?;
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
        enforce_progress(
            &mut progress_window_started,
            &mut progress_window_bytes,
            limits.min_progress_interval,
            limits.min_progress_bytes,
        )?;
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
        let chunk = next_chunk.map_err(|err| transport_error(err, Some(observed)))?;
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

fn transport_error<E>(error: E, observed_bytes: Option<u64>) -> FetchError
where
    E: Error + Send + Sync + 'static,
{
    let message = error_chain_message(&error);
    if is_permanent_transport_error(&error) {
        FetchError::PermanentTransport {
            message,
            observed_bytes,
            source: Box::new(error),
        }
    } else {
        FetchError::Transport {
            message,
            observed_bytes,
            source: Box::new(error),
        }
    }
}

fn is_permanent_transport_error(error: &(dyn Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(reqwest_error) = error.downcast_ref::<reqwest::Error>()
            && reqwest_error.is_builder()
        {
            return true;
        }
        let text = error.to_string().to_ascii_lowercase();
        if text.contains("dns error")
            || text.contains("failed to lookup address information")
            || text.contains("invalid peer certificate")
            || text.contains("certificate verify failed")
            || text.contains("self signed certificate")
            || text.contains("unknown issuer")
        {
            return true;
        }
        current = error.source();
    }
    false
}

fn error_chain_message(error: &(dyn Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut current = error.source();
    while let Some(source) = current {
        message.push_str(": ");
        message.push_str(&source.to_string());
        current = source.source();
    }
    message
}

#[cfg(test)]
mod tests;
