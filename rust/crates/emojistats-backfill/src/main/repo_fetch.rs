use std::time::{Duration, Instant, SystemTime};

use jacquard_common::{deps::fluent_uri::Uri, types::did::Did};
use sha2::{Digest, Sha256};

use super::{
    super::{
        CRAWLER_USER_AGENT, FETCH_TRANSPORT_ATTEMPTS, FETCH_TRANSPORT_RETRY_BASE_DELAY,
        FETCH_TRANSPORT_RETRY_MAX_DELAY, cli::HttpProtocol, failure::elapsed_ms,
    },
    host_rate_limit::record_rate_limit_snapshot,
    processed_repo::FetchedRepo,
};
use crate::{
    scheduler::{HostPacer, SharedHostPacer},
    transport::{FetchConfig, FetchError, fetch_repo_with_rate_limit_observer},
};

pub(super) struct FetchStep<'a> {
    pub(super) http: &'a reqwest::Client,
    pub(super) pds: &'a Uri<String>,
    pub(super) did: &'a Did,
    pub(super) did_str: &'a str,
    pub(super) host: &'a str,
    pub(super) host_min_interval: Option<Duration>,
    pub(super) config: &'a FetchConfig,
    pub(super) host_pacer: Option<&'a SharedHostPacer>,
}

pub(super) async fn fetch_spooled_repo(step: FetchStep<'_>) -> Result<FetchedRepo, FetchError> {
    let fetch_started = Instant::now();
    let mut attempt = 1_u8;
    loop {
        reserve_host_send_for_fetch(step.host_pacer, step.host, step.host_min_interval).await?;
        match fetch_repo_with_rate_limit_observer(
            step.http,
            step.pds,
            step.did,
            step.config,
            |rate_limit| {
                record_rate_limit_snapshot(
                    step.host_pacer,
                    step.host,
                    rate_limit,
                    SystemTime::now(),
                );
            },
        )
        .await
        {
            Ok(spooled) => {
                return Ok(FetchedRepo {
                    spooled,
                    fetch_ms: elapsed_ms(fetch_started),
                });
            }
            Err(err)
                if is_retryable_stream_fetch_error(&err) && attempt < FETCH_TRANSPORT_ATTEMPTS =>
            {
                let delay = transport_retry_delay(step.did_str, attempt);
                eprintln!(
                    "fetch retry {next_attempt}/{max_attempts} for {did} after {delay_ms} ms: {err}",
                    next_attempt = attempt.saturating_add(1),
                    max_attempts = FETCH_TRANSPORT_ATTEMPTS,
                    did = step.did_str,
                    delay_ms = delay.as_millis()
                );
                tokio::time::sleep(delay).await;
                attempt = attempt.saturating_add(1);
            }
            Err(err) => {
                return Err(err);
            }
        }
    }
}

pub(super) fn repo_fetch_client(
    http_protocol: HttpProtocol,
) -> Result<reqwest::Client, reqwest::Error> {
    let builder = reqwest::Client::builder()
        .user_agent(CRAWLER_USER_AGENT)
        .tcp_keepalive(Duration::from_secs(60))
        .connect_timeout(Duration::from_secs(30));
    match http_protocol {
        HttpProtocol::Http1 => builder.http1_only().build(),
        HttpProtocol::Auto => builder.build(),
    }
}

async fn reserve_host_send_for_fetch(
    host_pacer: Option<&SharedHostPacer>,
    host: &str,
    min_interval: Option<Duration>,
) -> Result<(), FetchError> {
    let Some(pacer) = host_pacer else {
        return Ok(());
    };
    HostPacer::reserve_next_request(pacer, host, min_interval)
        .await
        .map_err(|err| FetchError::Transport {
            message: format!("host pacing for {host}: {err}"),
            observed_bytes: None,
            source: Box::new(err),
        })
}

fn transport_retry_delay(did: &str, failed_attempt: u8) -> Duration {
    let exponent = u32::from(failed_attempt.saturating_sub(1));
    let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
    let base = FETCH_TRANSPORT_RETRY_BASE_DELAY
        .checked_mul(multiplier)
        .unwrap_or(FETCH_TRANSPORT_RETRY_MAX_DELAY)
        .min(FETCH_TRANSPORT_RETRY_MAX_DELAY);
    base.checked_add(transport_retry_jitter(did, failed_attempt, base))
        .unwrap_or(FETCH_TRANSPORT_RETRY_MAX_DELAY)
        .min(FETCH_TRANSPORT_RETRY_MAX_DELAY)
}

fn transport_retry_jitter(did: &str, failed_attempt: u8, base: Duration) -> Duration {
    let window_millis = u64::try_from(base.as_millis() / 2).unwrap_or(u64::MAX);
    if window_millis == 0 {
        return Duration::ZERO;
    }
    let modulus = window_millis.saturating_add(1);
    let mut hasher = Sha256::new();
    hasher.update(did.as_bytes());
    hasher.update([failed_attempt]);
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    for (destination, source) in bytes.iter_mut().zip(digest) {
        *destination = source;
    }
    let jitter_millis = u64::from_be_bytes(bytes).checked_rem(modulus).unwrap_or(0);
    Duration::from_millis(jitter_millis)
}

const fn is_retryable_stream_fetch_error(error: &FetchError) -> bool {
    matches!(
        error,
        FetchError::Transport { .. }
            | FetchError::InactivityTimeout { .. }
            | FetchError::DownloadTimeout { .. }
            | FetchError::ResponseHeaderTimeout { .. }
            | FetchError::ProgressTimeout { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn immediate_stream_retry_includes_timeout_categories() {
        assert!(is_retryable_stream_fetch_error(&FetchError::Transport {
            message: "connection reset".to_owned(),
            observed_bytes: None,
            source: Box::new(std::io::Error::other("connection reset")),
        }));
        assert!(is_retryable_stream_fetch_error(
            &FetchError::InactivityTimeout {
                timeout: Duration::from_secs(30),
            }
        ));
        assert!(is_retryable_stream_fetch_error(
            &FetchError::DownloadTimeout {
                timeout: Duration::from_secs(600),
                observed_bytes: 12,
            }
        ));
        assert!(is_retryable_stream_fetch_error(
            &FetchError::ResponseHeaderTimeout {
                timeout: Duration::from_secs(60),
            }
        ));
        assert!(is_retryable_stream_fetch_error(
            &FetchError::ProgressTimeout {
                interval: Duration::from_secs(60),
                min_bytes: 16_384,
                observed_bytes: 1024,
            }
        ));
    }
}
