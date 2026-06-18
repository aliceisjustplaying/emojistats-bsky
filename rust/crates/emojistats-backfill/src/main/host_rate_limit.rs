#![allow(clippy::redundant_pub_crate)]

use std::time::SystemTime;

use super::super::FetchOneFailure;
use crate::{
    ledger::AttemptOutcome,
    scheduler::{HostPacer, SharedHostPacer},
    transport::RateLimitSnapshot,
};

pub(crate) fn record_rate_limit_cooldown(
    host_pacer: Option<&SharedHostPacer>,
    host: &str,
    failure: &FetchOneFailure,
) {
    if let AttemptOutcome::RateLimited { retry_after } = &failure.outcome
        && let Some(pacer) = host_pacer
        && let Err(pacer_error) = HostPacer::record_retry_after(pacer, host, *retry_after)
    {
        eprintln!("failed to record host cooldown for {host}: {pacer_error}");
    }
}

pub(crate) fn record_rate_limit_snapshot(
    host_pacer: Option<&SharedHostPacer>,
    host: &str,
    rate_limit: &RateLimitSnapshot,
    observed_at: SystemTime,
) {
    if let Some(pacer) = host_pacer
        && let Err(pacer_error) = HostPacer::record_rate_limit(pacer, host, rate_limit, observed_at)
    {
        eprintln!("failed to record host rate-limit snapshot for {host}: {pacer_error}");
    }
}
