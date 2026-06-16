//! Small scheduling primitives for fleet backfill runs.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{ledger::ShardFilter, transport::RateLimitSnapshot};

const LOW_REMAINING_RATIO_DENOMINATOR: u64 = 10;

/// Shared per-host pacing state used by concurrent fleet attempts.
pub type SharedHostPacer = Arc<Mutex<HostPacer>>;

/// Host cooldown table fed by retry-after outcomes and rate-limit window headers.
#[derive(Debug, Default)]
pub struct HostPacer {
    cooldowns: HashMap<String, Instant>,
}

impl HostPacer {
    #[must_use]
    pub fn shared() -> SharedHostPacer {
        Arc::new(Mutex::new(Self::default()))
    }

    /// Wait until `host` is outside its current cooldown window.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::PacerPoisoned`] if another holder panicked while
    /// mutating the cooldown table.
    pub async fn wait_until_ready(
        shared: &SharedHostPacer,
        host: &str,
    ) -> Result<(), SchedulerError> {
        loop {
            let delay = {
                let guard = shared
                    .lock()
                    .map_err(|_err| SchedulerError::PacerPoisoned)?;
                guard.ready_delay(host, Instant::now())
            };
            match delay {
                Some(delay) if !delay.is_zero() => tokio::time::sleep(delay).await,
                Some(_) | None => return Ok(()),
            }
        }
    }

    /// Record a host-level retry-after cooldown.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::PacerPoisoned`] if another holder panicked while
    /// mutating the cooldown table.
    pub fn record_retry_after(
        shared: &SharedHostPacer,
        host: &str,
        retry_after: Duration,
    ) -> Result<(), SchedulerError> {
        shared
            .lock()
            .map_err(|_err| SchedulerError::PacerPoisoned)?
            .apply_retry_after(host, retry_after, Instant::now());
        Ok(())
    }

    /// Record a host-level cooldown implied by successful rate-limit headers.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::PacerPoisoned`] if another holder panicked while
    /// mutating the cooldown table.
    pub fn record_rate_limit(
        shared: &SharedHostPacer,
        host: &str,
        rate_limit: &RateLimitSnapshot,
        observed_at: SystemTime,
    ) -> Result<(), SchedulerError> {
        let Some(delay) = Self::rate_limit_delay(rate_limit, observed_at) else {
            return Ok(());
        };
        shared
            .lock()
            .map_err(|_err| SchedulerError::PacerPoisoned)?
            .apply_retry_after(host, delay, Instant::now());
        Ok(())
    }

    #[must_use]
    pub fn ready_delay(&self, host: &str, now: Instant) -> Option<Duration> {
        self.cooldowns
            .get(host)
            .and_then(|deadline| deadline.checked_duration_since(now))
    }

    pub fn apply_retry_after(&mut self, host: &str, retry_after: Duration, now: Instant) {
        let Some(deadline) = now.checked_add(retry_after) else {
            return;
        };
        self.cooldowns
            .entry(host.to_owned())
            .and_modify(|existing| {
                if *existing < deadline {
                    *existing = deadline;
                }
            })
            .or_insert(deadline);
    }

    #[must_use]
    pub fn rate_limit_delay(
        rate_limit: &RateLimitSnapshot,
        observed_at: SystemTime,
    ) -> Option<Duration> {
        if let Some(retry_after) = rate_limit.retry_after {
            return Some(retry_after);
        }
        let reset = reset_delay(rate_limit.reset?, observed_at)?;
        match rate_limit.remaining {
            Some(0) => Some(reset),
            Some(remaining) if is_low_remaining(rate_limit.limit, remaining) => reset
                .checked_div(u32::try_from(remaining.saturating_add(1)).unwrap_or(u32::MAX))
                .filter(|delay| !delay.is_zero()),
            _ => None,
        }
    }
}

fn is_low_remaining(limit: Option<u64>, remaining: u64) -> bool {
    let Some(limit) = limit else {
        return false;
    };
    if limit == 0 {
        return false;
    }
    let low_watermark = limit
        .checked_div(LOW_REMAINING_RATIO_DENOMINATOR)
        .unwrap_or(0)
        .max(1);
    remaining <= low_watermark
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

/// Claim-scope hooks kept explicit while the Rust fleet runner grows host and
/// shard-aware claiming.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClaimScope {
    pub shard_filter: Option<ShardFilter>,
}

impl ClaimScope {
    #[must_use]
    pub const fn shard_filter(&self) -> Option<ShardFilter> {
        self.shard_filter
    }

    #[must_use]
    pub fn includes_did(&self, did: &str) -> bool {
        self.shard_filter
            .is_none_or(|shard_filter| shard_filter.contains_did(did))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SchedulerError {
    #[error("host pacer mutex poisoned")]
    PacerPoisoned,
    #[error("scheduler concurrency must be positive")]
    InvalidConcurrency,
    #[error("scheduler claim limit overflow")]
    ClaimLimitOverflow,
}

/// Validate a positive concurrency bound.
///
/// # Errors
///
/// Returns [`SchedulerError::InvalidConcurrency`] when `value` is zero.
pub const fn checked_concurrency(value: usize) -> Result<usize, SchedulerError> {
    if value == 0 {
        return Err(SchedulerError::InvalidConcurrency);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]

    use std::time::{Duration, Instant, UNIX_EPOCH};

    use crate::{
        scheduler::{ClaimScope, HostPacer, checked_concurrency},
        transport::RateLimitSnapshot,
    };

    #[test]
    fn retry_after_keeps_the_longest_host_cooldown() {
        let now = Instant::now();
        let mut pacer = HostPacer::default();

        pacer.apply_retry_after("pds.example", Duration::from_secs(10), now);
        pacer.apply_retry_after("pds.example", Duration::from_secs(3), now);

        assert_eq!(
            pacer.ready_delay("pds.example", now + Duration::from_secs(5)),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            pacer.ready_delay("pds.example", now + Duration::from_secs(10)),
            Some(Duration::ZERO)
        );
        assert_eq!(
            pacer.ready_delay("pds.example", now + Duration::from_secs(11)),
            None
        );
    }

    #[test]
    #[allow(clippy::duration_suboptimal_units)]
    fn rate_limit_delay_waits_until_reset_when_remaining_is_empty() {
        let observed_at = UNIX_EPOCH + Duration::from_secs(1_781_568_000);
        let snapshot = RateLimitSnapshot {
            limit: Some(100),
            remaining: Some(0),
            reset: Some(1_781_568_030),
            ..RateLimitSnapshot::default()
        };

        assert_eq!(
            HostPacer::rate_limit_delay(&snapshot, observed_at),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    #[allow(clippy::duration_suboptimal_units)]
    fn rate_limit_delay_spreads_low_remaining_across_reset_window() {
        let observed_at = UNIX_EPOCH + Duration::from_secs(1_781_568_000);
        let snapshot = RateLimitSnapshot {
            limit: Some(100),
            remaining: Some(4),
            reset: Some(1_781_568_100),
            ..RateLimitSnapshot::default()
        };

        assert_eq!(
            HostPacer::rate_limit_delay(&snapshot, observed_at),
            Some(Duration::from_secs(20))
        );
    }

    #[test]
    #[allow(clippy::duration_suboptimal_units)]
    fn rate_limit_delay_ignores_non_low_remaining() {
        let observed_at = UNIX_EPOCH + Duration::from_secs(1_781_568_000);
        let snapshot = RateLimitSnapshot {
            limit: Some(100),
            remaining: Some(11),
            reset: Some(1_781_568_100),
            ..RateLimitSnapshot::default()
        };

        assert_eq!(HostPacer::rate_limit_delay(&snapshot, observed_at), None);
    }

    #[test]
    fn claim_scope_includes_only_selected_shard_bucket() {
        let did = "did:plc:abc";
        let bucket = crate::ledger::did_shard_bucket(did);
        let mut other_did = "did:plc:other0".to_owned();
        let mut suffix = 1_u32;
        while crate::ledger::did_shard_bucket(&other_did) == bucket {
            other_did = format!("did:plc:other{suffix}");
            suffix = suffix.checked_add(1).unwrap();
        }
        let scope = ClaimScope {
            shard_filter: Some(crate::ledger::ShardFilter::new(bucket).unwrap()),
        };

        assert!(scope.includes_did(did));
        assert!(!scope.includes_did(&other_did));
    }

    #[test]
    fn concurrency_must_be_positive() {
        assert!(checked_concurrency(1).is_ok());
        assert!(checked_concurrency(0).is_err());
    }
}
