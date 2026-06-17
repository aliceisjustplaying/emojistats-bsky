//! Small scheduling primitives for fleet backfill runs.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{ledger::ShardFilter, transport::RateLimitSnapshot};

/// Shared per-host pacing state used by concurrent fleet attempts.
pub type SharedHostPacer = Arc<Mutex<HostPacer>>;

/// Host pacing table fed by request reservations, retry-after outcomes, and
/// rate-limit window headers.
#[derive(Debug, Default)]
pub struct HostPacer {
    hosts: HashMap<String, HostPacingState>,
}

#[derive(Debug, Default)]
struct HostPacingState {
    limit: Option<u64>,
    remaining: Option<u64>,
    reset_at: Option<u64>,
    last_sent_at: Option<Instant>,
    next_send_at: Option<Instant>,
    retry_after_until: Option<Instant>,
    rate_limit_until: Option<Instant>,
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

    /// Serialize request admission for a host and reserve the next send time.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::PacerPoisoned`] if another holder panicked while
    /// mutating the pacing table.
    pub async fn reserve_next_request(
        shared: &SharedHostPacer,
        host: &str,
        min_interval: Option<Duration>,
    ) -> Result<(), SchedulerError> {
        loop {
            let delay = {
                let mut guard = shared
                    .lock()
                    .map_err(|_err| SchedulerError::PacerPoisoned)?;
                let now = Instant::now();
                match guard.ready_delay(host, now) {
                    Some(delay) if !delay.is_zero() => Some(delay),
                    Some(_) | None => {
                        guard.reserve_send_at(host, now, min_interval);
                        drop(guard);
                        return Ok(());
                    }
                }
            };
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
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
        shared
            .lock()
            .map_err(|_err| SchedulerError::PacerPoisoned)?
            .record_rate_limit_state(host, rate_limit, observed_at, Instant::now());
        Ok(())
    }

    #[must_use]
    pub fn ready_delay(&self, host: &str, now: Instant) -> Option<Duration> {
        self.hosts
            .get(host)
            .and_then(|state| state.ready_delay(now))
    }

    pub fn apply_retry_after(&mut self, host: &str, retry_after: Duration, now: Instant) {
        let Some(deadline) = now.checked_add(retry_after) else {
            return;
        };
        let state = self.hosts.entry(host.to_owned()).or_default();
        state.retry_after_until = Some(max_instant(state.retry_after_until, deadline));
    }

    fn reserve_send_at(&mut self, host: &str, now: Instant, min_interval: Option<Duration>) {
        let state = self.hosts.entry(host.to_owned()).or_default();
        state.last_sent_at = Some(now);
        if let Some(min_interval) = min_interval
            && let Some(deadline) = now.checked_add(min_interval)
        {
            state.next_send_at = Some(max_instant(state.next_send_at, deadline));
        }
    }

    fn record_rate_limit_state(
        &mut self,
        host: &str,
        rate_limit: &RateLimitSnapshot,
        observed_at: SystemTime,
        now: Instant,
    ) {
        let state = self.hosts.entry(host.to_owned()).or_default();
        state.limit = rate_limit.limit;
        state.remaining = rate_limit.remaining;
        state.reset_at = rate_limit.reset;
        if let Some(retry_after) = rate_limit.retry_after
            && let Some(deadline) = now.checked_add(retry_after)
        {
            state.retry_after_until = Some(max_instant(state.retry_after_until, deadline));
        }
        if let Some(delay) = Self::rate_limit_delay_without_retry_after(rate_limit, observed_at)
            && let Some(deadline) = now.checked_add(delay)
        {
            state.rate_limit_until = Some(max_instant(state.rate_limit_until, deadline));
        }
    }

    #[cfg(test)]
    fn host_state(&self, host: &str) -> Option<&HostPacingState> {
        self.hosts.get(host)
    }

    #[must_use]
    pub fn rate_limit_delay(
        rate_limit: &RateLimitSnapshot,
        observed_at: SystemTime,
    ) -> Option<Duration> {
        if let Some(retry_after) = rate_limit.retry_after {
            return Some(retry_after);
        }
        Self::rate_limit_delay_without_retry_after(rate_limit, observed_at)
    }

    fn rate_limit_delay_without_retry_after(
        rate_limit: &RateLimitSnapshot,
        observed_at: SystemTime,
    ) -> Option<Duration> {
        let reset = reset_delay(rate_limit.reset?, observed_at)?;
        match rate_limit.remaining {
            Some(0) => Some(reset),
            Some(remaining) => reset
                .checked_div(u32::try_from(remaining.saturating_add(1)).unwrap_or(u32::MAX))
                .filter(|delay| !delay.is_zero()),
            _ => None,
        }
    }
}

impl HostPacingState {
    fn ready_delay(&self, now: Instant) -> Option<Duration> {
        [
            self.next_send_at,
            self.retry_after_until,
            self.rate_limit_until,
        ]
        .into_iter()
        .flatten()
        .filter_map(|deadline| deadline.checked_duration_since(now))
        .max()
    }
}

fn max_instant(current: Option<Instant>, candidate: Instant) -> Instant {
    match current {
        Some(current) if current > candidate => current,
        _ => candidate,
    }
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
    fn rate_limit_headers_update_explicit_host_state() {
        let mut pacer = HostPacer::default();
        let observed_at = UNIX_EPOCH + Duration::from_secs(100);
        let snapshot = RateLimitSnapshot {
            limit: Some(300),
            remaining: Some(2),
            reset: Some(130),
            retry_after: None,
            policy: None,
        };

        pacer.record_rate_limit_state("pds.example", &snapshot, observed_at, Instant::now());

        let state = pacer
            .host_state("pds.example")
            .expect("host state should be recorded");
        assert_eq!(state.limit, Some(300));
        assert_eq!(state.remaining, Some(2));
        assert_eq!(state.reset_at, Some(130));
        assert!(state.rate_limit_until.is_some());
        assert!(state.retry_after_until.is_none());
    }

    #[tokio::test]
    async fn reserve_next_request_applies_min_interval_at_admission() {
        let pacer = HostPacer::shared();

        HostPacer::reserve_next_request(&pacer, "pds.example", Some(Duration::from_millis(20)))
            .await
            .unwrap();
        let started = Instant::now();
        HostPacer::reserve_next_request(&pacer, "pds.example", Some(Duration::from_millis(1)))
            .await
            .unwrap();

        assert!(started.elapsed() >= Duration::from_millis(15));
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
    fn rate_limit_delay_spreads_remaining_across_reset_window() {
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
    fn rate_limit_delay_spreads_high_remaining_across_reset_window() {
        let observed_at = UNIX_EPOCH + Duration::from_secs(1_781_568_000);
        let snapshot = RateLimitSnapshot {
            limit: Some(100),
            remaining: Some(11),
            reset: Some(1_781_568_100),
            ..RateLimitSnapshot::default()
        };

        assert_eq!(
            HostPacer::rate_limit_delay(&snapshot, observed_at),
            Some(Duration::new(8, 333_333_333))
        );
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
