//! Small scheduling primitives for fleet backfill runs.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// Shared per-host pacing state used by concurrent fleet attempts.
pub type SharedHostPacer = Arc<Mutex<HostPacer>>;

/// Host cooldown table fed by retry-after outcomes.
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
}

/// Claim-scope hooks kept explicit while the Rust fleet runner grows host and
/// shard-aware claiming.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClaimScope {
    pub host_overrides: HashMap<String, String>,
    pub shard_filter: Option<String>,
}

impl ClaimScope {
    #[must_use]
    pub fn host_for(&self, did: &str, resolved_host: &str) -> String {
        self.host_overrides
            .get(did)
            .cloned()
            .unwrap_or_else(|| resolved_host.to_owned())
    }

    #[must_use]
    pub const fn includes_did(&self, _did: &str) -> bool {
        self.shard_filter.is_none()
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

    use std::time::{Duration, Instant};

    use crate::scheduler::{ClaimScope, HostPacer, checked_concurrency};

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
    fn claim_scope_applies_host_override() {
        let mut scope = ClaimScope::default();
        scope.host_overrides.insert(
            "did:plc:abc".to_owned(),
            "https://override.example".to_owned(),
        );

        assert_eq!(
            scope.host_for("did:plc:abc", "https://resolved.example"),
            "https://override.example"
        );
        assert_eq!(
            scope.host_for("did:plc:other", "https://resolved.example"),
            "https://resolved.example"
        );
    }

    #[test]
    fn concurrency_must_be_positive() {
        assert!(checked_concurrency(1).is_ok());
        assert!(checked_concurrency(0).is_err());
    }
}
