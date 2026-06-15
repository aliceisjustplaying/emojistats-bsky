//! Crawler ledger state transitions for the v2 backfill.

use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::transport::AccountState;

/// Stable identifier for one fetch attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptId {
    pub run_id: String,
    pub did: String,
    pub sequence: u64,
}

impl AttemptId {
    #[must_use]
    pub fn new(run_id: impl Into<String>, did: impl Into<String>, sequence: u64) -> Self {
        Self {
            run_id: run_id.into(),
            did: did.into(),
            sequence,
        }
    }
}

/// Durable per-repo crawler state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepoLedgerStatus {
    Pending,
    Claimed,
    Succeeded,
    RetryableFailure,
    Throttled,
    ResourceLimited,
    TerminalAccount(AccountState),
    PermanentFailure,
}

/// One ledger row before persistence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoLedgerEntry {
    pub did: String,
    pub status: RepoLedgerStatus,
    pub attempts: u32,
    pub next_attempt_after: Option<SystemTime>,
    pub last_attempt: Option<AttemptId>,
    pub last_error: Option<String>,
}

impl RepoLedgerEntry {
    #[must_use]
    pub fn pending(did: impl Into<String>) -> Self {
        Self {
            did: did.into(),
            status: RepoLedgerStatus::Pending,
            attempts: 0,
            next_attempt_after: None,
            last_attempt: None,
            last_error: None,
        }
    }

    #[must_use]
    pub fn can_claim_at(&self, now: SystemTime) -> bool {
        match self.status {
            RepoLedgerStatus::Pending | RepoLedgerStatus::RetryableFailure => self
                .next_attempt_after
                .is_none_or(|next_attempt_after| next_attempt_after <= now),
            RepoLedgerStatus::Throttled => self
                .next_attempt_after
                .is_some_and(|next_attempt_after| next_attempt_after <= now),
            RepoLedgerStatus::Claimed
            | RepoLedgerStatus::Succeeded
            | RepoLedgerStatus::ResourceLimited
            | RepoLedgerStatus::TerminalAccount(_)
            | RepoLedgerStatus::PermanentFailure => false,
        }
    }
}

/// Retry policy for transient fetch/parse/archive failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_secs(60),
            max_delay: Duration::from_secs(3_600),
        }
    }
}

/// Result class emitted by one repo attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptOutcome {
    Succeeded,
    AccountState(AccountState),
    RateLimited { retry_after: Duration },
    RetryableFailure { message: String },
    ResourceLimitExceeded { message: String },
    PermanentFailure { message: String },
}

/// Mark a pending repo as claimed by a concrete attempt.
///
/// # Errors
///
/// Returns [`LedgerError`] if the repo is not claimable at `now`.
pub fn claim_repo(
    entry: &RepoLedgerEntry,
    attempt: AttemptId,
    now: SystemTime,
) -> Result<RepoLedgerEntry, LedgerError> {
    if !entry.can_claim_at(now) {
        return Err(LedgerError::NotClaimable {
            did: entry.did.clone(),
            status: entry.status.clone(),
        });
    }
    let mut claimed = entry.clone();
    claimed.status = RepoLedgerStatus::Claimed;
    claimed.attempts = claimed
        .attempts
        .checked_add(1)
        .ok_or(LedgerError::AttemptOverflow)?;
    claimed.last_attempt = Some(attempt);
    claimed.last_error = None;
    claimed.next_attempt_after = None;
    Ok(claimed)
}

/// Apply an attempt outcome to a claimed repo ledger entry.
///
/// # Errors
///
/// Returns [`LedgerError`] if the entry is not currently claimed or counters overflow.
pub fn complete_attempt(
    entry: &RepoLedgerEntry,
    outcome: AttemptOutcome,
    now: SystemTime,
    policy: RetryPolicy,
) -> Result<RepoLedgerEntry, LedgerError> {
    if entry.status != RepoLedgerStatus::Claimed {
        return Err(LedgerError::NotClaimed {
            did: entry.did.clone(),
            status: entry.status.clone(),
        });
    }

    let mut next = entry.clone();
    match outcome {
        AttemptOutcome::Succeeded => {
            next.status = RepoLedgerStatus::Succeeded;
            next.next_attempt_after = None;
            next.last_error = None;
        }
        AttemptOutcome::AccountState(state) => {
            next.status = RepoLedgerStatus::TerminalAccount(state);
            next.next_attempt_after = None;
            next.last_error = Some(state.to_string());
        }
        AttemptOutcome::RateLimited { retry_after } => {
            next.status = RepoLedgerStatus::Throttled;
            next.next_attempt_after = Some(add_duration(now, retry_after)?);
            next.last_error = Some("rate_limited".to_owned());
        }
        AttemptOutcome::RetryableFailure { message } => {
            if next.attempts >= policy.max_attempts {
                next.status = RepoLedgerStatus::PermanentFailure;
                next.next_attempt_after = None;
            } else {
                next.status = RepoLedgerStatus::RetryableFailure;
                next.next_attempt_after =
                    Some(add_duration(now, retry_delay(next.attempts, policy)?)?);
            }
            next.last_error = Some(message);
        }
        AttemptOutcome::ResourceLimitExceeded { message } => {
            next.status = RepoLedgerStatus::ResourceLimited;
            next.next_attempt_after = None;
            next.last_error = Some(message);
        }
        AttemptOutcome::PermanentFailure { message } => {
            next.status = RepoLedgerStatus::PermanentFailure;
            next.next_attempt_after = None;
            next.last_error = Some(message);
        }
    }
    Ok(next)
}

fn retry_delay(attempts: u32, policy: RetryPolicy) -> Result<Duration, LedgerError> {
    let exponent = attempts.saturating_sub(1).min(31);
    let multiplier = 1_u64
        .checked_shl(exponent)
        .ok_or(LedgerError::AttemptOverflow)?;
    let delay = policy
        .base_delay
        .checked_mul(u32::try_from(multiplier).map_err(|_err| LedgerError::AttemptOverflow)?)
        .ok_or(LedgerError::AttemptOverflow)?;
    Ok(delay.min(policy.max_delay))
}

fn add_duration(now: SystemTime, delay: Duration) -> Result<SystemTime, LedgerError> {
    now.checked_add(delay).ok_or(LedgerError::TimeOverflow)
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LedgerError {
    #[error("repo {did} is not claimable in status {status:?}")]
    NotClaimable {
        did: String,
        status: RepoLedgerStatus,
    },
    #[error("repo {did} is not claimed in status {status:?}")]
    NotClaimed {
        did: String,
        status: RepoLedgerStatus,
    },
    #[error("ledger attempt counter overflow")]
    AttemptOverflow,
    #[error("ledger time overflow")]
    TimeOverflow,
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use crate::{
        ledger::{
            AttemptId, AttemptOutcome, LedgerError, RepoLedgerEntry, RepoLedgerStatus, RetryPolicy,
            claim_repo, complete_attempt,
        },
        transport::AccountState,
    };

    #[test]
    fn pending_repo_claims_with_attempt_identity() {
        let now = UNIX_EPOCH;
        let entry = RepoLedgerEntry::pending("did:plc:abc");
        let attempt = AttemptId::new("run-1", "did:plc:abc", 7);

        let claimed = claim_repo(&entry, attempt.clone(), now).unwrap();

        assert_eq!(claimed.status, RepoLedgerStatus::Claimed);
        assert_eq!(claimed.attempts, 1);
        assert_eq!(claimed.last_attempt, Some(attempt));
    }

    #[test]
    fn account_state_is_terminal() {
        let now = UNIX_EPOCH;
        let entry = claim_repo(
            &RepoLedgerEntry::pending("did:plc:abc"),
            AttemptId::new("run-1", "did:plc:abc", 1),
            now,
        )
        .unwrap();

        let completed = complete_attempt(
            &entry,
            AttemptOutcome::AccountState(AccountState::RepoTakendown),
            now,
            RetryPolicy::default(),
        )
        .unwrap();

        assert_eq!(
            completed.status,
            RepoLedgerStatus::TerminalAccount(AccountState::RepoTakendown)
        );
        assert!(!completed.can_claim_at(now + Duration::from_secs(10_000)));
    }

    #[test]
    fn retryable_failure_backs_off_until_due() {
        let now = UNIX_EPOCH;
        let claimed = claim_repo(
            &RepoLedgerEntry::pending("did:plc:abc"),
            AttemptId::new("run-1", "did:plc:abc", 1),
            now,
        )
        .unwrap();

        let failed = complete_attempt(
            &claimed,
            AttemptOutcome::RetryableFailure {
                message: "socket reset".to_owned(),
            },
            now,
            RetryPolicy {
                max_attempts: 3,
                base_delay: Duration::from_secs(60),
                max_delay: Duration::from_secs(300),
            },
        )
        .unwrap();

        assert_eq!(failed.status, RepoLedgerStatus::RetryableFailure);
        assert!(!failed.can_claim_at(now + Duration::from_secs(59)));
        assert!(failed.can_claim_at(now + Duration::from_secs(60)));
    }

    #[test]
    fn retryable_failure_becomes_permanent_at_attempt_cap() {
        let now = UNIX_EPOCH;
        let first = claim_repo(
            &RepoLedgerEntry::pending("did:plc:abc"),
            AttemptId::new("run-1", "did:plc:abc", 1),
            now,
        )
        .unwrap();
        let failed = complete_attempt(
            &first,
            AttemptOutcome::RetryableFailure {
                message: "timeout".to_owned(),
            },
            now,
            RetryPolicy {
                max_attempts: 1,
                base_delay: Duration::from_secs(1),
                max_delay: Duration::from_secs(1),
            },
        )
        .unwrap();

        assert_eq!(failed.status, RepoLedgerStatus::PermanentFailure);
        assert!(!failed.can_claim_at(now + Duration::from_secs(1)));
    }

    #[test]
    fn unready_throttled_repo_is_not_claimable() {
        let now = UNIX_EPOCH;
        let claimed = claim_repo(
            &RepoLedgerEntry::pending("did:plc:abc"),
            AttemptId::new("run-1", "did:plc:abc", 1),
            now,
        )
        .unwrap();
        let throttled = complete_attempt(
            &claimed,
            AttemptOutcome::RateLimited {
                retry_after: Duration::from_secs(30),
            },
            now,
            RetryPolicy::default(),
        )
        .unwrap();

        let error = claim_repo(
            &throttled,
            AttemptId::new("run-1", "did:plc:abc", 2),
            now + Duration::from_secs(29),
        )
        .unwrap_err();

        assert!(matches!(error, LedgerError::NotClaimable { .. }));
    }
}
