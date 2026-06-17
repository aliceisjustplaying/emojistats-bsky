//! Crawler ledger state transitions and SQLite persistence for the v2 backfill.

use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::transport::AccountState;

/// Pinned v2 shard bucket count for persisted repo ledger rows.
pub const DID_SHARD_BUCKET_MODULUS: u64 = 8;
/// Default amount of time a fleet worker owns a claimed repo before recovery may requeue it.
#[allow(clippy::duration_suboptimal_units)]
pub const DEFAULT_CLAIM_LEASE_DURATION: Duration = Duration::from_secs(1_800);
const SHARD_BUCKET_MIGRATION_BATCH_SIZE: i64 = 1_000;

/// Return the stable persisted shard bucket for a DID.
#[must_use]
pub fn did_shard_bucket(did: &str) -> u64 {
    let digest = Sha256::digest(did.as_bytes());
    let mut bytes = [0_u8; 8];
    for (destination, source) in bytes.iter_mut().zip(digest) {
        *destination = source;
    }
    u64::from_be_bytes(bytes).wrapping_rem(DID_SHARD_BUCKET_MODULUS)
}

/// A single shard bucket visible to one crawler worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardFilter {
    bucket: u64,
}

impl ShardFilter {
    /// Build a shard filter for one persisted bucket.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] when `bucket` is outside the pinned bucket modulus.
    pub const fn new(bucket: u64) -> Result<Self, LedgerError> {
        if bucket >= DID_SHARD_BUCKET_MODULUS {
            return Err(LedgerError::InvalidShardBucket {
                bucket,
                modulus: DID_SHARD_BUCKET_MODULUS,
            });
        }
        Ok(Self { bucket })
    }

    /// Return the persisted bucket selected by this filter.
    #[must_use]
    pub const fn bucket(self) -> u64 {
        self.bucket
    }

    /// Return whether `did` belongs to this shard filter.
    #[must_use]
    pub fn contains_did(self, did: &str) -> bool {
        did_shard_bucket(did) == self.bucket
    }
}

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
    OperatorDeferred,
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
    pub worker_id: Option<String>,
    pub claimed_at: Option<SystemTime>,
    pub lease_until: Option<SystemTime>,
}

/// Insert-or-ignore result for a bounded seed batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LedgerSeedBatchSummary {
    pub inserted: u64,
    pub existing: u64,
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
            worker_id: None,
            claimed_at: None,
            lease_until: None,
        }
    }

    #[must_use]
    pub fn can_claim_at(&self, now: SystemTime) -> bool {
        match self.status {
            RepoLedgerStatus::Pending | RepoLedgerStatus::RetryableFailure => self
                .next_attempt_after
                .is_none_or(|next_attempt_after| next_attempt_after <= now),
            RepoLedgerStatus::Throttled | RepoLedgerStatus::OperatorDeferred => self
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
    RateLimited {
        retry_after: Duration,
    },
    OperatorDeferred {
        retry_after: Option<Duration>,
        message: String,
    },
    RetryableFailure {
        message: String,
    },
    ResourceLimitExceeded {
        message: String,
    },
    PermanentFailure {
        message: String,
    },
}

/// Forced fetch path for a host override.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForcedFetchMode {
    GetRepo,
    ListRecords,
}

/// Operator-controlled host override persisted in the ledger database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostOverride {
    pub host: String,
    pub disabled: bool,
    pub concurrency_cap: Option<u32>,
    pub min_interval: Option<Duration>,
    pub revive_after: Option<SystemTime>,
    pub force_mode: Option<ForcedFetchMode>,
    pub force_mode_revive_after: Option<SystemTime>,
    pub never_diff: bool,
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
    claim_repo_transition(entry, attempt, now, None, None)
}

/// Mark a claimable repo as claimed by a concrete worker until a lease deadline.
///
/// # Errors
///
/// Returns [`LedgerError`] if the repo is not claimable at `now`, the worker id is invalid,
/// or `lease_duration` overflows.
pub fn claim_repo_with_lease(
    entry: &RepoLedgerEntry,
    attempt: AttemptId,
    now: SystemTime,
    worker_id: &str,
    lease_duration: Duration,
) -> Result<RepoLedgerEntry, LedgerError> {
    validate_worker_id(worker_id)?;
    let lease_until = add_duration(now, lease_duration)?;
    claim_repo_transition(
        entry,
        attempt,
        now,
        Some(worker_id.to_owned()),
        Some(lease_until),
    )
}

fn claim_repo_transition(
    entry: &RepoLedgerEntry,
    attempt: AttemptId,
    now: SystemTime,
    worker_id: Option<String>,
    lease_until: Option<SystemTime>,
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
    claimed.worker_id = worker_id;
    claimed.claimed_at = lease_until.map(|_| now);
    claimed.lease_until = lease_until;
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
    next.worker_id = None;
    next.claimed_at = None;
    next.lease_until = None;
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
        AttemptOutcome::OperatorDeferred {
            retry_after,
            message,
        } => {
            next.status = RepoLedgerStatus::OperatorDeferred;
            next.attempts = next
                .attempts
                .checked_sub(1)
                .ok_or(LedgerError::AttemptOverflow)?;
            next.next_attempt_after = retry_after
                .map(|delay| add_duration(now, delay))
                .transpose()?;
            next.last_error = Some(message);
        }
        AttemptOutcome::RetryableFailure { message } => {
            if next.attempts >= policy.max_attempts {
                next.status = RepoLedgerStatus::PermanentFailure;
                next.next_attempt_after = None;
            } else {
                next.status = RepoLedgerStatus::RetryableFailure;
                next.next_attempt_after = Some(add_duration(
                    now,
                    retry_delay(&next.did, next.attempts, policy)?,
                )?);
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

mod codec;
mod store;

pub use store::SqliteLedger;

fn retry_delay(did: &str, attempts: u32, policy: RetryPolicy) -> Result<Duration, LedgerError> {
    let exponent = attempts.saturating_sub(1).min(31);
    let multiplier = 1_u64
        .checked_shl(exponent)
        .ok_or(LedgerError::AttemptOverflow)?;
    let delay = policy
        .base_delay
        .checked_mul(u32::try_from(multiplier).map_err(|_err| LedgerError::AttemptOverflow)?)
        .ok_or(LedgerError::AttemptOverflow)?;
    let delay = delay.min(policy.max_delay);
    let jitter = retry_jitter(did, attempts, policy.base_delay)?;
    Ok(delay
        .checked_add(jitter)
        .ok_or(LedgerError::TimeOverflow)?
        .min(policy.max_delay))
}

fn retry_jitter(did: &str, attempts: u32, base_delay: Duration) -> Result<Duration, LedgerError> {
    let window_millis =
        u64::try_from(base_delay.as_millis() / 4).map_err(|_err| LedgerError::AttemptOverflow)?;
    if window_millis == 0 {
        return Ok(Duration::ZERO);
    }
    let modulus = window_millis
        .checked_add(1)
        .ok_or(LedgerError::AttemptOverflow)?;
    let mut hasher = Sha256::new();
    hasher.update(did.as_bytes());
    hasher.update(attempts.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    for (destination, source) in bytes.iter_mut().zip(digest) {
        *destination = source;
    }
    Ok(Duration::from_millis(
        u64::from_be_bytes(bytes)
            .checked_rem(modulus)
            .ok_or(LedgerError::AttemptOverflow)?,
    ))
}

fn add_duration(now: SystemTime, delay: Duration) -> Result<SystemTime, LedgerError> {
    now.checked_add(delay).ok_or(LedgerError::TimeOverflow)
}

fn validate_worker_id(worker_id: &str) -> Result<(), LedgerError> {
    if worker_id.trim().is_empty() {
        return Err(LedgerError::InvalidWorkerId);
    }
    Ok(())
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
    #[error("invalid shard bucket {bucket}; expected 0 <= bucket < {modulus}")]
    InvalidShardBucket { bucket: u64, modulus: u64 },
    #[error("ledger worker id must not be blank")]
    InvalidWorkerId,
}

#[derive(Debug, thiserror::Error)]
pub enum LedgerStoreError {
    #[error("sqlite ledger error")]
    Sqlite(#[from] rusqlite::Error),
    #[error("sqlite ledger migration error")]
    Migration(#[from] rusqlite_migration::Error),
    #[error("ledger time is before unix epoch")]
    TimeBeforeUnixEpoch,
    #[error("ledger integer overflow")]
    IntegerOverflow,
    #[error("unknown ledger status {status}")]
    UnknownStatus { status: String },
    #[error("invalid terminal account state {state}")]
    InvalidTerminalAccountState { state: String },
    #[error("ledger row has inconsistent attempt identity")]
    InconsistentAttemptIdentity,
    #[error("ledger row has inconsistent terminal status")]
    InconsistentTerminalStatus,
    #[error("invalid host override: {message}")]
    InvalidHostOverride { message: String },
    #[error("invalid host override disabled value {value}")]
    InvalidHostOverrideDisabled { value: i64 },
    #[error("invalid forced fetch mode {mode}")]
    InvalidForcedFetchMode { mode: String },
    #[error("ledger state transition error")]
    Ledger(#[from] LedgerError),
    #[error("claimed entry disappeared during claim transaction")]
    MissingClaimedEntry,
}

#[cfg(test)]
mod tests;
