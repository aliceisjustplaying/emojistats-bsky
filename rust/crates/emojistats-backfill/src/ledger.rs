//! Crawler ledger state transitions and SQLite persistence for the v2 backfill.

use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, params};
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

/// SQLite-backed store for durable per-repo crawler state.
pub struct SqliteLedger {
    connection: Connection,
}

impl SqliteLedger {
    /// Open a SQLite ledger and create its schema when it does not exist.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite cannot open the database or create the schema.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LedgerStoreError> {
        Self::from_connection(Connection::open(path)?)
    }

    /// Open an in-memory SQLite ledger.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite cannot create the in-memory database or schema.
    pub fn open_in_memory() -> Result<Self, LedgerStoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    /// Wrap an existing SQLite connection and create the ledger schema.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when schema creation fails.
    pub fn from_connection(connection: Connection) -> Result<Self, LedgerStoreError> {
        let ledger = Self { connection };
        ledger.create_schema()?;
        Ok(ledger)
    }

    /// Create the ledger schema if it does not already exist.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite rejects the schema statement.
    pub fn create_schema(&self) -> Result<(), LedgerStoreError> {
        self.connection.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS repo_ledger (
                did TEXT PRIMARY KEY NOT NULL,
                status TEXT NOT NULL,
                terminal_account_state TEXT,
                attempts INTEGER NOT NULL CHECK (attempts >= 0),
                next_attempt_after_ms INTEGER,
                last_attempt_run_id TEXT,
                last_attempt_did TEXT,
                last_attempt_sequence INTEGER,
                last_error TEXT,
                CHECK (
                    (status = 'terminal_account' AND terminal_account_state IS NOT NULL)
                    OR (status <> 'terminal_account' AND terminal_account_state IS NULL)
                ),
                CHECK (
                    (last_attempt_run_id IS NULL AND last_attempt_did IS NULL AND last_attempt_sequence IS NULL)
                    OR (last_attempt_run_id IS NOT NULL AND last_attempt_did IS NOT NULL AND last_attempt_sequence IS NOT NULL)
                )
            );
            ",
        )?;
        Ok(())
    }

    /// Insert or replace one ledger entry by DID.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when the entry cannot be encoded or persisted.
    pub fn upsert_entry(&self, entry: &RepoLedgerEntry) -> Result<(), LedgerStoreError> {
        let status = StoredStatus::from(&entry.status);
        let next_attempt_after_ms = optional_time_to_millis(entry.next_attempt_after)?;
        let last_attempt_sequence = entry
            .last_attempt
            .as_ref()
            .map(|attempt| i64::try_from(attempt.sequence))
            .transpose()
            .map_err(|_err| LedgerStoreError::IntegerOverflow)?;
        self.connection.execute(
            "
            INSERT INTO repo_ledger (
                did,
                status,
                terminal_account_state,
                attempts,
                next_attempt_after_ms,
                last_attempt_run_id,
                last_attempt_did,
                last_attempt_sequence,
                last_error
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(did) DO UPDATE SET
                status = excluded.status,
                terminal_account_state = excluded.terminal_account_state,
                attempts = excluded.attempts,
                next_attempt_after_ms = excluded.next_attempt_after_ms,
                last_attempt_run_id = excluded.last_attempt_run_id,
                last_attempt_did = excluded.last_attempt_did,
                last_attempt_sequence = excluded.last_attempt_sequence,
                last_error = excluded.last_error
            ",
            params![
                entry.did.as_str(),
                status.status,
                status.terminal_account_state,
                i64::from(entry.attempts),
                next_attempt_after_ms,
                entry
                    .last_attempt
                    .as_ref()
                    .map(|attempt| attempt.run_id.as_str()),
                entry
                    .last_attempt
                    .as_ref()
                    .map(|attempt| attempt.did.as_str()),
                last_attempt_sequence,
                entry.last_error.as_deref(),
            ],
        )?;
        Ok(())
    }

    /// Persist an entry returned by a state transition.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when the entry cannot be encoded or persisted.
    pub fn save_transitioned_entry(&self, entry: &RepoLedgerEntry) -> Result<(), LedgerStoreError> {
        self.upsert_entry(entry)
    }

    /// Load one ledger entry by DID.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite fails or a stored row cannot be decoded.
    pub fn load_entry(&self, did: &str) -> Result<Option<RepoLedgerEntry>, LedgerStoreError> {
        self.connection
            .query_row(
                "
                SELECT
                    did,
                    status,
                    terminal_account_state,
                    attempts,
                    next_attempt_after_ms,
                    last_attempt_run_id,
                    last_attempt_did,
                    last_attempt_sequence,
                    last_error
                FROM repo_ledger
                WHERE did = ?1
                ",
                params![did],
                row_to_entry,
            )
            .optional()
            .map_err(Into::into)
            .and_then(Option::transpose)
    }

    /// Return entries that can be claimed at `now`, ordered by retry time and DID.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite fails or a stored row cannot be decoded.
    pub fn claimable_entries(
        &self,
        now: SystemTime,
        limit: u32,
    ) -> Result<Vec<RepoLedgerEntry>, LedgerStoreError> {
        let now_ms = time_to_millis(now)?;
        let limit = i64::from(limit);
        let mut statement = self.connection.prepare(
            "
            SELECT
                did,
                status,
                terminal_account_state,
                attempts,
                next_attempt_after_ms,
                last_attempt_run_id,
                last_attempt_did,
                last_attempt_sequence,
                last_error
            FROM repo_ledger
            WHERE
                (
                    status IN ('pending', 'retryable_failure')
                    AND (next_attempt_after_ms IS NULL OR next_attempt_after_ms <= ?1)
                )
                OR (
                    status = 'throttled'
                    AND next_attempt_after_ms IS NOT NULL
                    AND next_attempt_after_ms <= ?1
                )
            ORDER BY COALESCE(next_attempt_after_ms, 0), did
            LIMIT ?2
            ",
        )?;
        let rows = statement.query_map(params![now_ms, limit], row_to_entry)?;
        let entries = rows.collect::<Result<Vec<_>, _>>()?;
        entries.into_iter().collect()
    }
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

#[derive(Debug, thiserror::Error)]
pub enum LedgerStoreError {
    #[error("sqlite ledger error")]
    Sqlite(#[from] rusqlite::Error),
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
}

struct StoredStatus {
    status: &'static str,
    terminal_account_state: Option<&'static str>,
}

impl From<&RepoLedgerStatus> for StoredStatus {
    fn from(status: &RepoLedgerStatus) -> Self {
        match status {
            RepoLedgerStatus::Pending => Self {
                status: "pending",
                terminal_account_state: None,
            },
            RepoLedgerStatus::Claimed => Self {
                status: "claimed",
                terminal_account_state: None,
            },
            RepoLedgerStatus::Succeeded => Self {
                status: "succeeded",
                terminal_account_state: None,
            },
            RepoLedgerStatus::RetryableFailure => Self {
                status: "retryable_failure",
                terminal_account_state: None,
            },
            RepoLedgerStatus::Throttled => Self {
                status: "throttled",
                terminal_account_state: None,
            },
            RepoLedgerStatus::ResourceLimited => Self {
                status: "resource_limited",
                terminal_account_state: None,
            },
            RepoLedgerStatus::TerminalAccount(state) => Self {
                status: "terminal_account",
                terminal_account_state: Some(account_state_name(*state)),
            },
            RepoLedgerStatus::PermanentFailure => Self {
                status: "permanent_failure",
                terminal_account_state: None,
            },
        }
    }
}

fn row_to_entry(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<RepoLedgerEntry, LedgerStoreError>> {
    let status: String = row.get(1)?;
    let terminal_account_state: Option<String> = row.get(2)?;
    let attempts: i64 = row.get(3)?;
    let next_attempt_after_ms: Option<i64> = row.get(4)?;
    let last_attempt_run_id: Option<String> = row.get(5)?;
    let last_attempt_did: Option<String> = row.get(6)?;
    let last_attempt_sequence: Option<i64> = row.get(7)?;

    Ok(build_entry(
        row.get(0)?,
        &status,
        terminal_account_state.as_deref(),
        attempts,
        next_attempt_after_ms,
        last_attempt_run_id,
        last_attempt_did,
        last_attempt_sequence,
        row.get(8)?,
    ))
}

#[allow(clippy::too_many_arguments)]
fn build_entry(
    did: String,
    status: &str,
    terminal_account_state: Option<&str>,
    attempts: i64,
    next_attempt_after_ms: Option<i64>,
    last_attempt_run_id: Option<String>,
    last_attempt_did: Option<String>,
    last_attempt_sequence: Option<i64>,
    last_error: Option<String>,
) -> Result<RepoLedgerEntry, LedgerStoreError> {
    let status = parse_status(status, terminal_account_state)?;
    let attempts = u32::try_from(attempts).map_err(|_err| LedgerStoreError::IntegerOverflow)?;
    let next_attempt_after = next_attempt_after_ms.map(time_from_millis).transpose()?;
    let last_attempt = match (last_attempt_run_id, last_attempt_did, last_attempt_sequence) {
        (None, None, None) => None,
        (Some(run_id), Some(did), Some(sequence)) => Some(AttemptId {
            run_id,
            did,
            sequence: u64::try_from(sequence).map_err(|_err| LedgerStoreError::IntegerOverflow)?,
        }),
        _ => return Err(LedgerStoreError::InconsistentAttemptIdentity),
    };

    Ok(RepoLedgerEntry {
        did,
        status,
        attempts,
        next_attempt_after,
        last_attempt,
        last_error,
    })
}

fn parse_status(
    status: &str,
    terminal_account_state: Option<&str>,
) -> Result<RepoLedgerStatus, LedgerStoreError> {
    let parsed = match status {
        "pending" => RepoLedgerStatus::Pending,
        "claimed" => RepoLedgerStatus::Claimed,
        "succeeded" => RepoLedgerStatus::Succeeded,
        "retryable_failure" => RepoLedgerStatus::RetryableFailure,
        "throttled" => RepoLedgerStatus::Throttled,
        "resource_limited" => RepoLedgerStatus::ResourceLimited,
        "terminal_account" => {
            let state =
                terminal_account_state.ok_or(LedgerStoreError::InconsistentTerminalStatus)?;
            RepoLedgerStatus::TerminalAccount(parse_account_state(state)?)
        }
        "permanent_failure" => RepoLedgerStatus::PermanentFailure,
        _ => {
            return Err(LedgerStoreError::UnknownStatus {
                status: status.to_owned(),
            });
        }
    };
    if !matches!(parsed, RepoLedgerStatus::TerminalAccount(_)) && terminal_account_state.is_some() {
        return Err(LedgerStoreError::InconsistentTerminalStatus);
    }
    Ok(parsed)
}

fn parse_account_state(state: &str) -> Result<AccountState, LedgerStoreError> {
    match state {
        "RepoNotFound" => Ok(AccountState::RepoNotFound),
        "RepoTakendown" => Ok(AccountState::RepoTakendown),
        "RepoSuspended" => Ok(AccountState::RepoSuspended),
        "RepoDeactivated" => Ok(AccountState::RepoDeactivated),
        _ => Err(LedgerStoreError::InvalidTerminalAccountState {
            state: state.to_owned(),
        }),
    }
}

const fn account_state_name(state: AccountState) -> &'static str {
    match state {
        AccountState::RepoNotFound => "RepoNotFound",
        AccountState::RepoTakendown => "RepoTakendown",
        AccountState::RepoSuspended => "RepoSuspended",
        AccountState::RepoDeactivated => "RepoDeactivated",
    }
}

fn optional_time_to_millis(time: Option<SystemTime>) -> Result<Option<i64>, LedgerStoreError> {
    time.map(time_to_millis).transpose()
}

fn time_to_millis(time: SystemTime) -> Result<i64, LedgerStoreError> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_err| LedgerStoreError::TimeBeforeUnixEpoch)?;
    let millis = duration
        .as_secs()
        .checked_mul(1_000)
        .and_then(|seconds| seconds.checked_add(u64::from(duration.subsec_millis())))
        .ok_or(LedgerStoreError::IntegerOverflow)?;
    i64::try_from(millis).map_err(|_err| LedgerStoreError::IntegerOverflow)
}

fn time_from_millis(millis: i64) -> Result<SystemTime, LedgerStoreError> {
    let millis = u64::try_from(millis).map_err(|_err| LedgerStoreError::IntegerOverflow)?;
    UNIX_EPOCH
        .checked_add(Duration::from_millis(millis))
        .ok_or(LedgerStoreError::IntegerOverflow)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use crate::{
        ledger::{
            AttemptId, AttemptOutcome, LedgerError, RepoLedgerEntry, RepoLedgerStatus, RetryPolicy,
            SqliteLedger, claim_repo, complete_attempt,
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

    #[test]
    fn sqlite_upsert_loads_full_entry() {
        let store = SqliteLedger::open_in_memory().unwrap();
        let entry = RepoLedgerEntry {
            did: "did:plc:abc".to_owned(),
            status: RepoLedgerStatus::TerminalAccount(AccountState::RepoSuspended),
            attempts: 2,
            next_attempt_after: Some(UNIX_EPOCH + Duration::from_secs(1_234)),
            last_attempt: Some(AttemptId::new("run-1", "did:plc:abc", 9)),
            last_error: Some("RepoSuspended".to_owned()),
        };

        store.upsert_entry(&entry).unwrap();

        assert_eq!(store.load_entry("did:plc:abc").unwrap(), Some(entry));
    }

    #[test]
    fn sqlite_claimable_entries_are_due_at_now() {
        let store = SqliteLedger::open_in_memory().unwrap();
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        let due = RepoLedgerEntry {
            did: "did:plc:due".to_owned(),
            status: RepoLedgerStatus::Throttled,
            attempts: 1,
            next_attempt_after: Some(now - Duration::from_secs(1)),
            last_attempt: Some(AttemptId::new("run-1", "did:plc:due", 1)),
            last_error: Some("rate_limited".to_owned()),
        };
        let pending = RepoLedgerEntry::pending("did:plc:pending");
        let future_retry = RepoLedgerEntry {
            did: "did:plc:future".to_owned(),
            status: RepoLedgerStatus::RetryableFailure,
            attempts: 1,
            next_attempt_after: Some(now + Duration::from_secs(1)),
            last_attempt: Some(AttemptId::new("run-1", "did:plc:future", 1)),
            last_error: Some("timeout".to_owned()),
        };
        let succeeded = RepoLedgerEntry {
            did: "did:plc:done".to_owned(),
            status: RepoLedgerStatus::Succeeded,
            attempts: 1,
            next_attempt_after: None,
            last_attempt: Some(AttemptId::new("run-1", "did:plc:done", 1)),
            last_error: None,
        };

        for entry in [&due, &pending, &future_retry, &succeeded] {
            store.upsert_entry(entry).unwrap();
        }

        let claimable = store.claimable_entries(now, 10).unwrap();
        let dids = claimable
            .iter()
            .map(|entry| entry.did.as_str())
            .collect::<Vec<_>>();

        assert_eq!(dids, vec!["did:plc:pending", "did:plc:due"]);
    }

    #[test]
    fn sqlite_saves_transitioned_entries() {
        let store = SqliteLedger::open_in_memory().unwrap();
        let now = UNIX_EPOCH;
        let pending = RepoLedgerEntry::pending("did:plc:abc");
        store.upsert_entry(&pending).unwrap();

        let claimed = claim_repo(&pending, AttemptId::new("run-1", "did:plc:abc", 1), now).unwrap();
        store.save_transitioned_entry(&claimed).unwrap();

        let completed = complete_attempt(
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
        store.save_transitioned_entry(&completed).unwrap();

        assert_eq!(store.load_entry("did:plc:abc").unwrap(), Some(completed));
    }
}
