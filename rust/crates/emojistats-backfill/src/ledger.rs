//! Crawler ledger state transitions and SQLite persistence for the v2 backfill.

use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::transport::AccountState;

/// Pinned v2 shard bucket count for persisted repo ledger rows.
pub const DID_SHARD_BUCKET_MODULUS: u64 = 8;
/// Default amount of time a fleet worker owns a claimed repo before recovery may requeue it.
pub const DEFAULT_CLAIM_LEASE_DURATION: Duration = Duration::from_hours(6);

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
    pub revive_after: Option<SystemTime>,
    pub force_mode: Option<ForcedFetchMode>,
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
    pub fn from_connection(mut connection: Connection) -> Result<Self, LedgerStoreError> {
        connection.set_transaction_behavior(TransactionBehavior::Immediate);
        let ledger = Self { connection };
        ledger.configure_connection()?;
        ledger.create_schema()?;
        Ok(ledger)
    }

    fn configure_connection(&self) -> Result<(), LedgerStoreError> {
        self.connection.busy_timeout(Duration::from_secs(30))?;
        self.connection.pragma_update(None, "journal_mode", "WAL")?;
        self.connection
            .pragma_update(None, "synchronous", "NORMAL")?;
        Ok(())
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
                shard_bucket INTEGER NOT NULL CHECK (shard_bucket >= 0),
                status TEXT NOT NULL,
                terminal_account_state TEXT,
                attempts INTEGER NOT NULL CHECK (attempts >= 0),
                next_attempt_after_ms INTEGER,
                last_attempt_run_id TEXT,
                last_attempt_did TEXT,
                last_attempt_sequence INTEGER,
                last_error TEXT,
                worker_id TEXT,
                claimed_at_ms INTEGER,
                lease_until_ms INTEGER,
                CHECK (
                    (status = 'terminal_account' AND terminal_account_state IS NOT NULL)
                    OR (status <> 'terminal_account' AND terminal_account_state IS NULL)
                ),
                CHECK (
                    (last_attempt_run_id IS NULL AND last_attempt_did IS NULL AND last_attempt_sequence IS NULL)
                    OR (last_attempt_run_id IS NOT NULL AND last_attempt_did IS NOT NULL AND last_attempt_sequence IS NOT NULL)
                )
            );

            CREATE TABLE IF NOT EXISTS host_overrides (
                host TEXT PRIMARY KEY NOT NULL,
                disabled INTEGER NOT NULL CHECK (disabled IN (0, 1)),
                concurrency_cap INTEGER CHECK (concurrency_cap IS NULL OR concurrency_cap > 0),
                revive_after_ms INTEGER,
                force_mode TEXT CHECK (
                    force_mode IS NULL OR force_mode IN ('get_repo', 'list_records')
                )
            );
            ",
        )?;
        self.ensure_repo_ledger_columns()?;
        self.connection.execute_batch(
            "
            CREATE INDEX IF NOT EXISTS idx_repo_ledger_claim
                ON repo_ledger (status, shard_bucket, did);
            CREATE INDEX IF NOT EXISTS idx_repo_ledger_retry
                ON repo_ledger (status, shard_bucket, next_attempt_after_ms);
            CREATE INDEX IF NOT EXISTS idx_repo_ledger_claim_v2
                ON repo_ledger (status, shard_bucket, next_attempt_after_ms, did);
            CREATE INDEX IF NOT EXISTS idx_repo_ledger_lease_v2
                ON repo_ledger (status, lease_until_ms);
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
        let shard_bucket = shard_bucket_to_i64(did_shard_bucket(&entry.did))?;
        let claimed_at_ms = optional_time_to_millis(entry.claimed_at)?;
        let lease_until_ms = optional_time_to_millis(entry.lease_until)?;
        self.connection.execute(
            "
            INSERT INTO repo_ledger (
                did,
                shard_bucket,
                status,
                terminal_account_state,
                attempts,
                next_attempt_after_ms,
                last_attempt_run_id,
                last_attempt_did,
                last_attempt_sequence,
                last_error,
                worker_id,
                claimed_at_ms,
                lease_until_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ON CONFLICT(did) DO UPDATE SET
                shard_bucket = excluded.shard_bucket,
                status = excluded.status,
                terminal_account_state = excluded.terminal_account_state,
                attempts = excluded.attempts,
                next_attempt_after_ms = excluded.next_attempt_after_ms,
                last_attempt_run_id = excluded.last_attempt_run_id,
                last_attempt_did = excluded.last_attempt_did,
                last_attempt_sequence = excluded.last_attempt_sequence,
                last_error = excluded.last_error,
                worker_id = excluded.worker_id,
                claimed_at_ms = excluded.claimed_at_ms,
                lease_until_ms = excluded.lease_until_ms
            ",
            params![
                entry.did.as_str(),
                shard_bucket,
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
                entry.worker_id.as_deref(),
                claimed_at_ms,
                lease_until_ms,
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

    /// Atomically claim the next due repo for one worker.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite fails, values cannot be encoded, or
    /// `worker_id` is invalid.
    pub fn try_claim_next(
        &self,
        now: SystemTime,
        run_id: &str,
        worker_id: &str,
        lease_duration: Duration,
        shard: Option<ShardFilter>,
    ) -> Result<Option<RepoLedgerEntry>, LedgerStoreError> {
        validate_worker_id(worker_id).map_err(LedgerStoreError::Ledger)?;
        let now_ms = time_to_millis(now)?;
        let lease_until_ms = time_to_millis(
            now.checked_add(lease_duration)
                .ok_or(LedgerStoreError::IntegerOverflow)?,
        )?;
        let shard_bucket = shard
            .map(|filter| shard_bucket_to_i64(filter.bucket()))
            .transpose()?;
        let transaction = self.connection.unchecked_transaction()?;
        let did = transaction
            .query_row(
                "
                SELECT did
                FROM repo_ledger
                WHERE
                    (
                        (
                            status IN ('pending', 'retryable_failure')
                            AND (next_attempt_after_ms IS NULL OR next_attempt_after_ms <= ?1)
                        )
                        OR (
                            status = 'throttled'
                            AND next_attempt_after_ms IS NOT NULL
                            AND next_attempt_after_ms <= ?1
                        )
                    )
                    AND (?2 IS NULL OR shard_bucket = ?2)
                ORDER BY COALESCE(next_attempt_after_ms, 0), did
                LIMIT 1
                ",
                params![now_ms, shard_bucket],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(did) = did else {
            transaction.commit()?;
            return Ok(None);
        };

        let changed = transaction.execute(
            "
            UPDATE repo_ledger
            SET
                status = 'claimed',
                terminal_account_state = NULL,
                attempts = attempts + 1,
                next_attempt_after_ms = NULL,
                last_attempt_run_id = ?2,
                last_attempt_did = did,
                last_attempt_sequence = attempts + 1,
                last_error = NULL,
                worker_id = ?3,
                claimed_at_ms = ?4,
                lease_until_ms = ?5
            WHERE
                did = ?1
                AND (
                    (
                        status IN ('pending', 'retryable_failure')
                        AND (next_attempt_after_ms IS NULL OR next_attempt_after_ms <= ?4)
                    )
                    OR (
                        status = 'throttled'
                        AND next_attempt_after_ms IS NOT NULL
                        AND next_attempt_after_ms <= ?4
                    )
                )
                AND (?6 IS NULL OR shard_bucket = ?6)
            ",
            params![
                did.as_str(),
                run_id,
                worker_id,
                now_ms,
                lease_until_ms,
                shard_bucket,
            ],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(None);
        }
        let entry = load_entry_in_transaction(&transaction, &did)?
            .ok_or(LedgerStoreError::MissingClaimedEntry)?;
        transaction.commit()?;
        Ok(Some(entry))
    }

    /// Complete a claimed repo only when the stored row still belongs to the same worker attempt.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite fails or the completed state cannot be encoded.
    pub fn complete_owned_claim(
        &self,
        claimed: &RepoLedgerEntry,
        outcome: AttemptOutcome,
        now: SystemTime,
        policy: RetryPolicy,
    ) -> Result<Option<RepoLedgerEntry>, LedgerStoreError> {
        let Some(worker_id) = claimed.worker_id.as_deref() else {
            return Ok(None);
        };
        let Some(attempt) = claimed.last_attempt.as_ref() else {
            return Ok(None);
        };
        let transaction = self.connection.unchecked_transaction()?;
        let Some(current) = load_entry_in_transaction(&transaction, &claimed.did)? else {
            transaction.commit()?;
            return Ok(None);
        };
        if current.status != RepoLedgerStatus::Claimed
            || current.worker_id.as_deref() != Some(worker_id)
            || current.last_attempt.as_ref() != Some(attempt)
        {
            transaction.commit()?;
            return Ok(None);
        }

        let completed =
            complete_attempt(&current, outcome, now, policy).map_err(LedgerStoreError::Ledger)?;
        let changed = update_entry_if_owned(&transaction, &completed, worker_id, attempt)?;
        transaction.commit()?;
        if changed == 0 {
            return Ok(None);
        }
        Ok(Some(completed))
    }

    /// Requeue a claimed repo only after its lease has expired.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite fails or the recovered state cannot be encoded.
    pub fn recover_expired_claim(
        &self,
        did: &str,
        now: SystemTime,
        message: &str,
    ) -> Result<Option<RepoLedgerEntry>, LedgerStoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let Some(current) = load_entry_in_transaction(&transaction, did)? else {
            transaction.commit()?;
            return Ok(None);
        };
        if current.status != RepoLedgerStatus::Claimed
            || current
                .lease_until
                .is_none_or(|lease_until| lease_until > now)
        {
            transaction.commit()?;
            return Ok(None);
        }
        let recovered = complete_attempt(
            &current,
            AttemptOutcome::RetryableFailure {
                message: message.to_owned(),
            },
            now,
            RetryPolicy {
                max_attempts: u32::MAX,
                base_delay: Duration::ZERO,
                max_delay: Duration::ZERO,
            },
        )
        .map_err(LedgerStoreError::Ledger)?;
        let changed = update_expired_claim(&transaction, &recovered, now)?;
        transaction.commit()?;
        if changed == 0 {
            return Ok(None);
        }
        Ok(Some(recovered))
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
                    last_error,
                    worker_id,
                    claimed_at_ms,
                    lease_until_ms
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
        self.claimable_entries_with_shard(now, limit, None)
    }

    /// Return claimable entries for one shard bucket.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite fails or a stored row cannot be decoded.
    pub fn claimable_entries_for_shard(
        &self,
        now: SystemTime,
        limit: u32,
        shard: ShardFilter,
    ) -> Result<Vec<RepoLedgerEntry>, LedgerStoreError> {
        self.claimable_entries_with_shard(now, limit, Some(shard))
    }

    /// Insert or replace a host override record by host.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when the record cannot be encoded or persisted.
    pub fn upsert_host_override(&self, record: &HostOverride) -> Result<(), LedgerStoreError> {
        validate_host_override(record)?;
        let concurrency_cap = record.concurrency_cap.map(i64::from);
        let revive_after_ms = optional_time_to_millis(record.revive_after)?;
        self.connection.execute(
            "
            INSERT INTO host_overrides (
                host,
                disabled,
                concurrency_cap,
                revive_after_ms,
                force_mode
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(host) DO UPDATE SET
                disabled = excluded.disabled,
                concurrency_cap = excluded.concurrency_cap,
                revive_after_ms = excluded.revive_after_ms,
                force_mode = excluded.force_mode
            ",
            params![
                record.host.as_str(),
                bool_to_i64(record.disabled),
                concurrency_cap,
                revive_after_ms,
                record.force_mode.map(force_mode_name),
            ],
        )?;
        Ok(())
    }

    /// Load one host override by host.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite fails or a stored row cannot be decoded.
    pub fn load_host_override(&self, host: &str) -> Result<Option<HostOverride>, LedgerStoreError> {
        self.connection
            .query_row(
                "
                SELECT host, disabled, concurrency_cap, revive_after_ms, force_mode
                FROM host_overrides
                WHERE host = ?1
                ",
                params![host],
                row_to_host_override,
            )
            .optional()
            .map_err(Into::into)
            .and_then(Option::transpose)
    }

    fn claimable_entries_with_shard(
        &self,
        now: SystemTime,
        limit: u32,
        shard: Option<ShardFilter>,
    ) -> Result<Vec<RepoLedgerEntry>, LedgerStoreError> {
        let now_ms = time_to_millis(now)?;
        let limit = i64::from(limit);
        let shard_bucket = shard
            .map(|filter| shard_bucket_to_i64(filter.bucket()))
            .transpose()?;
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
                last_error,
                worker_id,
                claimed_at_ms,
                lease_until_ms
            FROM repo_ledger
            WHERE
                (
                    (
                        status IN ('pending', 'retryable_failure')
                        AND (next_attempt_after_ms IS NULL OR next_attempt_after_ms <= ?1)
                    )
                    OR (
                        status = 'throttled'
                        AND next_attempt_after_ms IS NOT NULL
                        AND next_attempt_after_ms <= ?1
                    )
                )
                AND (?3 IS NULL OR shard_bucket = ?3)
            ORDER BY COALESCE(next_attempt_after_ms, 0), did
            LIMIT ?2
            ",
        )?;
        let rows = statement.query_map(params![now_ms, limit, shard_bucket], row_to_entry)?;
        let entries = rows.collect::<Result<Vec<_>, _>>()?;
        entries.into_iter().collect()
    }

    fn ensure_repo_ledger_columns(&self) -> Result<(), LedgerStoreError> {
        let mut statement = self.connection.prepare("PRAGMA table_info(repo_ledger)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        if !columns.iter().any(|column| column == "shard_bucket") {
            self.connection.execute(
                "ALTER TABLE repo_ledger ADD COLUMN shard_bucket INTEGER",
                [],
            )?;
        }
        if !columns.iter().any(|column| column == "worker_id") {
            self.connection
                .execute("ALTER TABLE repo_ledger ADD COLUMN worker_id TEXT", [])?;
        }
        if !columns.iter().any(|column| column == "claimed_at_ms") {
            self.connection.execute(
                "ALTER TABLE repo_ledger ADD COLUMN claimed_at_ms INTEGER",
                [],
            )?;
        }
        if !columns.iter().any(|column| column == "lease_until_ms") {
            self.connection.execute(
                "ALTER TABLE repo_ledger ADD COLUMN lease_until_ms INTEGER",
                [],
            )?;
        }
        self.backfill_missing_shard_buckets()
    }

    fn backfill_missing_shard_buckets(&self) -> Result<(), LedgerStoreError> {
        let dids = {
            let mut statement = self
                .connection
                .prepare("SELECT did FROM repo_ledger WHERE shard_bucket IS NULL")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut statement = self
            .connection
            .prepare("UPDATE repo_ledger SET shard_bucket = ?1 WHERE did = ?2")?;
        for did in dids {
            statement.execute(params![
                shard_bucket_to_i64(did_shard_bucket(&did))?,
                did.as_str(),
            ])?;
        }
        Ok(())
    }
}

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
    let worker_id: Option<String> = row.get(9)?;
    let claimed_at_ms: Option<i64> = row.get(10)?;
    let lease_until_ms: Option<i64> = row.get(11)?;

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
        worker_id,
        claimed_at_ms,
        lease_until_ms,
    ))
}

fn load_entry_in_transaction(
    transaction: &Transaction<'_>,
    did: &str,
) -> Result<Option<RepoLedgerEntry>, LedgerStoreError> {
    transaction
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
                last_error,
                worker_id,
                claimed_at_ms,
                lease_until_ms
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

fn update_entry_if_owned(
    transaction: &Transaction<'_>,
    entry: &RepoLedgerEntry,
    worker_id: &str,
    attempt: &AttemptId,
) -> Result<usize, LedgerStoreError> {
    let status = StoredStatus::from(&entry.status);
    let next_attempt_after_ms = optional_time_to_millis(entry.next_attempt_after)?;
    let last_attempt_sequence = entry
        .last_attempt
        .as_ref()
        .map(|attempt| i64::try_from(attempt.sequence))
        .transpose()
        .map_err(|_err| LedgerStoreError::IntegerOverflow)?;
    let claimed_at_ms = optional_time_to_millis(entry.claimed_at)?;
    let lease_until_ms = optional_time_to_millis(entry.lease_until)?;
    let owned_attempt_sequence =
        i64::try_from(attempt.sequence).map_err(|_err| LedgerStoreError::IntegerOverflow)?;
    transaction
        .execute(
            "
            UPDATE repo_ledger
            SET
                status = ?2,
                terminal_account_state = ?3,
                attempts = ?4,
                next_attempt_after_ms = ?5,
                last_attempt_run_id = ?6,
                last_attempt_did = ?7,
                last_attempt_sequence = ?8,
                last_error = ?9,
                worker_id = ?10,
                claimed_at_ms = ?11,
                lease_until_ms = ?12
            WHERE
                did = ?1
                AND status = 'claimed'
                AND worker_id = ?13
                AND last_attempt_run_id = ?14
                AND last_attempt_did = ?15
                AND last_attempt_sequence = ?16
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
                entry.worker_id.as_deref(),
                claimed_at_ms,
                lease_until_ms,
                worker_id,
                attempt.run_id.as_str(),
                attempt.did.as_str(),
                owned_attempt_sequence,
            ],
        )
        .map_err(Into::into)
}

fn update_expired_claim(
    transaction: &Transaction<'_>,
    entry: &RepoLedgerEntry,
    now: SystemTime,
) -> Result<usize, LedgerStoreError> {
    let status = StoredStatus::from(&entry.status);
    let next_attempt_after_ms = optional_time_to_millis(entry.next_attempt_after)?;
    let last_attempt_sequence = entry
        .last_attempt
        .as_ref()
        .map(|attempt| i64::try_from(attempt.sequence))
        .transpose()
        .map_err(|_err| LedgerStoreError::IntegerOverflow)?;
    let claimed_at_ms = optional_time_to_millis(entry.claimed_at)?;
    let lease_until_ms = optional_time_to_millis(entry.lease_until)?;
    transaction
        .execute(
            "
            UPDATE repo_ledger
            SET
                status = ?2,
                terminal_account_state = ?3,
                attempts = ?4,
                next_attempt_after_ms = ?5,
                last_attempt_run_id = ?6,
                last_attempt_did = ?7,
                last_attempt_sequence = ?8,
                last_error = ?9,
                worker_id = ?10,
                claimed_at_ms = ?11,
                lease_until_ms = ?12
            WHERE
                did = ?1
                AND status = 'claimed'
                AND lease_until_ms IS NOT NULL
                AND lease_until_ms <= ?13
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
                entry.worker_id.as_deref(),
                claimed_at_ms,
                lease_until_ms,
                time_to_millis(now)?,
            ],
        )
        .map_err(Into::into)
}

fn row_to_host_override(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<HostOverride, LedgerStoreError>> {
    let host: String = row.get(0)?;
    let disabled: i64 = row.get(1)?;
    let concurrency_cap: Option<i64> = row.get(2)?;
    let revive_after_ms: Option<i64> = row.get(3)?;
    let force_mode: Option<String> = row.get(4)?;

    Ok(build_host_override(
        host,
        disabled,
        concurrency_cap,
        revive_after_ms,
        force_mode,
    ))
}

fn build_host_override(
    host: String,
    disabled: i64,
    concurrency_cap: Option<i64>,
    revive_after_ms: Option<i64>,
    force_mode: Option<String>,
) -> Result<HostOverride, LedgerStoreError> {
    let disabled = match disabled {
        0 => false,
        1 => true,
        value => return Err(LedgerStoreError::InvalidHostOverrideDisabled { value }),
    };
    let concurrency_cap = concurrency_cap
        .map(u32::try_from)
        .transpose()
        .map_err(|_err| LedgerStoreError::IntegerOverflow)?;
    let revive_after = revive_after_ms.map(time_from_millis).transpose()?;
    let force_mode = force_mode.map(|mode| parse_force_mode(&mode)).transpose()?;
    let record = HostOverride {
        host,
        disabled,
        concurrency_cap,
        revive_after,
        force_mode,
    };
    validate_host_override(&record)?;
    Ok(record)
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
    worker_id: Option<String>,
    claimed_at_ms: Option<i64>,
    lease_until_ms: Option<i64>,
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
        worker_id,
        claimed_at: claimed_at_ms.map(time_from_millis).transpose()?,
        lease_until: lease_until_ms.map(time_from_millis).transpose()?,
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

const fn force_mode_name(mode: ForcedFetchMode) -> &'static str {
    match mode {
        ForcedFetchMode::GetRepo => "get_repo",
        ForcedFetchMode::ListRecords => "list_records",
    }
}

fn parse_force_mode(mode: &str) -> Result<ForcedFetchMode, LedgerStoreError> {
    match mode {
        "get_repo" => Ok(ForcedFetchMode::GetRepo),
        "list_records" => Ok(ForcedFetchMode::ListRecords),
        _ => Err(LedgerStoreError::InvalidForcedFetchMode {
            mode: mode.to_owned(),
        }),
    }
}

fn validate_host_override(record: &HostOverride) -> Result<(), LedgerStoreError> {
    if record.host.trim().is_empty() {
        return Err(LedgerStoreError::InvalidHostOverride {
            message: "host must not be blank".to_owned(),
        });
    }
    if record.concurrency_cap == Some(0) {
        return Err(LedgerStoreError::InvalidHostOverride {
            message: "concurrency cap must be greater than zero".to_owned(),
        });
    }
    Ok(())
}

fn validate_worker_id(worker_id: &str) -> Result<(), LedgerError> {
    if worker_id.trim().is_empty() {
        return Err(LedgerError::InvalidWorkerId);
    }
    Ok(())
}

const fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn shard_bucket_to_i64(bucket: u64) -> Result<i64, LedgerStoreError> {
    i64::try_from(bucket).map_err(|_err| LedgerStoreError::IntegerOverflow)
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
    use std::{
        fs,
        path::PathBuf,
        sync::{Arc, Barrier},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use rusqlite::params;

    use crate::{
        ledger::{
            AttemptId, AttemptOutcome, DID_SHARD_BUCKET_MODULUS, ForcedFetchMode, HostOverride,
            LedgerError, LedgerStoreError, RepoLedgerEntry, RepoLedgerStatus, RetryPolicy,
            ShardFilter, SqliteLedger, claim_repo, complete_attempt, did_shard_bucket,
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
        assert!(failed.next_attempt_after.is_some_and(|due| due > now));
        assert!(failed.can_claim_at(failed.next_attempt_after.unwrap()));
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
    fn did_shard_bucket_is_deterministic_and_bounded() {
        let did = "did:plc:abc";

        let bucket = did_shard_bucket(did);

        assert_eq!(bucket, did_shard_bucket(did));
        assert!(bucket < DID_SHARD_BUCKET_MODULUS);
        assert!(ShardFilter::new(bucket).unwrap().contains_did(did));
        assert!(matches!(
            ShardFilter::new(DID_SHARD_BUCKET_MODULUS),
            Err(LedgerError::InvalidShardBucket { .. })
        ));
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
            worker_id: None,
            claimed_at: None,
            lease_until: None,
        };

        store.upsert_entry(&entry).unwrap();

        assert_eq!(store.load_entry("did:plc:abc").unwrap(), Some(entry));
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT shard_bucket FROM repo_ledger WHERE did = ?1",
                    params!["did:plc:abc"],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            i64::try_from(did_shard_bucket("did:plc:abc")).unwrap()
        );
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
            worker_id: None,
            claimed_at: None,
            lease_until: None,
        };
        let pending = RepoLedgerEntry::pending("did:plc:pending");
        let future_retry = RepoLedgerEntry {
            did: "did:plc:future".to_owned(),
            status: RepoLedgerStatus::RetryableFailure,
            attempts: 1,
            next_attempt_after: Some(now + Duration::from_secs(1)),
            last_attempt: Some(AttemptId::new("run-1", "did:plc:future", 1)),
            last_error: Some("timeout".to_owned()),
            worker_id: None,
            claimed_at: None,
            lease_until: None,
        };
        let succeeded = RepoLedgerEntry {
            did: "did:plc:done".to_owned(),
            status: RepoLedgerStatus::Succeeded,
            attempts: 1,
            next_attempt_after: None,
            last_attempt: Some(AttemptId::new("run-1", "did:plc:done", 1)),
            last_error: None,
            worker_id: None,
            claimed_at: None,
            lease_until: None,
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
    fn sqlite_claimable_entries_can_filter_by_shard() {
        let store = SqliteLedger::open_in_memory().unwrap();
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        let target = RepoLedgerEntry::pending("did:plc:target");
        let target_bucket = did_shard_bucket(&target.did);
        let other = (0_u64..1_000)
            .map(|index| RepoLedgerEntry::pending(format!("did:plc:other{index}")))
            .find(|entry| did_shard_bucket(&entry.did) != target_bucket)
            .unwrap();

        store.upsert_entry(&target).unwrap();
        store.upsert_entry(&other).unwrap();

        let claimable = store
            .claimable_entries_for_shard(now, 10, ShardFilter::new(target_bucket).unwrap())
            .unwrap();

        assert_eq!(claimable, vec![target]);
    }

    #[test]
    fn sqlite_atomic_claim_allows_exactly_one_owner_across_connections() {
        let db_path = temp_ledger_path("claim-race");
        let store = SqliteLedger::open(&db_path).unwrap();
        store
            .upsert_entry(&RepoLedgerEntry::pending("did:plc:race"))
            .unwrap();
        drop(store);
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        let open_barrier = Arc::new(Barrier::new(3));
        let claim_barrier = Arc::new(Barrier::new(3));
        let make_handle = |worker_id: &'static str| {
            let db_path = db_path.clone();
            let open_barrier = Arc::clone(&open_barrier);
            let claim_barrier = Arc::clone(&claim_barrier);
            thread::spawn(move || {
                let store = SqliteLedger::open(&db_path).unwrap();
                open_barrier.wait();
                claim_barrier.wait();
                store
                    .try_claim_next(now, "run-1", worker_id, Duration::from_secs(60), None)
                    .unwrap()
            })
        };
        let handles = [make_handle("worker-a"), make_handle("worker-b")];
        open_barrier.wait();
        claim_barrier.wait();

        let claims = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        let claimed = claims.iter().flatten().collect::<Vec<_>>();

        assert_eq!(claimed.len(), 1);
        let claimed = claimed.into_iter().next().unwrap();
        assert_eq!(claimed.status, RepoLedgerStatus::Claimed);
        assert_eq!(claimed.attempts, 1);
        assert!(matches!(
            claimed.worker_id.as_deref(),
            Some("worker-a" | "worker-b")
        ));
        fs::remove_file(db_path).unwrap();
    }

    #[test]
    fn sqlite_active_claim_is_not_recovered_before_lease_expiry() {
        let store = SqliteLedger::open_in_memory().unwrap();
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        store
            .upsert_entry(&RepoLedgerEntry::pending("did:plc:active"))
            .unwrap();
        let claimed = store
            .try_claim_next(now, "run-1", "worker-a", Duration::from_secs(60), None)
            .unwrap()
            .unwrap();

        let recovered = store
            .recover_expired_claim(
                "did:plc:active",
                now + Duration::from_secs(59),
                "expired claim",
            )
            .unwrap();

        assert_eq!(recovered, None);
        assert_eq!(store.load_entry("did:plc:active").unwrap(), Some(claimed));
    }

    #[test]
    fn sqlite_completion_requires_current_owner() {
        let store = SqliteLedger::open_in_memory().unwrap();
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        store
            .upsert_entry(&RepoLedgerEntry::pending("did:plc:owned"))
            .unwrap();
        let claimed = store
            .try_claim_next(now, "run-1", "worker-a", Duration::from_secs(60), None)
            .unwrap()
            .unwrap();
        let mut wrong_owner = claimed.clone();
        wrong_owner.worker_id = Some("worker-b".to_owned());

        let completed = store
            .complete_owned_claim(
                &wrong_owner,
                AttemptOutcome::Succeeded,
                now + Duration::from_secs(1),
                RetryPolicy::default(),
            )
            .unwrap();

        assert_eq!(completed, None);
        assert_eq!(store.load_entry("did:plc:owned").unwrap(), Some(claimed));
    }

    #[test]
    fn sqlite_migrates_missing_shard_bucket_column() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "
                CREATE TABLE repo_ledger (
                    did TEXT PRIMARY KEY NOT NULL,
                    status TEXT NOT NULL,
                    terminal_account_state TEXT,
                    attempts INTEGER NOT NULL CHECK (attempts >= 0),
                    next_attempt_after_ms INTEGER,
                    last_attempt_run_id TEXT,
                    last_attempt_did TEXT,
                    last_attempt_sequence INTEGER,
                    last_error TEXT
                );
                INSERT INTO repo_ledger (did, status, attempts)
                VALUES ('did:plc:legacy', 'pending', 0);
                ",
            )
            .unwrap();

        let store = SqliteLedger::from_connection(connection).unwrap();

        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT shard_bucket FROM repo_ledger WHERE did = ?1",
                    params!["did:plc:legacy"],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            i64::try_from(did_shard_bucket("did:plc:legacy")).unwrap()
        );
    }

    #[test]
    fn sqlite_host_overrides_round_trip() {
        let store = SqliteLedger::open_in_memory().unwrap();
        let override_record = HostOverride {
            host: "pds.example.com".to_owned(),
            disabled: true,
            concurrency_cap: Some(3),
            revive_after: Some(UNIX_EPOCH + Duration::from_secs(60)),
            force_mode: Some(ForcedFetchMode::ListRecords),
        };

        store.upsert_host_override(&override_record).unwrap();

        assert_eq!(
            store.load_host_override("pds.example.com").unwrap(),
            Some(override_record)
        );
    }

    #[test]
    fn sqlite_host_overrides_reject_blank_hosts_and_zero_caps() {
        let store = SqliteLedger::open_in_memory().unwrap();
        let blank_host = HostOverride {
            host: " ".to_owned(),
            disabled: true,
            concurrency_cap: None,
            revive_after: None,
            force_mode: None,
        };
        let zero_cap = HostOverride {
            host: "pds.example.com".to_owned(),
            disabled: false,
            concurrency_cap: Some(0),
            revive_after: None,
            force_mode: Some(ForcedFetchMode::GetRepo),
        };

        assert!(matches!(
            store.upsert_host_override(&blank_host),
            Err(LedgerStoreError::InvalidHostOverride { .. })
        ));
        assert!(matches!(
            store.upsert_host_override(&zero_cap),
            Err(LedgerStoreError::InvalidHostOverride { .. })
        ));
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

    fn temp_ledger_path(name: &str) -> PathBuf {
        let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        std::env::temp_dir().join(format!(
            "emojistats-backfill-ledger-{name}-{}-{}.sqlite",
            std::process::id(),
            since_epoch.as_nanos()
        ))
    }
}
