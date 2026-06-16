use std::{
    path::Path,
    time::{Duration, SystemTime},
};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use super::{
    AttemptOutcome, HostOverride, LedgerSeedBatchSummary, LedgerStoreError, RepoLedgerEntry,
    RepoLedgerStatus, RetryPolicy, SHARD_BUCKET_MIGRATION_BATCH_SIZE, ShardFilter,
    complete_attempt, did_shard_bucket, validate_worker_id,
};
use crate::ledger::codec::{
    StoredStatus, bool_to_i64, force_mode_name, load_entry_in_transaction, optional_time_to_millis,
    row_to_entry, row_to_host_override, shard_bucket_to_i64, time_to_millis, update_entry_if_owned,
    update_expired_claim, validate_host_override,
};

/// SQLite-backed store for durable per-repo crawler state.
pub struct SqliteLedger {
    pub(super) connection: Connection,
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

            CREATE TABLE IF NOT EXISTS manifest_sequences (
                run_id TEXT NOT NULL,
                shard TEXT NOT NULL,
                next_sequence INTEGER NOT NULL CHECK (next_sequence > 0),
                PRIMARY KEY (run_id, shard)
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

    /// Insert pending seed rows without loading each DID first.
    ///
    /// Existing rows are left unchanged, including terminal and succeeded rows.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when a DID cannot be encoded or SQLite rejects the batch.
    pub fn insert_pending_entries_ignore_existing<'a, I>(
        &self,
        dids: I,
    ) -> Result<LedgerSeedBatchSummary, LedgerStoreError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let transaction = self.connection.unchecked_transaction()?;
        let mut summary = LedgerSeedBatchSummary::default();
        {
            let mut statement = transaction.prepare(
                "
                INSERT OR IGNORE INTO repo_ledger (
                    did,
                    shard_bucket,
                    status,
                    attempts
                ) VALUES (?1, ?2, 'pending', 0)
                ",
            )?;
            for did in dids {
                let changed =
                    statement.execute(params![did, shard_bucket_to_i64(did_shard_bucket(did))?])?;
                if changed == 0 {
                    summary.existing = summary
                        .existing
                        .checked_add(1)
                        .ok_or(LedgerStoreError::IntegerOverflow)?;
                } else {
                    summary.inserted = summary
                        .inserted
                        .checked_add(1)
                        .ok_or(LedgerStoreError::IntegerOverflow)?;
                }
            }
        }
        transaction.commit()?;
        Ok(summary)
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

        let manifest_shard = manifest_sequence_shard(shard);
        let manifest_sequence =
            allocate_manifest_sequence(&transaction, run_id, shard, manifest_shard.as_str())?;
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
                last_attempt_sequence = ?7,
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
                manifest_sequence,
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

    /// Extend a claimed repo lease only when the stored row still belongs to this worker attempt.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite fails or the new lease deadline cannot be encoded.
    pub fn extend_owned_claim_lease(
        &self,
        claimed: &RepoLedgerEntry,
        now: SystemTime,
        lease_duration: Duration,
    ) -> Result<Option<RepoLedgerEntry>, LedgerStoreError> {
        let Some(worker_id) = claimed.worker_id.as_deref() else {
            return Ok(None);
        };
        let Some(attempt) = claimed.last_attempt.as_ref() else {
            return Ok(None);
        };
        let lease_until = now
            .checked_add(lease_duration)
            .ok_or(LedgerStoreError::IntegerOverflow)?;
        let lease_until_ms = time_to_millis(lease_until)?;
        let owned_attempt_sequence =
            i64::try_from(attempt.sequence).map_err(|_err| LedgerStoreError::IntegerOverflow)?;
        let transaction = self.connection.unchecked_transaction()?;
        let changed = transaction.execute(
            "
            UPDATE repo_ledger
            SET lease_until_ms = ?2
            WHERE
                did = ?1
                AND status = 'claimed'
                AND worker_id = ?3
                AND last_attempt_run_id = ?4
                AND last_attempt_did = ?5
                AND last_attempt_sequence = ?6
            ",
            params![
                claimed.did.as_str(),
                lease_until_ms,
                worker_id,
                attempt.run_id.as_str(),
                attempt.did.as_str(),
                owned_attempt_sequence,
            ],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(None);
        }
        let updated = load_entry_in_transaction(&transaction, &claimed.did)?
            .ok_or(LedgerStoreError::MissingClaimedEntry)?;
        transaction.commit()?;
        Ok(Some(updated))
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

    /// Requeue expired claimed repos in one bounded transaction.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite fails or recovered rows cannot be encoded.
    pub fn recover_expired_claims(
        &self,
        now: SystemTime,
        limit: u32,
        shard: Option<ShardFilter>,
        message: &str,
    ) -> Result<u64, LedgerStoreError> {
        if limit == 0 {
            return Ok(0);
        }
        let now_ms = time_to_millis(now)?;
        let shard_bucket = shard
            .map(|filter| shard_bucket_to_i64(filter.bucket()))
            .transpose()?;
        let transaction = self.connection.unchecked_transaction()?;
        let dids = {
            let mut statement = transaction.prepare(
                "
                SELECT did
                FROM repo_ledger
                WHERE
                    status = 'claimed'
                    AND lease_until_ms IS NOT NULL
                    AND lease_until_ms <= ?1
                    AND (?2 IS NULL OR shard_bucket = ?2)
                ORDER BY lease_until_ms, did
                LIMIT ?3
                ",
            )?;
            statement
                .query_map(params![now_ms, shard_bucket, i64::from(limit)], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut recovered_count = 0_u64;
        for did in dids {
            let Some(current) = load_entry_in_transaction(&transaction, &did)? else {
                continue;
            };
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
            recovered_count = recovered_count
                .checked_add(
                    u64::try_from(changed).map_err(|_err| LedgerStoreError::IntegerOverflow)?,
                )
                .ok_or(LedgerStoreError::IntegerOverflow)?;
        }
        transaction.commit()?;
        Ok(recovered_count)
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
        loop {
            let dids = {
                let mut statement = self.connection.prepare(
                    "
                    SELECT did
                    FROM repo_ledger
                    WHERE shard_bucket IS NULL
                    ORDER BY did
                    LIMIT ?1
                    ",
                )?;
                statement
                    .query_map(params![SHARD_BUCKET_MIGRATION_BATCH_SIZE], |row| {
                        row.get::<_, String>(0)
                    })?
                    .collect::<Result<Vec<_>, _>>()?
            };
            if dids.is_empty() {
                break;
            }
            let transaction = self.connection.unchecked_transaction()?;
            {
                let mut statement = transaction
                    .prepare("UPDATE repo_ledger SET shard_bucket = ?1 WHERE did = ?2")?;
                for did in &dids {
                    statement.execute(params![
                        shard_bucket_to_i64(did_shard_bucket(did))?,
                        did.as_str(),
                    ])?;
                }
            }
            transaction.commit()?;
        }
        Ok(())
    }
}

fn allocate_manifest_sequence(
    transaction: &Transaction<'_>,
    run_id: &str,
    shard_filter: Option<ShardFilter>,
    shard: &str,
) -> Result<i64, LedgerStoreError> {
    let seed_sequence = seed_manifest_sequence(transaction, run_id, shard_filter)?;
    transaction.execute(
        "
        INSERT OR IGNORE INTO manifest_sequences (run_id, shard, next_sequence)
        VALUES (?1, ?2, ?3)
        ",
        params![run_id, shard, seed_sequence],
    )?;
    let sequence = transaction.query_row(
        "
        SELECT next_sequence
        FROM manifest_sequences
        WHERE run_id = ?1 AND shard = ?2
        ",
        params![run_id, shard],
        |row| row.get::<_, i64>(0),
    )?;
    if sequence <= 0 {
        return Err(LedgerStoreError::IntegerOverflow);
    }
    let next_sequence = sequence
        .checked_add(1)
        .ok_or(LedgerStoreError::IntegerOverflow)?;
    transaction.execute(
        "
        UPDATE manifest_sequences
        SET next_sequence = ?3
        WHERE run_id = ?1 AND shard = ?2
        ",
        params![run_id, shard, next_sequence],
    )?;
    Ok(sequence)
}

fn seed_manifest_sequence(
    transaction: &Transaction<'_>,
    run_id: &str,
    shard_filter: Option<ShardFilter>,
) -> Result<i64, LedgerStoreError> {
    let shard_bucket = shard_filter
        .map(|filter| shard_bucket_to_i64(filter.bucket()))
        .transpose()?;
    let max_sequence = transaction.query_row(
        "
        SELECT MAX(last_attempt_sequence)
        FROM repo_ledger
        WHERE
            last_attempt_run_id = ?1
            AND last_attempt_sequence IS NOT NULL
            AND (?2 IS NULL OR shard_bucket = ?2)
        ",
        params![run_id, shard_bucket],
        |row| row.get::<_, Option<i64>>(0),
    )?;
    max_sequence.map_or(Ok(1), |sequence| {
        sequence
            .checked_add(1)
            .ok_or(LedgerStoreError::IntegerOverflow)
    })
}

fn manifest_sequence_shard(shard: Option<ShardFilter>) -> String {
    shard.map_or_else(
        || "all".to_owned(),
        |filter| format!("shard{}", filter.bucket()),
    )
}
