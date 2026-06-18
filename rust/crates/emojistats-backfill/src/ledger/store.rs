use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use rusqlite_migration::{M, Migrations};

use super::{
    AttemptOutcome, DeferredClaimSummary, ForcedFetchMode, HostOverride, LedgerSeedBatchSummary,
    LedgerStoreError, RepoLedgerEntry, RepoLedgerStatus, RetryPolicy,
    SHARD_BUCKET_MIGRATION_BATCH_SIZE, ShardFilter, complete_attempt, did_shard_bucket,
    validate_worker_id,
};
use crate::ledger::codec::{
    StoredStatus, bool_to_i64, force_mode_name, load_entry_in_transaction,
    optional_duration_to_millis, optional_time_to_millis, row_to_entry, row_to_host_override,
    shard_bucket_to_i64, time_to_millis, update_entry_if_owned, update_expired_claim,
    validate_host_override,
};

/// SQLite-backed store for durable per-repo crawler state.
pub struct SqliteLedger {
    pub(super) connection: Connection,
}

fn select_claimable_did(
    transaction: &Transaction<'_>,
    now_ms: i64,
    shard_bucket: Option<i64>,
) -> Result<Option<String>, rusqlite::Error> {
    transaction
        .query_row(
            "
            SELECT did
            FROM (
                SELECT did, 0 AS ready_at
                FROM repo_ledger
                WHERE status = 'pending'
                    AND next_attempt_after_ms IS NULL
                    AND (?2 IS NULL OR shard_bucket = ?2)
                UNION ALL
                SELECT did, next_attempt_after_ms AS ready_at
                FROM repo_ledger
                WHERE status = 'pending'
                    AND next_attempt_after_ms IS NOT NULL
                    AND next_attempt_after_ms <= ?1
                    AND (?2 IS NULL OR shard_bucket = ?2)
                UNION ALL
                SELECT did, 0 AS ready_at
                FROM repo_ledger
                WHERE status = 'retryable_failure'
                    AND next_attempt_after_ms IS NULL
                    AND (?2 IS NULL OR shard_bucket = ?2)
                UNION ALL
                SELECT did, next_attempt_after_ms AS ready_at
                FROM repo_ledger
                WHERE status = 'retryable_failure'
                    AND next_attempt_after_ms IS NOT NULL
                    AND next_attempt_after_ms <= ?1
                    AND (?2 IS NULL OR shard_bucket = ?2)
                UNION ALL
                SELECT did, next_attempt_after_ms AS ready_at
                FROM repo_ledger
                WHERE status IN ('throttled', 'operator_deferred')
                    AND next_attempt_after_ms IS NOT NULL
                    AND next_attempt_after_ms <= ?1
                    AND (?2 IS NULL OR shard_bucket = ?2)
            )
            ORDER BY ready_at, did
            LIMIT 1
            ",
            params![now_ms, shard_bucket],
            |row| row.get::<_, String>(0),
        )
        .optional()
}

fn ledger_migrations() -> Migrations<'static> {
    Migrations::new(vec![M::up_with_hook(
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
                min_interval_ms INTEGER CHECK (min_interval_ms IS NULL OR min_interval_ms > 0),
                revive_after_ms INTEGER,
                force_mode TEXT CHECK (
                    force_mode IS NULL OR force_mode IN ('get_repo', 'list_records')
                ),
                force_mode_revive_after_ms INTEGER,
                never_diff INTEGER NOT NULL DEFAULT 0 CHECK (never_diff IN (0, 1))
            );

            CREATE TABLE IF NOT EXISTS manifest_sequences (
                run_id TEXT NOT NULL,
                shard TEXT NOT NULL,
                next_sequence INTEGER NOT NULL CHECK (next_sequence > 0),
                PRIMARY KEY (run_id, shard)
            );
        ",
        migrate_legacy_schema,
    )])
}

fn migrate_legacy_schema(transaction: &Transaction<'_>) -> rusqlite_migration::HookResult {
    ensure_repo_ledger_columns(transaction)?;
    ensure_host_override_columns(transaction)?;
    backfill_missing_shard_buckets(transaction)?;
    ensure_indexes(transaction)?;
    Ok(())
}

fn ensure_repo_ledger_columns(transaction: &Transaction<'_>) -> rusqlite::Result<()> {
    if !table_has_column(transaction, "repo_ledger", "shard_bucket")? {
        transaction.execute(
            "ALTER TABLE repo_ledger ADD COLUMN shard_bucket INTEGER",
            [],
        )?;
    }
    if !table_has_column(transaction, "repo_ledger", "worker_id")? {
        transaction.execute("ALTER TABLE repo_ledger ADD COLUMN worker_id TEXT", [])?;
    }
    if !table_has_column(transaction, "repo_ledger", "claimed_at_ms")? {
        transaction.execute(
            "ALTER TABLE repo_ledger ADD COLUMN claimed_at_ms INTEGER",
            [],
        )?;
    }
    if !table_has_column(transaction, "repo_ledger", "lease_until_ms")? {
        transaction.execute(
            "ALTER TABLE repo_ledger ADD COLUMN lease_until_ms INTEGER",
            [],
        )?;
    }
    Ok(())
}

fn ensure_host_override_columns(transaction: &Transaction<'_>) -> rusqlite::Result<()> {
    if !table_has_column(transaction, "host_overrides", "min_interval_ms")? {
        transaction.execute(
            "ALTER TABLE host_overrides ADD COLUMN min_interval_ms INTEGER CHECK (min_interval_ms IS NULL OR min_interval_ms > 0)",
            [],
        )?;
    }
    if !table_has_column(transaction, "host_overrides", "never_diff")? {
        transaction.execute(
            "ALTER TABLE host_overrides ADD COLUMN never_diff INTEGER NOT NULL DEFAULT 0 CHECK (never_diff IN (0, 1))",
            [],
        )?;
    }
    if !table_has_column(transaction, "host_overrides", "force_mode_revive_after_ms")? {
        transaction.execute(
            "ALTER TABLE host_overrides ADD COLUMN force_mode_revive_after_ms INTEGER",
            [],
        )?;
    }
    Ok(())
}

fn table_has_column(
    transaction: &Transaction<'_>,
    table: &str,
    column: &str,
) -> rusqlite::Result<bool> {
    let pragma = format!("PRAGMA table_info({table})");
    let mut statement = transaction.prepare(&pragma)?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns.iter().any(|existing| existing == column))
}

fn ensure_indexes(transaction: &Transaction<'_>) -> rusqlite::Result<()> {
    transaction.execute_batch(
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
    )
}

fn backfill_missing_shard_buckets(transaction: &Transaction<'_>) -> rusqlite::Result<()> {
    loop {
        let dids = {
            let mut statement = transaction.prepare(
                "
                    SELECT did
                    FROM repo_ledger
                    WHERE shard_bucket IS NULL
                    ORDER BY did
                    LIMIT ?1
                    ",
            )?;
            let rows = statement.query_map(params![SHARD_BUCKET_MIGRATION_BATCH_SIZE], |row| {
                row.get::<_, String>(0)
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        if dids.is_empty() {
            return Ok(());
        }
        for did in dids {
            let bucket = shard_bucket_to_i64(did_shard_bucket(&did))
                .map_err(|_err| rusqlite::Error::IntegralValueOutOfRange(0, 0))?;
            transaction.execute(
                "UPDATE repo_ledger SET shard_bucket = ?2 WHERE did = ?1",
                params![did, bucket],
            )?;
        }
    }
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
        let mut ledger = Self { connection };
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

    /// Run ledger schema migrations.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite rejects a schema migration.
    pub fn create_schema(&mut self) -> Result<(), LedgerStoreError> {
        ledger_migrations().to_latest(&mut self.connection)?;
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
        let did = select_claimable_did(&transaction, now_ms, shard_bucket)?;
        let Some(did) = did else {
            transaction.commit()?;
            return Ok(None);
        };

        let manifest_shard = manifest_sequence_shard(shard);
        ensure_manifest_sequence(&transaction, run_id, shard, manifest_shard.as_str())?;
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
                last_attempt_sequence = (
                    SELECT next_sequence
                    FROM manifest_sequences
                    WHERE run_id = ?2 AND shard = ?7
                ),
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
                        status IN ('throttled', 'operator_deferred')
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
                manifest_shard.as_str(),
            ],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(None);
        }
        increment_manifest_sequence(&transaction, run_id, manifest_shard.as_str())?;
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
        excluded_worker_id: Option<&str>,
    ) -> Result<u64, LedgerStoreError> {
        if let Some(worker_id) = excluded_worker_id {
            validate_worker_id(worker_id).map_err(LedgerStoreError::Ledger)?;
        }
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
                    AND (?3 IS NULL OR worker_id IS NULL OR worker_id <> ?3)
                ORDER BY lease_until_ms, did
                LIMIT ?4
                ",
            )?;
            statement
                .query_map(
                    params![now_ms, shard_bucket, excluded_worker_id, i64::from(limit)],
                    |row| row.get::<_, String>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut recovered_count = 0_u64;
        for did in dids {
            let Some(current) = load_entry_in_transaction(&transaction, &did)? else {
                continue;
            };
            if excluded_worker_id
                .is_some_and(|worker_id| current.worker_id.as_deref() == Some(worker_id))
            {
                continue;
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
            recovered_count = recovered_count
                .checked_add(
                    u64::try_from(changed).map_err(|_err| LedgerStoreError::IntegerOverflow)?,
                )
                .ok_or(LedgerStoreError::IntegerOverflow)?;
        }
        transaction.commit()?;
        Ok(recovered_count)
    }

    /// Summarize retry/backoff rows that are not currently claimable but may become due later.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when SQLite fails or a stored deadline cannot be decoded.
    pub fn deferred_claim_summary(
        &self,
        now: SystemTime,
        shard: Option<ShardFilter>,
    ) -> Result<DeferredClaimSummary, LedgerStoreError> {
        let now_ms = time_to_millis(now)?;
        let shard_bucket = shard
            .map(|filter| shard_bucket_to_i64(filter.bucket()))
            .transpose()?;
        let (count, next_attempt_after_ms): (i64, Option<i64>) = self.connection.query_row(
            "
            SELECT COUNT(*), MIN(next_attempt_after_ms)
            FROM repo_ledger
            WHERE
                status IN ('pending', 'retryable_failure', 'throttled', 'operator_deferred')
                AND next_attempt_after_ms IS NOT NULL
                AND next_attempt_after_ms > ?1
                AND (?2 IS NULL OR shard_bucket = ?2)
            ",
            params![now_ms, shard_bucket],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok(DeferredClaimSummary {
            count: u64::try_from(count).map_err(|_err| LedgerStoreError::IntegerOverflow)?,
            next_attempt_after: next_attempt_after_ms.map(time_from_millis).transpose()?,
        })
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
        let min_interval_ms = optional_duration_to_millis(record.min_interval)?;
        let revive_after_ms = optional_time_to_millis(record.revive_after)?;
        let force_mode_revive_after_ms = optional_time_to_millis(record.force_mode_revive_after)?;
        self.connection.execute(
            "
            INSERT INTO host_overrides (
                host,
                disabled,
                concurrency_cap,
                min_interval_ms,
                revive_after_ms,
                force_mode,
                force_mode_revive_after_ms,
                never_diff
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(host) DO UPDATE SET
                disabled = excluded.disabled,
                concurrency_cap = excluded.concurrency_cap,
                min_interval_ms = excluded.min_interval_ms,
                revive_after_ms = excluded.revive_after_ms,
                force_mode = excluded.force_mode,
                force_mode_revive_after_ms = excluded.force_mode_revive_after_ms,
                never_diff = excluded.never_diff
            ",
            params![
                record.host.as_str(),
                bool_to_i64(record.disabled),
                concurrency_cap,
                min_interval_ms,
                revive_after_ms,
                record.force_mode.map(force_mode_name),
                force_mode_revive_after_ms,
                bool_to_i64(record.never_diff),
            ],
        )?;
        Ok(())
    }

    /// Set only the forced fetch mode for one host, preserving existing operator policy fields.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerStoreError`] when the host/default record cannot be encoded or persisted.
    pub fn upsert_host_override_force_mode(
        &self,
        host: &str,
        force_mode: Option<ForcedFetchMode>,
        default_min_interval: Option<Duration>,
        force_mode_revive_after: Option<SystemTime>,
    ) -> Result<(), LedgerStoreError> {
        let default_record = HostOverride {
            host: host.to_owned(),
            disabled: false,
            concurrency_cap: None,
            min_interval: default_min_interval,
            revive_after: None,
            force_mode,
            force_mode_revive_after,
            never_diff: false,
        };
        validate_host_override(&default_record)?;
        let min_interval_ms = optional_duration_to_millis(default_min_interval)?;
        let force_mode_revive_after_ms = optional_time_to_millis(force_mode_revive_after)?;
        self.connection.execute(
            "
            INSERT INTO host_overrides (
                host,
                disabled,
                concurrency_cap,
                min_interval_ms,
                revive_after_ms,
                force_mode,
                force_mode_revive_after_ms,
                never_diff
            ) VALUES (?1, 0, NULL, ?2, NULL, ?3, ?4, 0)
            ON CONFLICT(host) DO UPDATE SET
                force_mode = excluded.force_mode,
                force_mode_revive_after_ms = excluded.force_mode_revive_after_ms
            ",
            params![
                host,
                min_interval_ms,
                force_mode.map(force_mode_name),
                force_mode_revive_after_ms,
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
                SELECT host, disabled, concurrency_cap, min_interval_ms, revive_after_ms, force_mode, force_mode_revive_after_ms, never_diff
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
                        status IN ('throttled', 'operator_deferred')
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
}

fn ensure_manifest_sequence(
    transaction: &Transaction<'_>,
    run_id: &str,
    shard_filter: Option<ShardFilter>,
    shard: &str,
) -> Result<(), LedgerStoreError> {
    let seed_sequence = seed_manifest_sequence(transaction, run_id, shard_filter)?;
    transaction.execute(
        "
        INSERT OR IGNORE INTO manifest_sequences (run_id, shard, next_sequence)
        VALUES (?1, ?2, ?3)
        ",
        params![run_id, shard, seed_sequence],
    )?;
    Ok(())
}

fn increment_manifest_sequence(
    transaction: &Transaction<'_>,
    run_id: &str,
    shard: &str,
) -> Result<(), LedgerStoreError> {
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
    Ok(())
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

fn time_from_millis(millis: i64) -> Result<SystemTime, LedgerStoreError> {
    let millis = u64::try_from(millis).map_err(|_err| LedgerStoreError::IntegerOverflow)?;
    UNIX_EPOCH
        .checked_add(Duration::from_millis(millis))
        .ok_or(LedgerStoreError::IntegerOverflow)
}
