use std::{
    path::PathBuf,
    time::{Duration, SystemTime},
};

use emojistats_backfill::{
    ledger::{AttemptOutcome, RepoLedgerEntry, RetryPolicy, SqliteLedger},
    scheduler::ClaimScope,
};

pub(super) async fn recover_stale_claimed_entries_for_scope(
    ledger_path: PathBuf,
    now: SystemTime,
    claim_scope: ClaimScope,
    message: &'static str,
) -> anyhow::Result<u64> {
    tokio::task::spawn_blocking(move || {
        let ledger = SqliteLedger::open(&ledger_path)?;
        super::ledger_io::recover_stale_claimed_entries_for_scope_with_message(
            &ledger,
            now,
            &claim_scope,
            message,
        )
    })
    .await?
}

pub(super) async fn try_claim_next(
    ledger_path: PathBuf,
    now: SystemTime,
    run_id: String,
    worker_id: String,
    lease_duration: Duration,
    claim_scope: ClaimScope,
) -> anyhow::Result<Option<RepoLedgerEntry>> {
    tokio::task::spawn_blocking(move || {
        let ledger = SqliteLedger::open(&ledger_path)?;
        ledger
            .try_claim_next(
                now,
                &run_id,
                &worker_id,
                lease_duration,
                claim_scope.shard_filter(),
            )
            .map_err(Into::into)
    })
    .await?
}

pub(super) async fn extend_owned_claim_lease(
    ledger_path: PathBuf,
    claimed: RepoLedgerEntry,
    now: SystemTime,
    lease_duration: Duration,
) -> anyhow::Result<Option<RepoLedgerEntry>> {
    tokio::task::spawn_blocking(move || {
        let ledger = SqliteLedger::open(&ledger_path)?;
        ledger
            .extend_owned_claim_lease(&claimed, now, lease_duration)
            .map_err(Into::into)
    })
    .await?
}

pub(super) async fn complete_owned_claim(
    ledger_path: PathBuf,
    claimed: RepoLedgerEntry,
    outcome: AttemptOutcome,
    completed_at: SystemTime,
    retry_policy: RetryPolicy,
) -> anyhow::Result<Option<RepoLedgerEntry>> {
    tokio::task::spawn_blocking(move || {
        let ledger = SqliteLedger::open(&ledger_path)?;
        ledger
            .complete_owned_claim(&claimed, outcome, completed_at, retry_policy)
            .map_err(Into::into)
    })
    .await?
}
