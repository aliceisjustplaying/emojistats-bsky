use std::{
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use crate::{
    ledger::{AttemptOutcome, DeferredClaimSummary, RepoLedgerEntry, RetryPolicy, SqliteLedger},
    scheduler::ClaimScope,
};

#[derive(Clone)]
pub(super) struct SharedBlockingLedger {
    ledger: Arc<Mutex<SqliteLedger>>,
}

impl fmt::Debug for SharedBlockingLedger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SharedBlockingLedger")
    }
}

impl SharedBlockingLedger {
    pub(super) fn new(ledger: SqliteLedger) -> Self {
        Self {
            ledger: Arc::new(Mutex::new(ledger)),
        }
    }

    pub(super) async fn recover_stale_claimed_entries_for_scope(
        &self,
        now: SystemTime,
        claim_scope: ClaimScope,
        message: &'static str,
        excluded_worker_id: Option<String>,
    ) -> anyhow::Result<u64> {
        let ledger = Arc::clone(&self.ledger);
        tokio::task::spawn_blocking(move || {
            let ledger = ledger
                .lock()
                .map_err(|_err| anyhow::anyhow!("shared ledger mutex poisoned"))?;
            super::ledger_io::recover_stale_claimed_entries_for_scope_with_message(
                &ledger,
                now,
                &claim_scope,
                message,
                excluded_worker_id.as_deref(),
            )
        })
        .await?
    }

    pub(super) async fn try_claim_next(
        &self,
        now: SystemTime,
        run_id: String,
        worker_id: String,
        lease_duration: Duration,
        claim_scope: ClaimScope,
    ) -> anyhow::Result<Option<RepoLedgerEntry>> {
        let ledger = Arc::clone(&self.ledger);
        tokio::task::spawn_blocking(move || {
            ledger
                .lock()
                .map_err(|_err| anyhow::anyhow!("shared ledger mutex poisoned"))?
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
        &self,
        claimed: RepoLedgerEntry,
        now: SystemTime,
        lease_duration: Duration,
    ) -> anyhow::Result<Option<RepoLedgerEntry>> {
        let ledger = Arc::clone(&self.ledger);
        tokio::task::spawn_blocking(move || {
            ledger
                .lock()
                .map_err(|_err| anyhow::anyhow!("shared ledger mutex poisoned"))?
                .extend_owned_claim_lease(&claimed, now, lease_duration)
                .map_err(Into::into)
        })
        .await?
    }

    pub(super) async fn complete_owned_claim(
        &self,
        claimed: RepoLedgerEntry,
        outcome: AttemptOutcome,
        completed_at: SystemTime,
        retry_policy: RetryPolicy,
    ) -> anyhow::Result<Option<RepoLedgerEntry>> {
        let ledger = Arc::clone(&self.ledger);
        tokio::task::spawn_blocking(move || {
            ledger
                .lock()
                .map_err(|_err| anyhow::anyhow!("shared ledger mutex poisoned"))?
                .complete_owned_claim(&claimed, outcome, completed_at, retry_policy)
                .map_err(Into::into)
        })
        .await?
    }

    pub(super) async fn deferred_claim_summary(
        &self,
        now: SystemTime,
        claim_scope: ClaimScope,
    ) -> anyhow::Result<DeferredClaimSummary> {
        let ledger = Arc::clone(&self.ledger);
        tokio::task::spawn_blocking(move || {
            ledger
                .lock()
                .map_err(|_err| anyhow::anyhow!("shared ledger mutex poisoned"))?
                .deferred_claim_summary(now, claim_scope.shard_filter())
                .map_err(Into::into)
        })
        .await?
    }
}
