use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
    time::SystemTime,
};

#[cfg(test)]
use emojistats_backfill::ledger::RepoLedgerEntry;
use emojistats_backfill::{ledger::SqliteLedger, scheduler::ClaimScope};
use jacquard_common::types::did::Did;

use crate::{add_count, increment};

const SEED_BATCH_SIZE: usize = 1_000;
const STALE_RECOVERY_BATCH_SIZE: u32 = 512;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SeedSummary {
    pub inserted: u64,
    pub existing: u64,
    pub blank: u64,
}

#[cfg(test)]
pub fn claimable_entries_for_scope(
    ledger: &SqliteLedger,
    now: SystemTime,
    limit: u32,
    claim_scope: &ClaimScope,
) -> anyhow::Result<Vec<RepoLedgerEntry>> {
    claim_scope.shard_filter().map_or_else(
        || ledger.claimable_entries(now, limit).map_err(Into::into),
        |shard_filter| {
            ledger
                .claimable_entries_for_shard(now, limit, shard_filter)
                .map_err(Into::into)
        },
    )
}

#[cfg(test)]
pub fn recover_stale_claimed_entries(
    ledger: &SqliteLedger,
    _dids_file: &Path,
    now: SystemTime,
) -> anyhow::Result<u64> {
    recover_stale_claimed_entries_for_scope_with_message(
        ledger,
        now,
        &ClaimScope::default(),
        "expired claimed lease at fleet startup",
    )
}

pub(super) fn recover_stale_claimed_entries_for_scope_with_message(
    ledger: &SqliteLedger,
    now: SystemTime,
    claim_scope: &ClaimScope,
    message: &str,
) -> anyhow::Result<u64> {
    let mut recovered = 0_u64;
    loop {
        let batch_recovered = ledger.recover_expired_claims(
            now,
            STALE_RECOVERY_BATCH_SIZE,
            claim_scope.shard_filter(),
            message,
        )?;
        add_count(
            &mut recovered,
            batch_recovered,
            "stale claimed recovery count",
        )?;
        if batch_recovered < u64::from(STALE_RECOVERY_BATCH_SIZE) {
            break;
        }
    }
    Ok(recovered)
}

pub fn seed_ledger_from_file(
    ledger: &SqliteLedger,
    dids_file: &Path,
) -> anyhow::Result<SeedSummary> {
    let mut summary = SeedSummary::default();
    let mut batch = Vec::with_capacity(SEED_BATCH_SIZE);
    let file = File::open(dids_file)?;

    for line in BufReader::new(file).lines() {
        let line = line?;
        let did = line.trim();
        if did.is_empty() {
            increment(&mut summary.blank, "blank line count")?;
            continue;
        }
        let parsed: Did = Did::new_owned(did).map_err(|err| {
            anyhow::anyhow!("invalid DID {did:?} in {}: {err}", dids_file.display())
        })?;

        batch.push(parsed.as_str().to_owned());
        if batch.len() == SEED_BATCH_SIZE {
            flush_seed_batch(ledger, &mut summary, &batch)?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        flush_seed_batch(ledger, &mut summary, &batch)?;
    }

    Ok(summary)
}

fn flush_seed_batch(
    ledger: &SqliteLedger,
    summary: &mut SeedSummary,
    batch: &[String],
) -> anyhow::Result<()> {
    let batch_summary = ledger
        .insert_pending_entries_ignore_existing(batch.iter().map(std::string::String::as_str))?;
    add_count(
        &mut summary.inserted,
        batch_summary.inserted,
        "inserted seed count",
    )?;
    add_count(
        &mut summary.existing,
        batch_summary.existing,
        "existing seed count",
    )?;
    Ok(())
}
