use std::{path::PathBuf, time::SystemTime};

use emojistats_backfill::{
    archive::{ArchiveCommitContext, ArchiveStorageConfig},
    ledger::{
        AttemptId, AttemptOutcome, RepoLedgerEntry, RetryPolicy, claim_repo, complete_attempt,
    },
    parse::ParseConfig,
};

use super::{
    cli::{self, Command},
    failure::outcome_name,
    main::fetch_attempt::{LocalFetchOneAttemptConfig, fetch_one_attempt},
    storage::archive_storage_config,
};

pub(super) async fn run_fetch_one_command(command: Command) -> anyhow::Result<()> {
    let Command::FetchOne {
        did,
        spool_dir,
        max_bytes,
        archive_dir,
        archive_storage,
        cid_verification_threads,
        http_protocol,
    } = command
    else {
        anyhow::bail!("internal command dispatch mismatch for fetch-one");
    };
    let archive_storage = archive_storage_config(archive_storage)?;
    fetch_one(
        &did,
        spool_dir,
        max_bytes,
        archive_dir,
        archive_storage,
        cid_verification_threads,
        http_protocol,
    )
    .await
}

pub(super) fn parse_config_for_threads(cid_verification_threads: usize) -> ParseConfig {
    ParseConfig {
        cid_verification_threads,
        ..ParseConfig::default()
    }
}

async fn fetch_one(
    did_str: &str,
    spool_dir: PathBuf,
    max_bytes: u64,
    archive_dir: PathBuf,
    archive_storage: ArchiveStorageConfig,
    cid_verification_threads: usize,
    http_protocol: cli::HttpProtocol,
) -> anyhow::Result<()> {
    let now = SystemTime::now();
    let ledger = RepoLedgerEntry::pending(did_str);
    let claimed = claim_repo(&ledger, AttemptId::new("fetch-one-local", did_str, 1), now)
        .map_err(|err| anyhow::anyhow!("claim fetch-one ledger entry for {did_str}: {err}"))?;

    let result = fetch_one_attempt(LocalFetchOneAttemptConfig {
        did_str,
        spool_dir,
        max_bytes,
        archive_dir,
        archive_context: ArchiveCommitContext::fetch_one_local(),
        archive_storage,
        parse_config: parse_config_for_threads(cid_verification_threads),
        http_protocol,
    })
    .await;
    let outcome = result.as_ref().map_or_else(
        |failure| failure.outcome.clone(),
        |_success| AttemptOutcome::Succeeded,
    );
    let completed = complete_attempt(&claimed, outcome, SystemTime::now(), RetryPolicy::default())
        .map_err(|err| anyhow::anyhow!("complete fetch-one ledger entry for {did_str}: {err}"))?;
    println!(
        "ledger status for {} after {} attempt(s): {:?}",
        completed.did, completed.attempts, completed.status
    );

    result.map_err(|failure| {
        anyhow::anyhow!(
            "fetch-one failed with {}: {}",
            outcome_name(&failure.outcome),
            failure.error
        )
    })
}
