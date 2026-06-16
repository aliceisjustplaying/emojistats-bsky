//! emojistats-backfill — v2 Rust backfill CLI.
//!
//! Vertical-slice milestone in progress: `fetch-one <did>` resolves a DID to its PDS, then
//! (incrementally) fetches the repo via the streaming `getRepo` seam, proves snapshot
//! completeness, archives posts, and derives emoji rows. See `docs/backfill-v2-design.md`
//! ("First implementation milestone").

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use clap::Parser;
use emojistats_backfill::{
    archive::{
        ArchiveCommitContext, ArchiveError, CompletenessClass, FetchMethod, NormalizerVersion,
        StreamingArchiveSink, StreamingReceiptInput, archive_row_from_owned_post_observed_at,
        hash_profile_record,
    },
    clickhouse::create_schema_sql,
    ledger::{
        AttemptId, AttemptOutcome, ForcedFetchMode, HostOverride, RepoLedgerEntry, RetryPolicy,
        SqliteLedger, claim_repo, complete_attempt,
    },
    list_records::{ListRecordsConfig, fetch_and_archive_list_records_with_rate_limit_observer},
    parse::{ParseConfig, ParseVisitError, ParsedRepoSummary, parse_repo_for_did_with_state},
    scheduler::{ClaimScope, HostPacer, SharedHostPacer},
    transport::{FetchByteBudget, FetchConfig, FetchError, fetch_repo},
};
use jacquard_common::{deps::fluent_uri::Uri, types::did::Did};
use jacquard_identity::PublicResolver;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

mod cli;
mod derive_manifest_cmd;
mod failure;
mod fleet;
#[path = "main/mod.rs"]
mod main;
mod profile_cmd;

use cli::{Cli, Command};
use derive_manifest_cmd::DeriveManifestConfig;
use failure::{
    FetchOneFailure, SmokeTelemetry, classify_archive_error, classify_fetch_error,
    classify_list_records_error, classify_parse_error, current_rss_kb, elapsed_ms,
    emit_smoke_telemetry, outcome_name, permanent_failure, retryable_failure,
};
use fleet::{FleetConfig, HostConcurrencyLimiter, HostConcurrencyPermit, default_worker_id};
use main::{archive_host::parse_and_archive_spooled_repo, fetch_attempt::fetch_one_attempt};
#[cfg(test)]
use main::{
    archive_host::{fetch_mode_for_host, load_host_override, pds_host_key, prepare_fetch_host},
    fetch_attempt::{HostOverrideCache, should_fallback_get_repo_to_list_records},
};

const FETCH_TRANSPORT_ATTEMPTS: u8 = 3;
const FETCH_TRANSPORT_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
const FETCH_TRANSPORT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const CRAWLER_USER_AGENT: &str = "emojistats-backfill/0.1 (+https://emojistats.at)";
const HOST_OVERRIDE_CACHE_TTL: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::FetchOne {
            did,
            spool_dir,
            max_bytes,
            archive_dir,
            cid_verification_threads,
        } => {
            fetch_one(
                &did,
                spool_dir,
                max_bytes,
                archive_dir,
                cid_verification_threads,
            )
            .await
        }
        Command::ProfileCar {
            did,
            car_path,
            archive_dir,
            cid_verification_threads,
            parse_only,
        } => profile_cmd::run(
            &did,
            &car_path,
            &archive_dir,
            parse_only,
            cid_verification_threads,
        ),
        Command::RunFleet {
            dids_file,
            ledger_path,
            run_id,
            claim_limit,
            concurrency,
            parse_concurrency,
            max_inflight_spool_bytes,
            shard_bucket,
            spool_dir,
            max_bytes,
            archive_dir,
            cid_verification_threads,
        } => {
            let worker_id = default_worker_id(&run_id);
            fleet::run(FleetConfig {
                dids_file,
                ledger_path,
                run_id,
                worker_id,
                claim_limit,
                concurrency,
                parse_concurrency,
                max_inflight_spool_bytes,
                spool_dir,
                max_bytes,
                archive_dir,
                cid_verification_threads,
                claim_scope: ClaimScope {
                    shard_filter: shard_bucket,
                },
            })
            .await
        }
        Command::DeriveManifest {
            manifest_path,
            archive_root,
            clickhouse_url,
            clickhouse_database,
            clickhouse_user,
            clickhouse_password,
            dry_run,
        } => {
            derive_manifest_cmd::run(DeriveManifestConfig {
                manifest_path,
                archive_root,
                clickhouse_url,
                clickhouse_database,
                clickhouse_user,
                clickhouse_password,
                dry_run,
            })
            .await
        }
        Command::ClickhouseSchema {
            clickhouse_database,
        } => {
            println!("{}", create_schema_sql(&clickhouse_database)?);
            Ok(())
        }
    }
}

fn parse_config_for_threads(cid_verification_threads: usize) -> ParseConfig {
    ParseConfig {
        cid_verification_threads,
        ..ParseConfig::default()
    }
}

/// Resolve a DID to its PDS endpoint.
///
/// Remaining milestone steps build on this: `getRepo` via the `download()` seam over our
/// own reqwest `HttpClient` (capturing rate-limit headers), spool the `CAR` under Loud
/// Resource Caps, parse via an on-disk `BlockStore` + `MST` walk, prove Snapshot
/// Completeness, compute the row-content receipt, write `Parquet` + a manifest entry, and
/// derive emoji rows.
async fn fetch_one(
    did_str: &str,
    spool_dir: PathBuf,
    max_bytes: u64,
    archive_dir: PathBuf,
    cid_verification_threads: usize,
) -> anyhow::Result<()> {
    let now = SystemTime::now();
    let ledger = RepoLedgerEntry::pending(did_str);
    let claimed = claim_repo(&ledger, AttemptId::new("fetch-one-local", did_str, 1), now)
        .map_err(|err| anyhow::anyhow!("claim fetch-one ledger entry for {did_str}: {err}"))?;

    let result = fetch_one_attempt(
        did_str,
        spool_dir,
        max_bytes,
        archive_dir,
        ArchiveCommitContext::fetch_one_local(),
        parse_config_for_threads(cid_verification_threads),
    )
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

    result.map_err(|failure| failure.error)
}

fn increment(value: &mut u64, context: &str) -> anyhow::Result<()> {
    *value = value
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("{context} overflow"))?;
    Ok(())
}

fn add_count(value: &mut u64, addend: u64, context: &str) -> anyhow::Result<()> {
    *value = value
        .checked_add(addend)
        .ok_or_else(|| anyhow::anyhow!("{context} overflow"))?;
    Ok(())
}

fn count_len(value: usize, context: &str) -> anyhow::Result<u64> {
    u64::try_from(value).map_err(|_error| anyhow::anyhow!("{context} overflow"))
}

fn payload_row_count(
    payloads: &[emojistats_backfill::clickhouse::ClickHouseInsertPayload],
) -> anyhow::Result<u64> {
    payloads.iter().try_fold(0_u64, |total, payload| {
        let rows = count_len(payload.row_count, "payload row count")?;
        total
            .checked_add(rows)
            .ok_or_else(|| anyhow::anyhow!("payload row total overflow"))
    })
}

#[cfg(test)]
#[path = "main/tests.rs"]
mod tests;
