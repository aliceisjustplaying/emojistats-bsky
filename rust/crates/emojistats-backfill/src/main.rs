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
        ArchiveCommitContext, ArchiveError, ArchiveStorageConfig, CompletenessClass, FetchMethod,
        NormalizerVersion, StorageBoxArchiveConfig, StreamingArchiveSink, StreamingReceiptInput,
        archive_row_from_owned_post_observed_at, hash_profile_record,
    },
    clickhouse::create_schema_sql,
    ledger::{
        AttemptId, AttemptOutcome, DEFAULT_CLAIM_LEASE_DURATION, ForcedFetchMode, HostOverride,
        RepoLedgerEntry, RetryPolicy, SqliteLedger, claim_repo, complete_attempt,
    },
    list_records::{ListRecordsConfig, fetch_and_archive_list_records_with_precommit_check},
    parse::{ParseConfig, ParseVisitError, ParsedRepoSummary, parse_repo_for_did_with_state},
    scheduler::{ClaimScope, HostPacer, SharedHostPacer},
    transport::{FetchByteBudget, FetchConfig, FetchError, fetch_repo_with_rate_limit_observer},
};
use jacquard_common::{deps::fluent_uri::Uri, types::did::Did};
use jacquard_identity::PublicResolver;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

mod canary_cmd;
mod cli;
mod derive_manifest_cmd;
mod failure;
mod fleet;
#[path = "main/mod.rs"]
pub(crate) mod main;
mod profile_cmd;

use cli::{ArchiveBackend, Cli, Command, HttpProtocol};
use derive_manifest_cmd::DeriveManifestConfig;
use failure::{
    FetchOneFailure, SmokeTelemetry, classify_archive_error, classify_fetch_error,
    classify_list_records_error, classify_parse_error, current_rss_kb, elapsed_ms,
    emit_smoke_telemetry, outcome_name, permanent_failure, retryable_failure,
};
use fleet::{
    DEFAULT_HOST_CONCURRENCY_CAP, FleetConfig, HostConcurrencyLimiter, HostConcurrencyPermit,
    default_worker_id,
};
use main::{
    archive_host::parse_and_archive_spooled_repo,
    fetch_attempt::{LocalFetchOneAttemptConfig, fetch_one_attempt},
};
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
    run_command(Cli::parse().command).await
}

async fn run_command(command: Command) -> anyhow::Result<()> {
    match command {
        command @ Command::FetchOne { .. } => run_fetch_one_command(command).await,
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
        command @ Command::RunFleet { .. } => run_fleet_command(command).await,
        Command::DeriveManifest {
            manifest_path,
            archive_root,
            clickhouse_url,
            clickhouse_database,
            clickhouse_user,
            clickhouse_password,
            dry_run,
            derive_ledger_path,
        } => {
            derive_manifest_cmd::run(DeriveManifestConfig {
                manifest_path,
                archive_root,
                clickhouse_url,
                clickhouse_database,
                clickhouse_user,
                clickhouse_password,
                dry_run,
                derive_ledger_path,
            })
            .await
        }
        Command::ClickhouseSchema {
            clickhouse_database,
        } => {
            println!("{}", create_schema_sql(&clickhouse_database)?);
            Ok(())
        }
        Command::Canary {
            evidence_path,
            thresholds,
        } => canary_cmd::run(canary_cmd::CanaryCommandConfig {
            evidence_path,
            thresholds: thresholds.into_thresholds(),
        }),
    }
}

async fn run_fetch_one_command(command: Command) -> anyhow::Result<()> {
    let Command::FetchOne {
        did,
        spool_dir,
        max_bytes,
        archive_dir,
        archive_backend,
        storage_box_remote,
        storage_box_root,
        storage_box_ssh_program,
        storage_box_ssh_arg,
        storage_box_command_timeout_secs,
        cid_verification_threads,
        http_protocol,
    } = command
    else {
        anyhow::bail!("internal command dispatch mismatch for fetch-one");
    };
    let archive_storage = archive_storage_config(
        archive_backend,
        storage_box_remote,
        storage_box_root,
        storage_box_ssh_program,
        storage_box_ssh_arg,
        storage_box_command_timeout_secs,
    )?;
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

async fn run_fleet_command(command: Command) -> anyhow::Result<()> {
    let Command::RunFleet {
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
        archive_backend,
        storage_box_remote,
        storage_box_root,
        storage_box_ssh_program,
        storage_box_ssh_arg,
        storage_box_command_timeout_secs,
        cid_verification_threads,
        http_protocol,
        canary_evidence,
        bypass_canary,
        canary_thresholds,
    } = command
    else {
        anyhow::bail!("internal command dispatch mismatch for run-fleet");
    };
    enforce_canary_gate(canary_evidence.as_deref(), bypass_canary, canary_thresholds)?;
    validate_fleet_spool_budget(max_inflight_spool_bytes, max_bytes)?;
    let archive_storage = archive_storage_config(
        archive_backend,
        storage_box_remote,
        storage_box_root,
        storage_box_ssh_program,
        storage_box_ssh_arg,
        storage_box_command_timeout_secs,
    )?;
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
        archive_storage,
        cid_verification_threads,
        http_protocol,
        claim_scope: ClaimScope {
            shard_filter: shard_bucket,
        },
    })
    .await
}

fn enforce_canary_gate(
    canary_evidence: Option<&Path>,
    bypass_canary: bool,
    thresholds: cli::CanaryThresholdArgs,
) -> anyhow::Result<()> {
    if bypass_canary {
        eprintln!("run-fleet canary gate bypassed by explicit --bypass-canary");
        return Ok(());
    }
    let Some(path) = canary_evidence else {
        anyhow::bail!("run-fleet requires --canary-evidence <path> or explicit --bypass-canary");
    };
    canary_cmd::require_passing_evidence(path, thresholds.into_thresholds())
}

fn parse_config_for_threads(cid_verification_threads: usize) -> ParseConfig {
    ParseConfig {
        cid_verification_threads,
        ..ParseConfig::default()
    }
}

fn archive_storage_config(
    backend: ArchiveBackend,
    storage_box_remote: Option<String>,
    storage_box_root: Option<String>,
    storage_box_ssh_program: PathBuf,
    storage_box_ssh_arg: Vec<String>,
    storage_box_command_timeout_secs: u64,
) -> anyhow::Result<ArchiveStorageConfig> {
    match backend {
        ArchiveBackend::Local => Ok(ArchiveStorageConfig::Local),
        ArchiveBackend::StorageBoxSsh => {
            let remote = storage_box_remote
                .ok_or_else(|| anyhow::anyhow!("--storage-box-remote is required"))?;
            let root = storage_box_root
                .ok_or_else(|| anyhow::anyhow!("--storage-box-root is required"))?;
            let mut config = StorageBoxArchiveConfig::new(root, remote);
            config.ssh_program = storage_box_ssh_program;
            config.ssh_args = storage_box_ssh_arg;
            config.command_timeout = Duration::from_secs(storage_box_command_timeout_secs);
            Ok(ArchiveStorageConfig::StorageBoxSsh(config))
        }
    }
}

fn validate_fleet_spool_budget(
    max_inflight_spool_bytes: u64,
    max_bytes: u64,
) -> anyhow::Result<()> {
    if max_inflight_spool_bytes < max_bytes {
        anyhow::bail!(
            "--max-inflight-spool-bytes ({max_inflight_spool_bytes}) must be at least --max-bytes ({max_bytes}) so one repo cannot exceed the fleet byte budget"
        );
    }
    Ok(())
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
