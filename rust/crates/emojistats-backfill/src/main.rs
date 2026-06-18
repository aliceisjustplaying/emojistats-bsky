//! emojistats-backfill — v2 Rust backfill CLI.
//!
//! Vertical-slice milestone in progress: `fetch-one <did>` resolves a DID to its PDS, then
//! (incrementally) fetches the repo via the streaming `getRepo` seam, proves snapshot
//! completeness, archives posts, and derives emoji rows. See `docs/backfill-v2-design.md`
//! ("First implementation milestone").

use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};

use clap::Parser;
use emojistats_backfill::{
    archive::{
        ArchiveCommitContext, ArchiveError, ArchiveStorageConfig, CompletenessClass, FetchMethod,
        NormalizerVersion, StorageBoxArchiveConfig, StorageBoxRcloneArchiveConfig,
        StreamingArchiveSink, StreamingReceiptInput, archive_row_from_owned_post_observed_at,
        hash_profile_record,
    },
    census::{
        PdsCensusConfig, PlcMirrorConfig, PlcPlanConfig, mirror_plc_export, plan_plc_ranges,
        run_pds_census,
    },
    clickhouse::{
        ClickHouseClientConfig, aggregate_rebuild_sql, aggregate_rebuild_statements,
        create_schema_sql,
    },
    ledger::{
        AttemptId, AttemptOutcome, DEFAULT_CLAIM_LEASE_DURATION, ForcedFetchMode, HostOverride,
        RepoLedgerEntry, RetryPolicy, SqliteLedger, claim_repo, complete_attempt,
    },
    metrics::{SharedMetricsRecorder, jsonl_metrics_recorder, noop_metrics_recorder},
    parse::{ParseConfig, ParseVisitError, ParsedRepoSummary, parse_repo_for_did_with_state},
    scheduler::ClaimScope,
    transport::{FetchConfig, FetchError},
};
use jacquard_common::{deps::fluent_uri::Uri, types::did::Did};
use jacquard_identity::PublicResolver;

mod canary_cmd;
mod cli;
mod derive_manifest_cmd;
mod failure;
mod fleet;
#[path = "main/mod.rs"]
pub(crate) mod main;
mod profile_cmd;

use cli::{ArchiveBackend, Cli, Command};
use derive_manifest_cmd::DeriveManifestConfig;
use failure::{
    FetchOneFailure, SmokeTelemetry, classify_archive_error, classify_fetch_error,
    classify_parse_error, current_rss_kb, elapsed_ms, emit_smoke_telemetry, outcome_name,
    permanent_failure, retryable_failure,
};
use fleet::{FleetConfig, default_worker_id};
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
        command @ Command::PlcMirror { .. } => run_plc_mirror_command(command).await,
        command @ Command::PlcPlan { .. } => run_plc_plan_command(command).await,
        command @ Command::PdsCensus { .. } => run_pds_census_command(command).await,
        Command::DeriveManifest {
            manifest_path,
            archive_root,
            clickhouse_url,
            clickhouse_database,
            clickhouse_user,
            clickhouse_password,
            dry_run,
            derive_ledger_path,
            metrics_jsonl,
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
                metrics: metrics_recorder(metrics_jsonl.as_deref())?,
            })
            .await
        }
        Command::ClickhouseSchema {
            clickhouse_database,
        } => {
            println!("{}", create_schema_sql(&clickhouse_database)?);
            Ok(())
        }
        Command::ClickhouseRebuildAggregates {
            clickhouse_url,
            clickhouse_database,
            clickhouse_user,
            clickhouse_password,
            dry_run,
        } => {
            run_clickhouse_rebuild_aggregates_command(
                &clickhouse_url,
                &clickhouse_database,
                &clickhouse_user,
                &clickhouse_password,
                dry_run,
            )
            .await
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
        storage_box_rclone_remote,
        storage_box_rclone_config,
        storage_box_rclone_program,
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
    let archive_storage = archive_storage_config(ArchiveStorageArgs {
        backend: archive_backend,
        storage_box_remote,
        storage_box_rclone_remote,
        storage_box_rclone_config,
        storage_box_rclone_program,
        storage_box_root,
        storage_box_ssh_program,
        storage_box_ssh_arg,
        storage_box_command_timeout_secs,
    })?;
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
        storage_box_rclone_remote,
        storage_box_rclone_config,
        storage_box_rclone_program,
        storage_box_root,
        storage_box_ssh_program,
        storage_box_ssh_arg,
        storage_box_command_timeout_secs,
        cid_verification_threads,
        http_protocol,
        canary_evidence,
        bypass_canary,
        canary_thresholds,
        metrics_jsonl,
    } = command
    else {
        anyhow::bail!("internal command dispatch mismatch for run-fleet");
    };
    enforce_canary_gate(canary_evidence.as_deref(), bypass_canary, canary_thresholds)?;
    validate_fleet_spool_budget(max_inflight_spool_bytes, max_bytes)?;
    let archive_storage = archive_storage_config(ArchiveStorageArgs {
        backend: archive_backend,
        storage_box_remote,
        storage_box_rclone_remote,
        storage_box_rclone_config,
        storage_box_rclone_program,
        storage_box_root,
        storage_box_ssh_program,
        storage_box_ssh_arg,
        storage_box_command_timeout_secs,
    })?;
    let worker_id = default_worker_id(&run_id);
    let shard_label = shard_bucket.map_or_else(
        || "all".to_owned(),
        |shard| format!("shard{}", shard.bucket()),
    );
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
        shard_label,
        metrics: metrics_recorder(metrics_jsonl.as_deref())?,
    })
    .await
}

async fn run_plc_mirror_command(command: Command) -> anyhow::Result<()> {
    let Command::PlcMirror {
        ledger_path,
        mirror_dir,
        plc_directory_url,
        page_size,
        limit_pages,
        limit_ops,
        request_timeout_secs,
        workers,
        start_after,
        end_at,
    } = command
    else {
        anyhow::bail!("internal command dispatch mismatch for plc-mirror");
    };
    let mut config = PlcMirrorConfig::new(ledger_path, mirror_dir);
    config.plc_directory_url = plc_directory_url;
    config.page_size = page_size;
    config.limit_pages = limit_pages;
    config.limit_ops = limit_ops;
    config.request_timeout = Duration::from_secs(request_timeout_secs);
    config.workers = workers;
    config.start_after = start_after;
    config.end_at = end_at;
    let summary = mirror_plc_export(config).await?;
    println!(
        "plc_mirror pages={} ops={} upserted={} tombstoned={} skipped={} cursor={} caught_up={}",
        summary.pages,
        summary.ops,
        summary.upserted,
        summary.tombstoned,
        summary.skipped,
        summary.cursor,
        summary.caught_up
    );
    Ok(())
}

async fn run_plc_plan_command(command: Command) -> anyhow::Result<()> {
    let Command::PlcPlan {
        parts,
        plc_directory_url,
        page_size,
        start_after,
        request_timeout_secs,
    } = command
    else {
        anyhow::bail!("internal command dispatch mismatch for plc-plan");
    };
    let mut config = PlcPlanConfig::new(parts);
    config.plc_directory_url = plc_directory_url;
    config.page_size = page_size;
    config.start_after = start_after;
    config.request_timeout = Duration::from_secs(request_timeout_secs);
    let ranges = plan_plc_ranges(config).await?;
    for range in ranges {
        println!(
            "range={} start_after={} end_at={} args=\"--start-after {} --end-at {}\"",
            range.index, range.start_after, range.end_at, range.start_after, range.end_at
        );
    }
    Ok(())
}

async fn run_pds_census_command(command: Command) -> anyhow::Result<()> {
    let Command::PdsCensus {
        ledger_path,
        admitted_dids_file,
        quarantined_hosts_file,
        health_concurrency,
        request_timeout_secs,
        max_hosts,
        no_seed_ledger,
    } = command
    else {
        anyhow::bail!("internal command dispatch mismatch for pds-census");
    };
    let mut config = PdsCensusConfig::new(ledger_path);
    config.admitted_dids_path = admitted_dids_file;
    config.quarantined_hosts_path = quarantined_hosts_file;
    config.health_concurrency = health_concurrency;
    config.request_timeout = Duration::from_secs(request_timeout_secs);
    config.max_hosts = max_hosts;
    config.seed_ledger = !no_seed_ledger;
    let summary = run_pds_census(config).await?;
    println!(
        "pds_census hosts_checked={} hosts_admitted={} hosts_quarantined={} dids_admitted={} seed_inserted={} seed_existing={}",
        summary.hosts_checked,
        summary.hosts_admitted,
        summary.hosts_quarantined,
        summary.dids_admitted,
        summary.seed.inserted,
        summary.seed.existing
    );
    Ok(())
}

fn metrics_recorder(path: Option<&Path>) -> anyhow::Result<SharedMetricsRecorder> {
    path.map_or_else(|| Ok(noop_metrics_recorder()), jsonl_metrics_recorder)
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

struct ArchiveStorageArgs {
    backend: ArchiveBackend,
    storage_box_remote: Option<String>,
    storage_box_rclone_remote: String,
    storage_box_rclone_config: Option<PathBuf>,
    storage_box_rclone_program: PathBuf,
    storage_box_root: Option<String>,
    storage_box_ssh_program: PathBuf,
    storage_box_ssh_arg: Vec<String>,
    storage_box_command_timeout_secs: u64,
}

fn archive_storage_config(args: ArchiveStorageArgs) -> anyhow::Result<ArchiveStorageConfig> {
    let ArchiveStorageArgs {
        backend,
        storage_box_remote,
        storage_box_rclone_remote,
        storage_box_rclone_config,
        storage_box_rclone_program,
        storage_box_root,
        storage_box_ssh_program,
        storage_box_ssh_arg,
        storage_box_command_timeout_secs,
    } = args;
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
        ArchiveBackend::StorageBoxRclone => {
            let root = storage_box_root
                .ok_or_else(|| anyhow::anyhow!("--storage-box-root is required"))?;
            let config_path = storage_box_rclone_config
                .ok_or_else(|| anyhow::anyhow!("--storage-box-rclone-config is required"))?;
            let mut config =
                StorageBoxRcloneArchiveConfig::new(root, storage_box_rclone_remote, config_path);
            config.rclone_program = storage_box_rclone_program;
            config.command_timeout = Duration::from_secs(storage_box_command_timeout_secs);
            Ok(ArchiveStorageConfig::StorageBoxRclone(config))
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

async fn run_clickhouse_rebuild_aggregates_command(
    clickhouse_url: &str,
    clickhouse_database: &str,
    clickhouse_user: &str,
    clickhouse_password: &str,
    dry_run: bool,
) -> anyhow::Result<()> {
    if dry_run {
        println!("{}", aggregate_rebuild_sql(clickhouse_database)?);
        return Ok(());
    }

    let config = ClickHouseClientConfig::new(
        clickhouse_url,
        clickhouse_database,
        clickhouse_user,
        clickhouse_password,
        "emojistats-backfill-aggregate-rebuild",
    )?;
    let client = config.http_client()?;
    let statements = aggregate_rebuild_statements(clickhouse_database)?;
    let receipts = config.execute_sql_statements(&client, &statements).await?;
    println!(
        "rebuilt ClickHouse aggregates with {} statement(s)",
        receipts.len()
    );
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
