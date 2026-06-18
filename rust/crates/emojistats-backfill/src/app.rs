//! emojistats-backfill — v2 Rust backfill CLI.
//!
//! Vertical-slice milestone in progress: `fetch-one <did>` resolves a DID to its PDS, then
//! fetches the repo, archives posts, and derives emoji rows. See
//! `docs/backfill-v2-design.md` ("First implementation milestone").

use std::{
    path::Path,
    time::{Duration, Instant, SystemTime},
};

use clap::Parser;
use emojistats_backfill::{
    archive::{
        ArchiveCommitContext, ArchiveError, ArchiveStorageConfig, CompletenessClass, FetchMethod,
        NormalizerVersion, StreamingArchiveSink, StreamingReceiptInput,
        archive_row_from_owned_post_observed_at, hash_profile_record,
    },
    clickhouse::create_schema_sql,
    ledger::{
        AttemptOutcome, DEFAULT_CLAIM_LEASE_DURATION, ForcedFetchMode, HostOverride,
        RepoLedgerEntry,
    },
    parse::{ParseConfig, ParseVisitError, ParsedRepoSummary, parse_repo_for_did_with_state},
    scheduler::ClaimScope,
    transport::{FetchConfig, FetchError},
};
use jacquard_common::{deps::fluent_uri::Uri, types::did::Did};
use jacquard_identity::PublicResolver;

#[path = "canary_cmd.rs"]
mod canary_cmd;
#[path = "cli/mod.rs"]
mod cli;
#[path = "derive_manifest_cmd.rs"]
mod derive_manifest_cmd;
#[path = "failure.rs"]
mod failure;
#[path = "fleet.rs"]
mod fleet;
#[path = "main/mod.rs"]
mod main;
#[path = "profile_cmd.rs"]
mod profile_cmd;

#[path = "app/census_cmd.rs"]
mod census_cmd;
#[path = "app/clickhouse_cmd.rs"]
mod clickhouse_cmd;
#[path = "app/counts.rs"]
mod counts;
#[path = "app/fetch_one_cmd.rs"]
mod fetch_one_cmd;
#[path = "app/fleet_cmd.rs"]
mod fleet_cmd;
#[path = "app/metrics.rs"]
mod metrics;
#[path = "app/storage.rs"]
mod storage;

use census_cmd::{run_pds_census_command, run_plc_mirror_command};
use cli::{Cli, Command};
use clickhouse_cmd::run_clickhouse_rebuild_aggregates_command;
use counts::{add_count, count_len, increment, payload_row_count};
use derive_manifest_cmd::DeriveManifestConfig;
use failure::{
    FetchOneFailure, SmokeTelemetry, classify_archive_error, classify_fetch_error,
    classify_parse_error, current_rss_kb, elapsed_ms, emit_smoke_telemetry, outcome_name,
    permanent_failure, retryable_failure,
};
use fetch_one_cmd::{parse_config_for_threads, run_fetch_one_command};
use fleet_cmd::run_fleet_command;
use main::archive_host::parse_and_archive_spooled_repo;
use metrics::metrics_recorder;

/// Runs the CLI dispatcher.
///
/// # Errors
///
/// Returns an error when command parsing succeeds but the selected command fails.
pub async fn run_cli() -> anyhow::Result<()> {
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
        command @ Command::PdsCensus { .. } => run_pds_census_command(command).await,
        command @ Command::DeriveManifest { .. } => run_derive_manifest_command(command).await,
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
        Command::CanarySign {
            evidence_path,
            run_id,
            max_age_seconds,
            hmac_key_env,
            thresholds,
        } => canary_cmd::sign(canary_cmd::CanarySignConfig {
            evidence_path,
            run_id,
            max_age_seconds,
            hmac_key_env,
            thresholds: thresholds.into_thresholds(),
        }),
    }
}

async fn run_derive_manifest_command(command: Command) -> anyhow::Result<()> {
    let Command::DeriveManifest {
        manifest_path,
        archive_root,
        clickhouse_url,
        clickhouse_database,
        clickhouse_user,
        clickhouse_password,
        dry_run,
        derive_ledger_path,
        metrics_jsonl,
        claim_ledger_path,
        claim_worker_id,
        claim_max_entries,
        claim_max_rows,
        claim_stale_seconds,
        clickhouse_insert_slots_dir,
        clickhouse_insert_slots,
        clickhouse_insert_slot_timeout_secs,
    } = command
    else {
        anyhow::bail!("internal command dispatch mismatch for derive-manifest");
    };

    derive_manifest_cmd::run(DeriveManifestConfig {
        manifest_path,
        archive_root,
        clickhouse_url,
        clickhouse_database,
        clickhouse_user,
        clickhouse_password,
        dry_run,
        derive_ledger_path,
        claim_config: claim_ledger_path.map(|ledger_path| {
            derive_manifest_cmd::DeriveManifestClaimConfig {
                ledger_path,
                worker_id: claim_worker_id.unwrap_or_else(default_worker_id),
                max_entries: claim_max_entries,
                max_rows: claim_max_rows,
                stale_seconds: claim_stale_seconds,
            }
        }),
        throttle_config: clickhouse_insert_slots_dir.map(|slots_dir| {
            derive_manifest_cmd::ClickHouseInsertThrottleConfig {
                slots_dir,
                slots: clickhouse_insert_slots,
                max_wait_seconds: clickhouse_insert_slot_timeout_secs,
            }
        }),
        metrics: metrics_recorder(metrics_jsonl.as_deref())?,
    })
    .await
}

fn default_worker_id() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|host| !host.trim().is_empty())
        .map_or_else(
            || format!("worker-{}", std::process::id()),
            |host| format!("{host}-{}", std::process::id()),
        )
}

#[cfg(test)]
#[path = "main/tests.rs"]
mod tests;
