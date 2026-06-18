use std::path::Path;

use emojistats_backfill::scheduler::ClaimScope;

use super::{
    canary_cmd,
    cli::{self, Command},
    fleet::{self, FleetConfig, default_worker_id},
    metrics::metrics_recorder,
    storage::{ArchiveStorageArgs, archive_storage_config},
};

pub(super) async fn run_fleet_command(command: Command) -> anyhow::Result<()> {
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
        canary_evidence_hmac_key_env,
        bypass_canary,
        canary_thresholds,
        metrics_jsonl,
    } = command
    else {
        anyhow::bail!("internal command dispatch mismatch for run-fleet");
    };
    enforce_canary_gate(
        canary_evidence.as_deref(),
        &canary_evidence_hmac_key_env,
        bypass_canary,
        canary_thresholds,
        &run_id,
    )?;
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

fn enforce_canary_gate(
    canary_evidence: Option<&Path>,
    canary_evidence_hmac_key_env: &str,
    bypass_canary: bool,
    thresholds: cli::CanaryThresholdArgs,
    run_id: &str,
) -> anyhow::Result<()> {
    if bypass_canary {
        eprintln!("run-fleet canary gate bypassed by explicit --bypass-canary");
        return Ok(());
    }
    let Some(path) = canary_evidence else {
        anyhow::bail!("run-fleet requires --canary-evidence <path> or explicit --bypass-canary");
    };
    let signature_key =
        canary_cmd::CanaryEvidenceSignatureKey::from_env_var(canary_evidence_hmac_key_env)?;
    canary_cmd::require_passing_evidence(path, thresholds.into_thresholds(), run_id, &signature_key)
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
