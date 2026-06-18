#![allow(clippy::redundant_pub_crate)]

use std::{
    path::Path,
    time::{Duration, Instant, SystemTime},
};

use jacquard_common::{deps::fluent_uri::Uri, types::did::Did};

use super::super::{
    failure::{
        FetchOneFailure, SmokeTelemetry, classify_list_records_error, current_rss_kb, elapsed_ms,
        emit_smoke_telemetry, outcome_name,
    },
    main::{
        archive_host::ArchiveClaimCheck,
        host_rate_limit::{record_rate_limit_cooldown, record_rate_limit_snapshot},
        processed_repo::{
            ListRecordsProcessed, ListRecordsTimings, ProcessedRepo, ProcessedRepoArtifacts,
            ProcessedRepoCounts,
        },
    },
};
use crate::{
    archive::{ArchiveCommitContext, ArchiveStorageConfig},
    list_records::{ListRecordsConfig, fetch_and_archive_list_records_with_precommit_check},
    scheduler::SharedHostPacer,
};

pub(crate) struct ListRecordsStep<'a> {
    pub(crate) http: &'a reqwest::Client,
    pub(crate) pds: &'a Uri<String>,
    pub(crate) did: &'a Did,
    pub(crate) did_str: &'a str,
    pub(crate) host: &'a str,
    pub(crate) host_min_interval: Option<Duration>,
    pub(crate) archive_dir: &'a Path,
    pub(crate) archive_context: ArchiveCommitContext,
    pub(crate) archive_storage: ArchiveStorageConfig,
    pub(crate) host_pacer: Option<&'a SharedHostPacer>,
    pub(crate) claim_check: Option<ArchiveClaimCheck>,
    pub(crate) attempt_started: Instant,
}

pub(crate) async fn fetch_archive_list_records_or_emit_failure(
    step: ListRecordsStep<'_>,
) -> Result<ProcessedRepo, FetchOneFailure> {
    let fetch_started = Instant::now();
    emit_list_records_running(&step);
    let host_pacer = step.host_pacer;
    let host = step.host;
    let claim_check = step.claim_check.clone();
    let did_for_commit_check = step.did_str.to_owned();
    match fetch_and_archive_list_records_with_precommit_check(
        step.http,
        step.pds,
        step.did,
        step.did_str,
        step.archive_dir,
        step.archive_context.clone(),
        step.archive_storage.clone(),
        ListRecordsConfig::default(),
        host_pacer.map(|pacer| {
            crate::list_records::ListRecordsHostPacing::new(pacer, host, step.host_min_interval)
        }),
        |rate_limit| record_rate_limit_snapshot(host_pacer, host, rate_limit, SystemTime::now()),
        move || {
            if let Some(claim_check) = &claim_check {
                claim_check
                    .ensure_owned_before_commit(&did_for_commit_check)
                    .map_err(|err| err.error.to_string())?;
            }
            Ok(())
        },
    )
    .await
    {
        Ok(output) => {
            let processed = ProcessedRepo::ListRecords(ListRecordsProcessed {
                counts: ProcessedRepoCounts {
                    records: output.records,
                    archived_posts: output.archived_posts,
                    decode_errors: output.decode_errors,
                    emoji_rows: output.artifacts.emoji_rows,
                },
                artifacts: ProcessedRepoArtifacts {
                    post_rows_hash: output.receipt.post_rows_hash,
                    parquet_path: output.artifacts.parquet_path,
                    receipt_path: output.artifacts.receipt_path,
                    manifest_path: output.artifacts.manifest_path,
                    emoji_projection_path: output.artifacts.emoji_projection_path,
                },
                timings: ListRecordsTimings {
                    fetch_ms: elapsed_ms(fetch_started),
                    archive_ms: output.archive_ms,
                },
            });
            emit_list_records_success(&step, &processed);
            Ok(processed)
        }
        Err(error) => {
            if let Some(rate_limit) = error.rate_limit() {
                record_rate_limit_snapshot(
                    step.host_pacer,
                    step.host,
                    rate_limit,
                    SystemTime::now(),
                );
            }
            let failure = classify_list_records_error(step.did_str, &error);
            emit_list_records_failure(&step, &failure, fetch_started);
            record_rate_limit_cooldown(step.host_pacer, step.host, &failure);
            Err(failure)
        }
    }
}

fn emit_list_records_running(step: &ListRecordsStep<'_>) {
    emit_smoke_telemetry(&SmokeTelemetry {
        event: "smoke_repo_attempt",
        did: step.did_str,
        host: Some(step.host),
        outcome: "running",
        stage: "list_records_fetch",
        pressure_state: None,
        elapsed_ms: elapsed_ms(step.attempt_started),
        fetch_ms: None,
        parse_ms: None,
        archive_ms: None,
        bytes: None,
        records: None,
        archived_posts: None,
        decode_errors: None,
        emoji_rows: None,
        rss_kb: current_rss_kb(),
        error: None,
    });
}

fn emit_list_records_success(step: &ListRecordsStep<'_>, processed: &ProcessedRepo) {
    let counts = processed.counts();
    emit_smoke_telemetry(&SmokeTelemetry {
        event: "smoke_repo_attempt",
        did: step.did_str,
        host: Some(step.host),
        outcome: "running",
        stage: "list_records_archive_done",
        pressure_state: None,
        elapsed_ms: elapsed_ms(step.attempt_started),
        fetch_ms: processed.fetch_ms_opt(),
        parse_ms: processed.parse_ms(),
        archive_ms: Some(processed.archive_ms()),
        bytes: None,
        records: Some(counts.records),
        archived_posts: Some(counts.archived_posts),
        decode_errors: Some(counts.decode_errors),
        emoji_rows: Some(counts.emoji_rows),
        rss_kb: current_rss_kb(),
        error: None,
    });
}

fn emit_list_records_failure(
    step: &ListRecordsStep<'_>,
    failure: &FetchOneFailure,
    fetch_started: Instant,
) {
    emit_smoke_telemetry(&SmokeTelemetry {
        event: "smoke_repo_attempt",
        did: step.did_str,
        host: Some(step.host),
        outcome: outcome_name(&failure.outcome),
        stage: "list_records_fetch",
        pressure_state: None,
        elapsed_ms: elapsed_ms(step.attempt_started),
        fetch_ms: Some(elapsed_ms(fetch_started)),
        parse_ms: None,
        archive_ms: None,
        bytes: None,
        records: None,
        archived_posts: None,
        decode_errors: None,
        emoji_rows: None,
        rss_kb: current_rss_kb(),
        error: Some(failure.error.to_string()),
    });
}
