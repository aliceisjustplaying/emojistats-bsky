#![allow(clippy::redundant_pub_crate)]

use std::{path::Path, sync::Arc, time::Instant};

use emojistats_backfill::{
    archive::{ArchiveCommitContext, ArchiveStorageConfig},
    parse::ParseConfig,
};
use tokio::sync::Semaphore;

use super::super::{
    failure::{
        FetchOneFailure, SmokeTelemetry, current_rss_kb, elapsed_ms, emit_smoke_telemetry,
        outcome_name, retryable_failure,
    },
    main::{
        archive_host::{ArchiveClaimCheck, parse_and_archive_spooled_repo},
        processed_repo::{FetchedRepo, ProcessedRepo},
    },
};

pub(crate) struct ParseArchiveStep<'a> {
    pub(crate) did_str: &'a str,
    pub(crate) host: &'a str,
    pub(crate) fetched: &'a FetchedRepo,
    pub(crate) archive_dir: &'a Path,
    pub(crate) parse_permits: Option<&'a Arc<Semaphore>>,
    pub(crate) claim_check: Option<ArchiveClaimCheck>,
    pub(crate) archive_context: ArchiveCommitContext,
    pub(crate) archive_storage: ArchiveStorageConfig,
    pub(crate) parse_config: ParseConfig,
    pub(crate) attempt_started: Instant,
}

pub(crate) async fn parse_archive_or_emit_failure(
    step: ParseArchiveStep<'_>,
) -> Result<ProcessedRepo, FetchOneFailure> {
    emit_parse_archive_running(&step, "parse_wait");
    let _permit =
        match step.parse_permits {
            Some(permits) => Some(permits.clone().acquire_owned().await.map_err(|_error| {
                retryable_failure("parse/archive semaphore closed".to_owned())
            })?),
            None => None,
        };
    emit_parse_archive_running(&step, "parse_start");
    let did = step.did_str.to_owned();
    let car_path = step.fetched.spooled.car_path.clone();
    let archive_dir = step.archive_dir.to_path_buf();
    let archive_context = step.archive_context;
    let archive_storage = step.archive_storage;
    let parse_config = step.parse_config;
    let claim_check = step.claim_check;
    let parsed = tokio::task::spawn_blocking(move || {
        parse_and_archive_spooled_repo(
            &did,
            &car_path,
            &archive_dir,
            archive_context,
            archive_storage,
            parse_config,
            claim_check,
        )
    })
    .await
    .map_err(|err| {
        retryable_failure(format!(
            "parse/archive task failed for {}: {err}",
            step.did_str
        ))
    })?;
    match parsed {
        Ok(processed) => {
            let counts = processed.counts();
            emit_smoke_telemetry(&SmokeTelemetry {
                event: "smoke_repo_attempt",
                did: step.did_str,
                host: Some(step.host),
                outcome: "running",
                stage: "parse_archive_done",
                pressure_state: None,
                elapsed_ms: elapsed_ms(step.attempt_started),
                fetch_ms: Some(step.fetched.fetch_ms),
                parse_ms: processed.parse_ms(),
                archive_ms: Some(processed.archive_ms()),
                bytes: Some(step.fetched.spooled.bytes),
                records: Some(counts.records),
                archived_posts: Some(counts.archived_posts),
                decode_errors: Some(counts.decode_errors),
                emoji_rows: Some(counts.emoji_rows),
                rss_kb: current_rss_kb(),
                error: None,
            });
            Ok(processed)
        }
        Err(failure) => {
            emit_smoke_telemetry(&SmokeTelemetry {
                event: "smoke_repo_attempt",
                did: step.did_str,
                host: Some(step.host),
                outcome: outcome_name(&failure.outcome),
                stage: "parse_archive",
                pressure_state: None,
                elapsed_ms: elapsed_ms(step.attempt_started),
                fetch_ms: Some(step.fetched.fetch_ms),
                parse_ms: None,
                archive_ms: None,
                bytes: Some(step.fetched.spooled.bytes),
                records: None,
                archived_posts: None,
                decode_errors: None,
                emoji_rows: None,
                rss_kb: current_rss_kb(),
                error: Some(failure.error.to_string()),
            });
            Err(failure)
        }
    }
}

fn emit_parse_archive_running(step: &ParseArchiveStep<'_>, stage: &'static str) {
    emit_smoke_telemetry(&SmokeTelemetry {
        event: "smoke_repo_attempt",
        did: step.did_str,
        host: Some(step.host),
        outcome: "running",
        stage,
        pressure_state: pressure_state_for_stage(stage),
        elapsed_ms: elapsed_ms(step.attempt_started),
        fetch_ms: Some(step.fetched.fetch_ms),
        parse_ms: None,
        archive_ms: None,
        bytes: Some(step.fetched.spooled.bytes),
        records: None,
        archived_posts: None,
        decode_errors: None,
        emoji_rows: None,
        rss_kb: current_rss_kb(),
        error: None,
    });
}

fn pressure_state_for_stage(stage: &str) -> Option<&'static str> {
    match stage {
        "parse_wait" => Some("parse_backpressure"),
        "parse_start" => Some("parse_active"),
        _ => None,
    }
}
