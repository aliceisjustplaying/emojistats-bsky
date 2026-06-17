#![allow(clippy::redundant_pub_crate)]

use std::sync::Arc;

use emojistats_backfill::scheduler::SharedHostPacer;
use jacquard_identity::resolver::IdentityResolver;
use tokio::sync::Semaphore;

pub(crate) use super::attempt_runtime::{
    AttemptRuntime, FetchOneAttemptConfig, HostOverrideCache, HostOverrideCacheEntry,
    LocalFetchOneAttemptConfig,
};
use super::{
    super::{
        ArchiveCommitContext, ArchiveStorageConfig, ClaimScope, Did, Duration, FetchConfig,
        FetchError, FetchOneFailure, ForcedFetchMode, Instant, ParseConfig, Path, PublicResolver,
        SmokeTelemetry, SqliteLedger, SystemTime, Uri, classify_fetch_error, current_rss_kb,
        elapsed_ms, emit_smoke_telemetry, outcome_name, permanent_failure, retryable_failure,
    },
    archive_host::{ArchiveClaimCheck, prepare_fetch_host},
    attempt_runtime::acquire_host_fetch_permit,
    host_rate_limit::{record_rate_limit_cooldown, record_rate_limit_snapshot},
    list_records_attempt::{ListRecordsStep, fetch_archive_list_records_or_emit_failure},
    parse_archive_attempt::{ParseArchiveStep, parse_archive_or_emit_failure},
    processed_repo::ProcessedRepo,
    repo_fetch::{FetchStep, fetch_spooled_repo, repo_fetch_client},
};

pub(crate) async fn fetch_one_attempt(
    config: LocalFetchOneAttemptConfig<'_>,
) -> Result<(), FetchOneFailure> {
    let claim_scope = ClaimScope::default();
    fetch_one_attempt_with_pacer(FetchOneAttemptConfig {
        did_str: config.did_str,
        spool_dir: config.spool_dir,
        max_bytes: config.max_bytes,
        archive_dir: config.archive_dir,
        archive_context: config.archive_context,
        archive_storage: config.archive_storage,
        runtime: AttemptRuntime::Local { claim_scope },
        parse_config: config.parse_config,
        http_protocol: config.http_protocol,
    })
    .await
}

pub(crate) async fn fetch_one_attempt_with_pacer(
    config: FetchOneAttemptConfig<'_>,
) -> Result<(), FetchOneFailure> {
    let attempt_started = Instant::now();
    let input_did = config.did_str;
    let did: Did = Did::new_owned(input_did)
        .map_err(|err| permanent_failure(format!("invalid DID {input_did:?}: {err}")))?;
    let did_str = did.as_str();

    let resolver = PublicResolver::default();
    let pds = resolver
        .pds_for_did(&did)
        .await
        .map_err(|err| retryable_failure(format!("resolve PDS for {did_str}: {err}")))?;

    println!("{did_str} -> PDS {pds}");
    let prepared_host = prepare_fetch_host(
        did_str,
        &pds,
        config.runtime.claim_scope(),
        config.runtime.host_override_ledger_path(),
        config.runtime.host_override_cache(),
    )
    .await?;
    let _host_permit = acquire_host_fetch_permit(&config.runtime, &prepared_host).await?;
    let host = prepared_host.host;
    let host_min_interval = prepared_host
        .host_override
        .as_ref()
        .and_then(|override_record| override_record.min_interval);
    let http = repo_fetch_client(config.http_protocol).map_err(|err| {
        retryable_failure(format!("build repo fetch HTTP client for {did_str}: {err}"))
    })?;
    let mut fetch_config = FetchConfig::new(config.spool_dir);
    fetch_config.max_bytes = config.max_bytes;
    fetch_config.byte_budget = config.runtime.byte_budget();

    let processed = fetch_prepared_repo(
        FetchModeStep {
            http: &http,
            pds: &pds,
            did: &did,
            did_str,
            host: host.as_str(),
            host_min_interval,
            fetch_config: &fetch_config,
            archive_dir: &config.archive_dir,
            archive_context: config.archive_context,
            archive_storage: config.archive_storage,
            host_pacer: config.runtime.host_pacer(),
            host_override_ledger_path: config.runtime.host_override_ledger_path(),
            parse_permits: config.runtime.parse_permits(),
            claim_check: config.runtime.archive_claim_check(),
            parse_config: config.parse_config,
            attempt_started,
        },
        prepared_host.fetch_mode,
    )
    .await?;
    let counts = processed.counts();
    let artifacts = processed.artifacts();
    println!(
        "parsed {} records, {} posts, {} decode errors, {} emoji rows, post rows hash {}",
        counts.records,
        counts.archived_posts,
        counts.decode_errors,
        counts.emoji_rows,
        artifacts.post_rows_hash
    );
    println!(
        "wrote archive {}, receipt {}, manifest {}, emoji projection {}",
        artifacts.parquet_path.display(),
        artifacts.receipt_path.display(),
        artifacts.manifest_path.display(),
        artifacts.emoji_projection_path.display()
    );
    emit_smoke_telemetry(&SmokeTelemetry {
        event: "smoke_repo_attempt",
        did: did_str,
        host: Some(host.as_str()),
        outcome: "succeeded",
        stage: "complete",
        pressure_state: None,
        elapsed_ms: elapsed_ms(attempt_started),
        fetch_ms: processed.fetch_ms_opt(),
        parse_ms: processed.parse_ms(),
        archive_ms: Some(processed.archive_ms()),
        bytes: processed.bytes(),
        records: Some(counts.records),
        archived_posts: Some(counts.archived_posts),
        decode_errors: Some(counts.decode_errors),
        emoji_rows: Some(counts.emoji_rows),
        rss_kb: current_rss_kb(),
        error: None,
    });
    Ok(())
}

struct FetchModeStep<'a> {
    http: &'a reqwest::Client,
    pds: &'a Uri<String>,
    did: &'a Did,
    did_str: &'a str,
    host: &'a str,
    host_min_interval: Option<Duration>,
    fetch_config: &'a FetchConfig,
    archive_dir: &'a Path,
    archive_context: ArchiveCommitContext,
    archive_storage: ArchiveStorageConfig,
    host_pacer: Option<&'a SharedHostPacer>,
    host_override_ledger_path: Option<&'a Path>,
    parse_permits: Option<&'a Arc<Semaphore>>,
    claim_check: Option<ArchiveClaimCheck>,
    parse_config: ParseConfig,
    attempt_started: Instant,
}

async fn fetch_prepared_repo(
    step: FetchModeStep<'_>,
    fetch_mode: ForcedFetchMode,
) -> Result<ProcessedRepo, FetchOneFailure> {
    match fetch_mode {
        ForcedFetchMode::GetRepo => fetch_get_repo_and_archive(step).await,
        ForcedFetchMode::ListRecords => {
            fetch_archive_list_records_or_emit_failure(ListRecordsStep {
                http: step.http,
                pds: step.pds,
                did: step.did,
                did_str: step.did_str,
                host: step.host,
                host_min_interval: step.host_min_interval,
                archive_dir: step.archive_dir,
                archive_context: step.archive_context,
                archive_storage: step.archive_storage,
                host_pacer: step.host_pacer,
                claim_check: step.claim_check,
                attempt_started: step.attempt_started,
            })
            .await
        }
    }
}

async fn fetch_get_repo_and_archive(
    step: FetchModeStep<'_>,
) -> Result<ProcessedRepo, FetchOneFailure> {
    let fetched = match fetch_spooled_repo(FetchStep {
        http: step.http,
        pds: step.pds,
        did: step.did,
        did_str: step.did_str,
        host: step.host,
        host_min_interval: step.host_min_interval,
        config: step.fetch_config,
        host_pacer: step.host_pacer,
    })
    .await
    {
        Ok(fetched) => fetched,
        Err(err) if should_fallback_get_repo_to_list_records(&err) => {
            emit_get_repo_fallback(step.did_str, step.host, step.attempt_started, &err);
            let host_override_ledger_path = step.host_override_ledger_path;
            let host = step.host;
            let host_min_interval = step.host_min_interval;
            let result = fetch_archive_list_records_or_emit_failure(ListRecordsStep {
                http: step.http,
                pds: step.pds,
                did: step.did,
                did_str: step.did_str,
                host: step.host,
                host_min_interval: step.host_min_interval,
                archive_dir: step.archive_dir,
                archive_context: step.archive_context.clone(),
                archive_storage: step.archive_storage.clone(),
                host_pacer: step.host_pacer,
                claim_check: step.claim_check,
                attempt_started: step.attempt_started,
            })
            .await;
            if result.is_ok() {
                persist_list_records_method_wall_override(
                    host_override_ledger_path,
                    host,
                    host_min_interval,
                    &err,
                );
            }
            return result;
        }
        Err(err) => {
            if let Some(rate_limit) = err.rate_limit() {
                record_rate_limit_snapshot(
                    step.host_pacer,
                    step.host,
                    rate_limit,
                    SystemTime::now(),
                );
            }
            let failure = classify_fetch_error(step.did_str, &err);
            emit_fetch_failure(&step, &failure, step.attempt_started);
            record_rate_limit_cooldown(step.host_pacer, step.host, &failure);
            return Err(failure);
        }
    };
    println!(
        "spooled {} bytes from HTTP {} to {}",
        fetched.spooled.bytes,
        fetched.spooled.http_status,
        fetched.spooled.car_path.display()
    );
    let processed = parse_archive_or_emit_failure(ParseArchiveStep {
        did_str: step.did_str,
        host: step.host,
        fetched: &fetched,
        archive_dir: step.archive_dir,
        parse_permits: step.parse_permits,
        claim_check: step.claim_check,
        archive_context: step.archive_context,
        archive_storage: step.archive_storage,
        parse_config: step.parse_config,
        attempt_started: step.attempt_started,
    })
    .await?;
    Ok(processed.with_get_repo_fetch(fetched.fetch_ms, fetched.spooled.bytes))
}

fn emit_get_repo_fallback(did_str: &str, host: &str, attempt_started: Instant, error: &FetchError) {
    eprintln!("falling back to listRecords for {did_str} after getRepo method wall: {error}");
    emit_smoke_telemetry(&SmokeTelemetry {
        event: "smoke_repo_attempt",
        did: did_str,
        host: Some(host),
        outcome: "running",
        stage: "get_repo_fallback_list_records",
        pressure_state: None,
        elapsed_ms: elapsed_ms(attempt_started),
        fetch_ms: None,
        parse_ms: None,
        archive_ms: None,
        bytes: None,
        records: None,
        archived_posts: None,
        decode_errors: None,
        emoji_rows: None,
        rss_kb: current_rss_kb(),
        error: Some(error.to_string()),
    });
}

fn persist_list_records_method_wall_override(
    host_override_ledger_path: Option<&Path>,
    host: &str,
    host_min_interval: Option<Duration>,
    error: &FetchError,
) {
    let Some(ledger_path) = host_override_ledger_path else {
        return;
    };
    match SqliteLedger::open(ledger_path).and_then(|ledger| {
        ledger.upsert_host_override_force_mode(
            host,
            Some(ForcedFetchMode::ListRecords),
            host_min_interval,
        )
    }) {
        Ok(()) => eprintln!(
            "recorded listRecords host override for {host} after getRepo method wall: {error}"
        ),
        Err(err) => eprintln!(
            "failed to persist listRecords host override for {host} after getRepo method wall: {err}"
        ),
    }
}

fn emit_fetch_failure(step: &FetchModeStep<'_>, failure: &FetchOneFailure, started: Instant) {
    emit_smoke_telemetry(&SmokeTelemetry {
        event: "smoke_repo_attempt",
        did: step.did_str,
        host: Some(step.host),
        outcome: outcome_name(&failure.outcome),
        stage: "fetch",
        pressure_state: None,
        elapsed_ms: elapsed_ms(step.attempt_started),
        fetch_ms: Some(elapsed_ms(started)),
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

pub(crate) fn should_fallback_get_repo_to_list_records(error: &FetchError) -> bool {
    let FetchError::HttpStatus {
        status,
        error_code,
        message,
        ..
    } = error
    else {
        return false;
    };
    if matches!(*status, 405 | 501) {
        return true;
    }
    let code_is_method_wall = error_code
        .as_deref()
        .is_some_and(is_get_repo_method_wall_text);
    let message_is_method_wall = message.as_deref().is_some_and(is_get_repo_method_wall_text);
    (*status == 429 && message_is_method_wall) || code_is_method_wall
}

fn is_get_repo_method_wall_text(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "methodnotimplemented"
            | "methodnotsupported"
            | "getrepodisabled"
            | "syncdisabled"
            | "getrepo disabled"
            | "sync disabled"
            | "method not implemented"
            | "method not supported"
    )
}
