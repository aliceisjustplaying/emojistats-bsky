//! emojistats-backfill — v2 Rust backfill CLI.
//!
//! Vertical-slice milestone in progress: `fetch-one <did>` resolves a DID to its PDS, then
//! (incrementally) fetches the repo via the streaming `getRepo` seam, proves snapshot
//! completeness, archives posts, and derives emoji rows. See `docs/backfill-v2-design.md`
//! ("First implementation milestone").

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use clap::Parser;
use emojistats_backfill::{
    archive::{
        ArchiveCommitContext, ArchiveError, CompletenessClass, FetchMethod, NormalizerVersion,
        StreamingArchiveSink, StreamingReceiptInput, archive_row_from_owned_post,
        hash_profile_record,
    },
    clickhouse::create_schema_sql,
    ledger::{
        AttemptId, AttemptOutcome, ForcedFetchMode, HostOverride, RepoLedgerEntry, RetryPolicy,
        SqliteLedger, claim_repo, complete_attempt,
    },
    list_records::{ListRecordsConfig, fetch_and_archive_list_records},
    parse::{ParseConfig, ParseVisitError, ParsedRepoSummary, parse_repo_for_did_with_state},
    scheduler::{ClaimScope, HostPacer, SharedHostPacer},
    transport::{FetchByteBudget, FetchConfig, FetchError, fetch_repo},
};
use jacquard_common::{deps::fluent_uri::Uri, types::did::Did};
use jacquard_identity::{PublicResolver, resolver::IdentityResolver};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

mod cli;
mod derive_manifest_cmd;
mod failure;
mod fleet;
mod profile_cmd;

use cli::{Cli, Command};
use derive_manifest_cmd::DeriveManifestConfig;
use failure::{
    FetchOneFailure, SmokeTelemetry, classify_archive_error, classify_fetch_error,
    classify_list_records_error, classify_parse_error, current_rss_kb, elapsed_ms,
    emit_smoke_telemetry, outcome_name, permanent_failure, retryable_failure,
};
use fleet::{FleetConfig, HostConcurrencyLimiter, HostConcurrencyPermit, default_worker_id};

const FETCH_TRANSPORT_ATTEMPTS: u8 = 3;
const FETCH_TRANSPORT_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
const FETCH_TRANSPORT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const CRAWLER_USER_AGENT: &str = "emojistats-backfill/0.1 (+https://emojistats.at)";

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

async fn fetch_one_attempt(
    did_str: &str,
    spool_dir: PathBuf,
    max_bytes: u64,
    archive_dir: PathBuf,
    archive_context: ArchiveCommitContext,
    parse_config: ParseConfig,
) -> Result<(), FetchOneFailure> {
    let claim_scope = ClaimScope::default();
    fetch_one_attempt_with_pacer(FetchOneAttemptConfig {
        did_str,
        spool_dir,
        max_bytes,
        archive_dir,
        archive_context,
        runtime: AttemptRuntime::Local { claim_scope },
        parse_config,
    })
    .await
}

struct FetchOneAttemptConfig<'a> {
    did_str: &'a str,
    spool_dir: PathBuf,
    max_bytes: u64,
    archive_dir: PathBuf,
    archive_context: ArchiveCommitContext,
    runtime: AttemptRuntime<'a>,
    parse_config: ParseConfig,
}

enum AttemptRuntime<'a> {
    Local {
        claim_scope: ClaimScope,
    },
    Fleet {
        host_pacer: SharedHostPacer,
        host_limiter: HostConcurrencyLimiter,
        parse_permits: Arc<Semaphore>,
        byte_budget: FetchByteBudget,
        claim_scope: &'a ClaimScope,
        host_override_ledger_path: &'a Path,
    },
}

impl AttemptRuntime<'_> {
    const fn claim_scope(&self) -> &ClaimScope {
        match self {
            Self::Local { claim_scope } => claim_scope,
            Self::Fleet { claim_scope, .. } => claim_scope,
        }
    }

    const fn host_override_ledger_path(&self) -> Option<&Path> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet {
                host_override_ledger_path,
                ..
            } => Some(*host_override_ledger_path),
        }
    }

    const fn host_pacer(&self) -> Option<&SharedHostPacer> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet { host_pacer, .. } => Some(host_pacer),
        }
    }

    const fn host_limiter(&self) -> Option<&HostConcurrencyLimiter> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet { host_limiter, .. } => Some(host_limiter),
        }
    }

    const fn parse_permits(&self) -> Option<&Arc<Semaphore>> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet { parse_permits, .. } => Some(parse_permits),
        }
    }

    fn byte_budget(&self) -> Option<FetchByteBudget> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet { byte_budget, .. } => Some(byte_budget.clone()),
        }
    }
}

async fn fetch_one_attempt_with_pacer(
    config: FetchOneAttemptConfig<'_>,
) -> Result<(), FetchOneFailure> {
    let attempt_started = Instant::now();
    let did_str = config.did_str;
    let did: Did = Did::new_owned(did_str)
        .map_err(|err| permanent_failure(format!("invalid DID {did_str:?}: {err}")))?;

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
        config.runtime.host_pacer(),
    )
    .await?;
    let _host_permit =
        acquire_host_fetch_permit(config.runtime.host_limiter(), &prepared_host).await?;
    let host = prepared_host.host;
    let http = repo_fetch_client().map_err(|err| {
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
            fetch_config: &fetch_config,
            archive_dir: &config.archive_dir,
            archive_context: config.archive_context,
            host_pacer: config.runtime.host_pacer(),
            parse_permits: config.runtime.parse_permits(),
            parse_config: config.parse_config,
            attempt_started,
        },
        prepared_host.fetch_mode,
    )
    .await?;
    let counts = processed.counts();
    let artifacts = processed.artifacts();
    println!(
        "parsed {} records, {} posts, {} decode errors, {} emoji rows, receipt {}",
        counts.records,
        counts.archived_posts,
        counts.decode_errors,
        counts.emoji_rows,
        artifacts.receipt_hash
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

async fn acquire_host_fetch_permit(
    host_limiter: Option<&HostConcurrencyLimiter>,
    prepared_host: &PreparedFetchHost,
) -> Result<Option<HostConcurrencyPermit>, FetchOneFailure> {
    let Some(limiter) = host_limiter else {
        return Ok(None);
    };
    limiter
        .acquire(
            prepared_host.host.as_str(),
            prepared_host
                .host_override
                .as_ref()
                .and_then(|override_record| override_record.concurrency_cap),
        )
        .await
}

struct FetchModeStep<'a> {
    http: &'a reqwest::Client,
    pds: &'a Uri<String>,
    did: &'a Did,
    did_str: &'a str,
    host: &'a str,
    fetch_config: &'a FetchConfig,
    archive_dir: &'a Path,
    archive_context: ArchiveCommitContext,
    host_pacer: Option<&'a SharedHostPacer>,
    parse_permits: Option<&'a Arc<Semaphore>>,
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
                archive_dir: step.archive_dir,
                archive_context: step.archive_context,
                host_pacer: step.host_pacer,
                attempt_started: step.attempt_started,
            })
            .await
        }
    }
}

async fn fetch_get_repo_and_archive(
    step: FetchModeStep<'_>,
) -> Result<ProcessedRepo, FetchOneFailure> {
    let fetched = fetch_spooled_repo(FetchStep {
        http: step.http,
        pds: step.pds,
        did: step.did,
        did_str: step.did_str,
        host: step.host,
        config: step.fetch_config,
        host_pacer: step.host_pacer,
        attempt_started: step.attempt_started,
    })
    .await?;
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
        archive_context: step.archive_context,
        parse_config: step.parse_config,
        attempt_started: step.attempt_started,
    })
    .await?;
    Ok(processed.with_get_repo_fetch(fetched.fetch_ms, fetched.spooled.bytes))
}

struct FetchStep<'a> {
    http: &'a reqwest::Client,
    pds: &'a Uri<String>,
    did: &'a Did,
    did_str: &'a str,
    host: &'a str,
    config: &'a FetchConfig,
    host_pacer: Option<&'a SharedHostPacer>,
    attempt_started: Instant,
}

struct FetchedRepo {
    spooled: emojistats_backfill::transport::SpooledRepo,
    fetch_ms: u64,
}

async fn fetch_spooled_repo(step: FetchStep<'_>) -> Result<FetchedRepo, FetchOneFailure> {
    let fetch_started = Instant::now();
    let mut attempt = 1_u8;
    loop {
        match fetch_repo(step.http, step.pds, step.did, step.config).await {
            Ok(spooled) => {
                record_rate_limit_snapshot(
                    step.host_pacer,
                    step.host,
                    &spooled.rate_limit,
                    SystemTime::now(),
                );
                return Ok(FetchedRepo {
                    spooled,
                    fetch_ms: elapsed_ms(fetch_started),
                });
            }
            Err(err)
                if is_retryable_stream_fetch_error(&err) && attempt < FETCH_TRANSPORT_ATTEMPTS =>
            {
                let delay = transport_retry_delay(step.did_str, attempt);
                eprintln!(
                    "fetch retry {next_attempt}/{max_attempts} for {did} after {delay_ms} ms: {err}",
                    next_attempt = attempt.saturating_add(1),
                    max_attempts = FETCH_TRANSPORT_ATTEMPTS,
                    did = step.did_str,
                    delay_ms = delay.as_millis()
                );
                tokio::time::sleep(delay).await;
                attempt = attempt.saturating_add(1);
            }
            Err(err) => {
                let failure = classify_fetch_error(step.did_str, &err);
                emit_smoke_telemetry(&SmokeTelemetry {
                    event: "smoke_repo_attempt",
                    did: step.did_str,
                    host: Some(step.host),
                    outcome: outcome_name(&failure.outcome),
                    stage: "fetch",
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
                record_rate_limit_cooldown(step.host_pacer, step.host, &failure);
                return Err(failure);
            }
        }
    }
}

fn repo_fetch_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .user_agent(CRAWLER_USER_AGENT)
        .http1_only()
        .tcp_keepalive(Duration::from_secs(60))
        .connect_timeout(Duration::from_secs(30))
        .build()
}

fn transport_retry_delay(did: &str, failed_attempt: u8) -> Duration {
    let exponent = u32::from(failed_attempt.saturating_sub(1));
    let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
    let base = FETCH_TRANSPORT_RETRY_BASE_DELAY
        .checked_mul(multiplier)
        .unwrap_or(FETCH_TRANSPORT_RETRY_MAX_DELAY)
        .min(FETCH_TRANSPORT_RETRY_MAX_DELAY);
    base.checked_add(transport_retry_jitter(did, failed_attempt, base))
        .unwrap_or(FETCH_TRANSPORT_RETRY_MAX_DELAY)
        .min(FETCH_TRANSPORT_RETRY_MAX_DELAY)
}

fn transport_retry_jitter(did: &str, failed_attempt: u8, base: Duration) -> Duration {
    let window_millis = u64::try_from(base.as_millis() / 2).unwrap_or(u64::MAX);
    if window_millis == 0 {
        return Duration::ZERO;
    }
    let modulus = window_millis.saturating_add(1);
    let mut hasher = Sha256::new();
    hasher.update(did.as_bytes());
    hasher.update([failed_attempt]);
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    for (destination, source) in bytes.iter_mut().zip(digest) {
        *destination = source;
    }
    let jitter_millis = u64::from_be_bytes(bytes).checked_rem(modulus).unwrap_or(0);
    Duration::from_millis(jitter_millis)
}

const fn is_retryable_stream_fetch_error(error: &FetchError) -> bool {
    matches!(
        error,
        FetchError::Transport { .. } | FetchError::InactivityTimeout { .. }
    )
}

fn record_rate_limit_cooldown(
    host_pacer: Option<&SharedHostPacer>,
    host: &str,
    failure: &FetchOneFailure,
) {
    if let AttemptOutcome::RateLimited { retry_after } = &failure.outcome
        && let Some(pacer) = host_pacer
        && let Err(pacer_error) = HostPacer::record_retry_after(pacer, host, *retry_after)
    {
        eprintln!("failed to record host cooldown for {host}: {pacer_error}");
    }
}

fn record_rate_limit_snapshot(
    host_pacer: Option<&SharedHostPacer>,
    host: &str,
    rate_limit: &emojistats_backfill::transport::RateLimitSnapshot,
    observed_at: SystemTime,
) {
    if let Some(pacer) = host_pacer
        && let Err(pacer_error) = HostPacer::record_rate_limit(pacer, host, rate_limit, observed_at)
    {
        eprintln!("failed to record host rate-limit snapshot for {host}: {pacer_error}");
    }
}

struct ParseArchiveStep<'a> {
    did_str: &'a str,
    host: &'a str,
    fetched: &'a FetchedRepo,
    archive_dir: &'a Path,
    parse_permits: Option<&'a Arc<Semaphore>>,
    archive_context: ArchiveCommitContext,
    parse_config: ParseConfig,
    attempt_started: Instant,
}

async fn parse_archive_or_emit_failure(
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
    let parse_config = step.parse_config;
    let parsed = tokio::task::spawn_blocking(move || {
        parse_and_archive_spooled_repo(&did, &car_path, &archive_dir, archive_context, parse_config)
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

#[derive(Debug, Clone)]
struct ProcessedRepoCounts {
    records: u64,
    archived_posts: u64,
    decode_errors: u64,
    emoji_rows: u64,
}

#[derive(Debug, Clone)]
struct ProcessedRepoArtifacts {
    receipt_hash: String,
    parquet_path: PathBuf,
    receipt_path: PathBuf,
    manifest_path: PathBuf,
    emoji_projection_path: PathBuf,
}

#[derive(Debug, Clone)]
struct GetRepoTimings {
    fetch_ms: Option<u64>,
    bytes: Option<u64>,
    parse_ms: u64,
    parse_index_ms: u64,
    parse_commit_ms: u64,
    parse_walk_ms: u64,
    archive_ms: u64,
}

#[derive(Debug, Clone)]
struct ListRecordsTimings {
    fetch_ms: u64,
    archive_ms: u64,
}

#[derive(Debug, Clone)]
struct GetRepoProcessed {
    counts: ProcessedRepoCounts,
    artifacts: ProcessedRepoArtifacts,
    timings: GetRepoTimings,
}

#[derive(Debug, Clone)]
struct ListRecordsProcessed {
    counts: ProcessedRepoCounts,
    artifacts: ProcessedRepoArtifacts,
    timings: ListRecordsTimings,
}

#[derive(Debug, Clone)]
enum ProcessedRepo {
    GetRepo(GetRepoProcessed),
    ListRecords(ListRecordsProcessed),
}

impl ProcessedRepo {
    const fn counts(&self) -> &ProcessedRepoCounts {
        match self {
            Self::GetRepo(processed) => &processed.counts,
            Self::ListRecords(processed) => &processed.counts,
        }
    }

    const fn artifacts(&self) -> &ProcessedRepoArtifacts {
        match self {
            Self::GetRepo(processed) => &processed.artifacts,
            Self::ListRecords(processed) => &processed.artifacts,
        }
    }

    const fn fetch_ms_opt(&self) -> Option<u64> {
        match self {
            Self::GetRepo(processed) => processed.timings.fetch_ms,
            Self::ListRecords(processed) => Some(processed.timings.fetch_ms),
        }
    }

    const fn bytes(&self) -> Option<u64> {
        match self {
            Self::GetRepo(processed) => processed.timings.bytes,
            Self::ListRecords(_) => None,
        }
    }

    const fn parse_ms(&self) -> Option<u64> {
        match self {
            Self::GetRepo(processed) => Some(processed.timings.parse_ms),
            Self::ListRecords(_) => None,
        }
    }

    const fn archive_ms(&self) -> u64 {
        match self {
            Self::GetRepo(processed) => processed.timings.archive_ms,
            Self::ListRecords(processed) => processed.timings.archive_ms,
        }
    }

    const fn get_repo_timings(&self) -> Option<&GetRepoTimings> {
        match self {
            Self::GetRepo(processed) => Some(&processed.timings),
            Self::ListRecords(_) => None,
        }
    }

    const fn with_get_repo_fetch(mut self, fetch_ms: u64, bytes: u64) -> Self {
        if let Self::GetRepo(processed) = &mut self {
            processed.timings.fetch_ms = Some(fetch_ms);
            processed.timings.bytes = Some(bytes);
        }
        self
    }
}

struct ListRecordsStep<'a> {
    http: &'a reqwest::Client,
    pds: &'a Uri<String>,
    did: &'a Did,
    did_str: &'a str,
    host: &'a str,
    archive_dir: &'a Path,
    archive_context: ArchiveCommitContext,
    host_pacer: Option<&'a SharedHostPacer>,
    attempt_started: Instant,
}

async fn fetch_archive_list_records_or_emit_failure(
    step: ListRecordsStep<'_>,
) -> Result<ProcessedRepo, FetchOneFailure> {
    let fetch_started = Instant::now();
    emit_list_records_running(&step);
    match fetch_and_archive_list_records(
        step.http,
        step.pds,
        step.did,
        step.did_str,
        step.archive_dir,
        step.archive_context.clone(),
        ListRecordsConfig::default(),
    )
    .await
    {
        Ok(output) => {
            for rate_limit in &output.rate_limits {
                record_rate_limit_snapshot(
                    step.host_pacer,
                    step.host,
                    rate_limit,
                    SystemTime::now(),
                );
            }
            let processed = ProcessedRepo::ListRecords(ListRecordsProcessed {
                counts: ProcessedRepoCounts {
                    records: output.records,
                    archived_posts: output.archived_posts,
                    decode_errors: output.decode_errors,
                    emoji_rows: output.artifacts.emoji_rows,
                },
                artifacts: ProcessedRepoArtifacts {
                    receipt_hash: output.receipt.post_rows_hash,
                    parquet_path: output.artifacts.parquet_path,
                    receipt_path: output.artifacts.receipt_path,
                    manifest_path: output.artifacts.manifest_path,
                    emoji_projection_path: output.artifacts.emoji_projection_path,
                },
                timings: ListRecordsTimings {
                    fetch_ms: elapsed_ms(fetch_started),
                    archive_ms: elapsed_ms(fetch_started),
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

struct ArchiveRunState {
    sink: StreamingArchiveSink,
    archive_row_ns: u128,
    sink_push_ns: u128,
    profiled_posts: u64,
}

fn parse_and_archive_spooled_repo(
    did_str: &str,
    car_path: &Path,
    archive_dir: &Path,
    archive_context: ArchiveCommitContext,
    parse_config: ParseConfig,
) -> Result<ProcessedRepo, FetchOneFailure> {
    let parse_started = Instant::now();
    let sink = StreamingArchiveSink::new(archive_dir, did_str, archive_context).map_err(|err| {
        classify_archive_error(&format!("open streaming archive sink for {did_str}"), &err)
    })?;

    let normalizer = sink.normalizer().clone();
    let did = did_str.to_owned();
    let state = ArchiveRunState {
        sink,
        archive_row_ns: 0,
        sink_push_ns: 0,
        profiled_posts: 0,
    };
    let (parsed, state) = if std::env::var_os("EMOJISTATS_PROFILE_STAGES").is_some() {
        parse_repo_streaming_archive_profiled(
            car_path,
            did_str,
            parse_config,
            state,
            did,
            normalizer,
        )
    } else {
        parse_repo_streaming_archive_unprofiled(
            car_path,
            did_str,
            parse_config,
            state,
            did,
            normalizer,
        )
    }
    .map_err(|err| match err {
        ParseVisitError::Parse(err) => classify_parse_error(did_str, &err),
        ParseVisitError::Visit(err) => {
            classify_archive_error(&format!("stream archive row for {did_str}"), &err)
        }
    })?;
    let parse_ms = elapsed_ms(parse_started);
    let sink = state.sink;
    let archive_started = Instant::now();
    let profile_row_hash = hash_profile_record(parsed.profile.as_ref())
        .map_err(|err| classify_archive_error(&format!("hash profile row for {did_str}"), &err))?;
    let (receipt, artifacts) = sink
        .finish(
            StreamingReceiptInput {
                fetch_method: FetchMethod::GetRepo,
                completeness_class: CompletenessClass::SnapshotComplete,
                reachable_records_count: parsed.rkey_digest.all_records_count,
                reachable_post_records_count: parsed.rkey_digest.post_records_count,
                post_decode_error_count: parsed.post_decode_error_count,
                profile_row_hash,
                mst_root_cid: Some(parsed.commit.data.clone()),
                commit_cid: Some(parsed.commit.cid.clone()),
            },
            parsed.profile.as_ref(),
        )
        .map_err(|err| {
            classify_archive_error(&format!("finish archive artifacts for {did_str}"), &err)
        })?;
    Ok(ProcessedRepo::GetRepo(GetRepoProcessed {
        counts: ProcessedRepoCounts {
            records: parsed.rkey_digest.all_records_count,
            archived_posts: receipt.archived_post_rows_count,
            decode_errors: parsed.record_decode_error_count,
            emoji_rows: artifacts.emoji_rows,
        },
        artifacts: ProcessedRepoArtifacts {
            receipt_hash: receipt.post_rows_hash,
            parquet_path: artifacts.parquet_path,
            receipt_path: artifacts.receipt_path,
            manifest_path: artifacts.manifest_path,
            emoji_projection_path: artifacts.emoji_projection_path,
        },
        timings: GetRepoTimings {
            fetch_ms: None,
            bytes: None,
            parse_ms,
            parse_index_ms: parsed.timings.index_ms,
            parse_commit_ms: parsed.timings.commit_ms,
            parse_walk_ms: parsed.timings.walk_ms,
            archive_ms: elapsed_ms(archive_started),
        },
    }))
}

fn parse_repo_streaming_archive_unprofiled(
    car_path: &Path,
    did_str: &str,
    parse_config: ParseConfig,
    state: ArchiveRunState,
    did: String,
    normalizer: NormalizerVersion,
) -> Result<(ParsedRepoSummary, ArchiveRunState), ParseVisitError<ArchiveError>> {
    parse_repo_for_did_with_state(
        car_path,
        did_str,
        parse_config,
        state,
        move |state, post| {
            let row = archive_row_from_owned_post(&did, post, &normalizer)?;
            state.sink.push_row(row)
        },
    )
}

fn parse_repo_streaming_archive_profiled(
    car_path: &Path,
    did_str: &str,
    parse_config: ParseConfig,
    state: ArchiveRunState,
    did: String,
    normalizer: NormalizerVersion,
) -> Result<(ParsedRepoSummary, ArchiveRunState), ParseVisitError<ArchiveError>> {
    let (summary, state) = parse_repo_for_did_with_state(
        car_path,
        did_str,
        parse_config,
        state,
        move |state, post| {
            let archive_row_started = Instant::now();
            let row = archive_row_from_owned_post(&did, post, &normalizer)?;
            state.archive_row_ns = state
                .archive_row_ns
                .saturating_add(archive_row_started.elapsed().as_nanos());
            let sink_push_started = Instant::now();
            let result = state.sink.push_row(row);
            state.sink_push_ns = state
                .sink_push_ns
                .saturating_add(sink_push_started.elapsed().as_nanos());
            state.profiled_posts = state.profiled_posts.saturating_add(1);
            result
        },
    )?;
    eprintln!(
        "stage_profile posts={} archive_row_ms={} sink_push_ms={}",
        state.profiled_posts,
        state.archive_row_ns / 1_000_000,
        state.sink_push_ns / 1_000_000
    );
    Ok((summary, state))
}

async fn prepare_fetch_host(
    did_str: &str,
    pds: &Uri<String>,
    claim_scope: &ClaimScope,
    host_override_ledger_path: Option<&Path>,
    host_pacer: Option<&SharedHostPacer>,
) -> Result<PreparedFetchHost, FetchOneFailure> {
    if !claim_scope.includes_did(did_str) {
        return Err(retryable_failure(format!(
            "DID {did_str} is outside configured shard scope"
        )));
    }
    let host = pds_host_key(pds);
    let host_override = load_host_override(host_override_ledger_path, &host)?;
    let fetch_mode = fetch_mode_for_host(&host, host_override.as_ref(), SystemTime::now())?;
    if let Some(pacer) = host_pacer {
        HostPacer::wait_until_ready(pacer, &host)
            .await
            .map_err(|err| retryable_failure(format!("host pacing for {host}: {err}")))?;
    }
    Ok(PreparedFetchHost {
        host,
        host_override,
        fetch_mode,
    })
}

#[derive(Debug)]
struct PreparedFetchHost {
    host: String,
    host_override: Option<HostOverride>,
    fetch_mode: ForcedFetchMode,
}

fn pds_host_key(pds: &Uri<String>) -> String {
    pds.authority().map_or_else(
        || pds.as_str().to_owned(),
        |authority| authority.host().to_owned(),
    )
}

fn load_host_override(
    ledger_path: Option<&Path>,
    host: &str,
) -> Result<Option<HostOverride>, FetchOneFailure> {
    let Some(ledger_path) = ledger_path else {
        return Ok(None);
    };
    let ledger = SqliteLedger::open(ledger_path)
        .map_err(|err| retryable_failure(format!("open ledger for host override {host}: {err}")))?;
    ledger
        .load_host_override(host)
        .map_err(|err| retryable_failure(format!("load host override for {host}: {err}")))
}

fn fetch_mode_for_host(
    host: &str,
    host_override: Option<&HostOverride>,
    now: SystemTime,
) -> Result<ForcedFetchMode, FetchOneFailure> {
    let Some(host_override) = host_override else {
        return Ok(ForcedFetchMode::GetRepo);
    };
    if host_override.disabled {
        if let Some(revive_after) = host_override.revive_after
            && let Ok(retry_after) = revive_after.duration_since(now)
        {
            return Err(FetchOneFailure {
                outcome: AttemptOutcome::RateLimited { retry_after },
                error: anyhow::anyhow!("host {host} disabled by override until {revive_after:?}"),
            });
        }
        if host_override.revive_after.is_none() {
            return Err(retryable_failure(format!(
                "host {host} disabled by override"
            )));
        }
    }
    Ok(host_override.force_mode.unwrap_or(ForcedFetchMode::GetRepo))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]

    use std::{
        fs,
        path::PathBuf,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use clap::Parser;
    use emojistats_backfill::{
        ledger::{
            AttemptId, AttemptOutcome, ForcedFetchMode, HostOverride, RepoLedgerEntry,
            RepoLedgerStatus, ShardFilter, SqliteLedger, claim_repo_with_lease, did_shard_bucket,
        },
        parse::default_cid_verification_threads,
        scheduler::ClaimScope,
    };
    use jacquard_common::deps::fluent_uri::Uri;

    use super::{
        Cli, Command, fetch_mode_for_host, load_host_override, pds_host_key, prepare_fetch_host,
    };
    use crate::fleet::{
        HostConcurrencyLimiter, SeedSummary, claim_batch_limit, claimable_entries_for_scope,
        recover_stale_claimed_entries, seed_ledger_from_file,
    };

    #[test]
    fn parses_fetch_one_did() {
        let cli =
            Cli::try_parse_from(["emojistats-backfill", "fetch-one", "did:plc:abc123"]).unwrap();
        let Command::FetchOne {
            did,
            spool_dir,
            max_bytes,
            archive_dir,
            cid_verification_threads,
        } = cli.command
        else {
            unreachable!("expected fetch-one command");
        };
        assert_eq!(did, "did:plc:abc123");
        assert_eq!(spool_dir, PathBuf::from("data/spool"));
        assert_eq!(max_bytes, 2_147_483_648);
        assert_eq!(archive_dir, PathBuf::from("data/archive"));
        assert_eq!(cid_verification_threads, default_cid_verification_threads());
    }

    #[tokio::test]
    async fn host_concurrency_cap_serializes_same_host() {
        let limiter = HostConcurrencyLimiter::default();
        let first = limiter
            .acquire("pds.example.com", Some(1))
            .await
            .unwrap()
            .unwrap();
        let blocked = tokio::time::timeout(
            Duration::from_millis(10),
            limiter.acquire("pds.example.com", Some(1)),
        )
        .await;
        assert!(blocked.is_err());
        drop(blocked);
        drop(first);

        let second = tokio::time::timeout(
            Duration::from_secs(1),
            limiter.acquire("pds.example.com", Some(1)),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        drop(second);
    }

    #[tokio::test]
    async fn host_concurrency_cap_resizes_for_future_acquires() {
        let limiter = HostConcurrencyLimiter::default();
        let first = limiter
            .acquire("pds.example.com", Some(2))
            .await
            .unwrap()
            .unwrap();
        let second = limiter
            .acquire("pds.example.com", Some(2))
            .await
            .unwrap()
            .unwrap();

        let blocked = tokio::time::timeout(
            Duration::from_millis(10),
            limiter.acquire("pds.example.com", Some(1)),
        )
        .await;
        assert!(blocked.is_err());
        drop(blocked);

        let third = tokio::time::timeout(
            Duration::from_secs(1),
            limiter.acquire("pds.example.com", Some(3)),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        drop(third);
        drop(second);
        drop(first);
    }

    #[test]
    fn requires_a_subcommand() {
        assert!(Cli::try_parse_from(["emojistats-backfill"]).is_err());
    }

    #[test]
    fn parses_run_fleet_defaults() {
        let cli = Cli::try_parse_from(["emojistats-backfill", "run-fleet", "dids.txt"]).unwrap();
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
            cid_verification_threads,
        } = cli.command
        else {
            unreachable!("expected run-fleet command");
        };
        assert_eq!(dids_file, PathBuf::from("dids.txt"));
        assert_eq!(ledger_path, PathBuf::from("data/ledger/backfill.sqlite"));
        assert_eq!(run_id, "fleet-local");
        assert_eq!(claim_limit, 1);
        assert_eq!(concurrency, 4);
        assert_eq!(parse_concurrency, 1);
        assert_eq!(max_inflight_spool_bytes, 536_870_912);
        assert_eq!(shard_bucket, None);
        assert_eq!(spool_dir, PathBuf::from("data/spool"));
        assert_eq!(max_bytes, 2_147_483_648);
        assert_eq!(archive_dir, PathBuf::from("data/archive"));
        assert_eq!(cid_verification_threads, default_cid_verification_threads());
    }

    #[test]
    fn parses_run_fleet_resource_options() {
        let cli = Cli::try_parse_from([
            "emojistats-backfill",
            "run-fleet",
            "dids.txt",
            "--parse-concurrency",
            "2",
            "--max-inflight-spool-bytes",
            "123456",
            "--cid-verification-threads",
            "7",
        ])
        .unwrap();
        let Command::RunFleet {
            parse_concurrency,
            max_inflight_spool_bytes,
            cid_verification_threads,
            ..
        } = cli.command
        else {
            unreachable!("expected run-fleet command");
        };

        assert_eq!(parse_concurrency, 2);
        assert_eq!(max_inflight_spool_bytes, 123_456);
        assert_eq!(cid_verification_threads, 7);
    }

    #[test]
    fn parses_run_fleet_shard_bucket() {
        let cli = Cli::try_parse_from([
            "emojistats-backfill",
            "run-fleet",
            "dids.txt",
            "--shard-bucket",
            "3",
        ])
        .unwrap();
        let Command::RunFleet { shard_bucket, .. } = cli.command else {
            unreachable!("expected run-fleet command");
        };

        assert_eq!(shard_bucket, Some(ShardFilter::new(3).unwrap()));
    }

    #[test]
    fn parses_derive_manifest_defaults() {
        let cli = Cli::try_parse_from(["emojistats-backfill", "derive-manifest", "manifest.jsonl"])
            .unwrap();
        let Command::DeriveManifest {
            manifest_path,
            archive_root,
            clickhouse_url,
            clickhouse_database,
            clickhouse_user,
            clickhouse_password,
            dry_run,
        } = cli.command
        else {
            unreachable!("expected derive-manifest command");
        };

        assert_eq!(manifest_path, PathBuf::from("manifest.jsonl"));
        assert_eq!(archive_root, PathBuf::from("data/archive"));
        assert_eq!(clickhouse_url, "http://localhost:8123");
        assert_eq!(clickhouse_database, "emojistats");
        assert_eq!(clickhouse_user, "default");
        assert_eq!(clickhouse_password, "");
        assert!(!dry_run);
    }

    #[test]
    fn parses_derive_manifest_clickhouse_options() {
        let cli = Cli::try_parse_from([
            "emojistats-backfill",
            "derive-manifest",
            "manifest.jsonl",
            "--archive-root",
            "archive",
            "--clickhouse-url",
            "http://127.0.0.1:8123",
            "--clickhouse-database",
            "analytics",
            "--clickhouse-user",
            "writer",
            "--clickhouse-password",
            "secret",
            "--dry-run",
        ])
        .unwrap();
        let Command::DeriveManifest {
            archive_root,
            clickhouse_url,
            clickhouse_database,
            clickhouse_user,
            clickhouse_password,
            dry_run,
            ..
        } = cli.command
        else {
            unreachable!("expected derive-manifest command");
        };

        assert_eq!(archive_root, PathBuf::from("archive"));
        assert_eq!(clickhouse_url, "http://127.0.0.1:8123");
        assert_eq!(clickhouse_database, "analytics");
        assert_eq!(clickhouse_user, "writer");
        assert_eq!(clickhouse_password, "secret");
        assert!(dry_run);
    }

    #[test]
    fn parses_clickhouse_schema_defaults() {
        let cli = Cli::try_parse_from(["emojistats-backfill", "clickhouse-schema"]).unwrap();
        let Command::ClickhouseSchema {
            clickhouse_database,
        } = cli.command
        else {
            unreachable!("expected clickhouse-schema command");
        };

        assert_eq!(clickhouse_database, "emojistats");
    }

    #[test]
    fn parses_clickhouse_schema_database() {
        let cli = Cli::try_parse_from([
            "emojistats-backfill",
            "clickhouse-schema",
            "--clickhouse-database",
            "analytics",
        ])
        .unwrap();
        let Command::ClickhouseSchema {
            clickhouse_database,
        } = cli.command
        else {
            unreachable!("expected clickhouse-schema command");
        };

        assert_eq!(clickhouse_database, "analytics");
    }

    #[test]
    fn run_fleet_rejects_out_of_range_shard_bucket() {
        assert!(
            Cli::try_parse_from([
                "emojistats-backfill",
                "run-fleet",
                "dids.txt",
                "--shard-bucket",
                "8",
            ])
            .is_err()
        );
    }

    #[test]
    fn run_fleet_rejects_zero_claim_limit() {
        assert!(
            Cli::try_parse_from([
                "emojistats-backfill",
                "run-fleet",
                "dids.txt",
                "--claim-limit",
                "0",
            ])
            .is_err()
        );
    }

    #[test]
    fn run_fleet_rejects_zero_concurrency() {
        assert!(
            Cli::try_parse_from([
                "emojistats-backfill",
                "run-fleet",
                "dids.txt",
                "--concurrency",
                "0",
            ])
            .is_err()
        );
    }

    #[test]
    fn run_fleet_rejects_zero_parse_concurrency() {
        assert!(
            Cli::try_parse_from([
                "emojistats-backfill",
                "run-fleet",
                "dids.txt",
                "--parse-concurrency",
                "0",
            ])
            .is_err()
        );
    }

    #[test]
    fn run_fleet_rejects_zero_inflight_spool_bytes() {
        assert!(
            Cli::try_parse_from([
                "emojistats-backfill",
                "run-fleet",
                "dids.txt",
                "--max-inflight-spool-bytes",
                "0",
            ])
            .is_err()
        );
    }

    #[test]
    fn claim_batch_is_bounded_by_free_slots_and_remaining_limit() {
        assert_eq!(claim_batch_limit(4, 2, 10).unwrap(), 2);
        assert_eq!(claim_batch_limit(4, 0, 3).unwrap(), 3);
    }

    #[test]
    fn claimable_entries_for_scope_uses_shard_filter() {
        let store = SqliteLedger::open_in_memory().unwrap();
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        let target_did = "did:plc:target";
        let target_bucket = did_shard_bucket(target_did);
        let mut other_did = "did:plc:other0".to_owned();
        let mut suffix = 1_u32;
        while did_shard_bucket(&other_did) == target_bucket {
            other_did = format!("did:plc:other{suffix}");
            suffix = suffix.checked_add(1).unwrap();
        }
        let target = RepoLedgerEntry::pending(target_did);
        let other = RepoLedgerEntry::pending(&other_did);
        store.upsert_entry(&other).unwrap();
        store.upsert_entry(&target).unwrap();
        let scope = ClaimScope {
            shard_filter: Some(ShardFilter::new(target_bucket).unwrap()),
        };

        let claimable = claimable_entries_for_scope(&store, now, 10, &scope).unwrap();

        assert_eq!(claimable, vec![target]);
    }

    #[test]
    fn persisted_host_override_loads_by_resolved_pds_host() {
        let store = SqliteLedger::open_in_memory().unwrap();
        let db_path = temp_file_path("host-overrides").with_extension("sqlite");
        drop(store);
        let store = SqliteLedger::open(&db_path).unwrap();
        let override_record = HostOverride {
            host: "pds.example.com".to_owned(),
            disabled: false,
            concurrency_cap: None,
            revive_after: None,
            force_mode: Some(ForcedFetchMode::ListRecords),
        };
        store.upsert_host_override(&override_record).unwrap();
        drop(store);
        let pds = Uri::parse("https://pds.example.com").unwrap().to_owned();
        let host = pds_host_key(&pds);

        let loaded = load_host_override(Some(&db_path), &host).unwrap();

        assert_eq!(loaded, Some(override_record));
        fs::remove_file(db_path).unwrap();
    }

    #[tokio::test]
    async fn forced_list_records_host_preparation_is_allowed() {
        let db_path = temp_file_path("host-overrides-list-records").with_extension("sqlite");
        let store = SqliteLedger::open(&db_path).unwrap();
        store
            .upsert_host_override(&HostOverride {
                host: "pds.example.com".to_owned(),
                disabled: false,
                concurrency_cap: None,
                revive_after: None,
                force_mode: Some(ForcedFetchMode::ListRecords),
            })
            .unwrap();
        drop(store);
        let pds = Uri::parse("https://pds.example.com").unwrap().to_owned();

        let prepared = prepare_fetch_host(
            "did:plc:target",
            &pds,
            &ClaimScope::default(),
            Some(&db_path),
            None,
        )
        .await
        .unwrap();

        assert_eq!(prepared.fetch_mode, ForcedFetchMode::ListRecords);
        fs::remove_file(db_path).unwrap();
    }

    #[test]
    fn host_override_force_mode_and_disable_are_applied() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        let list_records = HostOverride {
            host: "pds.example.com".to_owned(),
            disabled: false,
            concurrency_cap: None,
            revive_after: None,
            force_mode: Some(ForcedFetchMode::ListRecords),
        };
        let disabled = HostOverride {
            host: "pds.example.com".to_owned(),
            disabled: true,
            concurrency_cap: None,
            revive_after: Some(now + Duration::from_secs(30)),
            force_mode: Some(ForcedFetchMode::GetRepo),
        };

        assert_eq!(
            fetch_mode_for_host("pds.example.com", Some(&list_records), now).unwrap(),
            ForcedFetchMode::ListRecords
        );
        let failure = fetch_mode_for_host("pds.example.com", Some(&disabled), now).unwrap_err();
        assert_eq!(
            failure.outcome,
            AttemptOutcome::RateLimited {
                retry_after: Duration::from_secs(30)
            }
        );
    }

    #[test]
    fn seed_ledger_from_file_inserts_only_missing_dids() {
        let store = SqliteLedger::open_in_memory().unwrap();
        let existing = RepoLedgerEntry {
            did: "did:plc:existing".to_owned(),
            status: RepoLedgerStatus::Succeeded,
            attempts: 1,
            next_attempt_after: None,
            last_attempt: None,
            last_error: None,
            worker_id: None,
            claimed_at: None,
            lease_until: None,
        };
        store.upsert_entry(&existing).unwrap();
        let dids_file = temp_file_path("seed-ledger");
        fs::write(
            &dids_file,
            "\ndid:plc:existing\ndid:plc:newrepo\ndid:plc:newrepo\n",
        )
        .unwrap();

        let summary = seed_ledger_from_file(&store, &dids_file).unwrap();

        assert_eq!(
            summary,
            SeedSummary {
                inserted: 1,
                existing: 2,
                blank: 1
            }
        );
        assert_eq!(
            store.load_entry("did:plc:existing").unwrap(),
            Some(existing)
        );
        assert_eq!(
            store.load_entry("did:plc:newrepo").unwrap().unwrap().status,
            RepoLedgerStatus::Pending
        );

        fs::remove_file(dids_file).unwrap();
    }

    #[test]
    fn stale_claimed_entries_from_seed_file_requeue_on_startup() {
        let store = SqliteLedger::open_in_memory().unwrap();
        let now = UNIX_EPOCH + std::time::Duration::from_secs(1_000);
        let pending = RepoLedgerEntry::pending("did:plc:stale");
        let claimed = claim_repo_with_lease(
            &pending,
            AttemptId::new("previous-run", "did:plc:stale", 1),
            now - Duration::from_secs(120),
            "previous-worker",
            Duration::from_secs(60),
        )
        .unwrap();
        store.upsert_entry(&claimed).unwrap();
        let dids_file = temp_file_path("stale-claimed");
        fs::write(&dids_file, "did:plc:stale\n").unwrap();

        let recovered = recover_stale_claimed_entries(&store, &dids_file, now).unwrap();
        let entry = store.load_entry("did:plc:stale").unwrap().unwrap();

        assert_eq!(recovered, 1);
        assert_eq!(entry.status, RepoLedgerStatus::RetryableFailure);
        assert!(entry.can_claim_at(now));
        assert_eq!(
            entry.last_error,
            Some("expired claimed lease at fleet startup".to_owned())
        );

        fs::remove_file(dids_file).unwrap();
    }

    fn temp_file_path(name: &str) -> PathBuf {
        let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        std::env::temp_dir().join(format!(
            "emojistats-backfill-{name}-{}-{}.txt",
            std::process::id(),
            since_epoch.as_nanos()
        ))
    }
}
