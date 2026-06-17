#![allow(clippy::redundant_pub_crate)]

use jacquard_identity::resolver::IdentityResolver;

use super::{
    super::{
        Arc, ArchiveCommitContext, ArchiveStorageConfig, AttemptOutcome, CRAWLER_USER_AGENT,
        ClaimScope, DEFAULT_HOST_CONCURRENCY_CAP, Did, Digest, Duration, FETCH_TRANSPORT_ATTEMPTS,
        FETCH_TRANSPORT_RETRY_BASE_DELAY, FETCH_TRANSPORT_RETRY_MAX_DELAY, FetchByteBudget,
        FetchConfig, FetchError, FetchOneFailure, ForcedFetchMode, HashMap, HostConcurrencyLimiter,
        HostConcurrencyPermit, HostOverride, HostPacer, HttpProtocol, Instant, ListRecordsConfig,
        Mutex, ParseConfig, Path, PathBuf, PublicResolver, RepoLedgerEntry, Semaphore, Sha256,
        SharedHostPacer, SmokeTelemetry, SystemTime, Uri, classify_fetch_error,
        classify_list_records_error, current_rss_kb, elapsed_ms, emit_smoke_telemetry,
        fetch_and_archive_list_records_with_rate_limit_observer, fetch_repo, outcome_name,
        permanent_failure, retryable_failure,
    },
    archive_host::{
        ArchiveClaimCheck, PreparedFetchHost, parse_and_archive_spooled_repo, prepare_fetch_host,
    },
};

pub(crate) struct LocalFetchOneAttemptConfig<'a> {
    pub(crate) did_str: &'a str,
    pub(crate) spool_dir: PathBuf,
    pub(crate) max_bytes: u64,
    pub(crate) archive_dir: PathBuf,
    pub(crate) archive_context: ArchiveCommitContext,
    pub(crate) archive_storage: ArchiveStorageConfig,
    pub(crate) parse_config: ParseConfig,
    pub(crate) http_protocol: HttpProtocol,
}

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

pub(crate) struct FetchOneAttemptConfig<'a> {
    pub(crate) did_str: &'a str,
    pub(crate) spool_dir: PathBuf,
    pub(crate) max_bytes: u64,
    pub(crate) archive_dir: PathBuf,
    pub(crate) archive_context: ArchiveCommitContext,
    pub(crate) archive_storage: ArchiveStorageConfig,
    pub(crate) runtime: AttemptRuntime<'a>,
    pub(crate) parse_config: ParseConfig,
    pub(crate) http_protocol: HttpProtocol,
}

pub(crate) enum AttemptRuntime<'a> {
    Local {
        claim_scope: ClaimScope,
    },
    Fleet {
        host_pacer: SharedHostPacer,
        host_limiter: HostConcurrencyLimiter,
        parse_permits: Arc<Semaphore>,
        byte_budget: FetchByteBudget,
        claimed: Box<RepoLedgerEntry>,
        claim_scope: &'a ClaimScope,
        host_override_ledger_path: &'a Path,
        host_override_cache: HostOverrideCache,
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

    fn host_override_cache(&self) -> Option<HostOverrideCache> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet {
                host_override_cache,
                ..
            } => Some(host_override_cache.clone()),
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

    fn archive_claim_check(&self) -> Option<ArchiveClaimCheck> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet {
                claimed,
                host_override_ledger_path,
                ..
            } => Some(ArchiveClaimCheck {
                ledger_path: (*host_override_ledger_path).to_path_buf(),
                claimed: (**claimed).clone(),
            }),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct HostOverrideCache {
    pub(super) entries: Arc<Mutex<HashMap<String, HostOverrideCacheEntry>>>,
}

#[derive(Debug, Clone)]
pub(super) struct HostOverrideCacheEntry {
    pub(super) loaded_at: Instant,
    pub(super) value: Option<HostOverride>,
}

pub(crate) async fn fetch_one_attempt_with_pacer(
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
        config.runtime.host_override_cache(),
    )
    .await?;
    let _host_permit =
        acquire_host_fetch_permit(config.runtime.host_limiter(), &prepared_host).await?;
    reserve_host_send(config.runtime.host_pacer(), &prepared_host).await?;
    let host = prepared_host.host;
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
            fetch_config: &fetch_config,
            archive_dir: &config.archive_dir,
            archive_context: config.archive_context,
            archive_storage: config.archive_storage,
            host_pacer: config.runtime.host_pacer(),
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
                .and_then(|override_record| override_record.concurrency_cap)
                .or(Some(DEFAULT_HOST_CONCURRENCY_CAP)),
        )
        .await
}

async fn reserve_host_send(
    host_pacer: Option<&SharedHostPacer>,
    prepared_host: &PreparedFetchHost,
) -> Result<(), FetchOneFailure> {
    let Some(pacer) = host_pacer else {
        return Ok(());
    };
    let min_interval = prepared_host
        .host_override
        .as_ref()
        .and_then(|override_record| override_record.min_interval);
    HostPacer::reserve_next_request(pacer, &prepared_host.host, min_interval)
        .await
        .map_err(|err| retryable_failure(format!("host pacing for {}: {err}", prepared_host.host)))
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
    archive_storage: ArchiveStorageConfig,
    host_pacer: Option<&'a SharedHostPacer>,
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
                archive_dir: step.archive_dir,
                archive_context: step.archive_context,
                archive_storage: step.archive_storage,
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
    let fetched = match fetch_spooled_repo(FetchStep {
        http: step.http,
        pds: step.pds,
        did: step.did,
        did_str: step.did_str,
        host: step.host,
        config: step.fetch_config,
        host_pacer: step.host_pacer,
    })
    .await
    {
        Ok(fetched) => fetched,
        Err(err) if should_fallback_get_repo_to_list_records(&err) => {
            emit_get_repo_fallback(step.did_str, step.host, step.attempt_started, &err);
            return fetch_archive_list_records_or_emit_failure(ListRecordsStep {
                http: step.http,
                pds: step.pds,
                did: step.did,
                did_str: step.did_str,
                host: step.host,
                archive_dir: step.archive_dir,
                archive_context: step.archive_context.clone(),
                archive_storage: step.archive_storage.clone(),
                host_pacer: step.host_pacer,
                attempt_started: step.attempt_started,
            })
            .await;
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

struct FetchStep<'a> {
    http: &'a reqwest::Client,
    pds: &'a Uri<String>,
    did: &'a Did,
    did_str: &'a str,
    host: &'a str,
    config: &'a FetchConfig,
    host_pacer: Option<&'a SharedHostPacer>,
}

struct FetchedRepo {
    spooled: emojistats_backfill::transport::SpooledRepo,
    fetch_ms: u64,
}

async fn fetch_spooled_repo(step: FetchStep<'_>) -> Result<FetchedRepo, FetchError> {
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
                return Err(err);
            }
        }
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

fn repo_fetch_client(http_protocol: HttpProtocol) -> Result<reqwest::Client, reqwest::Error> {
    let builder = reqwest::Client::builder()
        .user_agent(CRAWLER_USER_AGENT)
        .tcp_keepalive(Duration::from_secs(60))
        .connect_timeout(Duration::from_secs(30));
    match http_protocol {
        HttpProtocol::Http1 => builder.http1_only().build(),
        HttpProtocol::Auto => builder.build(),
    }
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
    claim_check: Option<ArchiveClaimCheck>,
    archive_context: ArchiveCommitContext,
    archive_storage: ArchiveStorageConfig,
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

#[derive(Debug, Clone)]
pub(crate) struct ProcessedRepoCounts {
    pub(crate) records: u64,
    pub(crate) archived_posts: u64,
    pub(crate) decode_errors: u64,
    pub(crate) emoji_rows: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ProcessedRepoArtifacts {
    pub(crate) receipt_hash: String,
    pub(crate) parquet_path: PathBuf,
    pub(crate) receipt_path: PathBuf,
    pub(crate) manifest_path: PathBuf,
    pub(crate) emoji_projection_path: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct GetRepoTimings {
    pub(crate) fetch_ms: Option<u64>,
    pub(crate) bytes: Option<u64>,
    pub(crate) parse_ms: u64,
    pub(crate) parse_index_ms: u64,
    pub(crate) parse_commit_ms: u64,
    pub(crate) parse_walk_ms: u64,
    pub(crate) archive_ms: u64,
}

#[derive(Debug, Clone)]
struct ListRecordsTimings {
    fetch_ms: u64,
    archive_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct GetRepoProcessed {
    pub(crate) counts: ProcessedRepoCounts,
    pub(crate) artifacts: ProcessedRepoArtifacts,
    pub(crate) timings: GetRepoTimings,
}

#[derive(Debug, Clone)]
pub(crate) struct ListRecordsProcessed {
    counts: ProcessedRepoCounts,
    artifacts: ProcessedRepoArtifacts,
    timings: ListRecordsTimings,
}

#[derive(Debug, Clone)]
pub(crate) enum ProcessedRepo {
    GetRepo(GetRepoProcessed),
    ListRecords(ListRecordsProcessed),
}

impl ProcessedRepo {
    pub(crate) const fn counts(&self) -> &ProcessedRepoCounts {
        match self {
            Self::GetRepo(processed) => &processed.counts,
            Self::ListRecords(processed) => &processed.counts,
        }
    }

    pub(crate) const fn artifacts(&self) -> &ProcessedRepoArtifacts {
        match self {
            Self::GetRepo(processed) => &processed.artifacts,
            Self::ListRecords(processed) => &processed.artifacts,
        }
    }

    pub(crate) const fn fetch_ms_opt(&self) -> Option<u64> {
        match self {
            Self::GetRepo(processed) => processed.timings.fetch_ms,
            Self::ListRecords(processed) => Some(processed.timings.fetch_ms),
        }
    }

    pub(crate) const fn bytes(&self) -> Option<u64> {
        match self {
            Self::GetRepo(processed) => processed.timings.bytes,
            Self::ListRecords(_) => None,
        }
    }

    pub(crate) const fn parse_ms(&self) -> Option<u64> {
        match self {
            Self::GetRepo(processed) => Some(processed.timings.parse_ms),
            Self::ListRecords(_) => None,
        }
    }

    pub(crate) const fn archive_ms(&self) -> u64 {
        match self {
            Self::GetRepo(processed) => processed.timings.archive_ms,
            Self::ListRecords(processed) => processed.timings.archive_ms,
        }
    }

    pub(crate) const fn get_repo_timings(&self) -> Option<&GetRepoTimings> {
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
    archive_storage: ArchiveStorageConfig,
    host_pacer: Option<&'a SharedHostPacer>,
    attempt_started: Instant,
}

async fn fetch_archive_list_records_or_emit_failure(
    step: ListRecordsStep<'_>,
) -> Result<ProcessedRepo, FetchOneFailure> {
    let fetch_started = Instant::now();
    emit_list_records_running(&step);
    let host_pacer = step.host_pacer;
    let host = step.host;
    match fetch_and_archive_list_records_with_rate_limit_observer(
        step.http,
        step.pds,
        step.did,
        step.did_str,
        step.archive_dir,
        step.archive_context.clone(),
        step.archive_storage.clone(),
        ListRecordsConfig::default(),
        |rate_limit| record_rate_limit_snapshot(host_pacer, host, rate_limit, SystemTime::now()),
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
