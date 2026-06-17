#![allow(clippy::redundant_pub_crate)]

use jacquard_identity::resolver::IdentityResolver;

use super::{
    super::{
        Arc, ArchiveCommitContext, ArchiveStorageConfig, CRAWLER_USER_AGENT, ClaimScope,
        DEFAULT_HOST_CONCURRENCY_CAP, Did, Digest, Duration, FETCH_TRANSPORT_ATTEMPTS,
        FETCH_TRANSPORT_RETRY_BASE_DELAY, FETCH_TRANSPORT_RETRY_MAX_DELAY, FetchByteBudget,
        FetchConfig, FetchError, FetchOneFailure, ForcedFetchMode, HashMap, HostConcurrencyLimiter,
        HostConcurrencyPermit, HostOverride, HostPacer, HttpProtocol, Instant, Mutex, ParseConfig,
        Path, PathBuf, PublicResolver, RepoLedgerEntry, Semaphore, Sha256, SharedHostPacer,
        SmokeTelemetry, SqliteLedger, SystemTime, Uri, classify_fetch_error, current_rss_kb,
        elapsed_ms, emit_smoke_telemetry, fetch_repo_with_rate_limit_observer, outcome_name,
        permanent_failure, retryable_failure,
    },
    archive_host::{ArchiveClaimCheck, PreparedFetchHost, prepare_fetch_host},
    host_rate_limit::{record_rate_limit_cooldown, record_rate_limit_snapshot},
    list_records_attempt::{ListRecordsStep, fetch_archive_list_records_or_emit_failure},
    parse_archive_attempt::{ParseArchiveStep, parse_archive_or_emit_failure},
    processed_repo::{FetchedRepo, ProcessedRepo},
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
            persist_list_records_method_wall_override(&step, &err);
            return fetch_archive_list_records_or_emit_failure(ListRecordsStep {
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

fn persist_list_records_method_wall_override(step: &FetchModeStep<'_>, error: &FetchError) {
    let Some(ledger_path) = step.host_override_ledger_path else {
        return;
    };
    let record = HostOverride {
        host: step.host.to_owned(),
        disabled: false,
        concurrency_cap: None,
        min_interval: step.host_min_interval,
        revive_after: None,
        force_mode: Some(ForcedFetchMode::ListRecords),
        never_diff: false,
    };
    match SqliteLedger::open(ledger_path).and_then(|ledger| ledger.upsert_host_override(&record)) {
        Ok(()) => eprintln!(
            "recorded listRecords host override for {} after getRepo method wall: {error}",
            step.host
        ),
        Err(err) => eprintln!(
            "failed to persist listRecords host override for {} after getRepo method wall: {err}",
            step.host
        ),
    }
}

struct FetchStep<'a> {
    http: &'a reqwest::Client,
    pds: &'a Uri<String>,
    did: &'a Did,
    did_str: &'a str,
    host: &'a str,
    host_min_interval: Option<Duration>,
    config: &'a FetchConfig,
    host_pacer: Option<&'a SharedHostPacer>,
}

async fn fetch_spooled_repo(step: FetchStep<'_>) -> Result<FetchedRepo, FetchError> {
    let fetch_started = Instant::now();
    let mut attempt = 1_u8;
    loop {
        reserve_host_send_for_fetch(step.host_pacer, step.host, step.host_min_interval).await?;
        match fetch_repo_with_rate_limit_observer(
            step.http,
            step.pds,
            step.did,
            step.config,
            |rate_limit| {
                record_rate_limit_snapshot(
                    step.host_pacer,
                    step.host,
                    rate_limit,
                    SystemTime::now(),
                );
            },
        )
        .await
        {
            Ok(spooled) => {
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

async fn reserve_host_send_for_fetch(
    host_pacer: Option<&SharedHostPacer>,
    host: &str,
    min_interval: Option<Duration>,
) -> Result<(), FetchError> {
    let Some(pacer) = host_pacer else {
        return Ok(());
    };
    HostPacer::reserve_next_request(pacer, host, min_interval)
        .await
        .map_err(|err| FetchError::Transport {
            message: format!("host pacing for {host}: {err}"),
            observed_bytes: None,
        })
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
        FetchError::Transport { .. }
            | FetchError::InactivityTimeout { .. }
            | FetchError::DownloadTimeout { .. }
            | FetchError::ResponseHeaderTimeout { .. }
            | FetchError::ProgressTimeout { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn immediate_stream_retry_includes_timeout_categories() {
        assert!(is_retryable_stream_fetch_error(&FetchError::Transport {
            message: "connection reset".to_owned(),
            observed_bytes: None,
        }));
        assert!(is_retryable_stream_fetch_error(
            &FetchError::InactivityTimeout {
                timeout: Duration::from_secs(30),
            }
        ));
        assert!(is_retryable_stream_fetch_error(
            &FetchError::DownloadTimeout {
                timeout: Duration::from_secs(600),
                observed_bytes: 12,
            }
        ));
        assert!(is_retryable_stream_fetch_error(
            &FetchError::ResponseHeaderTimeout {
                timeout: Duration::from_secs(60),
            }
        ));
        assert!(is_retryable_stream_fetch_error(
            &FetchError::ProgressTimeout {
                interval: Duration::from_secs(60),
                min_bytes: 16_384,
                observed_bytes: 1024,
            }
        ));
    }
}
