use std::{
    any::Any,
    fs,
    panic::AssertUnwindSafe,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use emojistats_backfill::{
    archive::{ArchiveCommitContext, ArchiveStorageConfig},
    ledger::{
        AttemptOutcome, DEFAULT_CLAIM_LEASE_DURATION, RepoLedgerEntry, RetryPolicy, SqliteLedger,
    },
    metrics::{MetricLabels, MetricName, MetricStage, PressureState, SharedMetricsRecorder},
    parse::ParseConfig,
    scheduler::{ClaimScope, HostPacer, SchedulerError, SharedHostPacer, checked_concurrency},
    transport::FetchByteBudget,
};
use futures_util::FutureExt;
use tokio::{
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
};

use super::{
    add_count,
    cli::HttpProtocol,
    failure::{FetchOneFailure, retryable_failure},
    increment,
    main::fetch_attempt::{
        AttemptResources, FetchOneAttemptConfig, HostOverrideCache, fetch_one_attempt_with_pacer,
    },
    parse_config_for_threads,
};

#[allow(clippy::duration_suboptimal_units)]
const CLAIM_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15 * 60);
const STALE_RECOVERY_INTERVAL: Duration = Duration::from_secs(60);
pub const DEFAULT_HOST_CONCURRENCY_CAP: u32 = 2;

mod host_limiter;
mod ledger_async;
mod ledger_io;

pub use host_limiter::{HostConcurrencyLimiter, HostConcurrencyPermit};
pub use ledger_io::{SeedSummary, seed_ledger_from_file};
#[cfg(test)]
pub use ledger_io::{claimable_entries_for_scope, recover_stale_claimed_entries};

pub struct FleetConfig {
    pub dids_file: PathBuf,
    pub ledger_path: PathBuf,
    pub run_id: String,
    pub worker_id: String,
    pub claim_limit: u32,
    pub concurrency: usize,
    pub parse_concurrency: usize,
    pub max_inflight_spool_bytes: u64,
    pub spool_dir: PathBuf,
    pub max_bytes: u64,
    pub archive_dir: PathBuf,
    pub archive_storage: ArchiveStorageConfig,
    pub cid_verification_threads: usize,
    pub http_protocol: HttpProtocol,
    pub claim_scope: ClaimScope,
    pub shard_label: String,
    pub metrics: SharedMetricsRecorder,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct FleetSummary {
    seed: SeedSummary,
    stale_recovered: u64,
    claimed: u64,
    succeeded: u64,
    failed: u64,
}

#[allow(clippy::too_many_lines)]
pub async fn run(config: FleetConfig) -> anyhow::Result<()> {
    checked_concurrency(config.concurrency)?;
    checked_concurrency(config.parse_concurrency)?;
    if let Some(parent) = config
        .ledger_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let ledger = SqliteLedger::open(&config.ledger_path)?;
    let mut summary = FleetSummary {
        seed: seed_ledger_from_file(&ledger, &config.dids_file)?,
        ..FleetSummary::default()
    };
    summary.stale_recovered = ledger_io::recover_stale_claimed_entries_for_scope_with_message(
        &ledger,
        SystemTime::now(),
        &config.claim_scope,
        "expired claimed lease at fleet startup",
    )?;
    let host_pacer = HostPacer::shared();
    let host_limiter = HostConcurrencyLimiter::default();
    let host_override_cache = HostOverrideCache::default();
    let parse_permits = Arc::new(Semaphore::new(config.parse_concurrency));
    let byte_budget = FetchByteBudget::new(config.max_inflight_spool_bytes);
    let mut active = JoinSet::new();
    let active_attempt_limit = active_attempt_limit(
        config.concurrency,
        config.parse_concurrency,
        config.max_inflight_spool_bytes,
        config.max_bytes,
    );
    let claim_limit = u64::from(config.claim_limit);
    let mut next_stale_recovery = next_stale_recovery_deadline(Instant::now())?;
    let mut draining = false;
    let shutdown_signal = shutdown_signal();
    tokio::pin!(shutdown_signal);

    loop {
        let now = Instant::now();
        if should_recover_stale_claims(active.len(), now, next_stale_recovery) {
            let recovered = ledger_async::recover_stale_claimed_entries_for_scope(
                config.ledger_path.clone(),
                SystemTime::now(),
                config.claim_scope.clone(),
                "expired claimed lease during fleet run",
            )
            .await?;
            if recovered > 0 {
                config.metrics.increment_counter(
                    MetricName::FleetStaleClaimsRecoveredTotal,
                    fleet_metric_labels(
                        &config,
                        None,
                        Some(MetricStage::Claim.as_str()),
                        None,
                        None,
                    ),
                    recovered,
                );
            }
            add_count(
                &mut summary.stale_recovered,
                recovered,
                "stale claimed recovery count",
            )?;
            next_stale_recovery = next_stale_recovery_deadline(now)?;
        }

        let mut claimable_exhausted = false;
        while !draining && active.len() < active_attempt_limit && summary.claimed < claim_limit {
            let remaining = claim_limit
                .checked_sub(summary.claimed)
                .ok_or(SchedulerError::ClaimLimitOverflow)?;
            if remaining == 0 {
                break;
            }
            let claimed = ledger_async::try_claim_next(
                config.ledger_path.clone(),
                SystemTime::now(),
                config.run_id.clone(),
                config.worker_id.clone(),
                DEFAULT_CLAIM_LEASE_DURATION,
                config.claim_scope.clone(),
            )
            .await?;
            let Some(claimed) = claimed else {
                claimable_exhausted = true;
                break;
            };
            let did = claimed.did.clone();
            increment(&mut summary.claimed, "claimed repo count")?;
            config.metrics.increment_counter(
                MetricName::FleetReposClaimedTotal,
                fleet_metric_labels(
                    &config,
                    None,
                    Some(MetricStage::Claim.as_str()),
                    Some("claimed"),
                    None,
                ),
                1,
            );
            active.spawn(run_fleet_attempt_isolated(FleetAttemptConfig {
                did,
                claimed,
                spool_dir: config.spool_dir.clone(),
                max_bytes: config.max_bytes,
                archive_dir: config.archive_dir.clone(),
                archive_storage: config.archive_storage.clone(),
                parse_config: parse_config_for_threads(config.cid_verification_threads),
                http_protocol: config.http_protocol,
                host_pacer: host_pacer.clone(),
                host_limiter: host_limiter.clone(),
                host_override_cache: host_override_cache.clone(),
                parse_permits: parse_permits.clone(),
                byte_budget: byte_budget.clone(),
                claim_scope: config.claim_scope.clone(),
                ledger_path: config.ledger_path.clone(),
            }));
            record_active_attempts(&config, active.len());
        }

        if active.is_empty() && (draining || summary.claimed >= claim_limit || claimable_exhausted)
        {
            break;
        }

        tokio::select! {
            joined = active.join_next(), if !active.is_empty() => {
                let Some(joined) = joined else {
                    continue;
                };
                let attempt_result = joined
                    .map_err(|err| anyhow::anyhow!("fleet attempt task failed outside panic guard: {err}"))?;
                complete_fleet_attempt(&mut summary, &config, attempt_result).await?;
                record_active_attempts(&config, active.len());
            }
            signal_result = &mut shutdown_signal, if !draining => {
                match signal_result {
                    Ok(signal) => {
                        eprintln!(
                            "received {signal}; draining {} active fleet attempt(s) before shutdown",
                            active.len()
                        );
                    }
                    Err(error) => {
                        eprintln!(
                            "shutdown signal listener failed: {error}; draining {} active fleet attempt(s)",
                            active.len()
                        );
                    }
                }
                draining = true;
            }
        }
    }

    println!(
        "fleet summary: seeded {}, existing {}, blank {}, stale_recovered {}, claimed {}, succeeded {}, failed {}",
        summary.seed.inserted,
        summary.seed.existing,
        summary.seed.blank,
        summary.stale_recovered,
        summary.claimed,
        summary.succeeded,
        summary.failed
    );
    Ok(())
}

#[derive(Debug)]
struct FleetAttemptConfig {
    did: String,
    claimed: RepoLedgerEntry,
    spool_dir: PathBuf,
    max_bytes: u64,
    archive_dir: PathBuf,
    archive_storage: ArchiveStorageConfig,
    parse_config: ParseConfig,
    http_protocol: HttpProtocol,
    host_pacer: SharedHostPacer,
    host_limiter: HostConcurrencyLimiter,
    host_override_cache: HostOverrideCache,
    parse_permits: Arc<Semaphore>,
    byte_budget: FetchByteBudget,
    claim_scope: ClaimScope,
    ledger_path: PathBuf,
}

#[derive(Debug)]
struct FleetAttemptResult {
    did: String,
    claimed: RepoLedgerEntry,
    result: Result<(), FetchOneFailure>,
    elapsed: Duration,
}

async fn run_fleet_attempt_isolated(config: FleetAttemptConfig) -> FleetAttemptResult {
    let started = Instant::now();
    let did = config.did.clone();
    let claimed = config.claimed.clone();
    let result = AssertUnwindSafe(run_fleet_attempt(config))
        .catch_unwind()
        .await;
    match result {
        Ok(result) => result,
        Err(payload) => FleetAttemptResult {
            did,
            claimed,
            result: Err(retryable_failure(format!(
                "fleet attempt panicked: {}",
                panic_payload_message(payload.as_ref())
            ))),
            elapsed: started.elapsed(),
        },
    }
}

async fn run_fleet_attempt(config: FleetAttemptConfig) -> FleetAttemptResult {
    let started = Instant::now();
    let archive_context = archive_context_for_claim(&config.claimed, &config.claim_scope);
    let heartbeat = ClaimHeartbeat::spawn(config.ledger_path.clone(), config.claimed.clone());
    let result = match archive_context {
        Ok(archive_context) => {
            fetch_one_attempt_with_pacer(FetchOneAttemptConfig {
                did_str: &config.did,
                spool_dir: config.spool_dir,
                max_bytes: config.max_bytes,
                archive_dir: config.archive_dir,
                archive_storage: config.archive_storage,
                archive_context,
                http_protocol: config.http_protocol,
                resources: AttemptResources::Fleet {
                    host_pacer: config.host_pacer,
                    host_limiter: config.host_limiter,
                    parse_permits: config.parse_permits,
                    byte_budget: config.byte_budget,
                    claimed: Box::new(config.claimed.clone()),
                    claim_scope: &config.claim_scope,
                    host_override_ledger_path: &config.ledger_path,
                    host_override_cache: config.host_override_cache,
                },
                parse_config: config.parse_config,
            })
            .await
        }
        Err(failure) => Err(failure),
    };
    heartbeat.stop().await;
    FleetAttemptResult {
        did: config.did,
        claimed: config.claimed,
        result,
        elapsed: started.elapsed(),
    }
}

struct ClaimHeartbeat {
    handle: Option<JoinHandle<()>>,
}

impl ClaimHeartbeat {
    fn spawn(ledger_path: PathBuf, claimed: RepoLedgerEntry) -> Self {
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(CLAIM_HEARTBEAT_INTERVAL).await;
                let now = SystemTime::now();
                match ledger_async::extend_owned_claim_lease(
                    ledger_path.clone(),
                    claimed.clone(),
                    now,
                    DEFAULT_CLAIM_LEASE_DURATION,
                )
                .await
                {
                    Ok(Some(_entry)) => {}
                    Ok(None) => {
                        eprintln!(
                            "stopping claim heartbeat for {} because this worker no longer owns it",
                            claimed.did
                        );
                        break;
                    }
                    Err(error) => {
                        eprintln!("claim heartbeat failed for {}: {error}", claimed.did);
                    }
                }
            }
        });
        Self {
            handle: Some(handle),
        }
    }

    async fn stop(mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        handle.abort();
        let _ignored = handle.await;
    }
}

impl Drop for ClaimHeartbeat {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

fn archive_context_for_claim(
    claimed: &RepoLedgerEntry,
    claim_scope: &ClaimScope,
) -> Result<ArchiveCommitContext, FetchOneFailure> {
    let attempt = claimed.last_attempt.as_ref().ok_or_else(|| {
        retryable_failure(format!(
            "claimed repo {} has no attempt identity",
            claimed.did
        ))
    })?;
    Ok(ArchiveCommitContext::new(
        attempt.run_id.clone(),
        archive_shard_label(claim_scope),
        attempt.sequence,
    ))
}

fn archive_shard_label(claim_scope: &ClaimScope) -> String {
    claim_scope.shard_filter().map_or_else(
        || "all".to_owned(),
        |shard| format!("shard{}", shard.bucket()),
    )
}

async fn complete_fleet_attempt(
    summary: &mut FleetSummary,
    config: &FleetConfig,
    attempt_result: FleetAttemptResult,
) -> anyhow::Result<()> {
    let outcome = attempt_result.result.as_ref().map_or_else(
        |failure| failure.outcome.clone(),
        |_success| AttemptOutcome::Succeeded,
    );
    let completed = ledger_async::complete_owned_claim(
        config.ledger_path.clone(),
        attempt_result.claimed.clone(),
        outcome,
        SystemTime::now(),
        RetryPolicy::default(),
    )
    .await?;
    let Some(completed) = completed else {
        eprintln!(
            "skipping completion for {} because this worker no longer owns the claim",
            attempt_result.did
        );
        return Ok(());
    };

    let outcome_label = outcome_name_for_attempt(&attempt_result.result);
    config.metrics.increment_counter(
        MetricName::FleetAttemptsTotal,
        fleet_metric_labels(
            config,
            None,
            Some(MetricStage::Complete.as_str()),
            Some(outcome_label),
            attempt_pressure_state(&attempt_result.result),
        ),
        1,
    );
    config.metrics.record_histogram(
        MetricName::FleetAttemptDurationSeconds,
        fleet_metric_labels(
            config,
            None,
            Some(MetricStage::Complete.as_str()),
            Some(outcome_label),
            None,
        ),
        attempt_result.elapsed.as_secs_f64(),
    );
    if let Some(pressure_state) = attempt_pressure_state(&attempt_result.result) {
        config.metrics.record_gauge(
            MetricName::FleetPressureState,
            fleet_metric_labels(
                config,
                None,
                Some(MetricStage::Complete.as_str()),
                Some(outcome_label),
                Some(pressure_state),
            ),
            1,
        );
    }

    match attempt_result.result {
        Ok(()) => increment(&mut summary.succeeded, "succeeded repo count")?,
        Err(failure) => {
            increment(&mut summary.failed, "failed repo count")?;
            eprintln!(
                "attempt failed for {}: {}",
                attempt_result.did, failure.error
            );
        }
    }
    println!(
        "ledger status for {} after {} attempt(s): {:?}",
        completed.did, completed.attempts, completed.status
    );
    Ok(())
}

fn record_active_attempts(config: &FleetConfig, active_len: usize) {
    let Ok(active) = i64::try_from(active_len) else {
        return;
    };
    config.metrics.record_gauge(
        MetricName::FleetActiveAttempts,
        fleet_metric_labels(
            config,
            None,
            Some(MetricStage::Fetch.as_str()),
            Some("active"),
            None,
        ),
        active,
    );
}

#[allow(clippy::missing_const_for_fn)]
fn fleet_metric_labels<'a>(
    config: &'a FleetConfig,
    host: Option<&'a str>,
    stage: Option<&'a str>,
    outcome: Option<&'a str>,
    pressure_state: Option<&'a str>,
) -> MetricLabels<'a> {
    MetricLabels {
        run_id: Some(config.run_id.as_str()),
        worker_id: Some(config.worker_id.as_str()),
        shard: Some(config.shard_label.as_str()),
        host,
        stage,
        outcome,
        pressure_state,
        backend: Some(config.archive_storage.backend_name()),
    }
}

#[allow(clippy::missing_const_for_fn)]
fn outcome_name_for_attempt(result: &Result<(), FetchOneFailure>) -> &'static str {
    match result {
        Ok(()) => "succeeded",
        Err(failure) => match failure.outcome {
            AttemptOutcome::Succeeded => "succeeded",
            AttemptOutcome::AccountState(_) => "account_state",
            AttemptOutcome::RetryableFailure { .. } => "retryable_failure",
            AttemptOutcome::RateLimited { .. } => "rate_limited",
            AttemptOutcome::ResourceLimitExceeded { .. } => "resource_limited",
            AttemptOutcome::PermanentFailure { .. } => "permanent_failure",
            AttemptOutcome::OperatorDeferred { .. } => "operator_deferred",
        },
    }
}

#[allow(clippy::missing_const_for_fn)]
fn attempt_pressure_state(result: &Result<(), FetchOneFailure>) -> Option<&'static str> {
    match result {
        Ok(()) => None,
        Err(failure) => match failure.outcome {
            AttemptOutcome::RateLimited { .. } => Some(PressureState::RateLimitSleep.as_str()),
            AttemptOutcome::ResourceLimitExceeded { .. } => {
                Some(PressureState::FetchByteBudget.as_str())
            }
            AttemptOutcome::OperatorDeferred { .. } => Some(PressureState::OperatorPause.as_str()),
            AttemptOutcome::Succeeded
            | AttemptOutcome::AccountState(_)
            | AttemptOutcome::RetryableFailure { .. }
            | AttemptOutcome::PermanentFailure { .. } => None,
        },
    }
}

fn next_stale_recovery_deadline(now: Instant) -> anyhow::Result<Instant> {
    now.checked_add(STALE_RECOVERY_INTERVAL)
        .ok_or_else(|| anyhow::anyhow!("stale recovery timer overflow"))
}

fn should_recover_stale_claims(
    active_attempts: usize,
    now: Instant,
    next_recovery: Instant,
) -> bool {
    active_attempts == 0 && now >= next_recovery
}

fn active_attempt_limit(
    concurrency: usize,
    parse_concurrency: usize,
    max_inflight_spool_bytes: u64,
    max_bytes: u64,
) -> usize {
    let spool_slots = if max_bytes == 0 || max_inflight_spool_bytes == 0 {
        concurrency
    } else {
        usize::try_from(
            max_inflight_spool_bytes
                .checked_div(max_bytes)
                .unwrap_or(u64::MAX),
        )
        .unwrap_or(usize::MAX)
        .max(1)
    };
    let backpressure_limit = parse_concurrency.saturating_add(spool_slots).max(1);
    concurrency.min(backpressure_limit).max(1)
}

fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&'static str>()
        .map(|message| (*message).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic payload".to_owned())
}

async fn shutdown_signal() -> anyhow::Result<&'static str> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                Ok("SIGINT")
            }
            _ = terminate.recv() => Ok("SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok("SIGINT")
    }
}

pub fn default_worker_id(run_id: &str) -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_err| "unknown-host".to_owned());
    format!("{run_id}:{host}:{}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_recovery_waits_until_no_active_attempts() {
        let now = Instant::now();
        assert!(!should_recover_stale_claims(1, now, now));
        assert!(should_recover_stale_claims(0, now, now));
    }

    #[test]
    fn active_attempt_limit_accounts_for_parse_and_spool_backpressure() {
        assert_eq!(active_attempt_limit(8, 1, 2_000, 1_000), 3);
        assert_eq!(active_attempt_limit(2, 8, 2_000, 1_000), 2);
        assert_eq!(active_attempt_limit(8, 1, 500, 1_000), 2);
    }
}
