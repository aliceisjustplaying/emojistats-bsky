use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use emojistats_backfill::{
    archive::ArchiveCommitContext,
    ledger::{
        AttemptOutcome, DEFAULT_CLAIM_LEASE_DURATION, RepoLedgerEntry, RetryPolicy, SqliteLedger,
    },
    parse::ParseConfig,
    scheduler::{ClaimScope, HostPacer, SchedulerError, SharedHostPacer, checked_concurrency},
    transport::FetchByteBudget,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use jacquard_common::types::did::Did;
use tokio::{
    sync::{Notify, Semaphore},
    task::JoinHandle,
};

use super::{
    add_count,
    cli::HttpProtocol,
    failure::{FetchOneFailure, retryable_failure},
    increment,
    main::fetch_attempt::{
        AttemptRuntime, FetchOneAttemptConfig, HostOverrideCache, fetch_one_attempt_with_pacer,
    },
    parse_config_for_threads,
};

const SEED_BATCH_SIZE: usize = 1_000;
const STALE_RECOVERY_BATCH_SIZE: u32 = 512;
#[allow(clippy::duration_suboptimal_units)]
const CLAIM_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15 * 60);
const STALE_RECOVERY_INTERVAL: Duration = Duration::from_secs(60);
pub const DEFAULT_HOST_CONCURRENCY_CAP: u32 = 2;

#[derive(Debug)]
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
    pub cid_verification_threads: usize,
    pub http_protocol: HttpProtocol,
    pub claim_scope: ClaimScope,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SeedSummary {
    pub inserted: u64,
    pub existing: u64,
    pub blank: u64,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct FleetSummary {
    seed: SeedSummary,
    stale_recovered: u64,
    claimed: u64,
    succeeded: u64,
    failed: u64,
}

#[derive(Debug, Clone, Default)]
pub struct HostConcurrencyLimiter {
    hosts: Arc<Mutex<HashMap<String, Arc<HostConcurrencyState>>>>,
}

#[derive(Debug)]
struct HostConcurrencyState {
    inner: Mutex<HostConcurrencyInner>,
    notify: Notify,
}

#[derive(Debug)]
struct HostConcurrencyInner {
    cap: usize,
    in_use: usize,
}

#[derive(Debug)]
pub struct HostConcurrencyPermit {
    state: Arc<HostConcurrencyState>,
}

impl HostConcurrencyLimiter {
    pub async fn acquire(
        &self,
        host: &str,
        concurrency_cap: Option<u32>,
    ) -> Result<Option<HostConcurrencyPermit>, FetchOneFailure> {
        let Some(concurrency_cap) = concurrency_cap else {
            return Ok(None);
        };
        let cap = usize::try_from(concurrency_cap)
            .map_err(|_err| retryable_failure(format!("host cap overflows usize for {host}")))?;
        let state = {
            let mut hosts = self
                .hosts
                .lock()
                .map_err(|_err| retryable_failure("host limiter lock poisoned".to_owned()))?;
            Arc::clone(hosts.entry(host.to_owned()).or_insert_with(|| {
                Arc::new(HostConcurrencyState {
                    inner: Mutex::new(HostConcurrencyInner { cap, in_use: 0 }),
                    notify: Notify::new(),
                })
            }))
        };
        state.acquire(host, cap).await.map(Some)
    }
}

impl HostConcurrencyState {
    async fn acquire(
        self: Arc<Self>,
        host: &str,
        cap: usize,
    ) -> Result<HostConcurrencyPermit, FetchOneFailure> {
        loop {
            let notified = self.notify.notified();
            {
                let mut inner = self.inner.lock().map_err(|_err| {
                    retryable_failure(format!("host limiter lock poisoned for {host}"))
                })?;
                inner.cap = cap;
                if inner.in_use < inner.cap {
                    inner.in_use = inner.in_use.checked_add(1).ok_or_else(|| {
                        retryable_failure(format!("host limiter in-use count overflow for {host}"))
                    })?;
                    let state = Arc::clone(&self);
                    drop(inner);
                    return Ok(HostConcurrencyPermit { state });
                }
            }
            notified.await;
        }
    }
}

impl Drop for HostConcurrencyPermit {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.state.inner.lock() {
            inner.in_use = inner.in_use.saturating_sub(1);
        }
        self.state.notify.notify_waiters();
    }
}

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
    summary.stale_recovered = recover_stale_claimed_entries_for_scope_with_message(
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
    let mut active = FuturesUnordered::new();
    let claim_limit = u64::from(config.claim_limit);
    let mut next_stale_recovery = next_stale_recovery_deadline(Instant::now())?;

    loop {
        let now = Instant::now();
        if now >= next_stale_recovery {
            let recovered = recover_stale_claimed_entries_for_scope(
                &ledger,
                SystemTime::now(),
                &config.claim_scope,
            )?;
            add_count(
                &mut summary.stale_recovered,
                recovered,
                "stale claimed recovery count",
            )?;
            next_stale_recovery = next_stale_recovery_deadline(now)?;
        }

        while active.len() < config.concurrency && summary.claimed < claim_limit {
            let remaining = claim_limit
                .checked_sub(summary.claimed)
                .ok_or(SchedulerError::ClaimLimitOverflow)?;
            if remaining == 0 {
                break;
            }
            let claimed = ledger.try_claim_next(
                SystemTime::now(),
                &config.run_id,
                &config.worker_id,
                DEFAULT_CLAIM_LEASE_DURATION,
                config.claim_scope.shard_filter(),
            )?;
            let Some(claimed) = claimed else {
                break;
            };
            let did = claimed.did.clone();
            increment(&mut summary.claimed, "claimed repo count")?;
            active.push(run_fleet_attempt(FleetAttemptConfig {
                did,
                claimed,
                spool_dir: config.spool_dir.clone(),
                max_bytes: config.max_bytes,
                archive_dir: config.archive_dir.clone(),
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
        }

        let Some(attempt_result) = active.next().await else {
            break;
        };
        complete_fleet_attempt(&ledger, &mut summary, attempt_result)?;
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
}

async fn run_fleet_attempt(config: FleetAttemptConfig) -> FleetAttemptResult {
    let archive_context = archive_context_for_claim(&config.claimed, &config.claim_scope);
    let heartbeat = spawn_claim_heartbeat(config.ledger_path.clone(), config.claimed.clone());
    let result = match archive_context {
        Ok(archive_context) => {
            fetch_one_attempt_with_pacer(FetchOneAttemptConfig {
                did_str: &config.did,
                spool_dir: config.spool_dir,
                max_bytes: config.max_bytes,
                archive_dir: config.archive_dir,
                archive_context,
                http_protocol: config.http_protocol,
                runtime: AttemptRuntime::Fleet {
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
    heartbeat.abort();
    let _ignored = heartbeat.await;
    FleetAttemptResult {
        did: config.did,
        claimed: config.claimed,
        result,
    }
}

fn spawn_claim_heartbeat(ledger_path: PathBuf, claimed: RepoLedgerEntry) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(CLAIM_HEARTBEAT_INTERVAL).await;
            let now = SystemTime::now();
            match SqliteLedger::open(&ledger_path).and_then(|ledger| {
                ledger.extend_owned_claim_lease(&claimed, now, DEFAULT_CLAIM_LEASE_DURATION)
            }) {
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
    })
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

fn complete_fleet_attempt(
    ledger: &SqliteLedger,
    summary: &mut FleetSummary,
    attempt_result: FleetAttemptResult,
) -> anyhow::Result<()> {
    let outcome = attempt_result.result.as_ref().map_or_else(
        |failure| failure.outcome.clone(),
        |_success| AttemptOutcome::Succeeded,
    );
    let completed = ledger.complete_owned_claim(
        &attempt_result.claimed,
        outcome,
        SystemTime::now(),
        RetryPolicy::default(),
    )?;
    let Some(completed) = completed else {
        eprintln!(
            "skipping completion for {} because this worker no longer owns the claim",
            attempt_result.did
        );
        return Ok(());
    };

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

#[cfg(test)]
pub fn claimable_entries_for_scope(
    ledger: &SqliteLedger,
    now: SystemTime,
    limit: u32,
    claim_scope: &ClaimScope,
) -> anyhow::Result<Vec<RepoLedgerEntry>> {
    claim_scope.shard_filter().map_or_else(
        || ledger.claimable_entries(now, limit).map_err(Into::into),
        |shard_filter| {
            ledger
                .claimable_entries_for_shard(now, limit, shard_filter)
                .map_err(Into::into)
        },
    )
}

#[cfg(test)]
pub fn recover_stale_claimed_entries(
    ledger: &SqliteLedger,
    _dids_file: &Path,
    now: SystemTime,
) -> anyhow::Result<u64> {
    recover_stale_claimed_entries_for_scope_with_message(
        ledger,
        now,
        &ClaimScope::default(),
        "expired claimed lease at fleet startup",
    )
}

pub fn recover_stale_claimed_entries_for_scope(
    ledger: &SqliteLedger,
    now: SystemTime,
    claim_scope: &ClaimScope,
) -> anyhow::Result<u64> {
    recover_stale_claimed_entries_for_scope_with_message(
        ledger,
        now,
        claim_scope,
        "expired claimed lease during fleet run",
    )
}

fn recover_stale_claimed_entries_for_scope_with_message(
    ledger: &SqliteLedger,
    now: SystemTime,
    claim_scope: &ClaimScope,
    message: &str,
) -> anyhow::Result<u64> {
    let mut recovered = 0_u64;
    loop {
        let batch_recovered = ledger.recover_expired_claims(
            now,
            STALE_RECOVERY_BATCH_SIZE,
            claim_scope.shard_filter(),
            message,
        )?;
        add_count(
            &mut recovered,
            batch_recovered,
            "stale claimed recovery count",
        )?;
        if batch_recovered < u64::from(STALE_RECOVERY_BATCH_SIZE) {
            break;
        }
    }
    Ok(recovered)
}

fn next_stale_recovery_deadline(now: Instant) -> anyhow::Result<Instant> {
    now.checked_add(STALE_RECOVERY_INTERVAL)
        .ok_or_else(|| anyhow::anyhow!("stale recovery timer overflow"))
}

pub fn seed_ledger_from_file(
    ledger: &SqliteLedger,
    dids_file: &Path,
) -> anyhow::Result<SeedSummary> {
    let mut summary = SeedSummary::default();
    let mut batch = Vec::with_capacity(SEED_BATCH_SIZE);
    let file = File::open(dids_file)?;

    for line in BufReader::new(file).lines() {
        let line = line?;
        let did = line.trim();
        if did.is_empty() {
            increment(&mut summary.blank, "blank line count")?;
            continue;
        }
        let _parsed: Did = Did::new_owned(did).map_err(|err| {
            anyhow::anyhow!("invalid DID {did:?} in {}: {err}", dids_file.display())
        })?;

        batch.push(did.to_owned());
        if batch.len() == SEED_BATCH_SIZE {
            flush_seed_batch(ledger, &mut summary, &batch)?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        flush_seed_batch(ledger, &mut summary, &batch)?;
    }

    Ok(summary)
}

fn flush_seed_batch(
    ledger: &SqliteLedger,
    summary: &mut SeedSummary,
    batch: &[String],
) -> anyhow::Result<()> {
    let batch_summary = ledger
        .insert_pending_entries_ignore_existing(batch.iter().map(std::string::String::as_str))?;
    add_count(
        &mut summary.inserted,
        batch_summary.inserted,
        "inserted seed count",
    )?;
    add_count(
        &mut summary.existing,
        batch_summary.existing,
        "existing seed count",
    )?;
    Ok(())
}

pub fn default_worker_id(run_id: &str) -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_err| "unknown-host".to_owned());
    format!("{run_id}:{host}:{}", std::process::id())
}
