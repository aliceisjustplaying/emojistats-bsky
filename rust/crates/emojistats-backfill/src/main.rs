//! emojistats-backfill — v2 Rust backfill CLI.
//!
//! Vertical-slice milestone in progress: `fetch-one <did>` resolves a DID to its PDS, then
//! (incrementally) fetches the repo via the streaming `getRepo` seam, proves snapshot
//! completeness, archives posts, and derives emoji rows. See `docs/backfill-v2-design.md`
//! ("First implementation milestone").

use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use clap::{Parser, Subcommand};
use emojistats_backfill::{
    archive::{
        ArchiveError, RepoReceiptInput, archive_rows_from_parsed_repo, build_repo_receipt,
        current_normalizer, hash_profile_record, write_archive_artifacts,
    },
    ledger::{
        AttemptId, AttemptOutcome, ForcedFetchMode, HostOverride, RepoLedgerEntry,
        RepoLedgerStatus, RetryPolicy, ShardFilter, SqliteLedger, claim_repo, complete_attempt,
    },
    parse::{ParseError, parse_repo_for_did},
    scheduler::{ClaimScope, HostPacer, SchedulerError, SharedHostPacer, checked_concurrency},
    transport::{FetchConfig, FetchError, fetch_repo},
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use jacquard_common::{deps::fluent_uri::Uri, types::did::Did};
use jacquard_identity::{PublicResolver, resolver::IdentityResolver};

/// emojistats v2 backfill tool.
#[derive(Parser, Debug)]
#[command(name = "emojistats-backfill", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Fetch and process a single repo by DID (vertical-slice milestone).
    FetchOne {
        /// The DID to fetch, e.g. did:plc:....
        did: String,
        /// Directory for local `CAR` spooling.
        #[arg(long, default_value = "data/spool")]
        spool_dir: PathBuf,
        /// Loud single-repo byte cap for the spooled `CAR`.
        #[arg(long, default_value_t = 2_147_483_648)]
        max_bytes: u64,
        /// Directory for local archive artifacts.
        #[arg(long, default_value = "data/archive")]
        archive_dir: PathBuf,
    },
    /// Seed, claim, and process repos from a newline-delimited DID file.
    RunFleet {
        /// Newline-delimited file of DIDs to seed into the SQLite ledger.
        dids_file: PathBuf,
        /// SQLite ledger path.
        #[arg(long, default_value = "data/ledger/backfill.sqlite")]
        ledger_path: PathBuf,
        /// Stable run id stored on claimed attempts.
        #[arg(long, default_value = "fleet-local")]
        run_id: String,
        /// Maximum claimable repos to process in this invocation.
        #[arg(long, default_value_t = 1, value_parser = parse_positive_u32)]
        claim_limit: u32,
        /// Maximum concurrent repo attempts.
        #[arg(long, default_value_t = 4, value_parser = parse_positive_usize)]
        concurrency: usize,
        /// Restrict claims to one persisted DID shard bucket.
        #[arg(long, value_name = "BUCKET", value_parser = parse_shard_filter)]
        shard_bucket: Option<ShardFilter>,
        /// Directory for local `CAR` spooling.
        #[arg(long, default_value = "data/spool")]
        spool_dir: PathBuf,
        /// Loud single-repo byte cap for each spooled `CAR`.
        #[arg(long, default_value_t = 2_147_483_648)]
        max_bytes: u64,
        /// Directory for local archive artifacts.
        #[arg(long, default_value = "data/archive")]
        archive_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::FetchOne {
            did,
            spool_dir,
            max_bytes,
            archive_dir,
        } => fetch_one(&did, spool_dir, max_bytes, archive_dir).await,
        Command::RunFleet {
            dids_file,
            ledger_path,
            run_id,
            claim_limit,
            concurrency,
            shard_bucket,
            spool_dir,
            max_bytes,
            archive_dir,
        } => {
            run_fleet(FleetConfig {
                dids_file,
                ledger_path,
                run_id,
                claim_limit,
                concurrency,
                spool_dir,
                max_bytes,
                archive_dir,
                claim_scope: ClaimScope {
                    shard_filter: shard_bucket,
                },
            })
            .await
        }
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
) -> anyhow::Result<()> {
    let now = SystemTime::now();
    let ledger = RepoLedgerEntry::pending(did_str);
    let claimed = claim_repo(&ledger, AttemptId::new("fetch-one-local", did_str, 1), now)
        .map_err(|err| anyhow::anyhow!("claim fetch-one ledger entry for {did_str}: {err}"))?;

    let result = fetch_one_attempt(did_str, spool_dir, max_bytes, archive_dir).await;
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

#[derive(Debug)]
struct FleetConfig {
    dids_file: PathBuf,
    ledger_path: PathBuf,
    run_id: String,
    claim_limit: u32,
    concurrency: usize,
    spool_dir: PathBuf,
    max_bytes: u64,
    archive_dir: PathBuf,
    claim_scope: ClaimScope,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SeedSummary {
    inserted: u64,
    existing: u64,
    blank: u64,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct FleetSummary {
    seed: SeedSummary,
    stale_recovered: u64,
    claimed: u64,
    succeeded: u64,
    failed: u64,
}

async fn run_fleet(config: FleetConfig) -> anyhow::Result<()> {
    checked_concurrency(config.concurrency)?;
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
    summary.stale_recovered =
        recover_stale_claimed_entries(&ledger, &config.dids_file, SystemTime::now())?;
    let host_pacer = HostPacer::shared();
    let mut active = FuturesUnordered::new();
    let claim_limit = u64::from(config.claim_limit);

    loop {
        while active.len() < config.concurrency && summary.claimed < claim_limit {
            let remaining = claim_limit
                .checked_sub(summary.claimed)
                .ok_or(SchedulerError::ClaimLimitOverflow)?;
            let batch_limit = claim_batch_limit(config.concurrency, active.len(), remaining)?;
            let claimable = claimable_entries_for_scope(
                &ledger,
                SystemTime::now(),
                batch_limit,
                &config.claim_scope,
            )?;
            if claimable.is_empty() {
                break;
            }

            for entry in claimable {
                let did = entry.did.clone();
                let attempt = AttemptId::new(&config.run_id, &did, next_attempt_sequence(&entry)?);
                let claimed = claim_repo(&entry, attempt, SystemTime::now())
                    .map_err(|err| anyhow::anyhow!("claim ledger entry for {did}: {err}"))?;
                ledger.save_transitioned_entry(&claimed)?;
                increment(&mut summary.claimed, "claimed repo count")?;
                active.push(run_fleet_attempt(FleetAttemptConfig {
                    did,
                    claimed,
                    spool_dir: config.spool_dir.clone(),
                    max_bytes: config.max_bytes,
                    archive_dir: config.archive_dir.clone(),
                    host_pacer: host_pacer.clone(),
                    claim_scope: config.claim_scope.clone(),
                    ledger_path: config.ledger_path.clone(),
                }));
            }
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
    host_pacer: SharedHostPacer,
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
    let result = fetch_one_attempt_with_pacer(
        &config.did,
        config.spool_dir,
        config.max_bytes,
        config.archive_dir,
        Some(config.host_pacer),
        &config.claim_scope,
        Some(&config.ledger_path),
    )
    .await;
    FleetAttemptResult {
        did: config.did,
        claimed: config.claimed,
        result,
    }
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
    let completed = complete_attempt(
        &attempt_result.claimed,
        outcome,
        SystemTime::now(),
        RetryPolicy::default(),
    )
    .map_err(|err| anyhow::anyhow!("complete ledger entry for {}: {err}", attempt_result.did))?;
    ledger.save_transitioned_entry(&completed)?;

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

fn claim_batch_limit(concurrency: usize, in_flight: usize, remaining: u64) -> anyhow::Result<u32> {
    let available = concurrency
        .checked_sub(in_flight)
        .ok_or(SchedulerError::InvalidConcurrency)?;
    let available = u64::try_from(available)?;
    let limit = available.min(remaining).min(u64::from(u32::MAX));
    u32::try_from(limit).map_err(Into::into)
}

fn claimable_entries_for_scope(
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

fn recover_stale_claimed_entries(
    ledger: &SqliteLedger,
    dids_file: &Path,
    now: SystemTime,
) -> anyhow::Result<u64> {
    let contents = fs::read_to_string(dids_file)?;
    let mut recovered = 0_u64;
    for line in contents.lines() {
        let did = line.trim();
        if did.is_empty() {
            continue;
        }
        let Some(entry) = ledger.load_entry(did)? else {
            continue;
        };
        if entry.status != RepoLedgerStatus::Claimed {
            continue;
        }
        let recovered_entry = complete_attempt(
            &entry,
            AttemptOutcome::RetryableFailure {
                message: "stale claimed state at fleet startup".to_owned(),
            },
            now,
            RetryPolicy {
                max_attempts: u32::MAX,
                base_delay: Duration::ZERO,
                max_delay: Duration::ZERO,
            },
        )
        .map_err(|err| anyhow::anyhow!("recover stale claimed ledger entry for {did}: {err}"))?;
        ledger.save_transitioned_entry(&recovered_entry)?;
        increment(&mut recovered, "stale claimed recovery count")?;
    }
    Ok(recovered)
}

fn seed_ledger_from_file(ledger: &SqliteLedger, dids_file: &Path) -> anyhow::Result<SeedSummary> {
    let mut summary = SeedSummary::default();
    let contents = fs::read_to_string(dids_file)?;

    for line in contents.lines() {
        let did = line.trim();
        if did.is_empty() {
            increment(&mut summary.blank, "blank line count")?;
            continue;
        }
        let _parsed: Did = Did::new_owned(did).map_err(|err| {
            anyhow::anyhow!("invalid DID {did:?} in {}: {err}", dids_file.display())
        })?;

        if ledger.load_entry(did)?.is_some() {
            increment(&mut summary.existing, "existing seed count")?;
            continue;
        }

        ledger.upsert_entry(&RepoLedgerEntry::pending(did))?;
        increment(&mut summary.inserted, "inserted seed count")?;
    }

    Ok(summary)
}

fn next_attempt_sequence(entry: &RepoLedgerEntry) -> anyhow::Result<u64> {
    u64::from(entry.attempts)
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("attempt sequence overflow for {}", entry.did))
}

fn increment(value: &mut u64, context: &str) -> anyhow::Result<()> {
    *value = value
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("{context} overflow"))?;
    Ok(())
}

fn parse_positive_u32(value: &str) -> Result<u32, String> {
    let parsed = value
        .parse::<u32>()
        .map_err(|err| format!("expected a positive integer: {err}"))?;
    if parsed == 0 {
        return Err("expected a positive integer".to_owned());
    }
    Ok(parsed)
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|err| format!("expected a positive integer: {err}"))?;
    if parsed == 0 {
        return Err("expected a positive integer".to_owned());
    }
    Ok(parsed)
}

fn parse_shard_filter(value: &str) -> Result<ShardFilter, String> {
    let bucket = value
        .parse::<u64>()
        .map_err(|err| format!("expected a shard bucket integer: {err}"))?;
    ShardFilter::new(bucket).map_err(|err| err.to_string())
}

async fn fetch_one_attempt(
    did_str: &str,
    spool_dir: PathBuf,
    max_bytes: u64,
    archive_dir: PathBuf,
) -> Result<(), FetchOneFailure> {
    let claim_scope = ClaimScope::default();
    fetch_one_attempt_with_pacer(
        did_str,
        spool_dir,
        max_bytes,
        archive_dir,
        None,
        &claim_scope,
        None,
    )
    .await
}

async fn fetch_one_attempt_with_pacer(
    did_str: &str,
    spool_dir: PathBuf,
    max_bytes: u64,
    archive_dir: PathBuf,
    host_pacer: Option<SharedHostPacer>,
    claim_scope: &ClaimScope,
    host_override_ledger_path: Option<&Path>,
) -> Result<(), FetchOneFailure> {
    let did: Did = Did::new_owned(did_str)
        .map_err(|err| permanent_failure(format!("invalid DID {did_str:?}: {err}")))?;

    let resolver = PublicResolver::default();
    let pds = resolver
        .pds_for_did(&did)
        .await
        .map_err(|err| retryable_failure(format!("resolve PDS for {did_str}: {err}")))?;

    println!("{did_str} -> PDS {pds}");
    let host = prepare_fetch_host(
        did_str,
        &pds,
        claim_scope,
        host_override_ledger_path,
        host_pacer.as_ref(),
    )
    .await?;
    let http = reqwest::Client::new();
    let mut config = FetchConfig::new(spool_dir);
    config.max_bytes = max_bytes;

    let spooled = match fetch_repo(&http, &pds, &did, &config).await {
        Ok(spooled) => spooled,
        Err(err) => {
            let failure = classify_fetch_error(did_str, &err);
            if let AttemptOutcome::RateLimited { retry_after } = &failure.outcome
                && let Some(pacer) = &host_pacer
                && let Err(pacer_error) = HostPacer::record_retry_after(pacer, &host, *retry_after)
            {
                eprintln!("failed to record host cooldown for {host}: {pacer_error}");
            }
            return Err(failure);
        }
    };
    println!(
        "spooled {} bytes from HTTP {} to {}",
        spooled.bytes,
        spooled.http_status,
        spooled.car_path.display()
    );

    let parsed = parse_repo_for_did(&spooled.car_path, did_str)
        .map_err(|err| classify_parse_error(did_str, &err))?;
    let rows = archive_rows_from_parsed_repo(&parsed).map_err(|err| {
        classify_archive_error(&format!("build archive rows for {did_str}"), &err)
    })?;
    let profile_row_hash = hash_profile_record(parsed.profile.as_ref())
        .map_err(|err| classify_archive_error(&format!("hash profile row for {did_str}"), &err))?;
    let post_decode_error_count = parsed
        .record_decode_errors
        .iter()
        .filter(|error| error.collection == "app.bsky.feed.post")
        .count()
        .try_into()
        .map_err(|_err| {
            resource_failure(format!("post decode error count overflow for {did_str}"))
        })?;
    let receipt = build_repo_receipt(RepoReceiptInput {
        rows: &rows,
        reachable_records_count: parsed.rkey_digest.all_records_count,
        reachable_post_records_count: parsed.rkey_digest.post_records_count,
        post_decode_error_count,
        profile_row_hash,
        mst_root_cid: Some(parsed.commit.data.clone()),
        commit_cid: Some(parsed.commit.cid.clone()),
        normalizer: current_normalizer(),
    })
    .map_err(|err| classify_archive_error(&format!("build receipt for {did_str}"), &err))?;
    let artifacts = write_archive_artifacts(
        &archive_dir,
        did_str,
        &rows,
        parsed.profile.as_ref(),
        &receipt,
    )
    .map_err(|err| {
        classify_archive_error(&format!("write archive artifacts for {did_str}"), &err)
    })?;
    println!(
        "parsed {} records, {} posts, {} decode errors, {} emoji rows, receipt {}",
        parsed.rkey_digest.all_records_count,
        receipt.archived_post_rows_count,
        parsed.record_decode_errors.len(),
        artifacts.emoji_rows,
        receipt.post_rows_hash
    );
    println!(
        "wrote archive {}, receipt {}, manifest {}, emoji projection {}",
        artifacts.parquet_path.display(),
        artifacts.receipt_path.display(),
        artifacts.manifest_path.display(),
        artifacts.emoji_projection_path.display()
    );
    Ok(())
}

async fn prepare_fetch_host(
    did_str: &str,
    pds: &Uri<String>,
    claim_scope: &ClaimScope,
    host_override_ledger_path: Option<&Path>,
    host_pacer: Option<&SharedHostPacer>,
) -> Result<String, FetchOneFailure> {
    if !claim_scope.includes_did(did_str) {
        return Err(retryable_failure(format!(
            "DID {did_str} is outside configured shard scope"
        )));
    }
    let host = pds_host_key(pds);
    let host_override = load_host_override(host_override_ledger_path, &host)?;
    let fetch_mode = fetch_mode_for_host(&host, host_override.as_ref(), SystemTime::now())?;
    if fetch_mode == ForcedFetchMode::ListRecords {
        return Err(retryable_failure(format!(
            "host {host} is forced to list_records, but listRecords fetch is not implemented"
        )));
    }
    if let Some(pacer) = host_pacer {
        HostPacer::wait_until_ready(pacer, &host)
            .await
            .map_err(|err| retryable_failure(format!("host pacing for {host}: {err}")))?;
    }
    Ok(host)
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

#[derive(Debug)]
struct FetchOneFailure {
    outcome: AttemptOutcome,
    error: anyhow::Error,
}

fn classify_fetch_error(did: &str, error: &FetchError) -> FetchOneFailure {
    let message = format!("fetch getRepo for {did}: {error}");
    let outcome = match &error {
        FetchError::AccountState { state, .. } => AttemptOutcome::AccountState(*state),
        FetchError::HttpStatus {
            status, rate_limit, ..
        } if *status == 429 => rate_limit.retry_after.map_or_else(
            || AttemptOutcome::RetryableFailure {
                message: message.clone(),
            },
            |retry_after| AttemptOutcome::RateLimited { retry_after },
        ),
        FetchError::HttpStatus { status, .. } if *status >= 500 => {
            AttemptOutcome::RetryableFailure {
                message: message.clone(),
            }
        }
        FetchError::InactivityTimeout { .. }
        | FetchError::Transport { .. }
        | FetchError::Io { .. } => AttemptOutcome::RetryableFailure {
            message: message.clone(),
        },
        FetchError::MaxBytesExceeded { .. } | FetchError::ErrorBodyTooLarge { .. } => {
            AttemptOutcome::ResourceLimitExceeded {
                message: message.clone(),
            }
        }
        FetchError::HttpStatus { .. } => AttemptOutcome::PermanentFailure {
            message: message.clone(),
        },
    };
    FetchOneFailure {
        outcome,
        error: anyhow::anyhow!(message),
    }
}

fn classify_parse_error(did: &str, error: &ParseError) -> FetchOneFailure {
    let message = format!("parse CAR for {did}: {error}");
    let outcome = match error {
        ParseError::ResourceLimitExceeded { .. } | ParseError::ResourceCountOverflow { .. } => {
            AttemptOutcome::ResourceLimitExceeded {
                message: message.clone(),
            }
        }
        ParseError::Io { .. } | ParseError::Runtime(_) | ParseError::ThreadSpawn(_) => {
            AttemptOutcome::RetryableFailure {
                message: message.clone(),
            }
        }
        ParseError::Repo(_)
        | ParseError::InvalidRoots(_)
        | ParseError::CidMismatch { .. }
        | ParseError::UnsupportedCodec { .. }
        | ParseError::CommitNotFound { .. }
        | ParseError::RootCommitDecode { .. }
        | ParseError::CommitDidMismatch { .. }
        | ParseError::MissingBlock { .. }
        | ParseError::RecordDecode { .. }
        | ParseError::MstRootMismatch { .. }
        | ParseError::Unsupported { .. }
        | ParseError::NotYetImplemented { .. }
        | ParseError::RuntimeThreadTerminated
        | ParseError::MalformedVarint
        | ParseError::CarLengthOverflow { .. }
        | ParseError::MalformedCar(_)
        | ParseError::CidRead(_) => AttemptOutcome::PermanentFailure {
            message: message.clone(),
        },
    };
    FetchOneFailure {
        outcome,
        error: anyhow::anyhow!(message),
    }
}

fn classify_archive_error(context: &str, error: &ArchiveError) -> FetchOneFailure {
    let message = format!("{context}: {error}");
    let outcome = match error {
        ArchiveError::Io(_) | ArchiveError::Commit(_) => AttemptOutcome::RetryableFailure {
            message: message.clone(),
        },
        ArchiveError::CountOverflow { .. } => AttemptOutcome::ResourceLimitExceeded {
            message: message.clone(),
        },
        ArchiveError::Parquet(_)
        | ArchiveError::Arrow(_)
        | ArchiveError::Json(_)
        | ArchiveError::InvalidParquetColumn { .. }
        | ArchiveError::InvalidParquetValue { .. }
        | ArchiveError::UnexpectedParquetNull { .. }
        | ArchiveError::InvalidCompression(_)
        | ArchiveError::InvalidPath { .. }
        | ArchiveError::InvalidRecordJson => AttemptOutcome::PermanentFailure {
            message: message.clone(),
        },
    };
    FetchOneFailure {
        outcome,
        error: anyhow::anyhow!(message),
    }
}

fn retryable_failure(message: String) -> FetchOneFailure {
    FetchOneFailure {
        outcome: AttemptOutcome::RetryableFailure {
            message: message.clone(),
        },
        error: anyhow::anyhow!(message),
    }
}

fn permanent_failure(message: String) -> FetchOneFailure {
    FetchOneFailure {
        outcome: AttemptOutcome::PermanentFailure {
            message: message.clone(),
        },
        error: anyhow::anyhow!(message),
    }
}

fn resource_failure(message: String) -> FetchOneFailure {
    FetchOneFailure {
        outcome: AttemptOutcome::ResourceLimitExceeded {
            message: message.clone(),
        },
        error: anyhow::anyhow!(message),
    }
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
            RepoLedgerStatus, ShardFilter, SqliteLedger, claim_repo, did_shard_bucket,
        },
        scheduler::ClaimScope,
    };
    use jacquard_common::deps::fluent_uri::Uri;

    use super::{
        Cli, Command, SeedSummary, claim_batch_limit, claimable_entries_for_scope,
        fetch_mode_for_host, load_host_override, pds_host_key, recover_stale_claimed_entries,
        seed_ledger_from_file,
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
        } = cli.command
        else {
            unreachable!("expected fetch-one command");
        };
        assert_eq!(did, "did:plc:abc123");
        assert_eq!(spool_dir, PathBuf::from("data/spool"));
        assert_eq!(max_bytes, 2_147_483_648);
        assert_eq!(archive_dir, PathBuf::from("data/archive"));
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
            shard_bucket,
            spool_dir,
            max_bytes,
            archive_dir,
        } = cli.command
        else {
            unreachable!("expected run-fleet command");
        };
        assert_eq!(dids_file, PathBuf::from("dids.txt"));
        assert_eq!(ledger_path, PathBuf::from("data/ledger/backfill.sqlite"));
        assert_eq!(run_id, "fleet-local");
        assert_eq!(claim_limit, 1);
        assert_eq!(concurrency, 4);
        assert_eq!(shard_bucket, None);
        assert_eq!(spool_dir, PathBuf::from("data/spool"));
        assert_eq!(max_bytes, 2_147_483_648);
        assert_eq!(archive_dir, PathBuf::from("data/archive"));
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
        let claimed = claim_repo(
            &pending,
            AttemptId::new("previous-run", "did:plc:stale", 1),
            now,
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
            Some("stale claimed state at fleet startup".to_owned())
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
