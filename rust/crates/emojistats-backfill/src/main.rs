//! emojistats-backfill — v2 Rust backfill CLI.
//!
//! Vertical-slice milestone in progress: `fetch-one <did>` resolves a DID to its PDS, then
//! (incrementally) fetches the repo via the streaming `getRepo` seam, proves snapshot
//! completeness, archives posts, and derives emoji rows. See `docs/backfill-v2-design.md`
//! ("First implementation milestone").

use std::{
    fs::{self, File},
    io::BufReader,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use clap::{Parser, Subcommand};
use emojistats_backfill::{
    archive::{
        ArchiveError, ArchivePostRowsHasher, EmojiProjectionRow, StreamingArchiveSink,
        StreamingReceiptInput, archive_post_rows_from_record_batch, archive_row_from_post,
        hash_profile_record,
    },
    clickhouse::{
        ClickHouseClientConfig, ClickHouseInsertPayload, create_schema_sql,
        emoji_serving_rows_insert_payload, execute_insert_payloads,
        total_post_counter_insert_payload_for_counter,
    },
    derive::{
        BACKFILL_DERIVE_SOURCE, DeriveManifestIdentity, TotalPostCounterInput,
        emoji_projection_rows_for_post,
    },
    ledger::{
        AttemptId, AttemptOutcome, ForcedFetchMode, HostOverride, RepoLedgerEntry,
        RepoLedgerStatus, RetryPolicy, ShardFilter, SqliteLedger, claim_repo, complete_attempt,
    },
    manifest_derive::{
        VerifiedLoaderInput, read_committed_jsonl, verify_loader_input_for_streaming,
    },
    parse::{ParseConfig, ParseError, ParseVisitError, parse_repo_for_did_with_state},
    scheduler::{ClaimScope, HostPacer, SchedulerError, SharedHostPacer, checked_concurrency},
    transport::{FetchByteBudget, FetchConfig, FetchError, fetch_repo},
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use jacquard_common::{deps::fluent_uri::Uri, types::did::Did};
use jacquard_identity::{PublicResolver, resolver::IdentityResolver};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

const DEFAULT_PARSE_CONCURRENCY: usize = 1;
const DEFAULT_MAX_INFLIGHT_SPOOL_BYTES: u64 = 536_870_912;
const DERIVE_EMOJI_CHUNK_ROWS: usize = 10_000;

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
        /// Maximum concurrent parse/archive stages.
        #[arg(long, default_value_t = DEFAULT_PARSE_CONCURRENCY, value_parser = parse_positive_usize)]
        parse_concurrency: usize,
        /// Maximum bytes held by in-flight streamed `CAR` files.
        #[arg(long, default_value_t = DEFAULT_MAX_INFLIGHT_SPOOL_BYTES, value_parser = parse_positive_u64)]
        max_inflight_spool_bytes: u64,
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
    /// Verify a committed archive manifest and load derived rows into `ClickHouse`.
    DeriveManifest {
        /// Committed JSONL manifest path.
        manifest_path: PathBuf,
        /// Archive root used to resolve manifest object paths.
        #[arg(long, default_value = "data/archive")]
        archive_root: PathBuf,
        /// `ClickHouse` HTTP endpoint.
        #[arg(long, default_value = "http://localhost:8123")]
        clickhouse_url: String,
        /// `ClickHouse` database.
        #[arg(long, default_value = "emojistats")]
        clickhouse_database: String,
        /// `ClickHouse` username.
        #[arg(long, default_value = "default")]
        clickhouse_user: String,
        /// `ClickHouse` password.
        #[arg(long, default_value = "")]
        clickhouse_password: String,
        /// Validate and format payloads without sending inserts.
        #[arg(long)]
        dry_run: bool,
    },
    /// Print the v2 `ClickHouse` schema SQL.
    ClickhouseSchema {
        /// `ClickHouse` database.
        #[arg(long, default_value = "emojistats")]
        clickhouse_database: String,
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
            parse_concurrency,
            max_inflight_spool_bytes,
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
                parse_concurrency,
                max_inflight_spool_bytes,
                spool_dir,
                max_bytes,
                archive_dir,
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
            derive_manifest(DeriveManifestConfig {
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

#[derive(Debug)]
struct DeriveManifestConfig {
    manifest_path: PathBuf,
    archive_root: PathBuf,
    clickhouse_url: String,
    clickhouse_database: String,
    clickhouse_user: String,
    clickhouse_password: String,
    dry_run: bool,
}

#[derive(Debug, Default, Serialize)]
struct DeriveManifestSummary {
    manifest_entries: u64,
    skipped_entries: u64,
    batches: u64,
    payloads: u64,
    rows: u64,
    inserted_payloads: u64,
}

async fn derive_manifest(config: DeriveManifestConfig) -> anyhow::Result<()> {
    let file = File::open(&config.manifest_path)?;
    let plan = read_committed_jsonl(BufReader::new(file))?;
    let clickhouse = ClickHouseClientConfig::new(
        &config.clickhouse_url,
        &config.clickhouse_database,
        config.clickhouse_user,
        config.clickhouse_password,
        "emojistats-backfill-derive",
    )?;
    let http = reqwest::Client::new();
    let mut summary = DeriveManifestSummary {
        manifest_entries: count_len(plan.inputs.len(), "manifest_entries")?,
        skipped_entries: count_len(plan.skipped_entries.len(), "skipped_entries")?,
        ..DeriveManifestSummary::default()
    };

    for input in &plan.inputs {
        let verified = verify_loader_input_for_streaming(&config.archive_root, input)?;
        derive_verified_input_streaming(
            &verified,
            &http,
            &clickhouse,
            config.dry_run,
            &mut summary,
        )
        .await?;
    }

    println!(
        "derive_manifest_summary {}",
        serde_json::to_string(&summary)?
    );
    Ok(())
}

async fn derive_verified_input_streaming(
    verified: &VerifiedLoaderInput,
    http: &reqwest::Client,
    clickhouse: &ClickHouseClientConfig,
    dry_run: bool,
    summary: &mut DeriveManifestSummary,
) -> anyhow::Result<()> {
    let file = File::open(&verified.object_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut state = StreamingDeriveState::new(verified);

    for batch in reader {
        let rows = archive_post_rows_from_record_batch(&batch?)?;
        let payloads = state.consume_rows(&rows)?;
        apply_derive_payloads(http, clickhouse, dry_run, summary, &payloads).await?;
    }

    let payloads = state.finish()?;
    apply_derive_payloads(http, clickhouse, dry_run, summary, &payloads).await?;
    increment(&mut summary.batches, "derive batch count")
}

async fn apply_derive_payloads(
    http: &reqwest::Client,
    clickhouse: &ClickHouseClientConfig,
    dry_run: bool,
    summary: &mut DeriveManifestSummary,
    payloads: &[ClickHouseInsertPayload],
) -> anyhow::Result<()> {
    if payloads.is_empty() {
        return Ok(());
    }
    add_count(
        &mut summary.payloads,
        count_len(payloads.len(), "derive payload count")?,
        "derive payload total",
    )?;
    add_count(
        &mut summary.rows,
        payload_row_count(payloads)?,
        "derive row total",
    )?;
    if !dry_run {
        let receipts = execute_insert_payloads(http, clickhouse, payloads).await?;
        add_count(
            &mut summary.inserted_payloads,
            count_len(receipts.len(), "insert receipt count")?,
            "inserted payload total",
        )?;
    }
    Ok(())
}

struct StreamingDeriveState<'a> {
    verified: &'a VerifiedLoaderInput,
    row_hasher: ArchivePostRowsHasher,
    rows: u64,
    posts_with_emojis: u64,
    emoji_occurrences: u64,
    emoji_chunk_rows: Vec<EmojiProjectionRow>,
    emoji_chunk_index: u64,
}

impl<'a> StreamingDeriveState<'a> {
    fn new(verified: &'a VerifiedLoaderInput) -> Self {
        Self {
            verified,
            row_hasher: ArchivePostRowsHasher::new(),
            rows: 0,
            posts_with_emojis: 0,
            emoji_occurrences: 0,
            emoji_chunk_rows: Vec::with_capacity(DERIVE_EMOJI_CHUNK_ROWS),
            emoji_chunk_index: 0,
        }
    }

    fn consume_rows(
        &mut self,
        rows: &[emojistats_backfill::archive::ArchivePostRow],
    ) -> anyhow::Result<Vec<ClickHouseInsertPayload>> {
        let mut payloads = Vec::new();
        for row in rows {
            self.row_hasher.push_row(row)?;
            increment(&mut self.rows, "streaming derive row count")?;
            if !row.emoji_sequence.is_empty() {
                increment(
                    &mut self.posts_with_emojis,
                    "streaming derive emoji post count",
                )?;
            }
            add_count(
                &mut self.emoji_occurrences,
                count_len(
                    row.emoji_sequence.len(),
                    "streaming derive emoji occurrence count",
                )?,
                "streaming derive emoji occurrence total",
            )?;
            let projection_rows = emoji_projection_rows_for_post(row)?;
            for projection_row in projection_rows {
                self.emoji_chunk_rows.push(projection_row);
                if self.emoji_chunk_rows.len() >= DERIVE_EMOJI_CHUNK_ROWS {
                    payloads.push(self.flush_emoji_chunk()?);
                }
            }
        }
        Ok(payloads)
    }

    fn finish(mut self) -> anyhow::Result<Vec<ClickHouseInsertPayload>> {
        let mut payloads = Vec::new();
        if !self.emoji_chunk_rows.is_empty() {
            payloads.push(self.flush_emoji_chunk()?);
        }
        let row_hash = std::mem::take(&mut self.row_hasher).finish();
        self.validate_receipts(&row_hash)?;
        let counter = TotalPostCounterInput {
            source: BACKFILL_DERIVE_SOURCE.to_owned(),
            run_id: self.verified.manifest.run_id.clone(),
            shard: self.verified.manifest.shard.clone(),
            file_sequence: self.verified.manifest.file_sequence,
            receipt_hash: self.verified.manifest.receipt_hash.clone(),
            posts_processed: self.rows,
            posts_with_emojis: self.posts_with_emojis,
            emoji_occurrences: self.emoji_occurrences,
            min_created_at_normalized: self.verified.manifest.min_created_at_normalized.clone(),
            max_created_at_normalized: self.verified.manifest.max_created_at_normalized.clone(),
        };
        let token = streaming_dedupe_token(&self.verified.identity, "counter", None, &counter)?;
        payloads.push(total_post_counter_insert_payload_for_counter(
            &counter, token,
        )?);
        Ok(payloads)
    }

    fn flush_emoji_chunk(&mut self) -> anyhow::Result<ClickHouseInsertPayload> {
        let rows = std::mem::take(&mut self.emoji_chunk_rows);
        let token = streaming_dedupe_token(
            &self.verified.identity,
            "emoji",
            Some(self.emoji_chunk_index),
            &rows,
        )?;
        increment(
            &mut self.emoji_chunk_index,
            "streaming derive emoji chunk index",
        )?;
        self.emoji_chunk_rows = Vec::with_capacity(DERIVE_EMOJI_CHUNK_ROWS);
        Ok(emoji_serving_rows_insert_payload(
            &self.verified.identity,
            &rows,
            token,
        )?)
    }

    fn validate_receipts(&self, row_hash: &str) -> anyhow::Result<()> {
        if self.verified.manifest.row_count != self.rows {
            anyhow::bail!(
                "manifest row_count {} did not match streamed archive row count {} for {}",
                self.verified.manifest.row_count,
                self.rows,
                self.verified.object_path.display()
            );
        }
        let Some(receipt) = &self.verified.repo_receipt else {
            return Ok(());
        };
        if receipt.archived_post_rows_count != self.rows {
            anyhow::bail!(
                "repo receipt archived_post_rows_count {} did not match streamed archive row count {} for {}",
                receipt.archived_post_rows_count,
                self.rows,
                self.verified.object_path.display()
            );
        }
        if receipt.normalizer != self.verified.manifest.normalizer {
            anyhow::bail!(
                "repo receipt normalizer did not match manifest normalizer for {}",
                self.verified.object_path.display()
            );
        }
        if receipt.post_rows_hash != row_hash || receipt.archive_rows_hash != row_hash {
            anyhow::bail!(
                "repo receipt row hash did not match streamed archive rows for {}",
                self.verified.object_path.display()
            );
        }
        let receipt_hash = hash_serialized(receipt)?;
        if self.verified.manifest.receipt_hash != receipt_hash {
            anyhow::bail!(
                "manifest receipt_hash {} did not match repo receipt hash {} for {}",
                self.verified.manifest.receipt_hash,
                receipt_hash,
                self.verified.object_path.display()
            );
        }
        Ok(())
    }
}

fn streaming_dedupe_token<T: Serialize>(
    identity: &DeriveManifestIdentity,
    lane: &'static str,
    chunk_index: Option<u64>,
    value: &T,
) -> anyhow::Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"emojistats-backfill-streaming-derive-v1");
    hasher.update(serde_json::to_vec(identity)?);
    hasher.update(lane.as_bytes());
    if let Some(chunk_index) = chunk_index {
        hasher.update(chunk_index.to_be_bytes());
    }
    hasher.update(serde_json::to_vec(value)?);
    Ok(format!(
        "derive:{}:{}",
        lane,
        hex::encode(hasher.finalize())
    ))
}

fn hash_serialized<T: Serialize>(value: &T) -> anyhow::Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(value)?);
    Ok(hex::encode(hasher.finalize()))
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
    parse_concurrency: usize,
    max_inflight_spool_bytes: u64,
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
    summary.stale_recovered =
        recover_stale_claimed_entries(&ledger, &config.dids_file, SystemTime::now())?;
    let host_pacer = HostPacer::shared();
    let parse_permits = Arc::new(Semaphore::new(config.parse_concurrency));
    let byte_budget = FetchByteBudget::new(config.max_inflight_spool_bytes);
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
                    parse_permits: parse_permits.clone(),
                    byte_budget: byte_budget.clone(),
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
    let result = fetch_one_attempt_with_pacer(FetchOneAttemptConfig {
        did_str: &config.did,
        spool_dir: config.spool_dir,
        max_bytes: config.max_bytes,
        archive_dir: config.archive_dir,
        host_pacer: Some(config.host_pacer),
        parse_permits: Some(config.parse_permits),
        byte_budget: Some(config.byte_budget),
        claim_scope: &config.claim_scope,
        host_override_ledger_path: Some(&config.ledger_path),
    })
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

fn parse_positive_u64(value: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
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
    fetch_one_attempt_with_pacer(FetchOneAttemptConfig {
        did_str,
        spool_dir,
        max_bytes,
        archive_dir,
        host_pacer: None,
        parse_permits: None,
        byte_budget: None,
        claim_scope: &claim_scope,
        host_override_ledger_path: None,
    })
    .await
}

struct FetchOneAttemptConfig<'a> {
    did_str: &'a str,
    spool_dir: PathBuf,
    max_bytes: u64,
    archive_dir: PathBuf,
    host_pacer: Option<SharedHostPacer>,
    parse_permits: Option<Arc<Semaphore>>,
    byte_budget: Option<FetchByteBudget>,
    claim_scope: &'a ClaimScope,
    host_override_ledger_path: Option<&'a Path>,
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
    let host = prepare_fetch_host(
        did_str,
        &pds,
        config.claim_scope,
        config.host_override_ledger_path,
        config.host_pacer.as_ref(),
    )
    .await?;
    let http = reqwest::Client::new();
    let mut fetch_config = FetchConfig::new(config.spool_dir);
    fetch_config.max_bytes = config.max_bytes;
    fetch_config.byte_budget = config.byte_budget;

    let fetched = fetch_spooled_repo(FetchStep {
        http: &http,
        pds: &pds,
        did: &did,
        did_str,
        host: host.as_str(),
        config: &fetch_config,
        host_pacer: config.host_pacer.as_ref(),
        attempt_started,
    })
    .await?;
    println!(
        "spooled {} bytes from HTTP {} to {}",
        fetched.spooled.bytes,
        fetched.spooled.http_status,
        fetched.spooled.car_path.display()
    );

    let processed = parse_archive_or_emit_failure(
        did_str,
        host.as_str(),
        &fetched,
        &config.archive_dir,
        config.parse_permits.as_ref(),
        attempt_started,
    )
    .await?;
    println!(
        "parsed {} records, {} posts, {} decode errors, {} emoji rows, receipt {}",
        processed.records,
        processed.archived_posts,
        processed.decode_errors,
        processed.emoji_rows,
        processed.receipt_hash
    );
    println!(
        "wrote archive {}, receipt {}, manifest {}, emoji projection {}",
        processed.parquet_path.display(),
        processed.receipt_path.display(),
        processed.manifest_path.display(),
        processed.emoji_projection_path.display()
    );
    emit_smoke_telemetry(&SmokeTelemetry {
        event: "smoke_repo_attempt",
        did: did_str,
        host: Some(host.as_str()),
        outcome: "succeeded",
        stage: "complete",
        elapsed_ms: elapsed_ms(attempt_started),
        fetch_ms: Some(fetched.fetch_ms),
        parse_ms: Some(processed.parse_ms),
        archive_ms: Some(processed.archive_ms),
        bytes: Some(fetched.spooled.bytes),
        records: Some(processed.records),
        archived_posts: Some(processed.archived_posts),
        decode_errors: Some(processed.decode_errors),
        emoji_rows: Some(processed.emoji_rows),
        rss_kb: current_rss_kb(),
        error: None,
    });
    Ok(())
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
    match fetch_repo(step.http, step.pds, step.did, step.config).await {
        Ok(spooled) => Ok(FetchedRepo {
            spooled,
            fetch_ms: elapsed_ms(fetch_started),
        }),
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
            Err(failure)
        }
    }
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

async fn parse_archive_or_emit_failure(
    did_str: &str,
    host: &str,
    fetched: &FetchedRepo,
    archive_dir: &Path,
    parse_permits: Option<&Arc<Semaphore>>,
    attempt_started: Instant,
) -> Result<ProcessedRepo, FetchOneFailure> {
    emit_smoke_telemetry(&SmokeTelemetry {
        event: "smoke_repo_attempt",
        did: did_str,
        host: Some(host),
        outcome: "running",
        stage: "parse_wait",
        elapsed_ms: elapsed_ms(attempt_started),
        fetch_ms: Some(fetched.fetch_ms),
        parse_ms: None,
        archive_ms: None,
        bytes: Some(fetched.spooled.bytes),
        records: None,
        archived_posts: None,
        decode_errors: None,
        emoji_rows: None,
        rss_kb: current_rss_kb(),
        error: None,
    });
    let _permit =
        match parse_permits {
            Some(permits) => Some(permits.clone().acquire_owned().await.map_err(|_error| {
                retryable_failure("parse/archive semaphore closed".to_owned())
            })?),
            None => None,
        };
    emit_smoke_telemetry(&SmokeTelemetry {
        event: "smoke_repo_attempt",
        did: did_str,
        host: Some(host),
        outcome: "running",
        stage: "parse_start",
        elapsed_ms: elapsed_ms(attempt_started),
        fetch_ms: Some(fetched.fetch_ms),
        parse_ms: None,
        archive_ms: None,
        bytes: Some(fetched.spooled.bytes),
        records: None,
        archived_posts: None,
        decode_errors: None,
        emoji_rows: None,
        rss_kb: current_rss_kb(),
        error: None,
    });
    match parse_and_archive_spooled_repo(did_str, &fetched.spooled.car_path, archive_dir) {
        Ok(processed) => {
            emit_smoke_telemetry(&SmokeTelemetry {
                event: "smoke_repo_attempt",
                did: did_str,
                host: Some(host),
                outcome: "running",
                stage: "parse_archive_done",
                elapsed_ms: elapsed_ms(attempt_started),
                fetch_ms: Some(fetched.fetch_ms),
                parse_ms: Some(processed.parse_ms),
                archive_ms: Some(processed.archive_ms),
                bytes: Some(fetched.spooled.bytes),
                records: Some(processed.records),
                archived_posts: Some(processed.archived_posts),
                decode_errors: Some(processed.decode_errors),
                emoji_rows: Some(processed.emoji_rows),
                rss_kb: current_rss_kb(),
                error: None,
            });
            Ok(processed)
        }
        Err(failure) => {
            emit_smoke_telemetry(&SmokeTelemetry {
                event: "smoke_repo_attempt",
                did: did_str,
                host: Some(host),
                outcome: outcome_name(&failure.outcome),
                stage: "parse_archive",
                elapsed_ms: elapsed_ms(attempt_started),
                fetch_ms: Some(fetched.fetch_ms),
                parse_ms: None,
                archive_ms: None,
                bytes: Some(fetched.spooled.bytes),
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

#[derive(Debug)]
struct ProcessedRepo {
    records: u64,
    archived_posts: u64,
    decode_errors: u64,
    emoji_rows: u64,
    receipt_hash: String,
    parquet_path: PathBuf,
    receipt_path: PathBuf,
    manifest_path: PathBuf,
    emoji_projection_path: PathBuf,
    parse_ms: u64,
    archive_ms: u64,
}

fn parse_and_archive_spooled_repo(
    did_str: &str,
    car_path: &Path,
    archive_dir: &Path,
) -> Result<ProcessedRepo, FetchOneFailure> {
    let parse_started = Instant::now();
    let sink = StreamingArchiveSink::new(archive_dir, did_str).map_err(|err| {
        classify_archive_error(&format!("open streaming archive sink for {did_str}"), &err)
    })?;
    let normalizer = sink.normalizer().clone();
    let did = did_str.to_owned();
    let (parsed, sink) = parse_repo_for_did_with_state(
        car_path,
        did_str,
        ParseConfig::default(),
        sink,
        move |sink, post| {
            let row = archive_row_from_post(&did, &post, &normalizer)?;
            sink.push_row(row)
        },
    )
    .map_err(|err| match err {
        ParseVisitError::Parse(err) => classify_parse_error(did_str, &err),
        ParseVisitError::Visit(err) => {
            classify_archive_error(&format!("stream archive row for {did_str}"), &err)
        }
    })?;
    let parse_ms = elapsed_ms(parse_started);
    let archive_started = Instant::now();
    let profile_row_hash = hash_profile_record(parsed.profile.as_ref())
        .map_err(|err| classify_archive_error(&format!("hash profile row for {did_str}"), &err))?;
    let (receipt, artifacts) = sink
        .finish(
            StreamingReceiptInput {
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
    Ok(ProcessedRepo {
        records: parsed.rkey_digest.all_records_count,
        archived_posts: receipt.archived_post_rows_count,
        decode_errors: parsed.record_decode_error_count,
        emoji_rows: artifacts.emoji_rows,
        receipt_hash: receipt.post_rows_hash,
        parquet_path: artifacts.parquet_path,
        receipt_path: artifacts.receipt_path,
        manifest_path: artifacts.manifest_path,
        emoji_projection_path: artifacts.emoji_projection_path,
        parse_ms,
        archive_ms: elapsed_ms(archive_started),
    })
}

#[derive(Serialize)]
struct SmokeTelemetry<'a> {
    event: &'static str,
    did: &'a str,
    host: Option<&'a str>,
    outcome: &'static str,
    stage: &'static str,
    elapsed_ms: u64,
    fetch_ms: Option<u64>,
    parse_ms: Option<u64>,
    archive_ms: Option<u64>,
    bytes: Option<u64>,
    records: Option<u64>,
    archived_posts: Option<u64>,
    decode_errors: Option<u64>,
    emoji_rows: Option<u64>,
    rss_kb: Option<u64>,
    error: Option<String>,
}

fn emit_smoke_telemetry(telemetry: &SmokeTelemetry<'_>) {
    match serde_json::to_string(telemetry) {
        Ok(line) => println!("smoke_telemetry {line}"),
        Err(error) => eprintln!("failed to serialize smoke telemetry: {error}"),
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn current_rss_kb() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?.trim();
        let kb = value.split_whitespace().next()?;
        kb.parse::<u64>().ok()
    })
}

const fn outcome_name(outcome: &AttemptOutcome) -> &'static str {
    match outcome {
        AttemptOutcome::Succeeded => "succeeded",
        AttemptOutcome::AccountState(_) => "account_state",
        AttemptOutcome::RateLimited { .. } => "rate_limited",
        AttemptOutcome::RetryableFailure { .. } => "retryable_failure",
        AttemptOutcome::ResourceLimitExceeded { .. } => "resource_limit_exceeded",
        AttemptOutcome::PermanentFailure { .. } => "permanent_failure",
    }
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
        | FetchError::Io { .. }
        | FetchError::ByteBudgetPoisoned => AttemptOutcome::RetryableFailure {
            message: message.clone(),
        },
        FetchError::MaxBytesExceeded { .. }
        | FetchError::ErrorBodyTooLarge { .. }
        | FetchError::InFlightBytesExceeded { .. } => AttemptOutcome::ResourceLimitExceeded {
            message: message.clone(),
        },
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
            parse_concurrency,
            max_inflight_spool_bytes,
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
        assert_eq!(parse_concurrency, 1);
        assert_eq!(max_inflight_spool_bytes, 536_870_912);
        assert_eq!(shard_bucket, None);
        assert_eq!(spool_dir, PathBuf::from("data/spool"));
        assert_eq!(max_bytes, 2_147_483_648);
        assert_eq!(archive_dir, PathBuf::from("data/archive"));
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
        ])
        .unwrap();
        let Command::RunFleet {
            parse_concurrency,
            max_inflight_spool_bytes,
            ..
        } = cli.command
        else {
            unreachable!("expected run-fleet command");
        };

        assert_eq!(parse_concurrency, 2);
        assert_eq!(max_inflight_spool_bytes, 123_456);
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
