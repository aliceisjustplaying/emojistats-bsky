use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Cursor, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use emojistats_backfill::{
    archive::archive_post_rows_from_record_batch,
    clickhouse::{
        ClickHouseClientConfig, ClickHouseInsertContext, ClickHouseInsertPayload,
        ClickHouseInsertReceipt, derive_payload_exists, execute_insert_payloads,
    },
    manifest_derive::{
        LoaderInput, ManifestReadItem, VerifiedLoaderInput, stream_committed_jsonl,
        verify_loader_input_for_streaming,
    },
    metrics::{MetricLabels, MetricName, MetricStage, PressureState, SharedMetricsRecorder},
};
use fs4::{FileExt, TryLockError};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempPath};

use super::{add_count, count_len, increment, payload_row_count};

#[path = "derive_manifest_cmd/ledger.rs"]
mod ledger;
#[path = "derive_manifest_cmd/streaming.rs"]
mod streaming;

#[cfg(test)]
use emojistats_backfill::clickhouse::DEFAULT_POST_SERVING_PAYLOAD_MAX_BYTES;
#[cfg(test)]
use emojistats_backfill::derive::{
    DeriveManifestIdentity, PostServingRow, TotalPostCounterInput,
    canonical_streaming_counter_dedupe_token, canonical_streaming_post_dedupe_token,
};
use ledger::DeriveLedger;
use streaming::CanonicalStreamingPayloadState;

pub struct DeriveManifestConfig {
    pub manifest_path: PathBuf,
    pub archive_root: PathBuf,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub dry_run: bool,
    pub derive_ledger_path: Option<PathBuf>,
    pub claim_config: Option<DeriveManifestClaimConfig>,
    pub throttle_config: Option<ClickHouseInsertThrottleConfig>,
    pub metrics: SharedMetricsRecorder,
}

pub struct DeriveManifestClaimConfig {
    pub ledger_path: PathBuf,
    pub worker_id: String,
    pub max_entries: usize,
    pub max_rows: u64,
    pub stale_seconds: u64,
}

pub struct ClickHouseInsertThrottleConfig {
    pub slots_dir: PathBuf,
    pub slots: usize,
    pub max_wait_seconds: u64,
}

#[derive(Debug, Default, Serialize)]
struct DeriveManifestSummary {
    manifest_entries: u64,
    skipped_entries: u64,
    archive_files: u64,
    skipped_insert_payloads: u64,
    skipped_insert_rows: u64,
    attempted_insert_payloads: u64,
    attempted_insert_rows: u64,
    inserted_payloads: u64,
    inserted_rows: u64,
}

struct DeriveRunContext<'a> {
    http: &'a reqwest::Client,
    clickhouse: &'a ClickHouseClientConfig,
    dry_run: bool,
    derive_ledger: &'a mut DeriveLedger,
    summary: &'a mut DeriveManifestSummary,
    metrics: &'a SharedMetricsRecorder,
    throttle: Option<ClickHouseInsertThrottle>,
}

#[derive(Default)]
struct PendingDryRunInserts {
    attempted_payloads: u64,
    attempted_rows: u64,
}

impl PendingDryRunInserts {
    fn add_payloads(&mut self, payloads: &[ClickHouseInsertPayload]) -> anyhow::Result<()> {
        add_count(
            &mut self.attempted_payloads,
            count_len(payloads.len(), "derive payload count")?,
            "derive attempted payload total",
        )?;
        add_count(
            &mut self.attempted_rows,
            payload_row_count(payloads)?,
            "derive attempted row total",
        )
    }

    fn commit(self, summary: &mut DeriveManifestSummary) -> anyhow::Result<()> {
        add_count(
            &mut summary.attempted_insert_payloads,
            self.attempted_payloads,
            "derive attempted payload total",
        )?;
        add_count(
            &mut summary.attempted_insert_rows,
            self.attempted_rows,
            "derive attempted row total",
        )
    }
}

#[derive(Default)]
struct StagedDerivePayloads {
    payloads: Vec<StagedDerivePayload>,
}

struct StagedDerivePayload {
    table: emojistats_backfill::clickhouse::ClickHouseTable,
    format: &'static str,
    body_path: TempPath,
    row_count: usize,
    dedupe_token: String,
    checkpoint_key: emojistats_backfill::derive::DeriveCheckpointKey,
}

impl StagedDerivePayloads {
    fn add_payloads(&mut self, payloads: &[ClickHouseInsertPayload]) -> anyhow::Result<()> {
        for payload in payloads {
            self.payloads
                .push(StagedDerivePayload::from_payload(payload)?);
        }
        Ok(())
    }
}

impl StagedDerivePayload {
    fn from_payload(payload: &ClickHouseInsertPayload) -> anyhow::Result<Self> {
        let mut body_file = NamedTempFile::new()?;
        body_file.write_all(payload.body.as_bytes())?;
        body_file.flush()?;
        Ok(Self {
            table: payload.table,
            format: payload.format,
            body_path: body_file.into_temp_path(),
            row_count: payload.row_count,
            dedupe_token: payload.dedupe_token.clone(),
            checkpoint_key: payload.checkpoint_key.clone(),
        })
    }

    fn into_payload(self) -> anyhow::Result<ClickHouseInsertPayload> {
        let body = fs::read_to_string(&self.body_path)?;
        Ok(ClickHouseInsertPayload {
            table: self.table,
            format: self.format,
            body,
            row_count: self.row_count,
            dedupe_token: self.dedupe_token,
            checkpoint_key: self.checkpoint_key,
        })
    }
}

pub async fn run(config: DeriveManifestConfig) -> anyhow::Result<()> {
    if config.claim_config.is_some() && !config.dry_run && config.derive_ledger_path.is_none() {
        anyhow::bail!(
            "derive-manifest --claim-ledger-path requires --derive-ledger-path unless --dry-run is set"
        );
    }
    let claim = match &config.claim_config {
        Some(claim_config) => {
            let ledger = ManifestClaimLedger::new(
                &claim_config.ledger_path,
                &config.manifest_path,
                claim_config,
            )?;
            let Some(claim) = ledger.claim_next()? else {
                let summary = DeriveManifestSummary::default();
                println!("derive_manifest_claim none");
                println!(
                    "derive_manifest_summary {}",
                    serde_json::to_string(&summary)?
                );
                return Ok(());
            };
            eprintln!(
                "derive-manifest claimed chunk {} lines {}..={} rows {} entries {}",
                claim.chunk_id, claim.start_line, claim.end_line, claim.row_count, claim.entries
            );
            Some(claim)
        }
        None => None,
    };
    let clickhouse = ClickHouseClientConfig::new(
        &config.clickhouse_url,
        &config.clickhouse_database,
        config.clickhouse_user,
        config.clickhouse_password,
        "emojistats-backfill-derive",
    )?;
    let http = clickhouse.http_client()?;
    let mut summary = DeriveManifestSummary::default();
    let mut derive_ledger = DeriveLedger::new(config.derive_ledger_path.as_deref())?;
    let throttle = config
        .throttle_config
        .map(ClickHouseInsertThrottle::new)
        .transpose()?;
    let mut derive_context = DeriveRunContext {
        http: &http,
        clickhouse: &clickhouse,
        dry_run: config.dry_run,
        derive_ledger: &mut derive_ledger,
        summary: &mut summary,
        metrics: &config.metrics,
        throttle,
    };
    let manifest_reader = manifest_reader_for_claim(&config.manifest_path, claim.as_ref())?;

    for item in stream_committed_jsonl(manifest_reader) {
        match item? {
            ManifestReadItem::Input(input) => {
                increment(
                    &mut derive_context.summary.manifest_entries,
                    "manifest entry count",
                )?;
                derive_context.metrics.increment_counter(
                    MetricName::DeriveManifestEntriesSeenTotal,
                    derive_metric_labels(
                        None,
                        Some(MetricStage::DeriveManifestScan.as_str()),
                        None,
                    ),
                    1,
                );
                derive_loader_input_canonical_streaming(
                    &config.archive_root,
                    &input,
                    &mut derive_context,
                )
                .await?;
            }
            ManifestReadItem::Skipped(_skip) => {
                increment(
                    &mut derive_context.summary.skipped_entries,
                    "skipped manifest entry count",
                )?;
            }
        }
    }

    if let (Some(claim_config), Some(claim)) = (&config.claim_config, &claim) {
        let ledger = ManifestClaimLedger::new(
            &claim_config.ledger_path,
            &config.manifest_path,
            claim_config,
        )?;
        ledger.complete(claim)?;
    }

    println!(
        "derive_manifest_summary {}",
        serde_json::to_string(derive_context.summary)?
    );
    Ok(())
}

async fn derive_loader_input_canonical_streaming(
    archive_root: &Path,
    input: &LoaderInput,
    context: &mut DeriveRunContext<'_>,
) -> anyhow::Result<()> {
    let verified = verify_loader_input_for_streaming(archive_root, input)?;
    derive_verified_input_canonical_streaming(&verified, context).await
}

async fn derive_verified_input_canonical_streaming(
    verified: &VerifiedLoaderInput,
    context: &mut DeriveRunContext<'_>,
) -> anyhow::Result<()> {
    stream_verified_input_payloads_canonical_streaming(verified, context).await?;
    context.metrics.increment_counter(
        MetricName::DeriveRowsVerifiedTotal,
        derive_metric_labels(
            Some(verified),
            Some(MetricStage::DeriveReceiptVerify.as_str()),
            Some("verified"),
        ),
        verified.repo_receipt.archived_post_rows_count,
    );
    context.metrics.increment_counter(
        MetricName::DeriveFilesReadTotal,
        derive_metric_labels(
            Some(verified),
            Some(MetricStage::DeriveFileRead.as_str()),
            Some("read"),
        ),
        1,
    );
    increment(
        &mut context.summary.archive_files,
        "derive archive file count",
    )
}

async fn stream_verified_input_payloads_canonical_streaming(
    verified: &VerifiedLoaderInput,
    context: &mut DeriveRunContext<'_>,
) -> anyhow::Result<()> {
    let file = File::open(&verified.object_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut state = CanonicalStreamingPayloadState::new(verified);
    let mut pending_dry_run = PendingDryRunInserts::default();
    let mut staged_payloads = StagedDerivePayloads::default();

    for batch in reader {
        let rows = archive_post_rows_from_record_batch(&batch?)?;
        let payloads = state.consume_rows(&rows)?;
        stage_streamed_derive_payloads(
            context,
            &payloads,
            &mut pending_dry_run,
            &mut staged_payloads,
        )?;
    }

    let payloads = state.finish()?;
    stage_streamed_derive_payloads(
        context,
        &payloads,
        &mut pending_dry_run,
        &mut staged_payloads,
    )?;
    if context.dry_run {
        pending_dry_run.commit(context.summary)?;
    } else {
        apply_staged_derive_payloads(context, verified, staged_payloads).await?;
    }
    Ok(())
}

fn stage_streamed_derive_payloads(
    context: &DeriveRunContext<'_>,
    payloads: &[ClickHouseInsertPayload],
    pending_dry_run: &mut PendingDryRunInserts,
    staged_payloads: &mut StagedDerivePayloads,
) -> anyhow::Result<()> {
    if context.dry_run {
        pending_dry_run.add_payloads(payloads)
    } else {
        staged_payloads.add_payloads(payloads)
    }
}

async fn apply_staged_derive_payloads(
    context: &mut DeriveRunContext<'_>,
    verified: &VerifiedLoaderInput,
    staged_payloads: StagedDerivePayloads,
) -> anyhow::Result<()> {
    for staged_payload in staged_payloads.payloads {
        let payload = staged_payload.into_payload()?;
        apply_derive_payloads(context, verified, std::slice::from_ref(&payload)).await?;
    }
    Ok(())
}

async fn apply_derive_payloads(
    context: &mut DeriveRunContext<'_>,
    verified: &VerifiedLoaderInput,
    payloads: &[ClickHouseInsertPayload],
) -> anyhow::Result<()> {
    if payloads.is_empty() {
        return Ok(());
    }
    if context.dry_run {
        add_count(
            &mut context.summary.attempted_insert_payloads,
            count_len(payloads.len(), "derive payload count")?,
            "derive attempted payload total",
        )?;
        let attempted_rows = payload_row_count(payloads)?;
        add_count(
            &mut context.summary.attempted_insert_rows,
            attempted_rows,
            "derive attempted row total",
        )?;
        return Ok(());
    }

    for payload in payloads {
        let payload_rows = count_len(payload.row_count, "derive payload row count")?;
        if payload_completed(context, verified, payload).await? {
            increment(
                &mut context.summary.skipped_insert_payloads,
                "skipped payload total",
            )?;
            add_count(
                &mut context.summary.skipped_insert_rows,
                payload_rows,
                "skipped row total",
            )?;
            continue;
        }

        increment(
            &mut context.summary.attempted_insert_payloads,
            "derive attempted payload total",
        )?;
        add_count(
            &mut context.summary.attempted_insert_rows,
            payload_rows,
            "derive attempted row total",
        )?;
        let _insert_slot = match &context.throttle {
            Some(throttle) => {
                let wait_started = Instant::now();
                let guard = throttle.acquire().await?;
                let waited = wait_started.elapsed().as_secs_f64();
                if waited >= 0.001 {
                    context.metrics.record_histogram(
                        MetricName::DeriveBatchDurationSeconds,
                        derive_pressure_metric_labels(
                            Some(verified),
                            Some(MetricStage::DeriveClickHouseCommit.as_str()),
                            Some("insert_slot_acquired"),
                            PressureState::ClickHouseBackpressure.as_str(),
                        ),
                        waited,
                    );
                }
                Some(guard)
            }
            None => None,
        };
        let insert_started = Instant::now();
        let mut receipts = execute_insert_payloads(
            context.http,
            context.clickhouse,
            std::slice::from_ref(payload),
        )
        .await?;
        let insert_seconds = insert_started.elapsed().as_secs_f64();
        let receipt = receipts
            .pop()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse insert returned no receipt"))?;
        context
            .derive_ledger
            .append_success(verified, payload, &receipt)?;
        record_insert_metrics(context, verified, &receipt, insert_seconds)?;
        increment(
            &mut context.summary.inserted_payloads,
            "inserted payload total",
        )?;
        add_count(
            &mut context.summary.inserted_rows,
            count_len(receipt.context.row_count, "inserted row count")?,
            "inserted row total",
        )?;
    }
    Ok(())
}

async fn payload_completed(
    context: &mut DeriveRunContext<'_>,
    verified: &VerifiedLoaderInput,
    payload: &ClickHouseInsertPayload,
) -> anyhow::Result<bool> {
    if context.derive_ledger.is_completed(verified, payload)? {
        return Ok(true);
    }
    if !context.derive_ledger.is_durable()
        || !derive_payload_exists(context.http, context.clickhouse, payload).await?
    {
        return Ok(false);
    }
    let receipt = ClickHouseInsertReceipt {
        context: ClickHouseInsertContext {
            table: payload.table,
            row_count: payload.row_count,
            dedupe_token: payload.dedupe_token.clone(),
            insert_deduplicate: true,
        },
        status: 200,
        response_snippet: Some("derive payload already present in ClickHouse".to_owned()),
    };
    context
        .derive_ledger
        .append_success(verified, payload, &receipt)?;
    Ok(true)
}

fn record_insert_metrics(
    context: &DeriveRunContext<'_>,
    verified: &VerifiedLoaderInput,
    receipt: &ClickHouseInsertReceipt,
    insert_seconds: f64,
) -> anyhow::Result<()> {
    let clickhouse_labels = derive_metric_labels(
        Some(verified),
        Some(MetricStage::ClickHouseInsert.as_str()),
        Some("committed"),
    );
    context.metrics.increment_counter(
        MetricName::ClickHouseInsertBatchesTotal,
        clickhouse_labels,
        1,
    );
    context.metrics.increment_counter(
        MetricName::ClickHouseInsertRowsTotal,
        clickhouse_labels,
        count_len(receipt.context.row_count, "inserted row count")?,
    );
    context.metrics.record_histogram(
        MetricName::ClickHouseInsertDurationSeconds,
        clickhouse_labels,
        insert_seconds,
    );

    let derive_labels = derive_metric_labels(
        Some(verified),
        Some(MetricStage::DeriveClickHouseCommit.as_str()),
        Some("committed"),
    );
    context.metrics.increment_counter(
        MetricName::DeriveClickHouseBatchesCommittedTotal,
        derive_labels,
        1,
    );
    context.metrics.record_histogram(
        MetricName::DeriveBatchDurationSeconds,
        derive_labels,
        insert_seconds,
    );
    Ok(())
}

fn derive_metric_labels<'a>(
    verified: Option<&'a VerifiedLoaderInput>,
    stage: Option<&'a str>,
    outcome: Option<&'a str>,
) -> MetricLabels<'a> {
    MetricLabels {
        run_id: verified.map(|input| input.manifest.run_id.as_str()),
        worker_id: None,
        shard: verified.map(|input| input.manifest.shard.as_str()),
        host: None,
        stage,
        outcome,
        pressure_state: None,
        backend: None,
    }
}

fn derive_pressure_metric_labels<'a>(
    verified: Option<&'a VerifiedLoaderInput>,
    stage: Option<&'a str>,
    outcome: Option<&'a str>,
    pressure_state: &'a str,
) -> MetricLabels<'a> {
    MetricLabels {
        run_id: verified.map(|input| input.manifest.run_id.as_str()),
        worker_id: None,
        shard: verified.map(|input| input.manifest.shard.as_str()),
        host: None,
        stage,
        outcome,
        pressure_state: Some(pressure_state),
        backend: None,
    }
}

fn manifest_reader_for_claim(
    manifest_path: &Path,
    claim: Option<&ClaimedManifestChunk>,
) -> anyhow::Result<Box<dyn BufRead + Send>> {
    if let Some(claim) = claim {
        let bytes = read_claimed_manifest_lines(manifest_path, claim.start_line, claim.end_line)?;
        Ok(Box::new(BufReader::new(Cursor::new(bytes))))
    } else {
        let file = File::open(manifest_path)?;
        Ok(Box::new(BufReader::new(file)))
    }
}

fn read_claimed_manifest_lines(
    manifest_path: &Path,
    start_line: u64,
    end_line: u64,
) -> anyhow::Result<Vec<u8>> {
    let file = File::open(manifest_path)?;
    let mut selected = Vec::new();
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line_number = u64::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| anyhow::anyhow!("manifest line number overflow"))?;
        if line_number < start_line {
            continue;
        }
        if line_number > end_line {
            break;
        }
        selected.extend_from_slice(line?.as_bytes());
        selected.push(b'\n');
    }
    Ok(selected)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManifestChunk {
    chunk_id: String,
    start_line: u64,
    end_line: u64,
    row_count: u64,
    entries: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ClaimedManifestChunk {
    manifest_path: String,
    chunk_id: String,
    start_line: u64,
    end_line: u64,
    row_count: u64,
    entries: u64,
    worker_id: String,
    claimed_at_unix_ms: u64,
    expires_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum ClaimLedgerRecord {
    Claim(ClaimedManifestChunk),
    Complete {
        manifest_path: String,
        chunk_id: String,
        worker_id: String,
        completed_at_unix_ms: u64,
    },
}

struct ManifestClaimLedger<'a> {
    ledger_path: &'a Path,
    manifest_path: &'a Path,
    manifest_key: String,
    config: &'a DeriveManifestClaimConfig,
}

impl<'a> ManifestClaimLedger<'a> {
    fn new(
        ledger_path: &'a Path,
        manifest_path: &'a Path,
        config: &'a DeriveManifestClaimConfig,
    ) -> anyhow::Result<Self> {
        if let Some(parent) = ledger_path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self {
            ledger_path,
            manifest_path,
            manifest_key: manifest_path.to_string_lossy().into_owned(),
            config,
        })
    }

    fn claim_next(&self) -> anyhow::Result<Option<ClaimedManifestChunk>> {
        let _lock = FileLock::acquire(self.ledger_path, "derive claim ledger")?;
        let state = ClaimLedgerState::read(self.ledger_path, self.manifest_key())?;
        let chunks = build_manifest_chunks(
            self.manifest_path,
            self.config.max_entries,
            self.config.max_rows,
        )?;
        let now = unix_ms()?;
        let stale_after = self
            .config
            .stale_seconds
            .checked_mul(1_000)
            .ok_or_else(|| anyhow::anyhow!("claim stale timeout overflow"))?;
        let expires_at = now
            .checked_add(stale_after)
            .ok_or_else(|| anyhow::anyhow!("claim expiry overflow"))?;
        let Some(chunk) = chunks
            .into_iter()
            .find(|chunk| state.is_claimable(chunk, now))
        else {
            return Ok(None);
        };
        let claim = ClaimedManifestChunk {
            manifest_path: self.manifest_key().to_owned(),
            chunk_id: chunk.chunk_id,
            start_line: chunk.start_line,
            end_line: chunk.end_line,
            row_count: chunk.row_count,
            entries: chunk.entries,
            worker_id: self.config.worker_id.clone(),
            claimed_at_unix_ms: now,
            expires_at_unix_ms: expires_at,
        };
        append_claim_record(self.ledger_path, &ClaimLedgerRecord::Claim(claim.clone()))?;
        Ok(Some(claim))
    }

    fn complete(&self, claim: &ClaimedManifestChunk) -> anyhow::Result<()> {
        let _lock = FileLock::acquire(self.ledger_path, "derive claim ledger")?;
        append_claim_record(
            self.ledger_path,
            &ClaimLedgerRecord::Complete {
                manifest_path: claim.manifest_path.clone(),
                chunk_id: claim.chunk_id.clone(),
                worker_id: self.config.worker_id.clone(),
                completed_at_unix_ms: unix_ms()?,
            },
        )
    }

    const fn manifest_key(&self) -> &str {
        self.manifest_key.as_str()
    }
}

struct ClaimLedgerState {
    completed: BTreeSet<String>,
    active_claims: BTreeMap<String, u64>,
}

impl ClaimLedgerState {
    fn read(path: &Path, manifest_key: &str) -> anyhow::Result<Self> {
        let mut completed = BTreeSet::new();
        let mut active_claims = BTreeMap::new();
        let contents = match fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(source) => return Err(source.into()),
        };
        for line in contents.lines().filter(|line| !line.trim().is_empty()) {
            let record: ClaimLedgerRecord = serde_json::from_str(line)?;
            match record {
                ClaimLedgerRecord::Claim(claim) if claim.manifest_path == manifest_key => {
                    active_claims.insert(claim.chunk_id, claim.expires_at_unix_ms);
                }
                ClaimLedgerRecord::Complete {
                    manifest_path,
                    chunk_id,
                    ..
                } if manifest_path == manifest_key => {
                    completed.insert(chunk_id.clone());
                    active_claims.remove(&chunk_id);
                }
                _ => {}
            }
        }
        Ok(Self {
            completed,
            active_claims,
        })
    }

    fn is_claimable(&self, chunk: &ManifestChunk, now_unix_ms: u64) -> bool {
        if self.completed.contains(&chunk.chunk_id) {
            return false;
        }
        self.active_claims
            .get(&chunk.chunk_id)
            .is_none_or(|expires_at| *expires_at <= now_unix_ms)
    }
}

fn append_claim_record(path: &Path, record: &ClaimLedgerRecord) -> anyhow::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, record)?;
    writeln!(file)?;
    file.sync_all()?;
    Ok(())
}

fn build_manifest_chunks(
    manifest_path: &Path,
    max_entries: usize,
    max_rows: u64,
) -> anyhow::Result<Vec<ManifestChunk>> {
    let file = File::open(manifest_path)?;
    let mut chunks = Vec::new();
    let mut current: Option<ManifestChunk> = None;

    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let line_number = u64::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| anyhow::anyhow!("manifest line number overflow"))?;
        let entry: emojistats_backfill::commit::ManifestEntry = serde_json::from_str(&line)?;
        let row_count = if entry.dataset == "raw_archive_posts" {
            entry.row_count
        } else {
            0
        };
        if let Some(chunk) = &current {
            let would_exceed_entries =
                usize::try_from(chunk.entries).is_ok_and(|entries| entries >= max_entries);
            let would_exceed_rows = chunk.row_count > 0
                && chunk
                    .row_count
                    .checked_add(row_count)
                    .is_none_or(|rows| rows > max_rows);
            if would_exceed_entries || would_exceed_rows {
                let Some(chunk) = current.take() else {
                    anyhow::bail!("manifest chunk state disappeared before rollover");
                };
                chunks.push(chunk);
            }
        }
        let chunk = current.get_or_insert_with(|| ManifestChunk {
            chunk_id: String::new(),
            start_line: line_number,
            end_line: line_number,
            row_count: 0,
            entries: 0,
        });
        chunk.end_line = line_number;
        chunk.row_count = chunk
            .row_count
            .checked_add(row_count)
            .ok_or_else(|| anyhow::anyhow!("manifest chunk row count overflow"))?;
        chunk.entries = chunk
            .entries
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("manifest chunk entry count overflow"))?;
    }
    if let Some(chunk) = current {
        chunks.push(chunk);
    }
    for chunk in &mut chunks {
        chunk.chunk_id = format!("lines-{}-{}", chunk.start_line, chunk.end_line);
    }
    Ok(chunks)
}

#[derive(Clone)]
struct ClickHouseInsertThrottle {
    slots_dir: PathBuf,
    slots: usize,
    max_wait: Duration,
}

impl ClickHouseInsertThrottle {
    fn new(config: ClickHouseInsertThrottleConfig) -> anyhow::Result<Self> {
        fs::create_dir_all(&config.slots_dir)?;
        Ok(Self {
            slots_dir: config.slots_dir,
            slots: config.slots,
            max_wait: Duration::from_secs(config.max_wait_seconds),
        })
    }

    async fn acquire(&self) -> anyhow::Result<InsertSlotGuard> {
        let started = Instant::now();
        loop {
            for slot in 0..self.slots {
                let path = self.slots_dir.join(format!("slot-{slot:04}.lock"));
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(&path)?;
                match FileExt::try_lock(&file) {
                    Ok(()) => return Ok(InsertSlotGuard { file }),
                    Err(TryLockError::WouldBlock) => {}
                    Err(TryLockError::Error(source)) => return Err(source.into()),
                }
            }
            if started.elapsed() >= self.max_wait {
                anyhow::bail!(
                    "timed out waiting for ClickHouse insert slot in {}",
                    self.slots_dir.display()
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

struct InsertSlotGuard {
    file: File,
}

impl Drop for InsertSlotGuard {
    fn drop(&mut self) {
        let _ignored = FileExt::unlock(&self.file);
    }
}

struct FileLock {
    file: File,
}

impl FileLock {
    fn acquire(path: &Path, name: &str) -> anyhow::Result<Self> {
        let lock_path = path.with_extension("lock");
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)?;
        let started = Instant::now();
        loop {
            match FileExt::try_lock(&file) {
                Ok(()) => return Ok(Self { file }),
                Err(TryLockError::WouldBlock) => {
                    if started.elapsed() >= Duration::from_secs(60) {
                        anyhow::bail!(
                            "timed out waiting for {name} lock at {}",
                            lock_path.display()
                        );
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(TryLockError::Error(source)) => return Err(source.into()),
            }
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ignored = FileExt::unlock(&self.file);
    }
}

fn unix_ms() -> anyhow::Result<u64> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    u64::try_from(millis).map_err(Into::into)
}

#[cfg(test)]
#[path = "derive_manifest_cmd/tests.rs"]
mod tests;
