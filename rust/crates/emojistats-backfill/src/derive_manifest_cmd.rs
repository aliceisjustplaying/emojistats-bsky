use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use emojistats_backfill::{
    archive::{ArchivePostRowsHasher, NormalizerVersion, archive_post_rows_from_record_batch},
    clickhouse::{
        ClickHouseClientConfig, ClickHouseInsertPayload, ClickHouseInsertReceipt,
        DEFAULT_POST_SERVING_PAYLOAD_MAX_BYTES, execute_insert_payloads,
        post_serving_row_body_bytes, post_serving_rows_insert_payload,
        total_post_counter_insert_payload_for_counter,
    },
    derive::{
        BACKFILL_DERIVE_SOURCE, DeriveManifestIdentity, PostServingRow, TotalPostCounterInput,
        post_serving_row_for_post,
    },
    hash::hash_serialized_json,
    manifest_derive::{
        LoaderInput, ManifestReadItem, VerifiedLoaderInput, stream_committed_jsonl,
        verify_loader_input_for_streaming,
    },
    metrics::{MetricLabels, MetricName, MetricStage, SharedMetricsRecorder},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{add_count, count_len, increment, payload_row_count};

const DERIVE_POST_CHUNK_ROWS: usize = 10_000;
// Canonical streaming derive tokens are lane/chunk-framed and use the same stable manifest
// identity fields as full-batch derive tokens: dataset, DID, proof hashes, schema, and normalizer.
const STREAMING_DEDUPE_TOKEN_DOMAIN: &str = "emojistats-backfill-streaming-derive-token-v1";

pub struct DeriveManifestConfig {
    pub manifest_path: PathBuf,
    pub archive_root: PathBuf,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub dry_run: bool,
    pub derive_ledger_path: Option<PathBuf>,
    pub metrics: SharedMetricsRecorder,
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
}

#[derive(Debug)]
struct DeriveLedger {
    path: Option<PathBuf>,
    completed: HashSet<DerivePayloadCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize, Serialize)]
struct DerivePayloadCheckpoint {
    source: String,
    content_hash: String,
    receipt_hash: String,
    table: String,
    dedupe_token: String,
    row_count: usize,
    payload_hash: String,
}

#[derive(Debug, serde::Deserialize, Serialize)]
struct DeriveLedgerRecord {
    checkpoint: DerivePayloadCheckpoint,
    run_id: String,
    shard: String,
    file_sequence: u64,
    dataset: String,
    schema_version: u16,
    object_path: String,
    clickhouse_status: u16,
}

impl DeriveLedger {
    fn new(path: Option<&Path>) -> anyhow::Result<Self> {
        if let Some(path) = path
            && let Some(parent) = path.parent()
        {
            fs::create_dir_all(parent)?;
        }
        let completed = match path {
            Some(path) if path.try_exists()? => Self::read_completed(path)?,
            Some(_) | None => HashSet::new(),
        };
        Ok(Self {
            path: path.map(Path::to_path_buf),
            completed,
        })
    }

    fn is_completed(
        &self,
        verified: &VerifiedLoaderInput,
        payload: &ClickHouseInsertPayload,
    ) -> bool {
        self.completed
            .contains(&Self::checkpoint(verified, payload))
    }

    fn append_success(
        &mut self,
        verified: &VerifiedLoaderInput,
        payload: &ClickHouseInsertPayload,
        receipt: &ClickHouseInsertReceipt,
    ) -> anyhow::Result<()> {
        let checkpoint = Self::checkpoint(verified, payload);
        let Some(path) = &self.path else {
            self.completed.insert(checkpoint);
            return Ok(());
        };
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        let record = DeriveLedgerRecord {
            checkpoint: checkpoint.clone(),
            run_id: verified.manifest.run_id.clone(),
            shard: verified.manifest.shard.clone(),
            file_sequence: verified.manifest.file_sequence,
            dataset: verified.manifest.dataset.clone(),
            schema_version: verified.manifest.schema_version,
            object_path: verified.object_path.to_string_lossy().into_owned(),
            clickhouse_status: receipt.status,
        };
        serde_json::to_writer(&mut file, &record)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        self.completed.insert(checkpoint);
        Ok(())
    }

    fn read_completed(path: &Path) -> anyhow::Result<HashSet<DerivePayloadCheckpoint>> {
        let file = File::open(path)?;
        let mut completed = HashSet::new();
        for (line_index, line) in BufReader::new(file).lines().enumerate() {
            let line = line?;
            let line_number = line_index
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("derive ledger line number overflow"))?;
            if line.trim().is_empty() {
                continue;
            }
            let record: DeriveLedgerRecord = serde_json::from_str(&line).map_err(|source| {
                anyhow::anyhow!(
                    "parse derive ledger {} line {}: {source}",
                    path.display(),
                    line_number
                )
            })?;
            completed.insert(record.checkpoint);
        }
        Ok(completed)
    }

    fn checkpoint(
        verified: &VerifiedLoaderInput,
        payload: &ClickHouseInsertPayload,
    ) -> DerivePayloadCheckpoint {
        DerivePayloadCheckpoint {
            source: BACKFILL_DERIVE_SOURCE.to_owned(),
            content_hash: verified.manifest.content_hash.clone(),
            receipt_hash: verified.manifest.receipt_hash.clone(),
            table: payload.table.name().to_owned(),
            dedupe_token: payload.dedupe_token.clone(),
            row_count: payload.row_count,
            payload_hash: hash_payload_body(&payload.body),
        }
    }
}

pub async fn run(config: DeriveManifestConfig) -> anyhow::Result<()> {
    let file = File::open(&config.manifest_path)?;
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
    let mut derive_context = DeriveRunContext {
        http: &http,
        clickhouse: &clickhouse,
        dry_run: config.dry_run,
        derive_ledger: &mut derive_ledger,
        summary: &mut summary,
        metrics: &config.metrics,
    };

    for item in stream_committed_jsonl(BufReader::new(file)) {
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
    validate_canonical_streaming_proof(verified)?;
    context.metrics.increment_counter(
        MetricName::DeriveRowsVerifiedTotal,
        derive_metric_labels(
            Some(verified),
            Some(MetricStage::DeriveReceiptVerify.as_str()),
            Some("verified"),
        ),
        verified.repo_receipt.archived_post_rows_count,
    );
    insert_verified_input_canonical_streaming(verified, context).await?;
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

fn validate_canonical_streaming_proof(verified: &VerifiedLoaderInput) -> anyhow::Result<()> {
    let file = File::open(&verified.object_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut state = CanonicalStreamingValidationState::new(verified);

    for batch in reader {
        let rows = archive_post_rows_from_record_batch(&batch?)?;
        state.consume_rows(&rows)?;
    }

    state.finish()
}

async fn insert_verified_input_canonical_streaming(
    verified: &VerifiedLoaderInput,
    context: &mut DeriveRunContext<'_>,
) -> anyhow::Result<()> {
    let file = File::open(&verified.object_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut state = CanonicalStreamingPayloadState::new(verified);

    for batch in reader {
        let rows = archive_post_rows_from_record_batch(&batch?)?;
        let payloads = state.consume_rows(&rows)?;
        apply_derive_payloads(context, verified, &payloads).await?;
    }

    let payloads = state.finish()?;
    apply_derive_payloads(context, verified, &payloads).await
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
        if context.derive_ledger.is_completed(verified, payload) {
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

struct CanonicalStreamingValidationState<'a> {
    verified: &'a VerifiedLoaderInput,
    row_hasher: ArchivePostRowsHasher,
    rows: u64,
}

impl<'a> CanonicalStreamingValidationState<'a> {
    fn new(verified: &'a VerifiedLoaderInput) -> Self {
        Self {
            verified,
            row_hasher: ArchivePostRowsHasher::new(),
            rows: 0,
        }
    }

    fn consume_rows(
        &mut self,
        rows: &[emojistats_backfill::archive::ArchivePostRow],
    ) -> anyhow::Result<()> {
        for row in rows {
            self.row_hasher.push_row(row)?;
            increment(&mut self.rows, "streaming derive validation row count")?;
        }
        Ok(())
    }

    fn finish(mut self) -> anyhow::Result<()> {
        let row_hash = std::mem::take(&mut self.row_hasher).finish();
        self.validate_receipt(&row_hash)
    }

    fn validate_receipt(&self, row_hash: &str) -> anyhow::Result<()> {
        if self.verified.manifest.row_count != self.rows {
            anyhow::bail!(
                "manifest row_count {} did not match streamed archive row count {} for {}",
                self.verified.manifest.row_count,
                self.rows,
                self.verified.object_path.display()
            );
        }
        let receipt = &self.verified.repo_receipt;
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
        let receipt_hash = hash_serialized_json(receipt)?;
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

struct CanonicalStreamingPayloadState<'a> {
    verified: &'a VerifiedLoaderInput,
    rows: u64,
    posts_with_emojis: u64,
    emoji_occurrences: u64,
    post_chunk_rows: Vec<PostServingRow>,
    post_chunk_body_bytes: usize,
    post_chunk_index: u64,
}

impl<'a> CanonicalStreamingPayloadState<'a> {
    fn new(verified: &'a VerifiedLoaderInput) -> Self {
        Self {
            verified,
            rows: 0,
            posts_with_emojis: 0,
            emoji_occurrences: 0,
            post_chunk_rows: Vec::with_capacity(DERIVE_POST_CHUNK_ROWS),
            post_chunk_body_bytes: 0,
            post_chunk_index: 0,
        }
    }

    fn consume_rows(
        &mut self,
        rows: &[emojistats_backfill::archive::ArchivePostRow],
    ) -> anyhow::Result<Vec<ClickHouseInsertPayload>> {
        let mut payloads = Vec::new();
        for row in rows {
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
            let post_row = post_serving_row_for_post(row);
            let row_body_bytes = post_serving_row_body_bytes(&self.verified.identity, &post_row)?;
            validate_post_row_payload_size(row_body_bytes, &post_row)?;
            self.flush_post_chunk_if_needed(row_body_bytes, &mut payloads)?;
            self.post_chunk_rows.push(post_row);
            self.post_chunk_body_bytes = self
                .post_chunk_body_bytes
                .checked_add(row_body_bytes)
                .ok_or_else(|| anyhow::anyhow!("streaming derive post payload bytes overflow"))?;
            if self.post_chunk_rows.len() >= DERIVE_POST_CHUNK_ROWS {
                payloads.push(self.flush_post_chunk()?);
            }
        }
        Ok(payloads)
    }

    fn flush_post_chunk_if_needed(
        &mut self,
        row_body_bytes: usize,
        payloads: &mut Vec<ClickHouseInsertPayload>,
    ) -> anyhow::Result<()> {
        if self.post_chunk_rows.is_empty() {
            return Ok(());
        }
        let next_bytes = self
            .post_chunk_body_bytes
            .checked_add(row_body_bytes)
            .ok_or_else(|| anyhow::anyhow!("streaming derive post payload bytes overflow"))?;
        if next_bytes > DEFAULT_POST_SERVING_PAYLOAD_MAX_BYTES {
            payloads.push(self.flush_post_chunk()?);
        }
        Ok(())
    }

    fn finish(mut self) -> anyhow::Result<Vec<ClickHouseInsertPayload>> {
        let mut payloads = Vec::new();
        if !self.post_chunk_rows.is_empty() {
            payloads.push(self.flush_post_chunk()?);
        }
        let counter = TotalPostCounterInput {
            source: BACKFILL_DERIVE_SOURCE.to_owned(),
            run_id: self.verified.manifest.run_id.clone(),
            shard: self.verified.manifest.shard.clone(),
            file_sequence: self.verified.manifest.file_sequence,
            did: self.verified.manifest.did.clone(),
            dataset: self.verified.identity.dataset.clone(),
            fetch_method: self.verified.identity.fetch_method.clone(),
            completeness_class: self.verified.identity.completeness_class.clone(),
            receipt_hash: self.verified.manifest.receipt_hash.clone(),
            normalizer: self.verified.manifest.normalizer.clone(),
            posts_processed: self.rows,
            posts_with_emojis: self.posts_with_emojis,
            emoji_occurrences: self.emoji_occurrences,
            min_created_at_normalized: self.verified.manifest.min_created_at_normalized.clone(),
            max_created_at_normalized: self.verified.manifest.max_created_at_normalized.clone(),
        };
        let token = canonical_streaming_counter_dedupe_token(&self.verified.identity, &counter)?;
        payloads.push(total_post_counter_insert_payload_for_counter(
            &counter, token,
        )?);
        Ok(payloads)
    }

    fn flush_post_chunk(&mut self) -> anyhow::Result<ClickHouseInsertPayload> {
        let rows = std::mem::take(&mut self.post_chunk_rows);
        let token = canonical_streaming_post_dedupe_token(
            &self.verified.identity,
            self.post_chunk_index,
            &rows,
        )?;
        increment(
            &mut self.post_chunk_index,
            "streaming derive post chunk index",
        )?;
        self.post_chunk_rows = Vec::with_capacity(DERIVE_POST_CHUNK_ROWS);
        self.post_chunk_body_bytes = 0;
        Ok(post_serving_rows_insert_payload(
            &self.verified.identity,
            &rows,
            token,
        )?)
    }
}

fn validate_post_row_payload_size(
    row_body_bytes: usize,
    row: &PostServingRow,
) -> anyhow::Result<()> {
    if row_body_bytes <= DEFAULT_POST_SERVING_PAYLOAD_MAX_BYTES {
        return Ok(());
    }
    anyhow::bail!(
        "post serving row exceeds payload byte cap before chunking: did={} rkey={} row_body_bytes={} max={}",
        row.did,
        row.rkey,
        row_body_bytes,
        DEFAULT_POST_SERVING_PAYLOAD_MAX_BYTES
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamingDedupeLane {
    Post,
    Counter,
}

impl StreamingDedupeLane {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Post => "post",
            Self::Counter => "counter",
        }
    }
}

fn canonical_streaming_post_dedupe_token(
    identity: &DeriveManifestIdentity,
    chunk_index: u64,
    rows: &[PostServingRow],
) -> anyhow::Result<String> {
    let mut hasher =
        canonical_streaming_dedupe_hasher(identity, StreamingDedupeLane::Post, Some(chunk_index))?;
    hash_str_frame(&mut hasher, "payload.kind", "post_rows")?;
    hash_u64_frame(
        &mut hasher,
        "post_rows.len",
        count_len(rows.len(), "streaming dedupe post row count")?,
    )?;
    for (index, row) in rows.iter().enumerate() {
        hash_u64_frame(
            &mut hasher,
            "post_row.index",
            count_len(index, "streaming dedupe post row index")?,
        )?;
        hash_post_row_frames(&mut hasher, row)?;
    }
    Ok(streaming_dedupe_token(StreamingDedupeLane::Post, hasher))
}

fn canonical_streaming_counter_dedupe_token(
    identity: &DeriveManifestIdentity,
    counter: &TotalPostCounterInput,
) -> anyhow::Result<String> {
    let mut hasher =
        canonical_streaming_dedupe_hasher(identity, StreamingDedupeLane::Counter, None)?;
    hash_str_frame(&mut hasher, "payload.kind", "total_post_counter")?;
    hash_counter_frames(&mut hasher, counter)?;
    Ok(streaming_dedupe_token(StreamingDedupeLane::Counter, hasher))
}

fn canonical_streaming_dedupe_hasher(
    identity: &DeriveManifestIdentity,
    lane: StreamingDedupeLane,
    chunk_index: Option<u64>,
) -> anyhow::Result<Sha256> {
    let mut hasher = Sha256::new();
    hash_str_frame(&mut hasher, "domain", STREAMING_DEDUPE_TOKEN_DOMAIN)?;
    hash_str_frame(&mut hasher, "lane", lane.as_str())?;
    hash_optional_u64_frame(&mut hasher, "chunk_index", chunk_index)?;
    hash_identity_frames(&mut hasher, identity)?;
    Ok(hasher)
}

fn streaming_dedupe_token(lane: StreamingDedupeLane, hasher: Sha256) -> String {
    format!(
        "derive:{}:{}",
        lane.as_str(),
        hex::encode(hasher.finalize())
    )
}

fn hash_payload_body(body: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body.as_bytes());
    hex::encode(hasher.finalize())
}

fn hash_identity_frames(
    hasher: &mut Sha256,
    identity: &DeriveManifestIdentity,
) -> anyhow::Result<()> {
    hash_str_frame(hasher, "identity.dataset", &identity.dataset)?;
    hash_str_frame(hasher, "identity.did", &identity.did)?;
    hash_str_frame(hasher, "identity.fetch_method", &identity.fetch_method)?;
    hash_str_frame(
        hasher,
        "identity.completeness_class",
        &identity.completeness_class,
    )?;
    hash_str_frame(hasher, "identity.content_hash", &identity.content_hash)?;
    hash_str_frame(hasher, "identity.receipt_hash", &identity.receipt_hash)?;
    hash_str_frame(hasher, "identity.observed_at", &identity.observed_at)?;
    hash_u16_frame(hasher, "identity.schema_version", identity.schema_version)?;
    hash_normalizer_frames(hasher, "identity.normalizer", &identity.normalizer)
}

fn hash_post_row_frames(hasher: &mut Sha256, row: &PostServingRow) -> anyhow::Result<()> {
    hash_str_frame(hasher, "post_row.did", &row.did)?;
    hash_str_frame(hasher, "post_row.rkey", &row.rkey)?;
    hash_optional_str_frame(
        hasher,
        "post_row.created_at_normalized",
        row.created_at_normalized.as_deref(),
    )?;
    hash_str_frame(
        hasher,
        "post_row.created_at_parse_status",
        row.created_at_parse_status.as_str(),
    )?;
    hash_u64_frame(
        hasher,
        "post_row.langs.len",
        count_len(row.langs.len(), "streaming dedupe language count")?,
    )?;
    for (index, lang) in row.langs.iter().enumerate() {
        hash_u64_frame(
            hasher,
            "post_row.lang.index",
            count_len(index, "streaming dedupe language index")?,
        )?;
        hash_str_frame(hasher, "post_row.lang", lang)?;
    }
    hash_u64_frame(
        hasher,
        "post_row.emojis.len",
        count_len(row.emojis.len(), "streaming dedupe emoji count")?,
    )?;
    for (index, emoji) in row.emojis.iter().enumerate() {
        hash_u64_frame(
            hasher,
            "post_row.emoji.index",
            count_len(index, "streaming dedupe emoji index")?,
        )?;
        hash_str_frame(hasher, "post_row.emoji", emoji)?;
    }
    Ok(())
}

fn hash_counter_frames(hasher: &mut Sha256, counter: &TotalPostCounterInput) -> anyhow::Result<()> {
    hash_str_frame(hasher, "counter.source", &counter.source)?;
    hash_str_frame(hasher, "counter.did", &counter.did)?;
    hash_str_frame(hasher, "counter.dataset", &counter.dataset)?;
    hash_str_frame(hasher, "counter.fetch_method", &counter.fetch_method)?;
    hash_str_frame(
        hasher,
        "counter.completeness_class",
        &counter.completeness_class,
    )?;
    hash_str_frame(hasher, "counter.receipt_hash", &counter.receipt_hash)?;
    hash_normalizer_frames(hasher, "counter.normalizer", &counter.normalizer)?;
    hash_u64_frame(hasher, "counter.posts_processed", counter.posts_processed)?;
    hash_u64_frame(
        hasher,
        "counter.posts_with_emojis",
        counter.posts_with_emojis,
    )?;
    hash_u64_frame(
        hasher,
        "counter.emoji_occurrences",
        counter.emoji_occurrences,
    )?;
    hash_optional_str_frame(
        hasher,
        "counter.min_created_at_normalized",
        counter.min_created_at_normalized.as_deref(),
    )?;
    hash_optional_str_frame(
        hasher,
        "counter.max_created_at_normalized",
        counter.max_created_at_normalized.as_deref(),
    )
}

fn hash_normalizer_frames(
    hasher: &mut Sha256,
    label: &'static str,
    normalizer: &NormalizerVersion,
) -> anyhow::Result<()> {
    hash_str_frame(hasher, label, &normalizer.name)?;
    hash_str_frame(hasher, label, &normalizer.semver)?;
    hash_str_frame(hasher, label, &normalizer.git_rev)?;
    hash_str_frame(hasher, label, &normalizer.unicode_version)?;
    hash_str_frame(hasher, label, &normalizer.emoji_data_version)
}

fn hash_optional_str_frame(
    hasher: &mut Sha256,
    label: &'static str,
    value: Option<&str>,
) -> anyhow::Result<()> {
    match value {
        Some(value) => {
            hash_str_frame(hasher, label, "some")?;
            hash_str_frame(hasher, label, value)
        }
        None => hash_str_frame(hasher, label, "none"),
    }
}

fn hash_optional_u64_frame(
    hasher: &mut Sha256,
    label: &'static str,
    value: Option<u64>,
) -> anyhow::Result<()> {
    match value {
        Some(value) => {
            hash_str_frame(hasher, label, "some")?;
            hash_u64_frame(hasher, label, value)
        }
        None => hash_str_frame(hasher, label, "none"),
    }
}

fn hash_str_frame(hasher: &mut Sha256, label: &'static str, value: &str) -> anyhow::Result<()> {
    hash_frame(hasher, label, value.as_bytes())
}

fn hash_u64_frame(hasher: &mut Sha256, label: &'static str, value: u64) -> anyhow::Result<()> {
    hash_frame(hasher, label, &value.to_be_bytes())
}

fn hash_u16_frame(hasher: &mut Sha256, label: &'static str, value: u16) -> anyhow::Result<()> {
    hash_frame(hasher, label, &value.to_be_bytes())
}

fn hash_frame(hasher: &mut Sha256, label: &'static str, value: &[u8]) -> anyhow::Result<()> {
    hasher.update(count_len(label.len(), "streaming dedupe frame label length")?.to_be_bytes());
    hasher.update(label.as_bytes());
    hasher.update(count_len(value.len(), "streaming dedupe frame value length")?.to_be_bytes());
    hasher.update(value);
    Ok(())
}

#[cfg(test)]
mod tests;
