use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use emojistats_backfill::{
    archive::{
        ArchivePostRowsHasher, EmojiProjectionRow, NormalizerVersion,
        archive_post_rows_from_record_batch,
    },
    clickhouse::{
        ClickHouseClientConfig, ClickHouseInsertPayload, ClickHouseInsertReceipt,
        emoji_serving_rows_insert_payload, execute_insert_payloads,
        total_post_counter_insert_payload_for_counter,
    },
    derive::{
        BACKFILL_DERIVE_SOURCE, DeriveManifestIdentity, TotalPostCounterInput,
        emoji_projection_rows_for_post,
    },
    hash::hash_serialized_json,
    manifest_derive::{
        LoaderInput, VerifiedLoaderInput, read_committed_jsonl, verify_loader_input_for_streaming,
    },
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{add_count, count_len, increment, payload_row_count};

const DERIVE_EMOJI_CHUNK_ROWS: usize = 10_000;
const STREAMING_DEDUPE_TOKEN_DOMAIN: &str = "emojistats-backfill-streaming-derive-token-v1";

#[derive(Debug)]
pub struct DeriveManifestConfig {
    pub manifest_path: PathBuf,
    pub archive_root: PathBuf,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub dry_run: bool,
    pub derive_ledger_path: Option<PathBuf>,
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
    let plan = read_committed_jsonl(BufReader::new(file))?;
    let clickhouse = ClickHouseClientConfig::new(
        &config.clickhouse_url,
        &config.clickhouse_database,
        config.clickhouse_user,
        config.clickhouse_password,
        "emojistats-backfill-derive",
    )?;
    let http = clickhouse.http_client()?;
    let mut summary = DeriveManifestSummary {
        manifest_entries: count_len(plan.inputs.len(), "manifest_entries")?,
        skipped_entries: count_len(plan.skipped_entries.len(), "skipped_entries")?,
        ..DeriveManifestSummary::default()
    };
    let mut derive_ledger = DeriveLedger::new(config.derive_ledger_path.as_deref())?;

    for input in &plan.inputs {
        derive_loader_input_streaming(
            &config.archive_root,
            input,
            &http,
            &clickhouse,
            config.dry_run,
            &mut derive_ledger,
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

async fn derive_loader_input_streaming(
    archive_root: &Path,
    input: &LoaderInput,
    http: &reqwest::Client,
    clickhouse: &ClickHouseClientConfig,
    dry_run: bool,
    derive_ledger: &mut DeriveLedger,
    summary: &mut DeriveManifestSummary,
) -> anyhow::Result<()> {
    let verified = verify_loader_input_for_streaming(archive_root, input)?;
    derive_verified_input_streaming(&verified, http, clickhouse, dry_run, derive_ledger, summary)
        .await
}

async fn derive_verified_input_streaming(
    verified: &VerifiedLoaderInput,
    http: &reqwest::Client,
    clickhouse: &ClickHouseClientConfig,
    dry_run: bool,
    derive_ledger: &mut DeriveLedger,
    summary: &mut DeriveManifestSummary,
) -> anyhow::Result<()> {
    validate_verified_input_streaming(verified)?;
    insert_verified_input_streaming(verified, http, clickhouse, dry_run, derive_ledger, summary)
        .await?;
    increment(&mut summary.archive_files, "derive archive file count")
}

fn validate_verified_input_streaming(verified: &VerifiedLoaderInput) -> anyhow::Result<()> {
    let file = File::open(&verified.object_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut state = StreamingValidationState::new(verified);

    for batch in reader {
        let rows = archive_post_rows_from_record_batch(&batch?)?;
        state.consume_rows(&rows)?;
    }

    state.finish()
}

async fn insert_verified_input_streaming(
    verified: &VerifiedLoaderInput,
    http: &reqwest::Client,
    clickhouse: &ClickHouseClientConfig,
    dry_run: bool,
    derive_ledger: &mut DeriveLedger,
    summary: &mut DeriveManifestSummary,
) -> anyhow::Result<()> {
    let file = File::open(&verified.object_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut state = StreamingPayloadState::new(verified);

    for batch in reader {
        let rows = archive_post_rows_from_record_batch(&batch?)?;
        let payloads = state.consume_rows(&rows)?;
        apply_derive_payloads(
            http,
            clickhouse,
            dry_run,
            derive_ledger,
            summary,
            verified,
            &payloads,
        )
        .await?;
    }

    let payloads = state.finish()?;
    apply_derive_payloads(
        http,
        clickhouse,
        dry_run,
        derive_ledger,
        summary,
        verified,
        &payloads,
    )
    .await
}

async fn apply_derive_payloads(
    http: &reqwest::Client,
    clickhouse: &ClickHouseClientConfig,
    dry_run: bool,
    derive_ledger: &mut DeriveLedger,
    summary: &mut DeriveManifestSummary,
    verified: &VerifiedLoaderInput,
    payloads: &[ClickHouseInsertPayload],
) -> anyhow::Result<()> {
    if payloads.is_empty() {
        return Ok(());
    }
    if dry_run {
        add_count(
            &mut summary.attempted_insert_payloads,
            count_len(payloads.len(), "derive payload count")?,
            "derive attempted payload total",
        )?;
        let attempted_rows = payload_row_count(payloads)?;
        add_count(
            &mut summary.attempted_insert_rows,
            attempted_rows,
            "derive attempted row total",
        )?;
        return Ok(());
    }

    for payload in payloads {
        let payload_rows = count_len(payload.row_count, "derive payload row count")?;
        if derive_ledger.is_completed(verified, payload) {
            increment(
                &mut summary.skipped_insert_payloads,
                "skipped payload total",
            )?;
            add_count(
                &mut summary.skipped_insert_rows,
                payload_rows,
                "skipped row total",
            )?;
            continue;
        }

        increment(
            &mut summary.attempted_insert_payloads,
            "derive attempted payload total",
        )?;
        add_count(
            &mut summary.attempted_insert_rows,
            payload_rows,
            "derive attempted row total",
        )?;
        let mut receipts =
            execute_insert_payloads(http, clickhouse, std::slice::from_ref(payload)).await?;
        let receipt = receipts
            .pop()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse insert returned no receipt"))?;
        derive_ledger.append_success(verified, payload, &receipt)?;
        increment(&mut summary.inserted_payloads, "inserted payload total")?;
        add_count(
            &mut summary.inserted_rows,
            count_len(receipt.context.row_count, "inserted row count")?,
            "inserted row total",
        )?;
    }
    Ok(())
}

struct StreamingValidationState<'a> {
    verified: &'a VerifiedLoaderInput,
    row_hasher: ArchivePostRowsHasher,
    rows: u64,
}

impl<'a> StreamingValidationState<'a> {
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

struct StreamingPayloadState<'a> {
    verified: &'a VerifiedLoaderInput,
    rows: u64,
    posts_with_emojis: u64,
    emoji_occurrences: u64,
    emoji_chunk_rows: Vec<EmojiProjectionRow>,
    emoji_chunk_index: u64,
}

impl<'a> StreamingPayloadState<'a> {
    fn new(verified: &'a VerifiedLoaderInput) -> Self {
        Self {
            verified,
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
        let counter = TotalPostCounterInput {
            source: BACKFILL_DERIVE_SOURCE.to_owned(),
            run_id: self.verified.manifest.run_id.clone(),
            shard: self.verified.manifest.shard.clone(),
            file_sequence: self.verified.manifest.file_sequence,
            receipt_hash: self.verified.manifest.receipt_hash.clone(),
            normalizer: self.verified.manifest.normalizer.clone(),
            posts_processed: self.rows,
            posts_with_emojis: self.posts_with_emojis,
            emoji_occurrences: self.emoji_occurrences,
            min_created_at_normalized: self.verified.manifest.min_created_at_normalized.clone(),
            max_created_at_normalized: self.verified.manifest.max_created_at_normalized.clone(),
        };
        let token = streaming_counter_dedupe_token(&self.verified.identity, &counter)?;
        payloads.push(total_post_counter_insert_payload_for_counter(
            &counter, token,
        )?);
        Ok(payloads)
    }

    fn flush_emoji_chunk(&mut self) -> anyhow::Result<ClickHouseInsertPayload> {
        let rows = std::mem::take(&mut self.emoji_chunk_rows);
        let token =
            streaming_emoji_dedupe_token(&self.verified.identity, self.emoji_chunk_index, &rows)?;
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamingDedupeLane {
    Emoji,
    Counter,
}

impl StreamingDedupeLane {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Emoji => "emoji",
            Self::Counter => "counter",
        }
    }
}

fn streaming_emoji_dedupe_token(
    identity: &DeriveManifestIdentity,
    chunk_index: u64,
    rows: &[EmojiProjectionRow],
) -> anyhow::Result<String> {
    let mut hasher =
        streaming_dedupe_hasher(identity, StreamingDedupeLane::Emoji, Some(chunk_index))?;
    hash_str_frame(&mut hasher, "payload.kind", "emoji_rows")?;
    hash_u64_frame(
        &mut hasher,
        "emoji_rows.len",
        count_len(rows.len(), "streaming dedupe emoji row count")?,
    )?;
    for (index, row) in rows.iter().enumerate() {
        hash_u64_frame(
            &mut hasher,
            "emoji_row.index",
            count_len(index, "streaming dedupe emoji row index")?,
        )?;
        hash_emoji_row_frames(&mut hasher, row)?;
    }
    Ok(streaming_dedupe_token(StreamingDedupeLane::Emoji, hasher))
}

fn streaming_counter_dedupe_token(
    identity: &DeriveManifestIdentity,
    counter: &TotalPostCounterInput,
) -> anyhow::Result<String> {
    let mut hasher = streaming_dedupe_hasher(identity, StreamingDedupeLane::Counter, None)?;
    hash_str_frame(&mut hasher, "payload.kind", "total_post_counter")?;
    hash_counter_frames(&mut hasher, counter)?;
    Ok(streaming_dedupe_token(StreamingDedupeLane::Counter, hasher))
}

fn streaming_dedupe_hasher(
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
    hash_str_frame(hasher, "identity.content_hash", &identity.content_hash)?;
    hash_str_frame(hasher, "identity.receipt_hash", &identity.receipt_hash)?;
    hash_u16_frame(hasher, "identity.schema_version", identity.schema_version)?;
    hash_normalizer_frames(hasher, "identity.normalizer", &identity.normalizer)
}

fn hash_emoji_row_frames(hasher: &mut Sha256, row: &EmojiProjectionRow) -> anyhow::Result<()> {
    hash_str_frame(hasher, "emoji_row.did", &row.did)?;
    hash_str_frame(hasher, "emoji_row.rkey", &row.rkey)?;
    hash_optional_str_frame(
        hasher,
        "emoji_row.created_at_normalized",
        row.created_at_normalized.as_deref(),
    )?;
    hash_str_frame(hasher, "emoji_row.emoji", &row.emoji)?;
    hash_u64_frame(hasher, "emoji_row.occurrences", row.occurrences)?;
    hash_u64_frame(
        hasher,
        "emoji_row.langs.len",
        count_len(row.langs.len(), "streaming dedupe language count")?,
    )?;
    for (index, lang) in row.langs.iter().enumerate() {
        hash_u64_frame(
            hasher,
            "emoji_row.lang.index",
            count_len(index, "streaming dedupe language index")?,
        )?;
        hash_str_frame(hasher, "emoji_row.lang", lang)?;
    }
    Ok(())
}

fn hash_counter_frames(hasher: &mut Sha256, counter: &TotalPostCounterInput) -> anyhow::Result<()> {
    hash_str_frame(hasher, "counter.source", &counter.source)?;
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
