use std::{fs::File, io::BufReader, path::PathBuf};

use emojistats_backfill::{
    archive::{ArchivePostRowsHasher, EmojiProjectionRow, archive_post_rows_from_record_batch},
    clickhouse::{
        ClickHouseClientConfig, ClickHouseInsertPayload, emoji_serving_rows_insert_payload,
        execute_insert_payloads, total_post_counter_insert_payload_for_counter,
    },
    derive::{
        BACKFILL_DERIVE_SOURCE, DeriveManifestIdentity, TotalPostCounterInput,
        emoji_projection_rows_for_post,
    },
    hash::hash_serialized_json,
    manifest_derive::{
        VerifiedLoaderInput, read_committed_jsonl, verify_loader_input_for_streaming,
    },
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{add_count, count_len, increment, payload_row_count};

const DERIVE_EMOJI_CHUNK_ROWS: usize = 10_000;

#[derive(Debug)]
pub struct DeriveManifestConfig {
    pub manifest_path: PathBuf,
    pub archive_root: PathBuf,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub dry_run: bool,
}

#[derive(Debug, Default, Serialize)]
struct DeriveManifestSummary {
    manifest_entries: u64,
    skipped_entries: u64,
    archive_files: u64,
    attempted_insert_payloads: u64,
    attempted_insert_rows: u64,
    inserted_payloads: u64,
    inserted_rows: u64,
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
    increment(&mut summary.archive_files, "derive archive file count")
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
    if !dry_run {
        let receipts = execute_insert_payloads(http, clickhouse, payloads).await?;
        add_count(
            &mut summary.inserted_payloads,
            count_len(receipts.len(), "insert receipt count")?,
            "inserted payload total",
        )?;
        let mut inserted_rows = 0_u64;
        for receipt in &receipts {
            add_count(
                &mut inserted_rows,
                count_len(receipt.context.row_count, "inserted row count")?,
                "inserted row total",
            )?;
        }
        add_count(
            &mut summary.inserted_rows,
            inserted_rows,
            "inserted row total",
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
