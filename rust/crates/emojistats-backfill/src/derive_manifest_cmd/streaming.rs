use std::fs::File;

use emojistats_backfill::{
    archive::{ArchivePostRow, ArchivePostRowsHasher, archive_post_rows_from_record_batch},
    clickhouse::{
        ClickHouseInsertPayload, DEFAULT_POST_SERVING_PAYLOAD_MAX_BYTES,
        post_serving_row_body_bytes, post_serving_rows_insert_payload,
        total_post_counter_insert_payload_for_counter,
    },
    derive::{
        BACKFILL_DERIVE_SOURCE, PostServingRow, TotalPostCounterInput, post_serving_row_for_post,
    },
    hash::hash_serialized_json,
    manifest_derive::VerifiedLoaderInput,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{
    add_count, count_len, increment,
    tokens::{canonical_streaming_counter_dedupe_token, canonical_streaming_post_dedupe_token},
};

const DERIVE_POST_CHUNK_ROWS: usize = 10_000;

pub(super) fn validate_canonical_streaming_proof(
    verified: &VerifiedLoaderInput,
) -> anyhow::Result<()> {
    let file = File::open(&verified.object_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut state = CanonicalStreamingValidationState::new(verified);

    for batch in reader {
        let rows = archive_post_rows_from_record_batch(&batch?)?;
        state.consume_rows(&rows)?;
    }

    state.finish()
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

    fn consume_rows(&mut self, rows: &[ArchivePostRow]) -> anyhow::Result<()> {
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

pub(super) struct CanonicalStreamingPayloadState<'a> {
    verified: &'a VerifiedLoaderInput,
    rows: u64,
    posts_with_emojis: u64,
    emoji_occurrences: u64,
    post_chunk_rows: Vec<PostServingRow>,
    post_chunk_body_bytes: usize,
    post_chunk_index: u64,
}

impl<'a> CanonicalStreamingPayloadState<'a> {
    pub(super) fn new(verified: &'a VerifiedLoaderInput) -> Self {
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

    pub(super) fn consume_rows(
        &mut self,
        rows: &[ArchivePostRow],
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

    pub(super) fn finish(mut self) -> anyhow::Result<Vec<ClickHouseInsertPayload>> {
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
