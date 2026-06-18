use std::{
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    time::Instant,
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;

use super::{add_count, count_len, increment, payload_row_count};
use crate::{
    archive::archive_post_rows_from_record_batch,
    clickhouse::{
        ClickHouseClientConfig, ClickHouseInsertPayload, ClickHouseInsertReceipt,
        execute_insert_payloads,
    },
    manifest_derive::{
        LoaderInput, ManifestReadItem, VerifiedLoaderInput, stream_committed_jsonl,
        verify_loader_input_for_streaming,
    },
    metrics::{MetricLabels, MetricName, MetricStage, SharedMetricsRecorder},
};

#[path = "derive_manifest_cmd/ledger.rs"]
mod ledger;
#[path = "derive_manifest_cmd/streaming.rs"]
mod streaming;

use ledger::DeriveLedger;
use streaming::{CanonicalStreamingPayloadState, CanonicalStreamingValidationState};

#[cfg(test)]
use crate::clickhouse::DEFAULT_POST_SERVING_PAYLOAD_MAX_BYTES;
#[cfg(test)]
use crate::derive::{
    DeriveManifestIdentity, PostServingRow, TotalPostCounterInput,
    canonical_streaming_counter_dedupe_token, canonical_streaming_post_dedupe_token,
};

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
    validate_verified_input_canonical_streaming(verified)?;
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

fn validate_verified_input_canonical_streaming(
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

async fn stream_verified_input_payloads_canonical_streaming(
    verified: &VerifiedLoaderInput,
    context: &mut DeriveRunContext<'_>,
) -> anyhow::Result<()> {
    let file = File::open(&verified.object_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut state = CanonicalStreamingPayloadState::new(verified);
    let mut pending_dry_run = PendingDryRunInserts::default();

    for batch in reader {
        let rows = archive_post_rows_from_record_batch(&batch?)?;
        let payloads = state.consume_rows(&rows)?;
        handle_streamed_derive_payloads(context, verified, &payloads, &mut pending_dry_run).await?;
    }

    let payloads = state.finish()?;
    handle_streamed_derive_payloads(context, verified, &payloads, &mut pending_dry_run).await?;
    if context.dry_run {
        pending_dry_run.commit(context.summary)?;
    }
    Ok(())
}

async fn handle_streamed_derive_payloads(
    context: &mut DeriveRunContext<'_>,
    verified: &VerifiedLoaderInput,
    payloads: &[ClickHouseInsertPayload],
    pending_dry_run: &mut PendingDryRunInserts,
) -> anyhow::Result<()> {
    if context.dry_run {
        pending_dry_run.add_payloads(payloads)
    } else {
        apply_derive_payloads(context, verified, payloads).await
    }
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
        if context.derive_ledger.is_completed(verified, payload)? {
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

#[cfg(test)]
#[path = "derive_manifest_cmd/tests.rs"]
mod tests;
