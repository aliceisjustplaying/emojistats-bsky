//! Stable metric vocabulary and JSONL recorder for the v2 backfill runtime.

use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

/// Pipeline area that owns an observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricsScope {
    Fleet,
    Derive,
    #[serde(rename = "storage_box")]
    StorageBox,
    #[serde(rename = "clickhouse")]
    ClickHouse,
}

/// Low-cardinality labels shared by the first metrics surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct MetricLabels<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pressure_state: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend: Option<&'a str>,
}

/// Stable metric names for the launch exporter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricName {
    FleetReposClaimedTotal,
    FleetAttemptsTotal,
    FleetAttemptDurationSeconds,
    FleetActiveAttempts,
    FleetPressureState,
    FleetStaleClaimsRecoveredTotal,
    DeriveManifestEntriesSeenTotal,
    DeriveFilesReadTotal,
    DeriveRowsVerifiedTotal,
    DeriveClickHouseBatchesCommittedTotal,
    DeriveBatchDurationSeconds,
    StorageBoxUploadsTotal,
    StorageBoxUploadBytesTotal,
    StorageBoxCommitDurationSeconds,
    StorageBoxBackpressureSecondsTotal,
    ClickHouseInsertBatchesTotal,
    ClickHouseInsertRowsTotal,
    ClickHouseInsertDurationSeconds,
    ClickHouseRetriesTotal,
    ClickHouseDedupeConflictsTotal,
}

/// Stable metric names in launch contract order.
pub const ALL_METRIC_NAMES: [MetricName; 20] = [
    MetricName::FleetReposClaimedTotal,
    MetricName::FleetAttemptsTotal,
    MetricName::FleetAttemptDurationSeconds,
    MetricName::FleetActiveAttempts,
    MetricName::FleetPressureState,
    MetricName::FleetStaleClaimsRecoveredTotal,
    MetricName::DeriveManifestEntriesSeenTotal,
    MetricName::DeriveFilesReadTotal,
    MetricName::DeriveRowsVerifiedTotal,
    MetricName::DeriveClickHouseBatchesCommittedTotal,
    MetricName::DeriveBatchDurationSeconds,
    MetricName::StorageBoxUploadsTotal,
    MetricName::StorageBoxUploadBytesTotal,
    MetricName::StorageBoxCommitDurationSeconds,
    MetricName::StorageBoxBackpressureSecondsTotal,
    MetricName::ClickHouseInsertBatchesTotal,
    MetricName::ClickHouseInsertRowsTotal,
    MetricName::ClickHouseInsertDurationSeconds,
    MetricName::ClickHouseRetriesTotal,
    MetricName::ClickHouseDedupeConflictsTotal,
];

impl MetricName {
    /// Prometheus-compatible metric name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FleetReposClaimedTotal => "backfill_fleet_repos_claimed_total",
            Self::FleetAttemptsTotal => "backfill_fleet_attempts_total",
            Self::FleetAttemptDurationSeconds => "backfill_fleet_attempt_duration_seconds",
            Self::FleetActiveAttempts => "backfill_fleet_active_attempts",
            Self::FleetPressureState => "backfill_fleet_pressure_state",
            Self::FleetStaleClaimsRecoveredTotal => "backfill_fleet_stale_claims_recovered_total",
            Self::DeriveManifestEntriesSeenTotal => "backfill_derive_manifest_entries_seen_total",
            Self::DeriveFilesReadTotal => "backfill_derive_files_read_total",
            Self::DeriveRowsVerifiedTotal => "backfill_derive_rows_verified_total",
            Self::DeriveClickHouseBatchesCommittedTotal => {
                "backfill_derive_clickhouse_batches_committed_total"
            }
            Self::DeriveBatchDurationSeconds => "backfill_derive_batch_duration_seconds",
            Self::StorageBoxUploadsTotal => "backfill_storage_box_uploads_total",
            Self::StorageBoxUploadBytesTotal => "backfill_storage_box_upload_bytes_total",
            Self::StorageBoxCommitDurationSeconds => "backfill_storage_box_commit_duration_seconds",
            Self::StorageBoxBackpressureSecondsTotal => {
                "backfill_storage_box_backpressure_seconds_total"
            }
            Self::ClickHouseInsertBatchesTotal => "backfill_clickhouse_insert_batches_total",
            Self::ClickHouseInsertRowsTotal => "backfill_clickhouse_insert_rows_total",
            Self::ClickHouseInsertDurationSeconds => "backfill_clickhouse_insert_duration_seconds",
            Self::ClickHouseRetriesTotal => "backfill_clickhouse_retries_total",
            Self::ClickHouseDedupeConflictsTotal => "backfill_clickhouse_dedupe_conflicts_total",
        }
    }

    /// Pipeline area expected to emit the metric.
    #[must_use]
    pub const fn scope(self) -> MetricsScope {
        match self {
            Self::FleetReposClaimedTotal
            | Self::FleetAttemptsTotal
            | Self::FleetAttemptDurationSeconds
            | Self::FleetActiveAttempts
            | Self::FleetPressureState
            | Self::FleetStaleClaimsRecoveredTotal => MetricsScope::Fleet,
            Self::DeriveManifestEntriesSeenTotal
            | Self::DeriveFilesReadTotal
            | Self::DeriveRowsVerifiedTotal
            | Self::DeriveClickHouseBatchesCommittedTotal
            | Self::DeriveBatchDurationSeconds => MetricsScope::Derive,
            Self::StorageBoxUploadsTotal
            | Self::StorageBoxUploadBytesTotal
            | Self::StorageBoxCommitDurationSeconds
            | Self::StorageBoxBackpressureSecondsTotal => MetricsScope::StorageBox,
            Self::ClickHouseInsertBatchesTotal
            | Self::ClickHouseInsertRowsTotal
            | Self::ClickHouseInsertDurationSeconds
            | Self::ClickHouseRetriesTotal
            | Self::ClickHouseDedupeConflictsTotal => MetricsScope::ClickHouse,
        }
    }

    /// Instrument type expected by the launch contract.
    #[must_use]
    pub const fn kind(self) -> MetricKind {
        match self {
            Self::FleetAttemptDurationSeconds
            | Self::DeriveBatchDurationSeconds
            | Self::StorageBoxCommitDurationSeconds
            | Self::ClickHouseInsertDurationSeconds => MetricKind::Histogram,
            Self::FleetActiveAttempts | Self::FleetPressureState => MetricKind::Gauge,
            Self::FleetReposClaimedTotal
            | Self::FleetAttemptsTotal
            | Self::FleetStaleClaimsRecoveredTotal
            | Self::DeriveManifestEntriesSeenTotal
            | Self::DeriveFilesReadTotal
            | Self::DeriveRowsVerifiedTotal
            | Self::DeriveClickHouseBatchesCommittedTotal
            | Self::StorageBoxUploadsTotal
            | Self::StorageBoxUploadBytesTotal
            | Self::StorageBoxBackpressureSecondsTotal
            | Self::ClickHouseInsertBatchesTotal
            | Self::ClickHouseInsertRowsTotal
            | Self::ClickHouseRetriesTotal
            | Self::ClickHouseDedupeConflictsTotal => MetricKind::Counter,
        }
    }
}

/// Metric emission kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

/// Stable stage values allowed in the `stage` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricStage {
    Claim,
    Fetch,
    ListRecordsFetch,
    FallbackListRecords,
    ParseWait,
    ParseStart,
    ParseArchive,
    ArchiveCommit,
    DeriveManifestScan,
    DeriveFileRead,
    DeriveReceiptVerify,
    DeriveClickHouseCommit,
    StorageBoxUpload,
    StorageBoxCommit,
    ClickHouseInsert,
    Complete,
}

impl MetricStage {
    /// Stable stage label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::Fetch => "fetch",
            Self::ListRecordsFetch => "list_records_fetch",
            Self::FallbackListRecords => "fallback_list_records",
            Self::ParseWait => "parse_wait",
            Self::ParseStart => "parse_start",
            Self::ParseArchive => "parse_archive",
            Self::ArchiveCommit => "archive_commit",
            Self::DeriveManifestScan => "derive_manifest_scan",
            Self::DeriveFileRead => "derive_file_read",
            Self::DeriveReceiptVerify => "derive_receipt_verify",
            Self::DeriveClickHouseCommit => "derive_clickhouse_commit",
            Self::StorageBoxUpload => "storage_box_upload",
            Self::StorageBoxCommit => "storage_box_commit",
            Self::ClickHouseInsert => "clickhouse_insert",
            Self::Complete => "complete",
        }
    }
}

/// Stable pressure states allowed in the `pressure_state` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PressureState {
    HostPacing,
    FetchByteBudget,
    DiskPressure,
    ParseBackpressure,
    ParseActive,
    StorageBoxBackpressure,
    ClickHouseBackpressure,
    RateLimitSleep,
    OperatorPause,
}

impl PressureState {
    /// Stable pressure-state label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HostPacing => "host_pacing",
            Self::FetchByteBudget => "fetch_byte_budget",
            Self::DiskPressure => "disk_pressure",
            Self::ParseBackpressure => "parse_backpressure",
            Self::ParseActive => "parse_active",
            Self::StorageBoxBackpressure => "storage_box_backpressure",
            Self::ClickHouseBackpressure => "clickhouse_backpressure",
            Self::RateLimitSleep => "rate_limit_sleep",
            Self::OperatorPause => "operator_pause",
        }
    }
}

/// JSONL event used until the final Prometheus/OpenTelemetry exporter is selected.
#[derive(Debug, Serialize)]
pub struct MetricEvent<'a> {
    pub event: &'static str,
    pub observed_unix_ms: u128,
    pub name: &'static str,
    pub scope: MetricsScope,
    pub kind: MetricKind,
    pub value: serde_json::Value,
    pub labels: MetricLabels<'a>,
}

impl<'a> MetricEvent<'a> {
    /// Build a deterministic counter event for sidecars and tests.
    #[must_use]
    pub fn counter(name: MetricName, labels: MetricLabels<'a>, value: u64) -> Self {
        Self::new(name, labels, MetricKind::Counter, serde_json::json!(value))
    }

    /// Build a deterministic gauge event for sidecars and tests.
    #[must_use]
    pub fn gauge(name: MetricName, labels: MetricLabels<'a>, value: i64) -> Self {
        Self::new(name, labels, MetricKind::Gauge, serde_json::json!(value))
    }

    /// Build a deterministic histogram event for sidecars and tests.
    #[must_use]
    pub fn histogram(name: MetricName, labels: MetricLabels<'a>, value: f64) -> Self {
        Self::new(
            name,
            labels,
            MetricKind::Histogram,
            serde_json::json!(value),
        )
    }

    fn new(
        name: MetricName,
        labels: MetricLabels<'a>,
        kind: MetricKind,
        value: serde_json::Value,
    ) -> Self {
        debug_assert_eq!(name.kind(), kind);
        Self {
            event: "backfill_metric",
            observed_unix_ms: 0,
            name: name.as_str(),
            scope: name.scope(),
            kind,
            value,
            labels,
        }
    }
}

/// Exporter boundary for the future Prometheus/OpenTelemetry implementation.
pub trait MetricsRecorder: Send + Sync {
    fn increment_counter(&self, name: MetricName, labels: MetricLabels<'_>, value: u64);

    fn record_gauge(&self, name: MetricName, labels: MetricLabels<'_>, value: i64);

    fn record_histogram(&self, name: MetricName, labels: MetricLabels<'_>, value: f64);
}

/// Shared recorder handle used by async fleet tasks.
pub type SharedMetricsRecorder = Arc<dyn MetricsRecorder>;

/// No-op recorder for smoke runs, tests, and disabled metrics.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopMetricsRecorder;

impl MetricsRecorder for NoopMetricsRecorder {
    fn increment_counter(&self, _name: MetricName, _labels: MetricLabels<'_>, _value: u64) {}

    fn record_gauge(&self, _name: MetricName, _labels: MetricLabels<'_>, _value: i64) {}

    fn record_histogram(&self, _name: MetricName, _labels: MetricLabels<'_>, _value: f64) {}
}

/// JSONL recorder for canaries, smoke runs, and launch dry-runs.
#[derive(Debug)]
pub struct JsonLineMetricsRecorder {
    file: Mutex<File>,
}

impl JsonLineMetricsRecorder {
    /// Open a JSONL metrics file for append.
    ///
    /// # Errors
    ///
    /// Returns an error when the parent directory or file cannot be created.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    fn emit(
        &self,
        name: MetricName,
        labels: MetricLabels<'_>,
        kind: MetricKind,
        value: serde_json::Value,
    ) {
        debug_assert_eq!(name.kind(), kind);
        let event = MetricEvent {
            event: "backfill_metric",
            observed_unix_ms: unix_ms_now(),
            name: name.as_str(),
            scope: name.scope(),
            kind,
            value,
            labels,
        };
        let Ok(mut file) = self.file.lock() else {
            eprintln!("metrics recorder lock poisoned for {}", name.as_str());
            return;
        };
        if let Err(error) = serde_json::to_writer(&mut *file, &event) {
            eprintln!("failed to write metric {}: {error}", name.as_str());
            return;
        }
        if let Err(error) = file.write_all(b"\n") {
            eprintln!("failed to newline metric {}: {error}", name.as_str());
        }
    }
}

impl MetricsRecorder for JsonLineMetricsRecorder {
    fn increment_counter(&self, name: MetricName, labels: MetricLabels<'_>, value: u64) {
        self.emit(name, labels, MetricKind::Counter, serde_json::json!(value));
    }

    fn record_gauge(&self, name: MetricName, labels: MetricLabels<'_>, value: i64) {
        self.emit(name, labels, MetricKind::Gauge, serde_json::json!(value));
    }

    fn record_histogram(&self, name: MetricName, labels: MetricLabels<'_>, value: f64) {
        self.emit(
            name,
            labels,
            MetricKind::Histogram,
            serde_json::json!(value),
        );
    }
}

#[must_use]
pub fn noop_metrics_recorder() -> SharedMetricsRecorder {
    Arc::new(NoopMetricsRecorder)
}

/// Build a JSONL recorder behind the shared trait handle.
///
/// # Errors
///
/// Returns an error when the recorder file cannot be opened.
pub fn jsonl_metrics_recorder(path: &Path) -> anyhow::Result<SharedMetricsRecorder> {
    Ok(Arc::new(JsonLineMetricsRecorder::open(path)?))
}

fn unix_ms_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

#[cfg(test)]
#[path = "metrics/tests.rs"]
mod contract_tests;
