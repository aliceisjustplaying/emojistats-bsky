//! Stable metric vocabulary for the v2 backfill runtime.
//!
//! This is a launch scaffold only. Runtime code should call typed observation methods
//! once the Prometheus/OpenTelemetry exporter is selected.

/// Pipeline area that owns an observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsScope {
    Fleet,
    Derive,
    StorageBox,
    ClickHouse,
}

/// Low-cardinality labels shared by the first metrics surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MetricLabels<'a> {
    pub run_id: Option<&'a str>,
    pub worker_id: Option<&'a str>,
    pub shard: Option<&'a str>,
    pub host: Option<&'a str>,
    pub stage: Option<&'a str>,
    pub outcome: Option<&'a str>,
    pub pressure_state: Option<&'a str>,
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
}

/// Exporter boundary for the future Prometheus/OpenTelemetry implementation.
pub trait MetricsRecorder {
    fn increment_counter(&self, name: MetricName, labels: MetricLabels<'_>, value: u64);

    fn record_gauge(&self, name: MetricName, labels: MetricLabels<'_>, value: i64);

    fn record_histogram(&self, name: MetricName, labels: MetricLabels<'_>, value: f64);
}

/// No-op recorder for smoke runs, tests, and disabled metrics.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopMetricsRecorder;

impl MetricsRecorder for NoopMetricsRecorder {
    fn increment_counter(&self, _name: MetricName, _labels: MetricLabels<'_>, _value: u64) {}

    fn record_gauge(&self, _name: MetricName, _labels: MetricLabels<'_>, _value: i64) {}

    fn record_histogram(&self, _name: MetricName, _labels: MetricLabels<'_>, _value: f64) {}
}
