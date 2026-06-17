use std::{collections::BTreeMap, sync::Mutex};

use super::{
    ALL_METRIC_NAMES, MetricEvent, MetricKind, MetricLabels, MetricName, MetricStage,
    MetricsRecorder, MetricsScope, PressureState,
};

#[test]
fn metric_event_serializes_stable_contract() {
    let event = MetricEvent::gauge(
        MetricName::FleetPressureState,
        MetricLabels {
            run_id: Some("run-1"),
            worker_id: Some("worker-7"),
            shard: Some("shard3"),
            host: Some("pds.example"),
            stage: Some(MetricStage::ParseWait.as_str()),
            outcome: None,
            pressure_state: Some(PressureState::ParseBackpressure.as_str()),
            backend: None,
        },
        1,
    );

    let value = serde_json::to_value(event).expect("metric event should serialize");

    assert_json_eq(&value, "event", "backfill_metric");
    assert_json_eq(&value, "name", "backfill_fleet_pressure_state");
    assert_json_eq(&value, "scope", "fleet");
    assert_json_eq(&value, "kind", "gauge");
    assert_json_eq(&value, "value", 1);
    let labels = value.get("labels").expect("labels should exist");
    assert_json_eq(labels, "stage", "parse_wait");
    assert_json_eq(labels, "pressure_state", "parse_backpressure");
    assert!(labels.get("backend").is_none());
}

#[test]
#[allow(clippy::too_many_lines)]
fn metric_names_scopes_and_kinds_match_launch_contract() {
    let expected = [
        (
            MetricName::FleetReposClaimedTotal,
            MetricsScope::Fleet,
            MetricKind::Counter,
        ),
        (
            MetricName::FleetAttemptsTotal,
            MetricsScope::Fleet,
            MetricKind::Counter,
        ),
        (
            MetricName::FleetAttemptDurationSeconds,
            MetricsScope::Fleet,
            MetricKind::Histogram,
        ),
        (
            MetricName::FleetActiveAttempts,
            MetricsScope::Fleet,
            MetricKind::Gauge,
        ),
        (
            MetricName::FleetPressureState,
            MetricsScope::Fleet,
            MetricKind::Gauge,
        ),
        (
            MetricName::FleetStaleClaimsRecoveredTotal,
            MetricsScope::Fleet,
            MetricKind::Counter,
        ),
        (
            MetricName::DeriveManifestEntriesSeenTotal,
            MetricsScope::Derive,
            MetricKind::Counter,
        ),
        (
            MetricName::DeriveFilesReadTotal,
            MetricsScope::Derive,
            MetricKind::Counter,
        ),
        (
            MetricName::DeriveRowsVerifiedTotal,
            MetricsScope::Derive,
            MetricKind::Counter,
        ),
        (
            MetricName::DeriveClickHouseBatchesCommittedTotal,
            MetricsScope::Derive,
            MetricKind::Counter,
        ),
        (
            MetricName::DeriveBatchDurationSeconds,
            MetricsScope::Derive,
            MetricKind::Histogram,
        ),
        (
            MetricName::StorageBoxUploadsTotal,
            MetricsScope::StorageBox,
            MetricKind::Counter,
        ),
        (
            MetricName::StorageBoxUploadBytesTotal,
            MetricsScope::StorageBox,
            MetricKind::Counter,
        ),
        (
            MetricName::StorageBoxCommitDurationSeconds,
            MetricsScope::StorageBox,
            MetricKind::Histogram,
        ),
        (
            MetricName::StorageBoxBackpressureSecondsTotal,
            MetricsScope::StorageBox,
            MetricKind::Counter,
        ),
        (
            MetricName::ClickHouseInsertBatchesTotal,
            MetricsScope::ClickHouse,
            MetricKind::Counter,
        ),
        (
            MetricName::ClickHouseInsertRowsTotal,
            MetricsScope::ClickHouse,
            MetricKind::Counter,
        ),
        (
            MetricName::ClickHouseInsertDurationSeconds,
            MetricsScope::ClickHouse,
            MetricKind::Histogram,
        ),
        (
            MetricName::ClickHouseRetriesTotal,
            MetricsScope::ClickHouse,
            MetricKind::Counter,
        ),
        (
            MetricName::ClickHouseDedupeConflictsTotal,
            MetricsScope::ClickHouse,
            MetricKind::Counter,
        ),
    ];

    assert_eq!(ALL_METRIC_NAMES.len(), expected.len());
    for (name, scope, kind) in expected {
        assert!(ALL_METRIC_NAMES.contains(&name));
        assert_eq!(name.scope(), scope);
        assert_eq!(name.kind(), kind);
        assert!(name.as_str().starts_with("backfill_"));
        assert!(
            name.as_str()
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
        );
    }
}

fn assert_json_eq(value: &serde_json::Value, key: &str, expected: impl Into<serde_json::Value>) {
    assert_eq!(value.get(key), Some(&expected.into()));
}

#[test]
fn stage_and_pressure_state_strings_are_stable() {
    assert_eq!(MetricStage::ClickHouseInsert.as_str(), "clickhouse_insert");
    assert_eq!(
        MetricStage::FallbackListRecords.as_str(),
        "fallback_list_records"
    );
    assert_eq!(
        PressureState::StorageBoxBackpressure.as_str(),
        "storage_box_backpressure"
    );
    assert_eq!(
        PressureState::ClickHouseBackpressure.as_str(),
        "clickhouse_backpressure"
    );
}

#[derive(Default)]
struct AggregatingRecorder {
    counters: Mutex<BTreeMap<String, u64>>,
    gauges: Mutex<BTreeMap<String, i64>>,
    histograms: Mutex<BTreeMap<String, (usize, f64)>>,
}

impl AggregatingRecorder {
    fn key(name: MetricName, labels: MetricLabels<'_>) -> String {
        format!(
            "{}|{}|{}|{}",
            name.as_str(),
            labels.stage.unwrap_or_default(),
            labels.outcome.unwrap_or_default(),
            labels.pressure_state.unwrap_or_default()
        )
    }
}

impl MetricsRecorder for AggregatingRecorder {
    #[allow(clippy::significant_drop_tightening)]
    fn increment_counter(&self, name: MetricName, labels: MetricLabels<'_>, value: u64) {
        let key = Self::key(name, labels);
        let mut counters = self.counters.lock().expect("counter lock");
        let current = counters.entry(key).or_default();
        *current = current.checked_add(value).expect("counter should fit");
    }

    fn record_gauge(&self, name: MetricName, labels: MetricLabels<'_>, value: i64) {
        self.gauges
            .lock()
            .expect("gauge lock")
            .insert(Self::key(name, labels), value);
    }

    #[allow(clippy::significant_drop_tightening)]
    fn record_histogram(&self, name: MetricName, labels: MetricLabels<'_>, value: f64) {
        let key = Self::key(name, labels);
        let mut histograms = self.histograms.lock().expect("histogram lock");
        let current = histograms.entry(key).or_default();
        current.0 = current.0.checked_add(1).expect("count should fit");
        current.1 += value;
    }
}

#[test]
fn recorder_trait_supports_event_aggregation() {
    let recorder = AggregatingRecorder::default();
    let complete = MetricLabels {
        run_id: Some("run-1"),
        worker_id: Some("worker-7"),
        shard: Some("shard3"),
        host: Some("pds.example"),
        stage: Some(MetricStage::Complete.as_str()),
        outcome: Some("succeeded"),
        pressure_state: None,
        backend: None,
    };

    recorder.increment_counter(MetricName::FleetAttemptsTotal, complete, 2);
    recorder.increment_counter(MetricName::FleetAttemptsTotal, complete, 3);
    recorder.record_histogram(MetricName::FleetAttemptDurationSeconds, complete, 0.25);
    recorder.record_histogram(MetricName::FleetAttemptDurationSeconds, complete, 0.75);

    let counter_key = AggregatingRecorder::key(MetricName::FleetAttemptsTotal, complete);
    assert_eq!(
        recorder
            .counters
            .lock()
            .expect("counter lock")
            .get(&counter_key),
        Some(&5)
    );
    let histogram_key = AggregatingRecorder::key(MetricName::FleetAttemptDurationSeconds, complete);
    assert_eq!(
        recorder
            .histograms
            .lock()
            .expect("histogram lock")
            .get(&histogram_key),
        Some(&(2, 1.0))
    );

    let pressure = MetricLabels {
        stage: Some(MetricStage::ParseWait.as_str()),
        pressure_state: Some(PressureState::ParseBackpressure.as_str()),
        ..complete
    };
    recorder.record_gauge(MetricName::FleetPressureState, pressure, 1);
    recorder.record_gauge(MetricName::FleetPressureState, pressure, 0);

    let gauge_key = AggregatingRecorder::key(MetricName::FleetPressureState, pressure);
    assert_eq!(
        recorder.gauges.lock().expect("gauge lock").get(&gauge_key),
        Some(&0)
    );
}
