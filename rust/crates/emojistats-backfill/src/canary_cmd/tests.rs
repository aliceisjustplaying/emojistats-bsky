use std::{fs, io::Write, path::Path};

use chrono::{TimeZone, Utc};
use serde_json::json;

use super::{evaluate_file, evaluate_file_for_run, read_evidence_file};
use crate::canary::{
    CanaryHardGate, CanaryStatus, CanaryThresholds, required_failure_injections,
    required_hard_gates, required_sample_categories,
};

#[test]
fn json_evidence_file_evaluates_passing_report() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let path = temp.path().join("canary.json");
    fs::write(
        &path,
        serde_json::to_vec(&passing_records()).expect("evidence should serialize"),
    )
    .expect("evidence should be written");

    let report = evaluate_file(&path, test_thresholds()).expect("canary should evaluate");

    assert_eq!(report.status(), CanaryStatus::Pass);
    assert!(report.findings.is_empty());
}

#[test]
fn jsonl_evidence_file_evaluates_passing_report() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let path = temp.path().join("canary.jsonl");
    write_passing_jsonl(&path);

    let report = evaluate_file(&path, test_thresholds()).expect("canary should evaluate");

    assert_eq!(report.status(), CanaryStatus::Pass);
    assert!(report.findings.is_empty());
}

#[test]
fn failed_gate_exits_as_failed_report() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let path = temp.path().join("canary.json");
    let mut evidence = passing_records();
    evidence.push(json!({
        "kind": "hard_gate",
        "gate": "archive_fits_storage_box",
        "measured_headroom_ratio": 0.01
    }));
    fs::write(
        &path,
        serde_json::to_vec(&evidence).expect("evidence should serialize"),
    )
    .expect("evidence should be written");

    let report = evaluate_file(&path, test_thresholds()).expect("canary should evaluate");

    assert_eq!(report.status(), CanaryStatus::Fail);
    assert!(report.findings.iter().any(|finding| {
        finding.subject == "archive_fits_storage_box" && finding.detail.contains("must be >=")
    }));
}

#[test]
fn status_only_numeric_gate_is_rejected() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let path = temp.path().join("canary.jsonl");
    fs::write(
        &path,
        json!({
            "kind": "hard_gate",
            "gate": "archive_fits_storage_box",
            "status": "pass"
        })
        .to_string(),
    )
    .expect("evidence should be written");

    let error = read_evidence_file(&path, &test_thresholds())
        .expect_err("numeric gate without measurement should fail");

    assert!(
        error
            .to_string()
            .contains("requires measurement field measured_headroom_ratio")
    );
}

#[test]
fn run_fleet_gate_requires_matching_fresh_metadata() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let path = temp.path().join("canary.jsonl");
    let mut file = fs::File::create(&path).expect("jsonl file should be created");
    writeln!(
        file,
        "{}",
        json!({
            "kind": "metadata",
            "run_id": "run-1",
            "generated_at": "2026-06-18T12:00:00Z",
            "max_age_seconds": 3600
        })
    )
    .expect("metadata line should be written");
    for record in passing_records() {
        writeln!(file, "{record}").expect("record line should be written");
    }

    let report = evaluate_file_for_run(
        &path,
        test_thresholds(),
        "run-1",
        Utc.with_ymd_and_hms(2026, 6, 18, 12, 30, 0)
            .single()
            .expect("valid timestamp"),
    )
    .expect("fresh matching metadata should pass");

    assert_eq!(report.status(), CanaryStatus::Pass);
}

#[test]
fn run_fleet_gate_rejects_wrong_run_id() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let path = temp.path().join("canary.jsonl");
    fs::write(
        &path,
        json!({
            "kind": "metadata",
            "run_id": "run-1",
            "generated_at": "2026-06-18T12:00:00Z"
        })
        .to_string(),
    )
    .expect("metadata should be written");

    let error = evaluate_file_for_run(
        &path,
        test_thresholds(),
        "run-2",
        Utc.with_ymd_and_hms(2026, 6, 18, 12, 30, 0)
            .single()
            .expect("valid timestamp"),
    )
    .expect_err("wrong run id should fail");

    assert!(error.to_string().contains("did not match requested run_id"));
}

#[test]
fn status_only_integrity_gate_is_rejected() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let path = temp.path().join("canary.jsonl");
    fs::write(
        &path,
        json!({
            "kind": "hard_gate",
            "gate": "whale_completes_cleanly",
            "status": "pass"
        })
        .to_string(),
    )
    .expect("evidence should be written");

    let error = read_evidence_file(&path, &test_thresholds())
        .expect_err("integrity gate without boolean measurement should fail");

    assert!(
        error
            .to_string()
            .contains("requires boolean measurement field whale_completed_cleanly")
    );
}

#[test]
fn jsonl_parse_errors_include_line_number() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let path = temp.path().join("bad.jsonl");
    fs::write(&path, "{}\nnot-json\n").expect("evidence should be written");

    let error = read_evidence_file(&path, &test_thresholds()).expect_err("bad jsonl should fail");

    assert!(error.to_string().contains("line 1"));
}

fn write_passing_jsonl(path: &Path) {
    let mut file = fs::File::create(path).expect("jsonl file should be created");
    for category in required_sample_categories() {
        writeln!(
            file,
            "{}",
            json!({
                "kind": "sample",
                "category": category,
                "repos_observed": 1,
                "status": "pass"
            })
        )
        .expect("sample line should be written");
    }
    for injection in required_failure_injections() {
        writeln!(
            file,
            "{}",
            json!({
                "kind": "failure_injection",
                "injection": injection,
                "status": "pass"
            })
        )
        .expect("injection line should be written");
    }
    for gate in required_hard_gates() {
        writeln!(file, "{}", passing_gate_record(gate)).expect("gate line should be written");
    }
}

fn passing_records() -> Vec<serde_json::Value> {
    let mut records = Vec::new();
    for category in required_sample_categories() {
        records.push(json!({
            "kind": "sample",
            "category": category,
            "repos_observed": 1,
            "status": "pass"
        }));
    }
    for injection in required_failure_injections() {
        records.push(json!({
            "kind": "failure_injection",
            "injection": injection,
            "status": "pass"
        }));
    }
    for gate in required_hard_gates() {
        records.push(passing_gate_record(gate));
    }
    records
}

fn passing_gate_record(gate: CanaryHardGate) -> serde_json::Value {
    match gate {
        CanaryHardGate::ArchiveFitsStorageBox => json!({
            "kind": "hard_gate",
            "gate": gate,
            "measured_headroom_ratio": test_thresholds().min_storage_box_headroom_ratio
        }),
        CanaryHardGate::ClickHouseFitsServingBox => json!({
            "kind": "hard_gate",
            "gate": gate,
            "measured_serving_box_ratio": test_thresholds().max_clickhouse_serving_box_ratio
        }),
        CanaryHardGate::DeriveKeepsPaceWithCrawl => json!({
            "kind": "hard_gate",
            "gate": gate,
            "measured_derive_to_crawl_ratio": test_thresholds().min_derive_to_crawl_ratio
        }),
        CanaryHardGate::HealthyThroughputProjection => json!({
            "kind": "hard_gate",
            "gate": gate,
            "measured_repos_per_second": test_thresholds().min_sustained_repos_per_second
        }),
        CanaryHardGate::MushroomBudgetSaturatedWithoutStorm => json!({
            "kind": "hard_gate",
            "gate": gate,
            "measured_budget_utilization_ratio": test_thresholds().min_mushroom_budget_utilization_ratio,
            "measured_429_ratio": test_thresholds().max_mushroom_429_ratio
        }),
        CanaryHardGate::AggregateRebuildWithinLaunchBudget => json!({
            "kind": "hard_gate",
            "gate": gate,
            "measured_hours": test_thresholds().max_aggregate_rebuild_hours
        }),
        CanaryHardGate::ReceiptRecomputationDetectsCorruption => json!({
            "kind": "hard_gate",
            "gate": gate,
            "receipt_recomputation_detected_corruption": true
        }),
        CanaryHardGate::StorageBoxManifestDetectsPartialUpload => json!({
            "kind": "hard_gate",
            "gate": gate,
            "storage_box_manifest_detected_partial_upload": true
        }),
        CanaryHardGate::WhaleCompletesCleanly => json!({
            "kind": "hard_gate",
            "gate": gate,
            "whale_completed_cleanly": true
        }),
        CanaryHardGate::InvalidReposClassifyLoudly => json!({
            "kind": "hard_gate",
            "gate": gate,
            "invalid_repos_classified_loudly": true
        }),
    }
}

const fn test_thresholds() -> CanaryThresholds {
    CanaryThresholds {
        min_storage_box_headroom_ratio: 0.2,
        max_clickhouse_serving_box_ratio: 0.8,
        min_derive_to_crawl_ratio: 1.0,
        min_sustained_repos_per_second: 1.0,
        min_mushroom_budget_utilization_ratio: 0.9,
        max_mushroom_429_ratio: 0.01,
        max_aggregate_rebuild_hours: 2.0,
    }
}
