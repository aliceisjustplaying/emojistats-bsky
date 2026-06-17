use std::{fs, io::Write, path::Path};

use emojistats_backfill::canary::{
    CanaryEvidence, CanaryGateObservation, CanaryHardGate, CanaryInjectionObservation,
    CanarySampleObservation, CanaryStatus, CanaryThresholds, required_failure_injections,
    required_hard_gates, required_sample_categories,
};
use serde_json::json;

use super::{evaluate_file, read_evidence_file};

#[test]
fn json_evidence_file_evaluates_passing_report() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let path = temp.path().join("canary.json");
    fs::write(
        &path,
        serde_json::to_vec(&passing_evidence()).expect("evidence should serialize"),
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
    let mut evidence = passing_evidence();
    evidence.gates.push(CanaryGateObservation {
        gate: CanaryHardGate::ArchiveFitsStorageBox,
        status: CanaryStatus::Fail,
        detail: Some("projected archive does not fit".to_owned()),
    });
    fs::write(
        &path,
        serde_json::to_vec(&evidence).expect("evidence should serialize"),
    )
    .expect("evidence should be written");

    let report = evaluate_file(&path, test_thresholds()).expect("canary should evaluate");

    assert_eq!(report.status(), CanaryStatus::Fail);
    assert!(report.findings.iter().any(|finding| {
        finding.subject == "archive_fits_storage_box"
            && finding.detail == "projected archive does not fit"
    }));
}

#[test]
fn jsonl_parse_errors_include_line_number() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let path = temp.path().join("bad.jsonl");
    fs::write(&path, "{}\nnot-json\n").expect("evidence should be written");

    let error = read_evidence_file(&path).expect_err("bad jsonl should fail");

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
        writeln!(
            file,
            "{}",
            json!({
                "kind": "hard_gate",
                "gate": gate,
                "status": "pass"
            })
        )
        .expect("gate line should be written");
    }
}

fn passing_evidence() -> CanaryEvidence {
    CanaryEvidence {
        samples: required_sample_categories()
            .into_iter()
            .map(|category| CanarySampleObservation {
                category,
                repos_observed: 1,
                status: CanaryStatus::Pass,
                detail: None,
            })
            .collect(),
        injections: required_failure_injections()
            .into_iter()
            .map(|injection| CanaryInjectionObservation {
                injection,
                status: CanaryStatus::Pass,
                detail: None,
            })
            .collect(),
        gates: required_hard_gates()
            .into_iter()
            .map(|gate| CanaryGateObservation {
                gate,
                status: CanaryStatus::Pass,
                detail: None,
            })
            .collect(),
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
