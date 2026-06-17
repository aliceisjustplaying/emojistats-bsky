use super::{
    CanaryEvidence, CanaryFailureInjection, CanaryFindingKind, CanaryGateObservation,
    CanaryHardGate, CanaryInjectionObservation, CanaryPolicy, CanarySampleCategory,
    CanarySampleObservation, CanaryStatus, CanaryThresholds, evaluate_canary,
    observe_aggregate_rebuild_hours, observe_clickhouse_serving_box_fit, observe_derive_crawl_pace,
    observe_mushroom_budget_and_429s, observe_storage_box_headroom, observe_sustained_throughput,
    required_failure_injections, required_hard_gates, required_sample_categories,
};

#[test]
fn missing_required_sample_category_fails_loudly() {
    let policy = CanaryPolicy::design_default(test_thresholds());
    let mut evidence = passing_evidence();
    evidence
        .samples
        .retain(|sample| sample.category != CanarySampleCategory::DidWeb);

    let report = evaluate_canary(&policy, &evidence);

    assert_eq!(report.status(), CanaryStatus::Fail);
    assert!(report.findings.iter().any(|finding| {
        finding.kind == CanaryFindingKind::SampleCategory
            && finding.subject == "did_web"
            && finding.detail == "missing required sample category"
    }));
}

#[test]
fn missing_required_failure_injection_fails_loudly() {
    let policy = CanaryPolicy::design_default(test_thresholds());
    let mut evidence = passing_evidence();
    evidence
        .injections
        .retain(|injection| injection.injection != CanaryFailureInjection::MalformedCar);

    let report = evaluate_canary(&policy, &evidence);

    assert_eq!(report.status(), CanaryStatus::Fail);
    assert!(report.findings.iter().any(|finding| {
        finding.kind == CanaryFindingKind::FailureInjection
            && finding.subject == "malformed_car"
            && finding.detail == "missing required failure injection"
    }));
}

#[test]
fn missing_required_hard_gate_fails_loudly() {
    let policy = CanaryPolicy::design_default(test_thresholds());
    let mut evidence = passing_evidence();
    evidence
        .gates
        .retain(|gate| gate.gate != CanaryHardGate::ArchiveFitsStorageBox);

    let report = evaluate_canary(&policy, &evidence);

    assert_eq!(report.status(), CanaryStatus::Fail);
    assert!(report.findings.iter().any(|finding| {
        finding.kind == CanaryFindingKind::HardGate
            && finding.subject == "archive_fits_storage_box"
            && finding.detail == "missing required hard gate"
    }));
}

#[test]
fn passing_inputs_produce_pass() {
    let policy = CanaryPolicy::design_default(test_thresholds());
    let evidence = passing_evidence();

    let report = evaluate_canary(&policy, &evidence);

    assert_eq!(report.status(), CanaryStatus::Pass);
    assert!(report.is_pass());
    assert!(report.findings.is_empty());
    assert_eq!(
        report.summary.passed_sample_categories,
        policy.required_sample_categories.len()
    );
    assert_eq!(
        report.summary.passed_failure_injections,
        policy.required_failure_injections.len()
    );
    assert_eq!(
        report.summary.passed_hard_gates,
        policy.required_hard_gates.len()
    );
}

#[test]
fn storage_headroom_gate_passes_at_minimum_and_fails_below() {
    let thresholds = test_thresholds();

    assert_gate(
        &observe_storage_box_headroom(&thresholds, thresholds.min_storage_box_headroom_ratio),
        CanaryHardGate::ArchiveFitsStorageBox,
        CanaryStatus::Pass,
    );
    assert_gate(
        &observe_storage_box_headroom(
            &thresholds,
            thresholds.min_storage_box_headroom_ratio - 0.01,
        ),
        CanaryHardGate::ArchiveFitsStorageBox,
        CanaryStatus::Fail,
    );
}

#[test]
fn clickhouse_fit_gate_passes_at_maximum_and_fails_above() {
    let thresholds = test_thresholds();

    assert_gate(
        &observe_clickhouse_serving_box_fit(
            &thresholds,
            thresholds.max_clickhouse_serving_box_ratio,
        ),
        CanaryHardGate::ClickHouseFitsServingBox,
        CanaryStatus::Pass,
    );
    assert_gate(
        &observe_clickhouse_serving_box_fit(
            &thresholds,
            thresholds.max_clickhouse_serving_box_ratio + 0.01,
        ),
        CanaryHardGate::ClickHouseFitsServingBox,
        CanaryStatus::Fail,
    );
}

#[test]
fn derive_crawl_pace_gate_passes_at_minimum_and_fails_below() {
    let thresholds = test_thresholds();

    assert_gate(
        &observe_derive_crawl_pace(&thresholds, thresholds.min_derive_to_crawl_ratio),
        CanaryHardGate::DeriveKeepsPaceWithCrawl,
        CanaryStatus::Pass,
    );
    assert_gate(
        &observe_derive_crawl_pace(&thresholds, thresholds.min_derive_to_crawl_ratio - 0.01),
        CanaryHardGate::DeriveKeepsPaceWithCrawl,
        CanaryStatus::Fail,
    );
}

#[test]
fn sustained_throughput_gate_passes_at_minimum_and_fails_below() {
    let thresholds = test_thresholds();

    assert_gate(
        &observe_sustained_throughput(&thresholds, thresholds.min_sustained_repos_per_second),
        CanaryHardGate::HealthyThroughputProjection,
        CanaryStatus::Pass,
    );
    assert_gate(
        &observe_sustained_throughput(
            &thresholds,
            thresholds.min_sustained_repos_per_second - 0.01,
        ),
        CanaryHardGate::HealthyThroughputProjection,
        CanaryStatus::Fail,
    );
}

#[test]
fn mushroom_gate_passes_at_boundaries_and_fails_on_budget_or_429_side() {
    let thresholds = test_thresholds();

    assert_gate(
        &observe_mushroom_budget_and_429s(
            &thresholds,
            thresholds.min_mushroom_budget_utilization_ratio,
            thresholds.max_mushroom_429_ratio,
        ),
        CanaryHardGate::MushroomBudgetSaturatedWithoutStorm,
        CanaryStatus::Pass,
    );
    assert_gate(
        &observe_mushroom_budget_and_429s(
            &thresholds,
            thresholds.min_mushroom_budget_utilization_ratio - 0.01,
            thresholds.max_mushroom_429_ratio,
        ),
        CanaryHardGate::MushroomBudgetSaturatedWithoutStorm,
        CanaryStatus::Fail,
    );
    assert_gate(
        &observe_mushroom_budget_and_429s(
            &thresholds,
            thresholds.min_mushroom_budget_utilization_ratio,
            thresholds.max_mushroom_429_ratio + 0.01,
        ),
        CanaryHardGate::MushroomBudgetSaturatedWithoutStorm,
        CanaryStatus::Fail,
    );
}

#[test]
fn aggregate_rebuild_gate_passes_at_maximum_and_fails_above() {
    let thresholds = test_thresholds();

    assert_gate(
        &observe_aggregate_rebuild_hours(&thresholds, thresholds.max_aggregate_rebuild_hours),
        CanaryHardGate::AggregateRebuildWithinLaunchBudget,
        CanaryStatus::Pass,
    );
    assert_gate(
        &observe_aggregate_rebuild_hours(
            &thresholds,
            thresholds.max_aggregate_rebuild_hours + 0.01,
        ),
        CanaryHardGate::AggregateRebuildWithinLaunchBudget,
        CanaryStatus::Fail,
    );
}

#[test]
fn numeric_gate_measurement_nan_fails() {
    let thresholds = test_thresholds();

    assert_gate(
        &observe_storage_box_headroom(&thresholds, f64::NAN),
        CanaryHardGate::ArchiveFitsStorageBox,
        CanaryStatus::Fail,
    );
}

fn assert_gate(observation: &CanaryGateObservation, gate: CanaryHardGate, status: CanaryStatus) {
    assert_eq!(observation.gate, gate);
    assert_eq!(observation.status, status);
    assert!(observation.detail.is_some());
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

fn test_thresholds() -> CanaryThresholds {
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
