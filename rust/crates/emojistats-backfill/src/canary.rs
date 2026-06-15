//! Stratified canary policy and evaluation primitives.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Repo populations and coverage exercises required before fleet fan-out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanarySampleCategory {
    NormalRandom,
    RecentHighVolumeMushroom,
    OldMonth,
    InvalidCreatedAt,
    MissingCreatedAt,
    FutureCreatedAt,
    LargestRepoWhale,
    EmojiHeavy,
    ThirdPartyPds,
    CapabilityVariant,
    DidWeb,
    LowVolumeEightBoxContention,
}

/// Failure paths that must be injected and detected by the canary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryFailureInjection {
    MalformedCar,
    MissingBlock,
    InvalidMst,
    SinglePostDrop,
    PartialRemoteUpload,
    ManifestCorruption,
    ClickHouseDuplicateInsert,
}

/// Hard gates from the v2 design document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryHardGate {
    ArchiveFitsStorageBox,
    ClickHouseFitsServingBox,
    DeriveKeepsPaceWithCrawl,
    ReceiptRecomputationDetectsCorruption,
    StorageBoxManifestDetectsPartialUpload,
    HealthyThroughputProjection,
    MushroomBudgetSaturatedWithoutStorm,
    WhaleCompletesCleanly,
    InvalidReposClassifyLoudly,
    AggregateRebuildWithinLaunchBudget,
}

/// Tri-state result for canary coverage, injections, and gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryStatus {
    Pass,
    Fail,
    Pending,
}

/// Numeric thresholds used to turn future measurements into hard-gate observations.
///
/// The design record leaves exact values pending, so callers must supply this config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanaryThresholds {
    pub min_storage_box_headroom_ratio: f64,
    pub max_clickhouse_serving_box_ratio: f64,
    pub min_derive_to_crawl_ratio: f64,
    pub min_sustained_repos_per_second: f64,
    pub min_mushroom_budget_utilization_ratio: f64,
    pub max_mushroom_429_ratio: f64,
    pub max_aggregate_rebuild_hours: f64,
}

/// Configurable canary policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanaryPolicy {
    pub required_sample_categories: BTreeSet<CanarySampleCategory>,
    pub required_failure_injections: BTreeSet<CanaryFailureInjection>,
    pub required_hard_gates: BTreeSet<CanaryHardGate>,
    pub thresholds: CanaryThresholds,
}

impl CanaryPolicy {
    /// Policy shape encoded from `docs/backfill-v2-design.md`.
    #[must_use]
    pub fn design_default(thresholds: CanaryThresholds) -> Self {
        Self {
            required_sample_categories: required_sample_categories(),
            required_failure_injections: required_failure_injections(),
            required_hard_gates: required_hard_gates(),
            thresholds,
        }
    }
}

/// Observed coverage for one canary sample category.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanarySampleObservation {
    pub category: CanarySampleCategory,
    pub repos_observed: u64,
    pub status: CanaryStatus,
    pub detail: Option<String>,
}

/// Observed result for one required failure injection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryInjectionObservation {
    pub injection: CanaryFailureInjection,
    pub status: CanaryStatus,
    pub detail: Option<String>,
}

/// Observed result for one hard gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryGateObservation {
    pub gate: CanaryHardGate,
    pub status: CanaryStatus,
    pub detail: Option<String>,
}

/// All inputs needed to evaluate canary readiness.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryEvidence {
    pub samples: Vec<CanarySampleObservation>,
    pub injections: Vec<CanaryInjectionObservation>,
    pub gates: Vec<CanaryGateObservation>,
}

/// One loud reason the canary is not passable yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryFinding {
    pub kind: CanaryFindingKind,
    pub status: CanaryStatus,
    pub subject: String,
    pub detail: String,
}

/// Finding category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryFindingKind {
    SampleCategory,
    FailureInjection,
    HardGate,
}

/// Small summary suitable for operator output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryReportSummary {
    pub status: CanaryStatus,
    pub required_sample_categories: usize,
    pub passed_sample_categories: usize,
    pub required_failure_injections: usize,
    pub passed_failure_injections: usize,
    pub required_hard_gates: usize,
    pub passed_hard_gates: usize,
    pub failures: usize,
    pub pending: usize,
}

/// Evaluation result for a canary run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryReport {
    pub summary: CanaryReportSummary,
    pub findings: Vec<CanaryFinding>,
}

impl CanaryReport {
    /// Overall canary status.
    #[must_use]
    pub const fn status(&self) -> CanaryStatus {
        self.summary.status
    }

    /// `true` only when every required sample, injection, and hard gate passed.
    #[must_use]
    pub const fn is_pass(&self) -> bool {
        matches!(self.status(), CanaryStatus::Pass)
    }
}

/// Required sample coverage from the design record.
#[must_use]
pub fn required_sample_categories() -> BTreeSet<CanarySampleCategory> {
    BTreeSet::from([
        CanarySampleCategory::NormalRandom,
        CanarySampleCategory::RecentHighVolumeMushroom,
        CanarySampleCategory::OldMonth,
        CanarySampleCategory::InvalidCreatedAt,
        CanarySampleCategory::MissingCreatedAt,
        CanarySampleCategory::FutureCreatedAt,
        CanarySampleCategory::LargestRepoWhale,
        CanarySampleCategory::EmojiHeavy,
        CanarySampleCategory::ThirdPartyPds,
        CanarySampleCategory::CapabilityVariant,
        CanarySampleCategory::DidWeb,
        CanarySampleCategory::LowVolumeEightBoxContention,
    ])
}

/// Required failure injections from the design record.
#[must_use]
pub fn required_failure_injections() -> BTreeSet<CanaryFailureInjection> {
    BTreeSet::from([
        CanaryFailureInjection::MalformedCar,
        CanaryFailureInjection::MissingBlock,
        CanaryFailureInjection::InvalidMst,
        CanaryFailureInjection::SinglePostDrop,
        CanaryFailureInjection::PartialRemoteUpload,
        CanaryFailureInjection::ManifestCorruption,
        CanaryFailureInjection::ClickHouseDuplicateInsert,
    ])
}

/// Required hard gates from the design record.
#[must_use]
pub fn required_hard_gates() -> BTreeSet<CanaryHardGate> {
    BTreeSet::from([
        CanaryHardGate::ArchiveFitsStorageBox,
        CanaryHardGate::ClickHouseFitsServingBox,
        CanaryHardGate::DeriveKeepsPaceWithCrawl,
        CanaryHardGate::ReceiptRecomputationDetectsCorruption,
        CanaryHardGate::StorageBoxManifestDetectsPartialUpload,
        CanaryHardGate::HealthyThroughputProjection,
        CanaryHardGate::MushroomBudgetSaturatedWithoutStorm,
        CanaryHardGate::WhaleCompletesCleanly,
        CanaryHardGate::InvalidReposClassifyLoudly,
        CanaryHardGate::AggregateRebuildWithinLaunchBudget,
    ])
}

/// Evaluate canary evidence against a policy.
#[must_use]
pub fn evaluate_canary(policy: &CanaryPolicy, evidence: &CanaryEvidence) -> CanaryReport {
    let samples = sample_statuses(&evidence.samples);
    let injections = injection_statuses(&evidence.injections);
    let gates = gate_statuses(&evidence.gates);
    let mut findings = Vec::new();

    let passed_sample_categories = evaluate_required_samples(policy, &samples, &mut findings);
    let passed_failure_injections =
        evaluate_required_injections(policy, &injections, &mut findings);
    let passed_hard_gates = evaluate_required_gates(policy, &gates, &mut findings);

    let failures = findings
        .iter()
        .filter(|finding| finding.status == CanaryStatus::Fail)
        .count();
    let pending = findings
        .iter()
        .filter(|finding| finding.status == CanaryStatus::Pending)
        .count();
    let status = overall_status(failures, pending);

    CanaryReport {
        summary: CanaryReportSummary {
            status,
            required_sample_categories: policy.required_sample_categories.len(),
            passed_sample_categories,
            required_failure_injections: policy.required_failure_injections.len(),
            passed_failure_injections,
            required_hard_gates: policy.required_hard_gates.len(),
            passed_hard_gates,
            failures,
            pending,
        },
        findings,
    }
}

fn evaluate_required_samples(
    policy: &CanaryPolicy,
    samples: &BTreeMap<CanarySampleCategory, CollapsedSample>,
    findings: &mut Vec<CanaryFinding>,
) -> usize {
    let mut passed = 0;
    for category in &policy.required_sample_categories {
        match samples.get(category) {
            Some(sample) if sample.status == CanaryStatus::Pass && sample.repos_observed > 0 => {
                passed = increment_passed(passed);
            }
            Some(sample) if sample.repos_observed == 0 => findings.push(CanaryFinding {
                kind: CanaryFindingKind::SampleCategory,
                status: CanaryStatus::Fail,
                subject: category.as_str().to_owned(),
                detail: "required sample category observed zero repos".to_owned(),
            }),
            Some(sample) => findings.push(CanaryFinding {
                kind: CanaryFindingKind::SampleCategory,
                status: sample.status,
                subject: category.as_str().to_owned(),
                detail: sample
                    .detail
                    .clone()
                    .unwrap_or_else(|| "required sample category did not pass canary".to_owned()),
            }),
            None => findings.push(CanaryFinding {
                kind: CanaryFindingKind::SampleCategory,
                status: CanaryStatus::Fail,
                subject: category.as_str().to_owned(),
                detail: "missing required sample category".to_owned(),
            }),
        }
    }
    passed
}

fn evaluate_required_injections(
    policy: &CanaryPolicy,
    injections: &BTreeMap<CanaryFailureInjection, CollapsedStatus>,
    findings: &mut Vec<CanaryFinding>,
) -> usize {
    let mut passed = 0;
    for injection in &policy.required_failure_injections {
        match injections.get(injection) {
            Some(observed) if observed.status == CanaryStatus::Pass => {
                passed = increment_passed(passed);
            }
            Some(observed) => findings.push(CanaryFinding {
                kind: CanaryFindingKind::FailureInjection,
                status: observed.status,
                subject: injection.as_str().to_owned(),
                detail: observed
                    .detail
                    .clone()
                    .unwrap_or_else(|| "required failure injection did not pass".to_owned()),
            }),
            None => findings.push(CanaryFinding {
                kind: CanaryFindingKind::FailureInjection,
                status: CanaryStatus::Fail,
                subject: injection.as_str().to_owned(),
                detail: "missing required failure injection".to_owned(),
            }),
        }
    }
    passed
}

fn evaluate_required_gates(
    policy: &CanaryPolicy,
    gates: &BTreeMap<CanaryHardGate, CollapsedStatus>,
    findings: &mut Vec<CanaryFinding>,
) -> usize {
    let mut passed = 0;
    for gate in &policy.required_hard_gates {
        match gates.get(gate) {
            Some(observed) if observed.status == CanaryStatus::Pass => {
                passed = increment_passed(passed);
            }
            Some(observed) => findings.push(CanaryFinding {
                kind: CanaryFindingKind::HardGate,
                status: observed.status,
                subject: gate.as_str().to_owned(),
                detail: observed
                    .detail
                    .clone()
                    .unwrap_or_else(|| "required hard gate did not pass".to_owned()),
            }),
            None => findings.push(CanaryFinding {
                kind: CanaryFindingKind::HardGate,
                status: CanaryStatus::Fail,
                subject: gate.as_str().to_owned(),
                detail: "missing required hard gate".to_owned(),
            }),
        }
    }
    passed
}

const fn increment_passed(value: usize) -> usize {
    value.saturating_add(1)
}

fn sample_statuses(
    observations: &[CanarySampleObservation],
) -> BTreeMap<CanarySampleCategory, CollapsedSample> {
    let mut statuses = BTreeMap::new();
    for observation in observations {
        statuses
            .entry(observation.category)
            .and_modify(|existing: &mut CollapsedSample| {
                existing.status = merge_status(existing.status, observation.status);
                existing.repos_observed = existing
                    .repos_observed
                    .saturating_add(observation.repos_observed);
                existing.detail = existing
                    .detail
                    .clone()
                    .or_else(|| observation.detail.clone());
            })
            .or_insert_with(|| CollapsedSample {
                status: observation.status,
                repos_observed: observation.repos_observed,
                detail: observation.detail.clone(),
            });
    }
    statuses
}

fn injection_statuses(
    observations: &[CanaryInjectionObservation],
) -> BTreeMap<CanaryFailureInjection, CollapsedStatus> {
    let mut statuses = BTreeMap::new();
    for observation in observations {
        statuses
            .entry(observation.injection)
            .and_modify(|existing: &mut CollapsedStatus| {
                existing.status = merge_status(existing.status, observation.status);
                existing.detail = existing
                    .detail
                    .clone()
                    .or_else(|| observation.detail.clone());
            })
            .or_insert_with(|| CollapsedStatus {
                status: observation.status,
                detail: observation.detail.clone(),
            });
    }
    statuses
}

fn gate_statuses(
    observations: &[CanaryGateObservation],
) -> BTreeMap<CanaryHardGate, CollapsedStatus> {
    let mut statuses = BTreeMap::new();
    for observation in observations {
        statuses
            .entry(observation.gate)
            .and_modify(|existing: &mut CollapsedStatus| {
                existing.status = merge_status(existing.status, observation.status);
                existing.detail = existing
                    .detail
                    .clone()
                    .or_else(|| observation.detail.clone());
            })
            .or_insert_with(|| CollapsedStatus {
                status: observation.status,
                detail: observation.detail.clone(),
            });
    }
    statuses
}

const fn merge_status(left: CanaryStatus, right: CanaryStatus) -> CanaryStatus {
    match (left, right) {
        (CanaryStatus::Fail, _) | (_, CanaryStatus::Fail) => CanaryStatus::Fail,
        (CanaryStatus::Pending, _) | (_, CanaryStatus::Pending) => CanaryStatus::Pending,
        (CanaryStatus::Pass, CanaryStatus::Pass) => CanaryStatus::Pass,
    }
}

const fn overall_status(failures: usize, pending: usize) -> CanaryStatus {
    if failures > 0 {
        CanaryStatus::Fail
    } else if pending > 0 {
        CanaryStatus::Pending
    } else {
        CanaryStatus::Pass
    }
}

#[derive(Debug, Clone)]
struct CollapsedSample {
    status: CanaryStatus,
    repos_observed: u64,
    detail: Option<String>,
}

#[derive(Debug, Clone)]
struct CollapsedStatus {
    status: CanaryStatus,
    detail: Option<String>,
}

impl CanarySampleCategory {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NormalRandom => "normal_random",
            Self::RecentHighVolumeMushroom => "recent_high_volume_mushroom",
            Self::OldMonth => "old_month",
            Self::InvalidCreatedAt => "invalid_created_at",
            Self::MissingCreatedAt => "missing_created_at",
            Self::FutureCreatedAt => "future_created_at",
            Self::LargestRepoWhale => "largest_repo_whale",
            Self::EmojiHeavy => "emoji_heavy",
            Self::ThirdPartyPds => "third_party_pds",
            Self::CapabilityVariant => "capability_variant",
            Self::DidWeb => "did_web",
            Self::LowVolumeEightBoxContention => "low_volume_eight_box_contention",
        }
    }
}

impl CanaryFailureInjection {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MalformedCar => "malformed_car",
            Self::MissingBlock => "missing_block",
            Self::InvalidMst => "invalid_mst",
            Self::SinglePostDrop => "single_post_drop",
            Self::PartialRemoteUpload => "partial_remote_upload",
            Self::ManifestCorruption => "manifest_corruption",
            Self::ClickHouseDuplicateInsert => "clickhouse_duplicate_insert",
        }
    }
}

impl CanaryHardGate {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ArchiveFitsStorageBox => "archive_fits_storage_box",
            Self::ClickHouseFitsServingBox => "clickhouse_fits_serving_box",
            Self::DeriveKeepsPaceWithCrawl => "derive_keeps_pace_with_crawl",
            Self::ReceiptRecomputationDetectsCorruption => {
                "receipt_recomputation_detects_corruption"
            }
            Self::StorageBoxManifestDetectsPartialUpload => {
                "storage_box_manifest_detects_partial_upload"
            }
            Self::HealthyThroughputProjection => "healthy_throughput_projection",
            Self::MushroomBudgetSaturatedWithoutStorm => "mushroom_budget_saturated_without_storm",
            Self::WhaleCompletesCleanly => "whale_completes_cleanly",
            Self::InvalidReposClassifyLoudly => "invalid_repos_classify_loudly",
            Self::AggregateRebuildWithinLaunchBudget => "aggregate_rebuild_within_launch_budget",
        }
    }
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::{
        CanaryEvidence, CanaryFailureInjection, CanaryFindingKind, CanaryGateObservation,
        CanaryHardGate, CanaryInjectionObservation, CanaryPolicy, CanarySampleCategory,
        CanarySampleObservation, CanaryStatus, CanaryThresholds, evaluate_canary,
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
}
