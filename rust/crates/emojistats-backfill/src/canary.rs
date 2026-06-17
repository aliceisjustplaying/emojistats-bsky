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
    #[serde(rename = "clickhouse_duplicate_insert")]
    ClickHouseDuplicateInsert,
}

/// Hard gates from the v2 design document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryHardGate {
    ArchiveFitsStorageBox,
    #[serde(rename = "clickhouse_fits_serving_box")]
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

/// Convert measured storage-box free headroom into the archive fit gate.
#[must_use]
pub fn observe_storage_box_headroom(
    thresholds: &CanaryThresholds,
    measured_headroom_ratio: f64,
) -> CanaryGateObservation {
    observe_minimum_ratio_gate(
        CanaryHardGate::ArchiveFitsStorageBox,
        measured_headroom_ratio,
        thresholds.min_storage_box_headroom_ratio,
        "storage box headroom ratio",
    )
}

/// Convert measured `ClickHouse` serving-box utilization into the serving fit gate.
#[must_use]
pub fn observe_clickhouse_serving_box_fit(
    thresholds: &CanaryThresholds,
    measured_serving_box_ratio: f64,
) -> CanaryGateObservation {
    observe_maximum_ratio_gate(
        CanaryHardGate::ClickHouseFitsServingBox,
        measured_serving_box_ratio,
        thresholds.max_clickhouse_serving_box_ratio,
        "clickhouse serving box ratio",
    )
}

/// Convert measured derive/crawl throughput ratio into the pace gate.
#[must_use]
pub fn observe_derive_crawl_pace(
    thresholds: &CanaryThresholds,
    measured_derive_to_crawl_ratio: f64,
) -> CanaryGateObservation {
    observe_minimum_ratio_gate(
        CanaryHardGate::DeriveKeepsPaceWithCrawl,
        measured_derive_to_crawl_ratio,
        thresholds.min_derive_to_crawl_ratio,
        "derive to crawl ratio",
    )
}

/// Convert measured sustained crawl throughput into the launch projection gate.
#[must_use]
pub fn observe_sustained_throughput(
    thresholds: &CanaryThresholds,
    measured_repos_per_second: f64,
) -> CanaryGateObservation {
    observe_minimum_ratio_gate(
        CanaryHardGate::HealthyThroughputProjection,
        measured_repos_per_second,
        thresholds.min_sustained_repos_per_second,
        "sustained repos per second",
    )
}

/// Convert measured mushroom budget utilization and 429 rate into the mushroom gate.
#[must_use]
pub fn observe_mushroom_budget_and_429s(
    thresholds: &CanaryThresholds,
    measured_budget_utilization_ratio: f64,
    measured_429_ratio: f64,
) -> CanaryGateObservation {
    if let Some(detail) = invalid_measurement_detail(
        "mushroom budget utilization ratio",
        measured_budget_utilization_ratio,
    )
    .or_else(|| invalid_measurement_detail("mushroom 429 ratio", measured_429_ratio))
    {
        return gate_observation(
            CanaryHardGate::MushroomBudgetSaturatedWithoutStorm,
            CanaryStatus::Fail,
            detail,
        );
    }

    let passes = measured_budget_utilization_ratio
        >= thresholds.min_mushroom_budget_utilization_ratio
        && measured_429_ratio <= thresholds.max_mushroom_429_ratio;
    let detail = format!(
        "mushroom budget utilization ratio {measured_budget_utilization_ratio} \
         must be >= {}; mushroom 429 ratio {measured_429_ratio} must be <= {}",
        thresholds.min_mushroom_budget_utilization_ratio, thresholds.max_mushroom_429_ratio
    );

    gate_observation(
        CanaryHardGate::MushroomBudgetSaturatedWithoutStorm,
        status_for(passes),
        detail,
    )
}

/// Convert measured aggregate rebuild runtime into the launch-budget gate.
#[must_use]
pub fn observe_aggregate_rebuild_hours(
    thresholds: &CanaryThresholds,
    measured_hours: f64,
) -> CanaryGateObservation {
    observe_maximum_ratio_gate(
        CanaryHardGate::AggregateRebuildWithinLaunchBudget,
        measured_hours,
        thresholds.max_aggregate_rebuild_hours,
        "aggregate rebuild hours",
    )
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

fn observe_minimum_ratio_gate(
    gate: CanaryHardGate,
    measured: f64,
    minimum: f64,
    label: &str,
) -> CanaryGateObservation {
    invalid_measurement_detail(label, measured).map_or_else(
        || {
            gate_observation(
                gate,
                status_for(measured >= minimum),
                format!("{label} {measured} must be >= {minimum}"),
            )
        },
        |detail| gate_observation(gate, CanaryStatus::Fail, detail),
    )
}

fn observe_maximum_ratio_gate(
    gate: CanaryHardGate,
    measured: f64,
    maximum: f64,
    label: &str,
) -> CanaryGateObservation {
    invalid_measurement_detail(label, measured).map_or_else(
        || {
            gate_observation(
                gate,
                status_for(measured <= maximum),
                format!("{label} {measured} must be <= {maximum}"),
            )
        },
        |detail| gate_observation(gate, CanaryStatus::Fail, detail),
    )
}

const fn gate_observation(
    gate: CanaryHardGate,
    status: CanaryStatus,
    detail: String,
) -> CanaryGateObservation {
    CanaryGateObservation {
        gate,
        status,
        detail: Some(detail),
    }
}

const fn status_for(passes: bool) -> CanaryStatus {
    if passes {
        CanaryStatus::Pass
    } else {
        CanaryStatus::Fail
    }
}

fn invalid_measurement_detail(label: &str, measured: f64) -> Option<String> {
    if measured.is_finite() {
        None
    } else {
        Some(format!("{label} measurement {measured} is not finite"))
    }
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
mod tests;
