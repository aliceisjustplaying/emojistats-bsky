use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::canary::{
    CanaryEvidence, CanaryFailureInjection, CanaryGateObservation, CanaryHardGate,
    CanaryInjectionObservation, CanaryPolicy, CanaryReport, CanarySampleCategory,
    CanarySampleObservation, CanaryStatus, CanaryThresholds, evaluate_canary,
    observe_aggregate_rebuild_hours, observe_clickhouse_serving_box_fit, observe_derive_crawl_pace,
    observe_mushroom_budget_and_429s, observe_storage_box_headroom, observe_sustained_throughput,
};

#[derive(Debug, Clone)]
pub struct CanaryCommandConfig {
    pub evidence_path: PathBuf,
    pub thresholds: CanaryThresholds,
}

#[derive(Debug, Clone, Deserialize)]
struct CanaryEvidenceMetadata {
    run_id: String,
    generated_at: DateTime<Utc>,
    #[serde(default)]
    max_age_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CanaryEvidenceRecord {
    Metadata {
        run_id: String,
        generated_at: DateTime<Utc>,
        #[serde(default)]
        max_age_seconds: Option<u64>,
    },
    Header {
        run_id: String,
        generated_at: DateTime<Utc>,
        #[serde(default)]
        max_age_seconds: Option<u64>,
    },
    Sample {
        category: CanarySampleCategory,
        repos_observed: u64,
        status: CanaryStatus,
        #[serde(default)]
        detail: Option<String>,
    },
    FailureInjection {
        injection: CanaryFailureInjection,
        status: CanaryStatus,
        #[serde(default)]
        detail: Option<String>,
    },
    Injection {
        injection: CanaryFailureInjection,
        status: CanaryStatus,
        #[serde(default)]
        detail: Option<String>,
    },
    HardGate {
        gate: CanaryHardGate,
        #[serde(default)]
        measured_headroom_ratio: Option<f64>,
        #[serde(default)]
        measured_serving_box_ratio: Option<f64>,
        #[serde(default)]
        measured_derive_to_crawl_ratio: Option<f64>,
        #[serde(default)]
        measured_repos_per_second: Option<f64>,
        #[serde(default)]
        measured_budget_utilization_ratio: Option<f64>,
        #[serde(default)]
        measured_429_ratio: Option<f64>,
        #[serde(default)]
        measured_hours: Option<f64>,
        #[serde(default)]
        receipt_recomputation_detected_corruption: Option<bool>,
        #[serde(default)]
        storage_box_manifest_detected_partial_upload: Option<bool>,
        #[serde(default)]
        whale_completed_cleanly: Option<bool>,
        #[serde(default)]
        invalid_repos_classified_loudly: Option<bool>,
        #[serde(default)]
        detail: Option<String>,
    },
    Gate {
        gate: CanaryHardGate,
        #[serde(default)]
        measured_headroom_ratio: Option<f64>,
        #[serde(default)]
        measured_serving_box_ratio: Option<f64>,
        #[serde(default)]
        measured_derive_to_crawl_ratio: Option<f64>,
        #[serde(default)]
        measured_repos_per_second: Option<f64>,
        #[serde(default)]
        measured_budget_utilization_ratio: Option<f64>,
        #[serde(default)]
        measured_429_ratio: Option<f64>,
        #[serde(default)]
        measured_hours: Option<f64>,
        #[serde(default)]
        receipt_recomputation_detected_corruption: Option<bool>,
        #[serde(default)]
        storage_box_manifest_detected_partial_upload: Option<bool>,
        #[serde(default)]
        whale_completed_cleanly: Option<bool>,
        #[serde(default)]
        invalid_repos_classified_loudly: Option<bool>,
        #[serde(default)]
        detail: Option<String>,
    },
}

pub fn run(config: CanaryCommandConfig) -> anyhow::Result<()> {
    let report = evaluate_file(&config.evidence_path, config.thresholds)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.is_pass() {
        return Ok(());
    }
    anyhow::bail!("canary policy did not pass: {:?}", report.status());
}

pub fn require_passing_evidence(
    path: &Path,
    thresholds: CanaryThresholds,
    expected_run_id: &str,
) -> anyhow::Result<()> {
    let report = evaluate_file_for_run(path, thresholds, expected_run_id, Utc::now())?;
    if report.is_pass() {
        return Ok(());
    }
    anyhow::bail!(
        "canary evidence {} did not pass: {:?}",
        path.display(),
        report.status()
    );
}

fn evaluate_file(path: &Path, thresholds: CanaryThresholds) -> anyhow::Result<CanaryReport> {
    let policy = CanaryPolicy::design_default(thresholds);
    let (_metadata, evidence) = read_evidence_file(path, &policy.thresholds)?;
    Ok(evaluate_canary(&policy, &evidence))
}

fn evaluate_file_for_run(
    path: &Path,
    thresholds: CanaryThresholds,
    expected_run_id: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<CanaryReport> {
    let policy = CanaryPolicy::design_default(thresholds);
    let (metadata, evidence) = read_evidence_file(path, &policy.thresholds)?;
    validate_metadata(path, metadata.as_ref(), expected_run_id, now)?;
    Ok(evaluate_canary(&policy, &evidence))
}

fn read_evidence_file(
    path: &Path,
    thresholds: &CanaryThresholds,
) -> anyhow::Result<(Option<CanaryEvidenceMetadata>, CanaryEvidence)> {
    let contents = fs::read_to_string(path)?;
    if let Ok(records) = serde_json::from_str::<Vec<CanaryEvidenceRecord>>(&contents) {
        return evidence_from_records(records, thresholds);
    }
    read_jsonl_evidence(path, &contents, thresholds)
}

fn read_jsonl_evidence(
    path: &Path,
    contents: &str,
    thresholds: &CanaryThresholds,
) -> anyhow::Result<(Option<CanaryEvidenceMetadata>, CanaryEvidence)> {
    let mut records = Vec::new();
    let mut count = 0_usize;

    for (line_index, line) in contents.lines().enumerate() {
        let line_number = line_index
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("canary evidence line number overflow"))?;
        if line.trim().is_empty() {
            continue;
        }
        let record: CanaryEvidenceRecord = serde_json::from_str(line).map_err(|source| {
            anyhow::anyhow!(
                "parse canary evidence {} line {}: {source}",
                path.display(),
                line_number
            )
        })?;
        records.push(record);
        count = count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("canary evidence record count overflow"))?;
    }

    if count == 0 {
        anyhow::bail!("canary evidence {} contained no records", path.display());
    }
    evidence_from_records(records, thresholds)
}

fn evidence_from_records(
    records: Vec<CanaryEvidenceRecord>,
    thresholds: &CanaryThresholds,
) -> anyhow::Result<(Option<CanaryEvidenceMetadata>, CanaryEvidence)> {
    let mut evidence = CanaryEvidence::default();
    let mut metadata = None;
    let mut record_count = 0_usize;

    for record in records {
        push_record(&mut evidence, &mut metadata, record, thresholds)?;
        record_count = record_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("canary evidence record count overflow"))?;
    }

    Ok((metadata, evidence))
}

fn push_record(
    evidence: &mut CanaryEvidence,
    metadata: &mut Option<CanaryEvidenceMetadata>,
    record: CanaryEvidenceRecord,
    thresholds: &CanaryThresholds,
) -> anyhow::Result<()> {
    match record {
        CanaryEvidenceRecord::Metadata {
            run_id,
            generated_at,
            max_age_seconds,
        }
        | CanaryEvidenceRecord::Header {
            run_id,
            generated_at,
            max_age_seconds,
        } => {
            if metadata.is_some() {
                anyhow::bail!("canary evidence contained multiple metadata records");
            }
            *metadata = Some(CanaryEvidenceMetadata {
                run_id,
                generated_at,
                max_age_seconds,
            });
        }
        CanaryEvidenceRecord::Sample {
            category,
            repos_observed,
            status,
            detail,
        } => evidence.samples.push(CanarySampleObservation {
            category,
            repos_observed,
            status,
            detail,
        }),
        CanaryEvidenceRecord::FailureInjection {
            injection,
            status,
            detail,
        }
        | CanaryEvidenceRecord::Injection {
            injection,
            status,
            detail,
        } => evidence.injections.push(CanaryInjectionObservation {
            injection,
            status,
            detail,
        }),
        CanaryEvidenceRecord::HardGate {
            gate,
            measured_headroom_ratio,
            measured_serving_box_ratio,
            measured_derive_to_crawl_ratio,
            measured_repos_per_second,
            measured_budget_utilization_ratio,
            measured_429_ratio,
            measured_hours,
            receipt_recomputation_detected_corruption,
            storage_box_manifest_detected_partial_upload,
            whale_completed_cleanly,
            invalid_repos_classified_loudly,
            detail,
        }
        | CanaryEvidenceRecord::Gate {
            gate,
            measured_headroom_ratio,
            measured_serving_box_ratio,
            measured_derive_to_crawl_ratio,
            measured_repos_per_second,
            measured_budget_utilization_ratio,
            measured_429_ratio,
            measured_hours,
            receipt_recomputation_detected_corruption,
            storage_box_manifest_detected_partial_upload,
            whale_completed_cleanly,
            invalid_repos_classified_loudly,
            detail,
        } => evidence.gates.push(gate_observation_from_record(
            thresholds,
            gate,
            detail,
            GateMeasurements {
                headroom_ratio: measured_headroom_ratio,
                serving_box_ratio: measured_serving_box_ratio,
                derive_to_crawl_ratio: measured_derive_to_crawl_ratio,
                repos_per_second: measured_repos_per_second,
                budget_utilization_ratio: measured_budget_utilization_ratio,
                http_429_ratio: measured_429_ratio,
                hours: measured_hours,
                receipt_recomputation_detected_corruption,
                storage_box_manifest_detected_partial_upload,
                whale_completed_cleanly,
                invalid_repos_classified_loudly,
            },
        )?),
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct GateMeasurements {
    headroom_ratio: Option<f64>,
    serving_box_ratio: Option<f64>,
    derive_to_crawl_ratio: Option<f64>,
    repos_per_second: Option<f64>,
    budget_utilization_ratio: Option<f64>,
    http_429_ratio: Option<f64>,
    hours: Option<f64>,
    receipt_recomputation_detected_corruption: Option<bool>,
    storage_box_manifest_detected_partial_upload: Option<bool>,
    whale_completed_cleanly: Option<bool>,
    invalid_repos_classified_loudly: Option<bool>,
}

fn gate_observation_from_record(
    thresholds: &CanaryThresholds,
    gate: CanaryHardGate,
    detail: Option<String>,
    measurements: GateMeasurements,
) -> anyhow::Result<CanaryGateObservation> {
    let observed = match gate {
        CanaryHardGate::ArchiveFitsStorageBox => observe_storage_box_headroom(
            thresholds,
            required_measurement(gate, measurements.headroom_ratio, "measured_headroom_ratio")?,
        ),
        CanaryHardGate::ClickHouseFitsServingBox => observe_clickhouse_serving_box_fit(
            thresholds,
            required_measurement(
                gate,
                measurements.serving_box_ratio,
                "measured_serving_box_ratio",
            )?,
        ),
        CanaryHardGate::DeriveKeepsPaceWithCrawl => observe_derive_crawl_pace(
            thresholds,
            required_measurement(
                gate,
                measurements.derive_to_crawl_ratio,
                "measured_derive_to_crawl_ratio",
            )?,
        ),
        CanaryHardGate::HealthyThroughputProjection => observe_sustained_throughput(
            thresholds,
            required_measurement(
                gate,
                measurements.repos_per_second,
                "measured_repos_per_second",
            )?,
        ),
        CanaryHardGate::MushroomBudgetSaturatedWithoutStorm => observe_mushroom_budget_and_429s(
            thresholds,
            required_measurement(
                gate,
                measurements.budget_utilization_ratio,
                "measured_budget_utilization_ratio",
            )?,
            required_measurement(gate, measurements.http_429_ratio, "measured_429_ratio")?,
        ),
        CanaryHardGate::AggregateRebuildWithinLaunchBudget => observe_aggregate_rebuild_hours(
            thresholds,
            required_measurement(gate, measurements.hours, "measured_hours")?,
        ),
        CanaryHardGate::ReceiptRecomputationDetectsCorruption => observe_bool_gate(
            gate,
            required_bool(
                gate,
                measurements.receipt_recomputation_detected_corruption,
                "receipt_recomputation_detected_corruption",
            )?,
            detail,
        ),
        CanaryHardGate::StorageBoxManifestDetectsPartialUpload => observe_bool_gate(
            gate,
            required_bool(
                gate,
                measurements.storage_box_manifest_detected_partial_upload,
                "storage_box_manifest_detected_partial_upload",
            )?,
            detail,
        ),
        CanaryHardGate::WhaleCompletesCleanly => observe_bool_gate(
            gate,
            required_bool(
                gate,
                measurements.whale_completed_cleanly,
                "whale_completed_cleanly",
            )?,
            detail,
        ),
        CanaryHardGate::InvalidReposClassifyLoudly => observe_bool_gate(
            gate,
            required_bool(
                gate,
                measurements.invalid_repos_classified_loudly,
                "invalid_repos_classified_loudly",
            )?,
            detail,
        ),
    };
    Ok(observed)
}

fn required_measurement(
    gate: CanaryHardGate,
    value: Option<f64>,
    field: &str,
) -> anyhow::Result<f64> {
    value.ok_or_else(|| {
        anyhow::anyhow!(
            "canary gate {} requires measurement field {field}",
            gate.as_str()
        )
    })
}

fn required_bool(gate: CanaryHardGate, value: Option<bool>, field: &str) -> anyhow::Result<bool> {
    value.ok_or_else(|| {
        anyhow::anyhow!(
            "canary gate {} requires boolean measurement field {field}",
            gate.as_str()
        )
    })
}

#[expect(clippy::missing_const_for_fn, reason = "detail owns a String")]
fn observe_bool_gate(
    gate: CanaryHardGate,
    passed: bool,
    detail: Option<String>,
) -> CanaryGateObservation {
    CanaryGateObservation {
        gate,
        status: if passed {
            CanaryStatus::Pass
        } else {
            CanaryStatus::Fail
        },
        detail,
    }
}

fn validate_metadata(
    path: &Path,
    metadata: Option<&CanaryEvidenceMetadata>,
    expected_run_id: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    const DEFAULT_MAX_AGE: Duration = Duration::from_hours(24);
    let metadata = metadata.ok_or_else(|| {
        anyhow::anyhow!(
            "canary evidence {} requires metadata record with run_id and generated_at",
            path.display()
        )
    })?;
    if metadata.run_id != expected_run_id {
        anyhow::bail!(
            "canary evidence run_id {} did not match requested run_id {}",
            metadata.run_id,
            expected_run_id
        );
    }
    if metadata.generated_at > now {
        anyhow::bail!("canary evidence generated_at is in the future");
    }
    let max_age = metadata
        .max_age_seconds
        .map_or(DEFAULT_MAX_AGE, Duration::from_secs);
    let age = now
        .signed_duration_since(metadata.generated_at)
        .to_std()
        .map_err(|_err| anyhow::anyhow!("canary evidence generated_at is in the future"))?;
    if age > max_age {
        anyhow::bail!("canary evidence is stale: age {age:?} exceeds max age {max_age:?}");
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
#[path = "canary_cmd/tests.rs"]
mod tests;
