use std::{
    fs,
    path::{Path, PathBuf},
};

use emojistats_backfill::canary::{
    CanaryEvidence, CanaryFailureInjection, CanaryGateObservation, CanaryHardGate,
    CanaryInjectionObservation, CanaryPolicy, CanaryReport, CanarySampleCategory,
    CanarySampleObservation, CanaryStatus, CanaryThresholds, evaluate_canary,
    observe_aggregate_rebuild_hours, observe_clickhouse_serving_box_fit, observe_derive_crawl_pace,
    observe_mushroom_budget_and_429s, observe_storage_box_headroom, observe_sustained_throughput,
};
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct CanaryCommandConfig {
    pub evidence_path: PathBuf,
    pub thresholds: CanaryThresholds,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CanaryEvidenceRecord {
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
        status: Option<CanaryStatus>,
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
        detail: Option<String>,
    },
    Gate {
        gate: CanaryHardGate,
        #[serde(default)]
        status: Option<CanaryStatus>,
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

pub fn require_passing_evidence(path: &Path, thresholds: CanaryThresholds) -> anyhow::Result<()> {
    let report = evaluate_file(path, thresholds)?;
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
    let evidence = read_evidence_file(path, &policy.thresholds)?;
    Ok(evaluate_canary(&policy, &evidence))
}

fn read_evidence_file(
    path: &Path,
    thresholds: &CanaryThresholds,
) -> anyhow::Result<CanaryEvidence> {
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
) -> anyhow::Result<CanaryEvidence> {
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
) -> anyhow::Result<CanaryEvidence> {
    let mut evidence = CanaryEvidence::default();
    let mut record_count = 0_usize;

    for record in records {
        push_record(&mut evidence, record, thresholds)?;
        record_count = record_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("canary evidence record count overflow"))?;
    }

    Ok(evidence)
}

fn push_record(
    evidence: &mut CanaryEvidence,
    record: CanaryEvidenceRecord,
    thresholds: &CanaryThresholds,
) -> anyhow::Result<()> {
    match record {
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
            status,
            measured_headroom_ratio,
            measured_serving_box_ratio,
            measured_derive_to_crawl_ratio,
            measured_repos_per_second,
            measured_budget_utilization_ratio,
            measured_429_ratio,
            measured_hours,
            detail,
        }
        | CanaryEvidenceRecord::Gate {
            gate,
            status,
            measured_headroom_ratio,
            measured_serving_box_ratio,
            measured_derive_to_crawl_ratio,
            measured_repos_per_second,
            measured_budget_utilization_ratio,
            measured_429_ratio,
            measured_hours,
            detail,
        } => evidence.gates.push(gate_observation_from_record(
            thresholds,
            gate,
            status,
            detail,
            GateMeasurements {
                headroom_ratio: measured_headroom_ratio,
                serving_box_ratio: measured_serving_box_ratio,
                derive_to_crawl_ratio: measured_derive_to_crawl_ratio,
                repos_per_second: measured_repos_per_second,
                budget_utilization_ratio: measured_budget_utilization_ratio,
                http_429_ratio: measured_429_ratio,
                hours: measured_hours,
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
}

fn gate_observation_from_record(
    thresholds: &CanaryThresholds,
    gate: CanaryHardGate,
    status: Option<CanaryStatus>,
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
        CanaryHardGate::ReceiptRecomputationDetectsCorruption
        | CanaryHardGate::StorageBoxManifestDetectsPartialUpload
        | CanaryHardGate::WhaleCompletesCleanly
        | CanaryHardGate::InvalidReposClassifyLoudly => CanaryGateObservation {
            gate,
            status: status.ok_or_else(|| {
                anyhow::anyhow!("canary gate {} requires explicit status", gate.as_str())
            })?,
            detail,
        },
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

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests;
