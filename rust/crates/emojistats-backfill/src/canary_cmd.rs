use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::{DateTime, Utc};
use emojistats_backfill::canary::{
    CanaryEvidence, CanaryFailureInjection, CanaryGateObservation, CanaryHardGate,
    CanaryInjectionObservation, CanaryPolicy, CanaryReport, CanarySampleCategory,
    CanarySampleObservation, CanaryStatus, CanaryThresholds, evaluate_canary,
    observe_aggregate_rebuild_hours, observe_clickhouse_serving_box_fit, observe_derive_crawl_pace,
    observe_mushroom_budget_and_429s, observe_storage_box_headroom, observe_sustained_throughput,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;

#[derive(Debug, Clone)]
pub struct CanaryCommandConfig {
    pub evidence_path: PathBuf,
    pub thresholds: CanaryThresholds,
}

#[derive(Debug, Clone)]
pub struct CanarySignConfig {
    pub evidence_path: PathBuf,
    pub run_id: String,
    pub max_age_seconds: u64,
    pub hmac_key_env: String,
    pub thresholds: CanaryThresholds,
}

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CanaryEvidenceMetadata {
    run_id: String,
    generated_at: DateTime<Utc>,
    #[serde(default)]
    max_age_seconds: Option<u64>,
    #[serde(default)]
    hmac_sha256: Option<String>,
}

#[derive(Clone)]
pub struct CanaryEvidenceSignatureKey {
    bytes: Vec<u8>,
}

impl CanaryEvidenceSignatureKey {
    /// Load the canary evidence HMAC key from an environment variable.
    ///
    /// # Errors
    ///
    /// Returns an error when the variable is missing or empty.
    pub fn from_env_var(name: &str) -> anyhow::Result<Self> {
        let value = env::var(name).map_err(|_err| {
            anyhow::anyhow!("canary evidence HMAC key env var {name} is not set")
        })?;
        if value.is_empty() {
            anyhow::bail!("canary evidence HMAC key env var {name} is empty");
        }
        Ok(Self {
            bytes: value.into_bytes(),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CanaryEvidenceRecord {
    Metadata {
        run_id: String,
        generated_at: DateTime<Utc>,
        #[serde(default)]
        max_age_seconds: Option<u64>,
        #[serde(default)]
        hmac_sha256: Option<String>,
    },
    Header {
        run_id: String,
        generated_at: DateTime<Utc>,
        #[serde(default)]
        max_age_seconds: Option<u64>,
        #[serde(default)]
        hmac_sha256: Option<String>,
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

pub fn sign(config: CanarySignConfig) -> anyhow::Result<()> {
    let policy = CanaryPolicy::design_default(config.thresholds);
    let values = read_evidence_values(&config.evidence_path)?;
    let unsigned_values = unsigned_evidence_values(values)?;
    let (_metadata, evidence) = evidence_from_values(&unsigned_values, &policy.thresholds)?;
    let signature_key = CanaryEvidenceSignatureKey::from_env_var(&config.hmac_key_env)?;
    let mut metadata = CanaryEvidenceMetadata {
        run_id: config.run_id,
        generated_at: Utc::now(),
        max_age_seconds: Some(config.max_age_seconds),
        hmac_sha256: None,
    };
    metadata.hmac_sha256 = Some(sign_evidence(&metadata, &evidence, &signature_key)?);
    print_signed_jsonl(&metadata, &unsigned_values)
}

pub fn require_passing_evidence(
    path: &Path,
    thresholds: CanaryThresholds,
    expected_run_id: &str,
    signature_key: &CanaryEvidenceSignatureKey,
) -> anyhow::Result<()> {
    let report =
        evaluate_file_for_run(path, thresholds, expected_run_id, Utc::now(), signature_key)?;
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
    signature_key: &CanaryEvidenceSignatureKey,
) -> anyhow::Result<CanaryReport> {
    let policy = CanaryPolicy::design_default(thresholds);
    let (metadata, evidence) = read_evidence_file(path, &policy.thresholds)?;
    let metadata = validate_metadata(path, metadata.as_ref(), expected_run_id, now)?;
    validate_signature(path, metadata, &evidence, signature_key)?;
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

fn read_evidence_values(path: &Path) -> anyhow::Result<Vec<Value>> {
    let contents = fs::read_to_string(path)?;
    if let Ok(records) = serde_json::from_str::<Vec<Value>>(&contents) {
        return Ok(records);
    }
    let mut records = Vec::new();
    let mut count = 0_usize;
    for (line_index, line) in contents.lines().enumerate() {
        let line_number = line_index
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("canary evidence line number overflow"))?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line).map_err(|source| {
            anyhow::anyhow!(
                "parse canary evidence {} line {}: {source}",
                path.display(),
                line_number
            )
        })?;
        records.push(value);
        count = count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("canary evidence record count overflow"))?;
    }
    if count == 0 {
        anyhow::bail!("canary evidence {} contained no records", path.display());
    }
    Ok(records)
}

fn unsigned_evidence_values(values: Vec<Value>) -> anyhow::Result<Vec<Value>> {
    let mut unsigned = Vec::new();
    for value in values {
        match record_kind(&value)? {
            "metadata" | "header" => {
                let hmac = value
                    .get("hmac_sha256")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if !hmac.is_empty() {
                    anyhow::bail!("canary-sign refuses already signed canary evidence");
                }
            }
            "gate_observation" => {
                anyhow::bail!(
                    "canary-sign requires measured hard-gate records, not bare gate_observation statuses"
                );
            }
            _ => unsigned.push(value),
        }
    }
    Ok(unsigned)
}

fn record_kind(value: &Value) -> anyhow::Result<&str> {
    value
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("canary evidence record missing string kind"))
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

fn evidence_from_values(
    values: &[Value],
    thresholds: &CanaryThresholds,
) -> anyhow::Result<(Option<CanaryEvidenceMetadata>, CanaryEvidence)> {
    let records = values
        .iter()
        .cloned()
        .map(serde_json::from_value::<CanaryEvidenceRecord>)
        .collect::<Result<Vec<_>, _>>()?;
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

#[allow(clippy::too_many_lines)]
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
            hmac_sha256,
        }
        | CanaryEvidenceRecord::Header {
            run_id,
            generated_at,
            max_age_seconds,
            hmac_sha256,
        } => {
            if metadata.is_some() {
                anyhow::bail!("canary evidence contained multiple metadata records");
            }
            *metadata = Some(CanaryEvidenceMetadata {
                run_id,
                generated_at,
                max_age_seconds,
                hmac_sha256,
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

fn validate_metadata<'a>(
    path: &Path,
    metadata: Option<&'a CanaryEvidenceMetadata>,
    expected_run_id: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<&'a CanaryEvidenceMetadata> {
    #[allow(clippy::duration_suboptimal_units)]
    const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
    const ALLOWED_CLOCK_SKEW: Duration = Duration::from_secs(300);
    let metadata = metadata.ok_or_else(|| {
        anyhow::anyhow!(
            "canary evidence {} requires metadata record with run_id, generated_at, and hmac_sha256",
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
    let max_age = metadata
        .max_age_seconds
        .map_or(DEFAULT_MAX_AGE, Duration::from_secs);
    let age = if metadata.generated_at > now {
        let skew = metadata
            .generated_at
            .signed_duration_since(now)
            .to_std()
            .map_err(|_err| anyhow::anyhow!("canary evidence generated_at is in the future"))?;
        if skew > ALLOWED_CLOCK_SKEW {
            anyhow::bail!(
                "canary evidence generated_at is {skew:?} in the future, exceeding allowed clock skew {ALLOWED_CLOCK_SKEW:?}"
            );
        }
        Duration::ZERO
    } else {
        now.signed_duration_since(metadata.generated_at)
            .to_std()
            .map_err(|_err| anyhow::anyhow!("canary evidence generated_at is in the future"))?
    };
    if age > max_age {
        anyhow::bail!("canary evidence is stale: age {age:?} exceeds max age {max_age:?}");
    }
    if metadata.hmac_sha256.as_deref().is_none_or(str::is_empty) {
        anyhow::bail!(
            "canary evidence {} requires metadata hmac_sha256 for run-fleet",
            path.display()
        );
    }
    Ok(metadata)
}

fn print_signed_jsonl(metadata: &CanaryEvidenceMetadata, records: &[Value]) -> anyhow::Result<()> {
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "kind": "metadata",
            "run_id": metadata.run_id,
            "generated_at": metadata.generated_at.to_rfc3339(),
            "max_age_seconds": metadata.max_age_seconds,
            "hmac_sha256": metadata.hmac_sha256,
        }))?
    );
    for record in records {
        println!("{}", serde_json::to_string(record)?);
    }
    Ok(())
}

#[derive(Serialize)]
struct SignedCanaryEvidence<'a> {
    run_id: &'a str,
    generated_at: DateTime<Utc>,
    max_age_seconds: Option<u64>,
    evidence: &'a CanaryEvidence,
}

fn validate_signature(
    path: &Path,
    metadata: &CanaryEvidenceMetadata,
    evidence: &CanaryEvidence,
    signature_key: &CanaryEvidenceSignatureKey,
) -> anyhow::Result<()> {
    let expected = metadata
        .hmac_sha256
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("canary evidence {} missing hmac_sha256", path.display()))?;
    let expected_bytes = hex::decode(expected).map_err(|err| {
        anyhow::anyhow!(
            "canary evidence {} has invalid hmac_sha256 hex: {err}",
            path.display()
        )
    })?;
    let payload = serde_json::to_vec(&SignedCanaryEvidence {
        run_id: metadata.run_id.as_str(),
        generated_at: metadata.generated_at,
        max_age_seconds: metadata.max_age_seconds,
        evidence,
    })?;
    let mut mac = HmacSha256::new_from_slice(&signature_key.bytes)
        .map_err(|err| anyhow::anyhow!("invalid canary evidence HMAC key: {err}"))?;
    mac.update(&payload);
    mac.verify_slice(&expected_bytes).map_err(|_err| {
        anyhow::anyhow!(
            "canary evidence {} hmac_sha256 did not validate",
            path.display()
        )
    })
}

fn sign_evidence(
    metadata: &CanaryEvidenceMetadata,
    evidence: &CanaryEvidence,
    signature_key: &CanaryEvidenceSignatureKey,
) -> anyhow::Result<String> {
    let payload = serde_json::to_vec(&SignedCanaryEvidence {
        run_id: metadata.run_id.as_str(),
        generated_at: metadata.generated_at,
        max_age_seconds: metadata.max_age_seconds,
        evidence,
    })?;
    let mut mac = HmacSha256::new_from_slice(&signature_key.bytes)
        .map_err(|err| anyhow::anyhow!("invalid canary evidence HMAC key: {err}"))?;
    mac.update(&payload);
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
#[path = "canary_cmd/tests.rs"]
mod tests;
