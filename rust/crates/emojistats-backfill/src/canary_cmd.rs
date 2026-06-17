use std::{
    fs,
    path::{Path, PathBuf},
};

use emojistats_backfill::canary::{
    CanaryEvidence, CanaryFailureInjection, CanaryGateObservation, CanaryHardGate,
    CanaryInjectionObservation, CanaryPolicy, CanaryReport, CanarySampleCategory,
    CanarySampleObservation, CanaryStatus, CanaryThresholds, evaluate_canary,
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
        status: CanaryStatus,
        #[serde(default)]
        detail: Option<String>,
    },
    Gate {
        gate: CanaryHardGate,
        status: CanaryStatus,
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

fn evaluate_file(path: &Path, thresholds: CanaryThresholds) -> anyhow::Result<CanaryReport> {
    let policy = CanaryPolicy::design_default(thresholds);
    let evidence = read_evidence_file(path)?;
    Ok(evaluate_canary(&policy, &evidence))
}

fn read_evidence_file(path: &Path) -> anyhow::Result<CanaryEvidence> {
    let contents = fs::read_to_string(path)?;
    if let Ok(evidence) = serde_json::from_str::<CanaryEvidence>(&contents) {
        return Ok(evidence);
    }
    read_jsonl_evidence(path, &contents)
}

fn read_jsonl_evidence(path: &Path, contents: &str) -> anyhow::Result<CanaryEvidence> {
    let mut evidence = CanaryEvidence::default();
    let mut records = 0_usize;

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
        push_record(&mut evidence, record);
        records = records
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("canary evidence record count overflow"))?;
    }

    if records == 0 {
        anyhow::bail!("canary evidence {} contained no records", path.display());
    }
    Ok(evidence)
}

fn push_record(evidence: &mut CanaryEvidence, record: CanaryEvidenceRecord) {
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
            detail,
        }
        | CanaryEvidenceRecord::Gate {
            gate,
            status,
            detail,
        } => evidence.gates.push(CanaryGateObservation {
            gate,
            status,
            detail,
        }),
    }
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests;
