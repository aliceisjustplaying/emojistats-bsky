use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::ledger::LedgerSeedBatchSummary;

const DEFAULT_EXPORT_PAGE_SIZE: u16 = 1_000;

/// Configuration for mirroring the PLC export into local census tables.
#[derive(Debug, Clone)]
pub struct PlcMirrorConfig {
    pub ledger_path: PathBuf,
    pub mirror_dir: PathBuf,
    pub plc_directory_url: String,
    pub page_size: u16,
    pub limit_pages: Option<u64>,
    pub limit_ops: Option<u64>,
    pub request_timeout: Duration,
    pub workers: usize,
    pub start_after: Option<u64>,
    pub end_at: Option<u64>,
}

impl PlcMirrorConfig {
    #[must_use]
    pub fn new(ledger_path: PathBuf, mirror_dir: PathBuf) -> Self {
        Self {
            ledger_path,
            mirror_dir,
            plc_directory_url: "https://plc.directory".to_owned(),
            page_size: DEFAULT_EXPORT_PAGE_SIZE,
            limit_pages: None,
            limit_ops: None,
            request_timeout: Duration::from_secs(60),
            workers: 1,
            start_after: None,
            end_at: None,
        }
    }
}

/// Summary emitted after a PLC mirror pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlcMirrorSummary {
    pub pages: u64,
    pub ops: u64,
    pub upserted: u64,
    pub tombstoned: u64,
    pub skipped: u64,
    pub cursor: u64,
    pub caught_up: bool,
}

/// Configuration for health-checking PDS hosts and seeding admitted DIDs.
#[derive(Debug, Clone)]
pub struct PdsCensusConfig {
    pub ledger_path: PathBuf,
    pub admitted_dids_path: Option<PathBuf>,
    pub quarantined_hosts_path: Option<PathBuf>,
    pub health_concurrency: usize,
    pub request_timeout: Duration,
    pub max_hosts: Option<u64>,
    pub seed_ledger: bool,
}

impl PdsCensusConfig {
    #[must_use]
    pub const fn new(ledger_path: PathBuf) -> Self {
        Self {
            ledger_path,
            admitted_dids_path: None,
            quarantined_hosts_path: None,
            health_concurrency: 64,
            request_timeout: Duration::from_secs(30),
            max_hosts: None,
            seed_ledger: true,
        }
    }
}

/// Summary emitted after the PDS census pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PdsCensusSummary {
    pub hosts_checked: u64,
    pub hosts_admitted: u64,
    pub hosts_quarantined: u64,
    pub dids_admitted: u64,
    pub seed: LedgerSeedBatchSummary,
}

#[derive(Debug, Deserialize)]
pub(super) struct PlcExportLine {
    pub(super) did: String,
    pub(super) seq: Option<u64>,
    #[serde(rename = "createdAt")]
    pub(super) created_at: Option<String>,
    pub(super) nullified: Option<bool>,
    pub(super) operation: PlcOperation,
}

#[derive(Debug, Deserialize)]
pub(super) struct PlcOperation {
    #[serde(rename = "type")]
    pub(super) kind: String,
    pub(super) service: Option<String>,
    pub(super) services: Option<BTreeMap<String, PlcService>>,
}

#[derive(Debug, Deserialize)]
pub(super) struct PlcService {
    pub(super) endpoint: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ListReposPage {
    pub(super) repos: Vec<ListReposRepo>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ListReposRepo {
    pub(super) did: String,
}

#[derive(Debug, Serialize)]
pub(super) struct QuarantinedHostRecord<'a> {
    pub(super) host: &'a str,
    pub(super) endpoint: Option<&'a str>,
    pub(super) reason: &'a str,
}

#[derive(Debug, Clone)]
pub(super) struct HostCandidate {
    pub(super) host: String,
    pub(super) endpoint: Option<String>,
}

#[derive(Debug)]
pub(super) struct PlcExportPacer {
    pub(super) next_request_at: Mutex<Instant>,
    pub(super) interval: Duration,
}

#[derive(Debug, Clone)]
pub(super) struct HostCheckResult {
    pub(super) host: String,
    pub(super) endpoint: Option<String>,
    pub(super) status: HostCensusStatus,
    pub(super) error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HostCensusStatus {
    Admitted,
    Quarantined,
}

impl HostCensusStatus {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Quarantined => "quarantined",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct PagePersistSummary {
    pub(super) ops: u64,
    pub(super) upserted: u64,
    pub(super) tombstoned: u64,
    pub(super) skipped: u64,
    pub(super) first_seq: u64,
    pub(super) cursor: u64,
}

pub(super) fn system_time_millis(time: SystemTime) -> anyhow::Result<i64> {
    let millis = time.duration_since(UNIX_EPOCH)?.as_millis();
    Ok(i64::try_from(millis)?)
}
