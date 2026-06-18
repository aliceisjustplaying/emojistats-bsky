//! PLC mirror and PDS admission census for building the finite backfill queue.

use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use reqwest::{Client, StatusCode, Url};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::Semaphore,
    time::{Instant, sleep},
};

use crate::ledger::{LedgerSeedBatchSummary, SqliteLedger, did_shard_bucket};

const PLC_META_CURSOR: &str = "plc_cursor";
const DEFAULT_EXPORT_PAGE_SIZE: u16 = 1_000;
const DEFAULT_SEED_BATCH_SIZE: usize = 10_000;
const PLC_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(300);
const PLC_RATE_LIMIT_REQUESTS: u32 = 2_000;
const PLC_RATE_LIMIT_HEADROOM_PERCENT: u32 = 10;

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

/// Configuration for printing split PLC seq ranges for multiple boxes.
#[derive(Debug, Clone)]
pub struct PlcPlanConfig {
    pub plc_directory_url: String,
    pub page_size: u16,
    pub request_timeout: Duration,
    pub parts: usize,
    pub start_after: u64,
}

impl PlcPlanConfig {
    #[must_use]
    pub fn new(parts: usize) -> Self {
        Self {
            plc_directory_url: "https://plc.directory".to_owned(),
            page_size: DEFAULT_EXPORT_PAGE_SIZE,
            request_timeout: Duration::from_secs(60),
            parts,
            start_after: 0,
        }
    }
}

/// One disjoint PLC sequence range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlcSeqRange {
    pub index: usize,
    pub start_after: u64,
    pub end_at: u64,
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
struct PlcExportLine {
    did: String,
    seq: Option<u64>,
    nullified: Option<bool>,
    operation: PlcOperation,
}

#[derive(Debug, Deserialize)]
struct PlcOperation {
    #[serde(rename = "type")]
    kind: String,
    service: Option<String>,
    services: Option<BTreeMap<String, PlcService>>,
}

#[derive(Debug, Deserialize)]
struct PlcService {
    endpoint: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListReposPage {
    repos: Vec<ListReposRepo>,
}

#[derive(Debug, Deserialize)]
struct ListReposRepo {
    did: String,
}

#[derive(Debug, Serialize)]
struct QuarantinedHostRecord<'a> {
    host: &'a str,
    endpoint: Option<&'a str>,
    reason: &'a str,
}

#[derive(Debug, Clone)]
struct HostCandidate {
    host: String,
    endpoint: Option<String>,
}

struct WorkerPage {
    first_seq: u64,
    cursor: u64,
    raw: String,
    lines: Vec<PlcExportLine>,
}

#[derive(Debug)]
struct PlcExportPacer {
    next_request_at: Mutex<Instant>,
    interval: Duration,
}

impl PlcExportPacer {
    fn new() -> Self {
        let request_budget = PLC_RATE_LIMIT_REQUESTS
            .saturating_mul(100_u32.saturating_sub(PLC_RATE_LIMIT_HEADROOM_PERCENT))
            .checked_div(100)
            .unwrap_or(PLC_RATE_LIMIT_REQUESTS)
            .max(1);
        let interval = PLC_RATE_LIMIT_WINDOW
            .checked_div(request_budget)
            .unwrap_or(Duration::from_millis(150));
        Self {
            next_request_at: Mutex::new(Instant::now()),
            interval,
        }
    }

    async fn wait_turn(&self) -> anyhow::Result<()> {
        let wait_until = {
            let mut guard = self
                .next_request_at
                .lock()
                .map_err(|_err| anyhow::anyhow!("PLC pacer mutex poisoned"))?;
            let now = Instant::now();
            let scheduled = if *guard > now { *guard } else { now };
            *guard = scheduled
                .checked_add(self.interval)
                .ok_or_else(|| anyhow::anyhow!("PLC pacer instant overflow"))?;
            scheduled
        };
        sleep(wait_until.saturating_duration_since(Instant::now())).await;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct HostCheckResult {
    host: String,
    endpoint: Option<String>,
    status: HostCensusStatus,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostCensusStatus {
    Admitted,
    Quarantined,
}

impl HostCensusStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Quarantined => "quarantined",
        }
    }
}

/// Mirror PLC export pages into `plc_identities` and raw page files.
///
/// # Errors
///
/// Returns an error when the PLC export cannot be fetched, parsed, or persisted.
pub async fn mirror_plc_export(config: PlcMirrorConfig) -> anyhow::Result<PlcMirrorSummary> {
    fs::create_dir_all(config.mirror_dir.join("pages"))?;
    let mut connection = open_census_connection(&config.ledger_path)?;
    create_census_schema(&connection)?;
    let client = Client::builder().timeout(config.request_timeout).build()?;
    let pacer = Arc::new(PlcExportPacer::new());
    let mut cursor = if let Some(start_after) = config.start_after {
        start_after
    } else {
        load_cursor(&connection)?
    };
    if config.workers > 1 && config.limit_pages.is_none() && config.limit_ops.is_none() {
        return mirror_plc_export_parallel(config, client, Arc::clone(&pacer), cursor).await;
    }
    let mut summary = PlcMirrorSummary {
        cursor,
        ..PlcMirrorSummary::default()
    };

    loop {
        if config
            .limit_pages
            .is_some_and(|limit| summary.pages >= limit)
            || config.limit_ops.is_some_and(|limit| summary.ops >= limit)
        {
            return Ok(summary);
        }
        let response = fetch_plc_page(&client, &config, &pacer, cursor).await?;
        if response.trim().is_empty() {
            summary.caught_up = true;
            return Ok(summary);
        }
        let page = parse_plc_page(&response)?;
        if page.is_empty() {
            summary.caught_up = true;
            return Ok(summary);
        }
        let page_summary = persist_plc_page(&mut connection, cursor, &page, true)?;
        cursor = page_summary.cursor;
        summary.cursor = cursor;
        summary.pages = summary
            .pages
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PLC page count overflow"))?;
        summary.ops = summary
            .ops
            .checked_add(page_summary.ops)
            .ok_or_else(|| anyhow::anyhow!("PLC op count overflow"))?;
        summary.upserted = summary
            .upserted
            .checked_add(page_summary.upserted)
            .ok_or_else(|| anyhow::anyhow!("PLC upsert count overflow"))?;
        summary.tombstoned = summary
            .tombstoned
            .checked_add(page_summary.tombstoned)
            .ok_or_else(|| anyhow::anyhow!("PLC tombstone count overflow"))?;
        summary.skipped = summary
            .skipped
            .checked_add(page_summary.skipped)
            .ok_or_else(|| anyhow::anyhow!("PLC skipped count overflow"))?;
        write_plc_page(
            &config.mirror_dir,
            page_summary.first_seq,
            page_summary.cursor,
            &response,
        )?;
        if u64::try_from(page.len())? < u64::from(config.page_size) {
            summary.caught_up = true;
            return Ok(summary);
        }
    }
}

/// Discover the current PLC export head and split it into disjoint seq ranges.
///
/// # Errors
///
/// Returns an error when the PLC export cannot be probed.
pub async fn plan_plc_ranges(config: PlcPlanConfig) -> anyhow::Result<Vec<PlcSeqRange>> {
    let client = Client::builder().timeout(config.request_timeout).build()?;
    let pacer = Arc::new(PlcExportPacer::new());
    let mirror_config = PlcMirrorConfig {
        ledger_path: PathBuf::new(),
        mirror_dir: PathBuf::new(),
        plc_directory_url: config.plc_directory_url,
        page_size: config.page_size,
        limit_pages: None,
        limit_ops: None,
        request_timeout: config.request_timeout,
        workers: 1,
        start_after: None,
        end_at: None,
    };
    let head_upper =
        discover_plc_head_upper(&client, &mirror_config, &pacer, config.start_after).await?;
    split_seq_ranges(config.start_after, head_upper, config.parts)?
        .into_iter()
        .enumerate()
        .map(|(index, range)| {
            Ok(PlcSeqRange {
                index,
                start_after: range.start_after,
                end_at: range.end_inclusive,
            })
        })
        .collect()
}

async fn mirror_plc_export_parallel(
    config: PlcMirrorConfig,
    client: Client,
    pacer: Arc<PlcExportPacer>,
    cursor: u64,
) -> anyhow::Result<PlcMirrorSummary> {
    let head_upper = match config.end_at {
        Some(end_at) => end_at,
        None => discover_plc_head_upper(&client, &config, &pacer, cursor).await?,
    };
    if cursor >= head_upper {
        return Ok(PlcMirrorSummary {
            cursor,
            caught_up: true,
            ..PlcMirrorSummary::default()
        });
    }
    let ranges = split_seq_ranges(cursor, head_upper, config.workers)?;
    let (sender, receiver) = mpsc::channel::<WorkerPage>();
    let ledger_path = config.ledger_path.clone();
    let mirror_dir = config.mirror_dir.clone();
    let writer = tokio::task::spawn_blocking(move || {
        write_worker_pages(&ledger_path, &mirror_dir, receiver)
    });
    let mut handles = Vec::with_capacity(ranges.len());
    for range in ranges {
        let worker_client = client.clone();
        let worker_config = config.clone();
        let worker_pacer = Arc::clone(&pacer);
        let worker_sender = sender.clone();
        handles.push(tokio::spawn(async move {
            fetch_plc_range(
                worker_client,
                worker_config,
                worker_pacer,
                range,
                worker_sender,
            )
            .await
        }));
    }
    drop(sender);
    for handle in handles {
        handle.await??;
    }
    let mut summary = writer.await??;
    let connection = open_census_connection(&config.ledger_path)?;
    set_cursor(&connection, head_upper)?;
    summary.cursor = head_upper;
    summary.caught_up = true;
    Ok(summary)
}

/// Health-check unique PDS hosts, quarantine failed hosts, and seed admitted DIDs.
///
/// # Errors
///
/// Returns an error when census state cannot be loaded or persisted.
pub async fn run_pds_census(config: PdsCensusConfig) -> anyhow::Result<PdsCensusSummary> {
    let connection = open_census_connection(&config.ledger_path)?;
    create_census_schema(&connection)?;
    let candidates = load_host_candidates(&connection, config.max_hosts)?;
    let checks = check_hosts(
        candidates,
        config.health_concurrency,
        config.request_timeout,
    )
    .await?;
    persist_host_checks(&connection, &checks)?;
    persist_disabled_host_overrides(&connection, &checks)?;
    let mut summary = PdsCensusSummary::default();
    for check in &checks {
        summary.hosts_checked = summary
            .hosts_checked
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("host count overflow"))?;
        match check.status {
            HostCensusStatus::Admitted => {
                summary.hosts_admitted = summary
                    .hosts_admitted
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("admitted host count overflow"))?;
            }
            HostCensusStatus::Quarantined => {
                summary.hosts_quarantined = summary
                    .hosts_quarantined
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("quarantined host count overflow"))?;
            }
        }
    }
    write_quarantined_hosts(config.quarantined_hosts_path.as_deref(), &checks)?;
    let (dids_admitted, seed) = export_and_seed_admitted_dids(
        &connection,
        config.admitted_dids_path.as_deref(),
        config.seed_ledger,
    )?;
    summary.dids_admitted = dids_admitted;
    summary.seed = seed;
    Ok(summary)
}

fn open_census_connection(path: &Path) -> anyhow::Result<Connection> {
    drop(SqliteLedger::open(path)?);
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(30))?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(connection)
}

fn create_census_schema(connection: &Connection) -> anyhow::Result<()> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS plc_meta (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS plc_identities (
            did TEXT PRIMARY KEY NOT NULL,
            pds_host TEXT,
            endpoint TEXT,
            seq INTEGER NOT NULL CHECK (seq >= 0),
            nullified INTEGER NOT NULL CHECK (nullified IN (0, 1))
        );
        CREATE INDEX IF NOT EXISTS idx_plc_identities_host
            ON plc_identities (pds_host, nullified);
        CREATE TABLE IF NOT EXISTS pds_census (
            host TEXT PRIMARY KEY NOT NULL,
            endpoint TEXT,
            status TEXT NOT NULL CHECK (status IN ('admitted', 'quarantined')),
            error TEXT,
            checked_at_ms INTEGER NOT NULL,
            repo_count INTEGER
        );
        ",
    )?;
    Ok(())
}

fn load_cursor(connection: &Connection) -> anyhow::Result<u64> {
    let cursor = connection
        .query_row(
            "SELECT value FROM plc_meta WHERE key = ?1",
            params![PLC_META_CURSOR],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map_or(Ok(0_u64), |value| value.parse::<u64>())?;
    Ok(cursor)
}

fn set_cursor(connection: &Connection, cursor: u64) -> anyhow::Result<()> {
    connection.execute(
        "
        INSERT INTO plc_meta (key, value) VALUES (?1, ?2)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        ",
        params![PLC_META_CURSOR, cursor.to_string()],
    )?;
    Ok(())
}

async fn fetch_plc_page(
    client: &Client,
    config: &PlcMirrorConfig,
    pacer: &Arc<PlcExportPacer>,
    cursor: u64,
) -> anyhow::Result<String> {
    let mut url = Url::parse(&config.plc_directory_url)?;
    url.path_segments_mut()
        .map_err(|()| anyhow::anyhow!("PLC directory URL cannot be a base"))?
        .pop_if_empty()
        .push("export");
    url.query_pairs_mut()
        .append_pair("count", &config.page_size.to_string())
        .append_pair("after", &cursor.to_string());
    for attempt in 1_u32..=6 {
        pacer.wait_turn().await?;
        let response = client.get(url.clone()).send().await?;
        let status = response.status();
        if status.is_success() {
            return Ok(response.text().await?);
        }
        let headers = response.headers().clone();
        let body = response.text().await.unwrap_or_default();
        if status == StatusCode::TOO_MANY_REQUESTS && attempt < 6 {
            let delay = plc_retry_delay(&headers, attempt);
            sleep(delay).await;
            continue;
        }
        anyhow::bail!("PLC export failed with HTTP {status}: {body}");
    }
    anyhow::bail!("PLC export failed after retries")
}

fn plc_retry_delay(headers: &http::HeaderMap, attempt: u32) -> Duration {
    headers
        .get(http::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_retry_after_seconds)
        .unwrap_or_else(|| Duration::from_secs(u64::from(attempt).saturating_mul(30)))
}

fn parse_retry_after_seconds(value: &str) -> Option<Duration> {
    let seconds = value.parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds))
}

fn parse_plc_page(raw: &str) -> anyhow::Result<Vec<PlcExportLine>> {
    raw.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<PlcExportLine>(line).map_err(Into::into))
        .collect()
}

async fn discover_plc_head_upper(
    client: &Client,
    config: &PlcMirrorConfig,
    pacer: &Arc<PlcExportPacer>,
    cursor: u64,
) -> anyhow::Result<u64> {
    let mut lower = cursor;
    let mut upper = cursor.checked_add(1_000_000).ok_or_else(|| {
        anyhow::anyhow!("PLC head discovery overflow while building initial upper bound")
    })?;
    while plc_page_exists(client, config, pacer, upper).await? {
        lower = upper;
        upper = upper
            .checked_mul(2)
            .ok_or_else(|| anyhow::anyhow!("PLC head discovery upper bound overflow"))?;
    }
    while upper.saturating_sub(lower) > u64::from(config.page_size) {
        let midpoint = lower
            .checked_add(upper.saturating_sub(lower) / 2)
            .ok_or_else(|| anyhow::anyhow!("PLC head discovery midpoint overflow"))?;
        if plc_page_exists(client, config, pacer, midpoint).await? {
            lower = midpoint;
        } else {
            upper = midpoint;
        }
    }
    Ok(upper)
}

async fn plc_page_exists(
    client: &Client,
    config: &PlcMirrorConfig,
    pacer: &Arc<PlcExportPacer>,
    cursor: u64,
) -> anyhow::Result<bool> {
    Ok(!fetch_plc_page(client, config, pacer, cursor)
        .await?
        .trim()
        .is_empty())
}

#[derive(Debug, Clone, Copy)]
struct SeqRange {
    start_after: u64,
    end_inclusive: u64,
}

fn split_seq_ranges(cursor: u64, head_upper: u64, workers: usize) -> anyhow::Result<Vec<SeqRange>> {
    let worker_count = u64::try_from(workers)?;
    let span = head_upper.saturating_sub(cursor);
    let chunk = span
        .checked_add(worker_count.saturating_sub(1))
        .ok_or_else(|| anyhow::anyhow!("PLC range chunk overflow"))?
        .checked_div(worker_count)
        .ok_or_else(|| anyhow::anyhow!("PLC range worker count cannot be zero"))?;
    let mut ranges = Vec::new();
    for index in 0..worker_count {
        let offset = index
            .checked_mul(chunk)
            .ok_or_else(|| anyhow::anyhow!("PLC range offset overflow"))?;
        let start_after = cursor
            .checked_add(offset)
            .ok_or_else(|| anyhow::anyhow!("PLC range start overflow"))?;
        if start_after >= head_upper {
            break;
        }
        let end_inclusive = start_after.saturating_add(chunk).min(head_upper);
        ranges.push(SeqRange {
            start_after,
            end_inclusive,
        });
    }
    Ok(ranges)
}

async fn fetch_plc_range(
    client: Client,
    config: PlcMirrorConfig,
    pacer: Arc<PlcExportPacer>,
    range: SeqRange,
    sender: mpsc::Sender<WorkerPage>,
) -> anyhow::Result<()> {
    let mut cursor = range.start_after;
    loop {
        let raw = fetch_plc_page(&client, &config, &pacer, cursor).await?;
        if raw.trim().is_empty() {
            return Ok(());
        }
        let raw_lines: Vec<&str> = raw.lines().filter(|line| !line.trim().is_empty()).collect();
        let raw_line_count = raw_lines.len();
        let page = parse_plc_page(&raw)?;
        let mut lines = Vec::new();
        let mut page_raw = String::new();
        let mut hit_range_end = false;
        for (raw_line, line) in raw_lines.into_iter().zip(page) {
            let seq = line
                .seq
                .ok_or_else(|| anyhow::anyhow!("PLC export line missing seq for {}", line.did))?;
            if seq > range.end_inclusive {
                hit_range_end = true;
                break;
            }
            cursor = seq;
            page_raw.push_str(raw_line);
            page_raw.push('\n');
            lines.push(line);
        }
        if lines.is_empty() {
            return Ok(());
        }
        let first_seq = lines
            .first()
            .and_then(|line| line.seq)
            .ok_or_else(|| anyhow::anyhow!("PLC worker page missing first seq"))?;
        sender.send(WorkerPage {
            first_seq,
            cursor,
            raw: page_raw,
            lines,
        })?;
        if hit_range_end
            || raw_line_count < usize::from(config.page_size)
            || cursor >= range.end_inclusive
        {
            return Ok(());
        }
    }
}

fn write_worker_pages(
    ledger_path: &Path,
    mirror_dir: &Path,
    receiver: mpsc::Receiver<WorkerPage>,
) -> anyhow::Result<PlcMirrorSummary> {
    let mut connection = open_census_connection(ledger_path)?;
    create_census_schema(&connection)?;
    let mut summary = PlcMirrorSummary::default();
    for page in receiver {
        let page_summary = persist_plc_page(&mut connection, summary.cursor, &page.lines, false)?;
        write_plc_page(mirror_dir, page.first_seq, page.cursor, &page.raw)?;
        summary.pages = summary
            .pages
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PLC page count overflow"))?;
        summary.ops = summary
            .ops
            .checked_add(page_summary.ops)
            .ok_or_else(|| anyhow::anyhow!("PLC op count overflow"))?;
        summary.upserted = summary
            .upserted
            .checked_add(page_summary.upserted)
            .ok_or_else(|| anyhow::anyhow!("PLC upsert count overflow"))?;
        summary.tombstoned = summary
            .tombstoned
            .checked_add(page_summary.tombstoned)
            .ok_or_else(|| anyhow::anyhow!("PLC tombstone count overflow"))?;
        summary.skipped = summary
            .skipped
            .checked_add(page_summary.skipped)
            .ok_or_else(|| anyhow::anyhow!("PLC skipped count overflow"))?;
    }
    Ok(summary)
}

#[derive(Debug, Clone, Copy, Default)]
struct PagePersistSummary {
    ops: u64,
    upserted: u64,
    tombstoned: u64,
    skipped: u64,
    first_seq: u64,
    cursor: u64,
}

fn persist_plc_page(
    connection: &mut Connection,
    previous_cursor: u64,
    page: &[PlcExportLine],
    update_cursor: bool,
) -> anyhow::Result<PagePersistSummary> {
    let transaction = connection.transaction()?;
    let mut summary = PagePersistSummary {
        first_seq: previous_cursor,
        cursor: previous_cursor,
        ..PagePersistSummary::default()
    };
    for line in page {
        let seq = line
            .seq
            .ok_or_else(|| anyhow::anyhow!("PLC export line missing seq for {}", line.did))?;
        if summary.ops == 0 {
            summary.first_seq = seq;
        }
        summary.cursor = seq;
        summary.ops = summary
            .ops
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PLC page op count overflow"))?;
        if line.nullified == Some(true) || line.operation.kind == "plc_tombstone" {
            upsert_plc_identity(&transaction, &line.did, None, None, seq, true)?;
            summary.tombstoned = summary
                .tombstoned
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("PLC page tombstone count overflow"))?;
            continue;
        }
        let Some(endpoint) = endpoint_from_operation(&line.operation) else {
            summary.skipped = summary
                .skipped
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("PLC page skipped count overflow"))?;
            continue;
        };
        let Some(host) = pds_host_from_endpoint(&endpoint) else {
            summary.skipped = summary
                .skipped
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("PLC page skipped count overflow"))?;
            continue;
        };
        upsert_plc_identity(
            &transaction,
            &line.did,
            Some(&host),
            Some(&endpoint),
            seq,
            false,
        )?;
        summary.upserted = summary
            .upserted
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PLC page upsert count overflow"))?;
    }
    if update_cursor {
        transaction.execute(
            "
            INSERT INTO plc_meta (key, value) VALUES (?1, ?2)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            ",
            params![PLC_META_CURSOR, summary.cursor.to_string()],
        )?;
    }
    transaction.commit()?;
    Ok(summary)
}

fn upsert_plc_identity(
    transaction: &Transaction<'_>,
    did: &str,
    host: Option<&str>,
    endpoint: Option<&str>,
    seq: u64,
    nullified: bool,
) -> anyhow::Result<()> {
    let seq_i64 = i64::try_from(seq)?;
    transaction.execute(
        "
        INSERT INTO plc_identities (did, pds_host, endpoint, seq, nullified)
        VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(did) DO UPDATE SET
            pds_host = excluded.pds_host,
            endpoint = excluded.endpoint,
            seq = excluded.seq,
            nullified = excluded.nullified
        WHERE excluded.seq >= plc_identities.seq
        ",
        params![did, host, endpoint, seq_i64, i64::from(nullified)],
    )?;
    Ok(())
}

fn endpoint_from_operation(operation: &PlcOperation) -> Option<String> {
    if operation.kind == "create" {
        return operation.service.clone();
    }
    if operation.kind == "plc_operation" {
        return operation
            .services
            .as_ref()
            .and_then(|services| services.get("atproto_pds"))
            .and_then(|service| service.endpoint.clone());
    }
    None
}

/// Normalize a PDS endpoint into the host key used by the crawl ledger.
#[must_use]
pub fn pds_host_from_endpoint(endpoint: &str) -> Option<String> {
    let url = Url::parse(endpoint).ok()?;
    let host = url.host_str()?.to_lowercase();
    if host.is_empty() {
        return None;
    }
    let host_with_port = url
        .port()
        .map_or_else(|| host.clone(), |port| format!("{host}:{port}"));
    match url.scheme() {
        "https" => Some(host_with_port),
        "http" => Some(format!("http://{host_with_port}")),
        _ => None,
    }
}

fn write_plc_page(
    mirror_dir: &Path,
    first_seq: u64,
    cursor: u64,
    response: &str,
) -> anyhow::Result<()> {
    let path = mirror_dir
        .join("pages")
        .join(format!("{first_seq:020}-{cursor:020}.jsonl"));
    if path.try_exists()? {
        return Ok(());
    }
    fs::write(path, response)?;
    Ok(())
}

fn load_host_candidates(
    connection: &Connection,
    max_hosts: Option<u64>,
) -> anyhow::Result<Vec<HostCandidate>> {
    let limit = max_hosts
        .map(i64::try_from)
        .transpose()?
        .unwrap_or(i64::MAX);
    let mut statement = connection.prepare(
        "
        SELECT pds_host, MIN(endpoint)
        FROM plc_identities
        WHERE nullified = 0 AND pds_host IS NOT NULL
        GROUP BY pds_host
        ORDER BY pds_host
        LIMIT ?1
        ",
    )?;
    let rows = statement.query_map(params![limit], |row| {
        Ok(HostCandidate {
            host: row.get(0)?,
            endpoint: row.get(1)?,
        })
    })?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

async fn check_hosts(
    candidates: Vec<HostCandidate>,
    concurrency: usize,
    timeout: Duration,
) -> anyhow::Result<Vec<HostCheckResult>> {
    let client = Client::builder().timeout(timeout).build()?;
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut handles = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let client = client.clone();
        let semaphore = Arc::clone(&semaphore);
        handles.push(tokio::spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .map_err(|err| anyhow::anyhow!("host census semaphore closed: {err}"))?;
            Ok::<_, anyhow::Error>(check_one_host(&client, candidate).await)
        }));
    }
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(handle.await??);
    }
    Ok(results)
}

async fn check_one_host(client: &Client, candidate: HostCandidate) -> HostCheckResult {
    if let Some(reason) = classify_unusable_pds_host(&candidate.host) {
        return HostCheckResult {
            host: candidate.host,
            endpoint: candidate.endpoint,
            status: HostCensusStatus::Quarantined,
            error: Some(format!("non-public PDS address: {reason}")),
        };
    }
    let url = list_repos_health_url(&candidate.host);
    let result = async {
        let response = client.get(url).send().await?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("HTTP {status}");
        }
        let page = response.json::<ListReposPage>().await?;
        let repo_count = page.repos.len();
        if repo_count == 0 {
            anyhow::bail!("empty listRepos health page");
        }
        let first_did_empty = page
            .repos
            .first()
            .is_some_and(|repo| repo.did.trim().is_empty());
        if first_did_empty {
            anyhow::bail!("listRepos returned empty DID");
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    match result {
        Ok(()) => HostCheckResult {
            host: candidate.host,
            endpoint: candidate.endpoint,
            status: HostCensusStatus::Admitted,
            error: None,
        },
        Err(err) => HostCheckResult {
            host: candidate.host,
            endpoint: candidate.endpoint,
            status: HostCensusStatus::Quarantined,
            error: Some(err.to_string()),
        },
    }
}

fn list_repos_health_url(host: &str) -> String {
    let base = if host.starts_with("http://") || host.starts_with("https://") {
        host.to_owned()
    } else {
        format!("https://{host}")
    };
    format!("{base}/xrpc/com.atproto.sync.listRepos?limit=1")
}

fn persist_host_checks(connection: &Connection, checks: &[HostCheckResult]) -> anyhow::Result<()> {
    let checked_at_ms = system_time_millis(SystemTime::now())?;
    let mut statement = connection.prepare(
        "
        INSERT INTO pds_census (host, endpoint, status, error, checked_at_ms, repo_count)
        VALUES (?1, ?2, ?3, ?4, ?5, NULL)
        ON CONFLICT(host) DO UPDATE SET
            endpoint = excluded.endpoint,
            status = excluded.status,
            error = excluded.error,
            checked_at_ms = excluded.checked_at_ms,
            repo_count = excluded.repo_count
        ",
    )?;
    for check in checks {
        statement.execute(params![
            check.host.as_str(),
            check.endpoint.as_deref(),
            check.status.as_str(),
            check.error.as_deref(),
            checked_at_ms,
        ])?;
    }
    Ok(())
}

fn persist_disabled_host_overrides(
    connection: &Connection,
    checks: &[HostCheckResult],
) -> anyhow::Result<()> {
    let mut statement = connection.prepare(
        "
        INSERT INTO host_overrides (
            host,
            disabled,
            concurrency_cap,
            min_interval_ms,
            revive_after_ms,
            force_mode,
            force_mode_revive_after_ms,
            never_diff
        ) VALUES (?1, 1, NULL, NULL, NULL, NULL, NULL, 1)
        ON CONFLICT(host) DO UPDATE SET
            disabled = 1,
            revive_after_ms = NULL,
            never_diff = 1
        ",
    )?;
    for check in checks
        .iter()
        .filter(|check| check.status == HostCensusStatus::Quarantined)
    {
        statement.execute(params![check.host.as_str()])?;
    }
    Ok(())
}

fn write_quarantined_hosts(path: Option<&Path>, checks: &[HostCheckResult]) -> anyhow::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    for check in checks
        .iter()
        .filter(|check| check.status == HostCensusStatus::Quarantined)
    {
        let reason = check.error.as_deref().unwrap_or("health check failed");
        let record = QuarantinedHostRecord {
            host: check.host.as_str(),
            endpoint: check.endpoint.as_deref(),
            reason,
        };
        serde_json::to_writer(&mut file, &record)?;
        file.write_all(b"\n")?;
    }
    file.sync_all()?;
    Ok(())
}

fn export_and_seed_admitted_dids(
    connection: &Connection,
    admitted_dids_path: Option<&Path>,
    seed_ledger: bool,
) -> anyhow::Result<(u64, LedgerSeedBatchSummary)> {
    let mut writer = admitted_dids_path
        .map(|path| {
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)
        })
        .transpose()?;
    let mut dids_admitted = 0_u64;
    let mut seed = LedgerSeedBatchSummary::default();
    let mut statement = connection.prepare(
        "
        SELECT i.did
        FROM plc_identities AS i
        JOIN pds_census AS h ON h.host = i.pds_host
        WHERE i.nullified = 0 AND h.status = 'admitted'
        ORDER BY i.did
        ",
    )?;
    let mut rows = statement.query([])?;
    let mut batch = Vec::with_capacity(DEFAULT_SEED_BATCH_SIZE);
    while let Some(row) = rows.next()? {
        let did: String = row.get(0)?;
        if let Some(file) = writer.as_mut() {
            file.write_all(did.as_bytes())?;
            file.write_all(b"\n")?;
        }
        dids_admitted = dids_admitted
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("admitted DID count overflow"))?;
        if seed_ledger {
            batch.push(did);
            if batch.len() >= DEFAULT_SEED_BATCH_SIZE {
                add_seed_batch(connection, &batch, &mut seed)?;
                batch.clear();
            }
        }
    }
    if seed_ledger && !batch.is_empty() {
        add_seed_batch(connection, &batch, &mut seed)?;
    }
    if let Some(file) = writer.as_mut() {
        file.sync_all()?;
    }
    Ok((dids_admitted, seed))
}

fn add_seed_batch(
    connection: &Connection,
    dids: &[String],
    summary: &mut LedgerSeedBatchSummary,
) -> anyhow::Result<()> {
    let transaction = connection.unchecked_transaction()?;
    {
        let mut statement = transaction.prepare(
            "
            INSERT OR IGNORE INTO repo_ledger (
                did,
                shard_bucket,
                status,
                attempts
            ) VALUES (?1, ?2, 'pending', 0)
            ",
        )?;
        for did in dids {
            let changed = statement.execute(params![did, shard_bucket_i64(did)?])?;
            if changed == 0 {
                summary.existing = summary
                    .existing
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("seed existing count overflow"))?;
            } else {
                summary.inserted = summary
                    .inserted
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("seed insert count overflow"))?;
            }
        }
    }
    transaction.commit()?;
    Ok(())
}

fn shard_bucket_i64(did: &str) -> anyhow::Result<i64> {
    Ok(i64::try_from(did_shard_bucket(did))?)
}

fn system_time_millis(time: SystemTime) -> anyhow::Result<i64> {
    let millis = time.duration_since(UNIX_EPOCH)?.as_millis();
    Ok(i64::try_from(millis)?)
}

fn classify_unusable_pds_host(host: &str) -> Option<&'static str> {
    let normalized = normalized_hostname(host)?;
    if normalized == "localhost" {
        return Some("loopback");
    }
    if has_reserved_suffix(&normalized) {
        if normalized.ends_with(".localhost") {
            return Some("loopback");
        }
        return Some("reserved");
    }
    if let Ok(ip) = normalized.parse::<IpAddr>() {
        return classify_ip(ip);
    }
    None
}

fn normalized_hostname(host: &str) -> Option<String> {
    let url = if host.starts_with("http://") || host.starts_with("https://") {
        Url::parse(host).ok()?
    } else {
        Url::parse(&format!("https://{host}")).ok()?
    };
    Some(url.host_str()?.to_lowercase())
}

fn has_reserved_suffix(host: &str) -> bool {
    let last_label = host.rsplit_once('.').map_or(host, |(_head, tail)| tail);
    matches!(
        last_label,
        "test" | "invalid" | "example" | "local" | "localhost" | "internal"
    ) || host == "home.arpa"
        || host
            .strip_suffix(".home.arpa")
            .is_some_and(|prefix| !prefix.is_empty())
}

fn classify_ip(ip: IpAddr) -> Option<&'static str> {
    match ip {
        IpAddr::V4(ipv4) => classify_ipv4(ipv4),
        IpAddr::V6(ipv6) => classify_ipv6(ipv6),
    }
}

fn classify_ipv4(ip: Ipv4Addr) -> Option<&'static str> {
    if ip.is_loopback() || ip.is_unspecified() {
        return Some("loopback");
    }
    if ip.is_private() {
        return Some("private");
    }
    if ip.is_link_local() {
        return Some("link-local");
    }
    if ip.octets().first().is_some_and(|first| *first >= 224) {
        return Some("reserved");
    }
    None
}

const fn classify_ipv6(ip: Ipv6Addr) -> Option<&'static str> {
    if ip.is_loopback() {
        return Some("loopback");
    }
    if ip.is_unspecified() {
        return Some("reserved");
    }
    if ip.is_unique_local() {
        return Some("private");
    }
    if ip.is_unicast_link_local() {
        return Some("link-local");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{classify_unusable_pds_host, pds_host_from_endpoint};

    #[test]
    fn endpoint_host_normalization_preserves_http_scheme() {
        assert_eq!(
            pds_host_from_endpoint("https://example.com/xrpc"),
            Some("example.com".to_owned())
        );
        assert_eq!(
            pds_host_from_endpoint("http://example.com:2583"),
            Some("http://example.com:2583".to_owned())
        );
    }

    #[test]
    fn host_policy_rejects_non_public_hosts() {
        assert_eq!(classify_unusable_pds_host("localhost"), Some("loopback"));
        assert_eq!(classify_unusable_pds_host("10.0.0.1"), Some("private"));
        assert_eq!(classify_unusable_pds_host("pds.example"), Some("reserved"));
        assert_eq!(classify_unusable_pds_host("pds.example.com"), None);
    }
}
