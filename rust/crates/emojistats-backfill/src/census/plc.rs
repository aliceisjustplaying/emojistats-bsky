use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

use reqwest::{Client, StatusCode, Url};
use tokio::time::{Instant, sleep};

use crate::census::{
    db::{create_census_schema, load_cursor, open_census_connection, persist_plc_page, set_cursor},
    types::{
        PagePersistSummary, PlcExportLine, PlcExportPacer, PlcMirrorConfig, PlcMirrorSummary,
        PlcPlanConfig, PlcRangeSummary, PlcSeqRange, SeqRange, WorkerPage,
    },
};

const PLC_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(300);
const PLC_RATE_LIMIT_REQUESTS: u32 = 2_000;
const PLC_RATE_LIMIT_HEADROOM_PERCENT: u32 = 10;

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
        add_page_summary(&mut summary, page_summary)?;
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
    let mut range_summaries = Vec::with_capacity(handles.len());
    for handle in handles {
        range_summaries.push(handle.await??);
    }
    let mut summary = writer.await??;
    let resume = parallel_resume_cursor(cursor, head_upper, &range_summaries);
    let connection = open_census_connection(&config.ledger_path)?;
    set_cursor(&connection, resume.cursor)?;
    summary.cursor = resume.cursor;
    summary.caught_up = resume.caught_up;
    Ok(summary)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParallelResume {
    cursor: u64,
    caught_up: bool,
}

fn parallel_resume_cursor(
    start_cursor: u64,
    head_upper: u64,
    range_summaries: &[PlcRangeSummary],
) -> ParallelResume {
    let completed_all_ranges = range_summaries
        .iter()
        .all(|range_summary| range_summary.complete);
    if completed_all_ranges {
        return ParallelResume {
            cursor: head_upper,
            caught_up: true,
        };
    }
    ParallelResume {
        cursor: range_summaries
            .iter()
            .map(|range_summary| range_summary.cursor)
            .min()
            .unwrap_or(start_cursor),
        caught_up: false,
    }
}

async fn fetch_plc_range(
    client: Client,
    config: PlcMirrorConfig,
    pacer: Arc<PlcExportPacer>,
    range: SeqRange,
    sender: mpsc::Sender<WorkerPage>,
) -> anyhow::Result<PlcRangeSummary> {
    let mut cursor = range.start_after;
    loop {
        let raw = fetch_plc_page(&client, &config, &pacer, cursor).await?;
        if raw.trim().is_empty() {
            return Ok(PlcRangeSummary {
                cursor,
                complete: true,
            });
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
            return Ok(PlcRangeSummary {
                cursor,
                complete: false,
            });
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
            return Ok(PlcRangeSummary {
                cursor,
                complete: true,
            });
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
        add_page_summary(&mut summary, page_summary)?;
    }
    Ok(summary)
}

fn add_page_summary(
    summary: &mut PlcMirrorSummary,
    page_summary: PagePersistSummary,
) -> anyhow::Result<()> {
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
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::{parallel_resume_cursor, pds_host_from_endpoint, split_seq_ranges};
    use crate::census::types::PlcRangeSummary;

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
    fn parallel_plc_ranges_are_disjoint_and_cover_head() {
        let ranges = split_seq_ranges(10, 23, 4).expect("ranges should split");

        let actual = ranges
            .iter()
            .map(|range| (range.start_after, range.end_inclusive))
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![(10, 14), (14, 18), (18, 22), (22, 23)]);
    }

    #[test]
    fn parallel_plc_resume_cursor_reaches_head_only_after_all_ranges_complete() {
        let summaries = [
            PlcRangeSummary {
                cursor: 14,
                complete: true,
            },
            PlcRangeSummary {
                cursor: 18,
                complete: true,
            },
            PlcRangeSummary {
                cursor: 23,
                complete: true,
            },
        ];

        let resume = parallel_resume_cursor(10, 23, &summaries);

        assert_eq!(resume.cursor, 23);
        assert!(resume.caught_up);
    }

    #[test]
    fn parallel_plc_resume_cursor_rewinds_to_lowest_incomplete_progress() {
        let summaries = [
            PlcRangeSummary {
                cursor: 14,
                complete: true,
            },
            PlcRangeSummary {
                cursor: 16,
                complete: false,
            },
            PlcRangeSummary {
                cursor: 23,
                complete: true,
            },
        ];

        let resume = parallel_resume_cursor(10, 23, &summaries);

        assert_eq!(resume.cursor, 14);
        assert!(!resume.caught_up);
    }
}
