use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use reqwest::{Client, StatusCode, Url};
use tokio::time::{Instant, sleep};

use crate::census::{
    db::{create_census_schema, load_cursor, open_census_connection, persist_plc_page},
    types::{PagePersistSummary, PlcExportLine, PlcExportPacer, PlcMirrorConfig, PlcMirrorSummary},
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
    if config.workers > 1 {
        anyhow::bail!(
            "parallel PLC mirror is disabled for plc.directory because its export cursor is createdAt, not seq"
        );
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
        if config.end_at.is_some_and(|end_at| cursor >= end_at) {
            return Ok(summary);
        }
    }
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
        .append_pair("after", &plc_after_cursor(cursor)?);
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

fn plc_after_cursor(cursor: u64) -> anyhow::Result<String> {
    if cursor == 0 {
        return Ok("1970-01-01T00:00:00.000Z".to_owned());
    }
    let millis = i64::try_from(cursor)?;
    let timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(millis)
        .ok_or_else(|| anyhow::anyhow!("PLC cursor millis out of timestamp range: {cursor}"))?;
    Ok(timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

#[cfg(test)]
fn plc_line_cursor(line: &PlcExportLine) -> anyhow::Result<u64> {
    if let Some(created_at) = line.created_at.as_deref() {
        let timestamp = chrono::DateTime::parse_from_rfc3339(created_at)?;
        return u64::try_from(timestamp.timestamp_millis()).map_err(Into::into);
    }
    line.seq
        .ok_or_else(|| anyhow::anyhow!("PLC export line missing createdAt/seq for {}", line.did))
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
    use super::{parse_plc_page, pds_host_from_endpoint, plc_after_cursor, plc_line_cursor};

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
    fn plc_directory_export_lines_use_created_at_cursor() {
        let page = parse_plc_page(
            r#"{"did":"did:plc:abc","createdAt":"2022-11-17T00:35:16.391Z","operation":{"type":"create","service":"https://bsky.social"},"nullified":false}"#,
        )
        .expect("parse plc page");
        let cursor = plc_line_cursor(page.first().expect("line")).expect("cursor");

        assert_eq!(cursor, 1_668_645_316_391);
        assert_eq!(
            plc_after_cursor(cursor).expect("format cursor"),
            "2022-11-17T00:35:16.391Z"
        );
    }
}
