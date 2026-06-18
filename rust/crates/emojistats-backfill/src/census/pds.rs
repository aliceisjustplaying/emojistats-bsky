use std::{
    fs::OpenOptions,
    io::Write,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    path::Path,
    sync::Arc,
    time::Duration,
};

use reqwest::{Client, Url};
use rusqlite::{Connection, params};
use tokio::sync::Semaphore;

use crate::{
    census::{
        db::{
            create_census_schema, load_host_candidates, open_census_connection,
            persist_disabled_host_overrides, persist_host_checks,
        },
        types::{
            HostCandidate, HostCensusStatus, HostCheckResult, ListReposPage, PdsCensusConfig,
            PdsCensusSummary, QuarantinedHostRecord,
        },
    },
    ledger::{LedgerSeedBatchSummary, did_shard_bucket},
};

const DEFAULT_SEED_BATCH_SIZE: usize = 10_000;

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
    use super::classify_unusable_pds_host;

    #[test]
    fn host_policy_rejects_non_public_hosts() {
        assert_eq!(classify_unusable_pds_host("localhost"), Some("loopback"));
        assert_eq!(classify_unusable_pds_host("10.0.0.1"), Some("private"));
        assert_eq!(classify_unusable_pds_host("pds.example"), Some("reserved"));
        assert_eq!(classify_unusable_pds_host("pds.example.com"), None);
    }
}
