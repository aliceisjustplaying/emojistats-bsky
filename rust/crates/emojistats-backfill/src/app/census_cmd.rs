use std::time::Duration;

use emojistats_backfill::census::{
    PdsCensusConfig, PlcMirrorConfig, mirror_plc_export, run_pds_census,
};

use super::cli::Command;

pub(super) async fn run_plc_mirror_command(command: Command) -> anyhow::Result<()> {
    let Command::PlcMirror {
        ledger_path,
        mirror_dir,
        plc_directory_url,
        page_size,
        limit_pages,
        limit_ops,
        request_timeout_secs,
        workers,
        start_after,
        end_at,
    } = command
    else {
        anyhow::bail!("internal command dispatch mismatch for plc-mirror");
    };
    let mut config = PlcMirrorConfig::new(ledger_path, mirror_dir);
    config.plc_directory_url = plc_directory_url;
    config.page_size = page_size;
    config.limit_pages = limit_pages;
    config.limit_ops = limit_ops;
    config.request_timeout = Duration::from_secs(request_timeout_secs);
    config.workers = workers;
    config.start_after = start_after;
    config.end_at = end_at;
    let summary = mirror_plc_export(config).await?;
    println!(
        "plc_mirror pages={} ops={} upserted={} tombstoned={} skipped={} cursor={} caught_up={}",
        summary.pages,
        summary.ops,
        summary.upserted,
        summary.tombstoned,
        summary.skipped,
        summary.cursor,
        summary.caught_up
    );
    Ok(())
}

pub(super) async fn run_pds_census_command(command: Command) -> anyhow::Result<()> {
    let Command::PdsCensus {
        ledger_path,
        admitted_dids_file,
        quarantined_hosts_file,
        health_concurrency,
        request_timeout_secs,
        max_hosts,
        no_seed_ledger,
    } = command
    else {
        anyhow::bail!("internal command dispatch mismatch for pds-census");
    };
    let mut config = PdsCensusConfig::new(ledger_path);
    config.admitted_dids_path = admitted_dids_file;
    config.quarantined_hosts_path = quarantined_hosts_file;
    config.health_concurrency = health_concurrency;
    config.request_timeout = Duration::from_secs(request_timeout_secs);
    config.max_hosts = max_hosts;
    config.seed_ledger = !no_seed_ledger;
    let summary = run_pds_census(config).await?;
    println!(
        "pds_census hosts_checked={} hosts_admitted={} hosts_quarantined={} dids_admitted={} seed_inserted={} seed_existing={}",
        summary.hosts_checked,
        summary.hosts_admitted,
        summary.hosts_quarantined,
        summary.dids_admitted,
        summary.seed.inserted,
        summary.seed.existing
    );
    Ok(())
}
