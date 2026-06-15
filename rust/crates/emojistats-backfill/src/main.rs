//! emojistats-backfill — v2 Rust backfill CLI.
//!
//! Vertical-slice milestone in progress: `fetch-one <did>` resolves a DID to its PDS, then
//! (incrementally) fetches the repo via the streaming `getRepo` seam, proves snapshot
//! completeness, archives posts, and derives emoji rows. See `docs/backfill-v2-design.md`
//! ("First implementation milestone").

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use emojistats_backfill::{
    archive::{
        archive_rows_from_parsed_repo, build_repo_receipt, current_normalizer,
        write_archive_artifacts,
    },
    parse::parse_repo,
    transport::{FetchConfig, fetch_repo},
};
use jacquard_common::types::did::Did;
use jacquard_identity::{PublicResolver, resolver::IdentityResolver};

/// emojistats v2 backfill tool.
#[derive(Parser, Debug)]
#[command(name = "emojistats-backfill", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Fetch and process a single repo by DID (vertical-slice milestone).
    FetchOne {
        /// The DID to fetch, e.g. did:plc:....
        did: String,
        /// Directory for local `CAR` spooling.
        #[arg(long, default_value = "data/spool")]
        spool_dir: PathBuf,
        /// Loud single-repo byte cap for the spooled `CAR`.
        #[arg(long, default_value_t = 2_147_483_648)]
        max_bytes: u64,
        /// Directory for local archive artifacts.
        #[arg(long, default_value = "data/archive")]
        archive_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::FetchOne {
            did,
            spool_dir,
            max_bytes,
            archive_dir,
        } => fetch_one(&did, spool_dir, max_bytes, archive_dir).await,
    }
}

/// Resolve a DID to its PDS endpoint.
///
/// Remaining milestone steps build on this: `getRepo` via the `download()` seam over our
/// own reqwest `HttpClient` (capturing rate-limit headers), spool the `CAR` under Loud
/// Resource Caps, parse via an on-disk `BlockStore` + `MST` walk, prove Snapshot
/// Completeness, compute the row-content receipt, write `Parquet` + a manifest entry, and
/// derive emoji rows.
async fn fetch_one(
    did_str: &str,
    spool_dir: PathBuf,
    max_bytes: u64,
    archive_dir: PathBuf,
) -> anyhow::Result<()> {
    let did: Did =
        Did::new_owned(did_str).map_err(|err| anyhow::anyhow!("invalid DID {did_str:?}: {err}"))?;

    let resolver = PublicResolver::default();
    let pds = resolver
        .pds_for_did(&did)
        .await
        .map_err(|err| anyhow::anyhow!("resolve PDS for {did_str}: {err}"))?;

    println!("{did_str} -> PDS {pds}");
    let http = reqwest::Client::new();
    let mut config = FetchConfig::new(spool_dir);
    config.max_bytes = max_bytes;

    let spooled = fetch_repo(&http, &pds, &did, &config)
        .await
        .map_err(|err| anyhow::anyhow!("fetch getRepo for {did_str}: {err}"))?;
    println!(
        "spooled {} bytes from HTTP {} to {}",
        spooled.bytes,
        spooled.http_status,
        spooled.car_path.display()
    );

    let parsed = parse_repo(&spooled.car_path)
        .map_err(|err| anyhow::anyhow!("parse CAR for {did_str}: {err}"))?;
    let rows = archive_rows_from_parsed_repo(&parsed);
    let receipt = build_repo_receipt(
        &rows,
        parsed.rkey_digest.all_records_count,
        Some(parsed.commit.data.clone()),
        Some(parsed.commit.cid.clone()),
        current_normalizer(),
    );
    let artifacts = write_archive_artifacts(&archive_dir, did_str, &rows, &receipt)
        .map_err(|err| anyhow::anyhow!("write archive artifacts for {did_str}: {err}"))?;
    println!(
        "parsed {} records, {} posts, {} decode errors, {} emoji rows, receipt {}",
        parsed.rkey_digest.all_records_count,
        receipt.all_posts_count,
        parsed.record_decode_errors.len(),
        artifacts.emoji_rows,
        receipt.post_rows_hash
    );
    println!(
        "wrote archive {}, receipt {}, manifest {}, emoji projection {}",
        artifacts.parquet_path.display(),
        artifacts.receipt_path.display(),
        artifacts.manifest_path.display(),
        artifacts.emoji_projection_path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;

    use super::{Cli, Command};

    #[test]
    fn parses_fetch_one_did() {
        let cli =
            Cli::try_parse_from(["emojistats-backfill", "fetch-one", "did:plc:abc123"]).unwrap();
        let Command::FetchOne {
            did,
            spool_dir,
            max_bytes,
            archive_dir,
        } = cli.command;
        assert_eq!(did, "did:plc:abc123");
        assert_eq!(spool_dir, PathBuf::from("data/spool"));
        assert_eq!(max_bytes, 2_147_483_648);
        assert_eq!(archive_dir, PathBuf::from("data/archive"));
    }

    #[test]
    fn requires_a_subcommand() {
        assert!(Cli::try_parse_from(["emojistats-backfill"]).is_err());
    }
}
