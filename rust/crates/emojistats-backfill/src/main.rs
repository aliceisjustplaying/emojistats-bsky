//! emojistats-backfill — v2 Rust backfill CLI.
//!
//! Vertical-slice milestone in progress: `fetch-one <did>` resolves a DID to its PDS, then
//! (incrementally) fetches the repo via the streaming `getRepo` seam, proves snapshot
//! completeness, archives posts, and derives emoji rows. See `docs/backfill-v2-design.md`
//! ("First implementation milestone").

use clap::{Parser, Subcommand};
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
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::FetchOne { did } => fetch_one(&did).await,
    }
}

/// Resolve a DID to its PDS endpoint.
///
/// Remaining milestone steps build on this: `getRepo` via the `download()` seam over our
/// own reqwest `HttpClient` (capturing rate-limit headers), spool the `CAR` under Loud
/// Resource Caps, parse via an on-disk `BlockStore` + `MST` walk, prove Snapshot
/// Completeness, compute the row-content receipt, write `Parquet` + a manifest entry, and
/// derive emoji rows.
async fn fetch_one(did_str: &str) -> anyhow::Result<()> {
    let did: Did =
        Did::new_owned(did_str).map_err(|err| anyhow::anyhow!("invalid DID {did_str:?}: {err}"))?;

    let resolver = PublicResolver::default();
    let pds = resolver
        .pds_for_did(&did)
        .await
        .map_err(|err| anyhow::anyhow!("resolve PDS for {did_str}: {err}"))?;

    println!("{did_str} -> PDS {pds}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Command};

    #[test]
    fn parses_fetch_one_did() {
        let cli =
            Cli::try_parse_from(["emojistats-backfill", "fetch-one", "did:plc:abc123"]).unwrap();
        let Command::FetchOne { did } = cli.command;
        assert_eq!(did, "did:plc:abc123");
    }

    #[test]
    fn requires_a_subcommand() {
        assert!(Cli::try_parse_from(["emojistats-backfill"]).is_err());
    }
}
