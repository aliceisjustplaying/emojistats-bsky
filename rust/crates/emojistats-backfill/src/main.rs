//! emojistats-backfill — v2 Rust backfill CLI (skeleton).
//!
//! This is the foundation scaffold for the emojistats v2 backfill rewrite. Only argument
//! parsing exists today; no fetch/parse/verify/archive logic is implemented yet. See
//! `docs/backfill-v2-design.md` ("First implementation milestone") for the vertical slice
//! this binary will grow into.

use clap::{Parser, Subcommand};

/// emojistats v2 backfill tool.
#[derive(Parser, Debug)]
#[command(name = "emojistats-backfill", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Fetch and process a single repo by DID (vertical-slice milestone; not yet implemented).
    FetchOne {
        /// The DID to fetch, e.g. did:plc:....
        did: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::FetchOne { did } => {
            // TODO: First implementation milestone — `fetch-one <did>` must, in order:
            //   - resolve DID/PDS (jacquard-identity);
            //   - fetch `getRepo` with bounded streaming via the low-level
            //     `client.xrpc(base).download()` seam over our own reqwest HttpClient,
            //     capturing rate-limit response headers (no high-level Agent/send path);
            //   - spool the CAR to local disk under Loud Resource Caps (every disk / repo
            //     size / block size / block count / record count / MST depth / wall-clock /
            //     idle / parse-progress / upload-progress limit emits an explicit status);
            //   - parse through an on-disk BlockStore + MST walk (Jacquard's in-RAM
            //     `Repository` cannot hold multi-GB whales; rkeys live in MST leaf keys);
            //   - extract `app.bsky.feed.post` records;
            //   - extract the profile sidecar (`app.bsky.actor.profile/self`) if present;
            //   - compute Snapshot Completeness (reconstructed root CID == commit.data, every
            //     reachable node/record block resolves by CID) — not authorship/identity;
            //   - compute the canonical ordered row-content receipt (row hashes, counts,
            //     normalizer/unicode/emoji-data versions, verification booleans);
            //   - write local Parquet (Data-Model Lossless schema);
            //   - write a local manifest entry (run/shard/sequence/dataset/path/counts/
            //     bytes/content hash/min-max timestamp/receipt hash/versions);
            //   - derive compact emoji serving rows locally (or into scratch ClickHouse).
            println!(
                "fetch-one {did}: not yet implemented — see docs/backfill-v2-design.md (First implementation milestone)"
            );
        }
    }
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
