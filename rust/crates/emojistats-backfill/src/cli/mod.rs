use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use emojistats_backfill::{ledger::ShardFilter, parse::default_cid_verification_threads};

const DEFAULT_PARSE_CONCURRENCY: usize = 1;
const DEFAULT_MAX_INFLIGHT_SPOOL_BYTES: u64 = 2_147_483_648;

/// emojistats v2 backfill tool.
#[derive(Parser, Debug)]
#[command(name = "emojistats-backfill", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum HttpProtocol {
    /// Force HTTP/1.1 for repo fetches.
    Http1,
    /// Let reqwest negotiate the protocol.
    Auto,
}

#[derive(Subcommand, Debug)]
pub enum Command {
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
        /// Worker threads used for CAR block CID verification.
        #[arg(long, default_value_t = default_cid_verification_threads(), value_parser = parse_positive_usize)]
        cid_verification_threads: usize,
        /// HTTP protocol behavior for repo fetches.
        #[arg(long, value_enum, default_value_t = HttpProtocol::Http1)]
        http_protocol: HttpProtocol,
    },
    /// Parse and archive an existing `CAR` without fetching it.
    ProfileCar {
        /// The DID expected in the repo commit.
        did: String,
        /// Existing `CAR` path.
        car_path: PathBuf,
        /// Directory for local archive artifacts.
        #[arg(long, default_value = "data/profile-archive")]
        archive_dir: PathBuf,
        /// Worker threads used for CAR block CID verification.
        #[arg(long, default_value_t = default_cid_verification_threads(), value_parser = parse_positive_usize)]
        cid_verification_threads: usize,
        /// Parse and count posts without writing archive artifacts.
        #[arg(long)]
        parse_only: bool,
    },
    /// Seed, claim, and process repos from a newline-delimited DID file.
    RunFleet {
        /// Newline-delimited file of DIDs to seed into the SQLite ledger.
        dids_file: PathBuf,
        /// SQLite ledger path.
        #[arg(long, default_value = "data/ledger/backfill.sqlite")]
        ledger_path: PathBuf,
        /// Stable run id stored on claimed attempts.
        #[arg(long, default_value = "fleet-local")]
        run_id: String,
        /// Maximum claimable repos to process in this invocation.
        #[arg(long, default_value_t = 1, value_parser = parse_positive_u32)]
        claim_limit: u32,
        /// Maximum concurrent repo attempts.
        #[arg(long, default_value_t = 4, value_parser = parse_positive_usize)]
        concurrency: usize,
        /// Maximum concurrent parse/archive stages.
        #[arg(long, default_value_t = DEFAULT_PARSE_CONCURRENCY, value_parser = parse_positive_usize)]
        parse_concurrency: usize,
        /// Maximum bytes held by in-flight streamed `CAR` files.
        #[arg(long, default_value_t = DEFAULT_MAX_INFLIGHT_SPOOL_BYTES, value_parser = parse_positive_u64)]
        max_inflight_spool_bytes: u64,
        /// Restrict claims to one persisted DID shard bucket.
        #[arg(long, value_name = "BUCKET", value_parser = parse_shard_filter)]
        shard_bucket: Option<ShardFilter>,
        /// Directory for local `CAR` spooling.
        #[arg(long, default_value = "data/spool")]
        spool_dir: PathBuf,
        /// Loud single-repo byte cap for each spooled `CAR`.
        #[arg(long, default_value_t = 2_147_483_648)]
        max_bytes: u64,
        /// Directory for local archive artifacts.
        #[arg(long, default_value = "data/archive")]
        archive_dir: PathBuf,
        /// Worker threads used for CAR block CID verification.
        #[arg(long, default_value_t = default_cid_verification_threads(), value_parser = parse_positive_usize)]
        cid_verification_threads: usize,
        /// HTTP protocol behavior for repo fetches.
        #[arg(long, value_enum, default_value_t = HttpProtocol::Http1)]
        http_protocol: HttpProtocol,
    },
    /// Verify a committed archive manifest and load derived rows into `ClickHouse`.
    DeriveManifest {
        /// Committed JSONL manifest path.
        manifest_path: PathBuf,
        /// Archive root used to resolve manifest object paths.
        #[arg(long, default_value = "data/archive")]
        archive_root: PathBuf,
        /// `ClickHouse` HTTP endpoint.
        #[arg(long, default_value = "http://localhost:8123")]
        clickhouse_url: String,
        /// `ClickHouse` database.
        #[arg(long, default_value = "emojistats")]
        clickhouse_database: String,
        /// `ClickHouse` username.
        #[arg(long, default_value = "default")]
        clickhouse_user: String,
        /// `ClickHouse` password.
        #[arg(long, default_value = "")]
        clickhouse_password: String,
        /// Validate and format payloads without sending inserts.
        #[arg(long)]
        dry_run: bool,
        /// Optional JSONL ledger recording successful derive payload inserts.
        #[arg(long)]
        derive_ledger_path: Option<PathBuf>,
    },
    /// Print the v2 `ClickHouse` schema SQL.
    ClickhouseSchema {
        /// `ClickHouse` database.
        #[arg(long, default_value = "emojistats")]
        clickhouse_database: String,
    },
}

fn parse_positive_u32(value: &str) -> Result<u32, String> {
    let parsed = value
        .parse::<u32>()
        .map_err(|err| format!("expected a positive integer: {err}"))?;
    if parsed == 0 {
        return Err("expected a positive integer".to_owned());
    }
    Ok(parsed)
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|err| format!("expected a positive integer: {err}"))?;
    if parsed == 0 {
        return Err("expected a positive integer".to_owned());
    }
    Ok(parsed)
}

fn parse_positive_u64(value: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|err| format!("expected a positive integer: {err}"))?;
    if parsed == 0 {
        return Err("expected a positive integer".to_owned());
    }
    Ok(parsed)
}

fn parse_shard_filter(value: &str) -> Result<ShardFilter, String> {
    let bucket = value
        .parse::<u64>()
        .map_err(|err| format!("expected a shard bucket integer: {err}"))?;
    ShardFilter::new(bucket).map_err(|err| err.to_string())
}
