//! emojistats-backfill — v2 Rust backfill CLI.
//!
//! Vertical-slice milestone in progress: `fetch-one <did>` resolves a DID to its PDS, then
//! (incrementally) fetches the repo via the streaming `getRepo` seam, proves snapshot
//! completeness, archives posts, and derives emoji rows. See `docs/backfill-v2-design.md`
//! ("First implementation milestone").

use std::{
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

use clap::{Parser, Subcommand};
use emojistats_backfill::{
    archive::{
        ArchiveError, RepoReceiptInput, archive_rows_from_parsed_repo, build_repo_receipt,
        current_normalizer, hash_profile_record, write_archive_artifacts,
    },
    ledger::{
        AttemptId, AttemptOutcome, RepoLedgerEntry, RetryPolicy, SqliteLedger, claim_repo,
        complete_attempt,
    },
    parse::{ParseError, parse_repo_for_did},
    transport::{FetchConfig, FetchError, fetch_repo},
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
        /// Directory for local `CAR` spooling.
        #[arg(long, default_value = "data/spool")]
        spool_dir: PathBuf,
        /// Loud single-repo byte cap for each spooled `CAR`.
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
        Command::RunFleet {
            dids_file,
            ledger_path,
            run_id,
            claim_limit,
            spool_dir,
            max_bytes,
            archive_dir,
        } => {
            run_fleet(FleetConfig {
                dids_file,
                ledger_path,
                run_id,
                claim_limit,
                spool_dir,
                max_bytes,
                archive_dir,
            })
            .await
        }
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
    let now = SystemTime::now();
    let ledger = RepoLedgerEntry::pending(did_str);
    let claimed = claim_repo(&ledger, AttemptId::new("fetch-one-local", did_str, 1), now)
        .map_err(|err| anyhow::anyhow!("claim fetch-one ledger entry for {did_str}: {err}"))?;

    let result = fetch_one_attempt(did_str, spool_dir, max_bytes, archive_dir).await;
    let outcome = result.as_ref().map_or_else(
        |failure| failure.outcome.clone(),
        |_success| AttemptOutcome::Succeeded,
    );
    let completed = complete_attempt(&claimed, outcome, SystemTime::now(), RetryPolicy::default())
        .map_err(|err| anyhow::anyhow!("complete fetch-one ledger entry for {did_str}: {err}"))?;
    println!(
        "ledger status for {} after {} attempt(s): {:?}",
        completed.did, completed.attempts, completed.status
    );

    result.map_err(|failure| failure.error)
}

#[derive(Debug)]
struct FleetConfig {
    dids_file: PathBuf,
    ledger_path: PathBuf,
    run_id: String,
    claim_limit: u32,
    spool_dir: PathBuf,
    max_bytes: u64,
    archive_dir: PathBuf,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SeedSummary {
    inserted: u64,
    existing: u64,
    blank: u64,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct FleetSummary {
    seed: SeedSummary,
    claimed: u64,
    succeeded: u64,
    failed: u64,
}

async fn run_fleet(config: FleetConfig) -> anyhow::Result<()> {
    if let Some(parent) = config
        .ledger_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let ledger = SqliteLedger::open(&config.ledger_path)?;
    let mut summary = FleetSummary {
        seed: seed_ledger_from_file(&ledger, &config.dids_file)?,
        ..FleetSummary::default()
    };
    let claimable = ledger.claimable_entries(SystemTime::now(), config.claim_limit)?;

    for entry in claimable {
        let did = entry.did.clone();
        let attempt = AttemptId::new(&config.run_id, &did, next_attempt_sequence(&entry)?);
        let claimed = claim_repo(&entry, attempt, SystemTime::now())
            .map_err(|err| anyhow::anyhow!("claim ledger entry for {did}: {err}"))?;
        ledger.save_transitioned_entry(&claimed)?;
        increment(&mut summary.claimed, "claimed repo count")?;

        let result = fetch_one_attempt(
            &did,
            config.spool_dir.clone(),
            config.max_bytes,
            config.archive_dir.clone(),
        )
        .await;
        let outcome = result.as_ref().map_or_else(
            |failure| failure.outcome.clone(),
            |_success| AttemptOutcome::Succeeded,
        );
        let completed =
            complete_attempt(&claimed, outcome, SystemTime::now(), RetryPolicy::default())
                .map_err(|err| anyhow::anyhow!("complete ledger entry for {did}: {err}"))?;
        ledger.save_transitioned_entry(&completed)?;

        match result {
            Ok(()) => increment(&mut summary.succeeded, "succeeded repo count")?,
            Err(failure) => {
                increment(&mut summary.failed, "failed repo count")?;
                eprintln!("attempt failed for {did}: {}", failure.error);
            }
        }
        println!(
            "ledger status for {} after {} attempt(s): {:?}",
            completed.did, completed.attempts, completed.status
        );
    }

    println!(
        "fleet summary: seeded {}, existing {}, blank {}, claimed {}, succeeded {}, failed {}",
        summary.seed.inserted,
        summary.seed.existing,
        summary.seed.blank,
        summary.claimed,
        summary.succeeded,
        summary.failed
    );
    Ok(())
}

fn seed_ledger_from_file(ledger: &SqliteLedger, dids_file: &Path) -> anyhow::Result<SeedSummary> {
    let mut summary = SeedSummary::default();
    let contents = fs::read_to_string(dids_file)?;

    for line in contents.lines() {
        let did = line.trim();
        if did.is_empty() {
            increment(&mut summary.blank, "blank line count")?;
            continue;
        }
        let _parsed: Did = Did::new_owned(did).map_err(|err| {
            anyhow::anyhow!("invalid DID {did:?} in {}: {err}", dids_file.display())
        })?;

        if ledger.load_entry(did)?.is_some() {
            increment(&mut summary.existing, "existing seed count")?;
            continue;
        }

        ledger.upsert_entry(&RepoLedgerEntry::pending(did))?;
        increment(&mut summary.inserted, "inserted seed count")?;
    }

    Ok(summary)
}

fn next_attempt_sequence(entry: &RepoLedgerEntry) -> anyhow::Result<u64> {
    u64::from(entry.attempts)
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("attempt sequence overflow for {}", entry.did))
}

fn increment(value: &mut u64, context: &str) -> anyhow::Result<()> {
    *value = value
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("{context} overflow"))?;
    Ok(())
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

async fn fetch_one_attempt(
    did_str: &str,
    spool_dir: PathBuf,
    max_bytes: u64,
    archive_dir: PathBuf,
) -> Result<(), FetchOneFailure> {
    let did: Did = Did::new_owned(did_str)
        .map_err(|err| permanent_failure(format!("invalid DID {did_str:?}: {err}")))?;

    let resolver = PublicResolver::default();
    let pds = resolver
        .pds_for_did(&did)
        .await
        .map_err(|err| retryable_failure(format!("resolve PDS for {did_str}: {err}")))?;

    println!("{did_str} -> PDS {pds}");
    let http = reqwest::Client::new();
    let mut config = FetchConfig::new(spool_dir);
    config.max_bytes = max_bytes;

    let spooled = fetch_repo(&http, &pds, &did, &config)
        .await
        .map_err(|err| classify_fetch_error(did_str, &err))?;
    println!(
        "spooled {} bytes from HTTP {} to {}",
        spooled.bytes,
        spooled.http_status,
        spooled.car_path.display()
    );

    let parsed = parse_repo_for_did(&spooled.car_path, did_str)
        .map_err(|err| classify_parse_error(did_str, &err))?;
    let rows = archive_rows_from_parsed_repo(&parsed).map_err(|err| {
        classify_archive_error(&format!("build archive rows for {did_str}"), &err)
    })?;
    let profile_row_hash = hash_profile_record(parsed.profile.as_ref())
        .map_err(|err| classify_archive_error(&format!("hash profile row for {did_str}"), &err))?;
    let post_decode_error_count = parsed
        .record_decode_errors
        .iter()
        .filter(|error| error.collection == "app.bsky.feed.post")
        .count()
        .try_into()
        .map_err(|_err| {
            resource_failure(format!("post decode error count overflow for {did_str}"))
        })?;
    let receipt = build_repo_receipt(RepoReceiptInput {
        rows: &rows,
        reachable_records_count: parsed.rkey_digest.all_records_count,
        reachable_post_records_count: parsed.rkey_digest.post_records_count,
        post_decode_error_count,
        profile_row_hash,
        mst_root_cid: Some(parsed.commit.data.clone()),
        commit_cid: Some(parsed.commit.cid.clone()),
        normalizer: current_normalizer(),
    })
    .map_err(|err| classify_archive_error(&format!("build receipt for {did_str}"), &err))?;
    let artifacts = write_archive_artifacts(
        &archive_dir,
        did_str,
        &rows,
        parsed.profile.as_ref(),
        &receipt,
    )
    .map_err(|err| {
        classify_archive_error(&format!("write archive artifacts for {did_str}"), &err)
    })?;
    println!(
        "parsed {} records, {} posts, {} decode errors, {} emoji rows, receipt {}",
        parsed.rkey_digest.all_records_count,
        receipt.archived_post_rows_count,
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

#[derive(Debug)]
struct FetchOneFailure {
    outcome: AttemptOutcome,
    error: anyhow::Error,
}

fn classify_fetch_error(did: &str, error: &FetchError) -> FetchOneFailure {
    let message = format!("fetch getRepo for {did}: {error}");
    let outcome = match &error {
        FetchError::AccountState { state, .. } => AttemptOutcome::AccountState(*state),
        FetchError::HttpStatus {
            status, rate_limit, ..
        } if *status == 429 => rate_limit.retry_after.map_or_else(
            || AttemptOutcome::RetryableFailure {
                message: message.clone(),
            },
            |retry_after| AttemptOutcome::RateLimited { retry_after },
        ),
        FetchError::HttpStatus { status, .. } if *status >= 500 => {
            AttemptOutcome::RetryableFailure {
                message: message.clone(),
            }
        }
        FetchError::InactivityTimeout { .. }
        | FetchError::Transport { .. }
        | FetchError::Io { .. } => AttemptOutcome::RetryableFailure {
            message: message.clone(),
        },
        FetchError::MaxBytesExceeded { .. } | FetchError::ErrorBodyTooLarge { .. } => {
            AttemptOutcome::ResourceLimitExceeded {
                message: message.clone(),
            }
        }
        FetchError::HttpStatus { .. } => AttemptOutcome::PermanentFailure {
            message: message.clone(),
        },
    };
    FetchOneFailure {
        outcome,
        error: anyhow::anyhow!(message),
    }
}

fn classify_parse_error(did: &str, error: &ParseError) -> FetchOneFailure {
    let message = format!("parse CAR for {did}: {error}");
    let outcome = match error {
        ParseError::ResourceLimitExceeded { .. } | ParseError::ResourceCountOverflow { .. } => {
            AttemptOutcome::ResourceLimitExceeded {
                message: message.clone(),
            }
        }
        ParseError::Io { .. } | ParseError::Runtime(_) | ParseError::ThreadSpawn(_) => {
            AttemptOutcome::RetryableFailure {
                message: message.clone(),
            }
        }
        ParseError::Repo(_)
        | ParseError::InvalidRoots(_)
        | ParseError::CidMismatch { .. }
        | ParseError::UnsupportedCodec { .. }
        | ParseError::CommitNotFound { .. }
        | ParseError::RootCommitDecode { .. }
        | ParseError::CommitDidMismatch { .. }
        | ParseError::MissingBlock { .. }
        | ParseError::RecordDecode { .. }
        | ParseError::MstRootMismatch { .. }
        | ParseError::Unsupported { .. }
        | ParseError::NotYetImplemented { .. }
        | ParseError::RuntimeThreadTerminated
        | ParseError::MalformedVarint
        | ParseError::CarLengthOverflow { .. }
        | ParseError::MalformedCar(_)
        | ParseError::CidRead(_) => AttemptOutcome::PermanentFailure {
            message: message.clone(),
        },
    };
    FetchOneFailure {
        outcome,
        error: anyhow::anyhow!(message),
    }
}

fn classify_archive_error(context: &str, error: &ArchiveError) -> FetchOneFailure {
    let message = format!("{context}: {error}");
    let outcome = match error {
        ArchiveError::Io(_) | ArchiveError::Commit(_) => AttemptOutcome::RetryableFailure {
            message: message.clone(),
        },
        ArchiveError::CountOverflow { .. } => AttemptOutcome::ResourceLimitExceeded {
            message: message.clone(),
        },
        ArchiveError::Parquet(_)
        | ArchiveError::Arrow(_)
        | ArchiveError::Json(_)
        | ArchiveError::InvalidCompression(_)
        | ArchiveError::InvalidPath { .. }
        | ArchiveError::InvalidRecordJson => AttemptOutcome::PermanentFailure {
            message: message.clone(),
        },
    };
    FetchOneFailure {
        outcome,
        error: anyhow::anyhow!(message),
    }
}

fn retryable_failure(message: String) -> FetchOneFailure {
    FetchOneFailure {
        outcome: AttemptOutcome::RetryableFailure {
            message: message.clone(),
        },
        error: anyhow::anyhow!(message),
    }
}

fn permanent_failure(message: String) -> FetchOneFailure {
    FetchOneFailure {
        outcome: AttemptOutcome::PermanentFailure {
            message: message.clone(),
        },
        error: anyhow::anyhow!(message),
    }
}

fn resource_failure(message: String) -> FetchOneFailure {
    FetchOneFailure {
        outcome: AttemptOutcome::ResourceLimitExceeded {
            message: message.clone(),
        },
        error: anyhow::anyhow!(message),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use clap::Parser;
    use emojistats_backfill::ledger::{RepoLedgerEntry, RepoLedgerStatus, SqliteLedger};

    use super::{Cli, Command, SeedSummary, seed_ledger_from_file};

    #[test]
    fn parses_fetch_one_did() {
        let cli =
            Cli::try_parse_from(["emojistats-backfill", "fetch-one", "did:plc:abc123"]).unwrap();
        let Command::FetchOne {
            did,
            spool_dir,
            max_bytes,
            archive_dir,
        } = cli.command
        else {
            unreachable!("expected fetch-one command");
        };
        assert_eq!(did, "did:plc:abc123");
        assert_eq!(spool_dir, PathBuf::from("data/spool"));
        assert_eq!(max_bytes, 2_147_483_648);
        assert_eq!(archive_dir, PathBuf::from("data/archive"));
    }

    #[test]
    fn requires_a_subcommand() {
        assert!(Cli::try_parse_from(["emojistats-backfill"]).is_err());
    }

    #[test]
    fn parses_run_fleet_defaults() {
        let cli = Cli::try_parse_from(["emojistats-backfill", "run-fleet", "dids.txt"]).unwrap();
        let Command::RunFleet {
            dids_file,
            ledger_path,
            run_id,
            claim_limit,
            spool_dir,
            max_bytes,
            archive_dir,
        } = cli.command
        else {
            unreachable!("expected run-fleet command");
        };
        assert_eq!(dids_file, PathBuf::from("dids.txt"));
        assert_eq!(ledger_path, PathBuf::from("data/ledger/backfill.sqlite"));
        assert_eq!(run_id, "fleet-local");
        assert_eq!(claim_limit, 1);
        assert_eq!(spool_dir, PathBuf::from("data/spool"));
        assert_eq!(max_bytes, 2_147_483_648);
        assert_eq!(archive_dir, PathBuf::from("data/archive"));
    }

    #[test]
    fn run_fleet_rejects_zero_claim_limit() {
        assert!(
            Cli::try_parse_from([
                "emojistats-backfill",
                "run-fleet",
                "dids.txt",
                "--claim-limit",
                "0",
            ])
            .is_err()
        );
    }

    #[test]
    fn seed_ledger_from_file_inserts_only_missing_dids() {
        let store = SqliteLedger::open_in_memory().unwrap();
        let existing = RepoLedgerEntry {
            did: "did:plc:existing".to_owned(),
            status: RepoLedgerStatus::Succeeded,
            attempts: 1,
            next_attempt_after: None,
            last_attempt: None,
            last_error: None,
        };
        store.upsert_entry(&existing).unwrap();
        let dids_file = temp_file_path("seed-ledger");
        fs::write(
            &dids_file,
            "\ndid:plc:existing\ndid:plc:newrepo\ndid:plc:newrepo\n",
        )
        .unwrap();

        let summary = seed_ledger_from_file(&store, &dids_file).unwrap();

        assert_eq!(
            summary,
            SeedSummary {
                inserted: 1,
                existing: 2,
                blank: 1
            }
        );
        assert_eq!(
            store.load_entry("did:plc:existing").unwrap(),
            Some(existing)
        );
        assert_eq!(
            store.load_entry("did:plc:newrepo").unwrap().unwrap().status,
            RepoLedgerStatus::Pending
        );

        fs::remove_file(dids_file).unwrap();
    }

    fn temp_file_path(name: &str) -> PathBuf {
        let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        std::env::temp_dir().join(format!(
            "emojistats-backfill-{name}-{}-{}.txt",
            std::process::id(),
            since_epoch.as_nanos()
        ))
    }
}
