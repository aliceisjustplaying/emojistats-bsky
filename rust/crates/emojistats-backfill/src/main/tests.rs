#![allow(clippy::arithmetic_side_effects)]

use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::Parser;
use emojistats_backfill::{
    ledger::{
        AttemptId, AttemptOutcome, ForcedFetchMode, HostOverride, RepoLedgerEntry,
        RepoLedgerStatus, ShardFilter, SqliteLedger, claim_repo_with_lease, did_shard_bucket,
    },
    parse::default_cid_verification_threads,
    scheduler::ClaimScope,
    transport::{FetchError, RateLimitSnapshot},
};
use jacquard_common::deps::fluent_uri::Uri;

use super::{
    Cli, Command,
    cli::ArchiveBackend,
    fleet::{
        HostConcurrencyLimiter, SeedSummary, SharedBlockingLedger, claimable_entries_for_scope,
        recover_stale_claimed_entries, seed_ledger_from_file,
    },
    main::{
        archive_host::{fetch_mode_for_host, load_host_override, pds_host_key, prepare_fetch_host},
        attempt_resources::HostOverrideCache,
        fetch_attempt::should_fallback_get_repo_to_list_records,
    },
};

fn assert_float_eq(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() <= f64::EPSILON,
        "expected {expected}, got {actual}"
    );
}

#[test]
fn parses_fetch_one_did() {
    let cli = Cli::try_parse_from(["emojistats-backfill", "fetch-one", "did:plc:abc123"]).unwrap();
    let Command::FetchOne {
        did,
        spool_dir,
        max_bytes,
        archive_dir,
        cid_verification_threads,
        ..
    } = cli.command
    else {
        unreachable!("expected fetch-one command");
    };
    assert_eq!(did, "did:plc:abc123");
    assert_eq!(spool_dir, PathBuf::from("data/spool"));
    assert_eq!(max_bytes, 2_147_483_648);
    assert_eq!(archive_dir, PathBuf::from("data/archive"));
    assert_eq!(cid_verification_threads, default_cid_verification_threads());
}

#[tokio::test]
async fn host_concurrency_cap_serializes_same_host() {
    let limiter = HostConcurrencyLimiter::default();
    let first = limiter
        .acquire("pds.example.com", Some(1))
        .await
        .unwrap()
        .unwrap();
    let blocked = tokio::time::timeout(
        Duration::from_millis(10),
        limiter.acquire("pds.example.com", Some(1)),
    )
    .await;
    assert!(blocked.is_err());
    drop(blocked);
    drop(first);

    let second = tokio::time::timeout(
        Duration::from_secs(1),
        limiter.acquire("pds.example.com", Some(1)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    drop(second);
}

#[tokio::test]
async fn host_concurrency_cap_resizes_for_future_acquires() {
    let limiter = HostConcurrencyLimiter::default();
    let first = limiter
        .acquire("pds.example.com", Some(2))
        .await
        .unwrap()
        .unwrap();
    let second = limiter
        .acquire("pds.example.com", Some(2))
        .await
        .unwrap()
        .unwrap();

    let blocked = tokio::time::timeout(
        Duration::from_millis(10),
        limiter.acquire("pds.example.com", Some(1)),
    )
    .await;
    assert!(blocked.is_err());
    drop(blocked);

    let third = tokio::time::timeout(
        Duration::from_secs(1),
        limiter.acquire("pds.example.com", Some(3)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    drop(third);
    drop(second);
    drop(first);
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
        concurrency,
        parse_concurrency,
        max_inflight_spool_bytes,
        shard_bucket,
        spool_dir,
        max_bytes,
        archive_dir,
        cid_verification_threads,
        ..
    } = cli.command
    else {
        unreachable!("expected run-fleet command");
    };
    assert_eq!(dids_file, PathBuf::from("dids.txt"));
    assert_eq!(ledger_path, PathBuf::from("data/ledger/backfill.sqlite"));
    assert_eq!(run_id, "fleet-local");
    assert_eq!(claim_limit, 1);
    assert_eq!(concurrency, 4);
    assert_eq!(parse_concurrency, 1);
    assert_eq!(max_inflight_spool_bytes, 2_147_483_648);
    assert_eq!(shard_bucket, None);
    assert_eq!(spool_dir, PathBuf::from("data/spool"));
    assert_eq!(max_bytes, 2_147_483_648);
    assert_eq!(archive_dir, PathBuf::from("data/archive"));
    assert_eq!(cid_verification_threads, default_cid_verification_threads());
}

#[test]
fn parses_run_fleet_resource_options() {
    let cli = Cli::try_parse_from([
        "emojistats-backfill",
        "run-fleet",
        "dids.txt",
        "--parse-concurrency",
        "2",
        "--max-inflight-spool-bytes",
        "123456",
        "--cid-verification-threads",
        "7",
    ])
    .unwrap();
    let Command::RunFleet {
        parse_concurrency,
        max_inflight_spool_bytes,
        cid_verification_threads,
        ..
    } = cli.command
    else {
        unreachable!("expected run-fleet command");
    };

    assert_eq!(parse_concurrency, 2);
    assert_eq!(max_inflight_spool_bytes, 123_456);
    assert_eq!(cid_verification_threads, 7);
}

#[test]
fn parses_storage_box_archive_backend_options() {
    let cli = Cli::try_parse_from([
        "emojistats-backfill",
        "run-fleet",
        "dids.txt",
        "--archive-backend",
        "storage-box-ssh",
        "--storage-box-remote",
        "u123@example.invalid",
        "--storage-box-root",
        "/storage-box/emojistats",
        "--storage-box-ssh-program",
        "/usr/bin/ssh",
        "--storage-box-ssh-arg",
        "-p",
        "--storage-box-ssh-arg",
        "23",
        "--storage-box-command-timeout-secs",
        "42",
    ])
    .unwrap();
    let Command::RunFleet {
        archive_storage, ..
    } = cli.command
    else {
        unreachable!("expected run-fleet command");
    };

    assert_eq!(
        archive_storage.archive_backend,
        ArchiveBackend::StorageBoxSsh
    );
    assert_eq!(
        archive_storage.storage_box_remote.as_deref(),
        Some("u123@example.invalid")
    );
    assert_eq!(
        archive_storage.storage_box_root.as_deref(),
        Some("/storage-box/emojistats")
    );
    assert_eq!(
        archive_storage.storage_box_ssh_program,
        PathBuf::from("/usr/bin/ssh")
    );
    assert_eq!(archive_storage.storage_box_ssh_arg, vec!["-p", "23"]);
    assert_eq!(archive_storage.storage_box_command_timeout_secs, 42);
}

#[test]
fn parses_storage_box_rclone_archive_backend_options() {
    let cli = Cli::try_parse_from([
        "emojistats-backfill",
        "run-fleet",
        "dids.txt",
        "--archive-backend",
        "storage-box-rclone",
        "--storage-box-root",
        "/emojistats-archive/canary",
        "--storage-box-rclone-remote",
        "storagebox",
        "--storage-box-rclone-config",
        "/run/secret/rclone.conf",
        "--storage-box-rclone-program",
        "/usr/bin/rclone",
        "--storage-box-command-timeout-secs",
        "42",
    ])
    .unwrap();
    let Command::RunFleet {
        archive_storage, ..
    } = cli.command
    else {
        unreachable!("expected run-fleet command");
    };

    assert_eq!(
        archive_storage.archive_backend,
        ArchiveBackend::StorageBoxRclone
    );
    assert_eq!(
        archive_storage.storage_box_root.as_deref(),
        Some("/emojistats-archive/canary")
    );
    assert_eq!(archive_storage.storage_box_rclone_remote, "storagebox");
    assert_eq!(
        archive_storage.storage_box_rclone_config.as_deref(),
        Some(Path::new("/run/secret/rclone.conf"))
    );
    assert_eq!(
        archive_storage.storage_box_rclone_program,
        PathBuf::from("/usr/bin/rclone")
    );
    assert_eq!(archive_storage.storage_box_command_timeout_secs, 42);
}

#[test]
fn parses_run_fleet_shard_bucket() {
    let cli = Cli::try_parse_from([
        "emojistats-backfill",
        "run-fleet",
        "dids.txt",
        "--shard-bucket",
        "7",
    ])
    .unwrap();
    let Command::RunFleet { shard_bucket, .. } = cli.command else {
        unreachable!("expected run-fleet command");
    };

    assert_eq!(shard_bucket, Some(ShardFilter::new(7).unwrap()));
}

#[test]
fn parses_canary_threshold_options() {
    let cli = Cli::try_parse_from([
        "emojistats-backfill",
        "run-fleet",
        "dids.txt",
        "--canary-evidence",
        "canary.jsonl",
        "--min-storage-box-headroom-ratio",
        "0.25",
        "--max-clickhouse-serving-box-ratio",
        "0.70",
        "--min-derive-to-crawl-ratio",
        "1.5",
        "--min-sustained-repos-per-second",
        "123.4",
        "--min-mushroom-budget-utilization-ratio",
        "0.80",
        "--max-mushroom-429-ratio",
        "0.02",
        "--max-aggregate-rebuild-hours",
        "1.25",
    ])
    .unwrap();
    let Command::RunFleet {
        canary_thresholds, ..
    } = cli.command
    else {
        unreachable!("expected run-fleet command");
    };

    assert_float_eq(canary_thresholds.min_storage_box_headroom_ratio, 0.25);
    assert_float_eq(canary_thresholds.max_clickhouse_serving_box_ratio, 0.70);
    assert_float_eq(canary_thresholds.min_derive_to_crawl_ratio, 1.5);
    assert_float_eq(canary_thresholds.min_sustained_repos_per_second, 123.4);
    assert_float_eq(
        canary_thresholds.min_mushroom_budget_utilization_ratio,
        0.80,
    );
    assert_float_eq(canary_thresholds.max_mushroom_429_ratio, 0.02);
    assert_float_eq(canary_thresholds.max_aggregate_rebuild_hours, 1.25);
}

#[test]
fn parses_derive_manifest_defaults() {
    let cli =
        Cli::try_parse_from(["emojistats-backfill", "derive-manifest", "manifest.jsonl"]).unwrap();
    let Command::DeriveManifest {
        manifest_path,
        archive_root,
        clickhouse_url,
        clickhouse_database,
        clickhouse_user,
        clickhouse_password,
        dry_run,
        ..
    } = cli.command
    else {
        unreachable!("expected derive-manifest command");
    };

    assert_eq!(manifest_path, PathBuf::from("manifest.jsonl"));
    assert_eq!(archive_root, PathBuf::from("data/archive"));
    assert_eq!(clickhouse_url, "http://localhost:8123");
    assert_eq!(clickhouse_database, "emojistats");
    assert_eq!(clickhouse_user, "default");
    assert_eq!(clickhouse_password, "");
    assert!(!dry_run);
}

#[test]
fn parses_derive_manifest_clickhouse_options() {
    let cli = Cli::try_parse_from([
        "emojistats-backfill",
        "derive-manifest",
        "manifest.jsonl",
        "--archive-root",
        "archive",
        "--clickhouse-url",
        "http://127.0.0.1:8123",
        "--clickhouse-database",
        "analytics",
        "--clickhouse-user",
        "writer",
        "--clickhouse-password",
        "secret",
        "--dry-run",
    ])
    .unwrap();
    let Command::DeriveManifest {
        archive_root,
        clickhouse_url,
        clickhouse_database,
        clickhouse_user,
        clickhouse_password,
        dry_run,
        ..
    } = cli.command
    else {
        unreachable!("expected derive-manifest command");
    };

    assert_eq!(archive_root, PathBuf::from("archive"));
    assert_eq!(clickhouse_url, "http://127.0.0.1:8123");
    assert_eq!(clickhouse_database, "analytics");
    assert_eq!(clickhouse_user, "writer");
    assert_eq!(clickhouse_password, "secret");
    assert!(dry_run);
}

#[test]
fn parses_derive_manifest_claim_and_throttle_options() {
    let cli = Cli::try_parse_from([
        "emojistats-backfill",
        "derive-manifest",
        "manifest.jsonl",
        "--claim-ledger-path",
        "claims.jsonl",
        "--claim-worker-id",
        "worker-a",
        "--claim-max-entries",
        "7",
        "--claim-max-rows",
        "12345",
        "--claim-stale-seconds",
        "99",
        "--clickhouse-insert-slots-dir",
        "slots",
        "--clickhouse-insert-slots",
        "3",
        "--clickhouse-insert-slot-timeout-secs",
        "42",
    ])
    .unwrap();
    let Command::DeriveManifest {
        claim_ledger_path,
        claim_worker_id,
        claim_max_entries,
        claim_max_rows,
        claim_stale_seconds,
        clickhouse_insert_slots_dir,
        clickhouse_insert_slots,
        clickhouse_insert_slot_timeout_secs,
        ..
    } = cli.command
    else {
        unreachable!("expected derive-manifest command");
    };

    assert_eq!(claim_ledger_path, Some(PathBuf::from("claims.jsonl")));
    assert_eq!(claim_worker_id.as_deref(), Some("worker-a"));
    assert_eq!(claim_max_entries, 7);
    assert_eq!(claim_max_rows, 12_345);
    assert_eq!(claim_stale_seconds, 99);
    assert_eq!(clickhouse_insert_slots_dir, Some(PathBuf::from("slots")));
    assert_eq!(clickhouse_insert_slots, 3);
    assert_eq!(clickhouse_insert_slot_timeout_secs, 42);
}

#[test]
fn rejects_zero_plc_page_size() {
    let error = Cli::try_parse_from(["emojistats-backfill", "plc-mirror", "--page-size", "0"])
        .expect_err("zero page size should be rejected");

    assert!(error.to_string().contains("expected a positive integer"));
}

#[test]
fn parses_clickhouse_schema_defaults() {
    let cli = Cli::try_parse_from(["emojistats-backfill", "clickhouse-schema"]).unwrap();
    let Command::ClickhouseSchema {
        clickhouse_database,
    } = cli.command
    else {
        unreachable!("expected clickhouse-schema command");
    };

    assert_eq!(clickhouse_database, "emojistats");
}

#[test]
fn parses_clickhouse_schema_database() {
    let cli = Cli::try_parse_from([
        "emojistats-backfill",
        "clickhouse-schema",
        "--clickhouse-database",
        "analytics",
    ])
    .unwrap();
    let Command::ClickhouseSchema {
        clickhouse_database,
    } = cli.command
    else {
        unreachable!("expected clickhouse-schema command");
    };

    assert_eq!(clickhouse_database, "analytics");
}

#[test]
fn parses_clickhouse_rebuild_aggregates_defaults() {
    let cli = Cli::try_parse_from(["emojistats-backfill", "clickhouse-rebuild-aggregates"])
        .expect("parse command");
    let Command::ClickhouseRebuildAggregates {
        clickhouse_url,
        clickhouse_database,
        clickhouse_user,
        clickhouse_password,
        dry_run,
    } = cli.command
    else {
        unreachable!("expected clickhouse-rebuild-aggregates command");
    };

    assert_eq!(clickhouse_url, "http://localhost:8123");
    assert_eq!(clickhouse_database, "emojistats");
    assert_eq!(clickhouse_user, "default");
    assert_eq!(clickhouse_password, "");
    assert!(!dry_run);
}

#[test]
fn parses_clickhouse_rebuild_aggregates_options() {
    let cli = Cli::try_parse_from([
        "emojistats-backfill",
        "clickhouse-rebuild-aggregates",
        "--clickhouse-url",
        "http://127.0.0.1:8123",
        "--clickhouse-database",
        "analytics",
        "--clickhouse-user",
        "writer",
        "--clickhouse-password",
        "secret",
        "--dry-run",
    ])
    .expect("parse command");
    let Command::ClickhouseRebuildAggregates {
        clickhouse_url,
        clickhouse_database,
        clickhouse_user,
        clickhouse_password,
        dry_run,
    } = cli.command
    else {
        unreachable!("expected clickhouse-rebuild-aggregates command");
    };

    assert_eq!(clickhouse_url, "http://127.0.0.1:8123");
    assert_eq!(clickhouse_database, "analytics");
    assert_eq!(clickhouse_user, "writer");
    assert_eq!(clickhouse_password, "secret");
    assert!(dry_run);
}

#[test]
fn run_fleet_rejects_out_of_range_shard_bucket() {
    assert!(
        Cli::try_parse_from([
            "emojistats-backfill",
            "run-fleet",
            "dids.txt",
            "--shard-bucket",
            "8",
        ])
        .is_err()
    );
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
fn run_fleet_rejects_zero_concurrency() {
    assert!(
        Cli::try_parse_from([
            "emojistats-backfill",
            "run-fleet",
            "dids.txt",
            "--concurrency",
            "0",
        ])
        .is_err()
    );
}

#[test]
fn run_fleet_rejects_zero_parse_concurrency() {
    assert!(
        Cli::try_parse_from([
            "emojistats-backfill",
            "run-fleet",
            "dids.txt",
            "--parse-concurrency",
            "0",
        ])
        .is_err()
    );
}

#[test]
fn run_fleet_rejects_zero_inflight_spool_bytes() {
    assert!(
        Cli::try_parse_from([
            "emojistats-backfill",
            "run-fleet",
            "dids.txt",
            "--max-inflight-spool-bytes",
            "0",
        ])
        .is_err()
    );
}

#[test]
fn claimable_entries_for_scope_uses_shard_filter() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    let target_did = "did:plc:target";
    let target_bucket = did_shard_bucket(target_did);
    let mut other_did = "did:plc:other0".to_owned();
    let mut suffix = 1_u32;
    while did_shard_bucket(&other_did) == target_bucket {
        other_did = format!("did:plc:other{suffix}");
        suffix = suffix.checked_add(1).unwrap();
    }
    let target = RepoLedgerEntry::pending(target_did);
    let other = RepoLedgerEntry::pending(&other_did);
    store.upsert_entry(&other).unwrap();
    store.upsert_entry(&target).unwrap();
    let scope = ClaimScope {
        shard_filter: Some(ShardFilter::new(target_bucket).unwrap()),
    };

    let claimable = claimable_entries_for_scope(&store, now, 10, &scope).unwrap();

    assert_eq!(claimable, vec![target]);
}

#[test]
fn persisted_host_override_loads_by_resolved_pds_host() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let db_path = temp_file_path("host-overrides").with_extension("sqlite");
    drop(store);
    let store = SqliteLedger::open(&db_path).unwrap();
    let override_record = HostOverride {
        host: "pds.example.com".to_owned(),
        disabled: false,
        concurrency_cap: None,
        min_interval: None,
        revive_after: None,
        force_mode: Some(ForcedFetchMode::ListRecords),
        force_mode_revive_after: None,
        never_diff: false,
    };
    store.upsert_host_override(&override_record).unwrap();
    drop(store);
    let pds = Uri::parse("https://pds.example.com").unwrap().to_owned();
    let host = pds_host_key(&pds);

    let loaded = load_host_override(Some(&db_path), None, &host, SystemTime::now()).unwrap();

    assert_eq!(loaded, Some(override_record));
    fs::remove_file(db_path).unwrap();
}

#[test]
fn host_override_cache_reuses_loaded_rows_and_clears_expired_disable() {
    let db_path = temp_file_path("host-overrides-cache").with_extension("sqlite");
    let store = SqliteLedger::open(&db_path).unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    store
        .upsert_host_override(&HostOverride {
            host: "pds.example.com".to_owned(),
            disabled: true,
            concurrency_cap: Some(1),
            min_interval: None,
            revive_after: Some(now - Duration::from_secs(1)),
            force_mode: Some(ForcedFetchMode::ListRecords),
            force_mode_revive_after: None,
            never_diff: false,
        })
        .unwrap();
    drop(store);
    let cache = HostOverrideCache::default();

    let loaded = load_host_override(Some(&db_path), Some(cache.clone()), "pds.example.com", now)
        .unwrap()
        .unwrap();
    assert!(!loaded.disabled);
    assert_eq!(loaded.revive_after, None);

    let store = SqliteLedger::open(&db_path).unwrap();
    store
        .upsert_host_override(&HostOverride {
            host: "pds.example.com".to_owned(),
            disabled: false,
            concurrency_cap: Some(2),
            min_interval: None,
            revive_after: None,
            force_mode: Some(ForcedFetchMode::GetRepo),
            force_mode_revive_after: None,
            never_diff: false,
        })
        .unwrap();
    drop(store);

    let cached = load_host_override(Some(&db_path), Some(cache), "pds.example.com", now)
        .unwrap()
        .unwrap();
    assert_eq!(cached.concurrency_cap, Some(1));
    assert_eq!(cached.force_mode, Some(ForcedFetchMode::ListRecords));
    fs::remove_file(db_path).unwrap();
}

#[test]
fn host_override_cache_clears_expired_force_mode() {
    let db_path = temp_file_path("host-overrides-force-mode-expiry").with_extension("sqlite");
    let store = SqliteLedger::open(&db_path).unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    store
        .upsert_host_override(&HostOverride {
            host: "pds.example.com".to_owned(),
            disabled: false,
            concurrency_cap: Some(4),
            min_interval: Some(Duration::from_millis(500)),
            revive_after: None,
            force_mode: Some(ForcedFetchMode::ListRecords),
            force_mode_revive_after: Some(now - Duration::from_secs(1)),
            never_diff: true,
        })
        .unwrap();
    drop(store);

    let loaded = load_host_override(Some(&db_path), None, "pds.example.com", now)
        .unwrap()
        .unwrap();

    assert_eq!(loaded.force_mode, None);
    assert_eq!(loaded.force_mode_revive_after, None);
    assert_eq!(loaded.concurrency_cap, Some(4));
    assert_eq!(loaded.min_interval, Some(Duration::from_millis(500)));
    assert!(loaded.never_diff);
    fs::remove_file(db_path).unwrap();
}

#[tokio::test]
async fn forced_list_records_host_preparation_is_allowed() {
    let db_path = temp_file_path("host-overrides-list-records").with_extension("sqlite");
    let store = SqliteLedger::open(&db_path).unwrap();
    store
        .upsert_host_override(&HostOverride {
            host: "pds.example.com".to_owned(),
            disabled: false,
            concurrency_cap: None,
            min_interval: None,
            revive_after: None,
            force_mode: Some(ForcedFetchMode::ListRecords),
            force_mode_revive_after: None,
            never_diff: false,
        })
        .unwrap();
    let shared_ledger = SharedBlockingLedger::new(store);
    let pds = Uri::parse("https://pds.example.com").unwrap().to_owned();

    let prepared = prepare_fetch_host(
        "did:plc:target",
        &pds,
        &ClaimScope::default(),
        Some(shared_ledger),
        None,
    )
    .await
    .unwrap();

    assert_eq!(prepared.fetch_mode, ForcedFetchMode::ListRecords);
    fs::remove_file(db_path).unwrap();
}

#[test]
fn host_override_force_mode_and_disable_are_applied() {
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    let list_records = HostOverride {
        host: "pds.example.com".to_owned(),
        disabled: false,
        concurrency_cap: None,
        min_interval: None,
        revive_after: None,
        force_mode: Some(ForcedFetchMode::ListRecords),
        force_mode_revive_after: None,
        never_diff: false,
    };
    let disabled = HostOverride {
        host: "pds.example.com".to_owned(),
        disabled: true,
        concurrency_cap: None,
        min_interval: None,
        revive_after: Some(now + Duration::from_secs(30)),
        force_mode: Some(ForcedFetchMode::GetRepo),
        force_mode_revive_after: None,
        never_diff: false,
    };

    assert_eq!(
        fetch_mode_for_host("pds.example.com", Some(&list_records), now).unwrap(),
        ForcedFetchMode::ListRecords
    );
    let failure = fetch_mode_for_host("pds.example.com", Some(&disabled), now).unwrap_err();
    assert_eq!(
        failure.outcome,
        AttemptOutcome::OperatorDeferred {
            retry_after: Some(Duration::from_secs(30)),
            message: format!(
                "host pds.example.com disabled by override until {:?}",
                now + Duration::from_secs(30)
            )
        }
    );

    let parked = HostOverride {
        host: "pds.example.com".to_owned(),
        disabled: true,
        concurrency_cap: None,
        min_interval: None,
        revive_after: None,
        force_mode: Some(ForcedFetchMode::GetRepo),
        force_mode_revive_after: None,
        never_diff: false,
    };
    let failure = fetch_mode_for_host("pds.example.com", Some(&parked), now).unwrap_err();
    assert_eq!(
        failure.outcome,
        AttemptOutcome::OperatorDeferred {
            retry_after: None,
            message: "host pds.example.com disabled by override".to_owned()
        }
    );
}

#[test]
fn get_repo_method_wall_uses_list_records_fallback() {
    assert!(!should_fallback_get_repo_to_list_records(
        &FetchError::HttpStatus {
            status: 429,
            error_code: None,
            message: Some("temporarily disabled for getRepo".into()),
            rate_limit: Box::new(RateLimitSnapshot::default()),
        }
    ));
    assert!(should_fallback_get_repo_to_list_records(
        &FetchError::HttpStatus {
            status: 501,
            error_code: Some("MethodNotImplemented".into()),
            message: None,
            rate_limit: Box::new(RateLimitSnapshot::default()),
        }
    ));
    assert!(should_fallback_get_repo_to_list_records(
        &FetchError::HttpStatus {
            status: 400,
            error_code: Some("MethodNotImplemented".into()),
            message: None,
            rate_limit: Box::new(RateLimitSnapshot::default()),
        }
    ));
    assert!(!should_fallback_get_repo_to_list_records(
        &FetchError::HttpStatus {
            status: 404,
            error_code: Some("RepoNotFound".into()),
            message: None,
            rate_limit: Box::new(RateLimitSnapshot::default()),
        }
    ));
    assert!(!should_fallback_get_repo_to_list_records(
        &FetchError::HttpStatus {
            status: 429,
            error_code: None,
            message: Some("rate limited".into()),
            rate_limit: Box::new(RateLimitSnapshot::default()),
        }
    ));
    assert!(!should_fallback_get_repo_to_list_records(
        &FetchError::HttpStatus {
            status: 429,
            error_code: None,
            message: Some("temporarily disabled".into()),
            rate_limit: Box::new(RateLimitSnapshot::default()),
        }
    ));
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
        worker_id: None,
        claimed_at: None,
        lease_until: None,
    };
    store.upsert_entry(&existing).unwrap();
    let dids_file = temp_file_path("seed-ledger");
    fs::write(
        &dids_file,
        "\ndid:plc:existing\ndid:plc:newrepo\nat://did:plc:newrepo\n",
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

#[test]
fn stale_claimed_entries_from_seed_file_requeue_on_startup() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let now = UNIX_EPOCH + std::time::Duration::from_secs(1_000);
    let pending = RepoLedgerEntry::pending("did:plc:stale");
    let claimed = claim_repo_with_lease(
        &pending,
        AttemptId::new("previous-run", "did:plc:stale", 1),
        now - Duration::from_secs(120),
        "previous-worker",
        Duration::from_secs(60),
    )
    .unwrap();
    store.upsert_entry(&claimed).unwrap();
    let dids_file = temp_file_path("stale-claimed");
    fs::write(&dids_file, "did:plc:stale\n").unwrap();

    let recovered = recover_stale_claimed_entries(&store, &dids_file, now).unwrap();
    let entry = store.load_entry("did:plc:stale").unwrap().unwrap();

    assert_eq!(recovered, 1);
    assert_eq!(entry.status, RepoLedgerStatus::RetryableFailure);
    assert!(entry.can_claim_at(now));
    assert_eq!(
        entry.last_error,
        Some("expired claimed lease at fleet startup".to_owned())
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
