use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::params;

use crate::{
    ledger::{
        AttemptId, AttemptOutcome, DID_SHARD_BUCKET_MODULUS, ForcedFetchMode, HostOverride,
        LedgerError, LedgerStoreError, RepoLedgerEntry, RepoLedgerStatus, RetryPolicy, ShardFilter,
        SqliteLedger, claim_repo, complete_attempt, did_shard_bucket,
    },
    transport::AccountState,
};

#[test]
fn pending_repo_claims_with_attempt_identity() {
    let now = UNIX_EPOCH;
    let entry = RepoLedgerEntry::pending("did:plc:abc");
    let attempt = AttemptId::new("run-1", "did:plc:abc", 7);

    let claimed = claim_repo(&entry, attempt.clone(), now).unwrap();

    assert_eq!(claimed.status, RepoLedgerStatus::Claimed);
    assert_eq!(claimed.attempts, 1);
    assert_eq!(claimed.last_attempt, Some(attempt));
}

#[test]
fn account_state_is_terminal() {
    let now = UNIX_EPOCH;
    let entry = claim_repo(
        &RepoLedgerEntry::pending("did:plc:abc"),
        AttemptId::new("run-1", "did:plc:abc", 1),
        now,
    )
    .unwrap();

    let completed = complete_attempt(
        &entry,
        AttemptOutcome::AccountState(AccountState::RepoTakendown),
        now,
        RetryPolicy::default(),
    )
    .unwrap();

    assert_eq!(
        completed.status,
        RepoLedgerStatus::TerminalAccount(AccountState::RepoTakendown)
    );
    assert!(!completed.can_claim_at(now + Duration::from_secs(10_000)));
}

#[test]
fn retryable_failure_backs_off_until_due() {
    let now = UNIX_EPOCH;
    let claimed = claim_repo(
        &RepoLedgerEntry::pending("did:plc:abc"),
        AttemptId::new("run-1", "did:plc:abc", 1),
        now,
    )
    .unwrap();

    let failed = complete_attempt(
        &claimed,
        AttemptOutcome::RetryableFailure {
            message: "socket reset".to_owned(),
        },
        now,
        RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_secs(60),
            max_delay: Duration::from_secs(300),
        },
    )
    .unwrap();

    assert_eq!(failed.status, RepoLedgerStatus::RetryableFailure);
    assert!(!failed.can_claim_at(now + Duration::from_secs(59)));
    assert!(failed.next_attempt_after.is_some_and(|due| due > now));
    assert!(failed.can_claim_at(failed.next_attempt_after.unwrap()));
}

#[test]
fn retryable_failure_becomes_permanent_at_attempt_cap() {
    let now = UNIX_EPOCH;
    let first = claim_repo(
        &RepoLedgerEntry::pending("did:plc:abc"),
        AttemptId::new("run-1", "did:plc:abc", 1),
        now,
    )
    .unwrap();
    let failed = complete_attempt(
        &first,
        AttemptOutcome::RetryableFailure {
            message: "timeout".to_owned(),
        },
        now,
        RetryPolicy {
            max_attempts: 1,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(1),
        },
    )
    .unwrap();

    assert_eq!(failed.status, RepoLedgerStatus::PermanentFailure);
    assert!(!failed.can_claim_at(now + Duration::from_secs(1)));
}

#[test]
fn unready_throttled_repo_is_not_claimable() {
    let now = UNIX_EPOCH;
    let claimed = claim_repo(
        &RepoLedgerEntry::pending("did:plc:abc"),
        AttemptId::new("run-1", "did:plc:abc", 1),
        now,
    )
    .unwrap();
    let throttled = complete_attempt(
        &claimed,
        AttemptOutcome::RateLimited {
            retry_after: Duration::from_secs(30),
        },
        now,
        RetryPolicy::default(),
    )
    .unwrap();

    let error = claim_repo(
        &throttled,
        AttemptId::new("run-1", "did:plc:abc", 2),
        now + Duration::from_secs(29),
    )
    .unwrap_err();

    assert!(matches!(error, LedgerError::NotClaimable { .. }));
}

#[test]
#[allow(clippy::duration_suboptimal_units)]
fn operator_deferred_rolls_back_claim_attempt() {
    let now = UNIX_EPOCH;
    let claimed = claim_repo(
        &RepoLedgerEntry::pending("did:plc:abc"),
        AttemptId::new("run-1", "did:plc:abc", 1),
        now,
    )
    .unwrap();

    let deferred = complete_attempt(
        &claimed,
        AttemptOutcome::OperatorDeferred {
            retry_after: None,
            message: "host parked".to_owned(),
        },
        now,
        RetryPolicy::default(),
    )
    .unwrap();

    assert_eq!(deferred.status, RepoLedgerStatus::OperatorDeferred);
    assert_eq!(deferred.attempts, 0);
    assert_eq!(deferred.next_attempt_after, None);
    assert!(!deferred.can_claim_at(now + Duration::from_secs(86_400)));
}

#[test]
fn did_shard_bucket_is_deterministic_and_bounded() {
    let did = "did:plc:abc";

    let bucket = did_shard_bucket(did);

    assert_eq!(bucket, did_shard_bucket(did));
    assert!(bucket < DID_SHARD_BUCKET_MODULUS);
    assert!(ShardFilter::new(bucket).unwrap().contains_did(did));
    assert!(matches!(
        ShardFilter::new(DID_SHARD_BUCKET_MODULUS),
        Err(LedgerError::InvalidShardBucket { .. })
    ));
}

#[test]
fn sqlite_upsert_loads_full_entry() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let entry = RepoLedgerEntry {
        did: "did:plc:abc".to_owned(),
        status: RepoLedgerStatus::TerminalAccount(AccountState::RepoSuspended),
        attempts: 2,
        next_attempt_after: Some(UNIX_EPOCH + Duration::from_secs(1_234)),
        last_attempt: Some(AttemptId::new("run-1", "did:plc:abc", 9)),
        last_error: Some("RepoSuspended".to_owned()),
        worker_id: None,
        claimed_at: None,
        lease_until: None,
    };

    store.upsert_entry(&entry).unwrap();

    assert_eq!(store.load_entry("did:plc:abc").unwrap(), Some(entry));
    assert_eq!(
        store
            .connection
            .query_row(
                "SELECT shard_bucket FROM repo_ledger WHERE did = ?1",
                params!["did:plc:abc"],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        i64::try_from(did_shard_bucket("did:plc:abc")).unwrap()
    );
}

#[test]
fn sqlite_insert_pending_entries_ignore_existing_preserves_existing_rows() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let existing = RepoLedgerEntry {
        did: "did:plc:existing".to_owned(),
        status: RepoLedgerStatus::Succeeded,
        attempts: 1,
        next_attempt_after: None,
        last_attempt: Some(AttemptId::new("run-1", "did:plc:existing", 1)),
        last_error: None,
        worker_id: None,
        claimed_at: None,
        lease_until: None,
    };
    store.upsert_entry(&existing).unwrap();

    let summary = store
        .insert_pending_entries_ignore_existing([
            "did:plc:existing",
            "did:plc:newrepo",
            "did:plc:newrepo",
        ])
        .unwrap();

    assert_eq!(summary.inserted, 1);
    assert_eq!(summary.existing, 2);
    assert_eq!(
        store.load_entry("did:plc:existing").unwrap(),
        Some(existing)
    );
    assert_eq!(
        store.load_entry("did:plc:newrepo").unwrap().unwrap().status,
        RepoLedgerStatus::Pending
    );
}

#[test]
fn sqlite_claimable_entries_are_due_at_now() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    let due = RepoLedgerEntry {
        did: "did:plc:due".to_owned(),
        status: RepoLedgerStatus::Throttled,
        attempts: 1,
        next_attempt_after: Some(now - Duration::from_secs(1)),
        last_attempt: Some(AttemptId::new("run-1", "did:plc:due", 1)),
        last_error: Some("rate_limited".to_owned()),
        worker_id: None,
        claimed_at: None,
        lease_until: None,
    };
    let pending = RepoLedgerEntry::pending("did:plc:pending");
    let future_retry = RepoLedgerEntry {
        did: "did:plc:future".to_owned(),
        status: RepoLedgerStatus::RetryableFailure,
        attempts: 1,
        next_attempt_after: Some(now + Duration::from_secs(1)),
        last_attempt: Some(AttemptId::new("run-1", "did:plc:future", 1)),
        last_error: Some("timeout".to_owned()),
        worker_id: None,
        claimed_at: None,
        lease_until: None,
    };
    let succeeded = RepoLedgerEntry {
        did: "did:plc:done".to_owned(),
        status: RepoLedgerStatus::Succeeded,
        attempts: 1,
        next_attempt_after: None,
        last_attempt: Some(AttemptId::new("run-1", "did:plc:done", 1)),
        last_error: None,
        worker_id: None,
        claimed_at: None,
        lease_until: None,
    };

    for entry in [&due, &pending, &future_retry, &succeeded] {
        store.upsert_entry(entry).unwrap();
    }

    let claimable = store.claimable_entries(now, 10).unwrap();
    let dids = claimable
        .iter()
        .map(|entry| entry.did.as_str())
        .collect::<Vec<_>>();

    assert_eq!(dids, vec!["did:plc:pending", "did:plc:due"]);
}

#[test]
fn sqlite_claimable_entries_can_filter_by_shard() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    let target = RepoLedgerEntry::pending("did:plc:target");
    let target_bucket = did_shard_bucket(&target.did);
    let other = (0_u64..1_000)
        .map(|index| RepoLedgerEntry::pending(format!("did:plc:other{index}")))
        .find(|entry| did_shard_bucket(&entry.did) != target_bucket)
        .unwrap();

    store.upsert_entry(&target).unwrap();
    store.upsert_entry(&other).unwrap();

    let claimable = store
        .claimable_entries_for_shard(now, 10, ShardFilter::new(target_bucket).unwrap())
        .unwrap();

    assert_eq!(claimable, vec![target]);
}

#[test]
fn sqlite_atomic_claim_allows_exactly_one_owner_across_connections() {
    let db_path = temp_ledger_path("claim-race");
    let store = SqliteLedger::open(&db_path).unwrap();
    store
        .upsert_entry(&RepoLedgerEntry::pending("did:plc:race"))
        .unwrap();
    drop(store);
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    let open_barrier = Arc::new(Barrier::new(3));
    let claim_barrier = Arc::new(Barrier::new(3));
    let make_handle = |worker_id: &'static str| {
        let db_path = db_path.clone();
        let open_barrier = Arc::clone(&open_barrier);
        let claim_barrier = Arc::clone(&claim_barrier);
        thread::spawn(move || {
            let store = SqliteLedger::open(&db_path).unwrap();
            open_barrier.wait();
            claim_barrier.wait();
            store
                .try_claim_next(now, "run-1", worker_id, Duration::from_secs(60), None)
                .unwrap()
        })
    };
    let handles = [make_handle("worker-a"), make_handle("worker-b")];
    open_barrier.wait();
    claim_barrier.wait();

    let claims = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    let claimed = claims.iter().flatten().collect::<Vec<_>>();

    assert_eq!(claimed.len(), 1);
    let claimed = claimed.into_iter().next().unwrap();
    assert_eq!(claimed.status, RepoLedgerStatus::Claimed);
    assert_eq!(claimed.attempts, 1);
    assert!(matches!(
        claimed.worker_id.as_deref(),
        Some("worker-a" | "worker-b")
    ));
    fs::remove_file(db_path).unwrap();
}

#[test]
fn sqlite_active_claim_is_not_recovered_before_lease_expiry() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    store
        .upsert_entry(&RepoLedgerEntry::pending("did:plc:active"))
        .unwrap();
    let claimed = store
        .try_claim_next(now, "run-1", "worker-a", Duration::from_secs(60), None)
        .unwrap()
        .unwrap();

    let recovered = store
        .recover_expired_claim(
            "did:plc:active",
            now + Duration::from_secs(59),
            "expired claim",
        )
        .unwrap();

    assert_eq!(recovered, None);
    assert_eq!(store.load_entry("did:plc:active").unwrap(), Some(claimed));
}

#[test]
fn sqlite_recovers_expired_claims_in_batch() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    for did in ["did:plc:stale1", "did:plc:stale2"] {
        store.upsert_entry(&RepoLedgerEntry::pending(did)).unwrap();
        store
            .try_claim_next(
                now - Duration::from_secs(120),
                "run-1",
                did,
                Duration::from_secs(60),
                None,
            )
            .unwrap();
    }

    let recovered = store
        .recover_expired_claims(now, 1, None, "expired claim")
        .unwrap();
    let retryable = ["did:plc:stale1", "did:plc:stale2"]
        .into_iter()
        .filter(|did| {
            store
                .load_entry(did)
                .unwrap()
                .as_ref()
                .is_some_and(|entry| entry.status == RepoLedgerStatus::RetryableFailure)
        })
        .count();

    assert_eq!(recovered, 1);
    assert_eq!(retryable, 1);
}

#[test]
fn sqlite_claim_allocates_manifest_sequence_per_run_and_shard() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    let target = RepoLedgerEntry::pending("did:plc:sequence-target");
    let target_bucket = did_shard_bucket(&target.did);
    let sibling = (0_u64..1_000)
        .map(|index| RepoLedgerEntry::pending(format!("did:plc:sequence{index}")))
        .find(|entry| did_shard_bucket(&entry.did) == target_bucket)
        .unwrap();
    store.upsert_entry(&target).unwrap();
    store.upsert_entry(&sibling).unwrap();
    let shard = Some(ShardFilter::new(target_bucket).unwrap());

    let first = store
        .try_claim_next(now, "run-1", "worker-a", Duration::from_secs(60), shard)
        .unwrap()
        .unwrap();
    let second = store
        .try_claim_next(now, "run-1", "worker-a", Duration::from_secs(60), shard)
        .unwrap()
        .unwrap();
    let other_run = RepoLedgerEntry::pending("did:plc:sequence-other-run");
    store.upsert_entry(&other_run).unwrap();
    let other = store
        .try_claim_next(
            now,
            "run-2",
            "worker-a",
            Duration::from_secs(60),
            Some(ShardFilter::new(did_shard_bucket(&other_run.did)).unwrap()),
        )
        .unwrap()
        .unwrap();

    assert_eq!(
        first.last_attempt.as_ref().map(|attempt| attempt.sequence),
        Some(1)
    );
    assert_eq!(
        second.last_attempt.as_ref().map(|attempt| attempt.sequence),
        Some(2)
    );
    assert_eq!(
        other.last_attempt.as_ref().map(|attempt| attempt.sequence),
        Some(1)
    );
}

#[test]
fn sqlite_manifest_sequence_starts_after_existing_run_shard_attempts() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    let existing_did = "did:plc:existing-sequence";
    let target_bucket = did_shard_bucket(existing_did);
    store
        .upsert_entry(&RepoLedgerEntry {
            did: existing_did.to_owned(),
            status: RepoLedgerStatus::Succeeded,
            attempts: 1,
            next_attempt_after: None,
            last_attempt: Some(AttemptId::new("run-1", existing_did, 9)),
            last_error: None,
            worker_id: None,
            claimed_at: None,
            lease_until: None,
        })
        .unwrap();
    let target = (0_u64..1_000)
        .map(|index| RepoLedgerEntry::pending(format!("did:plc:next-sequence{index}")))
        .find(|entry| did_shard_bucket(&entry.did) == target_bucket)
        .unwrap();
    store.upsert_entry(&target).unwrap();

    let claimed = store
        .try_claim_next(
            now,
            "run-1",
            "worker-a",
            Duration::from_secs(60),
            Some(ShardFilter::new(target_bucket).unwrap()),
        )
        .unwrap()
        .unwrap();

    assert_eq!(
        claimed
            .last_attempt
            .as_ref()
            .map(|attempt| attempt.sequence),
        Some(10)
    );
}

#[test]
fn sqlite_owned_claim_lease_extension_requires_current_owner() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    store
        .upsert_entry(&RepoLedgerEntry::pending("did:plc:heartbeat"))
        .unwrap();
    let claimed = store
        .try_claim_next(now, "run-1", "worker-a", Duration::from_secs(60), None)
        .unwrap()
        .unwrap();

    let extended = store
        .extend_owned_claim_lease(
            &claimed,
            now + Duration::from_secs(30),
            Duration::from_secs(120),
        )
        .unwrap()
        .unwrap();
    let mut wrong_owner = claimed;
    wrong_owner.worker_id = Some("worker-b".to_owned());
    let rejected = store
        .extend_owned_claim_lease(
            &wrong_owner,
            now + Duration::from_secs(40),
            Duration::from_secs(120),
        )
        .unwrap();

    assert_eq!(extended.lease_until, Some(now + Duration::from_secs(150)));
    assert_eq!(rejected, None);
    assert_eq!(
        store
            .load_entry("did:plc:heartbeat")
            .unwrap()
            .unwrap()
            .lease_until,
        Some(now + Duration::from_secs(150))
    );
}

#[test]
fn sqlite_completion_requires_current_owner() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    store
        .upsert_entry(&RepoLedgerEntry::pending("did:plc:owned"))
        .unwrap();
    let claimed = store
        .try_claim_next(now, "run-1", "worker-a", Duration::from_secs(60), None)
        .unwrap()
        .unwrap();
    let mut wrong_owner = claimed.clone();
    wrong_owner.worker_id = Some("worker-b".to_owned());

    let completed = store
        .complete_owned_claim(
            &wrong_owner,
            AttemptOutcome::Succeeded,
            now + Duration::from_secs(1),
            RetryPolicy::default(),
        )
        .unwrap();

    assert_eq!(completed, None);
    assert_eq!(store.load_entry("did:plc:owned").unwrap(), Some(claimed));
}

#[test]
fn sqlite_migrates_missing_shard_bucket_column() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "
                CREATE TABLE repo_ledger (
                    did TEXT PRIMARY KEY NOT NULL,
                    status TEXT NOT NULL,
                    terminal_account_state TEXT,
                    attempts INTEGER NOT NULL CHECK (attempts >= 0),
                    next_attempt_after_ms INTEGER,
                    last_attempt_run_id TEXT,
                    last_attempt_did TEXT,
                    last_attempt_sequence INTEGER,
                    last_error TEXT
                );
                INSERT INTO repo_ledger (did, status, attempts)
                VALUES ('did:plc:legacy', 'pending', 0);
                ",
        )
        .unwrap();

    let store = SqliteLedger::from_connection(connection).unwrap();

    assert_eq!(
        store
            .connection
            .query_row(
                "SELECT shard_bucket FROM repo_ledger WHERE did = ?1",
                params!["did:plc:legacy"],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        i64::try_from(did_shard_bucket("did:plc:legacy")).unwrap()
    );
}

#[test]
fn sqlite_schema_migration_sets_user_version() {
    let store = SqliteLedger::open_in_memory().unwrap();

    assert_eq!(
        store
            .connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn sqlite_host_overrides_round_trip() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let override_record = HostOverride {
        host: "pds.example.com".to_owned(),
        disabled: true,
        concurrency_cap: Some(3),
        min_interval: None,
        revive_after: Some(UNIX_EPOCH + Duration::from_secs(60)),
        force_mode: Some(ForcedFetchMode::ListRecords),
        never_diff: false,
    };

    store.upsert_host_override(&override_record).unwrap();

    assert_eq!(
        store.load_host_override("pds.example.com").unwrap(),
        Some(override_record)
    );
}

#[test]
fn sqlite_host_override_force_mode_update_preserves_operator_fields() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let original = HostOverride {
        host: "pds.example.com".to_owned(),
        disabled: true,
        concurrency_cap: Some(9),
        min_interval: Some(Duration::from_millis(250)),
        revive_after: Some(UNIX_EPOCH + Duration::from_secs(60)),
        force_mode: Some(ForcedFetchMode::GetRepo),
        never_diff: true,
    };
    store.upsert_host_override(&original).unwrap();

    store
        .upsert_host_override_force_mode(
            "pds.example.com",
            Some(ForcedFetchMode::ListRecords),
            Some(Duration::from_secs(1)),
        )
        .unwrap();

    assert_eq!(
        store.load_host_override("pds.example.com").unwrap(),
        Some(HostOverride {
            force_mode: Some(ForcedFetchMode::ListRecords),
            ..original
        })
    );
}

#[test]
fn sqlite_host_overrides_reject_blank_hosts_and_zero_caps() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let blank_host = HostOverride {
        host: " ".to_owned(),
        disabled: true,
        concurrency_cap: None,
        min_interval: None,
        revive_after: None,
        force_mode: None,
        never_diff: false,
    };
    let zero_cap = HostOverride {
        host: "pds.example.com".to_owned(),
        disabled: false,
        concurrency_cap: Some(0),
        min_interval: None,
        revive_after: None,
        force_mode: Some(ForcedFetchMode::GetRepo),
        never_diff: false,
    };

    assert!(matches!(
        store.upsert_host_override(&blank_host),
        Err(LedgerStoreError::InvalidHostOverride { .. })
    ));
    assert!(matches!(
        store.upsert_host_override(&zero_cap),
        Err(LedgerStoreError::InvalidHostOverride { .. })
    ));
}

#[test]
fn sqlite_saves_transitioned_entries() {
    let store = SqliteLedger::open_in_memory().unwrap();
    let now = UNIX_EPOCH;
    let pending = RepoLedgerEntry::pending("did:plc:abc");
    store.upsert_entry(&pending).unwrap();

    let claimed = claim_repo(&pending, AttemptId::new("run-1", "did:plc:abc", 1), now).unwrap();
    store.save_transitioned_entry(&claimed).unwrap();

    let completed = complete_attempt(
        &claimed,
        AttemptOutcome::RetryableFailure {
            message: "socket reset".to_owned(),
        },
        now,
        RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_secs(60),
            max_delay: Duration::from_secs(300),
        },
    )
    .unwrap();
    store.save_transitioned_entry(&completed).unwrap();

    assert_eq!(store.load_entry("did:plc:abc").unwrap(), Some(completed));
}

fn temp_ledger_path(name: &str) -> PathBuf {
    let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    std::env::temp_dir().join(format!(
        "emojistats-backfill-ledger-{name}-{}-{}.sqlite",
        std::process::id(),
        since_epoch.as_nanos()
    ))
}
