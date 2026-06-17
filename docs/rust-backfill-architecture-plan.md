# Rust backfill architecture plan

This plan addresses the remaining architecture debt from `docs/rust-backfill-review.md` after the round-1 correctness fixes.

## 1. Make archive commit backend real

Goal: Storage Box is a peer backend, not a local commit followed by a remote mirror.

Work:

- Define an `ArchiveBackend` trait with `commit_posts`, `commit_profile`, and `commit_auxiliary` verbs at the archive artifact level.
- Implement `LocalArchiveBackend` and `StorageBoxArchiveBackend`.
- Keep local temp files as staging inputs, but let the selected backend own final object, receipt, and manifest exposure.
- Add an optional `TeeArchiveBackend` for local plus remote canaries.
- Build the backend once per worker so SSH config/executor state is reused.

Exit criteria:

- `--archive-backend storage-box-ssh` can produce a remote archive that derives from remote manifest and remote receipts only.
- A failed remote commit leaves no selected-backend manifest entry.
- Local smoke still works with the local backend.

## 2. Centralize commit protocol invariants

Goal: local and remote commits share one state machine.

Work:

- Introduce a `CommitPlan` describing object path, receipt path, manifest path, hash, metadata, and auxiliary files.
- Move temp upload/write, no-clobber promote, final hash verification, receipt validation, and append-if-missing into one protocol driver.
- Parameterize filesystem operations behind a small transport trait.
- Make divergent existing receipts a single policy decision used by both backends.

Exit criteria:

- Local and Storage Box commit tests run through the same protocol tests.
- Existing-object retry behavior is byte-for-byte consistent across backends.

## 3. Split archive I/O by ownership

Goal: `archive/io.rs` stops being the schema, parquet, hash, receipt, and naming catch-all.

Work:

- Move Arrow/Parquet schema and batch encode/decode to `archive/parquet.rs`.
- Move row and projection hashing to `archive/hash.rs`.
- Move artifact names and path helpers to `archive/naming.rs`.
- Move receipt construction/metadata to `archive/receipt.rs`.
- Delete or test-gate `archive/full_write.rs` after tests use `StreamingArchiveSink`.

Exit criteria:

- There is one canonical archive row hasher.
- Streaming and test/full paths use the same receipt and naming helpers.
- No archive module exceeds a single responsibility.

## 4. Move blocking work off async workers

Goal: SQLite and SSH cannot starve fetch timers, pacers, or byte-budget waiters.

Work:

- Wrap ledger calls used by fleet, heartbeat, claim check, and host override lookup in `spawn_blocking`, or move them behind one ledger actor.
- Keep one fleet-owned ledger handle or actor instead of reopening SQLite for heartbeat and override calls.
- Move Storage Box SSH subprocess calls behind a bounded blocking pool.
- Add wait/latency metrics for ledger and remote commit operations.

Exit criteria:

- No synchronous SQLite or `std::process` waits run directly on async orchestration tasks.
- Fleet smoke reports ledger wait and remote commit wait timings.

## 5. Remove false scaffolding

Goal: exposed knobs either enforce behavior or disappear.

Work:

- Gate `storage-box-ssh` CLI and module wiring behind `experimental-storage-box`, or remove the cargo feature.
- Gate canary-only code behind `experimental-canary`, or remove that cargo feature.
- Wire `aggregate_rebuild_sql` into a real command before using it operationally.
- Delete the emoji projection artifact unless a consumer needs it for a measured workflow.
- Emit or delete Storage Box metrics that are currently only contract definitions.

Exit criteria:

- No cargo feature is dead.
- No hashed artifact is written without a consumer or explicit canary purpose.
- Operational commands cover schema creation, derive, canary, and aggregate rebuild.

## 6. Normalize attempt orchestration types

Goal: repo processing is not coupled to a bag of fleet runtime policy.

Work:

- Replace `AttemptRuntime` with an `AttemptResources` struct of optional resources.
- Split host policy, claim policy, and repo processing into separate modules.
- Collapse duplicate fetch/list/parse step structs into one `AttemptContext` plus stage-local inputs.
- Add constructors for smoke telemetry events.

Exit criteria:

- `fetch_attempt.rs` only coordinates stages.
- Host override, claim ownership, byte budget, and parse permits are owned by small policy modules.
