# Rust backfill rewrite — architecture review

Scope: `rust/` workspace on `v2-rust-backfill` (~31K LoC, 2 crates). Lens: architecture-primary,
correctness-secondary. Produced by 7 parallel subsystem reviews. File:line refs are to
`rust/crates/emojistats-backfill/src/` unless noted.

Bottom line: this is a **well-built tool with real discipline** (typed errors, `spawn_blocking`
hygiene, trait-injected transports, golden tests, checked arithmetic). Its problems are not sloppiness
— they're **over-modularization at the crate surface, under-consolidation at the naming level, a few
abstractions that don't actually deliver the guarantee their names claim, and lifecycle/idempotency
gaps that only bite on crash/retry.** The right direction is consolidation, not more abstraction.

---

## CRITICAL

**C1 [CORRECTNESS] rclone manifest append is a lock-free read-modify-write → silent record loss**
`storage_box/rclone.rs:244-259`. SSH wraps append in `flock` (`ssh.rs:104-119`); rclone does
read-all → mutate → `copyto`-overwrite with no remote lock. Two concurrent appenders both read the old
file and the second clobbers the first; the post-append `contains_manifest_record` check still passes
because each writer sees its own record. Bounded today only because manifests are single-entry (see H5).
Fix: reject `AppendJsonl` for rclone, or never let rclone publish the manifest.

---

## HIGH — architecture

**H1 [ARCH] The lib is `pub`-everything; it's not a library, it's the binary's drawer**
`lib.rs:3-18`. Every module but `post_decode` is `pub`; ~15 error enums + 20 config structs + dozens of
fns are `pub` with exactly one consumer — the sibling binary. The `pub` surface buys zero encapsulation
and makes every internal refactor look like an API change. This is *why* the git history is full of
"split module / tighten architecture" commits. Fix: `pub(crate)` by default, `pub` only what `tests/`
needs. (`emoji-normalizer` is the only genuine library and is correctly isolated.)

**H2 [ARCH] `main/` orchestration is wedged binary-side via `#[path]`; `fleet` reaches back into it**
`main.rs:46-47` mounts the fetch→parse→archive pipeline (`archive_host`, `fetch_attempt`, `repo_fetch`,
…) binary-side, but `fleet.rs` (lib-side) depends on it. Load-bearing logic split across the lib/binary
line for no reason — and it forces the `#[cfg(test)] use main::{...}` re-export hack at `main.rs:62-66`
so tests can reach it. Fix: move `main/` into the lib as a `pipeline`/`worker` module; shrink `main.rs`
to parse-config-dispatch.

**H3 [ARCH] No `ArchiveCommitBackend` trait — backend selection is a hand-copied `match`**
`archive/commit_backend.rs:39-70,114-156`. The *transport* seam (`StorageBoxCommands`, ssh vs rclone)
is clean, but Local-vs-StorageBox is two near-identical `match self.storage_config` arms that have
already drifted by copy. A 4th backend = editing two matches in lockstep. Fix: one
`ArchiveCommitBackend` trait with a shared `commit_prepared(...)` method.

**H4 [ARCH] The "manifest" is a per-file single-entry marker, not a manifest**
`archive/write.rs:289`, `archive/naming.rs:7-15`. `manifest_path = "{stem}.manifest.jsonl"` and `stem`
embeds `post_rows_hash`, so every object gets its own one-line manifest. The whole append-locking apparatus
(SSH `flock`, rclone RMW, C1) guards a file that by construction has one entry, while derive consumers must
glob millions of files to reconstruct a run. Fix: either a real per-shard append log (which *justifies*
the locking) or a plain marker write — not the worst of both.

**H5 [ARCH] Fleet's "two-stage" fetch/parse backpressure isn't two-staged**
`fleet.rs:100-101,169`, `parse_archive_attempt.rs:39-45`. Each attempt future does fetch → acquire parse
permit → parse *inline* on the same task, so a fetched repo holds its byte-budget reservation AND a
`concurrency` slot while blocked on `parse_permits`. With `concurrency > parse_concurrency`, fetched repos
pile up holding budget and the loop can't claim more work though the network is idle. Fix: a real
`mpsc`-decoupled producer/consumer; the byte budget then smooths instead of deadlocks.

**H6 [ARCH] "Streaming" derive fully materializes every payload body in RAM before inserting**
`derive_manifest_cmd.rs:171-188`. `streaming.rs` chunks at 10k rows / 8 MiB, but
`build_verified_input_payloads_canonical_streaming` pushes every `ClickHouseInsertPayload` (each owning a
full NDJSON `body: String`) into one `Vec`, *then* inserts. Peak RAM scales with whole-file size; the
chunking buys nothing. Fix: pass a sink/closure that inserts+ledgers each chunk, or a bounded channel.

**H7 [ARCH] "Snapshot completeness" is CID-chain consistency from an *unverified, unsigned* root**
`parse.rs:150-180,515`. `CompletenessClass` has one variant, assigned unconditionally;
`repo_commit_signature_verified`/`identity_verified` are hardcoded `false`. The proof shows the snapshot is
internally consistent, not complete or authentic — a server can return any self-consistent subtree (repo
missing half its posts under a forged commit) and it passes. Relatedly `mst_root_cid_verified` only checks
the root block is *present*, not that the walked MST re-encodes to `commit.data`. Fix: verify the commit
signature against the DID signing key, or rename the class/fields to stop claiming more than the code proves.

**H8 [ARCH] Emoji TS-parity is a per-glyph golden, not a segmentation oracle → silent drift**
`emoji-normalizer/tests/parity.rs:36-71`, `lib.rs:68-80`. The TS oracle finds emoji via `emoji-regex`;
Rust segments via UAX#29 grapheme clusters — *different boundary algorithms*. Every golden row is a single
isolated sequence, so the test only proves agreement on pre-isolated glyphs, never on emoji embedded in
mixed prose (the actual input). This is the one that affects every stored row and the derive dedupe token.
Fix: add golden rows embedding sequences in surrounding text/adjacent runs, generated through the same
`match(emojiRegex)` oracle.

**H9 [ARCH] Three copies of the chunk-stream loop; `list_records` bypasses the transport seam entirely**
`transport.rs:232-304` and `:363-430` duplicate the timeout/progress/cap loop; `list_records.rs:592-690`
is a third copy that uses raw `reqwest`/`bytes_stream()` instead of the `HttpClient`/`ByteStream`
abstraction — so the fallback can't share the byte budget, rate-limit wiring, or any transport fix. Fix:
one loop generic over a sink + cap-error fn; route list_records through the same primitive.

**H10 [ARCH] Canary evidence has no authenticity/freshness binding — a hand-written file passes the gate**
`canary_cmd.rs:114-157`, `canary.rs:345`. The gate that guards paid fleet fan-out reads an arbitrary file
with no run-id/timestamp/signature; `[{"kind":"gate",...,"status":"pass"}]` returns `Ok`. Worse, four
integrity gates (`whale_completes_cleanly`, `storage_box_manifest_detects_partial_upload`, …) pass their
`status` through verbatim with no measurement backing (`canary_cmd.rs:309-318`). The gate *math* is correct
and well-tested; the *trust model* is the hole. Fix: bind evidence to run-id + monotonic timestamp, reject
stale, derive integrity gates from structured observations.

**H11 [ARCH] Every ledger op opens a fresh SQLite connection and re-runs all migrations**
`fleet/ledger_async.rs:18,38,59,75`, `archive_host.rs:137`, `store.rs:267-272`. Claim, complete, heartbeat,
stale-sweep, and per-repo commit-check each `SqliteLedger::open` → reconfigure WAL → run migration planner,
all contending on the WAL write lock with a 30s busy timeout. Fix: one long-lived ledger actor task owning a
single connection, reached via an `mpsc` of commands; gives shutdown one place to release claims too.

**H12 [ARCH] The derive triad is three anagram modules for one stage**
`derive.rs` (DTOs), `manifest_derive.rs` (926-line loader), `derive_manifest_cmd.rs` (the command). Names
that are anagrams of each other for one pipeline stage = navigation tax + size-driven (not
responsibility-driven) split. Fix: one `derive/` dir: `mod.rs` (DTOs+tokens), `loader.rs`, `command.rs`.

---

## HIGH — correctness

**H13 [CORRECTNESS] No Ctrl-C / signal handling anywhere → claims leak, spool files accumulate**
`fleet.rs:106` (main loop); no `tokio::signal` in the crate. On SIGINT, in-flight claims stay `claimed`
until the 30-min lease expires, heartbeat tasks die, and `SpooledRepo::drop` never runs (process torn down
before unwind) → multi-GiB CARs leaked per in-flight repo. For a tool where "crawler hours cost real money,"
a draining shutdown is table stakes. Fix: top-level `select!` on `ctrl_c()` that stops claiming, drains
`active` with a deadline, then releases still-claimed entries.

**H14 [CORRECTNESS] A panic in any attempt aborts the whole orchestrator (attempts polled inline, not spawned)**
`fleet.rs:169,189`. `run_fleet_attempt` futures are pushed into a `FuturesUnordered` polled on the
orchestrator's own task; a panic in the async path unwinds `fleet::run`, killing all siblings and skipping
every pending `complete_owned_claim`. (Blocking parse is insulated via `spawn_blocking`; the async
orchestration is not.) Fix: `tokio::spawn`/`JoinSet` per attempt so one poisoned attempt fails in isolation.

**H15 [CORRECTNESS] Derive ledger and ClickHouse can silently diverge; exactly-once rests on a bounded CH window**
`derive_manifest_cmd.rs:238-250`, `ledger.rs:58-85`. Insert succeeds, *then* the ledger line is written —
crash between leaves rows in CH, absent from ledger → re-send on resume. Re-send is only safe via
`insert_deduplication_token`, bounded by `non_replicated_deduplication_window=10000` + time/part window; once
the part merges out of the window, the retry double-counts into `v2_total_post_counters_r3`. The ledger is
advisory, not authoritative. Fix: make ordering explicit and either treat ledger as source of truth (+ rely
on ReplacingMergeTree `FINAL`) or gate resends on a CH existence probe, not the dedup window.

**H16 [CORRECTNESS] `FetchError::Io` is blanket-retryable, including permanent local failures**
`failure.rs:96`. `FetchError::Io` covers `create_dir_all`, `NamedTempFile::new_in`, `persist_noclobber`
(fails on existing target — a logic conflict), `sync_parent_dir`. ENOSPC or a persist conflict is retried
forever, wedging the worker. `commit::Error::Io`/`storage_box::Error::LocalIo` already route these through
`is_operator_io_error` → `OperatorDeferred`; the fetch path should too.

**H17 [CORRECTNESS] Commits are retry-convergent, not crash-atomic (object promoted before manifest)**
`commit.rs:257-279` (local), `storage_box.rs:519-544` (remote). Both promote object → receipt → manifest as
separate steps; a crash between leaves a final object no manifest references. Local self-heals via
`write_manifest_if_missing` on retry; the *remote* manifest is the sole source of truth with weaker
reconciliation, so durability depends on the ledger guaranteeing a re-attempt. Fix: document the contract
explicitly and confirm the ledger never marks done before `finish()` fully returns.

---

## MEDIUM

**M1 [ARCH] Census schema lives outside the migration system and shares the ledger DB file**
`census.rs:472-508`. `plc_identities`/`pds_census`/`plc_meta` are raw `CREATE IF NOT EXISTS` into the same
file as the migration-versioned `repo_ledger`, with DEFERRED transactions vs the ledger's IMMEDIATE. Two
schema-governance regimes + two transaction behaviors in one file. Fix: separate DB file, or fold census DDL
into the migration vector.

**M2 [CORRECTNESS] Parallel PLC mirror advances the cursor to an *estimated* `head_upper` regardless of what was persisted**
`census.rs:415-420,681-712`. After parallel workers finish, cursor is set to `head_upper` and
`caught_up=true` unconditionally; a worker that stops early (empty/short page mid-range) leaves a permanent
data gap the cursor has skipped past. The serial path is safe. Fix: cursor = min of actually-persisted
per-range high-water marks. (Related: `write_worker_pages` threads a meaningless cross-range
`previous_cursor`, `census.rs:723-724`.)

**M3 [ARCH] Aggregate rebuild is TRUNCATE-then-INSERT: no atomicity, no memory caps, no streaming**
`schema.rs:84-96,262-333`, `main.rs:480`. One giant `INSERT … SELECT … FROM v2_post_serving_r3 FINAL GROUP
BY …` with no `max_bytes_before_external_group_by` / `max_memory_usage`. The recent "ClickHouse aggregate
rebuild canary fix" commit only **renamed a column** — the OOM exposure the canary flagged is *not* fixed.
TRUNCATE commits immediately, so an OOM/crash mid-INSERT leaves the serving table empty for users. Fix:
build into a shadow table, `EXCHANGE TABLES` atomically, attach explicit memory/external-aggregation settings.

**M4 [ARCH] Storage-Box CLI arg block is copy-pasted verbatim across `FetchOne` and `RunFleet`**
`cli/mod.rs:92-121` and `:176-205`; destructured identically at `main.rs:163-172,213-222`. Default drift
silently changes one command. You already use `#[command(flatten)]` for `CanaryThresholdArgs` — do the same
with a `StorageBoxArgs` struct, which also deletes the `ArchiveStorageArgs` plumbing layer.

**M5 [ARCH] `AttemptResources` Local/Fleet enum + Option fan-out is boilerplate that leaks the split everywhere**
`attempt_resources.rs:46-163`, consumers in `fetch_attempt.rs:65-101`. Eight `match`-and-return-`Option`
accessors (all `None` for Local), then ~10 `Option<&_>` re-threaded through every `*Step`. Fix: give Local
the same handles with no-op/unbounded defaults (no-op pacer, unbounded semaphore, zero byte-budget already
treated as unlimited); the `Option` fan-out disappears and the fleet path becomes testable via the shared entry.

**M6 [CORRECTNESS] Error/"absent" detection by stderr substring-matching**
`storage_box/rclone.rs:296-298` (`is_not_found_message` matches "not found"/"no such file"); transport
`is_permanent_transport_error` (`transport.rs:476-497`) and `failure.rs` also classify by lowercased
`to_string()`. Brittle across version/locale bumps: a real failure phrased "config not found" maps to
"absent" and can mask a missing upload; a reworded DNS/TLS error flips permanent↔retryable. Fix: rclone exit
codes / `lsjson`; downcast `reqwest::Error::is_connect/is_timeout` etc.; keep substrings as last resort.

**M7 [CORRECTNESS] Heartbeat is fire-and-forget; in-process stale-recovery can reclaim a self-owned active claim**
`fleet.rs:108-135,239,266-300`, `archive_host.rs:135-161`. The 60s recovery tick recovers any expired-lease
`claimed` row in scope — including one *this* process is still parsing if its heartbeat stalled past the
lease (blocking-pool starvation, which H5/H11 invite). The pre-commit `ArchiveClaimCheck` mostly catches it,
but fetch/parse work + byte budget is wasted and there's a TOCTOU window before `finish()`. Fix: exclude
self-`worker_id` rows from the in-process sweep; tie heartbeat lifetime to the attempt (JoinSet/drop-guard).

**M8 [CORRECTNESS] Ledger checkpoint degrades to a poison record on hashing failure instead of erroring**
`ledger.rs:110-123`. `checkpoint()` does `.unwrap_or_else(|_| { row_count: u64::MAX, payload_hash: "" })`.
The poisoned `(MAX,"")` key defeats both gating (`is_completed` never matches → re-insert every run) and
uniqueness (two poisoned payloads alias). Fix: propagate the error (`Result`), don't fabricate a record.
Related: `is_completed` keys on a checkpoint that omits `did`/`dataset`, so two entries sharing
run_id/shard/file_sequence/receipt_hash can alias (`ledger.rs:50-56`, `derive.rs:82-91`).

**M9 [CORRECTNESS] `v2_total_post_counters_r3` retry double-counts on pre-merge reads**
`schema.rs:166-192`. `ReplacingMergeTree(inserted_at)` ordered by identity collapses duplicate counter rows
*on merge*, but any read without `FINAL` before merge sums to 2×. This table is the audit ground-truth for
totals. Fix: confirm all readers (incl. out-of-scope serving queries) use `FINAL`/`argMax`.

**M10 [ARCH] SSH and rclone duplicate ~150 lines of process-spawn/timeout/pipe plumbing**
`storage_box/ssh.rs:237-418` vs `rclone.rs:301-443` (`run_command`, `wait_with_timeout`, `read_pipe`,
`PipeOutput`, `CommandStatus`, the `COMMAND_*` constants). This *is* the justified shared module (command
*construction* differs, the *runner* doesn't). Extract a `process_exec` helper; keep the two trait impls.

**M11 [CORRECTNESS] rclone remote SHA-256 always re-downloads the whole object to hash it**
`storage_box/rclone.rs:146-158` (`hashsum SHA-256 --download`), run on every commit verification + final-state
check. Roughly doubles egress at whale scale. Hetzner SFTP can't hash server-side, so if full-hash
verification is required the cost is intrinsic — gate it behind a flag with size+prefix as the default tier,
and document the double-transfer.

**M12 [CORRECTNESS] `lang_total.posts` mislabels its semantic and drops emoji-less languages**
`schema.rs:299-315`. After `ARRAY JOIN langs`, `posts = countIf(emoji_occurrences > 0)` — i.e. "posts in
this lang *with* an emoji," not post volume; a purely-non-emoji language vanishes. If serving reads it as
post volume it undercounts. Fix: pick one definition (`count()` for volume, keep emoji in `occurrences`).

**M13 [CORRECTNESS] `max_bytes` enforced mid-stream (good), but the progress watchdog checks before counting the just-arrived chunk**
`transport.rs:263` runs `enforce_progress` at loop top before `progress_window_bytes += chunk` (`:292`); a
body delivering exactly `min_progress_bytes` at the interval boundary can spuriously trip `ProgressTimeout`.
Fix: account the chunk before the check. (Cap enforcement itself is correct and well-tested.)

**M14 [CORRECTNESS] 429 without `Retry-After` carries no backoff in the outcome**
`failure.rs:80-85` maps it to bare `RetryableFailure` (no delay), relying on the pacer side-channel
(`fetch_attempt.rs:240-250`) rather than the outcome. Fix: map 429 →
`RateLimited { retry_after: cooldown_delay(now).unwrap_or(default) }` so backoff is intrinsic. Same pattern
in `classify_list_records_error` (`failure.rs:130-148`).

**M15 [ARCH] Census `open_census_connection` opens the DB twice per call**
`census.rs:472-478`: `drop(SqliteLedger::open(path)?)` (runs migrations) just to ensure schema, then reopens
a bare `Connection`. Doubles open cost on the per-worker writer. Fix: one function that opens once, migrates,
ensures census schema.

**M16 [CORRECTNESS] SSH `upload_command` is the one command that doesn't directly `validate_remote_path`**
`storage_box/ssh.rs:45-53,455-468`. `stat`/`sha256`/`remove`/`rename` validate the path explicitly; the
`cat > $path` upload validates only transitively via `remote_parent`. Safe today, emergent not enforced. Fix:
explicit `validate_remote_path(remote_path)?` at the top of `upload_command`. (Manifest-append also trusts
the serializer for "no interior newline" rather than asserting it — `storage_box.rs:590`.)

---

## MEDIUM/LOW — architecture hygiene

**M17 [ARCH] `census.rs` is a 1294-line three-command grab-bag** (`mirror`/`plan`/`pds`) — the only big
module never split, unlike `archive/`, `parse/`, `storage_box/`. Split into `census/{plc_mirror,plc_plan,pds}.rs`.

**M18 [ARCH] `main.rs` is a 566-line god-file** mixing 9-command dispatch, storage-config assembly, canary
gate, fetch-one orchestration, and `u64` helpers (`increment`/`add_count`/`count_len`) imported everywhere via
`super::`. Thin dispatcher + `commands/<name>.rs` + a `util`/`count` module.

**M19 [ARCH] Four `debug_`-prefixed `pub fn`s in `manifest_derive` are test-only API** with zero non-test
callers (`manifest_derive.rs:242,327,341,401`). A `pub` API named `debug_*` is a contradiction —
`#[cfg(test)] pub(crate)`.

**M20 [ARCH] `census`/`metrics` leak `anyhow` out of library APIs** (`census.rs:262/340/428`,
`metrics.rs:359/431`) while the rest of the codebase has 15 typed `thiserror` enums. Either give them error
enums or document the deliberate "fatal-to-command" anyhow choice.

**M21 [ARCH] `ArchiveError::CountOverflow` is overloaded as a catch-all** for missing-parquet-writer,
non-UTF-8 filename, and index-miss (`archive/write.rs:246,319`, `commit_backend.rs:179`) — misleads
operator triage. Add `Internal`/`InvalidPath` variants.

**M22 [ARCH] Four separate path-traversal guards** with four behaviors (`relative_path_string`,
`resolve_scoped`, `safe_file_component`, `safe_component`) across `storage_box/paths.rs`, `commit.rs`,
`naming.rs`. Consolidate on one validated `SafeRelPath` newtype.

---

## LOW (worth a pass, not blockers)

- **L1** `HostConcurrencyLimiter` hand-rolls a keyed semaphore with `Notify` (thundering-herd
  `notify_waiters`); `HashMap<String, Arc<Semaphore>>` + `acquire_owned()` is the same with correct
  single-waiter wakeups. `fleet/host_limiter.rs:32-93`.
- **L2** `RetryPolicy::default()` hard-coded at all three completion sites; not tunable from `FleetConfig`/CLI
  despite being crawler-politeness policy. `main.rs:525`, `fleet.rs:343`.
- **L3** Orchestrator busy-probes the claim query (full `spawn_blocking` + DB open) when work is temporarily
  unclaimable; add backoff or distinguish "exhausted" from "nothing claimable now." `fleet.rs:137-194`.
- **L4** `parse_repo_sync` (the default `parse_repo*` entry points) buffers every post in a `Vec` bounded only
  by `max_records=10M`; the streaming visitor exists but isn't the default. Document/steer. `parse.rs:458-490`.
- **L5** MST hot loop over-allocates: per-leaf `String` clones, `Vec<StreamingMstItem>` per node, `next()`
  clones each item. Drain/`mem::take` instead. `mst.rs:170,223-239`.
- **L6** No orphan/unreachable-block check, so `verified_block_count` (all CAR blocks) can exceed reachable
  records — overstates the snapshot if used as a metric. `car.rs:285-327`, `parse.rs:534`.
- **L7** `post_decode`/`raw_partial_post` duplicate the whole CBOR+JSON typed-then-raw fallback ladder — a
  parity-bug magnet. Normalize to one IR + one extractor.
- **L8** Spool temp file opened twice and reopened `write(true)` without `truncate(true)` — latent if the
  empty-file assumption ever breaks. `transport.rs:206,240`.
- **L9** No startup spool-dir sweeper: successful CAR cleanup + byte-budget release ride solely on
  `SpooledRepo::drop`, which doesn't run on `process::exit`/OOM. `spool.rs:202-209`.
- **L10** Dry-run derive diverges from the real path: it skips ledger gating, so it overstates pending work
  when a ledger exists. `derive_manifest_cmd.rs:198-211`.
- **L11** `JsonLineMetricsRecorder` holds a global `Mutex<File>` doing blocking JSON write on the per-repo hot
  path (correct, not racy, just coupled). Channel-backed recorder later. `metrics.rs:388`.
- **L12** `posts_hourly` buckets NULL/unparseable `created_at` into the 1970 hour via
  `coalesce(created_at,'1970-01-01')` — pollutes the series; exclude non-`Valid` rows. `schema.rs:317`.
- **L13** Aggregate-rebuild SQL statements are retried with no dedup token + no jitter; a timeout where the
  INSERT actually applied double-appends (masked only by the preceding TRUNCATE). `execute.rs:53-74`.
- **L14** `claimed_at` left `None` on the lease-less claim path though the row is Claimed. `ledger.rs:273`.

---

## Lint posture (cross-cutting opinion)

The panic/unwrap/expect/indexing bans are **principled hardening** — keep them; they're the payoff of the v1
silent-cap lesson. But blanket `arithmetic_side_effects = deny` is **over-applied**: it forces `checked_add`/
`try_from` at ~200 sites, spawns the `increment`/`add_count`/`count_len` ceremony in `main.rs`, clusters 40
deep in `census.rs`, and is *already cracking* (18 non-test `#[allow]` escape hatches). Demote it to `warn`
workspace-wide and `#[deny]` it on the untrusted-input modules (`parse/`, `transport/`, `archive/` — the real
DoS surface). The panic bans carry the safety story; blanket checked-arithmetic on internal loop counters is
noise that hurts readability.

---

## Top structural changes, ranked

1. **End the lib/binary fusion** (H1+H2): move `main/` into the lib, `pub(crate)` by default, `pub` only for
   `tests/`. Kills the `#[cfg(test)] use main::{...}` hack and most of the "split/tighten architecture" churn.
2. **Fix the things whose names overclaim their guarantee** (H7 completeness, H6 "streaming", H4 "manifest",
   H8 parity): either make them true or rename them. These are the findings most likely to cause a wrong
   decision downstream.
3. **Harden the fleet lifecycle** (H13 shutdown, H14 spawn-isolation, H11 ledger-actor, H5 real backpressure,
   M7 self-reclaim): the orchestrator is the riskiest code and has **no end-to-end test**.
4. **Nail down the derive idempotency contract** (H15, M8, M9, M3): decide source-of-truth (ledger vs CH),
   make the aggregate rebuild atomic + memory-capped (the "OOM fix" was a column rename), and verify `FINAL`
   readers. Add the test that injects a crash between insert and ledger-append.
5. **Consolidate the duplication** (H3 backend trait, H9 + M10 transport/process runners, M4 CLI args, M22
   path guards, H12 derive triad, M17/M18 god-files): same code in 2-4 places is where drift bugs already live.

Note the meta-signal: 40+ commits, many titled "Harden/Refactor/Tighten/Split … architecture." That churn is
the symptom of H1 (no encapsulation boundary → every change ripples) more than of genuine design evolution.
Fixing the boundary should slow the churn.
