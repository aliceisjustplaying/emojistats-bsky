# Rust backfill rewrite — verification round (post-fix)

Re-review of the original findings against on-disk code at `b57bc1d` (build green: `cargo check
--workspace --all-targets` clean; LSP diagnostics were stale and ignored). Verdicts from 7 parallel
subsystem re-reviews. Skeptical of symptom-fix theatre per the no-theatre rule.

**Headline:** the high-severity *correctness* bugs got real, tested fixes. The *architecture* findings
(the ones you said were primary) are mostly **not** fixed — and the meta-signal confirms it: 3 of the
last 4 commits are still "Tighten architecture/invariants," because the root cause (H1, no encapsulation
boundary) is intact. Two fixes also introduced new risks.

Tally: **17 genuinely fixed · ~10 partial · ~20 still open · 6 new issues (2 notable).**

---

## GENUINELY FIXED (cause-level, most tested)

- **C1 (CRITICAL)** rclone lock-free manifest append → now **fails closed** (`rclone.rs:244`) AND routed
  around: rclone commits with `ManifestMode::Skip` and the manifest is published *locally*
  (`commit_backend.rs:226-230`). Test `rclone_manifest_append_fails_closed_without_remote_lock`. ✅
- **M3** Aggregate-rebuild OOM — **genuinely fixed this time, not a rename.** Now shadow-table + `EXCHANGE
  TABLES` (atomic) + explicit `max_memory_usage=8GiB` / `max_bytes_before_external_group_by=1GiB`; `TRUNCATE`
  fully removed (test asserts its absence). Git confirms the column-rename (`83a76ed`) and the real fix
  (`b57bc1d`) were separate commits. `schema.rs:124-198`. ✅
- **H6** "Streaming" derive → now a true per-chunk producer/consumer; peak RAM = one ≤10k-row/≤8MiB chunk,
  not the whole file. `derive_manifest_cmd.rs:220-241`. ✅
- **H13** Draining shutdown: `select!` on `ctrl_c` + SIGTERM, stops claiming, drains `active`, releases
  claims, `SpooledRepo::drop` runs. `fleet.rs:117-236,610-628`. ✅
- **H14** Panic isolation: attempts now `JoinSet::spawn` + `AssertUnwindSafe(..).catch_unwind()` → a poisoned
  attempt becomes a retryable failure instead of aborting the orchestrator. `fleet.rs:107,284-298`. ✅
- **H16** `FetchError::Io` now routes ENOSPC / persist-conflict through `is_operator_io_error` →
  `OperatorDeferred` instead of retrying forever. `failure.rs:91-96`. ✅
- **M2** Parallel PLC cursor = `min` of per-range *persisted* high-water marks; early-stop no longer skips a
  gap. `plc.rs:198-214`. ✅ (but untested — see N-list)
- **M8** Poison checkpoint fixed: `checkpoint()` propagates the error via `?`; no more `u64::MAX`/empty-hash
  sentinel. `ledger.rs:110-117`, `derive.rs:162-178`. ✅
- **M14** 429 → `RateLimited { retry_after }` with derived cooldown (60s fallback), not bare retryable.
  `failure.rs:79-85,136-143`. ✅
- **H3** One `ArchiveCommitStore` trait; the drifted Local/SSH/Rclone match arms are gone. `commit_backend.rs:9-25`. ✅
- **H4** Manifest is now a real per-shard append log `manifests/{run_id}/{shard}.jsonl`, not a per-hash
  single-entry marker. `naming.rs:17-21`. ✅
- **L4** Streaming visitor is now the default parse path; the unbounded-`Vec` `parse_repo_sync` has no
  production callers. ✅
- **L7** CBOR+JSON decode ladder consolidated into `post_decode`. ✅
- **M17** `census.rs` split into `census/{db,pds,plc,types}` cleanly — and it's the *model* of the
  `pub(crate) mod` + curated re-export pattern H1 wants for `lib.rs`. ✅
- **M13** Progress watchdog reordered to count-then-check… **on the spool path only** (see N1). ◐
- **L3** Claim busy-probe removed… by making the run terminate when nothing is immediately claimable (see N2). ◐
- **L14** `claimed_at` None on lease-less path is effectively a non-issue (only the ephemeral fetch-one path). ✅

---

## PARTIAL / honest-but-incomplete

- **H7** Snapshot-completeness *overclaim* fixed by **honest docs/naming** (`CompletenessClass`,
  `mst_root_cid_verified` now say what they actually prove: "out of scope for Stage C"). Signature/identity
  verification and MST-root recompute are still **not** implemented — now openly admitted. `parse.rs:163-180`.
- **M12** Emoji-less languages no longer dropped (filter removed), **but** `lang_total.posts =
  countIf(emoji_occurrences > 0)` still mislabels "posts with an emoji" as post volume. `schema.rs:464-481`.
- **M6-rclone** Worst case (silent missing upload) closed via `lsjson --stat` → `MissingRemoteFile`; the
  brittle stderr substring fallback remains. `rclone.rs:123-144`.
- **M16** `upload_command` now validates the path; the manifest interior-newline assert was **not** added.
- **H9** Two transport loops share helpers; the **third copy** (`list_records.rs:592-636`) still drives raw
  `reqwest::bytes_stream()` and the whole list-records lane takes `&reqwest::Client`, bypassing the transport
  seam — no shared byte budget / typed classification.
- **H17** Retry-convergence machinery is solid and tested; the "manifest is sole source of truth, re-attempt
  required" invariant is still undocumented.
- **M19** `debug_*` fns gated `#[cfg(any(test, debug_assertions))]` but still `pub` (public in dev builds).
- **L6** Orphan-block gap documented, not checked.

---

## NEW ISSUES INTRODUCED OR NEWLY-EXPOSED BY THE FIXES

- **★ [HIGH/CORRECTNESS] L13 went from masked to LIVE.** The aggregate-rebuild SQL is retried with **no dedup
  token and no jitter** (`execute.rs:131-156`, `clickhouse.rs:208-229`). Previously the `TRUNCATE` masked a
  retried-INSERT double-append; now that M3 replaced TRUNCATE with shadow + `EXCHANGE`, a timeout where the
  INSERT actually applied double-appends into the `SummingMergeTree` shadow, which `EXCHANGE` then swaps live
  — an **active 2× count risk**. The M3 fix removed the thing that was hiding it. Fix: dedup token on rebuild
  inserts (or a per-run marker).
- **★ [HIGH/ARCH] Canary trust boundary still wide open (H10, NOT FIXED).** Evidence has no run-id / timestamp
  / signature — a hand-written JSON still passes the gate guarding paid fleet fan-out — and four integrity
  gates (`whale_completes_cleanly`, `storage_box_manifest_detects_partial_upload`,
  `receipt_recomputation_detects_corruption`, `invalid_repos_classify_loudly`) pass their `status` through
  **verbatim** with no measurement backing. `canary_cmd.rs:114-157,309-318`. This is the highest remaining risk.
- **[MEDIUM/CORRECTNESS] Sweep starvation (from the M7 fix).** Stale-recovery is now gated on
  `active_attempts == 0` (`fleet.rs:122,573-579`) instead of excluding self-`worker_id` rows. A box running
  near its attempt cap *never* idles, so it never reclaims a **peer** box's crashed claims — recovery waits for
  an idle box or process restart. Trades self-reclaim for a peer-recovery liveness gap.
- **[MEDIUM/ARCH] Fleet exits prematurely (from the L3 fix).** A single `try_claim_next → None` now ends the
  run once `active` drains, even when remaining entries are merely in `next_attempt_after` backoff
  (`fleet.rs:168-171,205-208`). One-shot drain may be intended, but it's an unflagged behavior change and
  relies on an external supervisor to relaunch.
- **[MEDIUM/CORRECTNESS] rclone `rename` is non-atomic (new in the rewrite).** `stat_len(to)` then `moveto`
  (TOCTOU; `moveto` overwrites by default) vs SSH's atomic `mv -n` (`rclone.rs:221-242`). Benign under
  content-hash naming, but a differing-content retry could clobber a good final object.
- **[MEDIUM/CORRECTNESS] N1: M13 reorder applied to only 1 of 3 loops.** `collect_body_with_cap`
  (`transport.rs:392`) and `read_response_body_with_cap` (`list_records.rs:610`) still check-then-count — the
  same spurious `ProgressTimeout` the M13 fix addressed.
- **[HIGH/ARCH] Duplicated checked-arithmetic helper families.** `main.rs:535-562` defines
  `increment`/`add_count`/`count_len`/`payload_row_count` (→anyhow) shared binary-wide via `super::`, while
  `derive/tokens.rs:253` has its *own* `count_len` (→DeriveError) and `parse.rs:613` a `checked_increment`
  (→ParseError). Two `count_len`s with different error types across lib/bin — the LINT ceremony just got
  relocated (non-test `#[allow]` count dropped 18→3 by hiding it in helpers, not removing it).
- **[LOW] `EXCHANGE TABLES` now requires an Atomic DB engine** — a new env precondition, unguarded.
- **[LOW] Drain has no deadline** — a slow in-flight whale fetch can hang "graceful" shutdown for minutes
  (`fleet.rs:205-236`).

---

## STILL OPEN — architecture (your primary lens; mostly untouched)

- **H1** `lib.rs` still `pub mod` on 14/15 modules — zero encapsulation, the churn cause. `lib.rs:3-18`.
- **H2** `main/` orchestration still binary-side; the `#[cfg(test)] use main::{...}` hack persists. `main.rs:62-66`.
- **H5** Fetch/parse still inline-coupled on one task; "fixed" via an `active_attempt_limit` **cap** (symptom
  mitigation) that also creates a silent throughput cliff when `max_bytes ≈ max_inflight_spool_bytes`. `fleet.rs:581-600`.
- **H11** Every ledger op still opens a fresh connection **and re-runs all migrations**; WAL + busy_timeout was
  added, which *masks* the contention rather than fixing it. `ledger_async.rs`, `store.rs:267-272`.
- **H12** Derive triad not consolidated (layering is sound; it's a rename/re-nest job). `M18` main.rs still 566 lines.
- **M1** Census schema still outside the migration system, shared ledger DB file, DEFERRED vs IMMEDIATE txns. `census/db.rs:18-54`.
- **M5** `AttemptResources` Local/Fleet enum + 8 Option accessors unchanged. **M10** process plumbing still
  duplicated ~140 lines. **M11** rclone re-downloads the whole object to hash, twice per commit. **M15** census
  double-open. **M20** census/metrics still leak anyhow. **M21** `CountOverflow` still a catch-all. **M22** 8
  fragmented path guards (rclone's weaker than SSH's). **M6-transport** still substring matching (added an
  unrelated `is_builder()` check — theatre).
- **L1, L2, L5, L8, L9, L10, L11** unchanged.

---

## What I'd fix next, ranked

1. **L13 — dedup token on rebuild inserts.** A fix (M3) un-masked a live 2× count path; close it before relying
   on the aggregates. Cheap.
2. **H15 — derive ledger↔ClickHouse reconciliation.** Still no real exactly-once; resume past the dedup window
   silently double-counts `v2_total_post_counters_r3`. Decide source-of-truth + `FINAL`/existence-probe.
3. **H10 — canary evidence provenance.** Bind to run-id + timestamp + staleness; derive the 4 integrity gates
   from structured observations. It guards paid fan-out and is hand-forgeable today.
4. **M7 follow-through — self-`worker_id` exclusion in the sweep** so it can run unconditionally (removes the
   peer-recovery starvation the current gate introduced).
5. **H1/H2 — the encapsulation boundary.** `pub(crate) mod` by default + move `main/` into the lib. `census/` is
   the in-repo template. This is what actually stops the "tighten architecture" commit churn.

**Bottom line:** "a lot has been fixed" is true for **correctness** — C1, M3, H6, H13, H14, H16, M2, M8, M14 are
real and tested, and M3 in particular is a genuine fix this round, not the column-rename you were burned by
before. But the **architecture** asks (H1, H2, H5, H11, M5, M1, M10, M20, M21, M22) are largely still open, a
couple of "fixes" are caps/gates/relocations rather than cause fixes, and the shadow-rebuild quietly exposed a
new live double-count (L13). I would not call the review closed.
