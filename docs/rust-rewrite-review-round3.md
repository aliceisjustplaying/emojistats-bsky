# Rust backfill rewrite — verification round 3 (post "Harden architecture pass")

Re-review against `6057a1c "Harden Rust backfill architecture pass"` (the single commit since round 2).
Build green: `cargo clippy --workspace --all-targets` clean (deny-warnings), `cargo nextest` 263/263 pass.
LSP ignored; disk + cargo authoritative.

## The headline you need to hear

**The commit's diff is loud (app.rs +583, main.rs −564, fleet/canary/ledger reworked) but most of it is
relocation, not repair.** `main.rs` is "now 4 lines" — but its 566-line body moved **verbatim** into a new
`app.rs` (583 lines), carrying the `#[cfg(test)] use main::{...}` hack (app.rs:69-73), the same 9-command
dispatch, the same u64 helpers, the same storage-arg copy-paste. That is precisely the *"don't claim a win
from 'now 1 line' when the substance moved to a sibling"* pattern. Two reviewers independently flagged that
the line-count is doing the persuading, not the code (the cited `failure.rs +8` and three of four
clickhouse-touched files are pure `emojistats_backfill::`→`crate::` import churn).

That said, **four real fixes did land** inside the noise. Don't let the theatre hide them.

---

## GENUINELY FIXED this round

- **M7 — stale-recovery self-worker exclusion. ✅ ROOT CAUSE, tested.** `recover_expired_claims` now takes
  `excluded_worker_id` with SQL `AND (?3 IS NULL OR worker_id IS NULL OR worker_id <> ?3)` (`store.rs:685`)
  + in-loop re-check; the `active==0` gate is gone (`fleet.rs:135,595-597`). New test
  `sqlite_expired_claim_recovery_excludes_current_worker`. This removes the peer-recovery starvation the
  *previous* round's fix introduced — the sweep runs unconditionally and a busy box recovers dead peers' claims.
- **L13 — rebuild double-append. ✅ FIXED (different mechanism than asked, valid).** Instead of a dedup token,
  the shadow `INSERT … __rebuild_shadow` is classified not-retry-safe (`execute.rs:201-229`): on timeout it
  fails fast, aborts the statement sequence so `EXCHANGE` never runs, and the next run's leading `DROP …
  __rebuild_shadow SYNC` wipes the half-built shadow. Tested (`aggregate_rebuild_insert_is_not_retried`). This
  is honest and arguably better than a token. **Caveat (new, LOW):** the guard is a substring match on
  `"INSERT INTO"` + `"__rebuild_shadow"` decoupled from the schema's own `AGGREGATE_REBUILD_SHADOW_SUFFIX`
  constant — rename the suffix without updating the matcher and L13 silently reopens.
- **N1 — progress watchdog reorder applied to all three loops. ✅** `collect_body_with_cap`
  (`transport.rs:421-426`) and `read_response_body_with_cap` (`list_records.rs:629-633`) now count-then-check
  like `stream_to_temp_file`. (Regression test still only covers the already-correct loop.)
- **M14 confirmed still holding** (429→RateLimited with derived cooldown); not regressed.

---

## PARTIAL — real work, but the finding is not closed

- **H10 — canary trust boundary: freshness YES, authenticity NO.** The commit did real work: it deleted the
  verbatim `status` field from the four integrity gates, which are now derived from typed bool observations
  (`canary_cmd.rs:393-470`, test `status_only_integrity_gate_is_rejected`), and it added run-id + timestamp +
  staleness validation on the paid path (`validate_metadata`, `canary_cmd.rs:472-506`, default 24h). **But there
  is zero authenticity binding** — grep for sign/hmac/checksum/digest/nonce returns nothing. Every field the
  gate checks (`run_id`, `generated_at`, `max_age_seconds`, the four bools) is hand-typeable into the same file;
  the operator even controls *both sides* of the run-id match, and `max_age_seconds` lives in the same file a
  forger writes. **A 3-line hand-written JSONL with a current timestamp still passes the gate guarding paid
  fan-out.** Freshness is now checked; forgery is not prevented. (New MEDIUM: the future-timestamp check is
  zero-tolerance wall-clock — a generator box whose clock is slightly ahead of the gate box gets valid evidence
  rejected; needs a skew window. New LOW: 12 sample + 7 injection observations still pass `status` verbatim — so
  19 of 29 checks remain hand-assertable.)
- **H11 — ledger connection reuse: orchestration only.** A real long-lived `SharedBlockingLedger(Arc<Mutex<
  SqliteLedger>>)` now backs claim/complete/heartbeat/sweep (`ledger_async.rs:12-128`, `fleet.rs:96,108`) —
  migrations run once for those. **But three per-repo/per-fetch hot paths still `SqliteLedger::open()` +
  re-migrate per call**: `archive_host.rs:137` (the per-repo commit-check H11 explicitly named),
  `archive_host.rs:279`, `fetch_attempt.rs:316`. The headline item is half-delivered.

---

## NOT FIXED — the "architecture pass" relabeled these

- **H1 — lib.rs encapsulation: marginally WORSE.** Now 16/17 `pub mod` (was 14/15); `app` was added as
  `pub mod app;` (`lib.rs:3`). The churn cause is untouched and the new entry point is the most-exported thing
  in the crate.
- **H2 / M18 — the move is a rename.** `main/` is still a private `#[path]` forest mounted under `app`
  (`app.rs:42-55`); the `#[cfg(test)] use main::{...}` hack persists verbatim (`app.rs:69-73`). app.rs is now a
  **god-root**: god-file *and* the binary's module mount point.
- **NEW [MEDIUM/ARCH] — inverted layering, entrenched.** Transport/fetch policy constants
  (`FETCH_TRANSPORT_*`, `CRAWLER_USER_AGENT`, `HOST_OVERRIDE_CACHE_TTL`) live in the CLI entry layer
  (`app.rs:75-79`) and are pulled *up* by leaf modules (`main/repo_fetch.rs`, `main/archive_host.rs`) via
  `super::super::`. The rename re-rooted this under `app`, making the wrong-layer ownership more permanent.
- **NEW [HIGH/ARCH] — count-helper triplication, relocated not unified.** `increment`/`add_count`/`count_len`/
  `payload_row_count` moved to `app.rs:552-579`, still shared binary-wide via `super::`, still duplicating a
  separate `count_len` (→DeriveError) in `derive/tokens.rs:253` and `checked_increment` (→ParseError) in
  `parse.rs:613`. Two functions named `count_len` with different error types across lib/bin.
- **M5** AttemptResources enum + 8 Option accessors unchanged (`attempt_resources.rs:49-134`). **M4** storage
  args still copy-pasted across FetchOne/RunFleet + now re-destructured in app.rs twice (3 places). **M19**
  `debug_*` still `#[cfg(any(test,debug_assertions))] pub` (public in dev builds). **LINT**
  `arithmetic_side_effects=deny` still blanket (the 4 app.rs helpers exist to satisfy it).
- **H5** fetch/parse still inline-coupled; still the `active_attempt_limit` cap with its throughput cliff. **L1**
  HostConcurrencyLimiter still hand-rolled Notify/thundering-herd (only an import path changed). **L2**
  RetryPolicy::default() still hard-coded (`fleet.rs:454`, `app.rs:542`).
- **FLEET-EXIT — theatre.** A `deferred_claim_summary` was added that *reports* the backoff-waiting rows
  (`fleet.rs:219-227`) — then the loop still `break`s and exits. It logs the symptom; the premature exit on
  temporarily-unclaimable work is unchanged. **DRAIN-DEADLINE** still none.
- **H15** derive ledger↔ClickHouse reconciliation untouched (the file changes were import churn) — counters
  table still double-counts past the dedup window on crash-resume. **M3-atomic-guard** EXCHANGE still assumes an
  Atomic DB engine, unguarded/undocumented. **M12** `lang_total.posts = countIf(emoji_occurrences>0)` still
  mislabels post-with-emoji as post volume. **M6-transport** still substring DNS/TLS matching (the cited
  `failure.rs +8` was pure import churn — no typed downcast added). **H9** list_records still a third raw
  `bytes_stream()` copy on `&reqwest::Client`, bypassing the transport seam. **N2** unchanged.
- **M10/M11/M15/M20/M21/M22** (process-plumbing dup, rclone re-download, census double-open, anyhow leaks,
  CountOverflow catch-all, fragmented path guards) — none addressed this round.

---

## Scorecard across all three rounds

- **Correctness, high-severity: genuinely in good shape.** C1, M3, H6, H13, H14, H16, M2, M8, M14, M7, L13, N1
  are real, tested fixes. The crawler's dangerous failure modes (manifest corruption, OOM rebuild, panic-abort,
  claim/spool leak on SIGINT, double-append, peer-recovery starvation) are closed.
- **Two correctness items still genuinely open and worth doing:** **H15** (ledger↔CH exactly-once — counters
  double-count past the dedup window) and **H10 authenticity** (the canary gate guarding paid fan-out is still
  hand-forgeable; freshness ≠ forgery-prevention).
- **Architecture (your primary lens): essentially unmoved across three rounds.** H1, H2, H5, H11(½), M1, M5,
  M10, M20, M21, M22, the lint posture, the count-helper duplication — all open. This round's "architecture
  pass" relocated the god-file rather than decomposing it and made the lib surface marginally worse. The
  meta-signal is now unmistakable: four straight commits titled "Harden/Tighten architecture," and the
  encapsulation boundary (H1) — the actual cause of that churn — has not been touched once.

## What actually moves the needle next (ranked)

1. **H10 authenticity** — have the canary *runner* emit an HMAC (key the hand-author lacks) or a gate-derived
   run nonce; otherwise the freshness work is a speed bump, not a gate. + skew tolerance on the timestamp check.
2. **H15** — pick a source of truth (ledger or CH) and reconcile; counters double-count on crash-resume today.
3. **H1 + H2 for real** — `pub(crate) mod` by default in lib.rs, move `main/` to a genuine lib module, delete
   the `#[cfg(test)] use main` hack, decompose app.rs into a thin router + command modules. `census/` is the
   in-repo template. This is the only thing that stops the "tighten architecture" commit treadmill.
4. **Finish H11** — route the three per-repo `SqliteLedger::open()` sites through the shared connection.
5. **L13 hardening** — key the retry-safety guard off the shared `AGGREGATE_REBUILD_SHADOW_SUFFIX` constant, not
   a substring literal.

**Bottom line:** real correctness fixes landed (M7 and L13 especially), and the build is green with 263 passing
tests — but calling `6057a1c` an "architecture pass" oversells it. It is a file rename plus four targeted fixes;
the architecture findings you prioritized are still open, H10 and H15 remain real risks, and the canary gate is
not as closed as the +225 lines suggest. I would not call the review closed.
