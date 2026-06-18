# Rust backfill rewrite — open items + assessment (round 4)

State: working tree on `v2-rust-backfill` (uncommitted fixes). Build green: clippy clean (deny-warnings),
`cargo nextest` **265/265 pass**. Disk + cargo authoritative; LSP ignored.

## This round's verdicts (the uncommitted "fixes are in" pass)

**Genuinely fixed — real, tested, cause-level:**
- **H15 — derive ledger↔ClickHouse double-count: FIXED (defense-in-depth).** Pre-resend CH existence probe
  (`SELECT count() … FINAL WHERE derive_dedupe_token=…`, gated on a durable ledger) repairs the missing ledger
  line instead of re-inserting; AND `derive_dedupe_token` is now a real column inside the `ReplacingMergeTree`
  ORDER BY of both serving tables, so a racing duplicate collapses under FINAL — independent of the
  dedup-window expiry that the old design relied on. `clickhouse.rs:598-631`, `derive_manifest_cmd.rs:333-360`,
  `schema.rs`. Tested.
- **H11 — per-repo ledger open+migrate: FIXED.** All three hot-path sites (`ensure_owned_before_commit`,
  host-override read, override write) now go through the shared `Arc<Mutex<SqliteLedger>>`. Grep confirms the
  only production `SqliteLedger::open` left are startup (`fleet.rs:97`) and the separate census command. `archive_host.rs`, `fleet/ledger_async.rs`.
- **L13 hardened** — the rebuild-INSERT non-retry guard now keys off the shared
  `AGGREGATE_REBUILD_SHADOW_SUFFIX` constant, not a literal. `execute.rs:228`.

**Partial — real work, gap remains:**
- **H10 — canary authenticity: PARTIAL (the gap is now narrow but real).** A genuine HMAC-SHA256 over the
  evidence with an env-var key (`EMOJISTATS_CANARY_HMAC_KEY`) was added, constant-time verified, key not
  Debug-printable — so a hand-forger now needs the secret, not just a text editor. **But:** (1) there is **no
  production signer** — only a `#[cfg(test)] sign_test_evidence`; the real `canary` command can't emit a signed
  file, so an operator with the key fabricates a passing signed file by hand without ever running the canary —
  the gate proves *key possession*, not *that a canary ran*. (2) thresholds aren't in the signed payload. (3)
  no clock-skew tolerance on the future-timestamp check. `canary_cmd.rs:545-593`.
- **M5 — AttemptResources: PARTIAL.** The specific re-threading theatre (a path threaded twice, redundant
  clones) is gone via a single `shared_ledger()` handle; the `Local`/`Fleet` enum + some `Option` returns
  remain. Good enough — not worth more churn.
- **H1 — lib encapsulation: marginally improved.** `pub mod app;` removed so the orchestration forest no
  longer leaks through the lib API; the other 14 modules are still blanket `pub`.

**Not addressed this round (deferred):** FLEET-EXIT (still report-then-exit on backoff), DRAIN-DEADLINE,
H5 (still the active_attempt_limit cap), M12, M3-atomic-guard, H2, M18, M4, COUNT-HELPERS,
INVERTED-LAYERING, M19, LINT.

---

## WHAT'S LEFT TO FIX

### Correctness / behavior (small list, mostly low-medium now)
1. **H10 authenticity finish** *(medium — it guards paid fan-out)*: make the canary *runner* the only thing
   that emits the HMAC (ideally over real execution artifacts), and add clock-skew tolerance. Today: gate =
   "you hold the key," not "the canary ran."
2. **FLEET-EXIT** *(medium)*: the run terminates when the only remaining work is in `next_attempt_after`
   backoff — it reports the deferred count, then exits. Either `sleep_until` the next eligible time and re-poll,
   or document that one-shot drain is intended and rely on a supervisor to relaunch.
3. **DRAIN-DEADLINE** *(low-medium)*: SIGINT/SIGTERM drain awaits in-flight attempts forever — a hung whale
   fetch blocks shutdown. Add a bounded drain timeout / second-signal hard-exit.
4. **M12** *(low)*: `lang_total.posts = countIf(emoji_occurrences>0)` mislabels "posts-with-emoji" as post
   volume (and drops emoji-less langs from that count); `posts_hourly` already does it right with `count()`.
5. **M3-atomic-guard** *(low)*: `EXCHANGE TABLES` assumes an Atomic DB engine — assert or document it.
6. **M11** *(low, rclone only)*: rclone re-downloads each object to hash it, twice per commit — gate behind a
   flag or document the double egress.

### Architecture / structure (your primary lens — the real backlog)
7. **M18 — decompose app.rs** *(high leverage)*: it's a 587-line god-file (9-command dispatch + storage config
   + canary gate + fetch_one orchestration + u64 helpers). **This is the root**: splitting it into a thin router
   + command modules dissolves M4, COUNT-HELPERS, and INVERTED-LAYERING at once.
8. **H2 — real module home for `main/`**: it's still a binary-side `#[path]` forest with a `#[cfg(test)] use
   main::{…}` re-export hack. Give those items proper `pub(crate)` homes and delete the hack.
9. **H1 residual** — audit the 14 remaining `pub mod` in lib.rs; tighten internals-only modules to `pub(crate)`.
   (`census/` is the in-repo template.)
10. **COUNT-HELPERS** — one shared checked-count module instead of app.rs's set + `derive/tokens.rs` +
    `parse.rs` (kills the `super::super::` reach-ups).
11. **M4** — extract Storage-Box args into a `#[command(flatten)]` struct (you already do this for
    `CanaryThresholdArgs`).
12. **LINT** — demote `arithmetic_side_effects` to `warn` + targeted `deny` on untrusted-input modules; removes
    the helper ceremony driving #10.
13. **H5** *(only if throughput matters)* — real mpsc fetch→parse decoupling instead of the cap; the cap also
    has a silent throughput cliff when `max_bytes ≈ max_inflight_spool_bytes`.
14. **Lower-priority dedup/hygiene**: M10 (ssh/rclone process-runner dup ~150 lines), M21 (`CountOverflow`
    catch-all), M22 (fragmented path guards, rclone's weaker than ssh's), M20 (census/metrics anyhow leaks),
    M6-transport (substring DNS/TLS classification), H9 (list_records bypasses the transport seam), M19, M15.

### Tests (the gap that under-guards everything above)
15. **No test drives `fleet::run`** (the orchestration loop — claim→process→complete, draining, backpressure)
    and **no test covers the census parallel-PLC path** — the two riskiest concurrency areas. Plus a
    change-detector tail (verbatim SQL/shell strings, magic byte-counts) that false-fails on benign edits.

---

## Overall assessment (updated)

**Production-code craft: B+ / A−.** Error handling is the standout (A): typed `thiserror` enums with `#[source]`
chains, retryable/permanent splits, operator-actionable recovery hints, anyhow confined to the command layer,
no success theatre. Dependency hygiene (32 deps, all used, feature-minimized) and public-API docs are excellent;
naming is precise and consistent. Held back from a solid A by self-inflicted things: checked-arithmetic
*ceremony* (~55-65% of 200+ sites guard provably-bounded counters), **~2 implementation comments in 24k lines**
(the subtle code — async semaphore, fleet loop, Unicode tables, magic constants — is unannotated), and
DRY-by-divergence in load-bearing code.

**Test suite: B / B−.** Real fixtures throughout (CARv1 from the production CID code, real sqlite, real-FS
backends, in-process HTTP servers) and the data-loss-critical invariants are precisely pinned — but the fleet
loop and census PLC path have zero coverage and there's a change-detector tail.

**Architecture: still the weak axis, and it's a process problem now.** The correctness work across four rounds
is genuinely good — the dangerous failure modes (manifest corruption, OOM rebuild, panic-abort, claim/spool
leak, double-append ×2, peer-recovery starvation, ledger contention) are closed and tested. But the structural
findings keep getting **relocated rather than resolved**: this round moved a 587-line god-file from lib-side to
binary-side and called it a pass. Five straight commits titled "Tighten/Harden architecture," and the god-file +
the encapsulation surface are essentially where they started. The fix is one focused decomposition pass (#7-#10
above), not another relocation.

**Is the review closeable?** For **correctness — effectively yes**: what's left is medium-to-low (H10 runner,
FLEET-EXIT, drain deadline, a couple of SQL/ClickHouse polish items). For **architecture — no, but it's now a
*known, bounded* backlog**, and none of it blocks running the backfill. If this were mine I'd: finish H10's
runner-signer, fix FLEET-EXIT + drain deadline, do the one app.rs decomposition pass, add a single fleet-loop
integration test — then ship and treat the rest (lint right-sizing, dedup consolidation) as ordinary cleanup.

**LOC verdict (recap):** 24.4k prod is justified for the scope (CAR/MST+CID, TS-parity emoji, claim/lease
ledger, PLC/PDS census, 3 archive backends, Parquet, ClickHouse derive+atomic rebuild, canary, fleet) — but
~10-15% is removable bulk from duplication + lint ceremony, not essential complexity.
