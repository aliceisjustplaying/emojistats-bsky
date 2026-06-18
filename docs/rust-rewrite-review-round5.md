# Rust backfill rewrite — review round 5 (hands-on, iterative re-verification)

Scope: `rust/` workspace on `v2-rust-backfill`, current working tree. Method: read + ran the code
directly (no subagents after the first draft). **Build status (current tree): `cargo clippy --workspace
--all-targets` clean (deny-warnings); `cargo nextest` 285/285 pass.**

This doc has been re-verified across several fix waves the author landed between review turns. It records
the **current** state plus an honest correction to one mistake I made mid-review.

---

## Correction I owe (emoji segmentation)

In an earlier pass I called the subagent's "emoji adjacent-modifier divergence" a **false positive**,
having run both pipelines and seen identical output. That conclusion was **wrong**: by the time I ran the
repro, the working tree had already been patched. The original *committed* `extract_emoji_sequence` did
plain UAX#29 grapheme splitting with no base+modifier or regional-indicator pairing, so it **did** diverge
from the TS `emoji-regex` pipeline (the bug the subagent flagged). The current tree fixes it
(`emoji-normalizer/src/lib.rs`: `push_regional_indicator_run` + base+modifier pairing in
`push_normalized_glyph`) and adds a regression test (`matches_legacy_adjacent_skin_tone_modifier_fallback`
asserting `"👌🏻","🏾"`). So: the finding was real; it is now fixed and tested. (The broader H8 point — the
parity corpus is per-glyph, not a true segmentation oracle — still stands as test-hygiene; only the two
concrete reproducers are pinned.)

---

## Fixed and verified in the current tree

- **Emoji adjacent skin-tone / regional-indicator segmentation** — fixed + tested (above).
- **Derive: single read + validate-before-insert, memory still bounded.** The two-pass double-read (R5-4)
  is gone — one `File::open` (`derive_manifest_cmd.rs:325`); chunks are **staged to temp files**
  (`StagedDerivePayload.body_path: TempPath`, `:152-165`), `state.finish()` validates the receipt hash
  (`:342`), then staged bodies are read back and inserted (`:352,370-380`). This resolves R5-4 *and* the
  insert-before-validate ordering concern while preserving H6 (peak RAM = one chunk; bodies spill to disk).
- **PLC mirror cursor (R5-1)** — `createdAt` ISO cursor, derived from `createdAt` not `seq`; verified
  against live `plc.directory/export`; tested.
- **PlcPlan / seq-range vestigial code — removed entirely** (grep for `split_seq_ranges` /
  `discover_plc_head_upper` / `plan_plc_ranges` / `PlcPlan` is empty). The createdAt/seq inconsistency I
  flagged is gone with it.
- **M15 census double-open — fixed.** `open_census_connection` is now
  `SqliteLedger::open(path)?.into_connection()` (`census/db.rs:20`) — one open, migrations run, connection
  reused.
- **Nightly `Duration::from_hours/from_mins` — fully removed** (incl. `canary_cmd.rs` →
  `from_secs(24*60*60)`); builds on stable. *(Still no `rust-toolchain.toml` — a nice-to-have, no longer a
  blocker.)*
- **M4** storage-box args flattened; **M10** ssh/rclone runner shared via `storage_box/process.rs`;
  **H11** ledger access via one `SharedBlockingLedger` — all still in place and tested.

Also sound on disk (re-confirmed): H15/L13/M3/M3-atomic-guard/M12/M13 derive + rebuild correctness; the
fail-closed drain (claims stay leased on deadline abort); list_records permanent/transient split.

---

## Confirmed false (first-draft subagent findings, retracted — verified against code)

Derive idempotency cluster: token is **not** in either ReplacingMergeTree ORDER BY (`schema.rs:279,309`),
`EXCHANGE TABLES` **is** retry-guarded (`execute.rs:223,231-234`), `RateLimited` **does** refund the
attempt count (`ledger.rs:320-323`), the existence probe uses a `{token:String}` param (not string
interpolation), and the drain path has no premature claim release. (The emoji finding, previously listed
here, is moved to the correction above — it was real.)

---

## Genuine remainder (real, low-severity / architecture backlog; no launch blockers)

- **H1 [ARCH] lib is `pub`-everything** — `lib.rs` 15 `pub mod`, only `post_decode` `pub(crate)`. The
  encapsulation boundary; highest-leverage structural cleanup, low harm for an internal single-crate tool.
- **H2 / inverted layering [ARCH]** — orchestration mounted binary-side via `#[path]` under `app.rs`, which
  imports library symbols only to re-vend them to its `#[path]` children via `super::`. Works; importing
  from `emojistats_backfill::` directly would remove the hub.
- **Not re-checked this pass / re-confirm before relying:** H10 canary authenticity (only got the
  `Duration` cleanup this round — signer still signs operator-supplied measurements, so it proves
  key-possession, not that a canary ran), M1 (census schema outside the migration system + shared DB file),
  M11 (rclone hash re-download), M20/M21/M22 (anyhow leaks / `CountOverflow` catch-all / path-guard
  fragmentation), lint posture (`arithmetic_side_effects = deny`).

---

## Verdict

The author has, across several waves, fixed essentially every concrete correctness/hygiene finding:
emoji segmentation (real, now tested), the PLC cursor, the derive read/validate path, census double-open,
the vestigial PLC planner, M4/M10/H11, and the nightly-API hazard. Current tree is clippy-clean with
285/285 tests passing.

What's left is the **architecture backlog** (H1 encapsulation, H2/inverted-layering) plus a short
"re-confirm" list led by **H10 canary authenticity** — none of it blocking a launch. Process lesson from
this round: when re-verifying a moving tree, check whether the thing under test changed since the finding
was written — I dismissed a real emoji bug because I measured the already-patched code.
