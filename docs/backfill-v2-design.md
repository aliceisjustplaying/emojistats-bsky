# Emojistats Backfill v2 — Design & Decision Record

> **Status:** Decisions locked, pre-implementation. **No code written yet.** This is the
> single source of truth for the v2 design. It is the output of a 2026-06-15 session:
> an adversarial critique of the original v2 plan against the v1 codebase, a full re-read
> of [`docs/retro.md`](retro.md), and a decision-by-decision grilling. Where this document
> and the original plan disagree, **this document wins** — it incorporates corrections the
> plan did not have.
>
> v2 is a clean **Rust rewrite** of the v1 TypeScript backfill (`packages/backfill`). v1
> already crawled ~35.89M repos / ~2.59B posts; the retro's thesis is that **the data was
> never the hard part** — the time went to telling a *stuck* system from a *slow* one,
> silent caps, reactive pacing, and an unscalable verifier. v2 is designed around removing
> those self-inflicted ceilings, not around making the crawler faster.

---

## Purpose & win condition

Re-backfill the full Bluesky network and stand up the public emoji-stats site on the
result, **greenfield** (treat v1/prod as not existing). Two products come out of it:

- the public **ClickHouse-served emoji site**, and
- the complete **raw post corpus published to HuggingFace** (the primary reason all raw
  post data is collected).

The win condition: get from **zero to a 100%-backfilled site running in under a day**,
bounded **only by the mushroom PDSes' per-IP rate limits** and nothing else self-inflicted.
Roughly half the exercise is *speed to 100%-backfilled site*; the other half is
correctness and a clean, publishable corpus.

### Tiebreaker

When decisions conflict: **correctness > operability > performance > craft.** "Performance"
here means *"no self-inflicted ceilings"* (saturate the host rate limits, freeze nothing),
not raw language speed — a Rust crawler that reintroduces an O(n) claim scan is exactly as
dead as the JS one was. Any decision trading correctness for "faster/cooler in Rust" loses.

---

## Pipeline overview

```
enumerate: PLC export + did:web seeds
  → CENSUS: listRepos each PDS (NEVER aggregators) → terminally classify junk, no getRepo
  → CANARY: 1 box, full pipeline, HARD-GATED ──GO──▶ fan out to 8
  → CRAWL (8 boxes · uniform DID-hash · per-IP maximally-fast):
       getRepo via Jacquard download() + own reqwest HttpClient
         + self-driven idle timeout (kills half-open sockets)
       → spool CAR to 512 GB local disk (no size cap)
       → parse: on-disk BlockStore + MST walk (Jacquard codec)
       → VERIFY: MST root == signed commit  (completeness PROOF)
       → write Parquet (flat cols + extras JSON) + per-repo receipt → 1 TB Storage Box
       → discard CAR
  ∥ DERIVE POOL (concurrent, paced): tail manifest → recompute-from-Parquet vs receipt
       → bulk-load emoji rows + aggregates → ClickHouse
  → rebuild aggregates (exact) → SITE LIVE
  → PACKAGE: dedup + repartition by created_at month + card → HuggingFace
```

The crawler's only output is Parquet. ClickHouse is a derived, rebuildable projection. The
Parquet archive is the single source of truth and the HuggingFace product.

---

## Scaling & fetch model

- **Rate limits are per-IP** (confirmed). getRepo is on the unauthenticated
  `com.atproto.sync` namespace and Bluesky rate-limits unauthenticated reads by client IP.
  Therefore **N boxes = N× the budget against each mushroom**, and scaling out genuinely
  buys throughput.
- **Maximally fast.** Each box paces to the full advertised per-IP budget, with **no
  cross-box coordination**. An over-served host self-corrects via AIMD + header pacing
  (per-box). Courtesy heads-up to mushroom operators before launch is a goodwill step, not
  a code constraint.
- **Sharding: uniform DID-hash across all 8 boxes**, persisted in the ledger's bucket
  column (modulus pinned with a constructor guard — shard count is baked in; resharding is
  a ledger migration, not a config flip). No whole-host assignment.
- **Two per-host concurrency regimes** (the chosen complexity ceiling):
  - **Mushrooms** — `*.bsky.network` **and `bsky.social`** — get a **dynamic cap derived
    from the advertised rate-limit headers** (≈60 s of advertised queue depth, capped).
    `bsky.social` MUST be in this regime: it fronts millions of pre-migration DIDs, and v1
    misclassifying it as third-party (cap 2) cost a 70-day ETA.
  - **Indie / third-party PDSes** get a **conservative constant cap** — polite, simple, and
    a tiny PDS hit by 8 IPs at a low constant still drains in seconds.
- **Bridgy / capability-variant PDSes:** `getRepo` returns HTTP 429 *"temporarily disabled
  12 hrs after repo creation"* — a permanent wall in a transient code. Carry a
  **capability probe + `getRepo` → `listRecords` fallback**, and escalate "429 with zero
  successes ever, over a long window" to terminal-for-method rather than retrying forever.

---

## Repo parse & verification

- **Spool → parse → verify → discard.** Stream the getRepo CAR to local disk (512 GB/box),
  parse from disk, verify, archive, then **discard the CAR**. Local disk is bounded by
  *concurrent in-flight* CARs, not cumulative; that disk budget is the backpressure knob
  for fetch concurrency. Whales (rare) are allowed to tie up a slot for minutes — no
  dedicated whale lane.
- **No silent caps, ever.** v1's worst bug dropped every repo over a 1 GiB CAR cap —
  wholly, posts and all — under a "quarantined" status nobody read (16 repos / 23.2M
  posts). v2 has **no size cap**; a whale is a throughput cost, never a data-loss boundary.
  The only rejections are **loud terminal classifications** (malformed CAR, missing blocks)
  that land on a re-fetch list. Audit for the *pattern*, not the instance (raising one cap
  reveals the next).
- **Parsing uses an on-disk BlockStore + MST walk.** Jacquard's built-in `Repository` is an
  in-RAM `BTreeMap` and cannot hold a multi-GB whale, so we implement our own on-disk
  `BlockStore` over the spooled CAR and drive `MstCursor` over it. The MST walk is required
  anyway — rkeys live in the MST leaf keys (not the record body), and the completeness
  proof needs the reconstructed tree.
- **Verification is a completeness PROOF, not "strong evidence."** Reconstruct the MST and
  check its **root CID == the repo's signed commit root** — this proves we hold every
  record the author committed. **Skip signature/identity verification** (proves *who*
  signed, not completeness; it was "dead weight at VPS scale"). A root mismatch or a missing
  block (`MstCursor` hard-aborts on a missing node) is the **loud incomplete → re-fetch**
  class, never a silent pass.
- **Per-repo receipt** (computed at parse time, while the CAR is in hand — it is about to be
  discarded): rkey-digest over **all** rkeys, post counts, root CID. Written to the archive
  manifest alongside the Parquet.
- **Durable-write check:** the derive pool **recomputes the digest/counts from the synced
  Parquet** and compares to the receipt before advancing a repo to `loaded`. That confirms
  the parser→Parquet→sync path, the silent-loss class v1 feared most.
- **No loose band.** Because the MST-root match proves per-repo completeness directly, v2
  has **no LOOSE-band convergence re-fetch loop** — the thing that cost v1 two days of
  ClickHouse OOMs, false passes, and header overflows simply does not exist here.

---

## Storage & the Parquet archive

- **All posts → zstd Parquet on a 1 TB Hetzner Storage Box.** This is the source of truth
  and the HuggingFace product. ClickHouse holds only the emoji-bearing serving subset.
- **Schema — flat common columns + one lossless catch-all, no duplication:**
  - flat columns: `did, rkey, cid, created_at, text, langs`, plus emoji-derived fields;
  - one lossless **`extras` JSON** column for everything else in the record — facets, reply
    refs, embeds, self-labels, tags, **and any field we did not model or that future
    lexicons add.**
  - No field is stored twice (storing `text` flat *and* in a full raw blob is what would
    balloon storage). This is the v1 widened schema done right — the catch-all captures
    unknowns, so there is no `archive_extras_since` re-crawl trap.
- **Sizing (measured, not napkin'd):** v1's live archive is **297.828 GiB across 4,958
  objects ≈ ~123 B/post** for the blended schema (well under the retro's 200 B/post
  *projection* — zstd over-delivered). A clean v2 full-network run (~3B posts) at
  ~130–150 B/post projects **~420 GB → fits 1 TB with ~580 GB headroom.** Live archiving
  adds ~200–400 GB/year, so 1 TB lasts ~2–3 years before a rotate/upsize. The **canary
  re-measures exact v2 bytes/post against 1 TB before the full run.**
- **Capture signals, filter nothing.** Self-labels, account status, and opt-out flags
  (`!no-unauthenticated`) are captured as metadata columns — but they are **never used to
  filter** (see privacy posture below).

---

## ClickHouse: a derived serving projection

- **The crawler writes Parquet only.** ClickHouse emoji rows + aggregates are **derived
  from the archive**, by a dedicated, paced **derive pool** that runs **concurrent with the
  crawl**: it tails the archive manifest, recomputes-from-Parquet to verify against the
  receipt, and bulk-loads CH. So CH fills *during* the crawl, and time-to-100%-website ≈
  crawl time + a short tail. The derive pool is **sized to keep pace** with the 8 crawlers
  (else it becomes the long pole).
- **One controlled writer-class into CH** (the paced derive pool), not 8 crawlers racing
  between fetches. ClickHouse is a **rebuildable projection** — re-derive from Parquet
  anytime; the truth was never in CH.
- **Emoji schema = v1's shape:** glyph-string keys (`LowCardinality(String)`), **no integer
  `emoji_dim`** (a glyph↔id bijection is a drift-prone sync point for marginal gain over the
  dictionary encoding CH already does). `langs` stays in `emoji_posts_v2` and the
  `emoji_*_by_lang` / `lang_total` aggregates — the language tabs are a shipped feature.
- **Total-post counter:** since non-emoji posts are not in CH, the "posts processed"
  counter and the emoji/total ratio come from a **separate total-post counter** — live
  ingest emits a total tick; backfill feeds it from the receipts' `posts_total`. Never
  `count()` over the emoji-only table.
- **Writer discipline:** async-insert / ~50k-row batches (no tiny frequent parts → no
  part-storm), **content-derived** `insert_deduplication_token` (not v1's
  length+boundary token, which the adversarial review flagged as collision-prone),
  off-thread **O(LIMIT)** telemetry (never the SUM-over-the-whole-ledger tick that wedged
  v1), staggered deploys (no lockstep thundering-herd).
- **Box sizing:** one CH VPS, **32 GB RAM during the backfill → 16 GB after.** On each
  resize, the `max_server_memory_usage` cap does **not** follow the box under NixOS — bump
  the config, **restart `clickhouse-server`**, and verify the **live** setting
  (`SELECT value FROM system.settings …`), not the config file. (v1 ran a rescaled box with
  a stale 5 GiB cap — "the 5 GiB cap that followed the box.")
- **Why 32 GB is safe where v1's 12 GiB cap was the constant villain:** v2's two
  CH-melting workloads are structurally gone — verify recomputes from Parquet (no `FINAL`
  scan over CH), there is no loose-convergence round, inserts are emoji-only (~⅕ the row
  volume), and HF-prep reads Parquet, not CH. Nothing heavy ever scans the serving box.

### Greenfield serving & cutover

There is no prod/v1 to cut over from, so **no shadow tables, no dual-write, no swap.** The
site comes up fresh on the v2 tables. Backfill (derive) and live ingest both write the
*same* tables; overlap collapses via **`ReplacingMergeTree(did, rkey)`** (no double-count).
Aggregates are MV-fed during the run (approximate, over-counting duplicate arrivals) then
**rebuilt from the deduped `emoji_posts_v2` at completion** for exact serving numbers
("every aggregate is a rebuildable cache"). **"100% website" = crawl + derive + verify +
rebuild aggregates, then expose.** Rollback = re-derive from Parquet.

---

## Emoji normalization

There is **one normalizer, written in Rust, compiled to WASM for the JS live ingest path.**
Single source of truth → **zero parity drift by construction.** This eliminates the
silent-corruption surface where Rust backfill and JS live could encode the same glyph
differently and split its count into two buckets forever. (It also turns the original
"PR3 normalizer parity" from an endless fixture cross-product chase into "build the shared
crate + a WASM binding.")

---

## Observability & status

- **Two auto-healing, progress-gated watchdogs** — the retro's #1 lesson. Liveness =
  **work counters advancing**, never CPU/log-freshness/unit-state (all of which lie):
  - **crawler** keys on **`archived`** advancing;
  - **derive pool** keys on **`loaded`** advancing.
  Each **restarts a confirmed-wedged process itself** (active auto-heal, not alert-only —
  the failures come at 3am), with a cooldown to avoid crash-loops. Layered over the
  fetch-level self-driven inactivity timeout and the AIMD host cool/park.
- **Status = authoritative-on-demand + labeled live progress** — the cure for v1's nine
  dashboard lies:
  - **Authoritative** numbers (loaded/verified/complete) are computed **on demand from
    ground truth** by **one component that owns the 8-shard ledger + manifest + receipt
    join.**
  - **Live progress** (throughput, heartbeats) is lightweight pushed telemetry, **explicitly
    labeled "live, may lag"** so it can never masquerade as authoritative.
  - Guardrails baked in: scope to **project lifetime, never "latest run_id"**; **one
    definition per metric** across all views; **bounded time windows + the right timestamp
    column named honestly** (`ingested_at` vs `created_at`; upper-bound windows because the
    network contains future-dated posts).

---

## The canary (launch gate)

8 boxes are provisioned up front, but the crawl is gated by a **one-box, full-pipeline,
hard-gated canary** that runs first; only on green do we fan out to 8 (the retro's "get one
box to a boring steady state, then multiply"). It runs on enough repos to fill **≥1 full
monthly partition**, then `OPTIMIZE` + re-measure so projections are post-merge steady
state. It must prove, as **hard go/no-go thresholds**:

- archive bytes/post (real v2 output) projected to full network **< 1 TB** with headroom;
- ClickHouse size (incl. the `*_by_lang` aggregates — the real growth vector) **< box disk**;
- **the derive pool keeps pace with the crawl** (else it is the long pole to 100%-website);
- **an injected single-post drop is DETECTED** (verify the verifier — v1's false-pass
  promoted ~2M repos it never classified);
- a **wall-clock projection** from sustained measured throughput on healthy software (the
  only ETA worth quoting), saturating one real mushroom's per-IP budget without 429-storm;
- a whale spools→parses→discards cleanly, and a malformed CAR classifies loud.

Fail any → fix before committing all 8.

---

## HuggingFace publication

A **post-backfill packaging job**, consistent with "Parquet is truth, everything else is a
projection":

- the archive stays **immutable, at-least-once, append-only** (the truth);
- a packaging pass (DuckDB/Rust over the archive) produces the **published copy**:
  **dedup** `DISTINCT ON (did, rkey)` latest-by-`ingested_at` (the at-least-once dups
  collapse here), **repartition** into clean, consistently-sized Parquet shards
  **partitioned by `created_at` month** (HF-idiomatic), and emit a **dataset card**
  (provenance, snapshot date, the flat-columns + `extras`-JSON schema, the
  *cumulative-ever / no-deletes / no-opt-out-filtering* caveat stated plainly, license);
- upload to HuggingFace. The published set is a **reproducible projection**; it never
  mutates the truth.

---

## Jacquard: scope & responsibilities

Use **Jacquard v0.12.0** — but **scoped to its primitives, never its high-level client.**
Pin an exact version and **vendor / fork-mirror** it (single maintainer, pre-1.0,
fast-moving, primary repo on tangled.org).

**Use Jacquard for:** the generated `jacquard-api` request/record/error types
(getRepo/listRepos/listRecords/describeRepo/feed.post/actor.profile), its per-endpoint error
enums (`RepoNotFound`/`Takendown`/`Suspended`/`Deactivated`), the `jacquard-repo` CAR/MST
codec, and the streaming `client.xrpc(base).download()` seam (which exposes raw headers on
every response + a chunk-level `ByteStream`).

**Do NOT use** the high-level `Agent`/`send()` path — it discards rate-limit headers,
buffers the whole CAR into memory, and collapses errors into an opaque type.

**Hand-roll (the load-bearing layer):**

- own `reqwest::Client` + `HttpClient` impl → per-host pacing + read `ratelimit-*` off every
  response (success, 429, 4xx);
- the **self-driven inactivity timeout** (`tokio::time::timeout` per chunk → drop the
  stream; `AbortSignal`-style deadlines do not interrupt a half-open socket);
- the **error → ledger-state classifier** (account-level states arrive via the
  `Other{error,message}` catch-all → string-match; 429/timeout/5xx off status, since
  `ClientErrorKind` is too coarse);
- the **on-disk `BlockStore` + `MstCursor` walk** (the built-in `Repository` is in-RAM);
- the **PLC bulk export** (Jacquard resolves one DID at a time only).

---

## Privacy / consent posture

Deliberate, informed call (exposure accepted): **no delete handling, ever; no
freshness/reconciliation pass; no `!no-unauthenticated`/opt-out honoring.** The published
HuggingFace dataset is the **full raw cumulative-ever snapshot.** Signals (self-labels,
account status, opt-out flags) are **captured as metadata but never used to filter.** The
dataset card states the cumulative-ever / no-filtering nature plainly.

---

## Out of scope

Post delete handling; post tombstone repair; avatar/banner blob downloads; full profile
search in ClickHouse; per-account `describeRepo` during the main crawl (except the Bridgy
capability probe); old v1 data migration (clean re-crawl); using ClickHouse as the full raw
post store. Profiles are acceptable only as a cheap sidecar from the same repo fetch
(`app.bsky.actor.profile/self` → profiles archive only), no extra media fetches, no
mandatory handle-enrichment crawl.

---

## Operational invariants carried from v1 (the retro's checklist)

- **Everything load-bearing lives in the NixOS/pix flake** — no ad-hoc host scripts or
  `/run`-only drop-ins that the next rebuild silently erases.
- **Deploy via git with rev verification**, not rsync (rsync caused silent version skew).
- **A scriptable fleet-ops entrypoint** (start a shard, change concurrency, revive a host)
  so a brand-new Rust binary does not reset the "operational re-derivation" footgun — the
  agent/operator calls one command, not a 6-parameter SSH invocation from memory.
- **Dead-host registry ships with its inverse** (`--revive-host`) — blacklisting without an
  un-blacklist means "park" silently becomes "abandon."
- **WAL-safe ledger backup:** stop crawler → SQLite `checkpoint(TRUNCATE)` → `.backup` →
  copy. A raw `cp` of a WAL-mode SQLite file can miss committed pages.
- **The claim path must be O(LIMIT), not O(n)** over a growing ledger (the recurring v1
  villain). Keep "is this host claimable" separate from "should this request wait" (pacing
  vs deadness conflated in the claim scan starved v1's scheduler for a 9-hour overnight).
- **ETAs only from sustained measured throughput on healthy software**; report posts/min
  next to repos/min so "stopped wasting" and "got faster" stay distinguishable.

---

## Open

**PR sequencing is deliberately deferred** ("decide per-item later"). The retro's top-5
time-savers (progress-gated watchdog, header pacing with the claimable-vs-wait split,
no-silent-caps, scalable Parquet-recompute verify, early `listrepos-diff` census) and the
checklist above are to be placed into the PR order item-by-item when build planning starts.
The first executable milestone is `fetch-one` → the canary; only on a green canary do we
fan out to 8 boxes.

---

## Provenance

Compiled 2026-06-15 from: a multi-agent adversarial critique of the original v2 plan against
the v1 codebase; a full read of [`docs/retro.md`](retro.md); a vetting of the Jacquard crate
against its docs.rs/source; and a decision-by-decision grilling session. The earlier plan's
biggest gaps that this record corrects: the verification model (emoji-only-in-CH had
silently broken v1's set-based completeness check — now an MST-root proof), the
storage-saving rationale (the real lever is dropping non-emoji rows, not text), the language
column (kept — the public site depends on it), and "no v1 migration" wastefulness (accepted
deliberately as a clean re-crawl with census-first junk deletion).
