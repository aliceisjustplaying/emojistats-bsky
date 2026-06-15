# Emojistats Backfill v2 — Design & Decision Record

> **Status:** Architecture direction accepted, pre-implementation. Public release policy
> and exact canary thresholds remain pending until explicitly resolved. This document is
> the implementation source of truth after applying the 2026-06-15 review-packet
> corrections.
>
> v2 is a clean **Rust rewrite** of the v1 TypeScript backfill (`packages/backfill`). v1
> already crawled ~35.89M repos / ~2.59B posts; the retro's thesis is that **the data was
> never the hard part**. The time went to telling a stuck system from a slow one, silent
> caps, reactive pacing, and an unscalable verifier. v2 exists to make the learned
> invariants structural: state machines, receipts, loud caps, progress-gated watchdogs,
> and tests.

---

## Purpose & win condition

Re-backfill the full Bluesky network and stand up the public emoji-stats site on the
result, **greenfield** (treat v1/prod as not existing). The products are:

- the public **ClickHouse-served emoji site**;
- a private **Raw Archive** on Storage Box, used as operational truth;
- a candidate **Published Raw Observed Corpus** containing raw post text and full record
  extras from backfill snapshots, with final public filtering/release policy decided
  separately before publication.

The win condition is a **100% Website** in under 24 hours: backfill crawl complete,
archive derive complete, Jetstream Catch-Up at live tail, final aggregate rebuild complete,
then expose the site. The crawl phase should be bounded primarily by mushroom PDS per-IP
rate limits **after** the canary proves parser, archive sync, derive, ClickHouse, and
aggregate rebuild are not the long pole.

### Tiebreaker

When decisions conflict: **correctness > operability > performance > craft**. Performance
means no self-inflicted ceilings: saturate advertised host budgets, avoid scheduler
freezes, avoid O(n) claim paths, and measure before claiming speed.

---

## Pipeline overview

```
enumerate: PLC export + did:web best-effort seeds
  -> CENSUS: listRepos each real PDS (never aggregators) with absence guardrails
  -> CANARY: stratified full-pipeline gate
  -> FAN-OUT: 8 boxes, uniform DID-hash, header-paced per IP
       optional/local Jetstream gap protection starts before retention is at risk
  -> CRAWL:
       getRepo via Jacquard download() + own reqwest HttpClient
       -> spool CAR to local disk under Loud Resource Caps
       -> parse: on-disk BlockStore + MST walk
       -> VERIFY: Snapshot Completeness, not identity/authorship
       -> write Parquet + row-content receipt + committed manifest to Storage Box
       -> discard CAR
  || DERIVE POOL:
       read committed manifest entries
       -> recompute Parquet row hashes vs receipts
       -> bulk-load compact emoji serving rows + total counters to ClickHouse
  -> JETSTREAM CATCH-UP:
       replay from backfill_started_at - 4h to live tail
       -> write directly to ClickHouse serving projection, not the Raw Archive
  -> rebuild aggregates from deduped serving rows
  -> SITE LIVE
  -> PACKAGE candidate Published Raw Observed Corpus from Raw Archive
```

The crawler's only durable output is Parquet plus receipts/manifests. ClickHouse is a
derived, rebuildable serving projection. Jetstream Catch-Up is for the serving site only;
it is not part of the Raw Archive or the Published Raw Observed Corpus.

---

## Scaling & fetch model

- **Rate limits are per-IP.** `getRepo` is on the unauthenticated `com.atproto.sync`
  namespace and Bluesky-hosted reads are rate-limited by client IP. N boxes can buy real
  throughput against mushroom hosts.
- **Rate-limit conforming, header-paced, assertive.** Each box paces to the full
  advertised per-IP budget using `ratelimit-*` / `x-ratelimit-*` headers. AIMD, 429
  backoff, stalls, and explicit host overrides can clamp that budget.
- **Sharding: uniform DID-hash across all 8 boxes**, persisted in the ledger's bucket
  column. The modulus is pinned with a constructor guard; resharding is a ledger migration,
  not a config flip. No whole-host assignment.
- **Mushrooms** (`*.bsky.network` and `bsky.social`) get a dynamic cap derived from
  advertised rate-limit headers, roughly 60 seconds of advertised queue depth, capped.
  `bsky.social` belongs in this regime: v1 misclassifying it as third-party cost a 70-day
  ETA.
- **Indie / third-party PDSes** get conservative defaults plus the same header pacing and
  backoff. Small hosts should drain politely, and host-specific incidents must not require
  a global fleet stop.
- **Host override mechanism required.** It can be a SQLite table, Nix option, CLI-managed
  table, or checked-in config; it does not need to be a separate TOML file. It must support
  disabling a host, capping host concurrency, changing a minimum interval, forcing
  `getRepo`/`listRecords` mode, reviving a host, and marking aggregators as never-diff.
- **Bridgy / capability-variant PDSes:** `getRepo` can return HTTP 429 for a permanent
  method wall. Carry a capability probe and a `getRepo` -> `listRecords` fallback, but
  label fallback output as **Collection-Paginated Record** data with weaker proof.

---

## Census & identity coverage

- **PLC export is the main enumerable identity source.**
- **DID Web Coverage is best-effort:** known PDS `listRepos`, live observations, and manual
  seeds. Do not claim a global `did:web` census.
- **Never diff aggregators.** A relay/listing aggregator in a PDS list is the deepest,
  most expensive possible false tail.
- **`listRepos` absence is terminal only after guardrails pass.** The account's current
  DID document/PLC entry must point at that host, the host listing must complete all pages
  without error, cursor/repo-count sanity checks must pass, the host must not be in a
  degraded/partial window, and the account must not look recently migrated. A suspect
  listing parks/retries; it does not delete work.

---

## Repo parse & verification

- **Spool -> parse -> verify -> archive -> discard.** Stream the `getRepo` CAR to local
  disk, parse from disk, verify, archive Parquet + receipts, then discard the CAR. Local
  disk is bounded by concurrent in-flight CARs and is the backpressure knob for fetch
  concurrency.
- **Loud Resource Caps, not no caps.** v1's worst bug dropped every repo over a 1 GiB CAR
  cap under a status nobody reexamined. v2 has no silent content cap. Every local disk,
  single-repo size, CAR block size/count, record count, MST depth, wall-clock, idle,
  parse-progress, and upload-progress limit produces an explicit status, metric, and
  recovery/operator-action path.
- **Parsing uses an on-disk BlockStore + MST walk.** Jacquard's built-in `Repository` is an
  in-RAM `BTreeMap` and cannot hold multi-GB whales. v2 implements an on-disk BlockStore
  over the spooled CAR and drives the MST cursor over it. The MST walk is required because
  rkeys live in MST leaf keys, and the verification proof needs the reconstructed tree.
- **Snapshot Completeness only.** Given a validated commit block and validated MST
  traversal from `commit.data`, if every reachable node and record block resolves by CID
  and the reconstructed root CID equals `commit.data`, then the CAR contains a complete,
  self-consistent repo snapshot for that commit. This is not an authorship or identity
  proof.
- **Signature and identity verification are separate fields.** By default:
  `completeness_verified = true`, `repo_commit_signature_verified = false`,
  `identity_verified = false`. If signature sampling or suspicious-host verification is
  added later, receipts record exactly what was checked.
- **Root mismatch, missing block, invalid MST, malformed CAR, or resource exhaustion never
  silently pass.** They produce loud terminal or operator-action statuses.
- **No LOOSE band for root-proofed archive verification.** The old v1 loose band existed
  because ClickHouse and live/backfill overlap could not prove set-subset. v2 archive
  verification is per repo from the CAR/MST and then from Parquet receipts. `listRecords`
  fallback and serving projection dedupe have separate proof classes.

---

## Receipts

Per-repo receipts are computed while parsing and recomputed from synced Parquet before
derive marks data loadable.

Minimum receipt fields:

- `fetch_method = getRepo | listRecords`
- `completeness_class = snapshot_complete | collection_paginated`
- `all_records_count`
- `all_posts_count`
- `emoji_posts_count`
- `emoji_occurrences_count`
- `mst_root_cid`, nullable for `listRecords`
- `commit_cid`, nullable for `listRecords`
- `archive_rows_hash`
- `post_rows_hash`
- `emoji_projection_hash`
- `profile_row_hash`, nullable
- `normalizer_name`
- `normalizer_semver`
- `normalizer_git_rev`
- `unicode_version`
- `emoji_data_version`
- signature/identity verification booleans

The row hash is an ordered content hash over the single collection `app.bsky.feed.post`:
`did`, `rkey`, `cid`, normalized timestamp fields, text, languages, emoji extraction output,
and canonical extras JSON. `collection` is recorded once as the collection constant; this
system only ingests `app.bsky.feed.post`.

---

## Storage & the Raw Archive

- **All backfill snapshot posts -> zstd Parquet on a 1 TB Hetzner Storage Box.** This is
  the private Raw Archive and source for candidate public packaging. ClickHouse holds only
  the serving projection.
- **Schema is Data-Model Lossless, not byte-lossless.** It preserves ATProto data-model
  fields after normalization into typed columns and canonical extras JSON. It does not
  promise byte-for-byte reconstruction of the original CBOR/CAR encoding.
- **Core post columns:** `did`, `rkey`, `cid`, `created_at_raw`,
  `created_at_normalized`, `created_at_parse_status`, `text`, `langs`, emoji-derived
  fields, normalizer version fields, account/status/public-content labels, and
  `extras_json`.
- **Canonical extras JSON** contains facets, reply refs, embeds, self-labels, tags, and
  future lexicon fields that are not modeled as typed columns. Known flat fields are not
  duplicated inside extras.
- **Created-at partitions are explicit:** `created_month = yyyy-mm | unknown | invalid |
  future`. Bad timestamps do not create nonsense public partitions or false time precision.
- **Profile Sidecar:** capture `app.bsky.actor.profile/self` from the same repo fetch when
  present. No avatar/banner blob downloads, no handle guarantee, no profile search in
  ClickHouse. Candidate public inclusion follows the publication policy review.
- **Sizing:** v1's live archive measured ~297.828 GiB across 4,958 objects, about
  ~123 B/post for the blended schema. A clean v2 run must remeasure exact bytes/post in the
  stratified canary and prove 1 TB has headroom before fan-out.

### Storage Box committed manifest

Storage Box is treated as durable file storage, not a transactional object store. Derive
reads only committed manifest entries.

Commit protocol:

1. write local temp Parquet;
2. fsync local temp;
3. rename local temp to local finalized path;
4. compute row count, byte count, and content hash;
5. upload remote temp path;
6. verify remote size/hash, either by readback or checksum sidecar chosen by canary;
7. remote rename temp to final object path;
8. append manifest entry only after the final object exists.

Manifest entries include run ID, shard, file sequence, dataset, remote path, row count,
bytes, content hash, min/max normalized timestamp, receipt hash, normalizer version, and
schema version.

---

## ClickHouse: derived serving projection

- **The crawler writes Parquet only.** ClickHouse emoji rows and total counters are derived
  from the Raw Archive by a paced derive pool. The derive pool tails committed manifest
  entries, recomputes Parquet row hashes against receipts, and bulk-loads compact serving
  rows.
- **ClickHouse omits CID.** Parquet and receipts carry CID. The serving projection uses
  `(did, rkey)` for dedupe because it is not the raw corpus.
- **Emoji schema keeps v1's serving shape:** glyph-string keys (`LowCardinality(String)`),
  no integer `emoji_dim` unless a canary proves strings are a real storage problem, and
  `langs` remains in the serving rows and language aggregates.
- **Total-post counter:** non-emoji posts are not in the emoji table, so posts processed and
  emoji/total ratio come from a separate total-post counter fed by receipts and live
  Jetstream ticks. Never derive it from the emoji-only table.
- **Insert dedupe tokens are an optimization.** Correctness comes from receipt hashes,
  derive batch ledgers, idempotent manifest processing, ReplacingMergeTree dedupe, and final
  aggregate rebuilds. The system must tolerate replay after ClickHouse's dedupe window
  expires.
- **Backfill and Jetstream do not overlap in the archive.** Backfill derives from Raw
  Archive. Jetstream Catch-Up later writes directly to the serving projection.
- **Aggregate MVs may overcount duplicate arrivals.** That is acceptable before public
  launch because a final aggregate rebuild runs after Jetstream Catch-Up reaches live tail.
  Public launch waits for that rebuild.
- **Aggregate rebuild is a measured launch gate.** It scans deduped serving rows, not the
  raw corpus. Canary must measure projected rebuild time; minutes to low hours is acceptable
  and anything threatening the 24-hour goal requires changing aggregate strategy before
  fan-out.
- **Box sizing:** one ClickHouse VPS, 32 GB RAM during the backfill and 16 GB after if
  measured safe. Under NixOS, resizing the box does not automatically update the live
  `max_server_memory_usage`; bump config, restart `clickhouse-server`, and verify the live
  setting with SQL.

---

## Jetstream Catch-Up and launch

The public site does not launch from a backfill-only state. After the backfill and archive
derive complete, Jetstream Catch-Up replays `app.bsky.feed.post` creates from
`backfill_started_at - 4h` to live tail, writing directly to ClickHouse.

Public Jetstream retention was spot-checked at about 36 hours on 2026-06-15. The target run
is 24 hours, but retention is an operational dependency, not a proof. Starting a local
Jetstream server only after old events age out cannot recover the gap. For a gap-free
launch, one of these must be true:

- a local Jetstream/spooler starts before fan-out or before the official retention window
  can no longer cover `backfill_started_at - 4h`; or
- the project explicitly downgrades catch-up after that point to best-effort and blocks
  "100% Website" wording until the gap is accepted.

The earlier "start local Jetstream after 24h slip" rule is only safe if it also starts a
consumer that drains public Jetstream from `backfill_started_at - 4h` before those events
expire. The canary/run monitor must project completion against the retention deadline and
trigger that path before coverage is lost.

---

## Published Raw Observed Corpus

The Published Raw Observed Corpus is a candidate public dataset produced from the Raw
Archive. It is **backfill snapshot only**; Jetstream Catch-Up rows are not archived and are
not part of this corpus.

Semantics:

- observed repository records present at crawl time;
- no reconstruction of records deleted before the crawl;
- post delete events are not observed by this system;
- raw post text plus full record extras are included in the candidate corpus;
- public filtering/release policy is decided separately before publication.

Packaging outputs:

- `published_raw_observed_records/`
  - identity: `did`, `rkey`, `cid`;
  - duplicate retry rows collapse only when `did/rkey/cid/content_hash` match;
  - partition by explicit `created_month`, including `unknown`, `invalid`, and `future`.
- optional `published_latest_snapshot/`
  - identity: `did`, `rkey`;
  - latest chosen by explicit snapshot/ingested ordering;
  - clearly labeled as a projection, not the raw observed record corpus.

Do not call the corpus cumulative-ever. Do not silently collapse different CIDs in the raw
observed record product.

---

## Publication / consent posture

The Raw Archive is private operational truth. The Published Raw Observed Corpus is a
candidate public dataset. Release policy is a separate decision recorded in
[`docs/adr/0001-raw-archive-and-public-corpus-boundary.md`](adr/0001-raw-archive-and-public-corpus-boundary.md).

Captured signals such as self-labels, account status, and `!no-unauthenticated` are stored
as metadata. Whether those signals filter the public corpus is not decided by crawler code,
derive code, packaging scripts, or dataset-card defaults.

---

## Emoji normalization

There is one normalizer, written in Rust. If JS live paths remain, they use the Rust
normalizer via WASM.

Every output that depends on normalization records:

- `normalizer_name`
- `normalizer_semver`
- `normalizer_git_rev`
- `unicode_version`
- `emoji_data_version`

Mixed normalizer versions within a run are rejected unless explicitly allowed.

---

## Observability & status

- **Progress-gated watchdogs are required from day one.** Liveness is work advancing, not
  CPU, log freshness, or unit state.
- **Crawler progress vector:** `bytes_downloaded`, `chunks_received`, `repos_fetched`,
  `records_parsed`, `parquet_rows_written`, `local_files_finalized`,
  `remote_files_committed`, `ledger_terminal_transitions`, and `ledger_loaded_transitions`.
- **Derive progress vector:** `manifest_entries_seen`, `files_read`,
  `rows_verified_against_receipts`, `clickhouse_batches_committed`,
  `aggregate_batches_committed`, and `repo_receipts_loaded`.
- **Restart only when none of the relevant counters advances** and the process is not
  intentionally sleeping on host pacing, disk pressure, Storage Box backpressure, or other
  declared pressure state.
- **Authoritative status comes from committed snapshots.** Crawler local SQLite ledgers are
  shard-local truth, but the status service reads exported, hash-checked ledger summaries,
  committed manifests, and receipts from Storage Box. "Authoritative" means authoritative
  as of committed snapshot generation N.
- **Exact final numbers require a drain point:** pause/drain crawlers, checkpoint WAL,
  export ledger snapshots, and join immutable files.
- **Live progress is labeled live.** Throughput and heartbeats are pushed telemetry and must
  never masquerade as authoritative counts.
- Dashboard metrics are scoped to project lifetime, not latest `run_id`; each metric has one
  definition across views; time windows are bounded and use the timestamp column named in the
  label.

---

## The stratified canary

8 boxes can be provisioned up front, but fan-out is gated by a one-box, full-pipeline,
hard-gated **Stratified Canary**. It exercises representative normal and edge-case repo
populations plus failure injection.

Canary coverage:

- normal random sample;
- recent high-volume mushroom sample;
- old-month sample;
- invalid/missing/future `created_at` sample;
- top-N largest repos / whales;
- top-N emoji-heavy repos;
- third-party PDS sample;
- Bridgy / capability-variant sample;
- `did:web` sample;
- malformed CAR fixture;
- missing-block / invalid-MST fixture;
- injected single-post drop;
- partial remote upload / manifest corruption injection;
- ClickHouse duplicate-insert injection;
- short 8-box low-volume contention test.

Hard gates:

- archive bytes/post projected to full network fits Storage Box with headroom;
- ClickHouse serving projection and aggregate tables fit the serving box;
- derive keeps pace with crawl;
- receipt recomputation detects injected row loss/content corruption;
- Storage Box committed-manifest protocol detects partial upload;
- wall-clock projection comes from sustained healthy throughput;
- one real mushroom per-IP budget is saturated without a 429 storm;
- whale spools, parses, archives, and discards cleanly;
- malformed/invalid/resource-exhausted repos classify loudly;
- final aggregate rebuild projects to minutes or low hours, not a material threat to the
  24-hour goal.

Fail any gate -> fix before all-8 fan-out.

---

## Jacquard: scope & responsibilities

Use Jacquard v0.12.0, scoped to primitives rather than the high-level client. Pin an exact
version and vendor/fork-mirror it.

Use Jacquard for generated API/record/error types, per-endpoint error enums, CAR/MST codec,
and the streaming `client.xrpc(base).download()` seam.

Hand-roll the load-bearing layer:

- own `reqwest::Client` + `HttpClient` implementation;
- per-host pacing and header parsing on success, 429, and terminal responses;
- self-driven inactivity timeouts per chunk;
- error-to-ledger-state classifier;
- on-disk BlockStore + MST walk;
- PLC bulk export;
- host override control surface;
- Storage Box manifest committer.

Do not use high-level `Agent`/`send()` paths for load-bearing fetches because they buffer,
hide rate-limit headers, and collapse errors too aggressively.

---

## Out of scope

Post delete handling; post tombstone repair; avatar/banner blob downloads; full profile
search in ClickHouse; mandatory handle-enrichment crawl; old v1 data migration; using
ClickHouse as the full raw post store; byte-for-byte CAR preservation after successful
archive receipt.

`listRecords` fallback is in scope for serving projection only. It is out of scope for
Snapshot Completeness and root-proofed raw corpus counts.

---

## Operational invariants carried from v1

- **pix** means the private NixOS infrastructure flake that deploys prod and crawler
  machines.
- Everything load-bearing lives in the NixOS/pix flake or the repo's scripted operational
  entrypoints. No ad-hoc host scripts or `/run`-only drop-ins.
- Deploy via git with revision verification, not rsync.
- A scriptable fleet-ops entrypoint starts shards, changes concurrency, revives hosts,
  starts local Jetstream/spoolers, and checks health. Agents and humans call one command.
- Host blacklisting ships with its inverse (`--revive-host`) and scoped reset.
- WAL-safe ledger backup: stop crawler -> SQLite `checkpoint(TRUNCATE)` -> `.backup` ->
  copy.
- The claim path must be O(LIMIT), not O(n). Keep "is this host claimable" separate from
  "should this request wait."
- ETAs come only from sustained measured throughput on healthy software. Report posts/min
  next to repos/min.
- Settings that failed belong in the runbook so the next operator does not retry them.

---

## First implementation milestone

The Rust rewrite starts with one vertical slice, not a full fleet scheduler:

```bash
emojistats-backfill fetch-one did:plc:...
```

`fetch-one` must:

- resolve DID/PDS;
- fetch `getRepo` with bounded streaming and rate-limit header capture;
- spool CAR to disk under Loud Resource Caps;
- parse through the on-disk BlockStore + MST walk;
- extract posts;
- extract profile sidecar if present;
- compute Snapshot Completeness;
- compute canonical row-content receipt;
- write local Parquet;
- write local manifest entry;
- derive compact emoji rows locally or into scratch ClickHouse.

If `fetch-one` works, continue to ledger + committed manifest protocol, then derive,
then canary, then fleet scheduling and NixOS rollout. If `fetch-one` turns into weeks of
abstraction, stop and reassess.

---

## Open before coding beyond `fetch-one`

- exact canary thresholds;
- exact local Jetstream/spooler operating mode for guaranteed catch-up;
- exact public corpus release/filtering policy;
- exact host override storage surface;
- final row schemas for Raw Archive, receipts, manifests, derive batches, and serving
  projection.

---

## Provenance

Compiled 2026-06-15 from: the original v2 design, a full read of
[`docs/retro.md`](retro.md), the second-round critique, the resolved review packet,
[`CONTEXT.md`](../CONTEXT.md), and ADR 0001. The major corrections are: Raw Archive vs
candidate public corpus, Observed Corpus not cumulative-ever, Snapshot Completeness not
authorship, row-content receipts, Loud Resource Caps, committed manifests, post-backfill
Jetstream Catch-Up, stratified canary, best-effort `did:web`, guarded `listRepos`
absence, host override control, and a vertical-slice-first Rust rewrite.
