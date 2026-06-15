# Rust Backfill Review Bundle

_Generated 2026-06-15 from branch `v2-rust-backfill` at `7d76e64`._

This file concatenates context, decisions/docs, implementation notes, and Rust source/config for review.

## Included Files

- `CONTEXT.md`
- `docs/adr/0001-raw-archive-and-public-corpus-boundary.md`
- `docs/backfill-v2-design.md`
- `rust/NOTES.md`
- `rust/Cargo.toml`
- `rust/crates/emojistats-backfill/Cargo.toml`
- `rust/crates/emojistats-backfill/src/lib.rs`
- `rust/crates/emojistats-backfill/src/main.rs`
- `rust/crates/emojistats-backfill/src/transport.rs`
- `rust/crates/emojistats-backfill/src/parse.rs`
- `rust/crates/emojistats-backfill/src/archive.rs`
- `rust/check.sh`
- `rust/clippy.toml`
- `rust/deny.toml`
- `rust/rustfmt.toml`


---

## `CONTEXT.md`

```markdown
# Emojistats

Emojistats measures emoji usage across Bluesky posts and publishes backfill-derived data products.

## Language

**Raw Archive**:
The private corpus of crawled post records retained as operational truth. It can include raw text, record extras, and provenance that are not necessarily safe for public redistribution.
_Avoid_: HuggingFace truth, public archive

**Published Raw Observed Corpus**:
The candidate public raw-text dataset produced from repository snapshots captured by the backfill. It includes raw post text and full record extras, with final filtering and release policy decided separately before publication.
_Avoid_: cumulative-ever snapshot, emoji-derived corpus

**Observed Corpus**:
A corpus made from repository records present at crawl time. It does not reconstruct records deleted before observation, and post delete events are not observed by this system.
_Avoid_: cumulative-ever corpus, full history

**DID Web Coverage**:
Best-effort coverage of `did:web` accounts discovered from known PDS listings, live observations, and manual seeds. It is not a guaranteed global census.
_Avoid_: complete did:web crawl

**Live Observed Post**:
A post record observed from the live stream after the live watermark. In the current product this means created posts only; it is used for the serving site, not for the Published Raw Observed Corpus.
_Avoid_: live mutation, delete event

**Jetstream Catch-Up**:
The serving-site catch-up phase that starts after the backfill and replays Jetstream from four hours before the backfill start time. It writes directly to the serving projection and is not part of the raw corpus.
_Avoid_: live/backfill overlap, dual-write

**Local Jetstream Fallback**:
A self-operated Jetstream server or spooler used when public Jetstream retention is not enough to guarantee catch-up. For a gap-free launch it must start before the public retention window can no longer cover the backfill rewind point.
_Avoid_: late-only fallback, mandatory live overlap

**Stratified Canary**:
The pre-fan-out test run that exercises representative normal and edge-case repository populations, storage publication, derive, ClickHouse, and failure injection. It must measure the launch-critical timings and sizes before the fleet run.
_Avoid_: monthly sample only, smoke test

**Record Extras**:
The non-core fields from a post record that are preserved alongside raw text, such as facets, reply references, embeds, self-labels, tags, and future lexicon fields.
_Avoid_: lossless JSON, blob

**Profile Sidecar**:
Profile metadata captured from `app.bsky.actor.profile/self` during the same repository fetch as posts. It does not imply handle verification, media fetching, or ClickHouse profile search.
_Avoid_: profile index, handle crawl

**Data-Model Lossless**:
Preservation of the post record's ATProto data-model fields after normalization into typed columns and canonical extras JSON. It does not promise byte-for-byte reconstruction of the original CBOR encoding.
_Avoid_: byte-lossless, CAR-lossless

**Normalizer Version**:
The version identity of the emoji normalization logic used to produce rows, including code revision and emoji data version. It travels with archive and serving outputs so mixed normalization can be detected.
_Avoid_: implicit normalizer, JS/Rust parity note

**Created-At Parse Status**:
The classification of a post record's author-supplied timestamp after parsing and normalization. It distinguishes valid, missing, invalid, and future timestamps so corpus partitions do not imply false time precision.
_Avoid_: created_at truth

**Snapshot Completeness**:
The claim that a fetched repository export contains a complete, self-consistent snapshot reachable from the exported commit data root. It is separate from signature verification and identity verification.
_Avoid_: authorship proof, identity proof

**Loud Resource Cap**:
A resource limit that rejects or pauses work only with an explicit status, metric, and recovery path. It prevents silent content loss while admitting that disk, time, parser, and upload limits are real.
_Avoid_: silent cap, no cap

**Collection-Paginated Record**:
A record fetched through paginated collection APIs when a full repository export is unavailable. It can support the serving projection but does not carry the Snapshot Completeness claim.
_Avoid_: root-proofed record, repo snapshot

**Observed Record Identity**:
The identity of a raw observed record in the archive and candidate public corpus, made from DID, record key, and CID. The serving emoji projection may use a smaller identity because it is not the raw corpus.
_Avoid_: rkey-only identity

**Receipt Row Hash**:
An ordered content hash over each archived post row's DID, record key, CID, normalized timestamp, text, languages, emoji extraction output, and canonical extras. It proves archived row content, not just key presence.
_Avoid_: rkey digest, count-only receipt

**Serving Emoji Projection**:
The ClickHouse-backed subset used by the public emoji stats site. It is derived from the Raw Archive and optimized for serving counts, not for preserving every raw record field.
_Avoid_: archive truth, raw corpus

**100% Website**:
The public site state after the backfill, archive derive, aggregate rebuild, and Jetstream Catch-Up have all completed. It excludes known launch gaps by definition.
_Avoid_: backfill-only site, partial launch

```

---

## `docs/adr/0001-raw-archive-and-public-corpus-boundary.md`

```markdown
# Raw Archive and Public Corpus Boundary

Backfill v2 keeps a private Raw Archive as operational truth and treats the Published Raw Observed Corpus as a candidate public dataset with raw text and full record extras. The public release policy, including any filtering for account status, public-content restrictions, takedowns, or legal and research-ethics concerns, is decided separately before publication. This keeps the crawler/archive design complete while preventing implementation code from silently deciding publication policy.

```

---

## `docs/backfill-v2-design.md`

```markdown
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

```

---

## `rust/NOTES.md`

```markdown
# v2 backfill (rust/) — implementation notes & continuity

Working notes for the `emojistats-backfill` Rust rewrite. Design source of truth:
`../docs/backfill-v2-design.md`. This file is the implementation-level companion (API map,
roadmap, conventions) so a fresh session can continue without re-deriving.

## Status (2026-06-15)

- Branch `v2-rust-backfill` (not pushed). Greenfield; no v1 reuse.
- **Checkpoint A done:** `fetch-one <did>` resolves DID→PDS over the live network
  (`did:plc:z72i7hdynmk6r22z27h6tvur` → `puffball.us-east.host.bsky.network`); invalid DIDs
  error cleanly. Full muster green.
- **Checkpoint B/C/D done locally:** `fetch-one <did>` now resolves DID→PDS, streams
  `getRepo` to a spooled `CAR`, parses from the `CAR` path with block `CID` verification
  and `MST` completeness, writes `Parquet` posts, writes receipt + local manifest JSON, and
  derives compact emoji JSONL rows.
- Real stress DID verified:
  `did:plc:vwzwgnygau7ed7b7wt5ux7y2` from `shiitake.us-east.host.bsky.network` spooled
  41,051,855 bytes, produced 6,407 post rows, 228 emoji rows, and carried 23,656 typed
  record decode failures as non-fatal parse diagnostics.
- Jacquard 0.12.0 via **fork-mirror git deps**: `github.com/aliceisjustplaying/jacquard`
  @ `39648622522fa62c4c0b12ac22b8a5f6893c845a` (== tag 0.12.0). reqwest pulls **rustls**
  (no openssl). Full 0.12.0 source also at `/tmp/jacquard` for reading (ephemeral).
- Build/gate: `./check.sh` (cc is on PATH now). All tools installed.

## fetch-one vertical slice

- **A — identity:** `src/main.rs` resolves DID→PDS using `PublicResolver`.
- **B — transport:** `src/transport.rs` streams `com.atproto.sync.getRepo` with Jacquard's
  `download()` path, captures standard and legacy rate-limit headers, writes the response
  body to a deterministic spool path, enforces idle timeout + byte cap, and classifies
  account-state, HTTP, timeout, cap, transport, and I/O errors.
- **C — parse:** `src/parse.rs` reads only a `CAR` path, indexes blocks by `CID`, verifies
  bytes hash back to the advertised `CID`, stores block offsets over the spooled file,
  parses the commit, proves `MST` root equality, walks records, and extracts typed
  `app.bsky.feed.post` plus optional profile data. Typed record decode failures are
  diagnostics; malformed records do not abort a complete snapshot.
- **D — archive/derive:** `src/archive.rs` converts parsed posts to archive rows, computes
  row-content receipt hashes and counts, writes `Parquet` with flat lossless columns plus
  `extras_json`, writes a local manifest entry, and derives local compact emoji JSONL rows.

## Next roadmap

- Add crawler ledger state and retry/account-state transitions around `fetch-one`.
- Implement the Storage Box committed-manifest protocol: temp upload, verify, final rename,
  receipt sidecar, manifest append only after the final object exists.
- Move emoji normalization into the shared WASM-able crate from the design before the
  browser/server serving path depends on it.
- Add derive/ClickHouse ingest from committed manifest entries, then run the stratified
  canary and fleet scheduler work.

### Defaulted design choices (revisit if needed)

- **BlockStore** = index the spooled CAR file (`CID → (offset,len)`, seek to read) rather
  than a second on-disk copy; spill the index if a whale's is too large for RAM.
- **Parquet** = `arrow` + `parquet` crates.
- **Emoji** = currently minimal local Rust extraction in `archive.rs`; still promote it to
  the shared `emoji-normalizer` crate before this becomes a serving contract.

## Jacquard 0.12.0 API map (load-bearing; from recon — verify against `/tmp/jacquard`)

### Transport — `jacquard-common` (features: std, service-auth, crypto, reqwest-client, streaming)
- `jacquard_common::http_client::HttpClient`: `async fn send_http(&self, http::Request<Vec<u8>>) -> Result<http::Response<Vec<u8>>, Self::Error>`; `Error: std::error::Error + Display + Send + Sync + 'static`.
- `HttpClientExt` (feat `streaming`): `async fn send_http_streaming(&self, req) -> Result<http::Response<ByteStream>, Error>` + `send_http_bidirectional<S>` (upload only — return an Err, NOT `unimplemented!`, under our lint bar).
- reqwest impl template (`http_client.rs:118`): copy **all** headers for **any** status (no `error_for_status`), `resp.bytes_stream()` → `ByteStream::new(...)`. Our per-chunk inactivity timeout wraps each `stream.next()`; per-host pacing wraps `req.send()`.
- `XrpcExt::xrpc(base: Uri<&str>) -> XrpcCall` (blanket impl on every `HttpClient`). **Avoid** the stateful `XrpcClient`/`Agent`/`send()` (buffer body, drop headers, collapse errors).
- `XrpcCall::download(&req) -> Result<StreamingResponse, StreamError>` (feat `streaming`). Does **not** status-check. `StreamingResponse::{status(), headers(), into_parts()->(Parts, ByteStream)}`. Read `ratelimit-*` from `headers()` before consuming the body. `ByteStream::into_inner()` → `Pin<Box<dyn Stream<Item=Result<Bytes, StreamError>> + Send>>`.
- `GetRepo` @ `jacquard_api::com_atproto::sync::get_repo`: `{ did: Did<S>, since: Option<Tid> }`; NSID `com.atproto.sync.getRepo`; `Accept: application/vnd.ipld.car`. Pass `&GetRepo` to `download()`.
- **No** rate-limit header parsing exists in Jacquard — hand-roll `ratelimit-limit/remaining/reset`, `x-ratelimit-*`, `retry-after`.

### Parse — `jacquard-repo`
- `jacquard_repo::storage::BlockStore` (trait, `Clone + Send + Sync + 'static`): `get/put/has/put_many/get_many/apply_commit`. MST read path uses only `get/get_many/has` → implement those over disk; stub `put/put_many/apply_commit` by returning Err. Hold the disk handle in `Arc` (cheap `Clone`).
- All 3 built-in stores are in-RAM (incl. `FileBlockStore`, which slurps the whole CAR) → write our own.
- `jacquard_repo::car::stream_car(path) -> CarBlockStream`; `.next() -> Option<(Cid, Bytes)>` — streaming, whale-safe. (`read_car`/`parse_car_bytes` buffer everything — avoid for whales.)
- `jacquard_repo::commit::Commit<S>` `{ did, version, data: IpldCid (=MST root), rev, prev, sig }`; `Commit::from_cbor(&bytes)`; `commit.data()`. (Skip signature verify per design.)
- `jacquard_repo::mst::Mst::load(Arc<Store>, cid, layer: Option<usize>)` (lazy). `mst.get_pointer()` recomputes the root CID → **Snapshot Completeness = `get_pointer() == commit.data`**. `MstCursor`/`leaves_sequential()` for whales (`leaves()`/`collect_blocks()` collect into RAM). rkeys are the reconstructed MST leaf keys.
- `jacquard_repo::mst::util::compute_cid(&[u8]) -> IpldCid` (SHA-256, dag-cbor codec `0x71`). **No read path verifies bytes-hash-to-CID** — WE must `compute_cid` per block at ingest and reject mismatches (the other half of completeness). Guard non-dag-cbor codecs (raw `0x55`).
- Reference pattern: `jacquard_repo::commit::firehose::validate_v1_0` (load MST → `get_pointer()` == expected root).

### Types / errors — `jacquard-api`, `jacquard-common`, `jacquard-identity`
- All generated types are generic `<S: BosStr = SmolStr>`; use the `SmolStr` default.
- `GetRepoError` (get_repo.rs): `RepoNotFound/RepoTakendown/RepoSuspended/RepoDeactivated(Option<SmolStr>)` + `#[serde(untagged)] Other { error: SmolStr, message }` (preserves the raw code). Deserialize the body into this **regardless of HTTP status** (we own transport). Other endpoints' errors are `GenericError` (private `Data` newtype) — re-deserialize the body into our own `{error,message}` to recover the code.
- `listRepos` `Repo.status`: `RepoStatus` enum `Takendown/Suspended/Deleted/Deactivated/Desynchronized/Throttled/Other(S)`.
- `app_bsky::feed::post::Post<S>`: `text:S`, `created_at: Datetime`, optional `facets/reply/embed/langs(Vec<Language>)/labels/tags`, `extra_data: Option<BTreeMap<SmolStr,Data>>` (`#[serde(flatten)]` catch-all). `embed` is an open union with an injected `Unknown(Data)`. `Datetime` preserves the original string. → flat columns + `extras_json`.
- `app_bsky::actor::profile::Profile<S>`: all optional + `extra_data`.
- Identity: `PublicResolver = JacquardResolver<reqwest::Client>`; `PublicResolver::default()`; `IdentityResolver::pds_for_did(&Did) -> Uri<String>`. `Did::new_owned(&str) -> Result`. PLC bulk export = hand-roll (Jacquard is one-DID-per-GET: `plc.directory/{did}`).

## Conventions under the strict lint bar (see Cargo.toml `[workspace.lints]`)

- No `unwrap`/`expect`/`panic`/`todo`/`unimplemented` in non-test code → return Errs (thiserror), `?`/`map_err`. Stub unwanted trait methods by returning Err, never `unimplemented!`.
- No `indexing_slicing` → `.get()`. No `arithmetic_side_effects` → `checked_*`/`saturating_*` on byte/record counters.
- `doc_markdown` (pedantic): backtick code/type/format terms in doc comments (`getRepo`, `HttpClient`, `BlockStore`, `MST`, `CAR`, `Parquet`, …).
- `nextest` fails on zero tests → every crate needs ≥1 test.
- `deny.toml`: git deps need a `version =` (else flagged wildcard); the license allow-list and advisory `ignore` are tuned for the current tree (re-tune when deps change).
- `./check.sh` runs the full muster: fmt · clippy -D warnings · test · nextest · deny · audit · machete · llvm-cov.

```

---

## `rust/Cargo.toml`

```toml
[workspace]
resolver = "2"
members = ["crates/*"]
# Jacquard 0.12.0 comes from our GitHub fork-mirror as SHA-pinned git dependencies
# (github.com/aliceisjustplaying/jacquard @ 39648622 == tag 0.12.0), added per-crate in the
# member crates as they're used — not vendored in-repo (avoids ~55 MB of ecosystem lexicons).

[workspace.package]
version = "0.1.0"
edition = "2024"
license = "MIT"
repository = "https://github.com/aliceisjustplaying/emojistats-bsky"
authors = ["alice <aliceisjustplaying@gmail.com>"]

[workspace.lints.rust]
unsafe_code = "forbid"
warnings = "deny"

[workspace.lints.clippy]
# Lint groups (priority -1 so the specific overrides below take precedence).
all = { level = "deny", priority = -1 }
pedantic = { level = "deny", priority = -1 }
nursery = { level = "deny", priority = -1 }
cargo = { level = "deny", priority = -1 }
# Restriction lints — the retro's silent-failure / overflow / panic foot-guns. Denied in
# library/bin code; relaxed in tests via clippy.toml.
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"
todo = "deny"
unimplemented = "deny"
indexing_slicing = "deny"
arithmetic_side_effects = "deny"
# Justified exception: transitive dependency trees we don't control will duplicate crate
# versions. Per-crate metadata (cargo_common_metadata) is still enforced.
multiple_crate_versions = "allow"

```

---

## `rust/crates/emojistats-backfill/Cargo.toml`

```toml
[package]
name = "emojistats-backfill"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
authors.workspace = true
description = "emojistats v2 backfill: fetch a Bluesky repo, prove snapshot completeness, archive posts, derive emoji rows."
readme = "README.md"
keywords = ["bluesky", "atproto", "backfill", "emoji"]
categories = ["command-line-utilities"]

[[bin]]
name = "emojistats-backfill"
path = "src/main.rs"

[dependencies]
clap = { version = "4", features = ["derive"] }
anyhow = "1"
hex = "0.4"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sha2 = "0.10"
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "http2"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
arrow-array = "57"
arrow-schema = "57"
emojis = "0.7"
parquet = { version = "57", default-features = false, features = ["arrow", "snap", "zstd"] }
unicode-segmentation = "1"
jacquard-common = { version = "0.12.0", git = "https://github.com/aliceisjustplaying/jacquard", rev = "39648622522fa62c4c0b12ac22b8a5f6893c845a", default-features = false, features = ["std", "service-auth", "crypto", "reqwest-client", "streaming"] }
jacquard-api = { version = "0.12.0", git = "https://github.com/aliceisjustplaying/jacquard", rev = "39648622522fa62c4c0b12ac22b8a5f6893c845a", features = ["app_bsky"] }
jacquard-identity = { version = "0.12.0", git = "https://github.com/aliceisjustplaying/jacquard", rev = "39648622522fa62c4c0b12ac22b8a5f6893c845a" }
jacquard-repo = { version = "0.12.0", git = "https://github.com/aliceisjustplaying/jacquard", rev = "39648622522fa62c4c0b12ac22b8a5f6893c845a" }
futures-util = "0.3"
http = "1"
bytes = "1"
cid = { version = "0.11", features = ["serde"] }
serde_ipld_dagcbor = "0.6"
smol_str = { version = "0.3", features = ["serde"] }
thiserror = "2"

[lints]
workspace = true

```

---

## `rust/crates/emojistats-backfill/src/lib.rs`

```rust
//! Library surface for the v2 backfill vertical slice.

pub mod archive;
pub mod parse;
pub mod transport;

```

---

## `rust/crates/emojistats-backfill/src/main.rs`

```rust
//! emojistats-backfill — v2 Rust backfill CLI.
//!
//! Vertical-slice milestone in progress: `fetch-one <did>` resolves a DID to its PDS, then
//! (incrementally) fetches the repo via the streaming `getRepo` seam, proves snapshot
//! completeness, archives posts, and derives emoji rows. See `docs/backfill-v2-design.md`
//! ("First implementation milestone").

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use emojistats_backfill::{
    archive::{
        RepoReceiptInput, archive_rows_from_parsed_repo, build_repo_receipt, current_normalizer,
        hash_profile_record, write_archive_artifacts,
    },
    parse::parse_repo_for_did,
    transport::{FetchConfig, fetch_repo},
};
use jacquard_common::types::did::Did;
use jacquard_identity::{PublicResolver, resolver::IdentityResolver};

/// emojistats v2 backfill tool.
#[derive(Parser, Debug)]
#[command(name = "emojistats-backfill", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Fetch and process a single repo by DID (vertical-slice milestone).
    FetchOne {
        /// The DID to fetch, e.g. did:plc:....
        did: String,
        /// Directory for local `CAR` spooling.
        #[arg(long, default_value = "data/spool")]
        spool_dir: PathBuf,
        /// Loud single-repo byte cap for the spooled `CAR`.
        #[arg(long, default_value_t = 2_147_483_648)]
        max_bytes: u64,
        /// Directory for local archive artifacts.
        #[arg(long, default_value = "data/archive")]
        archive_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::FetchOne {
            did,
            spool_dir,
            max_bytes,
            archive_dir,
        } => fetch_one(&did, spool_dir, max_bytes, archive_dir).await,
    }
}

/// Resolve a DID to its PDS endpoint.
///
/// Remaining milestone steps build on this: `getRepo` via the `download()` seam over our
/// own reqwest `HttpClient` (capturing rate-limit headers), spool the `CAR` under Loud
/// Resource Caps, parse via an on-disk `BlockStore` + `MST` walk, prove Snapshot
/// Completeness, compute the row-content receipt, write `Parquet` + a manifest entry, and
/// derive emoji rows.
async fn fetch_one(
    did_str: &str,
    spool_dir: PathBuf,
    max_bytes: u64,
    archive_dir: PathBuf,
) -> anyhow::Result<()> {
    let did: Did =
        Did::new_owned(did_str).map_err(|err| anyhow::anyhow!("invalid DID {did_str:?}: {err}"))?;

    let resolver = PublicResolver::default();
    let pds = resolver
        .pds_for_did(&did)
        .await
        .map_err(|err| anyhow::anyhow!("resolve PDS for {did_str}: {err}"))?;

    println!("{did_str} -> PDS {pds}");
    let http = reqwest::Client::new();
    let mut config = FetchConfig::new(spool_dir);
    config.max_bytes = max_bytes;

    let spooled = fetch_repo(&http, &pds, &did, &config)
        .await
        .map_err(|err| anyhow::anyhow!("fetch getRepo for {did_str}: {err}"))?;
    println!(
        "spooled {} bytes from HTTP {} to {}",
        spooled.bytes,
        spooled.http_status,
        spooled.car_path.display()
    );

    let parsed = parse_repo_for_did(&spooled.car_path, did_str)
        .map_err(|err| anyhow::anyhow!("parse CAR for {did_str}: {err}"))?;
    let rows = archive_rows_from_parsed_repo(&parsed)
        .map_err(|err| anyhow::anyhow!("build archive rows for {did_str}: {err}"))?;
    let profile_row_hash = hash_profile_record(parsed.profile.as_ref())
        .map_err(|err| anyhow::anyhow!("hash profile row for {did_str}: {err}"))?;
    let post_decode_error_count = parsed
        .record_decode_errors
        .iter()
        .filter(|error| error.collection == "app.bsky.feed.post")
        .count()
        .try_into()
        .map_err(|_err| anyhow::anyhow!("post decode error count overflow for {did_str}"))?;
    let receipt = build_repo_receipt(RepoReceiptInput {
        rows: &rows,
        reachable_records_count: parsed.rkey_digest.all_records_count,
        reachable_post_records_count: parsed.rkey_digest.post_records_count,
        post_decode_error_count,
        profile_row_hash,
        mst_root_cid: Some(parsed.commit.data.clone()),
        commit_cid: Some(parsed.commit.cid.clone()),
        normalizer: current_normalizer(),
    })
    .map_err(|err| anyhow::anyhow!("build receipt for {did_str}: {err}"))?;
    let artifacts = write_archive_artifacts(&archive_dir, did_str, &rows, &receipt)
        .map_err(|err| anyhow::anyhow!("write archive artifacts for {did_str}: {err}"))?;
    println!(
        "parsed {} records, {} posts, {} decode errors, {} emoji rows, receipt {}",
        parsed.rkey_digest.all_records_count,
        receipt.archived_post_rows_count,
        parsed.record_decode_errors.len(),
        artifacts.emoji_rows,
        receipt.post_rows_hash
    );
    println!(
        "wrote archive {}, receipt {}, manifest {}, emoji projection {}",
        artifacts.parquet_path.display(),
        artifacts.receipt_path.display(),
        artifacts.manifest_path.display(),
        artifacts.emoji_projection_path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;

    use super::{Cli, Command};

    #[test]
    fn parses_fetch_one_did() {
        let cli =
            Cli::try_parse_from(["emojistats-backfill", "fetch-one", "did:plc:abc123"]).unwrap();
        let Command::FetchOne {
            did,
            spool_dir,
            max_bytes,
            archive_dir,
        } = cli.command;
        assert_eq!(did, "did:plc:abc123");
        assert_eq!(spool_dir, PathBuf::from("data/spool"));
        assert_eq!(max_bytes, 2_147_483_648);
        assert_eq!(archive_dir, PathBuf::from("data/archive"));
    }

    #[test]
    fn requires_a_subcommand() {
        assert!(Cli::try_parse_from(["emojistats-backfill"]).is_err());
    }
}

```

---

## `rust/crates/emojistats-backfill/src/transport.rs`

```rust
//! Stage B `getRepo` transport.

use std::{
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::StreamExt as _;
use http::{HeaderMap, StatusCode};
use jacquard_api::com_atproto::sync::get_repo::{GetRepo, GetRepoError};
use jacquard_common::{
    deps::fluent_uri::Uri,
    http_client::{HttpClient, HttpClientExt},
    stream::ByteStream,
    types::did::Did,
    xrpc::XrpcExt as _,
};
use tokio::time;

const DEFAULT_CHUNK_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_BYTES: u64 = 2_147_483_648;
const ERROR_BODY_MAX_BYTES: u64 = 65_536;

/// Runtime limits and local storage path for Stage B repo transport.
#[derive(Debug, Clone)]
pub struct FetchConfig {
    /// Directory where the streamed `CAR` is written.
    pub spool_dir: PathBuf,
    /// Maximum silence while waiting for the next body chunk.
    pub chunk_idle_timeout: Duration,
    /// Loud single-repo byte cap for the spooled `CAR`.
    pub max_bytes: u64,
}

impl FetchConfig {
    /// Build a transport config with conservative defaults.
    #[must_use]
    pub fn new(spool_dir: impl Into<PathBuf>) -> Self {
        Self {
            spool_dir: spool_dir.into(),
            chunk_idle_timeout: DEFAULT_CHUNK_IDLE_TIMEOUT,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

/// A successfully spooled repo `CAR`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpooledRepo {
    /// Path to the local spooled `CAR`.
    pub car_path: PathBuf,
    /// HTTP status returned by `getRepo`.
    pub http_status: u16,
    /// Rate-limit headers observed on the response.
    pub rate_limit: RateLimitSnapshot,
    /// Bytes written to `car_path`.
    pub bytes: u64,
}

/// Parsed `ratelimit-*`, `x-ratelimit-*`, and `retry-after` headers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RateLimitSnapshot {
    /// Advertised request limit.
    pub limit: Option<u64>,
    /// Remaining requests in the current window.
    pub remaining: Option<u64>,
    /// Reset value as advertised by the host.
    pub reset: Option<u64>,
    /// Retry delay when the host provides a seconds-based `retry-after`.
    pub retry_after: Option<Duration>,
    /// Raw `ratelimit-policy` header.
    pub policy: Option<String>,
}

impl RateLimitSnapshot {
    /// Parse rate-limit headers from a response.
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Self {
        Self {
            limit: parse_u64_header(headers, "ratelimit-limit")
                .or_else(|| parse_u64_header(headers, "x-ratelimit-limit")),
            remaining: parse_u64_header(headers, "ratelimit-remaining")
                .or_else(|| parse_u64_header(headers, "x-ratelimit-remaining")),
            reset: parse_u64_header(headers, "ratelimit-reset")
                .or_else(|| parse_u64_header(headers, "x-ratelimit-reset")),
            retry_after: parse_u64_header(headers, "retry-after").map(Duration::from_secs),
            policy: parse_string_header(headers, "ratelimit-policy"),
        }
    }
}

/// Terminal account states returned by `com.atproto.sync.getRepo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountState {
    /// The repo does not exist on this host.
    RepoNotFound,
    /// The repo is taken down.
    RepoTakendown,
    /// The repo is suspended.
    RepoSuspended,
    /// The repo is deactivated.
    RepoDeactivated,
}

impl fmt::Display for AccountState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::RepoNotFound => "RepoNotFound",
            Self::RepoTakendown => "RepoTakendown",
            Self::RepoSuspended => "RepoSuspended",
            Self::RepoDeactivated => "RepoDeactivated",
        };
        f.write_str(name)
    }
}

/// Stage B fetch failures, split into account-state, HTTP, timeout, cap, stream, and I/O buckets.
#[derive(Debug)]
pub enum FetchError {
    /// The PDS returned a terminal account-state error.
    AccountState {
        /// Account-state code from the XRPC body.
        state: AccountState,
        /// HTTP status returned by the PDS.
        status: u16,
        /// Optional XRPC error message.
        message: Option<Box<str>>,
        /// Rate-limit headers observed on the response.
        rate_limit: Box<RateLimitSnapshot>,
    },
    /// The PDS returned a non-success HTTP status that was not a terminal account state.
    HttpStatus {
        /// HTTP status returned by the PDS.
        status: u16,
        /// XRPC error code when the body decoded as one.
        error_code: Option<Box<str>>,
        /// Optional XRPC error message.
        message: Option<Box<str>>,
        /// Rate-limit headers observed on the response.
        rate_limit: Box<RateLimitSnapshot>,
    },
    /// No body chunk arrived inside the configured idle timeout.
    InactivityTimeout {
        /// Timeout used for each chunk read.
        timeout: Duration,
    },
    /// The streamed body exceeded the configured single-repo byte cap.
    MaxBytesExceeded {
        /// Configured cap.
        max_bytes: u64,
        /// Bytes observed after accepting the chunk that crossed the cap.
        observed_bytes: u64,
    },
    /// The PDS response body used for error classification exceeded its safety cap.
    ErrorBodyTooLarge {
        /// Configured cap.
        max_bytes: u64,
        /// Bytes observed after accepting the chunk that crossed the cap.
        observed_bytes: u64,
    },
    /// A streaming transport error occurred before or during body download.
    Transport {
        /// Transport error message.
        message: String,
    },
    /// Local filesystem I/O failed.
    Io {
        /// Underlying I/O error.
        source: io::Error,
    },
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AccountState {
                state,
                status,
                message,
                rate_limit: _,
            } => write_fetch_message(
                f,
                &format_args!("account state {state}"),
                *status,
                message.as_deref(),
            ),
            Self::HttpStatus {
                status,
                error_code,
                message,
                rate_limit: _,
            } => match error_code {
                Some(code) => write_fetch_message(
                    f,
                    &format_args!("HTTP status {status} with XRPC error {code}"),
                    *status,
                    message.as_deref(),
                ),
                None => write!(f, "HTTP status {status}"),
            },
            Self::InactivityTimeout { timeout } => {
                write!(f, "no body chunk within {}", timeout.as_secs())
            }
            Self::MaxBytesExceeded {
                max_bytes,
                observed_bytes,
            } => write!(
                f,
                "spooled CAR exceeded max bytes: observed {observed_bytes}, max {max_bytes}"
            ),
            Self::ErrorBodyTooLarge {
                max_bytes,
                observed_bytes,
            } => write!(
                f,
                "error response body exceeded max bytes: observed {observed_bytes}, max {max_bytes}"
            ),
            Self::Transport { message } => write!(f, "transport error: {message}"),
            Self::Io { source } => write!(f, "I/O error: {source}"),
        }
    }
}

impl Error for FetchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source } => Some(source),
            Self::AccountState { .. }
            | Self::HttpStatus { .. }
            | Self::InactivityTimeout { .. }
            | Self::MaxBytesExceeded { .. }
            | Self::ErrorBodyTooLarge { .. }
            | Self::Transport { .. } => None,
        }
    }
}

impl From<io::Error> for FetchError {
    fn from(source: io::Error) -> Self {
        Self::Io { source }
    }
}

/// Stream `com.atproto.sync.getRepo` from `pds` into a local spool file.
///
/// # Errors
///
/// Returns [`FetchError`] when the PDS reports an account state or HTTP error, the body
/// stalls, the loud byte cap is hit, the stream fails, or local filesystem I/O fails.
pub async fn fetch_repo<C>(
    http: &C,
    pds: &Uri<String>,
    did: &Did,
    config: &FetchConfig,
) -> Result<SpooledRepo, FetchError>
where
    C: HttpClient + HttpClientExt + Sync,
{
    fs::create_dir_all(&config.spool_dir)?;

    let request = GetRepo {
        did: did.clone(),
        since: None,
    };
    let response = http
        .xrpc(pds.borrow())
        .download(&request)
        .await
        .map_err(|err| FetchError::Transport {
            message: err.to_string(),
        })?;
    let status = response.status();
    let rate_limit = RateLimitSnapshot::from_headers(response.headers());
    let (_parts, body) = response.into_parts();

    if !status.is_success() {
        let body_bytes =
            collect_body_with_cap(body, config.chunk_idle_timeout, ERROR_BODY_MAX_BYTES).await?;
        return Err(classify_http_error(status, rate_limit, &body_bytes));
    }

    let car_path = spool_path(&config.spool_dir, did);
    let bytes =
        stream_to_file(body, &car_path, config.chunk_idle_timeout, config.max_bytes).await?;

    Ok(SpooledRepo {
        car_path,
        http_status: status.as_u16(),
        rate_limit,
        bytes,
    })
}

async fn stream_to_file(
    body: ByteStream,
    car_path: &Path,
    chunk_idle_timeout: Duration,
    max_bytes: u64,
) -> Result<u64, FetchError> {
    let temp_path = temp_spool_path(car_path)?;
    match stream_to_temp_file(body, &temp_path, chunk_idle_timeout, max_bytes).await {
        Ok(bytes) => {
            fs::rename(&temp_path, car_path)?;
            sync_parent_dir(car_path)?;
            Ok(bytes)
        }
        Err(error) => {
            let _ignored = fs::remove_file(&temp_path);
            Err(error)
        }
    }
}

async fn stream_to_temp_file(
    body: ByteStream,
    temp_path: &Path,
    chunk_idle_timeout: Duration,
    max_bytes: u64,
) -> Result<u64, FetchError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp_path)?;
    let mut bytes = 0_u64;
    let mut stream = body.into_inner();

    while let Some(next_chunk) = time::timeout(chunk_idle_timeout, stream.next())
        .await
        .map_err(|_elapsed| FetchError::InactivityTimeout {
            timeout: chunk_idle_timeout,
        })?
    {
        let chunk = next_chunk.map_err(|err| FetchError::Transport {
            message: err.to_string(),
        })?;
        let chunk_len =
            u64::try_from(chunk.len()).map_err(|_err| FetchError::MaxBytesExceeded {
                max_bytes,
                observed_bytes: u64::MAX,
            })?;
        let observed_bytes = bytes
            .checked_add(chunk_len)
            .ok_or(FetchError::MaxBytesExceeded {
                max_bytes,
                observed_bytes: u64::MAX,
            })?;
        if observed_bytes > max_bytes {
            return Err(FetchError::MaxBytesExceeded {
                max_bytes,
                observed_bytes,
            });
        }
        file.write_all(chunk.as_ref())?;
        bytes = observed_bytes;
    }

    file.sync_all()?;
    Ok(bytes)
}

fn temp_spool_path(car_path: &Path) -> Result<PathBuf, FetchError> {
    let file_name = car_path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "spool path has no file name")
        })?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| io::Error::other(format!("system clock before UNIX epoch: {error}")))?;
    let temp_name = format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        timestamp.as_nanos()
    );
    Ok(car_path.with_file_name(temp_name))
}

fn sync_parent_dir(path: &Path) -> Result<(), FetchError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)?.sync_all()?;
    Ok(())
}

async fn collect_body_with_cap(
    body: ByteStream,
    chunk_idle_timeout: Duration,
    max_bytes: u64,
) -> Result<Vec<u8>, FetchError> {
    let mut bytes = Vec::new();
    let mut observed = 0_u64;
    let mut stream = body.into_inner();

    while let Some(next_chunk) = time::timeout(chunk_idle_timeout, stream.next())
        .await
        .map_err(|_elapsed| FetchError::InactivityTimeout {
            timeout: chunk_idle_timeout,
        })?
    {
        let chunk = next_chunk.map_err(|err| FetchError::Transport {
            message: err.to_string(),
        })?;
        let chunk_len =
            u64::try_from(chunk.len()).map_err(|_err| FetchError::ErrorBodyTooLarge {
                max_bytes,
                observed_bytes: u64::MAX,
            })?;
        let next_observed =
            observed
                .checked_add(chunk_len)
                .ok_or(FetchError::ErrorBodyTooLarge {
                    max_bytes,
                    observed_bytes: u64::MAX,
                })?;
        if next_observed > max_bytes {
            return Err(FetchError::ErrorBodyTooLarge {
                max_bytes,
                observed_bytes: next_observed,
            });
        }
        bytes.extend_from_slice(chunk.as_ref());
        observed = next_observed;
    }

    Ok(bytes)
}

fn classify_http_error(
    status: StatusCode,
    rate_limit: RateLimitSnapshot,
    body: &[u8],
) -> FetchError {
    match serde_json::from_slice::<GetRepoError>(body) {
        Ok(GetRepoError::RepoNotFound(message)) => FetchError::AccountState {
            state: AccountState::RepoNotFound,
            status: status.as_u16(),
            message: message.map(|value| value.to_string().into_boxed_str()),
            rate_limit: Box::new(rate_limit),
        },
        Ok(GetRepoError::RepoTakendown(message)) => FetchError::AccountState {
            state: AccountState::RepoTakendown,
            status: status.as_u16(),
            message: message.map(|value| value.to_string().into_boxed_str()),
            rate_limit: Box::new(rate_limit),
        },
        Ok(GetRepoError::RepoSuspended(message)) => FetchError::AccountState {
            state: AccountState::RepoSuspended,
            status: status.as_u16(),
            message: message.map(|value| value.to_string().into_boxed_str()),
            rate_limit: Box::new(rate_limit),
        },
        Ok(GetRepoError::RepoDeactivated(message)) => FetchError::AccountState {
            state: AccountState::RepoDeactivated,
            status: status.as_u16(),
            message: message.map(|value| value.to_string().into_boxed_str()),
            rate_limit: Box::new(rate_limit),
        },
        Ok(GetRepoError::Other { error, message }) => FetchError::HttpStatus {
            status: status.as_u16(),
            error_code: Some(error.to_string().into_boxed_str()),
            message: message.map(|value| value.to_string().into_boxed_str()),
            rate_limit: Box::new(rate_limit),
        },
        Err(_err) => FetchError::HttpStatus {
            status: status.as_u16(),
            error_code: None,
            message: String::from_utf8(body.to_vec())
                .ok()
                .map(String::into_boxed_str),
            rate_limit: Box::new(rate_limit),
        },
    }
}

fn spool_path(spool_dir: &Path, did: &Did) -> PathBuf {
    let mut file_name = String::from("repo-");
    for ch in did.as_str().chars() {
        if ch.is_ascii_alphanumeric() {
            file_name.push(ch);
        } else {
            file_name.push('_');
        }
    }
    file_name.push_str(".car");
    spool_dir.join(file_name)
}

fn parse_u64_header(headers: &HeaderMap, name: &'static str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn parse_string_header(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn write_fetch_message(
    f: &mut fmt::Formatter<'_>,
    prefix: &fmt::Arguments<'_>,
    status: u16,
    message: Option<&str>,
) -> fmt::Result {
    match message {
        Some(message) => write!(f, "{prefix} at HTTP status {status}: {message}"),
        None => write!(f, "{prefix} at HTTP status {status}"),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]

    use std::{path::PathBuf, time::Duration};

    use http::{HeaderMap, StatusCode};
    use jacquard_common::types::did::Did;

    use super::{
        AccountState, FetchConfig, FetchError, RateLimitSnapshot, classify_http_error, spool_path,
    };

    #[test]
    fn parses_standard_rate_limit_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("ratelimit-limit", "3000".parse().unwrap());
        headers.insert("ratelimit-remaining", "2999".parse().unwrap());
        headers.insert("ratelimit-reset", "42".parse().unwrap());
        headers.insert("ratelimit-policy", "3000;w=300".parse().unwrap());
        headers.insert("retry-after", "5".parse().unwrap());

        let snapshot = RateLimitSnapshot::from_headers(&headers);

        assert_eq!(snapshot.limit, Some(3000));
        assert_eq!(snapshot.remaining, Some(2999));
        assert_eq!(snapshot.reset, Some(42));
        assert_eq!(snapshot.retry_after, Some(Duration::from_secs(5)));
        assert_eq!(snapshot.policy.as_deref(), Some("3000;w=300"));
    }

    #[test]
    fn falls_back_to_x_rate_limit_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-limit", "100".parse().unwrap());
        headers.insert("x-ratelimit-remaining", "7".parse().unwrap());
        headers.insert("x-ratelimit-reset", "99".parse().unwrap());

        let snapshot = RateLimitSnapshot::from_headers(&headers);

        assert_eq!(snapshot.limit, Some(100));
        assert_eq!(snapshot.remaining, Some(7));
        assert_eq!(snapshot.reset, Some(99));
    }

    #[test]
    fn classifies_repo_account_states() {
        let body = br#"{"error":"RepoSuspended","message":"nope"}"#;

        let err = classify_http_error(StatusCode::FORBIDDEN, RateLimitSnapshot::default(), body);

        match err {
            FetchError::AccountState {
                state,
                status,
                message,
                rate_limit: _,
            } => {
                assert_eq!(state, AccountState::RepoSuspended);
                assert_eq!(status, 403);
                assert_eq!(message.as_deref(), Some("nope"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn preserves_unknown_xrpc_error_code() {
        let body = br#"{"error":"HostThrottled","message":"slow down"}"#;

        let err = classify_http_error(
            StatusCode::TOO_MANY_REQUESTS,
            RateLimitSnapshot::default(),
            body,
        );

        match err {
            FetchError::HttpStatus {
                status,
                error_code,
                message,
                rate_limit: _,
            } => {
                assert_eq!(status, 429);
                assert_eq!(error_code.as_deref(), Some("HostThrottled"));
                assert_eq!(message.as_deref(), Some("slow down"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn default_config_sets_spool_dir_and_limits() {
        let config = FetchConfig::new(PathBuf::from("/tmp/spool"));

        assert_eq!(config.spool_dir, PathBuf::from("/tmp/spool"));
        assert_eq!(config.chunk_idle_timeout, Duration::from_secs(30));
        assert_eq!(config.max_bytes, 2_147_483_648);
    }

    #[test]
    fn spool_path_sanitizes_did() {
        let did = Did::new_owned("did:plc:abc123").unwrap();

        let path = spool_path(PathBuf::from("/tmp/spool").as_path(), &did);

        assert_eq!(path, PathBuf::from("/tmp/spool/repo-did_plc_abc123.car"));
    }
}

```

---

## `rust/crates/emojistats-backfill/src/parse.rs`

```rust
//! Stage C `CAR` parser for the v2 backfill pipeline.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use cid::Cid as IpldCid;
use jacquard_api::app_bsky::{actor::profile::Profile, feed::post::Post};
use jacquard_repo::{
    DAG_CBOR_CID_CODEC, Mst, car,
    commit::Commit,
    error::RepoError,
    mst::{
        cursor::{CursorPosition, MstCursor},
        util::compute_cid,
    },
    storage::BlockStore,
};
use smol_str::SmolStr;

/// Parsed one-repo output from Stage C.
#[derive(Debug, Clone)]
pub struct ParsedRepo {
    /// Commit metadata from the repo root block.
    pub commit: CommitMeta,
    /// Snapshot completeness proof details.
    pub completeness: CompletenessProof,
    /// Extracted `app.bsky.feed.post` records.
    pub posts: Vec<PostRecord>,
    /// Deterministic key summary for the traversed `MST`.
    pub rkey_digest: RkeyDigest,
    /// Extracted `app.bsky.actor.profile/self`, when present.
    pub profile: Option<ProfileRecord>,
    /// Non-fatal profile sidecar decode error, when the post snapshot can still be parsed.
    pub profile_decode_error: Option<String>,
    /// Typed record decode failures observed while walking reachable records.
    pub record_decode_errors: Vec<RecordDecodeFailure>,
}

/// Resource caps for Stage C parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseConfig {
    /// Maximum number of `CAR` blocks accepted while verifying or indexing.
    pub max_car_blocks: u64,
    /// Maximum encoded `CAR` block section size accepted before allocation.
    pub max_block_bytes: u64,
    /// Maximum number of reachable repo records accepted while walking the `MST`.
    pub max_records: u64,
    /// Maximum `MST` cursor layer accepted while walking records.
    pub max_mst_depth: u64,
    /// Maximum number of non-fatal typed record decode errors accepted.
    pub max_decode_errors: u64,
    /// Maximum best-effort parser wall-clock time.
    pub max_parse_wall_clock: Duration,
}

impl Default for ParseConfig {
    fn default() -> Self {
        Self {
            max_car_blocks: 10_000_000,
            max_block_bytes: 67_108_864,
            max_records: 10_000_000,
            max_mst_depth: 256,
            max_decode_errors: 1_000_000,
            max_parse_wall_clock: Duration::from_mins(15),
        }
    }
}

/// Commit metadata needed by downstream archive and receipt code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitMeta {
    /// Commit block `CID`.
    pub cid: String,
    /// Repository `DID` claimed by the commit.
    pub did: String,
    /// Commit schema version.
    pub version: i64,
    /// Commit revision `TID`.
    pub rev: String,
    /// Commit `MST` root `CID`.
    pub data: String,
    /// Previous commit `CID`, if present.
    pub prev: Option<String>,
}

/// Completeness proof fields for a `getRepo` snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletenessProof {
    /// Proof class for the parsed input.
    pub class: CompletenessClass,
    /// Root `CID` entries declared by the `CAR` header.
    pub car_roots: Vec<String>,
    /// Number of `CAR` blocks with verified content-addressed `CID`s.
    pub verified_block_count: u64,
    /// Number of reachable `MST` leaves whose record block resolved by `CID`.
    pub reachable_record_count: u64,
    /// Whether the commit's `data` root matched the traversed `MST` root.
    pub mst_root_matches_commit: bool,
    /// Commit signature verification is deliberately out of scope for Stage C.
    pub repo_commit_signature_verified: bool,
    /// Identity verification is deliberately out of scope for Stage C.
    pub identity_verified: bool,
}

/// Completeness class assigned to the parsed repo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletenessClass {
    /// Complete `CAR` snapshot proven from commit root through `MST` leaves.
    SnapshotComplete,
}

/// Extracted post record plus repo key context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostRecord {
    /// Repo record key.
    pub rkey: String,
    /// Record block `CID`.
    pub cid: String,
    /// Typed Bluesky post record.
    pub record: Post<SmolStr>,
}

/// Extracted profile record plus repo key context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileRecord {
    /// Repo record key.
    pub rkey: String,
    /// Record block `CID`.
    pub cid: String,
    /// Typed Bluesky profile record.
    pub record: Profile<SmolStr>,
}

/// Reachable record that failed typed decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordDecodeFailure {
    /// Full repo key.
    pub key: String,
    /// Collection being decoded.
    pub collection: &'static str,
    /// Record block `CID`.
    pub cid: String,
    /// Decode error message.
    pub message: String,
}

/// Deterministic key summary for archive receipt wiring.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RkeyDigest {
    /// Number of reachable repo records.
    pub all_records_count: u64,
    /// Number of reachable `app.bsky.feed.post` records.
    pub post_records_count: u64,
    /// First reachable repo key in `MST` order.
    pub first_key: Option<String>,
    /// Last reachable repo key in `MST` order.
    pub last_key: Option<String>,
}

/// Stage C parse failures.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// Filesystem operation failed.
    #[error("I/O while parsing {path}: {source}")]
    Io {
        /// Path being read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Jacquard repo primitive failed.
    #[error("Jacquard repo parse failed: {0}")]
    Repo(#[from] RepoError),
    /// `CAR` root/header shape is not usable as a repo snapshot.
    #[error("invalid CAR root set: {0}")]
    InvalidRoots(String),
    /// `CAR` block bytes do not match their advertised `CID`.
    #[error("CAR block CID mismatch: block={block_cid}, computed={computed_cid}")]
    CidMismatch {
        /// Advertised block `CID`.
        block_cid: String,
        /// Computed block `CID`.
        computed_cid: String,
    },
    /// Non-`dag-cbor` block found in the repo `CAR`.
    #[error("unsupported CAR block codec {codec:#x} for CID {cid}")]
    UnsupportedCodec {
        /// Block `CID`.
        cid: String,
        /// CID multicodec.
        codec: u64,
    },
    /// Commit block could not be found or decoded.
    #[error("commit block not found for CAR root {root}")]
    CommitNotFound {
        /// Root `CID` declared by the `CAR`.
        root: String,
    },
    /// The single `CAR` root did not decode as a repo commit.
    #[error("CAR root {root} did not decode as a repo commit: {message}")]
    RootCommitDecode {
        /// Root `CID` declared by the `CAR`.
        root: String,
        /// Decode error message.
        message: String,
    },
    /// The repo commit claimed a different `DID` than the caller requested.
    #[error("commit DID mismatch: requested={requested}, actual={actual}")]
    CommitDidMismatch {
        /// Requested repo `DID`.
        requested: String,
        /// Commit repo `DID`.
        actual: String,
    },
    /// A reachable block was missing from the `CAR`.
    #[error("reachable block missing from CAR: {cid}")]
    MissingBlock {
        /// Missing block `CID`.
        cid: String,
    },
    /// A typed record block failed to decode.
    #[error("failed to decode {collection} record {key} at {cid}: {source}")]
    RecordDecode {
        /// Full repo key.
        key: String,
        /// Collection being decoded.
        collection: &'static str,
        /// Record block `CID`.
        cid: String,
        /// Underlying DAG-CBOR decode error.
        #[source]
        source: Box<serde_ipld_dagcbor::DecodeError<std::convert::Infallible>>,
    },
    /// The `MST` root reached from the commit did not match `commit.data`.
    #[error("MST root mismatch: commit data={commit_data}, traversed root={traversed_root}")]
    MstRootMismatch {
        /// Commit `data` root.
        commit_data: String,
        /// Traversed `MST` root.
        traversed_root: String,
    },
    /// Integer overflow while counting parser resources.
    #[error("resource counter overflow: {field}")]
    ResourceCountOverflow {
        /// Counter name.
        field: &'static str,
    },
    /// Configured parser resource cap was exceeded.
    #[error("parser resource limit exceeded: {limit} observed={observed}; recovery={recovery}")]
    ResourceLimitExceeded {
        /// Limit name.
        limit: &'static str,
        /// Observed value.
        observed: u64,
        /// Operator recovery hint.
        recovery: &'static str,
    },
    /// Unsupported parse case with an explicit status.
    #[error("unsupported Stage C parse case: {feature}")]
    Unsupported {
        /// Unsupported feature.
        feature: &'static str,
    },
    /// Planned proof/extraction work that is intentionally not hidden.
    #[error("Stage C proof step not yet implemented: {feature}")]
    NotYetImplemented {
        /// Missing proof step.
        feature: &'static str,
    },
    /// Parser runtime could not be started.
    #[error("parser runtime failed: {0}")]
    Runtime(#[source] std::io::Error),
    /// Parser thread could not be started.
    #[error("parser thread failed: {0}")]
    ThreadSpawn(#[source] std::io::Error),
    /// Parser thread terminated unexpectedly.
    #[error("parser thread terminated unexpectedly")]
    RuntimeThreadTerminated,
    /// `CAR` varint was malformed.
    #[error("malformed CAR varint")]
    MalformedVarint,
    /// `CAR` length arithmetic overflowed.
    #[error("CAR length overflow while reading {field}")]
    CarLengthOverflow {
        /// Length field being processed.
        field: &'static str,
    },
    /// `CAR` section was malformed.
    #[error("malformed CAR section: {0}")]
    MalformedCar(String),
    /// `CID` bytes inside the `CAR` failed to decode.
    #[error("failed to decode CID from CAR block: {0}")]
    CidRead(#[source] Box<cid::Error>),
}

/// Parse a spooled repo `CAR` from disk.
///
/// # Errors
///
/// Returns [`ParseError`] for malformed `CAR`s, `CID` mismatches, missing reachable blocks,
/// invalid commits, `MST` traversal failures, typed record decode failures, and local I/O errors.
pub fn parse_repo(car_path: &Path) -> Result<ParsedRepo, ParseError> {
    parse_repo_with_config(car_path, ParseConfig::default())
}

/// Parse a spooled repo `CAR` from disk with explicit resource caps.
///
/// # Errors
///
/// Returns [`ParseError`] for malformed `CAR`s, `CID` mismatches, missing reachable blocks,
/// invalid commits, `MST` traversal failures, typed record decode failures, configured resource
/// limits, and local I/O errors.
pub fn parse_repo_with_config(
    car_path: &Path,
    config: ParseConfig,
) -> Result<ParsedRepo, ParseError> {
    parse_repo_thread(car_path, None, config)
}

/// Parse a spooled repo `CAR` and assert that the commit `DID` matches the requested repo.
///
/// # Errors
///
/// Returns [`ParseError`] for the same cases as [`parse_repo`], plus a loud commit `DID`
/// mismatch when the root commit does not claim `requested_did`.
pub fn parse_repo_for_did(car_path: &Path, requested_did: &str) -> Result<ParsedRepo, ParseError> {
    parse_repo_for_did_with_config(car_path, requested_did, ParseConfig::default())
}

/// Parse a spooled repo `CAR` with explicit resource caps and a requested `DID` assertion.
///
/// # Errors
///
/// Returns [`ParseError`] for malformed input, configured resource limits, traversal failures,
/// local I/O errors, and commit `DID` mismatch.
pub fn parse_repo_for_did_with_config(
    car_path: &Path,
    requested_did: &str,
    config: ParseConfig,
) -> Result<ParsedRepo, ParseError> {
    parse_repo_thread(car_path, Some(requested_did.to_owned()), config)
}

fn parse_repo_thread(
    car_path: &Path,
    requested_did: Option<String>,
    config: ParseConfig,
) -> Result<ParsedRepo, ParseError> {
    let car_path = car_path.to_path_buf();
    std::thread::Builder::new()
        .name("emojistats-stage-c-parse".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(ParseError::Runtime)?;
            runtime.block_on(parse_repo_async(
                &car_path,
                requested_did.as_deref(),
                config,
            ))
        })
        .map_err(ParseError::ThreadSpawn)?
        .join()
        .map_err(|_err| ParseError::RuntimeThreadTerminated)?
}

async fn parse_repo_async(
    car_path: &Path,
    requested_did: Option<&str>,
    config: ParseConfig,
) -> Result<ParsedRepo, ParseError> {
    let deadline = ParseDeadline::start(config.max_parse_wall_clock);
    let stream_summary = verify_streamed_car(car_path, config, deadline).await?;
    deadline.ensure_not_exceeded()?;
    let store = IndexedCarBlockStore::load(car_path, config)?;
    deadline.ensure_not_exceeded()?;
    let commit_root = single_car_root(&stream_summary.roots)?;
    let (commit_cid, commit) = load_commit(commit_root, &store).await?;
    deadline.ensure_not_exceeded()?;
    assert_requested_did(requested_did, commit.did().as_str())?;
    let (posts, profile, profile_decode_error, record_decode_errors, rkey_digest) =
        walk_mst_records(commit.data, &store, config, deadline).await?;

    let proof = CompletenessProof {
        class: CompletenessClass::SnapshotComplete,
        car_roots: stream_summary
            .roots
            .iter()
            .map(ToString::to_string)
            .collect(),
        verified_block_count: stream_summary.verified_block_count,
        reachable_record_count: rkey_digest.all_records_count,
        mst_root_matches_commit: true,
        repo_commit_signature_verified: false,
        identity_verified: false,
    };

    Ok(ParsedRepo {
        commit: CommitMeta {
            cid: commit_cid.to_string(),
            did: commit.did().as_str().to_owned(),
            version: commit.version,
            rev: commit.rev().as_str().to_owned(),
            data: commit.data().to_string(),
            prev: commit.prev().map(ToString::to_string),
        },
        completeness: proof,
        posts,
        rkey_digest,
        profile,
        profile_decode_error,
        record_decode_errors,
    })
}

async fn verify_streamed_car(
    car_path: &Path,
    config: ParseConfig,
    deadline: ParseDeadline,
) -> Result<CarStreamSummary, ParseError> {
    let mut stream = car::stream_car(car_path).await?;
    let roots = stream.roots().to_vec();
    let mut verified_block_count = 0_u64;

    while let Some((cid, bytes)) = stream.next().await? {
        ensure_usize_at_most(
            bytes.len(),
            config.max_block_bytes,
            "max_block_bytes",
            "raise parser max_block_bytes only for a known-good repo",
        )?;
        verify_block_cid(cid, bytes.as_ref())?;
        verified_block_count = checked_increment(verified_block_count, "verified_block_count")?;
        deadline.ensure_not_exceeded()?;
        ensure_u64_at_most(
            verified_block_count,
            config.max_car_blocks,
            "max_car_blocks",
            "raise parser max_car_blocks only for a known-good repo",
        )?;
    }

    Ok(CarStreamSummary {
        roots,
        verified_block_count,
    })
}

fn single_car_root(roots: &[IpldCid]) -> Result<IpldCid, ParseError> {
    match roots {
        [] => Err(ParseError::InvalidRoots(
            "CAR header has no roots".to_owned(),
        )),
        [root] => Ok(*root),
        _many => Err(ParseError::Unsupported {
            feature: "multi-root repo CAR",
        }),
    }
}

async fn load_commit(
    root: IpldCid,
    store: &IndexedCarBlockStore,
) -> Result<(IpldCid, Commit<SmolStr>), ParseError> {
    let Some(bytes) = store.get(&root).await? else {
        return Err(ParseError::CommitNotFound {
            root: root.to_string(),
        });
    };

    let commit = Commit::<SmolStr>::from_cbor(bytes.as_ref()).map_err(|source| {
        ParseError::RootCommitDecode {
            root: root.to_string(),
            message: source.to_string(),
        }
    })?;
    Ok((root, commit))
}

fn assert_requested_did(requested_did: Option<&str>, actual_did: &str) -> Result<(), ParseError> {
    let Some(requested) = requested_did else {
        return Ok(());
    };
    if requested == actual_did {
        return Ok(());
    }

    Err(ParseError::CommitDidMismatch {
        requested: requested.to_owned(),
        actual: actual_did.to_owned(),
    })
}

async fn walk_mst_records(
    root: IpldCid,
    store: &IndexedCarBlockStore,
    config: ParseConfig,
    deadline: ParseDeadline,
) -> Result<
    (
        Vec<PostRecord>,
        Option<ProfileRecord>,
        Option<String>,
        Vec<RecordDecodeFailure>,
        RkeyDigest,
    ),
    ParseError,
> {
    let mst = Mst::load(Arc::new(store.clone()), root, None);
    let traversed_root = mst.root().await?;
    if traversed_root != root {
        return Err(ParseError::MstRootMismatch {
            commit_data: root.to_string(),
            traversed_root: traversed_root.to_string(),
        });
    }

    let mut cursor = MstCursor::new(mst);
    let mut posts = Vec::new();
    let mut profile = None;
    let mut profile_decode_error = None;
    let mut record_decode_errors = Vec::new();
    let mut digest = RkeyDigest::default();

    cursor.advance().await?;
    while !cursor.is_end() {
        ensure_usize_at_most(
            cursor.layer().await?,
            config.max_mst_depth,
            "max_mst_depth",
            "raise parser max_mst_depth only after inspecting the repo MST",
        )?;
        deadline.ensure_not_exceeded()?;
        match cursor.current().clone() {
            CursorPosition::Leaf { key, cid } => {
                let key_text = key.to_string();
                let record_bytes =
                    store
                        .get(&cid)
                        .await?
                        .ok_or_else(|| ParseError::MissingBlock {
                            cid: cid.to_string(),
                        })?;
                update_digest(&mut digest, &key_text, config)?;
                let mut sinks = RecordSinks {
                    posts: &mut posts,
                    profile: &mut profile,
                    profile_decode_error: &mut profile_decode_error,
                    record_decode_errors: &mut record_decode_errors,
                };
                extract_known_record(&key_text, cid, record_bytes.as_ref(), &mut sinks, config)?;
                cursor.advance().await?;
            }
            CursorPosition::Tree { .. } => {
                cursor.advance().await?;
            }
            CursorPosition::End => {}
        }
    }

    Ok((
        posts,
        profile,
        profile_decode_error,
        record_decode_errors,
        digest,
    ))
}

fn extract_known_record(
    key: &str,
    cid: IpldCid,
    record_bytes: &[u8],
    sinks: &mut RecordSinks<'_>,
    config: ParseConfig,
) -> Result<(), ParseError> {
    let Some((collection, rkey)) = split_repo_key(key) else {
        return Ok(());
    };

    match collection {
        POST_COLLECTION => match serde_ipld_dagcbor::from_slice::<Post<SmolStr>>(record_bytes) {
            Ok(record) => sinks.posts.push(PostRecord {
                rkey: rkey.to_owned(),
                cid: cid.to_string(),
                record,
            }),
            Err(error) => {
                sinks.record_decode_errors.push(RecordDecodeFailure {
                    key: key.to_owned(),
                    collection: POST_COLLECTION,
                    cid: cid.to_string(),
                    message: error.to_string(),
                });
                enforce_decode_error_limit(
                    sinks.record_decode_errors.len(),
                    config.max_decode_errors,
                )?;
            }
        },
        PROFILE_COLLECTION if rkey == PROFILE_RKEY => {
            match serde_ipld_dagcbor::from_slice::<Profile<SmolStr>>(record_bytes) {
                Ok(record) => {
                    *sinks.profile = Some(ProfileRecord {
                        rkey: rkey.to_owned(),
                        cid: cid.to_string(),
                        record,
                    });
                }
                Err(error) => {
                    let message = error.to_string();
                    *sinks.profile_decode_error =
                        Some(format!("{PROFILE_COLLECTION}/{rkey} at {cid}: {message}"));
                    sinks.record_decode_errors.push(RecordDecodeFailure {
                        key: key.to_owned(),
                        collection: PROFILE_COLLECTION,
                        cid: cid.to_string(),
                        message,
                    });
                    enforce_decode_error_limit(
                        sinks.record_decode_errors.len(),
                        config.max_decode_errors,
                    )?;
                }
            }
        }
        _other => {}
    }

    Ok(())
}

struct RecordSinks<'a> {
    posts: &'a mut Vec<PostRecord>,
    profile: &'a mut Option<ProfileRecord>,
    profile_decode_error: &'a mut Option<String>,
    record_decode_errors: &'a mut Vec<RecordDecodeFailure>,
}

fn update_digest(
    digest: &mut RkeyDigest,
    key: &str,
    config: ParseConfig,
) -> Result<(), ParseError> {
    digest.all_records_count = checked_increment(digest.all_records_count, "all_records_count")?;
    ensure_u64_at_most(
        digest.all_records_count,
        config.max_records,
        "max_records",
        "raise parser max_records only for a known-good repo",
    )?;
    if digest.first_key.is_none() {
        digest.first_key = Some(key.to_owned());
    }
    digest.last_key = Some(key.to_owned());

    if key.starts_with(POST_PREFIX) {
        digest.post_records_count =
            checked_increment(digest.post_records_count, "post_records_count")?;
    }

    Ok(())
}

fn split_repo_key(key: &str) -> Option<(&str, &str)> {
    key.split_once('/')
}

fn verify_block_cid(cid: IpldCid, data: &[u8]) -> Result<(), ParseError> {
    let codec = cid.codec();
    if codec != DAG_CBOR_CID_CODEC {
        return Err(ParseError::UnsupportedCodec {
            cid: cid.to_string(),
            codec,
        });
    }

    let computed_cid = compute_cid(data)?;
    if computed_cid != cid {
        return Err(ParseError::CidMismatch {
            block_cid: cid.to_string(),
            computed_cid: computed_cid.to_string(),
        });
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct IndexedCarBlockStore {
    path: Arc<PathBuf>,
    index: Arc<BTreeMap<IpldCid, BlockLocation>>,
}

impl IndexedCarBlockStore {
    fn load(path: &Path, config: ParseConfig) -> Result<Self, ParseError> {
        let index = index_car_blocks(path, config)?;
        Ok(Self {
            path: Arc::new(path.to_path_buf()),
            index: Arc::new(index),
        })
    }
}

#[allow(clippy::unused_async_trait_impl)]
impl BlockStore for IndexedCarBlockStore {
    async fn get(&self, cid: &IpldCid) -> jacquard_repo::Result<Option<Bytes>> {
        let Some(location) = self.index.get(cid) else {
            return Ok(None);
        };
        read_block_at(&self.path, location)
            .map(Bytes::from)
            .map(Some)
            .map_err(RepoError::io)
    }

    async fn put(&self, _data: &[u8]) -> jacquard_repo::Result<IpldCid> {
        Err(read_only_store_error())
    }

    async fn has(&self, cid: &IpldCid) -> jacquard_repo::Result<bool> {
        Ok(self.index.contains_key(cid))
    }

    async fn put_many(
        &self,
        _blocks: impl IntoIterator<Item = (IpldCid, Bytes)> + Send,
    ) -> jacquard_repo::Result<()> {
        Err(read_only_store_error())
    }

    async fn get_many(&self, cids: &[IpldCid]) -> jacquard_repo::Result<Vec<Option<Bytes>>> {
        let mut blocks = Vec::with_capacity(cids.len());
        for cid in cids {
            blocks.push(self.get(cid).await?);
        }
        Ok(blocks)
    }

    async fn apply_commit(&self, _commit: jacquard_repo::CommitData) -> jacquard_repo::Result<()> {
        Err(read_only_store_error())
    }
}

fn index_car_blocks(
    path: &Path,
    config: ParseConfig,
) -> Result<BTreeMap<IpldCid, BlockLocation>, ParseError> {
    let mut file = open_file(path)?;
    let Some(header_len) = read_varint(&mut file)? else {
        return Err(ParseError::InvalidRoots("CAR file is empty".to_owned()));
    };
    let mut offset = checked_add_u64(header_len.bytes_read, header_len.value, "header")?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|source| ParseError::Io {
            path: path.to_path_buf(),
            source,
        })?;

    let mut index = BTreeMap::new();
    let mut indexed_block_count = 0_u64;
    while let Some(section_len) = read_varint(&mut file)? {
        offset = checked_add_u64(offset, section_len.bytes_read, "section varint")?;
        let section_start = offset;
        ensure_u64_at_most(
            section_len.value,
            config.max_block_bytes,
            "max_block_bytes",
            "raise parser max_block_bytes only for a known-good repo",
        )?;
        let section_len_usize =
            usize::try_from(section_len.value).map_err(|_err| ParseError::CarLengthOverflow {
                field: "section length",
            })?;
        let mut section = vec![0_u8; section_len_usize];
        file.read_exact(&mut section)
            .map_err(|source| ParseError::Io {
                path: path.to_path_buf(),
                source,
            })?;

        let mut cursor = Cursor::new(section.as_slice());
        let cid = IpldCid::read_bytes(&mut cursor)
            .map_err(|source| ParseError::CidRead(Box::new(source)))?;
        let cid_len = cursor.position();
        let data_len = section_len
            .value
            .checked_sub(cid_len)
            .ok_or(ParseError::MalformedCar(
                "block section shorter than CID".to_owned(),
            ))?;
        let data_start =
            usize::try_from(cid_len).map_err(|_err| ParseError::CarLengthOverflow {
                field: "CID length",
            })?;
        let data = section.get(data_start..).ok_or(ParseError::MalformedCar(
            "block data slice outside section".to_owned(),
        ))?;
        verify_block_cid(cid, data)?;

        match index.entry(cid) {
            Entry::Vacant(entry) => {
                entry.insert(BlockLocation {
                    offset: checked_add_u64(section_start, cid_len, "block data offset")?,
                    len: usize::try_from(data_len).map_err(|_err| {
                        ParseError::CarLengthOverflow {
                            field: "block data length",
                        }
                    })?,
                });
            }
            Entry::Occupied(_entry) => {}
        }

        indexed_block_count = checked_increment(indexed_block_count, "indexed_block_count")?;
        ensure_u64_at_most(
            indexed_block_count,
            config.max_car_blocks,
            "max_car_blocks",
            "raise parser max_car_blocks only for a known-good repo",
        )?;
        offset = checked_add_u64(section_start, section_len.value, "section end")?;
    }

    Ok(index)
}

fn read_block_at(path: &Path, location: &BlockLocation) -> std::io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(location.offset))?;
    let mut bytes = vec![0_u8; location.len];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn open_file(path: &Path) -> Result<File, ParseError> {
    File::open(path).map_err(|source| ParseError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn read_varint(reader: &mut impl Read) -> Result<Option<Varint>, ParseError> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    let mut bytes_read = 0_u64;

    loop {
        let mut one_byte = [0_u8; 1];
        let read = reader
            .read(&mut one_byte)
            .map_err(|source| ParseError::Io {
                path: PathBuf::from("<car varint>"),
                source,
            })?;
        if read == 0 {
            return if bytes_read == 0 {
                Ok(None)
            } else {
                Err(ParseError::MalformedVarint)
            };
        }

        let [byte] = one_byte;
        bytes_read = checked_increment(bytes_read, "varint bytes")?;
        let chunk =
            u64::from(byte & 0x7f)
                .checked_shl(shift)
                .ok_or(ParseError::CarLengthOverflow {
                    field: "varint shift",
                })?;
        value = checked_add_u64(value, chunk, "varint value")?;

        if byte & 0x80 == 0 {
            return Ok(Some(Varint { value, bytes_read }));
        }

        shift = shift.checked_add(7).ok_or(ParseError::CarLengthOverflow {
            field: "varint shift",
        })?;
        if shift >= 64 {
            return Err(ParseError::MalformedVarint);
        }
    }
}

fn checked_increment(value: u64, field: &'static str) -> Result<u64, ParseError> {
    value
        .checked_add(1)
        .ok_or(ParseError::ResourceCountOverflow { field })
}

fn checked_add_u64(lhs: u64, rhs: u64, field: &'static str) -> Result<u64, ParseError> {
    lhs.checked_add(rhs)
        .ok_or(ParseError::CarLengthOverflow { field })
}

const fn ensure_u64_at_most(
    observed: u64,
    limit: u64,
    limit_name: &'static str,
    recovery: &'static str,
) -> Result<(), ParseError> {
    if observed <= limit {
        return Ok(());
    }

    Err(ParseError::ResourceLimitExceeded {
        limit: limit_name,
        observed,
        recovery,
    })
}

fn ensure_usize_at_most(
    observed: usize,
    limit: u64,
    limit_name: &'static str,
    recovery: &'static str,
) -> Result<(), ParseError> {
    let observed_u64 = u64::try_from(observed).map_err(|_err| ParseError::CarLengthOverflow {
        field: "usize observed length",
    })?;
    ensure_u64_at_most(observed_u64, limit, limit_name, recovery)
}

fn enforce_decode_error_limit(observed: usize, limit: u64) -> Result<(), ParseError> {
    ensure_usize_at_most(
        observed,
        limit,
        "max_decode_errors",
        "raise parser max_decode_errors only after inspecting malformed records",
    )
}

#[derive(Debug, Clone, Copy)]
struct ParseDeadline {
    started_at: Instant,
    max_wall_clock: Duration,
}

impl ParseDeadline {
    fn start(max_wall_clock: Duration) -> Self {
        Self {
            started_at: Instant::now(),
            max_wall_clock,
        }
    }

    fn ensure_not_exceeded(self) -> Result<(), ParseError> {
        let elapsed = self.started_at.elapsed();
        if elapsed <= self.max_wall_clock {
            return Ok(());
        }
        Err(ParseError::ResourceLimitExceeded {
            limit: "max_parse_wall_clock",
            observed: elapsed.as_secs(),
            recovery: "raise parser max_parse_wall_clock only for a known-good repo",
        })
    }
}

fn read_only_store_error() -> RepoError {
    RepoError::storage(std::io::Error::other(
        "indexed CAR block store is read-only",
    ))
}

#[derive(Debug, Clone)]
struct CarStreamSummary {
    roots: Vec<IpldCid>,
    verified_block_count: u64,
}

#[derive(Debug, Clone, Copy)]
struct BlockLocation {
    offset: u64,
    len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Varint {
    value: u64,
    bytes_read: u64,
}

const POST_COLLECTION: &str = "app.bsky.feed.post";
const POST_PREFIX: &str = "app.bsky.feed.post/";
const PROFILE_COLLECTION: &str = "app.bsky.actor.profile";
const PROFILE_RKEY: &str = "self";

#[cfg(test)]
mod tests {
    use super::{
        ParseConfig, ParseError, RkeyDigest, Varint, assert_requested_did,
        enforce_decode_error_limit, read_varint, split_repo_key, update_digest,
    };

    #[test]
    fn splits_repo_key_into_collection_and_rkey() {
        assert_eq!(
            split_repo_key("app.bsky.feed.post/3kabc"),
            Some(("app.bsky.feed.post", "3kabc"))
        );
        assert_eq!(split_repo_key("app.bsky.feed.post"), None);
    }

    #[test]
    fn reads_multibyte_varint() {
        let mut bytes = [0xac, 0x02].as_slice();
        assert_eq!(
            read_varint(&mut bytes).unwrap(),
            Some(Varint {
                value: 300,
                bytes_read: 2
            })
        );
    }

    #[test]
    fn digest_tracks_first_last_and_post_counts() {
        let mut digest = RkeyDigest::default();
        let config = ParseConfig::default();

        update_digest(&mut digest, "app.bsky.actor.profile/self", config).unwrap();
        update_digest(&mut digest, "app.bsky.feed.post/3kabc", config).unwrap();

        assert_eq!(digest.all_records_count, 2);
        assert_eq!(digest.post_records_count, 1);
        assert_eq!(
            digest.first_key.as_deref(),
            Some("app.bsky.actor.profile/self")
        );
        assert_eq!(digest.last_key.as_deref(), Some("app.bsky.feed.post/3kabc"));
    }

    #[test]
    fn requested_did_mismatch_is_loud() {
        let error = assert_requested_did(Some("did:plc:requested"), "did:plc:actual")
            .expect_err("mismatch should fail");

        assert!(matches!(
            error,
            ParseError::CommitDidMismatch {
                requested,
                actual
            } if requested == "did:plc:requested" && actual == "did:plc:actual"
        ));
    }

    #[test]
    fn record_limit_is_loud() {
        let mut digest = RkeyDigest::default();
        let config = ParseConfig {
            max_records: 1,
            ..ParseConfig::default()
        };

        update_digest(&mut digest, "app.bsky.feed.post/3kabc", config).unwrap();
        let error = update_digest(&mut digest, "app.bsky.feed.post/3kdef", config)
            .expect_err("second record should exceed cap");

        assert!(matches!(
            error,
            ParseError::ResourceLimitExceeded {
                limit: "max_records",
                observed: 2,
                ..
            }
        ));
    }

    #[test]
    fn decode_error_limit_is_loud() {
        let error = enforce_decode_error_limit(2, 1).expect_err("decode cap should fail");

        assert!(matches!(
            error,
            ParseError::ResourceLimitExceeded {
                limit: "max_decode_errors",
                observed: 2,
                ..
            }
        ));
    }
}

```

---

## `rust/crates/emojistats-backfill/src/archive.rs`

```rust
//! Archive receipt, `Parquet`, and manifest primitives for the `fetch-one` vertical slice.

use std::{
    error::Error,
    fmt, fs,
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use unicode_segmentation::UnicodeSegmentation;

use crate::parse::{ParsedRepo, PostRecord, ProfileRecord};

const POST_COLLECTION: &str = "app.bsky.feed.post";
const ARCHIVE_SCHEMA_VERSION: u16 = 1;
const PARQUET_BATCH_ROWS: usize = 1_024;
const HASH_BUFFER_BYTES: usize = 65_536;

/// Version identity for emoji normalization outputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizerVersion {
    pub name: String,
    pub semver: String,
    pub git_rev: String,
    pub unicode_version: String,
    pub emoji_data_version: String,
}

/// Data-model-lossless post row before `Parquet` encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchivePostRow {
    pub did: String,
    pub rkey: String,
    pub cid: String,
    pub normalizer: NormalizerVersion,
    pub account_status: Option<String>,
    pub record_status: Option<String>,
    pub public_content_label: Option<String>,
    pub created_at_raw: Option<String>,
    pub created_at_normalized: Option<String>,
    pub created_at_parse_status: CreatedAtParseStatus,
    pub text: String,
    pub langs: Vec<String>,
    pub emoji_sequence: Vec<String>,
    pub extras_json: serde_json::Value,
}

/// Compact local serving projection row derived from an archive row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmojiProjectionRow {
    pub did: String,
    pub rkey: String,
    pub created_at_normalized: Option<String>,
    pub emoji: String,
    pub occurrences: u64,
    pub langs: Vec<String>,
}

/// Classification for author-supplied `createdAt`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CreatedAtParseStatus {
    Valid,
    Missing,
    Invalid,
    Future,
}

/// Receipt over the rows produced for one fetched repo.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoReceipt {
    pub fetch_method: FetchMethod,
    pub completeness_class: CompletenessClass,
    pub reachable_records_count: u64,
    pub reachable_post_records_count: u64,
    pub archived_post_rows_count: u64,
    pub post_decode_error_count: u64,
    pub emoji_posts_count: u64,
    pub emoji_occurrences_count: u64,
    pub mst_root_cid: Option<String>,
    pub commit_cid: Option<String>,
    pub archive_rows_hash: String,
    pub post_rows_hash: String,
    pub emoji_projection_hash: String,
    pub profile_row_hash: Option<String>,
    pub normalizer: NormalizerVersion,
    pub repo_commit_signature_verified: bool,
    pub identity_verified: bool,
}

/// Inputs required to build one repo receipt.
#[derive(Debug, Clone)]
pub struct RepoReceiptInput<'a> {
    pub rows: &'a [ArchivePostRow],
    pub reachable_records_count: u64,
    pub reachable_post_records_count: u64,
    pub post_decode_error_count: u64,
    pub profile_row_hash: Option<String>,
    pub mst_root_cid: Option<String>,
    pub commit_cid: Option<String>,
    pub normalizer: NormalizerVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchMethod {
    GetRepo,
    ListRecords,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletenessClass {
    SnapshotComplete,
    CollectionPaginated,
}

/// Local manifest entry before the Storage Box commit protocol exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalManifestEntry {
    pub run_id: String,
    pub shard: String,
    pub file_sequence: u64,
    pub dataset: String,
    pub local_path: PathBuf,
    pub row_count: u64,
    pub bytes: u64,
    pub content_hash: String,
    pub min_created_at_normalized: Option<String>,
    pub max_created_at_normalized: Option<String>,
    pub receipt_hash: String,
    pub schema_version: u16,
    pub normalizer: NormalizerVersion,
}

/// Files produced by Stage D for one `fetch-one` run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveArtifacts {
    pub parquet_path: PathBuf,
    pub receipt_path: PathBuf,
    pub manifest_path: PathBuf,
    pub emoji_projection_path: PathBuf,
    pub manifest: LocalManifestEntry,
    pub emoji_rows: u64,
}

/// Stage D archive/derive failures.
#[derive(Debug)]
pub enum ArchiveError {
    Io(io::Error),
    Parquet(parquet::errors::ParquetError),
    Arrow(arrow_schema::ArrowError),
    Json(serde_json::Error),
    CountOverflow { field: &'static str },
    InvalidCompression(String),
    InvalidPath { path: PathBuf },
    InvalidRecordJson,
}

/// Convert parsed post records into the first archive-row shape.
///
/// # Errors
///
/// Returns [`ArchiveError`] if record extras cannot be serialized without loss.
pub fn archive_rows_from_parsed_repo(
    parsed: &ParsedRepo,
) -> Result<Vec<ArchivePostRow>, ArchiveError> {
    let normalizer = current_normalizer();
    parsed
        .posts
        .iter()
        .map(|post| {
            let created_at = post.record.created_at.as_str();
            let classified = classify_created_at(Some(created_at));
            Ok(ArchivePostRow {
                did: parsed.commit.did.clone(),
                rkey: post.rkey.clone(),
                cid: post.cid.clone(),
                normalizer: normalizer.clone(),
                account_status: None,
                record_status: None,
                public_content_label: None,
                created_at_raw: classified.raw,
                created_at_normalized: classified.normalized,
                created_at_parse_status: classified.status,
                text: post.record.text.to_string(),
                langs: post.record.langs.as_ref().map_or_else(Vec::new, |langs| {
                    langs.iter().map(ToString::to_string).collect()
                }),
                emoji_sequence: extract_emojis(post.record.text.as_str()),
                extras_json: record_extras_json(post)?,
            })
        })
        .collect()
}

/// Current vertical-slice normalizer identity.
#[must_use]
pub fn current_normalizer() -> NormalizerVersion {
    NormalizerVersion {
        name: "emoji-normalizer-rust-minimal".to_owned(),
        semver: "0.1.0".to_owned(),
        git_rev: option_env!("GIT_REV").unwrap_or("unknown").to_owned(),
        unicode_version: "emoji-rs".to_owned(),
        emoji_data_version: "emoji-rs".to_owned(),
    }
}

/// Write local archive artifacts for one parsed repo.
///
/// # Errors
///
/// Returns [`ArchiveError`] if local filesystem, `Parquet`, `Arrow`, serialization, or
/// resource-count work fails.
pub fn write_archive_artifacts(
    output_dir: &Path,
    did: &str,
    rows: &[ArchivePostRow],
    receipt: &RepoReceipt,
) -> Result<ArchiveArtifacts, ArchiveError> {
    fs::create_dir_all(output_dir)?;
    let safe_did = safe_file_component(did);
    let parquet_path = output_dir.join(format!("{safe_did}.posts.parquet"));
    let receipt_path = output_dir.join(format!("{safe_did}.receipt.json"));
    let manifest_path = output_dir.join(format!("{safe_did}.manifest.json"));
    let emoji_projection_path = output_dir.join(format!("{safe_did}.emoji.jsonl"));

    write_temp_rename(&parquet_path, |path| write_posts_parquet(path, rows))?;
    write_temp_rename(&receipt_path, |path| write_json_pretty(path, receipt))?;
    let emoji_projection_rows = derive_emoji_projection_rows(rows)?;
    let emoji_rows = u64::try_from(emoji_projection_rows.len()).map_err(|_error| {
        ArchiveError::CountOverflow {
            field: "emoji_rows",
        }
    })?;
    write_temp_rename(&emoji_projection_path, |path| {
        write_emoji_projection_jsonl(path, &emoji_projection_rows)
    })?;

    let manifest = build_manifest(&parquet_path, rows, receipt)?;
    write_temp_rename(&manifest_path, |path| write_json_pretty(path, &manifest))?;

    Ok(ArchiveArtifacts {
        parquet_path,
        receipt_path,
        manifest_path,
        emoji_projection_path,
        manifest,
        emoji_rows,
    })
}

/// Build a content receipt from already-normalized post rows.
///
/// # Errors
///
/// Returns [`ArchiveError`] if any counter or hash length overflows the receipt schema.
pub fn build_repo_receipt(input: RepoReceiptInput<'_>) -> Result<RepoReceipt, ArchiveError> {
    let rows = input.rows;
    let post_rows_hash = hash_post_rows(rows)?;
    let emoji_projection_rows = derive_emoji_projection_rows(rows)?;
    let emoji_projection_hash = hash_emoji_projection_rows(&emoji_projection_rows)?;
    Ok(RepoReceipt {
        fetch_method: FetchMethod::GetRepo,
        completeness_class: CompletenessClass::SnapshotComplete,
        reachable_records_count: input.reachable_records_count,
        reachable_post_records_count: input.reachable_post_records_count,
        archived_post_rows_count: u64::try_from(rows.len()).map_err(|_error| {
            ArchiveError::CountOverflow {
                field: "archived_post_rows_count",
            }
        })?,
        post_decode_error_count: input.post_decode_error_count,
        emoji_posts_count: count_emoji_posts(rows)?,
        emoji_occurrences_count: count_emoji_occurrences(rows)?,
        mst_root_cid: input.mst_root_cid,
        commit_cid: input.commit_cid,
        archive_rows_hash: post_rows_hash.clone(),
        post_rows_hash,
        emoji_projection_hash,
        profile_row_hash: input.profile_row_hash,
        normalizer: input.normalizer,
        repo_commit_signature_verified: false,
        identity_verified: false,
    })
}

/// Hash the canonical row content named in `docs/backfill-v2-design.md`.
///
/// # Errors
///
/// Returns [`ArchiveError`] if any hashed string length cannot fit the stable hash framing.
pub fn hash_post_rows(rows: &[ArchivePostRow]) -> Result<String, ArchiveError> {
    let mut hasher = Sha256::new();
    for row in rows {
        hash_field(&mut hasher, POST_COLLECTION)?;
        hash_field(&mut hasher, &row.did)?;
        hash_field(&mut hasher, &row.rkey)?;
        hash_field(&mut hasher, &row.cid)?;
        hash_normalizer(&mut hasher, &row.normalizer)?;
        hash_optional_field(&mut hasher, row.account_status.as_deref())?;
        hash_optional_field(&mut hasher, row.record_status.as_deref())?;
        hash_optional_field(&mut hasher, row.public_content_label.as_deref())?;
        hash_optional_field(&mut hasher, row.created_at_raw.as_deref())?;
        hash_optional_field(&mut hasher, row.created_at_normalized.as_deref())?;
        hash_field(&mut hasher, row.created_at_parse_status.as_str())?;
        hash_field(&mut hasher, &row.text)?;
        hash_string_slice(&mut hasher, &row.langs)?;
        hash_string_slice(&mut hasher, &row.emoji_sequence)?;
        hash_field(&mut hasher, &canonical_json(&row.extras_json)?)?;
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Hash a profile sidecar row when Stage C extracted one.
///
/// # Errors
///
/// Returns [`ArchiveError`] if the profile row cannot be serialized without loss.
pub fn hash_profile_record(
    profile: Option<&ProfileRecord>,
) -> Result<Option<String>, ArchiveError> {
    profile.map(hash_one_profile_record).transpose()
}

fn hash_one_profile_record(profile: &ProfileRecord) -> Result<String, ArchiveError> {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, &profile.rkey)?;
    hash_field(&mut hasher, &profile.cid)?;
    hash_field(&mut hasher, &json_string(&profile.record)?)?;
    Ok(hex::encode(hasher.finalize()))
}

fn hash_emoji_projection_rows(rows: &[EmojiProjectionRow]) -> Result<String, ArchiveError> {
    let mut hasher = Sha256::new();
    for row in rows {
        hash_field(&mut hasher, &json_string(row)?)?;
    }
    Ok(hex::encode(hasher.finalize()))
}

fn write_posts_parquet(path: &Path, rows: &[ArchivePostRow]) -> Result<(), ArchiveError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("did", DataType::Utf8, false),
        Field::new("rkey", DataType::Utf8, false),
        Field::new("cid", DataType::Utf8, false),
        Field::new("normalizer_name", DataType::Utf8, false),
        Field::new("normalizer_semver", DataType::Utf8, false),
        Field::new("normalizer_git_rev", DataType::Utf8, false),
        Field::new("normalizer_unicode_version", DataType::Utf8, false),
        Field::new("normalizer_emoji_data_version", DataType::Utf8, false),
        Field::new("account_status", DataType::Utf8, true),
        Field::new("record_status", DataType::Utf8, true),
        Field::new("public_content_label", DataType::Utf8, true),
        Field::new("created_at_raw", DataType::Utf8, true),
        Field::new("created_at_normalized", DataType::Utf8, true),
        Field::new("created_at_parse_status", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("langs_json", DataType::Utf8, false),
        Field::new("emoji_sequence_json", DataType::Utf8, false),
        Field::new("extras_json", DataType::Utf8, false),
    ]));

    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3)
                .map_err(|error| ArchiveError::InvalidCompression(error.to_string()))?,
        ))
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))?;
    for chunk in rows.chunks(PARQUET_BATCH_ROWS) {
        let batch = post_record_batch(&schema, chunk)?;
        writer.write(&batch)?;
    }
    writer.close()?;
    sync_file(path)?;
    Ok(())
}

fn post_record_batch(
    schema: &Arc<Schema>,
    rows: &[ArchivePostRow],
) -> Result<RecordBatch, ArchiveError> {
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            string_array(rows.iter().map(|row| Some(row.did.as_str()))),
            string_array(rows.iter().map(|row| Some(row.rkey.as_str()))),
            string_array(rows.iter().map(|row| Some(row.cid.as_str()))),
            string_array(rows.iter().map(|row| Some(row.normalizer.name.as_str()))),
            string_array(rows.iter().map(|row| Some(row.normalizer.semver.as_str()))),
            string_array(rows.iter().map(|row| Some(row.normalizer.git_rev.as_str()))),
            string_array(
                rows.iter()
                    .map(|row| Some(row.normalizer.unicode_version.as_str())),
            ),
            string_array(
                rows.iter()
                    .map(|row| Some(row.normalizer.emoji_data_version.as_str())),
            ),
            string_array(rows.iter().map(|row| row.account_status.as_deref())),
            string_array(rows.iter().map(|row| row.record_status.as_deref())),
            string_array(rows.iter().map(|row| row.public_content_label.as_deref())),
            string_array(rows.iter().map(|row| row.created_at_raw.as_deref())),
            string_array(rows.iter().map(|row| row.created_at_normalized.as_deref())),
            string_array(
                rows.iter()
                    .map(|row| Some(row.created_at_parse_status.as_str())),
            ),
            string_array(rows.iter().map(|row| Some(row.text.as_str()))),
            owned_string_array(rows.iter().map(|row| json_string(&row.langs)))?,
            owned_string_array(rows.iter().map(|row| json_string(&row.emoji_sequence)))?,
            owned_string_array(rows.iter().map(|row| canonical_json(&row.extras_json)))?,
        ],
    )?)
}

fn build_manifest(
    parquet_path: &Path,
    rows: &[ArchivePostRow],
    receipt: &RepoReceipt,
) -> Result<LocalManifestEntry, ArchiveError> {
    let metadata = fs::metadata(parquet_path)?;
    Ok(LocalManifestEntry {
        run_id: "fetch-one-local".to_owned(),
        shard: "single".to_owned(),
        file_sequence: 1,
        dataset: "raw_archive_posts".to_owned(),
        local_path: parquet_path.to_path_buf(),
        row_count: u64::try_from(rows.len())
            .map_err(|_error| ArchiveError::CountOverflow { field: "row_count" })?,
        bytes: metadata.len(),
        content_hash: hash_file(parquet_path)?,
        min_created_at_normalized: min_created_at(rows),
        max_created_at_normalized: max_created_at(rows),
        receipt_hash: hash_serialized(receipt)?,
        schema_version: ARCHIVE_SCHEMA_VERSION,
        normalizer: receipt.normalizer.clone(),
    })
}

fn write_emoji_projection_jsonl(
    path: &Path,
    rows: &[EmojiProjectionRow],
) -> Result<(), ArchiveError> {
    let mut file = File::create(path)?;
    for row in rows {
        serde_json::to_writer(&mut file, row)?;
        file.write_all(b"\n")?;
    }
    file.sync_all()?;
    Ok(())
}

fn derive_emoji_projection_rows(
    rows: &[ArchivePostRow],
) -> Result<Vec<EmojiProjectionRow>, ArchiveError> {
    let mut projected = Vec::new();
    for row in rows {
        projected.extend(emoji_projection_rows(row)?);
    }
    Ok(projected)
}

fn emoji_projection_rows(row: &ArchivePostRow) -> Result<Vec<EmojiProjectionRow>, ArchiveError> {
    let mut rows = Vec::new();
    for emoji in &row.emoji_sequence {
        if let Some(existing) = rows
            .iter_mut()
            .find(|candidate: &&mut EmojiProjectionRow| candidate.emoji == *emoji)
        {
            existing.occurrences =
                existing
                    .occurrences
                    .checked_add(1)
                    .ok_or(ArchiveError::CountOverflow {
                        field: "emoji_occurrences",
                    })?;
        } else {
            rows.push(EmojiProjectionRow {
                did: row.did.clone(),
                rkey: row.rkey.clone(),
                created_at_normalized: row.created_at_normalized.clone(),
                emoji: emoji.clone(),
                occurrences: 1,
                langs: row.langs.clone(),
            });
        }
    }
    Ok(rows)
}

fn extract_emojis(text: &str) -> Vec<String> {
    text.graphemes(true)
        .filter(|grapheme| emojis::get(grapheme).is_some())
        .map(ToOwned::to_owned)
        .collect()
}

fn count_emoji_posts(rows: &[ArchivePostRow]) -> Result<u64, ArchiveError> {
    u64::try_from(
        rows.iter()
            .filter(|row| !row.emoji_sequence.is_empty())
            .count(),
    )
    .map_err(|_error| ArchiveError::CountOverflow {
        field: "emoji_posts_count",
    })
}

fn count_emoji_occurrences(rows: &[ArchivePostRow]) -> Result<u64, ArchiveError> {
    rows.iter().try_fold(0_u64, |accumulator, row| {
        let row_count = u64::try_from(row.emoji_sequence.len()).map_err(|_error| {
            ArchiveError::CountOverflow {
                field: "emoji_occurrences_count",
            }
        })?;
        accumulator
            .checked_add(row_count)
            .ok_or(ArchiveError::CountOverflow {
                field: "emoji_occurrences_count",
            })
    })
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from(values.collect::<Vec<_>>()))
}

fn owned_string_array(
    values: impl Iterator<Item = Result<String, ArchiveError>>,
) -> Result<ArrayRef, ArchiveError> {
    Ok(Arc::new(StringArray::from(
        values.collect::<Result<Vec<_>, _>>()?,
    )))
}

fn json_string<T: Serialize>(value: &T) -> Result<String, ArchiveError> {
    Ok(serde_json::to_string(value)?)
}

fn write_json_pretty<T: Serialize>(path: &Path, value: &T) -> Result<(), ArchiveError> {
    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn hash_file(path: &Path) -> Result<String, ArchiveError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let chunk = buffer.get(..read).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "hash read exceeded buffer")
        })?;
        hasher.update(chunk);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn hash_serialized<T: Serialize>(value: &T) -> Result<String, ArchiveError> {
    let bytes = serde_json::to_vec(value)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(hex::encode(hasher.finalize()))
}

fn min_created_at(rows: &[ArchivePostRow]) -> Option<String> {
    rows.iter()
        .filter_map(|row| row.created_at_normalized.as_deref())
        .min()
        .map(ToOwned::to_owned)
}

fn max_created_at(rows: &[ArchivePostRow]) -> Option<String> {
    rows.iter()
        .filter_map(|row| row.created_at_normalized.as_deref())
        .max()
        .map(ToOwned::to_owned)
}

fn safe_file_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn hash_string_slice(hasher: &mut Sha256, values: &[String]) -> Result<(), ArchiveError> {
    for value in values {
        hash_field(hasher, value)?;
    }
    hash_field(hasher, "")
}

fn hash_optional_field(hasher: &mut Sha256, value: Option<&str>) -> Result<(), ArchiveError> {
    match value {
        Some(value) => {
            hash_field(hasher, "some")?;
            hash_field(hasher, value)
        }
        None => hash_field(hasher, "none"),
    }
}

fn hash_normalizer(
    hasher: &mut Sha256,
    normalizer: &NormalizerVersion,
) -> Result<(), ArchiveError> {
    hash_field(hasher, &normalizer.name)?;
    hash_field(hasher, &normalizer.semver)?;
    hash_field(hasher, &normalizer.git_rev)?;
    hash_field(hasher, &normalizer.unicode_version)?;
    hash_field(hasher, &normalizer.emoji_data_version)
}

fn hash_field(hasher: &mut Sha256, value: &str) -> Result<(), ArchiveError> {
    let len = u64::try_from(value.len()).map_err(|_error| ArchiveError::CountOverflow {
        field: "hash_field_length",
    })?;
    hasher.update(len.to_be_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

fn canonical_json(value: &serde_json::Value) -> Result<String, ArchiveError> {
    Ok(serde_json::to_string(value)?)
}

fn record_extras_json(post: &PostRecord) -> Result<serde_json::Value, ArchiveError> {
    let mut extras = serde_json::Map::new();
    insert_optional_json(&mut extras, "embed", post.record.embed.as_ref())?;
    insert_optional_json(&mut extras, "facets", post.record.facets.as_ref())?;
    insert_optional_json(&mut extras, "labels", post.record.labels.as_ref())?;
    insert_optional_json(&mut extras, "reply", post.record.reply.as_ref())?;
    insert_optional_json(&mut extras, "tags", post.record.tags.as_ref())?;
    insert_optional_json(&mut extras, "extra_data", post.record.extra_data.as_ref())?;
    Ok(serde_json::Value::Object(extras))
}

fn insert_optional_json<T: Serialize>(
    target: &mut serde_json::Map<String, serde_json::Value>,
    key: &'static str,
    value: Option<&T>,
) -> Result<(), ArchiveError> {
    if let Some(value) = value {
        target.insert(key.to_owned(), serde_json::to_value(value)?);
    }
    Ok(())
}

struct ClassifiedCreatedAt {
    raw: Option<String>,
    normalized: Option<String>,
    status: CreatedAtParseStatus,
}

fn classify_created_at(value: Option<&str>) -> ClassifiedCreatedAt {
    value.map_or_else(
        || ClassifiedCreatedAt {
            raw: None,
            normalized: None,
            status: CreatedAtParseStatus::Missing,
        },
        classify_present_created_at,
    )
}

fn classify_present_created_at(raw: &str) -> ClassifiedCreatedAt {
    let timestamp = parse_rfc3339_epoch_seconds(raw);
    let now = current_epoch_seconds();
    let status = match (timestamp, now) {
        (Some(timestamp), Some(now)) if timestamp > now => CreatedAtParseStatus::Future,
        (Some(_timestamp), _now) => CreatedAtParseStatus::Valid,
        (None, _now) => CreatedAtParseStatus::Invalid,
    };
    let normalized = if status == CreatedAtParseStatus::Valid {
        Some(raw.to_owned())
    } else {
        None
    };
    ClassifiedCreatedAt {
        raw: Some(raw.to_owned()),
        normalized,
        status,
    }
}

fn current_epoch_seconds() -> Option<i64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    i64::try_from(duration.as_secs()).ok()
}

fn parse_rfc3339_epoch_seconds(value: &str) -> Option<i64> {
    let mut chars = value.chars();
    let year = read_digits(&mut chars, 4)?;
    expect_char(&mut chars, '-')?;
    let month = read_digits(&mut chars, 2)?;
    expect_char(&mut chars, '-')?;
    let day = read_digits(&mut chars, 2)?;
    expect_char(&mut chars, 'T')?;
    let hour = read_digits(&mut chars, 2)?;
    expect_char(&mut chars, ':')?;
    let minute = read_digits(&mut chars, 2)?;
    expect_char(&mut chars, ':')?;
    let second = read_digits(&mut chars, 2)?;
    let timezone = read_timezone(&mut chars)?;
    validate_datetime_parts(year, month, day, hour, minute, second)?;
    let days = days_from_civil(year, month, day)?;
    let day_seconds = days.checked_mul(86_400)?;
    let hour_seconds = hour.checked_mul(3_600)?;
    let minute_seconds = minute.checked_mul(60)?;
    day_seconds
        .checked_add(hour_seconds)?
        .checked_add(minute_seconds)?
        .checked_add(second)?
        .checked_sub(timezone)
}

fn read_timezone(chars: &mut std::str::Chars<'_>) -> Option<i64> {
    let next = chars.next()?;
    match next {
        '.' => {
            read_fraction(chars)?;
            read_timezone(chars)
        }
        'Z' => {
            ensure_finished(chars)?;
            Some(0)
        }
        '+' | '-' => {
            let sign = if next == '+' { 1_i64 } else { -1_i64 };
            let hour = read_digits(chars, 2)?;
            expect_char(chars, ':')?;
            let minute = read_digits(chars, 2)?;
            ensure_finished(chars)?;
            validate_timezone(hour, minute)?;
            hour.checked_mul(3_600)?
                .checked_add(minute.checked_mul(60)?)?
                .checked_mul(sign)
        }
        _other => None,
    }
}

fn read_fraction(chars: &mut std::str::Chars<'_>) -> Option<()> {
    let mut saw_digit = false;
    loop {
        let mut clone = chars.clone();
        match clone.next() {
            Some(ch) if ch.is_ascii_digit() => {
                saw_digit = true;
                let _discarded = chars.next();
            }
            Some('Z' | '+' | '-') if saw_digit => return Some(()),
            _other => return None,
        }
    }
}

fn read_digits(chars: &mut std::str::Chars<'_>, count: usize) -> Option<i64> {
    let mut value = 0_i64;
    for _ in 0..count {
        let digit = chars.next()?.to_digit(10)?;
        value = value.checked_mul(10)?.checked_add(i64::from(digit))?;
    }
    Some(value)
}

fn expect_char(chars: &mut std::str::Chars<'_>, expected: char) -> Option<()> {
    (chars.next()? == expected).then_some(())
}

fn ensure_finished(chars: &mut std::str::Chars<'_>) -> Option<()> {
    chars.next().is_none().then_some(())
}

fn validate_datetime_parts(
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
) -> Option<()> {
    if !(1..=9999).contains(&year) {
        return None;
    }
    if !(1..=12).contains(&month) {
        return None;
    }
    if !(1..=days_in_month(year, month)?).contains(&day) {
        return None;
    }
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=60).contains(&second) {
        return None;
    }
    Some(())
}

fn validate_timezone(hour: i64, minute: i64) -> Option<()> {
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) {
        return None;
    }
    Some(())
}

fn days_in_month(year: i64, month: i64) -> Option<i64> {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => Some(31),
        4 | 6 | 9 | 11 => Some(30),
        2 if is_leap_year(year)? => Some(29),
        2 => Some(28),
        _other => None,
    }
}

fn is_leap_year(year: i64) -> Option<bool> {
    let by_four = year.checked_rem(4)? == 0;
    let by_hundred = year.checked_rem(100)? == 0;
    let by_four_hundred = year.checked_rem(400)? == 0;
    Some(by_four && (!by_hundred || by_four_hundred))
}

fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
    let adjusted_year = if month <= 2 {
        year.checked_sub(1)?
    } else {
        year
    };
    let era = adjusted_year.div_euclid(400);
    let era_years = era.checked_mul(400)?;
    let year_of_era = adjusted_year.checked_sub(era_years)?;
    let month_prime = if month > 2 {
        month.checked_sub(3)?
    } else {
        month.checked_add(9)?
    };
    let day_of_year = 153_i64
        .checked_mul(month_prime)?
        .checked_add(2)?
        .div_euclid(5)
        .checked_add(day)?
        .checked_sub(1)?;
    let year_days = year_of_era.checked_mul(365)?;
    let leap_days = year_of_era
        .div_euclid(4)
        .checked_sub(year_of_era.div_euclid(100))?;
    let day_of_era = year_days.checked_add(leap_days)?.checked_add(day_of_year)?;
    era.checked_mul(146_097)?
        .checked_add(day_of_era)?
        .checked_sub(719_468)
}

fn write_temp_rename<F>(path: &Path, write: F) -> Result<(), ArchiveError>
where
    F: FnOnce(&Path) -> Result<(), ArchiveError>,
{
    let temp_path = temp_path_for(path)?;
    if temp_path.try_exists()? {
        fs::remove_file(&temp_path)?;
    }
    match write(&temp_path) {
        Ok(()) => {
            sync_file(&temp_path)?;
            fs::rename(&temp_path, path)?;
            sync_parent_dir(path)?;
            Ok(())
        }
        Err(error) => {
            let _ignored = fs::remove_file(&temp_path);
            Err(error)
        }
    }
}

fn temp_path_for(path: &Path) -> Result<PathBuf, ArchiveError> {
    let file_name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| ArchiveError::InvalidPath {
            path: path.to_path_buf(),
        })?;
    Ok(path.with_file_name(format!("{file_name}.tmp.{}", std::process::id())))
}

fn sync_file(path: &Path) -> Result<(), ArchiveError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn sync_parent_dir(path: &Path) -> Result<(), ArchiveError> {
    let parent = path.parent().ok_or_else(|| ArchiveError::InvalidPath {
        path: path.to_path_buf(),
    })?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

impl CreatedAtParseStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Missing => "missing",
            Self::Invalid => "invalid",
            Self::Future => "future",
        }
    }
}

impl fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Parquet(error) => write!(f, "Parquet error: {error}"),
            Self::Arrow(error) => write!(f, "Arrow error: {error}"),
            Self::Json(error) => write!(f, "JSON error: {error}"),
            Self::CountOverflow { field } => write!(f, "count overflow for {field}"),
            Self::InvalidCompression(error) => write!(f, "invalid compression level: {error}"),
            Self::InvalidPath { path } => write!(f, "invalid archive path: {}", path.display()),
            Self::InvalidRecordJson => f.write_str("post record serialized to non-object JSON"),
        }
    }
}

impl Error for ArchiveError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Parquet(error) => Some(error),
            Self::Arrow(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::CountOverflow { .. }
            | Self::InvalidCompression(_)
            | Self::InvalidPath { .. }
            | Self::InvalidRecordJson => None,
        }
    }
}

impl From<io::Error> for ArchiveError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<parquet::errors::ParquetError> for ArchiveError {
    fn from(error: parquet::errors::ParquetError) -> Self {
        Self::Parquet(error)
    }
}

impl From<arrow_schema::ArrowError> for ArchiveError {
    fn from(error: arrow_schema::ArrowError) -> Self {
        Self::Arrow(error)
    }
}

impl From<serde_json::Error> for ArchiveError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ArchivePostRow, CreatedAtParseStatus, NormalizerVersion, RepoReceiptInput,
        build_repo_receipt, classify_created_at, extract_emojis, hash_post_rows,
    };

    fn normalizer() -> NormalizerVersion {
        NormalizerVersion {
            name: "emoji-normalizer".to_owned(),
            semver: "0.1.0".to_owned(),
            git_rev: "test".to_owned(),
            unicode_version: "16.0".to_owned(),
            emoji_data_version: "16.0".to_owned(),
        }
    }

    fn row(text: &str, emojis: &[&str]) -> ArchivePostRow {
        ArchivePostRow {
            did: "did:plc:test".to_owned(),
            rkey: "abc".to_owned(),
            cid: "bafy-test".to_owned(),
            normalizer: normalizer(),
            account_status: None,
            record_status: None,
            public_content_label: None,
            created_at_raw: Some("2026-06-15T00:00:00Z".to_owned()),
            created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
            created_at_parse_status: CreatedAtParseStatus::Valid,
            text: text.to_owned(),
            langs: vec!["en".to_owned()],
            emoji_sequence: emojis.iter().map(|emoji| (*emoji).to_owned()).collect(),
            extras_json: serde_json::json!({"facets": []}),
        }
    }

    #[test]
    fn row_hash_changes_when_content_changes() {
        let first = hash_post_rows(&[row("hello", &["✅"])]).expect("first row hash");
        let second = hash_post_rows(&[row("hello!", &["✅"])]).expect("second row hash");
        assert_ne!(first, second);
    }

    #[test]
    fn receipt_counts_posts_and_emoji_occurrences() {
        let rows = [row("a", &["✅", "✅"]), row("b", &[])];
        let receipt = build_repo_receipt(RepoReceiptInput {
            rows: &rows,
            reachable_records_count: 3,
            reachable_post_records_count: 2,
            post_decode_error_count: 1,
            profile_row_hash: Some("profile-hash".to_owned()),
            mst_root_cid: Some("root".to_owned()),
            commit_cid: Some("commit".to_owned()),
            normalizer: normalizer(),
        });
        let receipt = receipt.expect("receipt should build");
        assert_eq!(receipt.reachable_records_count, 3);
        assert_eq!(receipt.reachable_post_records_count, 2);
        assert_eq!(receipt.archived_post_rows_count, 2);
        assert_eq!(receipt.post_decode_error_count, 1);
        assert_eq!(receipt.emoji_posts_count, 1);
        assert_eq!(receipt.emoji_occurrences_count, 2);
        assert_eq!(receipt.profile_row_hash, Some("profile-hash".to_owned()));
    }

    #[test]
    fn extracts_grapheme_emoji_sequences() {
        assert_eq!(extract_emojis("hi ✅ 👩‍💻"), vec!["✅", "👩‍💻"]);
    }

    #[test]
    fn classifies_created_at_statuses() {
        let missing = classify_created_at(None);
        assert_eq!(missing.status, CreatedAtParseStatus::Missing);
        assert_eq!(missing.normalized, None);

        let invalid = classify_created_at(Some("not-a-date"));
        assert_eq!(invalid.status, CreatedAtParseStatus::Invalid);
        assert_eq!(invalid.raw, Some("not-a-date".to_owned()));
        assert_eq!(invalid.normalized, None);

        let future = classify_created_at(Some("9999-12-31T23:59:59Z"));
        assert_eq!(future.status, CreatedAtParseStatus::Future);
        assert_eq!(future.normalized, None);

        let valid = classify_created_at(Some("2020-01-02T03:04:05Z"));
        assert_eq!(valid.status, CreatedAtParseStatus::Valid);
        assert_eq!(valid.normalized, Some("2020-01-02T03:04:05Z".to_owned()));
    }
}

```

---

## `rust/check.sh`

```bash
#!/usr/bin/env bash
# Muster gate for the rust/ workspace — the local stand-in for CI (no CI wired yet).
# Every gate must pass before code is shippable.
#   exit 0 = all gates green
#   exit 1 = a gate failed
#   exit 2 = a gate was skipped (its tool isn't installed) — run is INCOMPLETE
set -uo pipefail
cd "$(dirname "$0")" || exit 1

# cargo needs a linker (`cc`). The dev sandbox has none on PATH; fall back to a nix-store
# gcc-wrapper if one exists. On a normally-provisioned box `cc` is already present.
if ! command -v cc >/dev/null 2>&1; then
  wrapper="$(ls -d /nix/store/*gcc-wrapper*/bin 2>/dev/null | head -1)"
  if [ -n "${wrapper}" ]; then
    export PATH="${wrapper}:${PATH}"
    echo "note: no cc on PATH; using ${wrapper}"
  fi
fi

failed=0
incomplete=0
missing=()

have() { command -v "$1" >/dev/null 2>&1; }

run() { # run NAME -- CMD...
  local name="$1"; shift; [ "$1" = "--" ] && shift
  printf '\n=== %s ===\n' "$name"
  if "$@"; then echo "PASS: ${name}"; else echo "FAIL: ${name}"; failed=1; fi
}

gated() { # gated NAME TOOL -- CMD...
  local name="$1" tool="$2"; shift 2; [ "$1" = "--" ] && shift
  if have "$tool"; then
    run "$name" -- "$@"
  else
    printf '\n=== %s ===\nSKIP: %s not installed\n' "$name" "$tool"
    incomplete=1; missing+=("$tool")
  fi
}

run   "fmt"      -- cargo fmt --all -- --check
run   "clippy"   -- cargo clippy --workspace --all-targets --all-features -- -D warnings
run   "test"     -- cargo test --workspace --all-features
gated "nextest"  cargo-nextest  -- cargo nextest run --workspace --all-features
gated "deny"     cargo-deny     -- cargo deny check
gated "audit"    cargo-audit    -- cargo audit
gated "machete"  cargo-machete  -- cargo machete
gated "coverage" cargo-llvm-cov -- cargo llvm-cov nextest --workspace --all-features

echo
echo "================ summary ================"
if [ "${failed}" -ne 0 ]; then
  echo "RESULT: FAILED"; exit 1
elif [ "${incomplete}" -ne 0 ]; then
  echo "RESULT: INCOMPLETE — missing tools: ${missing[*]}"
  echo "install on NixOS, e.g. add to systemPackages / devShell: ${missing[*]}"
  exit 2
else
  echo "RESULT: PASS"; exit 0
fi

```

---

## `rust/clippy.toml`

```toml
# Restriction lints (unwrap/expect/panic) are denied in library/bin code but relaxed in
# tests, where they're idiomatic for assertions. indexing_slicing / arithmetic_side_effects
# have no test-allow knob — relax those per-test-module with a scoped #[allow] when needed.
allow-unwrap-in-tests = true
allow-expect-in-tests = true
allow-panic-in-tests = true

```

---

## `rust/deny.toml`

```toml
# cargo-deny config. Starter values — the license allow-list and any advisory ignores will
# need a tuning pass the first time the full dependency tree (incl. vendored Jacquard) is in
# the graph; cargo-deny is iterative by nature.

[graph]
all-features = true

[advisories]
version = 2
yanked = "deny"
# atomic-polyfill (transitive via the crypto/embedded stack) is unmaintained — superseded by
# portable-atomic upstream; not a direct dep and not easily removable from the tree.
# paste is transitive via parquet/arrow. Parquet has no safe upgrade path that removes it yet.
ignore = ["RUSTSEC-2023-0089", "RUSTSEC-2024-0436"]

[licenses]
version = 2
confidence-threshold = 0.8
allow = [
  "MIT",
  "MIT-0",
  "Apache-2.0",
  "Apache-2.0 WITH LLVM-exception",
  "BSD-2-Clause",
  "BSD-3-Clause",
  "ISC",
  "Zlib",
  "MPL-2.0",
  "Unicode-DFS-2016",
  "Unicode-3.0",
  "CC0-1.0",
  "CDLA-Permissive-2.0",
  "BSL-1.0",
]

[bans]
# Transitive trees we don't control duplicate versions; surface but don't fail on it
# (mirrors clippy's multiple_crate_versions = "allow").
multiple-versions = "warn"
wildcards = "deny"

[sources]
unknown-registry = "deny"
unknown-git = "deny"
# Jacquard is pulled from our SHA-pinned GitHub fork-mirror.
allow-git = ["https://github.com/aliceisjustplaying/jacquard"]

```

---

## `rust/rustfmt.toml`

```toml
max_width = 100
# Nightly rustfmt options (the toolchain is nightly): group std / external / crate imports
# and merge each crate's imports into one statement.
group_imports = "StdExternalCrate"
imports_granularity = "Crate"

```
