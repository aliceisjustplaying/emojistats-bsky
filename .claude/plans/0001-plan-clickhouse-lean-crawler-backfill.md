# Plan 0001: ClickHouse + lean crawler — full-network backfill and storage architecture

Status: proposed (2026-06-11). Supersedes plan 0000 and the `unified-ingest` (Nexus → Timescale + Parquet) lineage.

## Decisions already made (with Alice, 2026-06-11)

- Backfill engine: **custom lean crawler** (TypeScript), not tap, not a zeppelin fork
- Text: REVISED (later on 2026-06-11, cost): ClickHouse stores the spine for all posts + text for **emoji posts only** (~37-40 GB total → CX32-class box); the **full text corpus goes to zstd Parquet on a Storage Box** (~75-90 GB, ~€4/mo), written by the crawl and an hourly live spool — mineable "another day" via DuckDB, doubles as DR. The CH `text` column stays in the schema (written `''` for non-emoji posts) as the upgrade path.
- Hardware: **permanent VPS + ephemeral hourly-billed crawl box**
- Deletes: **ignored** — emojitracker semantics, count posts as they happened
- Stack freedom: Postgres/Timescale, Redis/Valkey and BullMQ may all be removed

## Why this shape

- Relays are non-archival (replay window is hours, not years), so "replay the firehose since 2023" is impossible. Backfill means crawling every repo's CAR via `com.atproto.sync.getRepo`. Posts deleted before our crawl are gone forever — historical curves show *surviving* posts. ([bnewbold on relay sizing](https://whtwnd.com/bnewbold.net/3lo7a2a4qxg2l))
- [tap](https://atproto.com/blog/introducing-tap) (official, Dec 2025) is the turnkey option but verification can't be disabled, it maintains a full record index (~300+ GB of SQLite/Postgres we'd discard) and full-network mode is documented as "terabytes, days to weeks". Its verification protects repo *integrity*, not our pipeline's *completeness* — we need our own accounting either way. ([tap README](https://github.com/bluesky-social/indigo/blob/main/cmd/tap/README.md))
- [zeppelin-social/backfill-bsky](https://github.com/zeppelin-social/backfill-bsky) proved a full-network TS crawl in ~3 days (Jul 2025); we crib its enumeration and PDS-politeness patterns without its AppView-coupled storage.
- Nothing in [microcosm.blue](https://microcosm.blue/) replaces the crawl (Constellation = backlinks, UFOs = samples/stats), but **Hubble** — their whole-network mirror serving sync XRPC, funded by a Bluesky grant — launches around end of June 2026 and is an ideal *fallback CAR source* for repos whose PDS is dead or hostile.
- Scale numbers (verified by Alice, 2026-06-11): **44,778,860 users, 2.880B posts**. Total network CAR data "terabytes" (assume 15–25 TB transferred; dry-run measures bytes/repo for the real figure). Measured compression (2.15M-row sample): non-text columns 5.6 B/row, text 17.8 B/row → full dataset with all text ≈ **~78 GB** + aggregates. Sources: tap docs, [rsky-wintermute docs](https://github.com/blacksky-algorithms/rsky), live stats.

## Target architecture

```
        one-time                          forever
┌──────────────────────┐    ┌─────────────────────────────┐
│  Crawler (ephemeral   │    │  Jetstream (public instance) │
│  box, packages/       │    │            │                 │
│  backfill)            │    │            ▼                 │
│  PLC export → per-PDS │    │  Ingest worker               │
│  queues → getRepo →   │    │  (packages/ingest)           │
│  CAR parse → extract  │    │  normalize → batch → insert  │
└──────────┬───────────┘    └────────────┬────────────────┘
           │      both write raw rows     │
           ▼                              ▼
        ┌────────────────────────────────────┐
        │ ClickHouse (permanent box, single   │
        │ source of truth + serving layer)    │
        │ posts (raw) → MVs → hourly + totals │
        └────────────────┬───────────────────┘
                         ▼
        ┌────────────────────────────────────┐
        │ API server (packages/backend)       │
        │ 1s in-process cache → Socket.IO     │
        │ + new REST endpoints for analytics  │
        └────────────────────────────────────┘
```

Three long-running processes after backfill: ClickHouse, ingest worker, API server. **Removed entirely: Redis/Valkey, BullMQ + Bull-Board, Postgres/Timescale, the Lua scripts, `unified-ingest`, Nexus.** Frontend keeps its Socket.IO contract unchanged at first.

Why ClickHouse over the alternatives we're free to pick:
- Timescale: works (the `emoji_post` star schema got far) but row-oriented storage and dimension-upsert bookkeeping cost more disk, RAM and write-path complexity than CH's native columns + Summing/Replacing engines.
- DuckDB/Parquet: superb for offline analytics, wrong for continuous ingest + concurrent serving from one store.
- Redis for live: only ever held cumulative counters; CH aggregate tables answer the same 1 Hz queries in single-digit ms from a few thousand rows, so a second store isn't paying rent.

## ClickHouse schema (sketch — exact DDL at implementation)

```sql
-- Truth. One row per post, full text, emoji extraction materialized.
CREATE TABLE posts (
  did         String CODEC(ZSTD(1)),
  rkey        String CODEC(ZSTD(1)),
  created_at  DateTime('UTC') CODEC(Delta, ZSTD(1)),
  text        String CODEC(ZSTD(6)),
  langs       Array(LowCardinality(String)),
  emojis      Array(LowCardinality(String)),  -- normalized glyphs, occurrences incl. repeats
  src         LowCardinality(String),          -- 'backfill' | 'live'
  ingested_at DateTime('UTC')
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (did, rkey);

-- Hourly history (the white-whale query surface)
CREATE TABLE emoji_hourly (
  hour DateTime('UTC'), emoji LowCardinality(String),
  occurrences UInt64, posts UInt64
) ENGINE = SummingMergeTree PARTITION BY toYear(hour) ORDER BY (emoji, hour);

CREATE TABLE emoji_hourly_by_lang ( … lang first in ORDER BY (lang, emoji, hour) … );

CREATE TABLE posts_hourly (   -- totals incl. emoji-less posts, for density/ratio charts
  hour DateTime('UTC'), posts UInt64, posts_with_emojis UInt64, emoji_occurrences UInt64
) ENGINE = SummingMergeTree ORDER BY hour;

-- Live-serving totals (replaces every Redis sorted set / counter)
-- emoji_total, emoji_total_by_lang, lang_total, metrics_total — SummingMergeTree,
-- maintained by materialized views on insert into posts.
```

Principles:

- **Raw `posts` is the only truth; every aggregate is a disposable cache** rebuilt from raw at will. This is the core PTSD antidote: a counting bug never requires re-crawling, only re-aggregating (a plain columnar scan, minutes).
- Dedupe is structural: ReplacingMergeTree on `(did, rkey)` makes raw inserts idempotent. Backfill/live overlap, retries and re-fetches all collapse at merge time; `OPTIMIZE TABLE posts FINAL` once post-backfill, then rebuild all aggregates exactly.
- Time bucketing uses the record's `createdAt`, clamped to `[2022-11-01, fetch_time + 48h]`, falling back to the rkey TID when `createdAt` is absurd (client clocks lie); anomalies counted and logged, never silently dropped.
- Steady state self-heal: a scheduled job re-derives the last 7 days of hourly tables and periodically refreshes totals from raw, so any MV drift is bounded and corrected automatically.
- Query patterns this serves: per-emoji trend = index seek on `(emoji, hour)`; "top movers in week X" = window over `emoji_hourly`; live top-3790 = `sum()` over the small totals tables at 1 Hz behind the existing 1s in-process cache.

Sizing with full text (~3B posts): text dominates at roughly 60–120 GB compressed, everything else ~30–50 GB, aggregates a few GB. Budget **250–350 GB NVMe** with merge headroom. If disk gets tight, CH tiered storage can park old partitions on a cheap attached volume.

## Backfill design (`packages/backfill`)

The thoroughness lives in a **per-repo ledger** (SQLite on the crawl box): `did, pds_host, status, rev, car_bytes, records_total, posts_total, posts_with_emojis, emoji_occurrences, attempts, error, fetched_at, loaded_at`. Status walks `pending → fetching → parsed → loaded → verified`, or terminates in `empty | tombstoned | deactivated | takendown | unreachable | quarantined`. The ledger is the checkpoint — kill the box at any moment and nothing is lost; in-flight repos simply re-fetch.

Pipeline, all stages concurrent across repos:

- **Enumerate**: PLC directory bulk export (note the [January 2026 export API change](https://github.com/bluesky-social/atproto/discussions/4508) — verify current mechanics) for did→PDS mapping; union with relay `listRepos` to catch did:web and stragglers; diff between the two sources is itself a report.
- **Fetch**: `getRepo` grouped by PDS host, per-host token bucket (concurrency ~2–4, honor 429/Retry-After), global concurrency in the hundreds — most accounts sit on bsky.network's many mushroom PDSes so parallelism spreads naturally. Size caps and timeouts per repo; Hubble as fallback source for dead PDSes once it ships.
- **Parse + extract**: stream CAR → walk records, `app.bsky.feed.post` only, *no* MST/signature verification; `worker_threads` pool; malformed CBOR quarantines the repo with a logged reason instead of crashing the worker. Emoji extraction via the existing `emoji-normalization` package.
- **Load**: large batches into `posts` with `insert_deduplication_token = hash(did, rev)`; ledger flips to `loaded` only after the insert is acked. Re-runs are harmless by construction.
- **Retry waves**: unreachable PDSes get spaced retry passes during the crawl plus a final sweep; whatever remains is an explicit, queryable unreachable-list, not silence.
- **Live overlap**: the Jetstream ingest worker starts *before* the crawl (T0) and never stops; repos fetched mid-crawl contain posts the live path already wrote — structural dedupe absorbs it. No seen-set sized to the crawl window needed.

Acceptance criteria — backfill is "done" only when:

- every PLC DID has a terminal ledger status, and the unreachable/tombstoned report is generated
- `GROUP BY did` counts in ClickHouse match ledger `posts_total` per repo exactly (one join, zero tolerated mismatches)
- a random sample of ~1,000 repos re-fetched at the end yields identical post sets (catches systematic parse bugs)
- hourly series shows no discontinuity at the backfill/live boundary, and totals are within sane tolerance of public stats (jazco)
- the final ledger snapshot is imported into ClickHouse as `backfill_repos` for permanent provenance queries

Crawl box: hourly-billed big instance (e.g. Hetzner CCX63-class, ~48 vCPU) for roughly 4–7 days ≈ €50–80 one-time. CAR parsing is CPU-bound, transfer is 15–25 TB inbound (free at Hetzner; verify NIC throughput, that may be the real floor). The crawler writes straight into ClickHouse on the permanent box; nothing of value lives on the ephemeral machine except the ledger, which gets copied off.

## Live ingest (`packages/ingest`)

Jetstream (public instance, `wantedCollections=app.bsky.feed.post`) → normalize → 1s in-memory accumulation → batched insert into `posts` (MVs fan out to hourly + totals). Cursor in an atomically-written file with `CURSOR_OVERRIDE` support; on reconnect rewind ~10s and let a small 72h SQLite seen-set plus insert dedup tokens absorb the overlap. Prometheus metrics as today. No queue: at a few hundred posts/s peak, BullMQ was load-bearing for an architecture that no longer exists.

**The 1s flush + 1s UI tick is a product requirement** (Alice, 2026-06-11) — the blinking grid keeps its cadence. One insert/sec of a few hundred rows is within ClickHouse's comfort zone (background merges collapse tiny parts far below the 300-active-parts throw threshold). Contingency ladder if merge backlog ever appears in `system.merges`, in order, none of which slows the UI: enable `async_insert=1` so CH coalesces part creation server-side; as a last resort overlay the ingest worker's in-memory current-second delta onto the CH snapshot in the API server, decoupling UI freshness from storage cadence entirely. Never resolve part pressure by stretching the flush interval.

## API server (`packages/backend`, slimmed)

Same Socket.IO contract and 1s emit loop, reads through a 1s in-process cache to CH totals tables (per plan 0000's serving design, which stands). New REST endpoints for the analytics surface (per-emoji hourly series, top-N for arbitrary ranges, movers) once the frontend grows charts. Frontend untouched for the cutover.

## Cutover

Current Redis-based prod keeps running untouched during the entire build and crawl. The new stack runs in parallel (live ingest first, then crawl, then verification). Flip the frontend to the new API only after acceptance criteria pass. The new all-time numbers will be *larger* than today's since-some-cursor counters — that's the feature shipping.

## Resolved questions

- **Nexus = tap.** "Nexus" was tap's development name before the public release ([indigo commit 2025-11-26: "nexus -> tap"](https://github.com/bluesky-social/indigo/commits/main/cmd/tap)); `nexus.db`'s tables match tap's schema and `unified-ingest`'s adapter speaks tap's `/channel` ack protocol. The Nov 2025 attempt that went awry was a pre-release tap build — which retroactively validates not building attempt #3 on tap.
- **The remembered "Rust tap-variant without verification" does not exist as a public project** (swept GitHub, tangled.org, crates, the [official backfilling guide](https://atproto.com/guides/backfilling), microcosm). Closest real things: [rsky-wintermute](https://github.com/blacksky-algorithms/rsky) (Rust full-network backfiller, but welded into Blacksky's AppView) and microcosm's [repo-stream](https://tangled.org/@microcosm.blue/repo-stream) (Rust CAR→MST walker *library*, alpha, signature verification explicitly not implemented — a candidate parse-layer sidecar if the TS parser ever needs more speed, not a crawler replacement). `tapped`/`tapfall` are mere tap clients. The memory likely blended Nexus with one of these.

## Open questions

- `packages/unified-ingest` has **no TypeScript source on disk, only `dist/`** — rescue the source if it exists elsewhere before deleting the package.
- Parked (Alice, 2026-06-11): something smarter than the flat per-post emoji cap (now 300 = Bluesky's grapheme limit) for spam like the 🐑-wall bot — candidates: rank by `posts` instead of `occurrences`, per-DID contribution damping, uniq-poster counts. Raw text is stored, so any future policy can be re-derived over history.
- Verify live post counts and NIC/egress limits before committing to disk sizes and crawl-box class.
- Permanent box pick: CPX51-class cloud (~360 GB NVMe) vs CCX33 + volume vs a small dedicated (AX42-class is better price/perf if dedicated becomes acceptable).

## Sources

- https://atproto.com/blog/introducing-tap and https://github.com/bluesky-social/indigo/blob/main/cmd/tap/README.md
- https://github.com/zeppelin-social/backfill-bsky
- https://microcosm.blue/ (Constellation, UFOs, Slingshot, repo-stream, Hubble)
- https://whtwnd.com/bnewbold.net/3lo7a2a4qxg2l (relay/non-archival sizing)
- https://github.com/blacksky-algorithms/rsky (rsky-wintermute backfiller)
- https://github.com/bluesky-social/atproto/discussions/4508 (PLC export API change, Jan 2026)
- https://bsky.jazco.dev/ (live network stats)
- https://atproto.com/guides/backfilling (official backfilling guide)
- https://tangled.org/@microcosm.blue/repo-stream (Rust CAR/MST walker, potential parse sidecar)
