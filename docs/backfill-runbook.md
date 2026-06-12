# Backfill runbook

How to run the one-time full-network crawl (plan 0001): enumerate every DID from
the PLC directory, fetch every repo's CAR, extract posts, load them into
ClickHouse, and archive the full text corpus to parquet. The crawl is restartable
at any point — the SQLite ledger is the only checkpoint, and every load is
idempotent.

## What runs where

- Permanent box — ClickHouse (the `posts` truth table, aggregates, and the
  `backfill_progress` / `backfill_repo_events` telemetry tables) and the live
  Jetstream ingest worker. The ingest worker starts *before* the crawl and never
  stops; backfill/live overlap collapses structurally via ReplacingMergeTree.
- Crawl box (ephemeral, hourly billed) — the `packages/backfill` processes, the
  ledger at `LEDGER_DB_PATH` (default `packages/backfill/data/ledger.sqlite`),
  and the archive spool at `ARCHIVE_DIR`.
- Storage Box — receives finalized parquet files via `ARCHIVE_SYNC_COMMAND`.
  The archive is the ONLY durable home of full post text (ClickHouse keeps text
  for emoji posts only), so this hop is part of the critical path, not a backup.

## Before the crawl

- ClickHouse schema migrated (`bun run db:migrate` in `packages/ingest`) — this
  includes the telemetry tables.
- Live ingest worker running and healthy since before the first repo fetch.
- `.env` in `packages/backfill` on the crawl box:
  - `CLICKHOUSE_URL` / `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` /
    `CLICKHOUSE_DATABASE` pointing at the permanent box.
  - `BACKFILL_RUN_ID` set to something memorable for this run (e.g.
    `full-2026-06`); it tags every telemetry row, so keep it identical across
    all shards and boxes of the same run.
  - `ARCHIVE_DIR` on a disk with headroom — full text is roughly 75–90 GB of
    zstd parquet across the whole network, less per box when sharded.
  - `ARCHIVE_SYNC_COMMAND` (see the archive section) so finalized files leave
    the ephemeral box as soon as they rotate.
- Sanity-check connectivity with a tiny bounded run before committing the box:
  `bun run enumerate -- --limit 1000`, then `bun run crawl -- --limit 1000`,
  then `bun run verify`.

## Enumeration first

- `bun run enumerate` in `packages/backfill` streams the PLC `/export` feed
  into the ledger. The full directory at the self-imposed ~2 pages/sec
  (1,000 ops/page) takes on the order of **a day** — start it early, it is the
  long pole before any crawling can begin.
- It is resumable: the cursor (`plc_cursor` in ledger meta) commits atomically
  with each page, so a Ctrl-C or crash loses nothing; re-running continues
  where it left off. Re-running after the crawl also picks up newly created
  accounts.
- `--limit N` bounds the run by distinct DIDs touched (dry-runs); `--did <did>`
  enumerates individual DIDs by resolving their documents directly.
- Honest gap: enumeration covers PLC DIDs only. `did:web` accounts and the
  relay `listRepos` union/diff from the plan are not implemented yet; the few
  did:web stragglers can be added later with `--did`.

## Running the crawl

- `bun run crawl` claims pending repos (host-spread and claim-time capped so one
  cooling or already-full PDS cannot monopolize the scheduler's active slots),
  fetches CARs, extracts posts, archives full rows, loads ClickHouse, and
  updates the ledger. `--limit N` caps claims for a bounded run; `--did <did>`
  forces specific repos through the pipeline.
- Politeness knobs: `GLOBAL_CONCURRENCY` (default 32), `PER_HOST_CONCURRENCY`
  (default 2), `PER_HOST_CONCURRENCY_BSKY` (default 8 for the
  `*.bsky.network` mushroom fleet). 429/Retry-After is always honored on top:
  a 429 arms a per-host cooldown, later claims skip that host while it cools,
  and rate-limit retries do not burn the repo's reachability attempts.
- `TEXT_IN_CLICKHOUSE` (default `emoji`) controls what reaches ClickHouse:
  emoji-less posts get their `text` written as `''`; the archive always gets
  the full text regardless. `all` is the upgrade path if disk economics change.
- `bun run status` gives a one-glance readout (status counts, repos/min, last
  error, PLC cursor) without disturbing the run.
- Unreachable PDSes retry in spaced waves automatically. The run ends on its
  own when every repo is terminal and the remaining unreachables are out of
  attempts budget — they stay parked as the explicit unreachable list.
- One SIGINT/SIGTERM stops claiming and drains in-flight repos gracefully; a
  second one force-quits (safe — in-flight repos simply re-fetch next run).

## Sharding: multi-process and multi-box

- `CRAWL_SHARDS` / `CRAWL_SHARD_INDEX` partition the claimable set by a
  deterministic hash of the DID, evaluated inside SQLite. Each shard is its own
  `bun run crawl` process; shards never claim each other's repos. The default
  (1 shard, index 0) means no filtering.
- `SHARD_LABEL` (default `shard{N}`) names the shard's telemetry stream and its
  archive file prefix (`backfill-{SHARD_LABEL}-...parquet`). Keep it unique per
  process or the parquet files will collide.
- Within one box, all shard processes share the single ledger file — SQLite WAL
  handles the concurrency, and the guarded `fetching` transition makes claims
  race-safe. Consequence for telemetry: each process reports status counts and
  `posts_loaded` for its own shard slice only, so the dashboard sums them into
  exact fleet totals; `bytes_downloaded`, `rows_per_sec` and `in_flight` are
  per-process as always.
- The stale-`fetching` requeue at startup is shard-scoped too, so a shard
  started much later only requeues rows of its own slice and leaves its
  siblings' in-flight repos alone.
- Multi-box is a per-box-ledger model, stated honestly: the ledger does not
  replicate or merge on its own. Run the full enumeration once, copy the
  finished `ledger.sqlite` to every box, and give each box a complementary,
  non-overlapping set of shard indices — e.g. two boxes with `CRAWL_SHARDS=8`:
  one runs shard indices 0–3, the other 4–7, four processes each.
- Each box's ledger then records progress only for its own shards; rows
  belonging to the other box's shards sit in `pending` forever in the local
  file. That is expected, not a bug — but it means the final accounting must
  union the boxes: run `bun run verify` on each box against its own ledger
  (they all point at the same ClickHouse), and import each box's
  loaded/terminal rows when building the permanent `backfill_repos` snapshot,
  ignoring the foreign-shard pendings.
- Never run overlapping shard indices on two boxes against copies of the same
  ledger. ClickHouse dedupes the result so nothing breaks, but every repo in
  the overlap downloads twice and the archive gains duplicate rows.
- The multi-box copy model leaves the other boxes' shard rows permanently
  parked in `pending` in each local ledger. The crawl never sees them: a shard
  process's counts and idle/exit policy are scoped to its own shard slice, so
  a drained shard ends its run on its own, exactly like the single-box case —
  foreign-shard pendings are not remaining work. Only the ledger-wide tools
  (`bun run status`, verify) still count them, which is the global view those
  tools want.

## Telemetry and the dashboard

- Each crawl process inserts one `backfill_progress` row per
  `TELEMETRY_INTERVAL_MS` (default 10s), tagged `run_id` + `shard`, plus
  per-repo `backfill_repo_events` rows on every transition: `loaded`, `empty`,
  `retry`, `tombstoned`, `deactivated`, `takendown`, `quarantined`, `failed`.
- ClickHouse is the shared bus, so the dashboard (`packages/dashboard`,
  `/backfill` route) shows all shards and boxes in one place, and the
  throughput history survives restarts.
- Telemetry is deliberately NOT precious — the doctrinal opposite of the
  archive. A failed insert logs a warning and drops the batch; the crawl never
  crashes or stalls because of it. Gaps in the graphs are cosmetic; the ledger
  is the durable accounting.
- Latest state per shard, straight from the bus:
  `SELECT shard, argMax(posts_loaded, ts), argMax(rows_per_sec, ts) FROM backfill_progress WHERE run_id = '...' GROUP BY shard`.
- What broke most, when debugging a wave of retries:
  `SELECT event, error, count() FROM backfill_repo_events WHERE event IN ('retry','failed') GROUP BY event, error ORDER BY count() DESC LIMIT 20`.

## Archive and the Storage Box

- Every extracted row — full text, always, regardless of `TEXT_IN_CLICKHOUSE` —
  is appended to a rotating zstd-parquet sink before the ClickHouse load.
  Files rotate at `ARCHIVE_MAX_ROWS_PER_FILE` rows (default 1M) or
  `ARCHIVE_MAX_FILE_AGE_MS` (default 1h) and land in
  `${ARCHIVE_DIR}/finalized/`, each appended to `manifest.jsonl` with row
  counts and time bounds — the completeness accounting for later mining and
  restore.
- `ARCHIVE_SYNC_COMMAND` runs after each finalize with `{file}` substituted,
  e.g. `rclone copyto {file} storagebox:emojistats/backfill/$(basename {file})`
  or simply `rclone copy {file} storagebox:emojistats/backfill/`. A non-zero
  exit surfaces as an error by design — if the text is not on the Storage Box,
  it does not durably exist anywhere.
- Archive failures are FATAL to the run: one failed append trips the crawler —
  it stops claiming, drains in-flight repos, parks the affected repos as
  retryable, and exits non-zero with the dirty flag still set. Fix the disk or
  the sync target and restart; the parked repos re-fetch and re-archive.
- `ARCHIVE_ENABLED=false` exists for dry-runs only. Do not run the real crawl
  without the archive — with `TEXT_IN_CLICKHOUSE=emoji`, non-emoji text written
  nowhere is lost forever.
- Semantics are at-least-once, never at-most-once: a repo that gets re-fetched
  (crash before its ledger row flipped to loaded, retry waves, `--did` forcing)
  appends its rows again. Duplicates are possible, loss is not. Dedupe at
  mining time, e.g. in DuckDB:
  `SELECT DISTINCT ON (did, rkey) * FROM read_parquet('backfill-*.parquet')`.

## Verify and the final sweep

- During the crawl, unreachable PDSes already retry in waves with exponential
  backoff. Whatever the run leaves in `unreachable` / `failed` / `quarantined`
  is the explicit remainder, queryable in the ledger — silence is not an
  outcome.
- Final sweep: after a day or two, re-run `bun run crawl -- --final-sweep` —
  the flag zeroes the attempt budgets on parked unreachable rows (a plain
  re-run deliberately does NOT, so a crash loop can never hammer dead hosts)
  and stale PDS pointers re-resolve through the DID document on retry. Once Hubble (microcosm's whole-network
  mirror) ships, point the stragglers at it as a fallback CAR source.
- `bun run verify` is the acceptance-criteria engine: it reconciles every
  loaded repo's `posts_total` against ClickHouse per-DID counts (exact matches
  promote to `verified`, mismatches fail the run), prints the terminal-state
  report with the explicit DID lists, and `--sample N` re-fetches N random
  repos end to end to catch systematic parse bugs.
- The backfill is done only when every DID has a terminal status, verify passes
  with zero mismatches, the sampled repos match, the hourly series shows no
  discontinuity at the backfill/live boundary, and the final ledger snapshot is
  imported into ClickHouse as `backfill_repos`.

## Crash recovery semantics

- The ledger is the only checkpoint. Kill any process at any moment — power
  loss included — and nothing is lost; in-flight repos simply re-fetch.
- Normal crawler startup does not run the loaded-row ClickHouse digest audit:
  `CRASH_RECONCILE_ON_STARTUP=false` by default because `posts FINAL` over the
  hot table can pin the serving box during deploys. Turn it on only for an
  explicit recovery audit; `bun run verify` is the normal acceptance gate.
- Dirty flag: the crawler sets `crawl_dirty=1` in ledger meta at startup and
  `0` on clean exit. When `CRASH_RECONCILE_ON_STARTUP=true`, a dirty start
  reconciles the last hour of `loaded` rows against actual ClickHouse counts
  and requeues any mismatch — this covers the narrow window where an insert
  was acked into the OS page cache but never reached disk before the crash.
- Repos stuck in `fetching` from a killed run are requeued automatically at
  the next startup.
- ClickHouse loads are idempotent twice over: per-chunk
  `insert_deduplication_token` absorbs immediate re-sends, and
  ReplacingMergeTree on `(did, rkey)` collapses anything older at merge time.
  Aggregates over-count duplicates until rebuilt — by design, they are
  disposable caches rebuilt from `posts` after the backfill.
- Backfill writes use `CLICKHOUSE_REQUEST_TIMEOUT_MS` (default 180s). If the
  dashboard stops updating and crawler logs show `Timeout error` on
  `backfill_progress` or 200k-row inserts, the fleet is over ClickHouse's
  current write capacity; lower runtime concurrency before raising fetch caps.
- `backfill_progress` is not lossy: each crawler retains its newest status
  snapshot and retries it until ClickHouse accepts it. `backfill_repo_events`
  remains lossy dashboard/event telemetry. Dashboard freshness is the stalest
  shard, so status counts are current only when freshness is below the idle
  threshold.
- Telemetry emits once at startup and the scheduler yields during large claim
  scans. If a shard is active but its progress row is stale, check for a
  CPU-bound claim/refill loop before assuming ClickHouse is down.
- Claim refills exclude hosts whose local queue is already full or cooling.
  A pending ledger window dominated by a few capped hosts should not leave the
  crawler under-filled; check `topHosts`, `inFlight`, and first-window
  `pds_host` distribution together.
- The archive is at-least-once across crashes (see above): re-fetched repos
  re-append. Rows staged in the open file at crash time are recovered at the
  sink's next startup and finalized as their own parquet file; a hard crash
  can lose at most the last unflushed appender buffer of rows. The manifest
  only ever lists fully finalized files.
- Telemetry just resumes — gaps in `backfill_progress` across restarts are
  expected and meaningless.
