# Project Status (Nov 10 2025)

## What we know

- Emoji-bearing posts are ~22.65% of Bluesky traffic; a full historical ingest is ≈4.8e8 rows and stays <120 GB even when we store redundant URIs.
- The forked `packages/backfill` pipeline can enumerate PDS hosts via `listRepos`, fetch CAR archives, normalize emoji with `packages/emoji-normalization`, and stream results into Timescale + Parquet.
- Edge cases we already hardened against: repo deactivations/takedowns, corrupt CAR streams, invalid rkeys/TIDs, connection refusals, and UnknownXRPC 404s.
- Network limits: `getRepo` is enforced at 20 req/sec per PDS, and all other atproto calls share a global 3k/5‑min bucket. The code now has token buckets for both.
- External research: `futur_backfill_findings.md` captures lessons from Futur’s Oct 2025 AppView backfill (storage, COPY-based writes, worker sizing, live ingest throughput).
- Validation: the backfill now compares per-repo Parquet rows, INSERTed rows, and Timescale totals; if Timescale gains extra rows from live ingest during validation we log the delta but only fail when Timescale is _missing_ rows.
- Reference: the upstream Bluesky AppView backfill (~/src/a/atproto, branch `divy/backfill`) runs a ListRepos → Redis queue → `RepoBackfiller` pipeline with explicit stream backpressure, chunked CAR processing, and high-water-mark cutoffs—use it for rate-limit/backpressure ideas.

## Current setup

- Schema (`schema.sql`) defines Timescale hypertables plus hourly/daily continuous aggregates; helper scripts:
  - `bun run backfill` – emoji-only historical ingest.
  - `bun run refresh-aggregates` – refreshes `language_daily_totals` + `emoji_daily_stats` after a data load.
  - `bun run reset-db` – wipes fact + dimension tables (use with caution).
- Backfill emits Parquet snapshots under `packages/backfill/data/parquet/` for replay/debugging.
- Progress logging now shows scheduled/completed repos plus cumulative posts+emoji counts; terminal skips are recorded so we don’t retry dead repos.

## TODO / next steps

1. **Finish historical backfill** on a Hetzner box (AX102+). Monitor disk usage, repo lag, and `terminal_skips` until we hit cursor 0.
2. **Live ingest:** use `packages/live-ingest` to seed Redis (`bun run seed-redis`) and start the Jetstream worker (`bun run start`). Jetstream respects the same rate limits, so Redis + Timescale stay current without hammering PDS hosts.
3. **Deployment plan:** run backfill + live ingest as separate services (systemd/pm2), expose both Prometheus endpoints (`BACKFILL_METRICS_PORT`, `LIVE_METRICS_PORT`), and wire Grafana/alerts plus regular Timescale/Parquet backups.
4. **Expose stats:** backend endpoints for daily/hourly/top emoji queries; frontend toggle to read Timescale for historical ranges and Redis for the real-time ticker.
5. **Ops polish / stretch:** optional text storage toggle (~100 GB), thread-level analytics via reply URI splits, CLI tooling for selective reprocessing, automated Parquet lifecycle policies, and alerts for hitting rate-limit buckets.
6. **Backfill reliability backlog (2025-11-10T21:21:31Z):**
   - Validation artifacts: persist per-repo Timescale + Parquet counts (and checksum later) so retries know when a snapshot is trustworthy, plus tooling to diff/count when numbers drift.
   - Queue/backpressure split: decouple `listRepos` enumeration from repo fetch/writes (Redis or similar) and add a high-water-mark check like Bluesky’s `streamLengthBackpressure`.
   - COPY metrics & tuning: benchmark the new temp-table/COPY path, expose flush duration + rows/sec, and add knobs for batch size before raising repo concurrency.
   - Retry hygiene: cap per-PDS retry loops, track retry counters in metrics, and store high-water marks (last seq per DID) so we skip CARs that only contain duplicates.
   - Live ingest SLOs: define acceptable Jetstream lag + throughput, add dashboards/alerts, and be ready to swap runtimes if Node workers cap out around 200 events/sec.
   - Storage lifecycle: formalize Parquet retention + Timescale chunk pruning/backups to keep the <120 GB target even if scope expands.
7. **Status checkpoint (2025-11-10T21:21:31Z):**
   - Implemented: COPY-based writer, per-repo validation logging, snapshot counts persisted in `repo_progress`, faster parallel validation checker, and a best-effort in-memory in-flight guard.
   - Current issue: even on a reset DB, `listRepos` emits duplicate descriptors quickly enough that we re-run the same DID sequentially, so Timescale sees duplicates/extras immediately and `check-validation` reports drift for every repo.
   - Planned fix: refactor to Bluesky/Futur’s Redis-backed pipeline—`listRepos` producer writes repo jobs into a Redis stream (with cursors + backpressure) and consumer workers pull from it via consumer groups, giving durable in-flight tracking and eliminating duplicate scheduling.
