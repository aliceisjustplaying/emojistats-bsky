# Project Status (Nov 10 2025)

## What we know

- Emoji-bearing posts are ~22.65% of Bluesky traffic; a full historical ingest is ≈4.8e8 rows and stays <120 GB even when we store redundant URIs.
- The forked `packages/backfill` pipeline can enumerate PDS hosts via `listRepos`, fetch CAR archives, normalize emoji with `packages/emoji-normalization`, and stream results into Timescale + Parquet.
- Edge cases we already hardened against: repo deactivations/takedowns, corrupt CAR streams, invalid rkeys/TIDs, connection refusals, and UnknownXRPC 404s.
- Network limits: `getRepo` is enforced at 20 req/sec per PDS, and all other atproto calls share a global 3k/5‑min bucket. The code now has token buckets for both.
- External research: `futur_backfill_findings.md` captures lessons from Futur’s Oct 2025 AppView backfill (storage, COPY-based writes, worker sizing, live ingest throughput).
- Validation: the backfill now compares per-repo Parquet rows, INSERTed rows, and Timescale totals; if Timescale gains extra rows from live ingest during validation we log the delta but only fail when Timescale is _missing_ rows.
- Reference: the upstream Bluesky AppView backfill (~/src/a/atproto, branch `divy/backfill`) runs a ListRepos → Redis queue → `RepoBackfiller` pipeline with explicit stream backpressure, chunked CAR processing, and high-water-mark cutoffs—use it for rate-limit/backpressure ideas.
- Migration in progress (2025-11-12): implementing Nexus-based backfill via `packages/unified-ingest` to replace the Bun/Redis pipeline. See `nexus_migration_plan.md` for step-by-step guide.

## Current setup

- Schema (`schema.sql`) defines Timescale hypertables plus hourly/daily continuous aggregates; helper scripts:
  - `bun run backfill` – emoji-only historical ingest.
  - `bun run refresh-aggregates` – refreshes `language_daily_totals` + `emoji_daily_stats` after a data load.
  - `bun run reset-db` – wipes fact + dimension tables (use with caution).
  - `bun run clean-state` – truncates DB tables, deletes the Redis stream, and removes the cursor cache so runs start fresh.
  - `bun run backfill:producer` – enumerates PDS hosts and writes `{did,pds}` jobs into Redis (rate-limited with a high-water mark).
- Backfill emits Parquet snapshots under `packages/backfill/data/parquet/` for replay/debugging.
- Progress logging now shows scheduled/completed repos plus cumulative posts+emoji counts; terminal skips are recorded so we don’t retry dead repos.
- Redis queue pipeline in progress:
  - `bun run backfill:producer` enumerates PDS hosts and enqueues `{did,pds}` jobs into the Redis stream (`BACKFILL_REDIS_URL`, `BACKFILL_STREAM_NAME`, `BACKFILL_GROUP_NAME`, `BACKFILL_HIGH_WATER`).
  - `bun run backfill` now acts as the consumer; set `BACKFILL_CONSUMER_NAME`, optional `BACKFILL_READ_COUNT/BLOCK_MS`, and it will pull from Redis, process repos, and expose backlog metrics via Prometheus.

### Running the Redis-backed backfill locally

1. `cd packages/backfill && bun run clean-state` – reset Timescale tables, clear the Redis stream, and delete `pds-cursor-cache.json`.
2. Ensure Redis is running (`BACKFILL_REDIS_URL`, default `redis://localhost:6379`) and adjust `.env` with any DID limits or concurrency overrides. If you run multiple consumers, give each a unique `BACKFILL_METRICS_PORT` (or set it to `0` to disable metrics for that worker).
3. In one shell, run `bun run backfill:producer` to enumerate PDS hosts; it will pause automatically if the Redis stream exceeds `BACKFILL_HIGH_WATER`.
4. In another shell (per worker), export a unique `BACKFILL_CONSUMER_NAME` and run `bun run backfill` to consume jobs; start multiple workers by giving each a distinct consumer name.
5. Monitor `http://<host>:BACKFILL_METRICS_PORT/metrics` for `emoji_backfill_stream_backlog`, `emoji_backfill_queue_*`, and per-PDS counters to verify progress; run `bun run refresh-aggregates` once historical ingest completes.

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
   - See `backfill_rewrite_v2_plan.md` for the detailed producer/consumer rewrite blueprint (Redis streams, durable in-flight tracking, backpressure, ops tooling).
7. **Status checkpoint (2025-11-10T21:21:31Z):**

   - Implemented: COPY-based writer, per-repo validation logging, snapshot counts persisted in `repo_progress`, faster parallel validation checker, and a best-effort in-memory in-flight guard.
   - Current issue: even on a reset DB, `listRepos` emits duplicate descriptors quickly enough that we re-run the same DID sequentially, so Timescale sees duplicates/extras immediately and `check-validation` reports drift for every repo.
   - Planned fix: refactor to Bluesky/Futur’s Redis-backed pipeline—`listRepos` producer writes repo jobs into a Redis stream (with cursors + backpressure) and consumer workers pull from it via consumer groups, giving durable in-flight tracking and eliminating duplicate scheduling.

8. **Status checkpoint (2025-11-10T22:38:41Z):**

   - Redis producer/consumer rewrite is functional, but the producer still lacks a hard `EMOJI_BACKFILL_DID_LIMIT`, so the first end-to-end test overshot the intended 10k repos and was halted manually.
   - Next actions before another run: add a global DID limit (or other stop conditions) to the producer, ship queue inspection/drain scripts, and decide on shared rate-limit coordination so multiple consumers don’t exceed per-PDS quotas.

9. **Status checkpoint (2025-11-12T19:00:50Z):**

   - We have only ever ran the backfill on my laptop for 10000 DIDs, and found the "drift" issue.

10. **Status checkpoint (2025-11-12T19:31:55Z):**

    - **Unified ingest package implemented**: Created `packages/unified-ingest` with Nexus and Jetstream adapters, unified event processing, and shared writer pipeline.
    - **Features**: Timer-based flush (60s), error handling for unhandled rejections, per-repo validation tracking, and comprehensive Prometheus metrics (ack lag, repo completions, validation errors, etc.).
    - **Architecture**: Single worker supports both Nexus (backfill) and Jetstream (live) sources via configurable `INGEST_SOURCE` env var. Reuses Timescale/Parquet writer components from `packages/backfill`.
    - **Next steps**: Follow `nexus_migration_plan.md` Step 1-2 (run Nexus locally, add test repos), then Step 9 (test with multiple repos) to validate before production deployment.
    - **Code review fixes**: Addressed timer-based flush, Jetstream error handling, repo validation tracking, and metrics coverage per review findings.

11. **Status checkpoint (2025-11-12T20:07:37Z):**
    - **Production-ready fixes**: Resolved critical batching, durability, and reliability issues:
      - **Batching restored**: Implemented flush promise system where multiple events share the same flush promise, maintaining batch performance (up to 500 rows per COPY) while ensuring durability before ack
      - **Error resilience**: Flush promise always resolves even on DB errors (try/finally), preventing pipeline from hanging after transient failures
      - **Concurrent acks**: Acks run concurrently via `Promise.allSettled` to prevent slow Redis writes from blocking Nexus acks when multiple sources configured
      - **Non-emoji acks**: All events (including filtered ones) are acknowledged in finally block to prevent Nexus stalling and ensure Jetstream cursor advances
    - **Status**: Code complete and tested locally. Ready for Step 9 (multi-repo testing) and production deployment.
