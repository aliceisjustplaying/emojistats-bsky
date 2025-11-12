# Project Status (Nov 12 2025)

## What we know

- Historical emoji ingest is handled by Nexus (backfill + live firehose) feeding the unified ingest worker in `packages/unified-ingest`.
- Nexus enumerates repos, fetches full CAR snapshots from each PDS, and streams live events (`live: true`). The worker reads those events, normalizes them, writes Timescale + Parquet, and validates each repo.
- Validation compares Parquet row counts, Timescale inserts, and existing totals. We treat extra Timescale rows (due to live Jetstream traffic) as informational but fail if Timescale is missing rows.
- Some repos never emit live emoji posts. `backfill_complete` flips to `true` once we see the first `live: true` emoji event. Emoji-scarce repos may stay `false` indefinitely even though their historical data is present.
- Slow PDS hosts require a larger Nexus repo-fetch timeout. Run Nexus with `--repo-fetch-timeout=180s` (or set `NEXUS_REPO_FETCH_TIMEOUT=180s`) so resync workers don’t time out while streaming large CARs.
- Unified ingest exposes Prometheus metrics (`INGEST_METRICS_PORT`), per-repo progress logging (`INGEST_PROGRESS_LOG_EVERY`, `INGEST_PROGRESS_LOG_INTERVAL_MS`), and drains Jetstream cursors from Redis.

## Current setup

- Schema (`schema.sql`) defines Timescale hypertables plus hourly/daily aggregates.
- Unified ingest helper scripts:
  - `bun run clean-state` (from `packages/unified-ingest`) – truncates DB tables, clears the Jetstream cursor key, and removes any cursor override file so runs start fresh.
  - `bun run start` – launches the worker. Configure with `.env` (database URL, `INGEST_SOURCE`, `NEXUS_URL`, Jetstream settings, etc.).
- Nexus helper notes:
  - Run with `--repo-fetch-timeout=180s --disable-acks=false` when consuming via unified ingest.
  - Monitor the `repos` table (`state = error/resyncing`) to requeue any DID stuck due to PDS failures.
- Parquet snapshots emit to `packages/unified-ingest/data/parquet/` for replay/debugging.
- Progress logging:
  - Per-repo logs fire every `INGEST_PROGRESS_LOG_EVERY` events (default 500) and at least every `INGEST_PROGRESS_LOG_INTERVAL_MS` milliseconds (default 30 s). These logs show `emojiEvents`, `filteredEvents`, and elapsed seconds per repo so long-running backfills appear “alive”.

## Running the Nexus-backed ingest locally

1. Start Nexus (SQLite is fine for dev):
   ```bash
   cd ~/src/a/indigo/cmd/nexus
   go run . --disable-acks=false \
     --repo-fetch-timeout=180s \
     --collection-filters=app.bsky.feed.post
   ```
2. In another shell, reset state: `cd packages/unified-ingest && bun run clean-state`.
3. Start the worker: `bun run start` (set `INGEST_SOURCE=nexus` or `both`).
4. Use `psql` to watch `repo_progress` and `repo_validation_log` for completion, or tail the logs for `"Repo ingest progress"` entries.
5. For repos that never emit live emoji, post a manual emoji from that DID to flip `backfill_complete` to `true` (optional).

## TODO / next steps

1. **Hetzner deployment:** run Nexus + unified ingest on a dedicated box (AX102+). Monitor disk usage, repo states (`repos.state` in Nexus), and unified-ingest metrics until all target DIDs validate.
2. **Live ingest:** keep the Jetstream adapter enabled (`INGEST_SOURCE=both`) so Timescale stays current once historical backfill completes. Jetstream cursor persistence lives in Redis (`JETSTREAM_CURSOR_KEY`).
3. **Monitoring/alerts:** wire Grafana dashboards for:
   - Nexus repo states (`pending`, `resyncing`, `error`, `active`)
   - Unified ingest counters (`emoji_ingest_events_total`, `events_failed_total`, ack lag)
   - Timescale/parquet row growth
4. **Ops polish:** decide how to mark repos “complete” when they’ll never emit live emoji (e.g., treat `repo_progress.backfill_complete=false` but `repo_validation_log` present as acceptable). Consider auto-flipping the flag after a successful validation even without a live emoji.
5. **Final migration:** once Nexus + unified ingest prove reliable, archive any remaining Bun/Redis tooling (already removed from the repo) and document the deployment playbook in `nexus_migration_plan.md`.

## Status checkpoints

- **2025-11-10:** Legacy Bun/Redis pipeline retired. Nexus + unified ingest under active development.
- **2025-11-12 21:45 UTC:** Durable acks, immediate flushes, and per-repo progress logging added to unified ingest. Nexus repo-fetch timeout made configurable (set to 180 s for slow hosts).
- **2025-11-12 22:54 UTC:** All six test repos drained via Nexus + unified ingest, `backfill_complete` flips once a live emoji is observed. Backfill cleanup scripts moved under `packages/unified-ingest`.
