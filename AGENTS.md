# Project Status (Nov 10 2025)

## What we know

- Emoji-bearing posts are ~22.65% of Bluesky traffic; a full historical ingest is ≈4.8e8 rows and stays <120 GB even when we store redundant URIs.
- The forked `packages/backfill` pipeline can enumerate PDS hosts via `listRepos`, fetch CAR archives, normalize emoji with `packages/emoji-normalization`, and stream results into Timescale + Parquet.
- Edge cases we already hardened against: repo deactivations/takedowns, corrupt CAR streams, invalid rkeys/TIDs, connection refusals, and UnknownXRPC 404s.

## Current setup

- Schema (`schema.sql`) defines Timescale hypertables plus hourly/daily continuous aggregates; helper scripts:
  - `bun run backfill` – emoji-only historical ingest.
  - `bun run refresh-aggregates` – refreshes `language_daily_totals` + `emoji_daily_stats` after a data load.
  - `bun run reset-db` – wipes fact + dimension tables (use with caution).
- Backfill emits Parquet snapshots under `packages/backfill/data/parquet/` for replay/debugging.
- Progress logging now shows scheduled/completed repos plus cumulative posts+emoji counts; terminal skips are recorded so we don’t retry dead repos.

## TODO / next steps

1. **Finish historical backfill** on a Hetzner box (AX102+). Monitor disk usage, repo lag, and `terminal_skips` until we hit cursor 0.
2. **Live ingest:** seed Redis 8 time-series from Timescale, then add a Jetstream consumer that keeps both Redis and Timescale in sync after backfill completes.
3. **Expose stats:** backend endpoints for daily/hourly/top emoji queries; frontend toggle to read Timescale for historical ranges and Redis for the real-time ticker.
4. **Ops polish:** add monitoring (Prometheus/Grafana), automated aggregate refresh jobs, backups for Parquet + Timescale chunks, and optional dimension seeding.
5. **Stretch goals:** text storage toggle (adds ~100 GB), thread-level analytics via reply URI splits, and CLI tooling for selective reprocessing.
