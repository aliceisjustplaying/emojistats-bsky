# Project Status (Nov 10 2025)

## What we know

- Emoji-bearing posts are ~22.65% of Bluesky traffic; a full historical ingest is ≈4.8e8 rows and stays <120 GB even when we store redundant URIs.
- The forked `packages/backfill` pipeline can enumerate PDS hosts via `listRepos`, fetch CAR archives, normalize emoji with `packages/emoji-normalization`, and stream results into Timescale + Parquet.
- Edge cases we already hardened against: repo deactivations/takedowns, corrupt CAR streams, invalid rkeys/TIDs, connection refusals, and UnknownXRPC 404s.
- Network limits: `getRepo` is enforced at 20 req/sec per PDS, and all other atproto calls share a global 3k/5‑min bucket. The code now has token buckets for both.

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
