# Emoji stats for Bluesky

## Current backfill status

The full-network backfill is running on the `emoji` server plus six crawler
boxes. Current stable deploy: `90b9de7` (`Compress ClickHouse backfill
uploads`) on all seven machines.

Live crawler runtime settings, as of 2026-06-12 13:40 UTC:

- `GLOBAL_CONCURRENCY=4096`
- `PER_HOST_CONCURRENCY_BSKY=96`
- `PER_HOST_CONCURRENCY=16`
- `LOADER_BATCH_ROWS=50000`

Current stable estimate is about **3.8 days remaining**, measured from
ledger-derived `backfill_progress` terminal-status deltas, not lossy
`backfill_repo_events`. The latest stable sample was ~10,122 terminal repos/min
with ~55.69M pending. The backfill target for this pause point is "under 4 days
and not crash-looping"; the original "under 1 day" target is not currently met.

Important operational notes:

- `backfill_progress` is the status source of truth for the dashboard and is
  retried until accepted.
- `backfill_repo_events` is best-effort telemetry and can drop batches during
  ClickHouse pressure.
- Do not raise global concurrency above 4096 without a new measurement window.
  The 6144 canary reduced throughput and made ClickHouse upload resets worse.
- Do not return to 5120/128/20. That canary produced ClickHouse timeouts,
  frozen telemetry and crawler restarts.

## Development & deployment

For local development, bring up ClickHouse with `docker compose up`, then use the `bun run dev` variants (`bun run dev` for backend + frontend together, or `dev:backend` / `dev:frontend` individually). Production deploys via the pix NixOS flake. Backfill operations are documented in [docs/backfill-runbook.md](docs/backfill-runbook.md).

## Current todos:

- [x] Cursor handling
- [x] Nicer tabs
- [x] Handle Weird Emojis
- [x] Initial blinking implementation
- [x] Postgres
- [ ] Backfill the entire network (in progress)
- [ ] Better design
- [ ] Explore/move to SSE?
- [ ] Send updates efficiently
- [ ] More performant frontend
- [ ] etc.
