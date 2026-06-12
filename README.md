# Emoji stats for Bluesky

## Current backfill status

The full-network backfill is running on the `emoji` server plus six crawler
boxes. Current deploy: `7401920` plus the 2026-06-12 afternoon working tree
(bottlenecks #11/#12: ledger-stats worker, claim-backlog retention, AIMD
host pressure, dead-host registry, async archive sync), shipped by rsync to
`/workspace/src/emojistats-bsky` on all seven machines — commit pending
operator validation.

Live crawler runtime settings (runtime drop-in, to be baked into pix —
2026-06-12 ~17:00 UTC):

- `GLOBAL_CONCURRENCY=4096`
- `PER_HOST_CONCURRENCY_BSKY=96` (AIMD ceiling now, not a fixed rate)
- `PER_HOST_CONCURRENCY=16`
- `LOADER_BATCH_ROWS=50000`
- `NODE_OPTIONS=--max-old-space-size=12288` (8192 on the 32GB crawl3)

Post-fix per-box resolution is 3-9k repos/min mid-ramp (vs ~0.4k in the
mid-day stall era); see docs/launch-log-2026-06-12.md for the bottleneck
archaeology and the measured ETA.

Important operational notes:

- `backfill_progress` is the status source of truth for the dashboard and is
  retried until accepted. ETA is computed from RESOLVED-status deltas —
  everything terminal except `unreachable`, because dead-host parking moves
  millions of rows into `unreachable` legitimately.
- `backfill_repo_events` is best-effort telemetry and can drop batches.
- ~40% of PLC DIDs are junk (one squatted domain held 17.9M rows). Dead
  hosts are auto-detected, persisted in ledger meta `dead_hosts`,
  bulk-parked onto the final-sweep list, and enumeration inserts their rows
  born-parked. `bun run healthcheck -- --park` does the same proactively.
- Never compute ledger aggregates on the crawl main thread (bottleneck #11);
  they live in the ledger-stats worker.

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
