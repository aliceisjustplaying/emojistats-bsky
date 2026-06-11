# Emoji stats for Bluesky

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
