# Live Ingest

Consumes the Bluesky Jetstream firehose, normalizes emoji-bearing posts, writes them into the Timescale schema, and mirrors counts into Redis for real-time dashboards.

## Scripts

- `bun run start` – starts the Jetstream consumer.
- `bun run seed-redis` – preloads Redis from existing Timescale aggregates.

Configure via `.env` (see `src/config.ts` for the full list).
