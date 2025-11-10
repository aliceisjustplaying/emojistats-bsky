# Live Ingest

Consumes the Bluesky Jetstream firehose, normalizes emoji-bearing posts, writes them into the Timescale schema, and mirrors counts into Redis for real-time dashboards.

## Scripts

- `bun run start` – starts the Jetstream consumer. Posts with more than `EMOJI_MAX_PER_POST` emojis (default 250) are ignored to avoid spam skew.
- `bun run seed-redis` – preloads Redis from existing Timescale aggregates.

Configure via `.env` (see `src/config.ts` for the full list, including `EMOJI_MAX_PER_POST`).
