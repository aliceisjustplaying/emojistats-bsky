# Emoji Backfill

This package replays **every** Bluesky repo, filters down to `app.bsky.feed.post` records that contain emoji, and stores them in the `emoji_post` hypertable created by `schema.sql`. Each post is also appended to a Parquet file so you can redo aggregations without touching Postgres.

## Prerequisites

1. TimescaleDB/Postgres running locally (or wherever you want to stage the data) with the schema from `/schema.sql` applied.
2. Redis is **not** required for the MVP backfill, but you can seed it later using the rows in `emoji_post` + the Parquet archive.
3. Bun 1.1+ for running the TypeScript entrypoint.

## Configuration

Copy `.env.example` to `.env` and update the values:

| Variable                       | Description                                                                     |
| ------------------------------ | ------------------------------------------------------------------------------- |
| `EMOJISTATS_DATABASE_URL`      | Connection string that points at the Timescale instance seeded by `schema.sql`. |
| `EMOJISTATS_DATABASE_SCHEMA`   | Schema name (defaults to `public`).                                             |
| `BSKY_DID_PLC_URL`             | PLC directory to resolve DIDs (e.g. `https://plc.directory`).                   |
| `FALLBACK_PLC_URL`             | Optional PLC mirror.                                                            |
| `EMOJI_BACKFILL_PARQUET_DIR`   | Where Parquet files are written (one file per run).                             |
| `EMOJI_BACKFILL_CURSOR_CACHE`  | Location for `pds-cursor-cache.json`; defaults to the copy in this package.     |
| `EMOJI_BACKFILL_DID_ALLOWLIST` | Optional newline-separated list of DIDs to run (useful for targeted tests).     |
| `EMOJI_BACKFILL_DID_LIMIT`     | Optional numeric limit so you can stop after _n_ repos (handy on laptops).      |
| `EMOJI_BACKFILL_CONCURRENCY`   | How many repos to process in parallel (defaults to ~half your local cores).     |

## Running Locally

```bash
cd packages/backfill
bun install
cp .env.example .env  # edit values
bun run backfill
```

Tips for local development:

- Set `EMOJI_BACKFILL_DID_LIMIT=1000` to ingest a small slice of the network.
- You can point `EMOJI_BACKFILL_DID_ALLOWLIST` at a text file with a handful of interesting DIDs to smoke-test the pipeline.
- Parquet output lands in `EMOJI_BACKFILL_PARQUET_DIR` (default `packages/backfill/data/parquet`).

## What the backfill does

1. Enumerates every PDS via `com.atproto.sync.listRepos`, honoring cursor state in `pds-cursor-cache.json`.
2. Fetches each repo’s CAR via `com.atproto.sync.getRepo` and parses only `app.bsky.feed.post` records.
3. Runs emoji normalization from `packages/emoji-normalization`, discarding posts without emoji.
4. Ensures language/client/emoji dimension rows exist (inserting them on demand).
5. Inserts the normalized post into `emoji_post` (Timescale hypertable) in batches and streams the same data to Parquet.
6. Marks the repo as complete in `repo_progress` so restarts can skip work.

Once the backfill reaches the present, you can pivot to Jetstream ingestion to stay up to date, and you’ll have both the relational store and Parquet archive ready for analytics + Redis seeding.
