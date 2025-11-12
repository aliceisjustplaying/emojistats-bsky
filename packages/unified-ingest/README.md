# Unified Ingest

Unified ingest worker that can consume from both Nexus (for backfill) and Jetstream (for live events).

## Configuration

Set `INGEST_SOURCE` environment variable:

- `nexus` - Consume from Nexus WebSocket (for backfill)
- `jetstream` - Consume from Jetstream (for live events)
- `both` - Consume from both sources simultaneously

### Required Environment Variables

- `EMOJISTATS_DATABASE_URL` - TimescaleDB connection string

### Optional Environment Variables

- `EMOJISTATS_DATABASE_SCHEMA` - Database schema (default: `public`)
- `EMOJI_BACKFILL_PARQUET_DIR` - Parquet output directory (default: `./data/parquet`)
- `EMOJI_MAX_PER_POST` - Maximum emojis per post (default: `250`)
- `INGEST_METRICS_PORT` - Prometheus metrics port (default: `0` = disabled)
- `INGEST_PROGRESS_LOG_EVERY` - Emit a per-repo progress log after this many events (default: `500`)
- `INGEST_PROGRESS_LOG_INTERVAL_MS` - Always emit a progress log at least this often (default: `30000`)
- `LOG_LEVEL` - Log level (default: `info`)

### Nexus Configuration

- `NEXUS_URL` - Nexus WebSocket URL (default: `ws://localhost:8080/channel`)
- `NEXUS_ACK_TIMEOUT` - Ack timeout in milliseconds (default: `10000`)

### Jetstream Configuration

- `JETSTREAM_ENDPOINT` - Jetstream WebSocket URL (default: `wss://jetstream.atproto.tools`)
- `JETSTREAM_CURSOR_KEY` - Redis key for cursor persistence (default: `unified:cursor`)
- `REDIS_URL` - Redis connection URL (default: `redis://127.0.0.1:6379`)
- `CURSOR_OVERRIDE` - Override cursor value (optional)

## Usage

### Testing Nexus Connection

```bash
cd packages/unified-ingest
bun run test-nexus
```

### Running the Worker

```bash
cd packages/unified-ingest
bun run start
```

## Architecture

The unified ingest worker:

1. Connects to one or both adapters (Nexus/Jetstream)
2. Receives events in `UnifiedEvent` format
3. Normalizes events (extracts emoji, language, etc.)
4. Writes to TimescaleDB and Parquet files
5. Sends acks/cursors back to adapters

## Development

See `nexus_migration_plan.md` in the root directory for step-by-step migration instructions.
