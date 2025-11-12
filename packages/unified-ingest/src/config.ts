function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var ${name}`);
  }
  return value;
}

function optionalNumber(name: string, fallback: number): number {
  const value = process.env[name];
  if (!value) return fallback;
  const parsed = Number(value);
  if (Number.isNaN(parsed)) {
    throw new Error(`Env var ${name} must be numeric`);
  }
  return parsed;
}

export const config = {
  ingestSource: (process.env.INGEST_SOURCE ?? "nexus") as
    | "nexus"
    | "jetstream"
    | "both",
  databaseUrl: requireEnv("EMOJISTATS_DATABASE_URL"),
  databaseSchema: process.env.EMOJISTATS_DATABASE_SCHEMA ?? "public",
  parquetDir: process.env.EMOJI_BACKFILL_PARQUET_DIR ?? "./data/parquet",
  emojiMaxPerPost: optionalNumber("EMOJI_MAX_PER_POST", 250),
  metricsPort: optionalNumber("INGEST_METRICS_PORT", 0),
  progressLogEvery: optionalNumber("INGEST_PROGRESS_LOG_EVERY", 500),
  progressLogIntervalMs: optionalNumber(
    "INGEST_PROGRESS_LOG_INTERVAL_MS",
    30_000,
  ),

  // Nexus config
  nexusUrl: process.env.NEXUS_URL ?? "ws://localhost:8080/channel",
  // Default must be longer than flush interval (60s) to prevent false timeouts
  nexusAckTimeout: optionalNumber("NEXUS_ACK_TIMEOUT", 90000), // 90 seconds

  // Jetstream config
  jetstreamEndpoint:
    process.env.JETSTREAM_ENDPOINT ?? "wss://jetstream.atproto.tools",
  jetstreamCursorKey: process.env.JETSTREAM_CURSOR_KEY ?? "unified:cursor",
  redisUrl: process.env.REDIS_URL ?? "redis://127.0.0.1:6379",
  cursorOverride: process.env.CURSOR_OVERRIDE
    ? Number(process.env.CURSOR_OVERRIDE)
    : undefined,
};
