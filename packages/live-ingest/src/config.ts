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
  databaseUrl: requireEnv("EMOJISTATS_DATABASE_URL"),
  databaseSchema: process.env.EMOJISTATS_DATABASE_SCHEMA ?? "public",
  redisUrl: process.env.REDIS_URL ?? "redis://127.0.0.1:6379",
  jetstreamEndpoint:
    process.env.JETSTREAM_ENDPOINT ?? "wss://jetstream.atproto.tools",
  jetstreamCursorKey: process.env.JETSTREAM_CURSOR_KEY ?? "live:cursor",
  metricsPort: optionalNumber("LIVE_METRICS_PORT", 9480),
  redisKeyPrefix: process.env.REDIS_KEY_PREFIX ?? "emoji",
  concurrency: optionalNumber("LIVE_INGEST_CONCURRENCY", 8),
  cursorOverridePath: process.env.CURSOR_OVERRIDE_PATH,
};
