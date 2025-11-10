import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

export type BackfillConfig = {
  databaseUrl: string;
  databaseSchema: string;
  plcUrl: string;
  fallbackPlcUrl?: string;
  parquetDir: string;
  cursorCachePath: string;
  allowlistPath?: string;
  didLimit?: number;
  repoConcurrency: number;
};

const DEFAULT_PARQUET_DIR = path.resolve(
  path.dirname(fileURLToPath(new URL("../package.json", import.meta.url))),
  "data/parquet",
);

const DEFAULT_CURSOR_CACHE = path.resolve(
  path.dirname(
    fileURLToPath(new URL("../pds-cursor-cache.json", import.meta.url)),
  ),
  "pds-cursor-cache.json",
);

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var ${name}`);
  }
  return value;
}

function optionalNumber(name: string): number | undefined {
  const raw = process.env[name];
  if (!raw) return undefined;
  const parsed = Number(raw);
  if (Number.isNaN(parsed)) {
    throw new Error(`Env var ${name} must be a number`);
  }
  return parsed;
}

export function loadConfig(): BackfillConfig {
  const databaseUrl = requireEnv("EMOJISTATS_DATABASE_URL");
  const databaseSchema = process.env.EMOJISTATS_DATABASE_SCHEMA ?? "public";
  const plcUrl = requireEnv("BSKY_DID_PLC_URL");
  const fallbackPlcUrl = process.env.FALLBACK_PLC_URL || undefined;

  const parquetDir = path.resolve(
    process.env.EMOJI_BACKFILL_PARQUET_DIR ?? DEFAULT_PARQUET_DIR,
  );
  const cursorCachePath = path.resolve(
    process.env.EMOJI_BACKFILL_CURSOR_CACHE ?? DEFAULT_CURSOR_CACHE,
  );
  const allowlistPath = process.env.EMOJI_BACKFILL_DID_ALLOWLIST
    ? path.resolve(process.env.EMOJI_BACKFILL_DID_ALLOWLIST)
    : undefined;
  const didLimit = optionalNumber("EMOJI_BACKFILL_DID_LIMIT");

  const cpuCount =
    typeof os.availableParallelism === "function"
      ? os.availableParallelism()
      : os.cpus().length;
  const repoConcurrency =
    optionalNumber("EMOJI_BACKFILL_CONCURRENCY") ??
    Math.max(2, Math.min(16, Math.floor(cpuCount / 2)));

  return {
    databaseUrl,
    databaseSchema,
    plcUrl,
    fallbackPlcUrl,
    parquetDir,
    cursorCachePath,
    allowlistPath,
    didLimit,
    repoConcurrency,
  };
}
