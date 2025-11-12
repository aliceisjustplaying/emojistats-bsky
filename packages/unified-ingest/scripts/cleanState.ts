import { existsSync } from "node:fs";
import { rm } from "node:fs/promises";
import { createPool } from "../src/db.js";
import { logger } from "../src/logger.js";
import { config } from "../src/config.js";
import { createClient } from "redis";

async function main() {
  await resetDatabase();
  await resetJetstreamCursor();
  await removeCursorOverride();
}

async function resetDatabase() {
  const pool = createPool({
    databaseUrl: config.databaseUrl,
    schema: config.databaseSchema,
  });
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("TRUNCATE TABLE emoji_post CASCADE");
    await client.query("TRUNCATE TABLE repo_progress CASCADE");
    await client.query("TRUNCATE TABLE repo_validation_log CASCADE");
    await client.query("TRUNCATE TABLE ingest_job_log CASCADE");
    await client.query("TRUNCATE TABLE ingest_watermark CASCADE");
    await client.query("TRUNCATE TABLE dim_client RESTART IDENTITY CASCADE");
    await client.query("TRUNCATE TABLE dim_emoji RESTART IDENTITY CASCADE");
    await client.query("TRUNCATE TABLE dim_language RESTART IDENTITY CASCADE");
    await client.query("COMMIT");
    logger.info("Database tables reset");
  } catch (error) {
    await client.query("ROLLBACK");
    logger.error({ err: error }, "Failed to reset database");
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}

async function resetJetstreamCursor() {
  if (!config.redisUrl || !config.jetstreamCursorKey) {
    return;
  }
  const redis = createClient({ url: config.redisUrl });
  redis.on("error", (err) => logger.error({ err }, "Redis error"));
  await redis.connect();
  try {
    await redis.del(config.jetstreamCursorKey);
    logger.info(
      { cursorKey: config.jetstreamCursorKey },
      "Cleared Jetstream cursor key",
    );
  } finally {
    await redis.disconnect();
  }
}

async function removeCursorOverride() {
  const overridePath = process.env.CURSOR_OVERRIDE_FILE;
  if (!overridePath) return;
  if (!existsSync(overridePath)) {
    logger.info({ overridePath }, "Cursor override file absent");
    return;
  }
  await rm(overridePath, { force: true });
  logger.info({ overridePath }, "Removed cursor override file");
}

await main().catch((error) => {
  logger.error({ err: error }, "Clean state script failed");
  process.exitCode = 1;
});
