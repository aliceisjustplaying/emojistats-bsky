import { existsSync } from "node:fs";
import { rm } from "node:fs/promises";
import { loadConfig } from "../backfill/config.js";
import { createPool } from "../backfill/db.js";
import { logger } from "../backfill/logger.js";
import { loadProducerQueueConfig } from "../backfill/queue/config.js";
import { createRedisStreamClient } from "../backfill/queue/redisStream.js";

async function main() {
  const config = loadConfig();
  const queueConfig = loadProducerQueueConfig();

  await resetDatabase(config.databaseUrl, config.databaseSchema);
  await resetRedis(queueConfig.redisUrl, queueConfig.streamName);
  await resetCursorCache(config.cursorCachePath);
}

async function resetDatabase(databaseUrl: string, schema: string) {
  const pool = createPool({ databaseUrl, schema });
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("TRUNCATE TABLE emoji_post");
    await client.query("TRUNCATE TABLE repo_progress");
    await client.query("TRUNCATE TABLE repo_validation_log");
    await client.query("TRUNCATE TABLE ingest_job_log");
    await client.query("TRUNCATE TABLE ingest_watermark");
    await client.query("DELETE FROM dim_client");
    await client.query("DELETE FROM dim_emoji");
    await client.query("DELETE FROM dim_language");
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

async function resetRedis(redisUrl: string, streamName: string) {
  const redis = createRedisStreamClient({ url: redisUrl, name: "cleaner" });
  await redis.connect();
  try {
    await redis.del(streamName);
    logger.info({ streamName }, "Redis stream cleared");
  } catch (error) {
    logger.error({ err: error }, "Failed to clear Redis stream");
    throw error;
  } finally {
    await redis.quit();
  }
}

async function resetCursorCache(cursorPath: string) {
  if (!cursorPath) return;
  if (!existsSync(cursorPath)) {
    logger.info({ cursorPath }, "Cursor cache already absent");
    return;
  }
  await rm(cursorPath, { force: true });
  logger.info({ cursorPath }, "Cursor cache removed");
}

await main().catch((error) => {
  logger.error({ err: error }, "Clean state script failed");
  process.exitCode = 1;
});
