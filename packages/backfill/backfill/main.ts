import { loadConfig } from "./config.js";
import { createPool } from "./db.js";
import { DimensionCache } from "./dimensions.js";
import { ParquetSink } from "./parquetSink.js";
import { EmojiPostWriter } from "./writer.js";
import { BackfillRunner } from "./runner.js";
import { setCursorCachePath } from "./util/fetch.js";
import { startMetricsServer } from "./metrics.js";
import { logger } from "./logger.js";
import { loadConsumerQueueConfig } from "./queue/config.js";
import { createRedisStreamClient } from "./queue/redisStream.js";

async function main() {
  const config = loadConfig();
  setCursorCachePath(config.cursorCachePath);

  const pool = createPool({
    databaseUrl: config.databaseUrl,
    schema: config.databaseSchema,
  });
  const dimensions = new DimensionCache(pool);
  await dimensions.hydrate();

  const parquet = await ParquetSink.create(config.parquetDir);
  const writer = new EmojiPostWriter(pool, dimensions, parquet);

  const queueConfig = loadConsumerQueueConfig();
  const redis = createRedisStreamClient({
    url: queueConfig.redisUrl,
    name: queueConfig.consumerName,
  });
  await redis.connect();

  const runner = new BackfillRunner(config, pool, writer, queueConfig, redis);
  const metricsServer = config.metricsPort
    ? startMetricsServer(config.metricsPort)
    : null;

  try {
    await runner.run();
  } finally {
    await writer.close();
    await pool.end();
    await redis.quit();
    metricsServer?.close();
  }
}

await main().catch((error) => {
  logger.error({ err: error }, "Backfill failed");
  process.exitCode = 1;
});
