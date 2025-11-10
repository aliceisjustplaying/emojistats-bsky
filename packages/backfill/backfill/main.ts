import { loadConfig } from "./config.js";
import { createPool } from "./db.js";
import { DimensionCache } from "./dimensions.js";
import { ParquetSink } from "./parquetSink.js";
import { EmojiPostWriter } from "./writer.js";
import { BackfillRunner } from "./runner.js";
import { setCursorCachePath } from "./util/fetch.js";
import { startMetricsServer } from "./metrics.js";

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

  const runner = new BackfillRunner(config, pool, writer);
  const metricsServer = startMetricsServer(config.metricsPort);

  try {
    await runner.run();
  } finally {
    await writer.close();
    await pool.end();
    metricsServer.close();
  }
}

await main().catch((error) => {
  console.error("Backfill failed", error);
  process.exitCode = 1;
});
