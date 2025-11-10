import "dotenv/config";
import { config } from "./config.js";
import { createPool } from "./db.js";
import { createRedisClient, RedisEmojiStore } from "./redisStore.js";
import { logger } from "./logger.js";

async function main() {
  const pool = createPool(config.databaseUrl, config.databaseSchema);
  const redisClient = await createRedisClient(config.redisUrl);
  const store = new RedisEmojiStore(redisClient, config.redisKeyPrefix);
  try {
    const globalRecords = await pool.query<{ glyph: string; posts: number }>(
      `SELECT e.glyph, SUM(ds.post_count)::bigint AS posts
		FROM emoji_daily_stats ds
		JOIN dim_emoji e ON e.emoji_id = ds.emoji_id
		GROUP BY e.glyph`,
    );
    await store.seedGlobal(globalRecords.rows);

    const langRecords = await pool.query<{
      glyph: string;
      lang: string;
      posts: number;
    }>(
      `SELECT e.glyph, l.bcp47 AS lang, SUM(ds.post_count)::bigint AS posts
		FROM emoji_daily_stats ds
		JOIN dim_emoji e ON e.emoji_id = ds.emoji_id
		JOIN dim_language l ON l.lang_id = ds.lang_id
		GROUP BY e.glyph, l.bcp47`,
    );
    await store.seedByLanguage(langRecords.rows);

    logger.info(
      { global: globalRecords.rowCount, langRows: langRecords.rowCount },
      "Redis seed complete",
    );
  } finally {
    await redisClient.disconnect();
    await pool.end();
  }
}

await main().catch((err) => {
  logger.error({ err }, "Failed to seed Redis");
  process.exit(1);
});
