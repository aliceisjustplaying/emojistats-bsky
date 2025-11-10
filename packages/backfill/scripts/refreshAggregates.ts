import { createPool } from "../backfill/db.js";
import { loadConfig } from "../backfill/config.js";

async function run() {
  const config = loadConfig();
  const pool = createPool({
    databaseUrl: config.databaseUrl,
    schema: config.databaseSchema,
  });
  const client = await pool.connect();
  try {
    await client.query(
      "CALL refresh_continuous_aggregate('language_daily_totals', NULL, NULL);",
    );
    await client.query(
      "CALL refresh_continuous_aggregate('emoji_daily_stats', NULL, NULL);",
    );
    console.info("Continuous aggregates refreshed.");
  } catch (error) {
    console.error("Failed to refresh aggregates", error);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

await run();
