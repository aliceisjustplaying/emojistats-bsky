import { createPool } from "../backfill/db.js";
import { loadConfig } from "../backfill/config.js";

async function main() {
  const config = loadConfig();
  const pool = createPool({
    databaseUrl: config.databaseUrl,
    schema: config.databaseSchema,
  });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("TRUNCATE TABLE emoji_post");
    await client.query("TRUNCATE TABLE repo_progress");
    await client.query("TRUNCATE TABLE ingest_job_log");
    await client.query("TRUNCATE TABLE ingest_watermark");
    await client.query("DELETE FROM dim_client");
    await client.query("DELETE FROM dim_emoji");
    await client.query("DELETE FROM dim_language");
    await client.query("COMMIT");
    console.info("Database reset complete.");
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Failed to reset database", error);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

await main();
