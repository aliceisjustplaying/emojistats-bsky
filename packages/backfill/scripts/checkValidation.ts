import type { Pool } from "pg";
import pLimit from "p-limit";
import { loadConfig } from "../backfill/config.js";
import { countRepoEmojiPosts, createPool } from "../backfill/db.js";

async function main() {
  const config = loadConfig();
  const pool = createPool({
    databaseUrl: config.databaseUrl,
    schema: config.databaseSchema,
  });
  const dids = process.argv.slice(2);
  try {
    const targets = await fetchTargets(pool, dids);
    if (targets.length === 0) {
      console.log("No repos found to check.");
      return;
    }
    const limit = pLimit(16);
    await Promise.all(
      targets.map((target) =>
        limit(async () => {
          const dbCount = await countRepoEmojiPosts(pool, target.repo_did);
          const latestValidation = await fetchLatestValidation(
            pool,
            target.repo_did,
          );
          const snapshotCount = toNumber(target.last_snapshot_row_count);
          const parquetCount = toNumber(target.last_snapshot_parquet_count);
          const status = computeStatus(
            snapshotCount,
            dbCount,
            latestValidation,
          );
          if (status !== "ok") {
            console.log(
              JSON.stringify({
                did: target.repo_did,
                status,
                snapshotCount,
                parquetCount,
                dbCount,
                snapshotPath: target.last_snapshot_path,
                lastValidatedAt: latestValidation?.validated_at ?? null,
                lastProcessedRows: latestValidation?.processed_rows ?? null,
              }),
            );
          }
        }),
      ),
    );
  } finally {
    await pool.end();
  }
}

type TargetRow = {
  repo_did: string;
  last_snapshot_row_count: number | string | null;
  last_snapshot_parquet_count: number | string | null;
  last_snapshot_path: string | null;
};

type ValidationRow = {
  processed_rows: number;
  validated_at: string;
};

async function fetchTargets(pool: Pool, dids: string[]) {
  if (dids.length === 0) {
    const { rows } = await pool.query<TargetRow>(
      "SELECT repo_did, last_snapshot_row_count, last_snapshot_parquet_count, last_snapshot_path FROM repo_progress WHERE backfill_complete = true",
    );
    return rows;
  }
  const { rows } = await pool.query<TargetRow>(
    "SELECT repo_did, last_snapshot_row_count, last_snapshot_parquet_count, last_snapshot_path FROM repo_progress WHERE repo_did = ANY($1)",
    [dids],
  );
  return rows;
}

async function fetchLatestValidation(pool: Pool, did: string) {
  const { rows } = await pool.query<ValidationRow>(
    `SELECT processed_rows, validated_at FROM repo_validation_log WHERE repo_did = $1 ORDER BY validated_at DESC LIMIT 1`,
    [did],
  );
  return rows[0];
}

function toNumber(value: number | string | null): number | null {
  if (value === null || value === undefined) return null;
  const num = typeof value === "number" ? value : Number(value);
  return Number.isNaN(num) ? null : num;
}

function computeStatus(
  snapshotCount: number | null,
  dbCount: number,
  latestValidation?: ValidationRow,
) {
  if (snapshotCount === null) {
    return latestValidation ? "partial" : "unvalidated";
  }
  return snapshotCount === dbCount ? "ok" : "drift";
}

await main().catch((error) => {
  console.error("Validation check failed", error);
  process.exitCode = 1;
});
