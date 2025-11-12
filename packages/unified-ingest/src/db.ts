import { Pool } from "pg";
import type { PoolClient } from "pg";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { from as copyFrom } from "pg-copy-streams";
import type { PreparedEmojiRow } from "./types.js";

export type DatabaseDeps = {
  databaseUrl: string;
  schema: string;
};

const quoteIdent = (identifier: string) =>
  `"${identifier.replace(/"/g, '""')}"`;

export function createPool({ databaseUrl, schema }: DatabaseDeps) {
  const pool = new Pool({ connectionString: databaseUrl });
  const searchPath = quoteIdent(schema);
  pool.on("connect", (client) => {
    void client.query(`SET search_path TO ${searchPath}`);
  });
  return pool;
}

const STAGING_TABLE = "emoji_post_stage";
const CSV_NULL = "\\N";
const INSERT_COLUMNS = [
  "post_uri",
  "repo_did",
  "rkey",
  "seq",
  "created_at",
  "received_at",
  "lang_id",
  "client_id",
  "emoji_ids",
  "author_did",
  "reply_root_uri",
  "reply_parent_uri",
  "hidden",
] as const;

const CREATE_STAGE_TABLE_SQL = `
  CREATE TEMP TABLE IF NOT EXISTS ${STAGING_TABLE} (
    post_uri         text not null,
    repo_did         text not null,
    rkey             text not null,
    seq              bigint not null,
    created_at       timestamptz not null,
    received_at      timestamptz not null,
    lang_id          smallint not null,
    client_id        smallint,
    emoji_ids        smallint[] not null,
    author_did       text not null,
    reply_root_uri   text,
    reply_parent_uri text,
    hidden           boolean not null default false
  ) ON COMMIT DELETE ROWS;
`;

const COPY_INTO_STAGE_SQL = `
  COPY ${STAGING_TABLE} (${INSERT_COLUMNS.join(", ")})
  FROM STDIN WITH (FORMAT csv, QUOTE '"', ESCAPE '"', NULL '${CSV_NULL}')
`;

const INSERT_FROM_STAGE_SQL = `
  INSERT INTO emoji_post (${INSERT_COLUMNS.join(", ")})
  SELECT ${INSERT_COLUMNS.join(", ")} FROM ${STAGING_TABLE}
  ON CONFLICT (repo_did, created_at, post_uri) DO NOTHING
  RETURNING repo_did
`;

// Verify dimension exists before insert (helps catch cache/db mismatches)
const VERIFY_LANG_SQL = `SELECT lang_id FROM dim_language WHERE lang_id = $1`;
const VERIFY_CLIENT_SQL = `SELECT client_id FROM dim_client WHERE client_id = $1`;
const VERIFY_EMOJI_SQL = `SELECT emoji_id FROM dim_emoji WHERE emoji_id = ANY($1)`;

export async function insertEmojiRows(
  pool: Pool,
  rows: PreparedEmojiRow[],
): Promise<Map<string, number>> {
  if (rows.length === 0) return new Map();

  // Verify all dimension IDs exist before attempting insert
  // This helps catch cache/database mismatches early
  const uniqueLangIds = new Set(rows.map((r) => r.langId));
  const uniqueClientIds = new Set(
    rows.map((r) => r.clientId).filter((id): id is number => id !== null),
  );
  const allEmojiIds = new Set(rows.flatMap((r) => r.emojiIds));

  const client = await pool.connect();
  try {
    // Verify languages exist
    for (const langId of uniqueLangIds) {
      const { rows: langRows } = await client.query(VERIFY_LANG_SQL, [langId]);
      if (langRows.length === 0) {
        throw new Error(
          `Language ID ${langId} does not exist in dim_language. Cache may be out of sync with database.`,
        );
      }
    }

    // Verify clients exist (if any)
    for (const clientId of uniqueClientIds) {
      const { rows: clientRows } = await client.query(VERIFY_CLIENT_SQL, [
        clientId,
      ]);
      if (clientRows.length === 0) {
        throw new Error(
          `Client ID ${clientId} does not exist in dim_client. Cache may be out of sync with database.`,
        );
      }
    }

    // Verify emojis exist (batch check)
    if (allEmojiIds.size > 0) {
      const emojiArray = Array.from(allEmojiIds);
      const { rows: emojiRows } = await client.query(VERIFY_EMOJI_SQL, [
        emojiArray,
      ]);
      const existingEmojiIds = new Set(emojiRows.map((r) => r.emoji_id));
      const missingEmojiIds = emojiArray.filter(
        (id) => !existingEmojiIds.has(id),
      );
      if (missingEmojiIds.length > 0) {
        throw new Error(
          `Emoji IDs ${missingEmojiIds.join(", ")} do not exist in dim_emoji. Cache may be out of sync with database.`,
        );
      }
    }
  } finally {
    client.release();
  }

  // Now proceed with the insert
  const insertClient = await pool.connect();
  let inTransaction = false;
  try {
    await insertClient.query("BEGIN");
    inTransaction = true;
    await insertClient.query(CREATE_STAGE_TABLE_SQL);
    await copyRowsIntoStage(insertClient, rows);
    const result = await insertClient.query<{ repo_did: string }>(
      INSERT_FROM_STAGE_SQL,
    );
    await insertClient.query("COMMIT");
    inTransaction = false;
    const counts = new Map<string, number>();
    for (const row of result.rows) {
      const current = counts.get(row.repo_did) ?? 0;
      counts.set(row.repo_did, current + 1);
    }
    return counts;
  } catch (error) {
    if (inTransaction) {
      try {
        await insertClient.query("ROLLBACK");
      } catch {
        // ignore rollback errors so we can surface the original failure
      }
    }
    throw error;
  } finally {
    insertClient.release();
  }
}

async function copyRowsIntoStage(client: PoolClient, rows: PreparedEmojiRow[]) {
  const copyStream = client.query(copyFrom(COPY_INTO_STAGE_SQL));
  const source = Readable.from(generateCsvRows(rows), {
    objectMode: false,
  });
  await pipeline(source, copyStream);
}

function* generateCsvRows(rows: PreparedEmojiRow[]) {
  for (const row of rows) {
    yield formatRow(row);
  }
}

function formatRow(row: PreparedEmojiRow): string {
  const values: Array<string | number | boolean | Date | null> = [
    row.postUri,
    row.repoDid,
    row.rkey,
    row.seq,
    row.createdAt,
    row.receivedAt,
    row.langId,
    row.clientId ?? null,
    formatEmojiArray(row.emojiIds),
    row.authorDid,
    row.replyRootUri ?? null,
    row.replyParentUri ?? null,
    false,
  ];
  const csvRow = values
    .map((value) => formatCsvValue(value))
    .join(",")
    .concat("\n");
  return csvRow;
}

function formatEmojiArray(ids: number[]): string {
  if (ids.length === 0) {
    throw new Error("emoji_ids array cannot be empty");
  }
  return `{${ids.join(",")}}`;
}

function formatCsvValue(
  value: string | number | boolean | Date | null,
): string {
  if (value === null || value === undefined) {
    return CSV_NULL;
  }
  let serialized: string;
  if (value instanceof Date) {
    serialized = value.toISOString();
  } else if (typeof value === "boolean") {
    serialized = value ? "true" : "false";
  } else {
    serialized = String(value);
  }
  const escaped = serialized.replace(/"/g, '""');
  return `"${escaped}"`;
}

export async function countRepoEmojiPosts(pool: Pool, did: string) {
  const { rows } = await pool.query<{ count: string }>(
    "SELECT COUNT(*)::bigint AS count FROM emoji_post WHERE repo_did = $1",
    [did],
  );
  return Number(rows[0]?.count ?? 0);
}

export async function isRepoComplete(
  pool: Pool,
  did: string,
): Promise<boolean> {
  const { rows } = await pool.query<{ backfill_complete: boolean }>(
    "SELECT backfill_complete FROM repo_progress WHERE repo_did = $1",
    [did],
  );
  return rows[0]?.backfill_complete ?? false;
}

export async function markRepoPending(pool: Pool, did: string) {
  await pool.query(
    `INSERT INTO repo_progress (repo_did, last_rev, last_seq, backfill_complete)
		VALUES ($1, $2, $3, false)
		ON CONFLICT (repo_did) DO UPDATE SET backfill_complete = false, updated_at = NOW()`,
    [did, "backfill", 0],
  );
}

export async function markRepoComplete(
  pool: Pool,
  did: string,
  rowCount: number | null,
  snapshotPath: string | null,
  parquetCount: number | null,
) {
  await pool.query(
    `INSERT INTO repo_progress (repo_did, last_rev, last_seq, backfill_complete, last_snapshot_row_count, last_snapshot_path, last_snapshot_parquet_count)
		VALUES ($1, $2, $3, true, $4, $5, $6)
		ON CONFLICT (repo_did) DO UPDATE SET last_rev = EXCLUDED.last_rev, last_seq = EXCLUDED.last_seq, backfill_complete = true, last_snapshot_row_count = EXCLUDED.last_snapshot_row_count, last_snapshot_path = EXCLUDED.last_snapshot_path, last_snapshot_parquet_count = EXCLUDED.last_snapshot_parquet_count, updated_at = NOW()`,
    [did, "backfill", 0, rowCount, snapshotPath, parquetCount],
  );
}

export type RepoValidationRecord = {
  repoDid: string;
  processedRows: number;
  insertedRows: number;
  parquetRows: number;
  existingRows: number;
  totalRows: number;
  snapshotPath: string | null;
  extrasDetected: boolean;
};

export async function recordRepoValidation(
  pool: Pool,
  record: RepoValidationRecord,
) {
  await pool.query(
    `INSERT INTO repo_validation_log
      (repo_did, processed_rows, inserted_rows, parquet_rows, existing_rows, total_rows, snapshot_path, extras_detected)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [
      record.repoDid,
      record.processedRows,
      record.insertedRows,
      record.parquetRows,
      record.existingRows,
      record.totalRows,
      record.snapshotPath,
      record.extrasDetected,
    ],
  );
}
