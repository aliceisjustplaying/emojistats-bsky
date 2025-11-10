import { Pool } from "pg";
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

export async function insertEmojiRows(
  pool: Pool,
  rows: PreparedEmojiRow[],
): Promise<void> {
  if (rows.length === 0) return;
  const columns = [
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
  ];
  const values: Array<string | number | Date | number[] | null | boolean> = [];
  let paramIndex = 1;
  const tuples = rows.map((row) => {
    const placeholders = [
      row.postUri,
      row.repoDid,
      row.rkey,
      row.seq,
      row.createdAt,
      row.receivedAt,
      row.langId,
      row.clientId,
      row.emojiIds,
      row.authorDid,
      row.replyRootUri,
      row.replyParentUri,
      false,
    ];
    const tuplePlaceholders = placeholders
      .map(() => `$${paramIndex++}`)
      .join(", ");
    values.push(...placeholders);
    return `(${tuplePlaceholders})`;
  });

  const sql = `INSERT INTO emoji_post (${columns.join(", ")}) VALUES ${tuples.join(", ")}
	ON CONFLICT (repo_did, created_at, post_uri) DO NOTHING`;
  await pool.query(sql, values);
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

export async function markRepoComplete(pool: Pool, did: string) {
  await pool.query(
    `INSERT INTO repo_progress (repo_did, last_rev, last_seq, backfill_complete)
	VALUES ($1, $2, $3, true)
	ON CONFLICT (repo_did) DO UPDATE SET last_rev = EXCLUDED.last_rev, last_seq = EXCLUDED.last_seq, backfill_complete = true, updated_at = NOW()`,
    [did, "backfill", 0],
  );
}
