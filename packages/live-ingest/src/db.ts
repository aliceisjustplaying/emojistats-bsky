import { Pool } from "pg";

export type PreparedEmojiRow = {
  postUri: string;
  repoDid: string;
  rkey: string;
  seq: number;
  createdAt: Date;
  receivedAt: Date;
  langId: number;
  clientId: number | null;
  emojiIds: number[];
  authorDid: string;
  replyRootUri: string | null;
  replyParentUri: string | null;
};

export function createPool(connectionString: string, schema: string) {
  const pool = new Pool({ connectionString });
  pool.on("connect", (client) => {
    void client.query(`SET search_path TO "${schema.replace(/"/g, '""')}"`);
  });
  return pool;
}

export async function insertEmojiRows(pool: Pool, rows: PreparedEmojiRow[]) {
  if (rows.length === 0) return;
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
    values.push(...placeholders);
    const tuple = `(${placeholders.map(() => `$${paramIndex++}`).join(", ")})`;
    return tuple;
  });

  const sql = `INSERT INTO emoji_post (
		post_uri, repo_did, rkey, seq, created_at, received_at, lang_id, client_id,
		emoji_ids, author_did, reply_root_uri, reply_parent_uri, hidden
	) VALUES ${tuples.join(", ")}
	ON CONFLICT (repo_did, created_at, post_uri) DO NOTHING`;

  await pool.query(sql, values);
}
