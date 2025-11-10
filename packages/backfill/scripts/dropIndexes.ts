import { Database } from "@zeppelin-social/bsky-backfill";
import { writeFileSync } from "node:fs";

const db = new Database({
  url: process.env.BSKY_DB_POSTGRES_URL,
  schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
  poolSize: 100,
});

const indexes = await db.pool.query(`
		SELECT pg_get_indexdef(i.indexrelid) AS createcmd,
			   'DROP INDEX ' || i.indexrelid::regclass AS dropcmd
		FROM pg_index i
		JOIN pg_class cl ON cl.oid = i.indexrelid
		LEFT JOIN pg_constraint co ON co.conindid = i.indexrelid
		WHERE cl.relname LIKE '%_idx'
		AND co.conindid IS NULL
		`);

await Promise.all(indexes.rows.map(({ dropcmd }) => db.pool.query(dropcmd)));

const create = indexes.rows.map(({ createcmd }) => createcmd).join(";\n");
console.log(create);
writeFileSync("create.sql", create);
