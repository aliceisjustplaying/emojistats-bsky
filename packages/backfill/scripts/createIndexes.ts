import { Database } from "@zeppelin-social/bsky-backfill";
import { readFileSync } from "node:fs";

const db = new Database({
  url: process.env.BSKY_DB_POSTGRES_URL,
  schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
  poolSize: 50,
});

const createCmds = readFileSync("create.sql", "utf-8")
  .split("\n")
  .map((cmd) => `SET statement_timeout = 0; ${cmd}`);
await Promise.allSettled(createCmds.map((cmd) => db.pool.query(cmd)));
