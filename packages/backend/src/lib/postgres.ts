import { Kysely, PostgresDialect } from 'kysely';
import type { DB } from './schema.js';
import pg from 'pg';
const { Pool } = pg;

const db = new Kysely<DB>({
  dialect: new PostgresDialect({
    pool: new Pool({
      connectionString: process.env.DATABASE_URL,
    }),
  }),
});

export { db };
