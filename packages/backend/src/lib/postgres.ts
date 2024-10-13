import { Kysely, PostgresDialect } from 'kysely';
import pg from 'pg';

import type { DB } from './schema.js';

const { native } = pg;
const { Pool } = native!;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 100,
  // we need these two, even though they are superflous
  // because the monitoring library depends on them
  port: parseInt(process.env.DATABASE_PORT ?? '5432', 10),
  host: process.env.DATABASE_HOST ?? 'localhost',
});

const dialect = new PostgresDialect({ pool });

const db = new Kysely<DB>({
  dialect,
});

export { pool, db };
