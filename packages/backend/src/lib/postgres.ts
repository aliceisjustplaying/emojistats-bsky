import { Kysely, PostgresDialect } from 'kysely';
import pg from 'pg';

import type { DB } from './schema.js';

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 100, // Increased pool size to handle more concurrent inserts
  // idleTimeoutMillis: 30000, // 30 seconds idle timeout
  // connectionTimeoutMillis: 5000, // 5 seconds connection timeout
  // allowExitOnIdle: false, // Prevent pool from shutting down during high load
  port: parseInt(process.env.DATABASE_PORT ?? '5432'),
  host: process.env.DATABASE_HOST ?? 'localhost',
});

const dialect = new PostgresDialect({ pool });

const db = new Kysely<DB>({
  dialect,
});

export { pool, db };
