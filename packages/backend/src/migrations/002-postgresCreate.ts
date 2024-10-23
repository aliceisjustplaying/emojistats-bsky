import 'dotenv/config';
import pg from 'pg';

const { Client } = pg;

const client = new Client({
  connectionString: process.env.DATABASE_URL!,
});

await client.connect();

export async function createTables() {
  await client.query(`
    CREATE TABLE IF NOT EXISTS posts (
      did TEXT NOT NULL, -- ~32 characters
      rkey TEXT NOT NULL, -- ~13 characters
      text TEXT,
      has_emojis BOOLEAN NOT NULL DEFAULT FALSE,
      langs TEXT[] NOT NULL DEFAULT '{}',
      emojis TEXT[] NOT NULL DEFAULT '{}',
      created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (now() at time zone 'utc')
    );
  `);
}

createTables()
  .catch((e: unknown) => {
    console.error(e);
  })
  .finally(() => void client.end());
