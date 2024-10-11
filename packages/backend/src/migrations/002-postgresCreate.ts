import pg from 'pg'
const { Client } = pg

const client = new Client({
  connectionString: process.env.DATABASE_URL!,
});

await client.connect();

export async function createTables() {
  await client.query(`
    CREATE TABLE IF NOT EXISTS posts (
      id SERIAL PRIMARY KEY,
      cid TEXT NOT NULL, -- 64 characters
      did TEXT NOT NULL, -- 32 characters
      rkey TEXT NOT NULL, -- 13 characters
      has_emojis BOOLEAN NOT NULL DEFAULT FALSE,
      langs TEXT[] NOT NULL DEFAULT '{}',
      created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (now() at time zone 'utc')
    );

    CREATE TABLE IF NOT EXISTS emojis (
      id SERIAL PRIMARY KEY,
      post_id INTEGER NOT NULL,
      emoji TEXT NOT NULL,
      lang TEXT NOT NULL, -- 2 or 5 characters
      created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (now() at time zone 'utc')
    );

    CREATE TABLE IF NOT EXISTS emoji_stats (
      lang TEXT NOT NULL, -- 2 or 5 characters
      emoji TEXT NOT NULL,
      count INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (now() at time zone 'utc'),
      PRIMARY KEY (lang, emoji)
    );
  `);
}

createTables().catch((e: unknown) => { console.error(e); }).finally(() => void client.end());

