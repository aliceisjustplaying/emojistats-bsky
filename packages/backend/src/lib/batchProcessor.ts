import { Insertable } from 'kysely';

import logger from './logger.js';
import { db } from './postgres.js';
import { Emojis, Posts } from './schema.js';

export async function flushBatchToDatabase(batch: { postData: Insertable<Posts>; emojiData: Insertable<Emojis>[] }[]) {
  const posts: Insertable<Posts>[] = batch.flatMap((item) => item.postData);
  const emojis: Insertable<Emojis>[] = batch.flatMap((item) => item.emojiData);

  if (posts.length === 0 && emojis.length === 0) {
    return;
  }

  try {
    await db.transaction().execute(async (tx) => {
      if (posts.length > 0) {
        await tx
          .insertInto('posts')
          .values(posts)
          .onConflict((b) => b.columns(['did', 'rkey']).doNothing())
          .execute();
      }

      if (emojis.length > 0) {
        await tx.insertInto('emojis').values(emojis).execute();
      }
    });
    logger.info(`Successfully inserted ${posts.length} posts and ${emojis.length} emojis.`);
  } catch (error) {
    logger.error(`Error inserting batch into database: ${(error as Error).message}`);
    throw error; // Let the worker handle retries
  }
}
