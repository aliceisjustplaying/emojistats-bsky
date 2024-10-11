import { CommitCreateEvent, Jetstream } from '@skyware/jetstream';

import { CURSOR_UPDATE_INTERVAL, FIREHOSE_URL } from '../config.js';
import { handleCreate } from './emojiStats.js';
import logger from './logger.js';
import { redis } from './redis.js';

let jetstream: Jetstream;
let cursor = 0;
let cursorUpdateInterval: NodeJS.Timeout;

function epochUsToDateTime(cursor: number): string {
  return new Date(cursor / 1000).toISOString();
}

export const initializeJetstream = async () => {
  const result = await redis.get('cursor');
  if (result === null) {
    logger.info('No cursor found, initializing with current epoch in microseconds...');
    cursor = Math.floor(Date.now() * 1000);
    await redis.set('cursor', cursor.toString());
    logger.info(`Initialized new cursor with value: ${cursor} (${epochUsToDateTime(cursor)})`);
  }
  logger.info(`Found existing cursor in Redis: ${result} (${epochUsToDateTime(Number(result))})`);

  jetstream = new Jetstream({
    wantedCollections: ['app.bsky.feed.post'],
    endpoint: FIREHOSE_URL,
    cursor,
  });

  jetstream.on('open', () => {
    logger.info('Connected to Jetstream');
    cursorUpdateInterval = setInterval(() => {
      if (jetstream.cursor) {
        logger.info(`Cursor updated to: ${jetstream.cursor} (${epochUsToDateTime(jetstream.cursor)})`);
        redis
          .set('cursor', jetstream.cursor.toString())
          .catch((err: unknown) => {
            logger.error(`Error updating cursor: ${(err as Error).message}`);
          });
      }
    }, CURSOR_UPDATE_INTERVAL);
  });

  jetstream.on('close', () => {
    clearInterval(cursorUpdateInterval);
    logger.info('Jetstream connection closed.');
  });

  jetstream.on('error', (error) => {
    logger.error(`Jetstream error: ${error.message}`);
  });

  jetstream.onCreate('app.bsky.feed.post', (event: CommitCreateEvent<'app.bsky.feed.post'>) => {
    void handleCreate(event);
  });
};

export { jetstream };
