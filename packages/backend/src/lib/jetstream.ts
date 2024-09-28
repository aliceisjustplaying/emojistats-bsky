import { CommitCreateEvent, Jetstream } from '@skyware/jetstream';

import { FIREHOSE_URL } from '../config.js';
import { initializeCursorUpdate } from './cursor.js';
import { handleCreate } from './emojiStats.js';
import logger from './logger.js';

let jetstream: Jetstream;

export const initializeJetstream = (cursor: string) => {
  jetstream = new Jetstream({
    wantedCollections: ['app.bsky.feed.post'],
    endpoint: FIREHOSE_URL,
    cursor,
  });

  jetstream.on('open', () => {
    logger.info('Connected to Jetstream');
    initializeCursorUpdate(cursor);
  });

  jetstream.on('close', () => {
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
