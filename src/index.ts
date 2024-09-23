import { CommitCreateEvent, Jetstream } from '@skyware/jetstream';

import emojiRegexFunc from 'emoji-regex';
import dotenv from 'dotenv';

import logger from './logger.js';

dotenv.config();

const currentEpochMicroseconds = BigInt(Date.now()) * 1000n;


const FIREHOSE_URL = process.env.FIREHOSE_URL ?? 'wss://jetstream.atproto.tools/subscribe'; // default to Jaz's Jetstream instance
const PORT = parseInt(process.env.PORT ?? '9201', 10);

const jetstream = new Jetstream({
  wantedCollections: ['app.bsky.feed.post'],
  endpoint: FIREHOSE_URL,
  cursor: currentEpochMicroseconds.toString(),
});

const emojiRegex : RegExp = emojiRegexFunc();

interface EmojiStats {
  emoji: string;
  count: number;
}

const emojiStats: EmojiStats[] = [];
let processedPosts = 0;

function handleCreate(event: CommitCreateEvent<'app.bsky.feed.post'>) {
  
  const { commit } = event;

  if (!commit.rkey) return;

  const { record } = commit;

  try {
    const text = record.text;

    const emojiMatches = text.match(emojiRegex);

    if (emojiMatches) {
      for (const emoji of emojiMatches) {
        const existingEmoji = emojiStats.find(e => e.emoji === emoji);
        if (existingEmoji) {
          existingEmoji.count++;
        } else {
          emojiStats.push({ emoji, count: 1 });
        }
      }
    }

    processedPosts++;

    setTimeout(() => {
      console.log(`Processed ${processedPosts} posts`);
      console.dir(emojiStats, { depth: null });
    }, 2000);
  } catch (error) {
    logger.error(`Error parsing record in "create" commit: ${(error as Error).message}`, { commit, record });
    logger.error(`Malformed record data: ${JSON.stringify(record)}`);
  }
}

jetstream.on('open', () => {
  logger.info('Connected to Jetstream firehose.');
});

jetstream.on('close', () => {
  logger.info('Jetstream firehose connection closed.');
  shutdown();
});

jetstream.on('error', (error) => {
  logger.error(`Jetstream firehose error: ${error.message}`);
});

jetstream.onCreate('app.bsky.feed.post', (event) => {
  handleCreate(event);
});

jetstream.start();

function shutdown() {
  logger.info('Shutting down gracefully...');

  // ...
  process.exit(0);

  setTimeout(() => {
    logger.error('Forcing shutdown.');
    process.exit(1);
  }, 60000);
}


process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
