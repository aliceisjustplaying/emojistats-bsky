import { CommitCreateEvent, Jetstream } from '@skyware/jetstream';
import emojiRegexFunc from 'emoji-regex';
import dotenv from 'dotenv';
import logger from './logger.js';
import { createServer } from "http";
import { Server } from "socket.io";

dotenv.config();

const FIREHOSE_URL = process.env.FIREHOSE_URL ?? 'wss://jetstream.atproto.tools/subscribe';
const MAX_EMOJIS = 1000;
const EMIT_INTERVAL = 1000;
const currentEpochMicroseconds = BigInt(Date.now()) * 1000n;

const httpServer = createServer();
const io = new Server(httpServer, {
  cors: {
    origin: "http://localhost:5173",
    methods: ["GET", "POST"]
  }
});

httpServer.listen(3000);

const jetstream = new Jetstream({
  wantedCollections: ['app.bsky.feed.post'],
  endpoint: FIREHOSE_URL,
  cursor: currentEpochMicroseconds.toString(),
});

const emojiRegex: RegExp = emojiRegexFunc();

interface EmojiStats {
  emoji: string;
  count: number;
}

const emojiStats = new Map<string, number>();
let processedPosts = 0;
let postsWithEmojis = 0;
let processedEmojis = 0;

function handleCreate(event: CommitCreateEvent<'app.bsky.feed.post'>) {
  const { commit } = event;

  if (!commit.rkey) return;

  const { record } = commit;

  try {
    const text = record.text;

    const emojiMatches = text.match(emojiRegex);

    if (emojiMatches) {
      postsWithEmojis++;
      for (const emoji of emojiMatches) {
        emojiStats.set(emoji, (emojiStats.get(emoji) || 0) + 1);
        processedEmojis++;
      }
    }

    processedPosts++;

  } catch (error) {
    logger.error(`Error parsing record in "create" commit: ${(error as Error).message}`, { commit, record });
    logger.error(`Malformed record data: ${JSON.stringify(record)}`);
  }
}

function getEmojiStats() {
  const topEmojis = Array.from(emojiStats.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, MAX_EMOJIS)
    .map(([emoji, count]) => ({ emoji, count }));

  const postsWithoutEmojis = processedPosts - postsWithEmojis;
  const ratio = postsWithoutEmojis > 0 ? (postsWithEmojis / postsWithoutEmojis).toFixed(2) : 'N/A';

  return {
    processedPosts,
    processedEmojis,
    postsWithEmojis,
    postsWithoutEmojis,
    ratio,
    topEmojis,
  };
}

function logEmojiStats() {
  const stats = getEmojiStats();
  logger.info(`Processed ${stats.processedPosts} posts`);
  logger.info(`Processed ${stats.processedEmojis} emojis`);
  logger.info(`Posts with emojis: ${stats.postsWithEmojis}`);
  logger.info(`Posts without emojis: ${stats.postsWithoutEmojis}`);
  logger.info(`Ratio of posts with emojis to posts without: ${stats.ratio}`);
  logger.info('Top 10 Emojis:');
  stats.topEmojis.slice(0, 10).forEach(({ emoji, count }) => {
    logger.info(`${emoji}: ${count}`);
  });
}

setInterval(() => {
  // Emit aggregated emoji stats every 3 seconds
  io.emit('emojiStats', getEmojiStats());
  logEmojiStats();
}, EMIT_INTERVAL);

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

const PORT = parseInt(process.env.PORT ?? '9202', 10);

function shutdown() {
  logger.info('Shutting down gracefully...');

  // Perform any cleanup here
  process.exit(0);

  setTimeout(() => {
    logger.error('Forcing shutdown.');
    process.exit(1);
  }, 60000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
