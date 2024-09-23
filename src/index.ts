import { CommitCreateEvent, Jetstream } from '@skyware/jetstream';
import emojiRegexFunc from 'emoji-regex';
import dotenv from 'dotenv';
import logger from './logger.js';
import { createServer } from "http";
import { createClient } from 'redis';
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

// Initialize Redis client
const redisClient = createClient({
  url: process.env.REDIS_URL ?? 'redis://localhost:6379',
  // Optional: Configure connection options for performance
  // For example, max retries, timeouts, etc.
});

// Handle Redis events
redisClient.on('error', (err: Error) => { logger.error('Redis Client Error', { error: err }); });
redisClient.on('connect', () => { logger.info('Connected to Redis'); });
redisClient.on('ready', () => { logger.info('Redis client ready'); });
redisClient.on('end', () => { logger.info('Redis client disconnected'); });

// Connect to Redis
await redisClient.connect();


const emojiRegex: RegExp = emojiRegexFunc();

const EMOJI_SORTED_SET_KEY = 'emojiStats';
const PROCESSED_POSTS_KEY = 'processedPosts';
const POSTS_WITH_EMOJIS_KEY = 'postsWithEmojis';
const PROCESSED_EMOJIS_KEY = 'processedEmojis';


async function handleCreate(event: CommitCreateEvent<'app.bsky.feed.post'>) {
  const { commit } = event;

  if (!commit.rkey) return;

  const { record } = commit;

  try {
    const text = record.text;

    const emojiMatches = text.match(emojiRegex);

    if (emojiMatches) {
      await redisClient.incr(POSTS_WITH_EMOJIS_KEY);

      const pipeline = redisClient.multi();
      for (const emoji of emojiMatches) {
        pipeline.zIncrBy(EMOJI_SORTED_SET_KEY, 1, emoji);
        pipeline.incr(PROCESSED_EMOJIS_KEY);
      }
      await pipeline.exec();
    }

    await redisClient.incr(PROCESSED_POSTS_KEY);

  } catch (error) {
    logger.error(`Error parsing record in "create" commit: ${(error as Error).message}`, { commit, record });
    logger.error(`Malformed record data: ${JSON.stringify(record)}`);
  }
}

async function getEmojiStats() {
  // Retrieve top emojis (up to MAX_EMOJIS)
  const topEmojis = await redisClient.zRangeWithScores(EMOJI_SORTED_SET_KEY, -MAX_EMOJIS, -1, { REV: true });


  
  // Retrieve counters
  const [processedPosts, postsWithEmojis, processedEmojis] = await redisClient.mGet([
    PROCESSED_POSTS_KEY,
    POSTS_WITH_EMOJIS_KEY,
    PROCESSED_EMOJIS_KEY
  ]);

  const postsWithoutEmojis = (Number(processedPosts) || 0) - (Number(postsWithEmojis) || 0);
  const ratio = postsWithoutEmojis > 0 ? ((Number(postsWithEmojis) || 0) / (Number(postsWithoutEmojis) || 1)).toFixed(2) : 'N/A';

  // Format top emojis
  const formattedTopEmojis = topEmojis.map(({ value, score }) => ({
    emoji: value,
    count: score,
  })).slice(0, MAX_EMOJIS);

  return {
    processedPosts: Number(processedPosts) || 0,
    processedEmojis: Number(processedEmojis) || 0,
    postsWithEmojis: Number(postsWithEmojis) || 0,
    postsWithoutEmojis,
    ratio,
    topEmojis: formattedTopEmojis,
  };
}

async function logEmojiStats() {
  const stats = await getEmojiStats();
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


setInterval(async () => {
  try {
    // Emit aggregated emoji stats every EMIT_INTERVAL milliseconds
    const stats = await getEmojiStats();
    io.emit('emojiStats', stats);
    await logEmojiStats();
  } catch (error) {
    logger.error(`Error emitting or logging emoji stats: ${(error as Error).message}`);
  }
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
  void handleCreate(event);
});

jetstream.start();

const PORT = parseInt(process.env.PORT ?? '9202', 10);

function shutdown() {
  logger.info('Shutting down gracefully...');

  try {
    void redisClient.quit();
    logger.info('Redis client disconnected');
  } catch (error) {
    logger.error('Error disconnecting Redis client:', error);
  }

  // Perform any other cleanup here
  process.exit(0);

  setTimeout(() => {
    logger.error('Forcing shutdown.');
    process.exit(1);
  }, 60000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
