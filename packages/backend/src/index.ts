import { CommitCreateEvent, Jetstream } from '@skyware/jetstream';
import dotenv from 'dotenv';
import emojiRegexFunc from 'emoji-regex';
import fs from 'fs';
import { createServer } from 'http';
import path from 'path';
import { dirname } from 'path';
import { createClient } from 'redis';
import { Server, Socket } from 'socket.io';
import { fileURLToPath } from 'url';

import logger from './logger.js';

dotenv.config();

const FIREHOSE_URL = process.env.FIREHOSE_URL ?? 'wss://jetstream.atproto.tools/subscribe';
const MAX_EMOJIS = 3790; // Per Unicode 16.0
const EMIT_INTERVAL = 1000;
const LOG_INTERVAL = 2 * 1000;
const TRIM_LANGUAGE_CODES = false;
const CURSOR_UPDATE_INTERVAL = 10 * 1000;

/* Redis initialization */
const redisClient = createClient({
  url: process.env.REDIS_URL ?? 'redis://localhost:6379',
});

redisClient.on('error', (err: Error) => {
  logger.error('Redis Client Error', { error: err });
});

redisClient.on('connect', () => {
  logger.info('Connected to Redis');
});

redisClient.on('ready', () => {
  logger.info('Redis client ready');
});

redisClient.on('end', () => {
  logger.info('Redis client disconnected');
});

await redisClient.connect();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const incrementEmojisScript = fs.readFileSync(path.join(__dirname, 'lua', 'incrementEmojis.lua'), 'utf8');
const SCRIPT_SHA = await redisClient.scriptLoad(incrementEmojisScript);
/* End Redis initialization */

/* cursor initialization */
let latestCursor = await getLastCursor();

export async function getLastCursor(): Promise<string> {
  logger.debug('Getting last cursor...');
  const result = await redisClient.get('cursor');
  if (!result) {
    logger.info('No cursor found, initializing with current epoch in microseconds...');
    const currentEpochMicroseconds = BigInt(Date.now()) * 1000n;
    await redisClient.set('cursor', currentEpochMicroseconds.toString());
    logger.info(
      `Initialized cursor with value: ${currentEpochMicroseconds} (${new Date(Number(currentEpochMicroseconds.toString()) / 1000).toISOString()})`,
    );
    return currentEpochMicroseconds.toString();
  }
  logger.info(`Returning cursor from Redis: ${result} (${new Date(Number(result) / 1000).toISOString()})`);
  return result;
}

export async function updateLastCursor(newCursor: string): Promise<void> {
  try {
    await redisClient.set('cursor', newCursor);
    logger.info(`Updated last cursor to ${newCursor} (${new Date(Number(newCursor) / 1000).toISOString()})`);
  } catch (error: unknown) {
    logger.error(`Error updating cursor: ${(error as Error).message}`);
  }
}

let cursorUpdateInterval: NodeJS.Timeout | undefined;

function initializeCursorUpdate() {
  cursorUpdateInterval = setInterval(() => {
    updateLastCursor(latestCursor)
      .then(() => {
        logger.info(`Cursor updated to ${latestCursor} at ${new Date().toISOString()}`);
      })
      .catch((error: unknown) => {
        logger.error(`Error updating cursor: ${(error as Error).message}`);
      });
  }, CURSOR_UPDATE_INTERVAL);
}

initializeCursorUpdate();
/* End cursor initialization */

/* socket.io server initialization */
const httpServer = createServer();
const io = new Server(httpServer, {
  cors: {
    origin: ['http://localhost:5173', 'https://emojitracker.bsky.sh'],
    methods: ['GET', 'POST'],
  },
});

io.on('connection', (socket: Socket) => {
  logger.info(`A user connected from ${socket.handshake.address}`);

  socket.on('getTopEmojisForLanguage', async (language: string) => {
    try {
      const topEmojis = await getTopEmojisForLanguage(language);
      socket.emit('topEmojisForLanguage', { language, topEmojis });
    } catch (error) {
      logger.error(`Error fetching top emojis for language ${language}: ${(error as Error).message}`);
      socket.emit('error', `Error fetching top emojis for language ${language}`);
    }
  });

  socket.on('disconnect', () => {
    logger.info('A user disconnected');
  });
});

httpServer.listen(process.env.PORT ?? 3000);
/* End socket.io server initialization */

/* Jetstream initialization */
const jetstream = new Jetstream({
  wantedCollections: ['app.bsky.feed.post'],
  endpoint: FIREHOSE_URL,
  cursor: latestCursor,
});

jetstream.on('open', () => {
  logger.info('Connected to Jetstream firehose.');
  if (!cursorUpdateInterval) {
    initializeCursorUpdate();
  }
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
/* End Jetstream initialization */
const emojiRegex: RegExp = emojiRegexFunc();

const EMOJI_SORTED_SET_KEY = 'emojiStats';
const LANGUAGE_SORTED_SET_KEY = 'languageStats';
const PROCESSED_POSTS_KEY = 'processedPosts';
const POSTS_WITH_EMOJIS_KEY = 'postsWithEmojis';
const PROCESSED_EMOJIS_KEY = 'processedEmojis';
interface LanguageStat {
  language: string;
  count: number;
}

async function handleCreate(event: CommitCreateEvent<'app.bsky.feed.post'>) {
  const { commit } = event;

  if (!commit.rkey) return;

  const { record } = commit;

  try {
    let langs = new Set<string>();
    if (record.langs && Array.isArray(record.langs)) {
      langs = new Set(
        record.langs.map((lang: string) =>
          // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
          TRIM_LANGUAGE_CODES ? lang.split('-')[0].toLowerCase().slice(0, 2) : lang.toLowerCase(),
        ),
      );
    } else {
      logger.debug(`"langs" field is missing or invalid in record ${JSON.stringify(record)}`);
      langs.add('UNKNOWN');
    }

    const text = record.text;
    const emojiMatches = text.match(emojiRegex) ?? [];

    logger.debug(`Processing post with languages: ${Array.from(langs).join(', ')}`);
    logger.debug(`Found ${emojiMatches.length} emojis in post.`);

    if (emojiMatches.length > 0) {
      const stringifiedLangs = JSON.stringify(Array.from(langs));

      const emojiPromises = emojiMatches.map((emoji, i) => {
        const isFirstEmoji = i === 0 ? '1' : '0';
        return redisClient.evalSha(SCRIPT_SHA, {
          arguments: [emoji, stringifiedLangs, isFirstEmoji],
        });
      });

      await Promise.all([...emojiPromises, redisClient.incr(PROCESSED_POSTS_KEY)]);
      logger.debug(`Emojis updated for languages: ${Array.from(langs).join(', ')}`);
    }

    await redisClient.incr(PROCESSED_POSTS_KEY);
    latestCursor = event.time_us.toString();
  } catch (error) {
    logger.error(`Error processing "create" commit: ${(error as Error).message}`, { commit, record });
    logger.error(`Malformed record data: ${JSON.stringify(record)}`);
  }
}

async function getEmojiStats() {
  const [topEmojisDesc, globalCounters] = await Promise.all([
    redisClient.zRangeWithScores(EMOJI_SORTED_SET_KEY, 0, MAX_EMOJIS - 1, { REV: true }),
    redisClient.mGet([PROCESSED_POSTS_KEY, POSTS_WITH_EMOJIS_KEY, PROCESSED_EMOJIS_KEY]),
  ]);

  const [processedPosts, postsWithEmojis, processedEmojis] = globalCounters;

  const postsWithoutEmojis = (Number(processedPosts) || 0) - (Number(postsWithEmojis) || 0);
  const ratio =
    postsWithoutEmojis > 0 ? ((Number(postsWithEmojis) || 0) / (Number(postsWithoutEmojis) || 1)).toFixed(2) : 'N/A';

  const formattedTopEmojis = topEmojisDesc
    .map(({ value, score }) => ({
      emoji: value,
      count: score,
    }))
    .slice(0, MAX_EMOJIS);

  return {
    processedPosts: Number(processedPosts) || 0,
    processedEmojis: Number(processedEmojis) || 0,
    postsWithEmojis: Number(postsWithEmojis) || 0,
    postsWithoutEmojis,
    ratio,
    topEmojis: formattedTopEmojis,
  };
}

async function getLanguageStats(): Promise<LanguageStat[]> {
  const topLanguagesDesc = await redisClient.zRangeWithScores(LANGUAGE_SORTED_SET_KEY, 0, 9, { REV: true });

  return topLanguagesDesc.map(({ value, score }) => ({
    language: value,
    count: score,
  }));
}

async function getTopEmojisForLanguage(language: string) {
  const topEmojisDesc = await redisClient.zRangeWithScores(language, 0, MAX_EMOJIS - 1, { REV: true });

  const formattedTopEmojis = topEmojisDesc
    .map(({ value, score }) => ({
      emoji: value,
      count: score,
    }))
    .slice(0, MAX_EMOJIS);

  return formattedTopEmojis;
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

setInterval(() => {
  Promise.all([getEmojiStats(), getLanguageStats()])
    .then(([stats, languages]) => {
      io.emit('emojiStats', stats);
      io.emit('languageStats', languages);
    })
    .catch((error: unknown) => {
      logger.error(`Error emitting or logging emoji stats: ${(error as Error).message}`);
    });
}, EMIT_INTERVAL);

setInterval(() => {
  getEmojiStats()
    .then(() => {
      return logEmojiStats();
    })
    .catch((error: unknown) => {
      logger.error(`Error emitting or logging emoji stats: ${(error as Error).message}`);
    });
}, LOG_INTERVAL);

function shutdown() {
  logger.info('Shutting down gracefully...');

  setTimeout(() => {
    logger.error('Forcing shutdown.');
    process.exit(1);
  }, 60000);

  redisClient
    .quit()
    .then(() => {
      logger.info('Redis client disconnected');
    })
    .catch((error: unknown) => {
      logger.error('Error disconnecting Redis client:', error);
    })
    .finally(() => {
      process.exit(0);
    });
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
