import { CommitCreateEvent, Jetstream } from '@skyware/jetstream';
import emojiRegexFunc from 'emoji-regex';
import dotenv from 'dotenv';
import logger from './logger.js';
import { createServer } from "http";
import { createClient } from 'redis';
import { Server, Socket } from "socket.io";
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

dotenv.config();

const FIREHOSE_URL = process.env.FIREHOSE_URL ?? 'wss://jetstream.atproto.tools/subscribe';
const MAX_EMOJIS = 1000;
const EMIT_INTERVAL = 1000;
const CURSOR_UPDATE_INTERVAL_MS = 10 * 1000;
const CURSOR_KEY = 'lastCursor';
let cursorUpdateInterval: NodeJS.Timeout | null = null;

// Initialize Redis client
const redisClient = createClient({
  url: process.env.REDIS_URL ?? 'redis://localhost:6379',
});
await redisClient.connect();

const httpServer = createServer();
const io = new Server(httpServer, {
  cors: {
    origin: ["http://localhost:5173", "https://emojitracker.bsky.sh"],
    methods: ["GET", "POST"]
  }
});

httpServer.listen(process.env.PORT ?? 3000);

let latestCursor = await getLastCursor();

const jetstream = new Jetstream({
  wantedCollections: ['app.bsky.feed.post'],
  endpoint: FIREHOSE_URL,
  cursor: latestCursor.toString(),
});



// Handle Redis events
redisClient.on('error', (err: Error) => { logger.error('Redis Client Error', { error: err }); });
redisClient.on('connect', () => { logger.info('Connected to Redis'); });
redisClient.on('ready', () => { logger.info('Redis client ready'); });
redisClient.on('end', () => { logger.info('Redis client disconnected'); });



const emojiRegex: RegExp = emojiRegexFunc();

const EMOJI_SORTED_SET_KEY = 'emojiStats';
const LANGUAGE_SORTED_SET_KEY = 'languageStats';
const PROCESSED_POSTS_KEY = 'processedPosts';
const POSTS_WITH_EMOJIS_KEY = 'postsWithEmojis';
const PROCESSED_EMOJIS_KEY = 'processedEmojis';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const incrementEmojisScript = fs.readFileSync(
  path.join(__dirname, 'lua', 'incrementEmojis.lua'),
  'utf8'
);

// Register the script
const SCRIPT_SHA = await redisClient.scriptLoad(incrementEmojisScript);

interface LanguageStat {
  language: string;
  count: number;
}

async function getLastCursor(): Promise<bigint> {
  const result = await redisClient.get(CURSOR_KEY);
  if (!result) {
    const currentEpochMicroseconds = BigInt(Date.now()) * 1000n;
    await redisClient.set(CURSOR_KEY, currentEpochMicroseconds.toString());
    logger.info(`Initialized cursor with value: ${currentEpochMicroseconds}`);
    return currentEpochMicroseconds;
  }
  return BigInt(result);
}

async function updateLastCursor(newCursor: bigint): Promise<void> {
  await redisClient.set(CURSOR_KEY, newCursor.toString());
}

function initializeCursorUpdate() {
  cursorUpdateInterval = setInterval(() => {
    if (latestCursor > 0n) {
      updateLastCursor(latestCursor)
        .then(() => {
          logger.info(`Cursor updated to ${latestCursor}`);
        })
        .catch((error: unknown) => {
          logger.error(`Error updating cursor: ${(error as Error).message}`);
        });
    }
  }, CURSOR_UPDATE_INTERVAL_MS);
}

async function handleCreate(event: CommitCreateEvent<'app.bsky.feed.post'>) {
  const { commit } = event;

  if (!commit.rkey) return;

  const { record } = commit;

  try {
    if (BigInt(event.time_us) > latestCursor) {
      latestCursor = BigInt(event.time_us);
    }
    let langs = new Set<string>();
    if (record.langs && Array.isArray(record.langs)) {
      langs = new Set(
        record.langs.map((lang: string) =>
          lang.split('-')[0].toLowerCase()
        )
      );
    } else {
      logger.debug(
        `"langs" field is missing or invalid in record ${JSON.stringify(
          record
        )}`
      );
      langs.add('UNKNOWN');
    }

    const text = record.text;
    const emojiMatches = text.match(emojiRegex) ?? [];

    logger.debug(`Processing post with languages: ${Array.from(langs).join(', ')}`);
    logger.debug(`Found ${emojiMatches.length} emojis in post.`);

    if (emojiMatches.length > 0) {
      const stringifiedLangs = JSON.stringify(Array.from(langs));

      const emojiPromises = emojiMatches.map((emoji, i) => {
        const isFirstEmoji = i === 0 ? "1" : "0";
        return redisClient.evalSha(SCRIPT_SHA, {
          arguments: [emoji, stringifiedLangs, isFirstEmoji]
        });
      });

      await Promise.all([...emojiPromises, redisClient.incr(PROCESSED_POSTS_KEY)]);
      logger.debug(`Emojis updated for languages: ${Array.from(langs).join(', ')}`);
    }

    await redisClient.incr(PROCESSED_POSTS_KEY);
  } catch (error) {
    logger.error(
      `Error processing "create" commit: ${(error as Error).message}`,
      { commit, record }
    );
    logger.error(`Malformed record data: ${JSON.stringify(record)}`);
  }
}

async function getEmojiStats() {
  const [topEmojisDesc, globalCounters] = await Promise.all([
    redisClient.zRangeWithScores(EMOJI_SORTED_SET_KEY, 0, MAX_EMOJIS - 1, { REV: true }),
    redisClient.mGet([PROCESSED_POSTS_KEY, POSTS_WITH_EMOJIS_KEY, PROCESSED_EMOJIS_KEY])
  ]);

  const [processedPosts, postsWithEmojis, processedEmojis] = globalCounters;

  const postsWithoutEmojis = (Number(processedPosts) || 0) - (Number(postsWithEmojis) || 0);
  const ratio = postsWithoutEmojis > 0 ? ((Number(postsWithEmojis) || 0) / (Number(postsWithoutEmojis) || 1)).toFixed(2) : 'N/A';

  const formattedTopEmojis = topEmojisDesc.map(({ value, score }) => ({
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

async function getLanguageStats(): Promise<LanguageStat[]> {
  const topLanguagesDesc = await redisClient.zRangeWithScores(LANGUAGE_SORTED_SET_KEY, 0, 9, { REV: true });
  
  return topLanguagesDesc.map(({ value, score }) => ({
    language: value,
    count: score,
  }));
}

async function getTopEmojisForLanguage(language: string) {
  const topEmojisDesc = await redisClient.zRangeWithScores(language, 0, MAX_EMOJIS - 1, { REV: true });
  
  const formattedTopEmojis = topEmojisDesc.map(({ value, score }) => ({
    emoji: value,
    count: score,
  })).slice(0, MAX_EMOJIS);

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
}, 10 * 1000);

io.on('connection', (socket: Socket) => {
  logger.info('A user connected');

  socket.on('getTopEmojisForLanguage', async (language: string) => {
    try {
      const topEmojis = await getTopEmojisForLanguage(language);
      console.log(`Emitting topEmojisForLanguage for ${language}:`, topEmojis);
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

function shutdown() {
  logger.info('Shutting down gracefully...');

  setTimeout(() => {
    logger.error('Forcing shutdown.');
    process.exit(1);
  }, 60000);

  if (cursorUpdateInterval) {
    clearInterval(cursorUpdateInterval);
  }

  redisClient.quit().then(() => {
    logger.info('Redis client disconnected');
  }).catch((error: unknown) => {
    logger.error('Error disconnecting Redis client:', error);
  }).finally(() => {
    process.exit(0);
  });
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
