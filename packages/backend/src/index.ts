import { CommitCreateEvent, Jetstream } from '@skyware/jetstream';
import emojiRegexFunc from 'emoji-regex';
import fs from 'fs';
import { createServer } from 'http';
import { Server, Socket } from 'socket.io';

import { EMIT_INTERVAL, FIREHOSE_URL, LOG_INTERVAL, MAX_EMOJIS, PORT, TRIM_LANGUAGE_CODES } from './config.js';
import { cursorUpdateInterval, getLastCursor, initializeCursorUpdate } from './lib/cursor.js';
import { SCRIPT_SHA, loadRedisScripts, redisClient } from './lib/redis.js';
import { Emoji, LanguageStat } from './lib/types.js';
import logger from './logger.js';

// source: https://github.com/amio/emoji.json/blob/master/emoji.json
const emojis = JSON.parse(fs.readFileSync(new URL('./data/emoji.json', import.meta.url), 'utf8')) as Emoji[];

/* redis initialization */
await redisClient.connect();
await loadRedisScripts();
/* End Redis initialization */

/* cursor initialization */
let latestCursor = await getLastCursor();
initializeCursorUpdate(latestCursor);
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

  socket.on('getEmojiInfo', (emoji: string) => {
    logger.info(`Getting emoji info for ${emoji}`);
    const emojiInfo = emojis.find((e) => e.char === emoji);
    socket.emit('emojiInfo', emojiInfo);
  });

  socket.on('disconnect', () => {
    logger.info('A user disconnected');
  });
});

httpServer.listen(PORT);
/* End socket.io server initialization */

/* Jetstream initialization */
const jetstream = new Jetstream({
  wantedCollections: ['app.bsky.feed.post'],
  endpoint: FIREHOSE_URL,
  cursor: latestCursor,
});

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
/* End Jetstream initialization */
const emojiRegex: RegExp = emojiRegexFunc();

const EMOJI_SORTED_SET_KEY = 'emojiStats';
const LANGUAGE_SORTED_SET_KEY = 'languageStats';
const PROCESSED_POSTS_KEY = 'processedPosts';
const POSTS_WITH_EMOJIS_KEY = 'postsWithEmojis';
const PROCESSED_EMOJIS_KEY = 'processedEmojis';

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
  logger.info(`Posts with: ${stats.postsWithEmojis}`);
  logger.info(`Posts without: ${stats.postsWithoutEmojis}`);
  logger.info(`Ratio: ${stats.ratio}`);
  logger.info('Top emojis:');
  stats.topEmojis.slice(0, 5).forEach(({ emoji, count }) => {
    logger.info(`${emoji}: ${count}`);
  });
  logger.info('---');
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

  clearInterval(cursorUpdateInterval);

  jetstream.close();

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
