import { CommitCreateEvent } from '@skyware/jetstream';
import emojiRegexFunc from 'emoji-regex';
import { Insertable } from 'kysely';

import { MAX_EMOJIS, MAX_TOP_LANGUAGES } from '../config.js';
import { batchNormalizeEmojis } from './emojiNormalization.js';
import logger from './logger.js';
import {
  concurrentHandleCreates,
  concurrentPostgresInserts,
  concurrentRedisInserts,
  incrementTotalEmojis,
  incrementTotalPosts,
  postProcessingDuration,
  totalPostsWithEmojis,
  totalPostsWithoutEmojis,
} from './metrics.js';
import { db } from './postgres.js';
import { SCRIPT_SHA, redis } from './redis.js';
import { Posts } from './schema.js';
import { LanguageStat } from './types.js';

const emojiRegex: RegExp = emojiRegexFunc();

const EMOJI_SORTED_SET = 'emojiStats';
const LANGUAGE_SORTED_SET = 'languageStats';
const PROCESSED_POSTS = 'processedPosts';
const POSTS_WITH_EMOJIS = 'postsWithEmojis';
const POSTS_WITHOUT_EMOJIS = 'postsWithoutEmojis';
const PROCESSED_EMOJIS = 'processedEmojis';

const BATCH_SIZE = 1000;
const BATCH_TIMEOUT_MS = 1000;

let postBatch: Insertable<Posts>[] = [];
let isBatching = false;
let batchTimer: NodeJS.Timeout | null = null;

let isShuttingDown = false;
let ongoingHandleCreates = 0;
let shutdownPromise: Promise<void> | null = null;

function createShutdownPromise(): Promise<void> {
  return new Promise<void>((resolve) => {
    const checkCompletion = setInterval(() => {
      logger.info(`Shutting down, ongoing handleCreates: ${ongoingHandleCreates}`);
      if (isShuttingDown && ongoingHandleCreates === 0) {
        logger.info('All ongoing handleCreate operations have finished.');
        clearInterval(checkCompletion);
        resolve();
      }
    }, 50);
  });
}

export function initiateShutdown(): Promise<void> {
  if (!shutdownPromise) {
    isShuttingDown = true;
    shutdownPromise = createShutdownPromise();
  }
  return shutdownPromise;
}

/**
 * Flush the current batch to the PostgreSQL database.
 */
export async function flushPostgresBatch() {
  if (postBatch.length === 0) {
    isBatching = false;
    return;
  }

  const currentBatch = [...postBatch];
  postBatch = [];
  isBatching = false;

  concurrentPostgresInserts.inc();

  try {
    await db.transaction().execute(async (tx) => {
      // Bulk insert posts
      await tx
        .insertInto('posts')
        .values(
          currentBatch.map((post) => ({
            did: post.did,
            rkey: post.rkey,
            text: post.text,
            has_emojis: post.has_emojis,
            langs: post.langs,
            emojis: post.emojis,
            created_at: post.created_at,
          })),
        )
        .execute();
    });
    concurrentPostgresInserts.dec();
  } catch (error) {
    logger.error(`Error flushing PostgreSQL batch: ${(error as Error).message}`);
    console.dir(error, { depth: null, colors: true });
    console.dir(currentBatch, { depth: null, colors: true });
    process.exit(1);
    // Optionally, you can re-add the failed batch back to `postBatch` for retry
    // postBatch = currentBatch.concat(postBatch);
  }
}

function scheduleBatchFlush() {
  if (batchTimer) {
    return;
  }
  batchTimer = setTimeout(() => {
    batchTimer = null;
    void flushPostgresBatch();
  }, BATCH_TIMEOUT_MS);
}

export async function handleCreate(event: CommitCreateEvent<'app.bsky.feed.post'>) {
  ongoingHandleCreates++;
  concurrentHandleCreates.inc();
  try {
    const timer = postProcessingDuration.startTimer();
    const { commit, did } = event;

    if (!commit.rkey) return;

    const { record, rkey } = commit;
    const { langs, text } = record;

    try {
      let langsSet = new Set<string>();
      if (langs && Array.isArray(langs) && langs.length > 0) {
        langsSet = new Set(langs);
      } else {
        langsSet.add('unknown');
      }

      const emojiMatches = text.match(emojiRegex) ?? [];
      const normalizedEmojis = batchNormalizeEmojis(emojiMatches);
      const has_emojis = normalizedEmojis.length > 0;

      // Add the post to the batch
      postBatch.push({
        did,
        rkey,
        text,
        has_emojis,
        langs: Array.from(langsSet),
        emojis: normalizedEmojis,
      });

      if (postBatch.length >= BATCH_SIZE && !isBatching) {
        isBatching = true;
        await flushPostgresBatch();
      } else if (!isBatching) {
        scheduleBatchFlush();
      }

      /* step 2: redis */
      concurrentRedisInserts.inc();
      if (!has_emojis) {
        await redis.incr(POSTS_WITHOUT_EMOJIS);
        totalPostsWithoutEmojis.inc();
      } else {
        await redis.evalSha(SCRIPT_SHA, {
          arguments: [JSON.stringify(normalizedEmojis), JSON.stringify(Array.from(langsSet))],
        });

        incrementTotalEmojis(normalizedEmojis.length);
        totalPostsWithEmojis.inc();
      }

      /* step 3: global metrics */
      await redis.incr(PROCESSED_POSTS);
      incrementTotalPosts();
      concurrentRedisInserts.dec();
    } catch (error) {
      console.error('Error processing "create" commit:', error);
      console.dir(commit, { depth: null, colors: true });
      console.dir(record, { depth: null, colors: true });
    } finally {
      timer();
    }
  } finally {
    concurrentHandleCreates.dec();
    ongoingHandleCreates--;
  }
}

export async function getEmojiStats() {
  /*
const EMOJI_SORTED_SET = 'emojiStats';
const LANGUAGE_SORTED_SET = 'languageStats';
const PROCESSED_POSTS = 'processedPosts';
const POSTS_WITH_EMOJIS = 'postsWithEmojis';
const POSTS_WITHOUT_EMOJIS = 'postsWithoutEmojis';
const PROCESSED_EMOJIS = 'processedEmojis';
*/
  const [topEmojisDesc, globalCounters] = await Promise.all([
    redis.zRangeWithScores(EMOJI_SORTED_SET, 0, MAX_EMOJIS - 1, { REV: true }),
    redis.mGet([PROCESSED_POSTS, POSTS_WITH_EMOJIS, POSTS_WITHOUT_EMOJIS, PROCESSED_EMOJIS]),
  ]);

  const [processedPosts, postsWithEmojis, postsWithoutEmojis, processedEmojis] = globalCounters;

  const ratio =
    Number(postsWithoutEmojis) > 0 ?
      ((Number(postsWithEmojis) || 0) / (Number(postsWithoutEmojis) || 1)).toFixed(4)
    : 'N/A';

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

export async function getTopLanguages(): Promise<LanguageStat[]> {
  const topLanguagesDesc = await redis.zRangeWithScores(LANGUAGE_SORTED_SET, 0, MAX_TOP_LANGUAGES - 1, {
    REV: true,
  });

  return topLanguagesDesc.map(({ value, score }) => ({
    language: value,
    count: score,
  }));
}

export async function getTopEmojisForLanguage(language: string) {
  const topEmojisDesc = await redis.zRangeWithScores(language, 0, MAX_EMOJIS - 1, { REV: true });

  const formattedTopEmojis = topEmojisDesc
    .map(({ value, score }) => ({
      emoji: value,
      count: score,
    }))
    .slice(0, MAX_EMOJIS);

  return formattedTopEmojis;
}

export async function logEmojiStats() {
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
