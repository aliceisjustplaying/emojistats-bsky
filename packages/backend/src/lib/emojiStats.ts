import { CommitCreateEvent } from '@skyware/jetstream';
import { batchNormalizeEmojis } from 'emoji-normalization';
import emojiRegexFunc from 'emoji-regex';
import { Insertable } from 'kysely';

import { MAX_EMOJIS, MAX_TOP_LANGUAGES } from '../config.js';
import logger from './logger.js';
import { concurrentHandleCreates, concurrentPostgresInserts, postProcessingDuration } from './metrics.js';
import { db } from './postgres.js';
import { Emojis, Posts } from './schema.js';
import { LanguageStat } from './types.js';

const emojiRegex: RegExp = emojiRegexFunc();

const BATCH_SIZE = 5000;
const BATCH_TIMEOUT_MS = 1000;

let postBatch: Insertable<Posts>[] = [];
let emojiBatch: Insertable<Emojis>[] = [];

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
  if (postBatch.length === 0 && emojiBatch.length === 0) {
    isBatching = false;
    return;
  }

  const currentPostBatch = [...postBatch];
  const currentEmojiBatch = [...emojiBatch];
  postBatch = [];
  emojiBatch = [];
  isBatching = false;

  concurrentPostgresInserts.inc();

  try {
    await db.transaction().execute(async (tx) => {
      await tx
        .insertInto('posts')
        .values(currentPostBatch)
        .onConflict((b) => b.columns(['did', 'rkey']).doNothing())
        .execute();
      await tx.insertInto('emojis').values(currentEmojiBatch).execute();
    });
    concurrentPostgresInserts.dec();
    return;
  } catch (error) {
    logger.error(`Error flushing PostgreSQL batch: ${(error as Error).message}`);
    console.log(currentPostBatch.length);
    console.log(currentEmojiBatch.length);
    console.dir(currentPostBatch, { depth: null, colors: true });
    console.dir(currentEmojiBatch, { depth: null, colors: true });
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
        created_at: new Date(),
      });

      if (has_emojis) {
        emojiBatch.push(
          ...normalizedEmojis.map((emoji) => ({
            did,
            rkey,
            emoji,
            lang: langsSet.values().next().value ?? 'unknown',
            created_at: new Date(),
          })),
        );
      }

      if ((postBatch.length >= BATCH_SIZE || emojiBatch.length >= BATCH_SIZE) && !isBatching) {
        isBatching = true;
        await flushPostgresBatch();
      } else if (!isBatching) {
        scheduleBatchFlush();
      }
    } catch (error) {
      logger.error('Error processing "create" commit:', error);
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

export async function getTopEmojisForLanguage(language: string) {
  return await db
    .selectFrom('emoji_stats_per_language')
    .select(['emoji', db.fn.sum('count').as('total_count')])
    .where('lang', '=', language)
    .groupBy('emoji')
    .orderBy('total_count', 'desc')
    .limit(MAX_EMOJIS)
    .execute();
}

export async function getEmojiStats() {
  const topEmojisOverall = await db
    .selectFrom('emoji_stats_overall')
    .select(['emoji', db.fn.sum('count').as('total_count')])
    .groupBy('emoji')
    .orderBy('total_count', 'desc')
    .limit(MAX_EMOJIS)
    .execute();

  const topEmojisPerLanguage = await db
    .selectFrom('emoji_stats_per_language')
    .select(['lang', 'emoji', db.fn.sum('count').as('total_count')])
    .groupBy(['lang', 'emoji'])
    .orderBy('lang', 'asc')
    .orderBy('total_count', 'desc')
    .limit(MAX_EMOJIS)
    .execute();

  const topLanguages = await db
    .selectFrom('language_stats')
    .select(['lang', db.fn.sum('count').as('count')])
    .groupBy('lang')
    .orderBy('count', 'desc')
    .limit(MAX_TOP_LANGUAGES)
    .execute();

  // Calculate ratio
  const postsWithEmojis = topEmojisPerLanguage.length;
  const postsWithoutEmojis = 0; // Since Redis is removed, adjust logic if necessary
  const ratio = postsWithoutEmojis > 0 ? (postsWithEmojis / postsWithoutEmojis).toFixed(4) : 'N/A';

  // Format top emojis
  const formattedTopEmojis = topEmojisOverall
    .map(({ emoji, total_count }) => ({
      emoji,
      count: Number(total_count),
    }))
    .slice(0, MAX_EMOJIS);

  return {
    processedPosts: topEmojisOverall.length, // Adjust as needed
    processedEmojis: topEmojisOverall.reduce((sum, e) => sum + Number(e.total_count), 0),
    postsWithEmojis: postsWithEmojis,
    postsWithoutEmojis: postsWithoutEmojis,
    topLanguages: topLanguages,
    ratio,
    topEmojis: formattedTopEmojis,
  };
}

export async function getTopLanguages(): Promise<LanguageStat[]> {
  const topLanguagesDesc = await db
    .selectFrom('language_stats')
    .select(['lang', db.fn.sum('count').as('count')])
    .groupBy('lang')
    .orderBy('count', 'desc')
    .limit(MAX_TOP_LANGUAGES)
    .execute();

  return topLanguagesDesc.map(({ lang, count }) => ({
    language: lang ?? 'unknown',
    count: Number(count),
  }));
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

export async function getEmojiStatsPerLanguage() {
  const result = await db
    .selectFrom('emoji_stats_per_language')
    .select(['lang', 'emoji', db.fn.sum('count').as('total_count')])
    .groupBy(['lang', 'emoji'])
    .orderBy('total_count', 'desc')
    .limit(100)
    .execute();

  return result;
}

export async function getEmojiStatsOverall() {
  const result = await db
    .selectFrom('emoji_stats_overall')
    .select(['emoji', db.fn.sum('count').as('total_count')])
    .groupBy('emoji')
    .orderBy('total_count', 'desc')
    .limit(100)
    .execute();

  return result;
}
