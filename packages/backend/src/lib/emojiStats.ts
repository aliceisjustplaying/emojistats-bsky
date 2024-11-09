import { CommitCreateEvent } from '@skyware/jetstream';
import { batchNormalizeEmojis } from 'emoji-normalization';
import emojiRegexFunc from 'emoji-regex';
import { Insertable } from 'kysely';

import { MAX_EMOJIS, MAX_TOP_LANGUAGES } from '../config.js';
import logger from './logger.js';
import { concurrentHandleCreates, postProcessingDuration } from './metrics.js';
import { db } from './postgres.js';
import { postQueue } from './queue.js';
import { Emojis, Posts } from './schema.js';
import { LanguageStat } from './types.js';

const emojiRegex: RegExp = emojiRegexFunc();

export async function handleCreate(event: CommitCreateEvent<'app.bsky.feed.post'>) {
  concurrentHandleCreates.inc();
  try {
    const timer = postProcessingDuration.startTimer();
    const { commit, did } = event;

    if (!commit.rkey) return;

    const { record, rkey } = commit;
    const { langs, text } = record;

    try {
      const langsSet = new Set<string>(langs && Array.isArray(langs) && langs.length > 0 ? langs : ['unknown']);

      const emojiMatches = text.match(emojiRegex) ?? [];
      const normalizedEmojis = batchNormalizeEmojis(emojiMatches);
      const has_emojis = normalizedEmojis.length > 0;

      const postData: Insertable<Posts> = {
        did,
        rkey,
        text,
        has_emojis,
        langs: Array.from(langsSet),
        emojis: normalizedEmojis,
        created_at: new Date(),
      };

      const emojiData: Insertable<Emojis>[] =
        has_emojis ?
          normalizedEmojis.map((emoji) => ({
            did,
            rkey,
            emoji,
            lang: langsSet.values().next().value ?? 'unknown',
            created_at: new Date(),
          }))
        : [];

      // Enqueue the event data
      await postQueue.add('process-post', { postData, emojiData });

      logger.debug(`Enqueued post ${did}-${rkey} for processing.`);
      timer();
    } catch (error) {
      logger.error('Error processing "create" commit:', error);
      console.dir(commit, { depth: null, colors: true });
      console.dir(record, { depth: null, colors: true });
    } finally {
      timer();
    }
  } finally {
    concurrentHandleCreates.dec();
  }
}

export async function getTopEmojisForLanguage(language: string) {
  return await db
    .selectFrom('emoji_stats_per_language_realtime')
    .select(['emoji', db.fn.sum('count').as('total_count')])
    .where('lang', '=', language)
    .groupBy('emoji')
    .orderBy('total_count', 'desc')
    .limit(MAX_EMOJIS)
    .execute();
}

export async function getEmojiStats() {
  const postCount = await db.selectFrom('posts').select(db.fn.countAll().as('total_count')).execute();
  const postWithEmojisCount = await db
    .selectFrom('posts')
    .where('has_emojis', '=', true)
    .select(db.fn.countAll().as('total_count'))
    .execute();
  const topEmojisOverall = await db
    .selectFrom('emoji_stats_realtime')
    .select(['emoji', db.fn.sum('count').as('total_count')])
    .groupBy('emoji')
    .orderBy('total_count', 'desc')
    .limit(MAX_EMOJIS)
    .execute();

  // const topEmojisPerLanguage = await db
  //   .selectFrom('emoji_stats_per_language_realtime')
  //   .select(['lang', 'emoji', db.fn.sum('count').as('total_count')])
  //   .groupBy(['lang', 'emoji'])
  //   .orderBy('lang', 'asc')
  //   .orderBy('total_count', 'desc')
  //   .limit(MAX_EMOJIS)
  //   .execute();

  const topLanguages = await db
    .selectFrom('language_stats_realtime')
    .select(['lang', db.fn.sum('count').as('count')])
    .groupBy('lang')
    .orderBy('count', 'desc')
    .limit(MAX_TOP_LANGUAGES)
    .execute();

  const processedPosts = Number(postCount[0].total_count);
  const postsWithEmojis = Number(postWithEmojisCount[0].total_count);
  const postsWithoutEmojis = processedPosts - postsWithEmojis;
  const processedEmojis = topEmojisOverall.reduce((sum, e) => sum + Number(e.total_count), 0);
  const ratio = postsWithoutEmojis > 0 ? (postsWithEmojis / postsWithoutEmojis).toFixed(4) : 'N/A';

  // Format top emojis
  const formattedTopEmojis = topEmojisOverall
    .map(({ emoji, total_count }) => ({
      emoji,
      count: Number(total_count),
    }))
    .slice(0, MAX_EMOJIS);

  return {
    processedPosts,
    processedEmojis,
    postsWithEmojis,
    postsWithoutEmojis,
    topLanguages,
    ratio,
    topEmojis: formattedTopEmojis,
  };
}

export async function getTopLanguages(): Promise<LanguageStat[]> {
  const topLanguagesDesc = await db
    .selectFrom('language_stats_realtime')
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
    .selectFrom('emoji_stats_per_language_realtime')
    .select(['lang', 'emoji', db.fn.sum('count').as('total_count')])
    .groupBy(['lang', 'emoji'])
    .orderBy('total_count', 'desc')
    .limit(100)
    .execute();

  return result;
}

export async function getEmojiStatsOverall() {
  const result = await db
    .selectFrom('emoji_stats_realtime')
    .select(['emoji', db.fn.sum('count').as('total_count')])
    .groupBy('emoji')
    .orderBy('total_count', 'desc')
    .limit(100)
    .execute();

  return result;
}
