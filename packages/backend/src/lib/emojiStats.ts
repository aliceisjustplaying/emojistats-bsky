import { CommitCreateEvent } from '@skyware/jetstream';
import emojiRegexFunc from 'emoji-regex';
import fs from 'fs';

import { MAX_EMOJIS, MAX_TOP_LANGUAGES } from '../config.js';
import { batchNormalizeEmojis } from './emojiNormalization.js';
import logger from './logger.js';
import {
  concurrentHandleCreates,
  incrementTotalEmojis,
  incrementTotalPosts,
  postProcessingDuration,
  totalPostsWithEmojis,
  totalPostsWithoutEmojis,
} from './metrics.js';
import { db } from './postgres.js';
import { SCRIPT_SHA, redis } from './redis.js';
import { Emoji, LanguageStat } from './types.js';

const emojiRegex: RegExp = emojiRegexFunc();

// source: https://github.com/amio/emoji.json/blob/master/emoji.json
// export const emojis = JSON.parse(fs.readFileSync(new URL('./data/emojiAmio.json', import.meta.url), 'utf8')) as EmojiAmio[];

// source: https://github.com/iamcal/emoji-data/blob/master/emoji.json
export const emojis = JSON.parse(fs.readFileSync(new URL('./data/emoji.json', import.meta.url), 'utf8')) as Emoji[];

const EMOJI_SORTED_SET = 'emojiStats';
const LANGUAGE_SORTED_SET = 'languageStats';
const PROCESSED_POSTS = 'processedPosts';
const POSTS_WITH_EMOJIS = 'postsWithEmojis';
const POSTS_WITHOUT_EMOJIS = 'postsWithoutEmojis';
const PROCESSED_EMOJIS = 'processedEmojis';

export async function handleCreate(event: CommitCreateEvent<'app.bsky.feed.post'>) {
  concurrentHandleCreates.inc();
  try {
    const timer = postProcessingDuration.startTimer();
    try {
      const { commit, did } = event;

      if (!commit.rkey) return;

      const { record, cid, rkey } = commit;

      try {
        let langs = new Set<string>();
        if (record.langs && Array.isArray(record.langs) && record.langs.length > 0) {
          langs = new Set(record.langs);
        } else {
          langs.add('unknown');
        }

        if (langs.size === 0) {
          logger.error('langs.size is 0, this should never happen');
          process.exit(1);
        }

        const emojiMatches = record.text.match(emojiRegex) ?? [];
        const normalizedEmojis = batchNormalizeEmojis(emojiMatches);
        const hasEmojis = normalizedEmojis.length > 0;

        /* step 1: postgres */
        await db.transaction().execute(async (tx) => {
          // add post to db
          const { id } = await tx
            .insertInto('posts')
            .values({
              cid: cid,
              did: did,
              rkey: rkey,
              has_emojis: hasEmojis,
              langs: Array.from(langs),
            })
            .returning('id')
            .executeTakeFirstOrThrow();

          if (hasEmojis) {
            // Prepare bulk insert for emojis
            const emojiInserts: { post_id: number; emoji: string; lang: string }[] = [];

            normalizedEmojis.forEach((emoji) => {
              langs.forEach((lang) => {
                emojiInserts.push({
                  post_id: id,
                  emoji: emoji,
                  lang: lang,
                });
              });
            });

            await tx.insertInto('emojis').values(emojiInserts).execute();
          }
        });

        /* step 2: redis */
        if (!hasEmojis) {
          await redis.incr(POSTS_WITHOUT_EMOJIS);
          totalPostsWithoutEmojis.inc();
        } else {
          await redis.evalSha(SCRIPT_SHA, {
            arguments: [JSON.stringify(normalizedEmojis), JSON.stringify(Array.from(langs))],
          });

          incrementTotalEmojis(normalizedEmojis.length);
          totalPostsWithEmojis.inc();
        }

        /* step 3: global metrics */
        await redis.incr(PROCESSED_POSTS);
        incrementTotalPosts();
      } catch (error) {
        logger.error(`Error processing "create" commit: ${(error as Error).message}`, { commit, record });
        logger.error(`Malformed record data: ${JSON.stringify(record)}`);
      }
    } finally {
      timer();
    }
  } finally {
    concurrentHandleCreates.dec();
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
