import { CommitCreateEvent } from '@skyware/jetstream';
import emojiRegexFunc from 'emoji-regex';
import fs from 'fs';

import { MAX_EMOJIS, MAX_TOP_LANGUAGES, TRIM_LANGUAGE_CODES } from '../config.js';
import { setLatestCursor } from './cursor.js';
import { batchNormalizeEmojis } from './emojiNormalization.js';
import logger from './logger.js';
import {
  incrementTotalEmojis,
  incrementTotalPosts,
  postProcessingDuration,
  totalPostsWithEmojis,
  totalPostsWithoutEmojis,
} from './metrics.js';
import { SCRIPT_SHA, redis } from './redis.js';
import { Emoji, EmojiVariationSequence, LanguageStat } from './types.js';

const emojiRegex: RegExp = emojiRegexFunc();

// source: https://github.com/amio/emoji.json/blob/master/emoji.json
// export const emojis = JSON.parse(fs.readFileSync(new URL('./data/emojiAmio.json', import.meta.url), 'utf8')) as EmojiAmio[];

// source: https://github.com/iamcal/emoji-data/blob/master/emoji.json
export const emojis = JSON.parse(fs.readFileSync(new URL('./data/emoji.json', import.meta.url), 'utf8')) as Emoji[];

// converted from: https://unicode.org/Public/emoji/12.1/emoji-variation-sequences.txt
// regex in Sublime Text form:
// find: ([0-9A-F]{4,5}) +FE0E +; +.+? style; +\# \((\d.\d)\) ([A-Z0-9\- ]+)\n[0-9A-F]{4,5} +FE0F +; +.+? style; +\# \(\d.\d\) [A-Z0-9\- ]+\n
// replace: {"code": "$1", "textStyle": "$1 FE0E", "emojiStyle": "$1 FE0F", "version": "$2", "name": "$3"},\n
export const emojiVariationSequences = JSON.parse(
  fs.readFileSync(new URL('./data/emojiVariationSequences.json', import.meta.url), 'utf8'),
) as EmojiVariationSequence[];

const EMOJI_SORTED_SET = 'emojiStats';
const LANGUAGE_SORTED_SET = 'languageStats';
const PROCESSED_POSTS = 'processedPosts';
const POSTS_WITH_EMOJIS = 'postsWithEmojis';
const POSTS_WITHOUT_EMOJIS = 'postsWithoutEmojis';
const PROCESSED_EMOJIS = 'processedEmojis';

export async function handleCreate(event: CommitCreateEvent<'app.bsky.feed.post'>) {
  const timer = postProcessingDuration.startTimer();
  try {
    const { commit } = event;

    if (!commit.rkey) return;

    const { record } = commit;

    try {
      let langs = new Set<string>();
      if (record.langs && Array.isArray(record.langs)) {
        langs = new Set(
          record.langs.map((lang: string) =>
            // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
            TRIM_LANGUAGE_CODES ? lang.split('-')[0].toLowerCase().slice(0, 2) : lang,
          ),
        );
      } else {
        langs.add('UNKNOWN');
      }

      const emojiMatches = record.text.match(emojiRegex) ?? [];

      if (emojiMatches.length > 0) {
        const stringifiedLangs = JSON.stringify(Array.from(langs));

        const normalizedEmojis = batchNormalizeEmojis(emojiMatches);

        const emojiPromises = normalizedEmojis.map((emoji, i) => {
          const isFirstEmoji = i === 0 ? '1' : '0';
          return redis.evalSha(SCRIPT_SHA, {
            arguments: [emoji, stringifiedLangs, isFirstEmoji],
          });
        });

        await Promise.all([...emojiPromises]);
        logger.debug(`Emojis updated for languages: ${Array.from(langs).join(', ')}`);
        incrementTotalEmojis(emojiMatches.length);
      } else {
        await redis.incr(POSTS_WITHOUT_EMOJIS);
      }

      await redis.incr(PROCESSED_POSTS);
      setLatestCursor(event.time_us.toString());
      incrementTotalPosts();
      if (emojiMatches.length > 0) {
        totalPostsWithEmojis.inc();
      } else {
        totalPostsWithoutEmojis.inc();
      }
    } catch (error) {
      logger.error(`Error processing "create" commit: ${(error as Error).message}`, { commit, record });
      logger.error(`Malformed record data: ${JSON.stringify(record)}`);
    }
  } finally {
    timer();
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
