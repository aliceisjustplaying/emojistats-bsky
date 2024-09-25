import { CommitCreateEvent } from '@skyware/jetstream';
import emojiRegexFunc from 'emoji-regex';
import fs from 'fs';

import { MAX_EMOJIS, MAX_TOP_LANGUAGES, TRIM_LANGUAGE_CODES } from '../config.js';
import logger from '../logger.js';
import { setLatestCursor } from './cursor.js';
import { SCRIPT_SHA, redisClient } from './redis.js';
import { Emoji, LanguageStat } from './types.js';

const emojiRegex: RegExp = emojiRegexFunc();

// source: https://github.com/amio/emoji.json/blob/master/emoji.json
export const emojis = JSON.parse(fs.readFileSync(new URL('./data/emoji.json', import.meta.url), 'utf8')) as Emoji[];

const EMOJI_SORTED_SET_KEY = 'emojiStats';
const LANGUAGE_SORTED_SET_KEY = 'languageStats';
const PROCESSED_POSTS_KEY = 'processedPosts';
const POSTS_WITH_EMOJIS_KEY = 'postsWithEmojis';
const PROCESSED_EMOJIS_KEY = 'processedEmojis';

export async function handleCreate(event: CommitCreateEvent<'app.bsky.feed.post'>) {
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

    setLatestCursor(event.time_us.toString());
  } catch (error) {
    logger.error(`Error processing "create" commit: ${(error as Error).message}`, { commit, record });
    logger.error(`Malformed record data: ${JSON.stringify(record)}`);
  }
}

export async function getEmojiStats() {
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

export async function getTopLanguages(): Promise<LanguageStat[]> {
  const topLanguagesDesc = await redisClient.zRangeWithScores(LANGUAGE_SORTED_SET_KEY, 0, MAX_TOP_LANGUAGES - 1, {
    REV: true,
  });

  return topLanguagesDesc.map(({ value, score }) => ({
    language: value,
    count: score,
  }));
}

export async function getTopEmojisForLanguage(language: string) {
  const topEmojisDesc = await redisClient.zRangeWithScores(language, 0, MAX_EMOJIS - 1, { REV: true });

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
