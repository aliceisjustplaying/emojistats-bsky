import fs from 'fs';
import { createClient } from 'redis';

import { codePointToEmoji, emojiToCodePoint, lowercaseObject } from '../lib/helpers.js';
import { Emoji, EmojiVariationSequence } from '../lib/types.js';

const dryRun = false;

// First Pass: Normalization using emojiVariationSequences.json
const eVSPath = new URL('../lib/data/emojiVariationSequences.json', import.meta.url);
const eVS: EmojiVariationSequence[] = JSON.parse(fs.readFileSync(eVSPath, 'utf8')) as EmojiVariationSequence[];

let normalizationMap: Record<string, string> = {};

eVS.forEach((seq) => {
  normalizationMap[seq.code] = seq.emojiStyle;
  normalizationMap[seq.textStyle] = seq.emojiStyle;
});

// --------------------------------------------------

// Second Pass: Normalization using emoji.json
const eJSONPath = new URL('../lib/data/emoji.json', import.meta.url);
const emojiData: Emoji[] = JSON.parse(fs.readFileSync(eJSONPath, 'utf8')) as Emoji[];

normalizationMap = lowercaseObject(normalizationMap);

let nonQualifiedMap: Record<string, string> = {};

emojiData.forEach((emojiEntry) => {
  if (emojiEntry.non_qualified && emojiEntry.unified) {
    nonQualifiedMap[emojiEntry.non_qualified.replaceAll('-', ' ')] = emojiEntry.unified.replaceAll('-', ' ');
  }
});

nonQualifiedMap = lowercaseObject(nonQualifiedMap);

console.dir(nonQualifiedMap, { depth: null });

async function normalizeEmojis() {
  const redisClient = createClient();
  redisClient.on('error', (err) => {
    console.error('Redis Client Error', err);
  });

  await redisClient.connect();

  async function normalizeEmojiSet(setKey: string, emojis: string[]) {
    for (const emoji of emojis) {
      const { emojiCodePoints, normalizedEmojiCodePoints, normalizedEmoji } = getNormalizedEmoji(emoji);
      if (normalizedEmojiCodePoints !== emojiCodePoints) {
        const originalScore = (await redisClient.zScore(setKey, emoji)) ?? 0;
        const normalizedScore = (await redisClient.zScore(setKey, normalizedEmoji)) ?? 0;
        const newScore = originalScore + normalizedScore;

        console.log(
          `Before: ${setKey}: O: ${emoji} (${emojiCodePoints}) - ${originalScore} | N: ${normalizedEmoji} (${normalizedEmojiCodePoints}) - ${normalizedScore}`,
        );

        if (originalScore !== 0) {
          // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
          if (!dryRun) {
            await redisClient.zRem(setKey, emoji);
            await redisClient.zAdd(setKey, { score: newScore, value: normalizedEmoji });
          }
          const resultScore = await redisClient.zScore(setKey, normalizedEmoji);
          console.log(`After: ${setKey}: N: ${normalizedEmoji} (${normalizedEmojiCodePoints}) - Score: ${resultScore}`);
        }
      }
    }
  }

  // Normalize global emojis in 'emojiStats'
  const globalEmojis = await redisClient.zRange('emojiStats', 0, -1);
  await normalizeEmojiSet('emojiStats', globalEmojis);

  // Get all language keys from 'languageStats'
  const languageStats = await redisClient.zRangeWithScores('languageStats', 0, -1);
  const languageKeys = languageStats.map((entry) => entry.value);

  for (const lang of languageKeys) {
    const emojis = await redisClient.zRange(lang, 0, -1);
    await normalizeEmojiSet(lang, emojis);
  }

  // Divider for Second Pass
  console.log('--- Starting Second Pass: Normalization Using emoji.json ---');

  async function secondPassNormalizeEmojiSet(setKey: string, emojis: string[]) {
    for (const emoji of emojis) {
      const emojiCodePoints = emojiToCodePoint(emoji);
      // console.log(emojiCodePoints);
      const unifiedCodePoints = nonQualifiedMap[emojiCodePoints];
      if (unifiedCodePoints && unifiedCodePoints !== emojiCodePoints) {
        const unifiedEmoji = codePointToEmoji(unifiedCodePoints);
        const originalScore = (await redisClient.zScore(setKey, emoji)) ?? 0;
        const unifiedScore = (await redisClient.zScore(setKey, unifiedEmoji)) ?? 0;
        const newScore = originalScore + unifiedScore;

        console.log(
          `Before: ${setKey}: N: ${emoji} (${emojiCodePoints}) - ${originalScore} | U: ${unifiedEmoji} (${unifiedCodePoints}) - ${unifiedScore}`,
        );

        if (originalScore !== 0) {
          // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
          if (!dryRun) {
            await redisClient.zRem(setKey, emoji);
            await redisClient.zAdd(setKey, { score: newScore, value: unifiedEmoji });
          }
          const resultScore = await redisClient.zScore(setKey, unifiedEmoji);
          console.log(`After: ${setKey}: ${unifiedEmoji} (${unifiedCodePoints}), Score: ${resultScore}`);
        }
      }
    }
  }

  // Apply second pass normalization to 'emojiStats'
  const globalEmojisSecondPass = await redisClient.zRange('emojiStats', 0, -1);
  await secondPassNormalizeEmojiSet('emojiStats', globalEmojisSecondPass);

  // Apply second pass normalization to each language key
  for (const lang of languageKeys) {
    const emojis = await redisClient.zRange(lang, 0, -1);
    await secondPassNormalizeEmojiSet(lang, emojis);
  }

  await redisClient.disconnect();
  console.log('Emoji normalization completed.');
}

function getNormalizedEmoji(emoji: string) {
  const emojiCodePoints = emojiToCodePoint(emoji);
  const normalizedEmojiCodePoints = normalizationMap[emojiCodePoints.toLowerCase()] || emojiCodePoints;
  const normalizedEmoji = codePointToEmoji(normalizedEmojiCodePoints);
  return { emojiCodePoints, normalizedEmojiCodePoints, normalizedEmoji };
}

normalizeEmojis().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
