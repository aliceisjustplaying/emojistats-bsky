import fs from 'fs';
import { createClient } from 'redis';

import { codePointToEmoji, emojiToCodePoint } from '../lib/helpers.js';
import { Emoji, EmojiVariationSequence } from '../lib/types.js';

const dryRun = false;

// First Pass: Normalization using emojiVariationSequences.json
const emojiVariationSequencesPath = new URL('../lib/data/emojiVariationSequences.json', import.meta.url);
const emojiVariationSequences: EmojiVariationSequence[] = JSON.parse(
  fs.readFileSync(emojiVariationSequencesPath, 'utf8'),
) as EmojiVariationSequence[];

const normalizationMap: Record<string, string> = {};

emojiVariationSequences.forEach((seq) => {
  normalizationMap[seq.code.toLowerCase()] = seq.emojiStyle.toLowerCase();
  normalizationMap[seq.textStyle.toLowerCase()] = seq.emojiStyle.toLowerCase();
});

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
        const originalScore = await redisClient.zScore(setKey, emoji);
        const normalizedScore = await redisClient.zScore(setKey, normalizedEmoji);
        const newScore = (originalScore ?? 0) + (normalizedScore ?? 0);
        const delta = newScore - (originalScore ?? 0);

        console.log(
          `Before - ${setKey}: O: ${emoji} (${emojiCodePoints}) - ${originalScore}, N: ${normalizedEmoji} (${normalizedEmojiCodePoints}) - ${normalizedScore}`,
        );

        if (originalScore !== null) {
          if (!dryRun) {
            await redisClient.zRem(setKey, emoji);
            await redisClient.zAdd(setKey, { score: newScore, value: normalizedEmoji });
          }
          const resultScore = await redisClient.zScore(setKey, normalizedEmoji);
          console.log(
            `1st - ${setKey}: ${emoji} (${emojiCodePoints}) -> ${normalizedEmoji} (${normalizedEmojiCodePoints}), After: ${normalizedEmoji} (${normalizedEmojiCodePoints}) - Score: ${resultScore}, Delta +${delta} -> ${resultScore}`,
          );
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

  // Second Pass: Normalization using emoji.json
  const emojiJsonPath = new URL('../lib/data/emoji.json', import.meta.url);
  const emojiData: Emoji[] = JSON.parse(fs.readFileSync(emojiJsonPath, 'utf8')) as Emoji[];

  // Build a map for non_qualified to unified
  const nonQualifiedMap: Record<string, string> = {};

  emojiData.forEach((emojiEntry) => {
    if (emojiEntry.non_qualified && emojiEntry.unified) {
      nonQualifiedMap[emojiEntry.non_qualified.toLowerCase()] = emojiEntry.unified.toLowerCase();
    }
  });

  async function secondPassNormalizeEmojiSet(setKey: string, emojis: string[]) {
    for (const emoji of emojis) {
      const emojiCodePoints = emojiToCodePoint(emoji).toLowerCase();
      const unifiedCodePoints = nonQualifiedMap[emojiCodePoints];
      if (unifiedCodePoints && unifiedCodePoints !== emojiCodePoints) {
        const unifiedEmoji = codePointToEmoji(unifiedCodePoints);
        const originalScore = await redisClient.zScore(setKey, emoji);
        const unifiedScore = await redisClient.zScore(setKey, unifiedEmoji);
        const newScore = (originalScore ?? 0) + (unifiedScore ?? 0);
        const delta = newScore - (originalScore ?? 0);

        console.log(
          `Before 2nd - ${setKey}: N: ${emoji} (${emojiCodePoints}) - ${originalScore}, U: ${unifiedEmoji} (${unifiedCodePoints}) - ${unifiedScore}`,
        );

        if (originalScore !== null) {
          if (!dryRun) {
            await redisClient.zRem(setKey, emoji);
            await redisClient.zAdd(setKey, { score: newScore, value: unifiedEmoji });
          }
          const resultScore = await redisClient.zScore(setKey, unifiedEmoji);
          console.log(
            `2nd - ${setKey}: ${emoji} (${emojiCodePoints}) -> ${unifiedEmoji} (${unifiedCodePoints}), Delta +${delta} -> ${resultScore}`,
          );
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

normalizeEmojis().catch(console.error);
