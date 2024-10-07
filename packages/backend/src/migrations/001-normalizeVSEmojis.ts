import fs from 'fs';
import { createClient } from 'redis';

import { codePointToEmoji, emojiToCodePoint } from '../lib/helpers.js';
import { EmojiVariationSequence } from '../lib/types.js';

const dryRun = false;

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

        console.log(`Before normalization - ${setKey}:`);
        console.log(`  Original: ${emoji} (${emojiCodePoints}) - Score: ${originalScore}`);
        console.log(`  Normalized: ${normalizedEmoji} (${normalizedEmojiCodePoints}) - Score: ${normalizedScore}`);

        if (originalScore !== null) {
          if (!dryRun) {
            await redisClient.zRem(setKey, emoji);
            await redisClient.zAdd(setKey, { score: newScore, value: normalizedEmoji });
          }
          const resultScore = await redisClient.zScore(setKey, normalizedEmoji);
          console.log(
            `Normalized emoji in ${setKey}: ${emoji} (${emojiCodePoints}) -> ${normalizedEmoji} (${normalizedEmojiCodePoints})`,
          );
          console.log(
            `After normalization - ${setKey}: ${normalizedEmoji} (${normalizedEmojiCodePoints}) - Score: ${resultScore}`,
          );
          console.log(`Moved score ${originalScore} -> ${newScore} = ${resultScore}`);
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

  await redisClient.disconnect();
  console.log('Emoji normalization completed.');
}

function getNormalizedEmoji(emoji: string) {
  const emojiCodePoints = emojiToCodePoint(emoji);
  const normalizedEmojiCodePoints = normalizationMap[emojiCodePoints] || emojiCodePoints;
  const normalizedEmoji = codePointToEmoji(normalizedEmojiCodePoints);
  return { emojiCodePoints, normalizedEmojiCodePoints, normalizedEmoji };
}

normalizeEmojis().catch(console.error);
