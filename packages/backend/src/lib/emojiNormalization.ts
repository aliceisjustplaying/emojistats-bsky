import fs from 'fs';

import { codePointToEmoji, emojiToCodePoint, lowercaseObject } from './helpers.js';
import { Emoji, EmojiVariationSequence } from './types.js';

const eVSPath = new URL('./data/emojiVariationSequences.json', import.meta.url);
const eJSONPath = new URL('./data/emoji.json', import.meta.url);

// Load and parse normalization data
const emojiVariationSequences: EmojiVariationSequence[] = JSON.parse(
  fs.readFileSync(eVSPath, 'utf8'),
) as EmojiVariationSequence[];

const emojiData: Emoji[] = JSON.parse(fs.readFileSync(eJSONPath, 'utf8')) as Emoji[];

// Build normalization maps
let normalizationMap: Record<string, string> = {};
emojiVariationSequences.forEach((seq) => {
  normalizationMap[seq.code] = seq.emojiStyle;
  normalizationMap[seq.textStyle] = seq.emojiStyle;
});

normalizationMap = lowercaseObject(normalizationMap);

let nonQualifiedMap: Record<string, string> = {};
emojiData.forEach((emojiEntry) => {
  if (emojiEntry.non_qualified && emojiEntry.unified) {
    nonQualifiedMap[emojiEntry.non_qualified.replaceAll('-', ' ')] = emojiEntry.unified.replaceAll('-', ' ');
  }
});

nonQualifiedMap = lowercaseObject(nonQualifiedMap);

/**
 * Normalize an emoji using both normalization maps.
 * @param emoji - The original emoji string.
 * @returns The normalized emoji string.
 */
export function normalizeEmoji(emoji: string): string {
  // First Pass: Variation Sequence Normalization
  const emojiCodePoints = emojiToCodePoint(emoji);
  let firstPass;
  if (normalizationMap[emojiCodePoints]) {
    firstPass = normalizationMap[emojiCodePoints];
    console.log(`first pass normalized: ${firstPass} (${emojiCodePoints})`);
  } else {
    firstPass = emojiCodePoints;
  }
  let normalizedEmoji = codePointToEmoji(firstPass);

  // Second Pass: Non-Qualified to Unified Normalization
  const unifiedCodePoints = nonQualifiedMap[firstPass];
  if (unifiedCodePoints && unifiedCodePoints !== firstPass) {
    normalizedEmoji = codePointToEmoji(unifiedCodePoints);
    console.log(`second pass normalized: ${normalizedEmoji} (${unifiedCodePoints})`);
  }

  return normalizedEmoji;
}

/**
 * Batch normalize a list of emojis.
 * @param emojis - Array of emojis to normalize.
 * @returns Array of normalized emojis.
 */
export function batchNormalizeEmojis(emojis: string[]): string[] {
  const normalizationResults: Record<string, string> = {};
  emojis.forEach((emoji) => {
    normalizationResults[emoji] = normalizeEmoji(emoji);
  });
  return Object.values(normalizationResults);
}
