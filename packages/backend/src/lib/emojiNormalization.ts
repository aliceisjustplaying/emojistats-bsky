import fs from 'fs';

import { codePointToEmoji, emojiToCodePoint, lowercaseObject } from './helpers.js';
import logger from './logger.js';
import { Emoji, EmojiVariationSequence } from './types.js';

// Load and parse normalization data
// converted from: https://unicode.org/Public/emoji/12.1/emoji-variation-sequences.txt
// regex in Sublime Text form:
// find: ([0-9A-F]{4,5}) +FE0E +; +.+? style; +\# \((\d.\d)\) ([A-Z0-9\- ]+)\n[0-9A-F]{4,5} +FE0F +; +.+? style; +\# \(\d.\d\) [A-Z0-9\- ]+\n
// replace: {"code": "$1", "textStyle": "$1 FE0E", "emojiStyle": "$1 FE0F", "version": "$2", "name": "$3"},\n
const eVSPath = new URL('./data/emojiVariationSequences.json', import.meta.url);

// source: https://github.com/iamcal/emoji-data/blob/master/emoji.json
const eJSONPath = new URL('./data/emoji.json', import.meta.url);

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

export function normalizeEmoji(emoji: string): string {
  // First Pass: Variation Sequence Normalization
  const emojiCodePoints = emojiToCodePoint(emoji);
  let firstPass;
  if (normalizationMap[emojiCodePoints]) {
    firstPass = normalizationMap[emojiCodePoints];
  } else {
    firstPass = emojiCodePoints;
  }
  let normalizedEmoji = codePointToEmoji(firstPass);

  // Second Pass: Non-Qualified to Unified Normalization
  const unifiedCodePoints = nonQualifiedMap[firstPass];
  if (unifiedCodePoints && unifiedCodePoints !== firstPass) {
    normalizedEmoji = codePointToEmoji(unifiedCodePoints);
  }

  return normalizedEmoji;
}

export function batchNormalizeEmojis(emojis: string[]): string[] {
  return emojis.map((emoji) => normalizeEmoji(emoji));
}
