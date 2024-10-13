import fs from 'node:fs';

import { codePointToEmoji, emojiToCodePoint, lowercaseObject } from './helpers.js';
import { Emoji, EmojiVariationSequence } from './types.js';

// Load and parse normalization data
// Converted from: https://unicode.org/Public/emoji/12.1/emoji-variation-sequences.txt
// Regex in Sublime Text form:
// Find: ([0-9A-F]{4,5}) +FE0E +; +.+? style; +\# \((\d.\d)\) ([A-Z0-9\- ]+)\n[0-9A-F]{4,5} +FE0F +; +.+? style; +\# \(\d.\d\) [A-Z0-9\- ]+\n
// Replace: {"code": "$1", "textStyle": "$1 FE0E", "emojiStyle": "$1 FE0F", "version": "$2", "name": "$3"},\n
const eVSPath = new URL('./data/emojiVariationSequences.json', import.meta.url);
const eJSONPath = new URL('./data/emoji.json', import.meta.url);

// Initialize normalization maps as Maps for faster lookups
const normalizationMap = new Map<string, string>();
const nonQualifiedMap = new Map<string, string>();

// Cache for memoization
const normalizationCache = new Map<string, string>();

// Function to load and process normalization data asynchronously
async function initializeNormalizationMaps() {
  const [eVSData, eJSONData] = await Promise.all([
    fs.promises.readFile(eVSPath, 'utf8'),
    fs.promises.readFile(eJSONPath, 'utf8'),
  ]);

  const emojiVariationSequences: EmojiVariationSequence[] = JSON.parse(eVSData) as EmojiVariationSequence[];
  const emojiData: Emoji[] = JSON.parse(eJSONData) as Emoji[];

  for (const seq of emojiVariationSequences) {
    normalizationMap.set(seq.code.toLowerCase(), seq.emojiStyle);
    normalizationMap.set(seq.textStyle.toLowerCase(), seq.emojiStyle);
  }

  const lowercasedNonQualifiedMap = lowercaseObject(Object.fromEntries(normalizationMap));
  normalizationMap.clear();
  for (const [key, value] of Object.entries(lowercasedNonQualifiedMap)) {
    normalizationMap.set(key, value);
  }

  for (const emojiEntry of emojiData) {
    if (emojiEntry.non_qualified && emojiEntry.unified) {
      nonQualifiedMap.set(
        emojiEntry.non_qualified.replaceAll('-', ' ').toLowerCase(),
        emojiEntry.unified.replaceAll('-', ' ').toLowerCase(),
      );
    }
  }

  const lowercasedNonQualified = lowercaseObject(Object.fromEntries(nonQualifiedMap));
  nonQualifiedMap.clear();
  for (const [key, value] of Object.entries(lowercasedNonQualified)) {
    nonQualifiedMap.set(key, value);
  }

  // Freeze the maps to prevent modifications
  Object.freeze(normalizationMap);
  Object.freeze(nonQualifiedMap);
}

// Initialize the maps at startup
initializeNormalizationMaps().catch((error: unknown) => {
  console.error('Failed to initialize normalization maps:', error);
  process.exit(1);
});

export function normalizeEmoji(emoji: string): string {
  if (normalizationCache.has(emoji)) {
    return normalizationCache.get(emoji)!;
  }

  // First Pass: Variation Sequence Normalization
  const emojiCodePoints = emojiToCodePoint(emoji).toLowerCase();
  const firstPass = normalizationMap.get(emojiCodePoints) ?? emojiCodePoints;
  let normalizedEmoji = codePointToEmoji(firstPass);

  // Second Pass: Non-Qualified to Unified Normalization
  const unifiedCodePoints = nonQualifiedMap.get(firstPass);
  if (unifiedCodePoints && unifiedCodePoints !== firstPass) {
    normalizedEmoji = codePointToEmoji(unifiedCodePoints);
  }

  normalizationCache.set(emoji, normalizedEmoji);
  return normalizedEmoji;
}

export function batchNormalizeEmojis(emojis: string[]): string[] {
  const result: string[] = new Array<string>(emojis.length);
  for (let i = 0; i < emojis.length; i++) {
    const emoji = emojis[i];
    if (normalizationCache.has(emoji)) {
      result[i] = normalizationCache.get(emoji)!;
    } else {
      // First Pass: Variation Sequence Normalization
      const emojiCodePoints = emojiToCodePoint(emoji).toLowerCase();
      const firstPass = normalizationMap.get(emojiCodePoints) ?? emojiCodePoints;
      let normalizedEmoji = codePointToEmoji(firstPass);

      // Second Pass: Non-Qualified to Unified Normalization
      const unifiedCodePoints = nonQualifiedMap.get(firstPass);
      if (unifiedCodePoints && unifiedCodePoints !== firstPass) {
        normalizedEmoji = codePointToEmoji(unifiedCodePoints);
      }

      normalizationCache.set(emoji, normalizedEmoji);
      result[i] = normalizedEmoji;
    }
  }
  return result;
}
