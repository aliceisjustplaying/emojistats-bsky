import { EMOJI } from "./emoji.js";
import { EMOJI_VARIATION_SEQUENCES } from "./emojiVariationSequences.js";
import { codePointToEmoji, emojiToCodePoint } from "./helpers.js";

const normalizationMap = EMOJI_VARIATION_SEQUENCES.reduce<
  Record<string, string>
>((acc, val) => {
  const code = val.code.toLowerCase();
  const textStyle = val.textStyle.toLowerCase();
  const emojiStyle = val.emojiStyle.toLowerCase();

  return {
    ...acc,
    [code]: emojiStyle,
    [textStyle]: emojiStyle,
  };
}, {});

const nonQualifiedMap = EMOJI.reduce<Record<string, string>>((acc, val) => {
  const unified = val.unified.replaceAll("-", " ").toLowerCase();
  const nonQualified = val.non_qualified?.replaceAll("-", " ").toLowerCase();

  if (nonQualified !== undefined) {
    acc[nonQualified] = unified;
  }

  return acc;
}, {});

export const batchNormalizeEmojis = (emojis: string[]): string[] => {
  const normalizedEmojis: string[] = [];

  for (const emoji of emojis) {
    if (emoji in normalizationMap) {
      normalizedEmojis.push(normalizationMap[emoji]);
      continue;
    }

    // First pass: variation sequence normalization
    const codePoints = emojiToCodePoint(emoji);
    let firstPass = codePoints;

    if (codePoints in normalizationMap) {
      firstPass = normalizationMap[codePoints];
    }

    // Second pass: non-qualified to unified normalization
    if (firstPass in nonQualifiedMap) {
      const unifiedCodePoints = nonQualifiedMap[firstPass];
      normalizedEmojis.push(codePointToEmoji(unifiedCodePoints));
      continue;
    }

    normalizedEmojis.push(codePointToEmoji(firstPass));
  }

  return normalizedEmojis;
};
