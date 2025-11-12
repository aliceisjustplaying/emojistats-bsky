import { EMOJI } from "./emoji.js";
import { EMOJI_VARIATION_SEQUENCES } from "./emojiVariationSequences.js";
import { codePointToEmoji, emojiToCodePoint } from "./helpers.js";

// Combine both normalization maps into a single map for simpler lookup
const normalizationMap = {
  ...EMOJI_VARIATION_SEQUENCES.reduce<Record<string, string>>(
    (acc, { code, textStyle, emojiStyle }) => ({
      ...acc,
      [code.toLowerCase()]: emojiStyle.toLowerCase(),
      [textStyle.toLowerCase()]: emojiStyle.toLowerCase(),
    }),
    {},
  ),
  ...EMOJI.reduce<Record<string, string>>(
    (acc, { unified, non_qualified }) =>
      non_qualified
        ? {
            ...acc,
            [non_qualified.replaceAll("-", " ").toLowerCase()]: unified
              .replaceAll("-", " ")
              .toLowerCase(),
          }
        : acc,
    {},
  ),
};

export const batchNormalizeEmojis = (emojis: string[]): string[] =>
  emojis.map((emoji) => {
    const codePoints = emojiToCodePoint(emoji);
    const normalized =
      normalizationMap[emoji] || normalizationMap[codePoints] || codePoints;
    return codePointToEmoji(normalized);
  });
