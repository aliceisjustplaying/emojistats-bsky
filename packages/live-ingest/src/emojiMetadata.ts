import { EMOJI } from "emoji-normalization/emoji.js";
import { codePointToEmoji } from "emoji-normalization";

export type EmojiMetadata = {
  groupName: string;
  shortcodes: string[];
};

const metadataByGlyph = new Map<string, EmojiMetadata>();

for (const entry of EMOJI) {
  const glyph = safeCodePointToEmoji(entry.unified);
  if (glyph) {
    metadataByGlyph.set(glyph, {
      groupName: entry.category ?? "Unknown",
      shortcodes: buildShortcodes(entry),
    });
  }
  if (entry.non_qualified) {
    const nonQualified = safeCodePointToEmoji(entry.non_qualified);
    if (nonQualified) {
      metadataByGlyph.set(nonQualified, {
        groupName: entry.category ?? "Unknown",
        shortcodes: buildShortcodes(entry),
      });
    }
  }
}

function buildShortcodes(entry: (typeof EMOJI)[number]): string[] {
  const codes =
    entry.short_names ?? (entry.short_name ? [entry.short_name] : []);
  return Array.from(codes);
}

function safeCodePointToEmoji(codePoint?: string | null): string | null {
  if (!codePoint) return null;
  const normalized = codePoint.replaceAll("-", " ");
  try {
    return codePointToEmoji(normalized);
  } catch {
    return null;
  }
}

export function lookupEmojiMetadata(glyph: string): EmojiMetadata {
  return metadataByGlyph.get(glyph) ?? { groupName: "Unknown", shortcodes: [] };
}
