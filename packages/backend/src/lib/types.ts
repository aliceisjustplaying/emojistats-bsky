import { EMOJI } from './data/emoji.js';
import { EMOJI_VARIATION_SEQUENCES } from './data/emojiVariationSequences.js';

export interface EmojiAmio {
  codes: string;
  char: string;
  name: string;
  category: string;
  group: string;
  subgroup: string;
}

export type Emoji = (typeof EMOJI)[number];
export type EmojiVariationSequence = (typeof EMOJI_VARIATION_SEQUENCES)[number];

export interface LanguageStat {
  language: string;
  count: number;
}
