import { EMOJI } from "./emoji.js";
import { EMOJI_VARIATION_SEQUENCES } from "./emojiVariationSequences.js";

export type Emoji = (typeof EMOJI)[number];
export type EmojiVariationSequence = (typeof EMOJI_VARIATION_SEQUENCES)[number];
