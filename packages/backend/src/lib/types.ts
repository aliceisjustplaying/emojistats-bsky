export interface EmojiAmio {
  codes: string;
  char: string;
  name: string;
  category: string;
  group: string;
  subgroup: string;
}

export interface LanguageStat {
  language: string;
  count: number;
}

// export interface Post {
//   did: string;
//   rkey: string;
//   text: string | null;
//   emojis: string[];
//   langs: string[];
//   created_at: string | Date | undefined;
// }

// export interface Emoji {
//   did: string;
//   rkey: string;
//   emoji: string;
//   lang: string;
//   created_at: string | Date | undefined;
// }
