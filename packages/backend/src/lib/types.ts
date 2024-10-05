export interface EmojiAmio {
  codes: string;
  char: string;
  name: string;
  category: string;
  group: string;
  subgroup: string;
}

export interface Emoji {
  name: string;
  unified: string;
  non_qualified?: string;
  docomo?: string;
  au?: string;
  softbank?: string;
  google?: string;
  image: string;
  sheet_x: number;
  sheet_y: number;
  short_name: string;
  short_names: string[];
  text: string | null;
  texts: string[] | null;
  category: string;
  subcategory: string;
  sort_order: number;
  added_in: string;
  has_img_apple: boolean;
  has_img_google: boolean;
  has_img_twitter: boolean;
  has_img_facebook: boolean;
}

export interface EmojiVariationSequence {
  code: string;
  textStyle: string;
  emojiStyle: string;
  version: string;
  name: string;
}

export interface LanguageStat {
  language: string;
  count: number;
}
