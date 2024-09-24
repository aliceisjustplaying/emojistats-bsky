// Language Code to Flag Emoji Lookup Table
const languageFlagMap: Record<string, string> = {
  // A
  aa: '🇪🇷', // Afar - Eritrea
  ab: '🇦🇧', // Abkhazian - Abkhazia (Disputed Region)
  ae: '🇮🇷', // Avestan - Iran (Ancient Language, using Iran flag)
  af: '🇿🇦', // Afrikaans - South Africa
  ak: '🇬🇭', // Akan - Ghana
  am: '🇪🇹', // Amharic - Ethiopia
  an: '🇪🇸', // Aragonese - Spain
  ar: '🇸🇦', // Arabic - Saudi Arabia
  as: '🇮🇳', // Assamese - India
  av: '🇷🇺', // Avaric - Russia
  ay: '🇧🇴', // Aymara - Bolivia
  az: '🇦🇿', // Azerbaijani - Azerbaijan

  // B
  ba: '🇷🇺', // Bashkir - Russia
  be: '🇧🇾', // Belarusian - Belarus
  bg: '🇧🇬', // Bulgarian - Bulgaria
  bh: '🇮🇳', // Bihari languages - India
  bi: '🇻🇺', // Bislama - Vanuatu
  bm: '🇲🇱', // Bambara - Mali
  bn: '🇧🇩', // Bengali - Bangladesh
  bo: '🇨🇳', // Tibetan - China
  br: '🇫🇷', // Breton - France
  bs: '🇧🇦', // Bosnian - Bosnia and Herzegovina

  // C
  ca: '🇪🇸', // Catalan; Valencian - Spain
  ce: '🇷🇺', // Chechen - Russia
  ch: '🇬🇺', // Chamorro - Guam (US Territory)
  co: '🇫🇷', // Corsican - France
  cr: '🇨🇦', // Cree - Canada
  cs: '🇨🇿', // Czech - Czech Republic
  cu: '🇷🇺', // Church Slavic; Old Slavonic - Russia
  cv: '🇷🇺', // Chuvash - Russia
  cy: '🇬🇧', // Welsh - United Kingdom

  // D
  da: '🇩🇰', // Danish - Denmark
  de: '🇩🇪', // German - Germany
  dv: '🇲🇻', // Divehi; Dhivehi; Maldivian - Maldives
  dz: '🇧🇹', // Dzongkha - Bhutan

  // E
  ee: '🇬🇭', // Ewe - Ghana
  el: '🇬🇷', // Greek, Modern - Greece
  en: '🇺🇸', // English - United States
  eo: '🏳️', // Esperanto - International Flag (No specific country)
  es: '🇲🇽', // Spanish; Castilian - Mexico
  et: '🇪🇪', // Estonian - Estonia
  eu: '🏳️', // Basque - European Union Flag or International Flag

  // F
  fa: '🇮🇷', // Persian - Iran
  ff: '🇸🇳', // Fula; Fulah; Pulaar; Pular - Senegal
  fi: '🇫🇮', // Finnish - Finland
  fj: '🇫🇯', // Fijian - Fiji
  fo: '🇫🇴', // Faroese - Faroe Islands
  fr: '🇫🇷', // French - France
  fy: '🇳🇱', // Western Frisian - Netherlands

  // G
  ga: '🇮🇪', // Irish - Ireland
  gd: '🇬🇧', // Gaelic; Scottish Gaelic - United Kingdom
  gl: '🇪🇸', // Galician - Spain
  gn: '🇵🇾', // Guarani - Paraguay
  gu: '🇮🇳', // Gujarati - India
  gv: '🇬🇧', // Manx - United Kingdom

  // H
  ha: '🇳🇬', // Hausa - Nigeria
  he: '🇮🇱', // Hebrew - Israel
  hi: '🇮🇳', // Hindi - India
  ho: '🇵🇬', // Hiri Motu - Papua New Guinea
  hr: '🇭🇷', // Croatian - Croatia
  ht: '🇭🇹', // Haitian; Haitian Creole - Haiti
  hu: '🇭🇺', // Hungarian - Hungary
  hy: '🇦🇲', // Armenian - Armenia

  // I
  ia: '🏳️', // Interlingua - International Flag
  id: '🇮🇩', // Indonesian - Indonesia
  ie: '🏳️', // Interlingue; Occidental - International Flag
  ig: '🇳🇬', // Igbo - Nigeria
  ii: '🇨🇳', // Sichuan Yi; Nuosu - China
  ik: '🇺🇸', // Inupiaq - United States
  io: '🏳️', // Ido - International Flag
  is: '🇮🇸', // Icelandic - Iceland
  it: '🇮🇹', // Italian - Italy

  // J
  iu: '🇨🇦', // Inuktitut - Canada
  ja: '🇯🇵', // Japanese - Japan
  jv: '🇮🇩', // Javanese - Indonesia

  // K
  ka: '🇬🇪', // Georgian - Georgia
  kg: '🇨🇩', // Kongo - Democratic Republic of the Congo
  ki: '🇰🇪', // Kikuyu; Gikuyu - Kenya
  kj: '🇳🇦', // Kuanyama; Kwanyama - Namibia
  kk: '🇰🇿', // Kazakh - Kazakhstan
  kl: '🇬🇱', // Kalaallisut; Greenlandic - Greenland
  km: '🇰🇭', // Central Khmer - Cambodia
  kn: '🇮🇳', // Kannada - India
  ko: '🇰🇷', // Korean - South Korea
  kr: '🇳🇬', // Kanuri - Nigeria
  ks: '🇮🇳', // Kashmiri - India
  ku: '🇹🇷', // Kurdish - Turkey
  kv: '🇷🇺', // Komi - Russia
  kw: '🇬🇧', // Cornish - United Kingdom
  ky: '🇰🇬', // Kirghiz; Kyrgyz - Kyrgyzstan

  // L
  la: '🏛️', // Latin - Historical Language (No specific flag)
  lb: '🇱🇺', // Luxembourgish; Letzeburgesch - Luxembourg
  lg: '🇺🇬', // Ganda - Uganda
  li: '🇳🇱', // Limburgan; Limburger; Limburgish - Netherlands
  ln: '🇨🇩', // Lingala - Democratic Republic of the Congo
  lo: '🇱🇦', // Lao - Laos
  lt: '🇱🇹', // Lithuanian - Lithuania
  lu: '🇨🇩', // Luba-Katanga - Democratic Republic of the Congo
  lv: '🇱🇻', // Latvian - Latvia

  // M
  mg: '🇲🇱', // Malagasy - Madagascar
  mh: '🇲🇭', // Marshallese - Marshall Islands
  mi: '🇳🇿', // Maori - New Zealand
  mk: '🇲🇰', // Macedonian - North Macedonia
  ml: '🇮🇳', // Malayalam - India
  mn: '🇲🇳', // Mongolian - Mongolia
  mr: '🇮🇳', // Marathi - India
  ms: '🇲🇾', // Malay - Malaysia
  mt: '🇲🇹', // Maltese - Malta
  my: '🇲🇲', // Burmese - Myanmar

  // N
  na: '🇳🇷', // Nauru - Nauru
  nb: '🇳🇴', // Norwegian Bokmål - Norway
  nd: '🇿🇼', // North Ndebele - Zimbabwe
  ne: '🇳🇵', // Nepali - Nepal
  ng: '🇳🇬', // Ndonga - Nigeria
  nl: '🇳🇱', // Dutch; Flemish - Netherlands
  nn: '🇳🇴', // Norwegian Nynorsk - Norway
  no: '🇳🇴', // Norwegian - Norway
  nr: '🇿🇼', // South Ndebele - Zimbabwe
  nv: '🇺🇸', // Navajo; Navaho - United States
  ny: '🇲🇼', // Chichewa; Chewa; Nyanja - Malawi

  // O
  oc: '🇫🇷', // Occitan (post 1500) - France
  oj: '🇨🇦', // Ojibwa - Canada
  om: '🇪🇹', // Oromo - Ethiopia
  or: '🇮🇳', // Oriya - India
  os: '🇷🇺', // Ossetian; Ossetic - Russia

  // P
  pa: '🇮🇳', // Panjabi; Punjabi - India
  pi: '🏳️', // Pali - International Flag
  pl: '🇵🇱', // Polish - Poland
  ps: '🇦🇫', // Pushto; Pashto - Afghanistan
  pt: '🇧🇷', // Portuguese - Brazil
  qu: '🇵🇪', // Quechua - Peru
  rm: '🇨🇭', // Romansh - Switzerland
  rn: '🇷🇼', // Rundi - Rwanda
  ro: '🇷🇴', // Romanian; Moldavian; Moldovan - Romania
  ru: '🇷🇺', // Russian - Russia
  rw: '🇷🇼', // Kinyarwanda - Rwanda

  // S
  sa: '🏛️', // Sanskrit - Historical Language (No specific flag)
  sc: '🇮🇹', // Sardinian - Italy
  sd: '🇵🇰', // Sindhi - Pakistan
  se: '🇳🇴', // Northern Sami - Norway
  sg: '🇨🇫', // Sango - Central African Republic
  si: '🇱🇰', // Sinhala; Sinhalese - Sri Lanka
  sk: '🇸🇰', // Slovak - Slovakia
  sl: '🇸🇮', // Slovenian - Slovenia
  sm: '🇼🇸', // Samoan - Samoa
  sn: '🇿🇼', // Shona - Zimbabwe
  so: '🇸🇴', // Somali - Somalia
  sq: '🇦🇱', // Albanian - Albania
  sr: '🇷🇸', // Serbian - Serbia
  ss: '🇸🇿', // Swati - Eswatini
  st: '🇱🇸', // Southern Sotho - Lesotho
  su: '🇸🇩', // Sundanese - Indonesia
  sv: '🇸🇪', // Swedish - Sweden
  sw: '🇰🇪', // Swahili - Kenya

  // T
  ta: '🇮🇳', // Tamil - India
  te: '🇮🇳', // Telugu - India
  tg: '🇹🇯', // Tajik - Tajikistan
  th: '🇹🇭', // Thai - Thailand
  ti: '🇪🇷', // Tigrinya - Eritrea
  tk: '🇹🇲', // Turkmen - Turkmenistan
  tl: '🇵🇭', // Tagalog - Philippines
  tn: '🇱🇸', // Tswana - Lesotho
  to: '🇹🇴', // Tonga (Tonga Islands) - Tonga
  tr: '🇹🇷', // Turkish - Turkey
  ts: '🇿🇦', // Tsonga - South Africa
  tt: '🇷🇺', // Tatar - Russia
  tw: '🇬🇭', // Twi - Ghana
  ty: '🇹🇫', // Tahitian - French Polynesia

  // U
  ug: '🇨🇳', // Uighur; Uyghur - China
  uk: '🇺🇦', // Ukrainian - Ukraine
  ur: '🇵🇰', // Urdu - Pakistan
  uz: '🇺🇿', // Uzbek - Uzbekistan

  // V
  ve: '🇿🇼', // Venda - Zimbabwe
  vi: '🇻🇳', // Vietnamese - Vietnam
  vo: '🏳️', // Volapük - International Flag

  // W
  wa: '🇧🇪', // Walloon - Belgium
  wo: '🇸🇳', // Wolof - Senegal

  // X
  xh: '🇿🇦', // Xhosa - South Africa

  // Y
  yi: '🇩🇪', // Yiddish - Germany
  yo: '🇳🇬', // Yoruba - Nigeria
  za: '🇨🇳', // Zhuang; Chuang - China
  zh: '🇨🇳', // Chinese - China
  zu: '🇿🇦', // Zulu - South Africa
};

// Utility function to retrieve the flag based on the language code
export function getFlagByLanguageCode(langCode: string): string {
  return languageFlagMap[langCode.toLowerCase()] || '🏳️'; // Returns a white flag if not found
}
