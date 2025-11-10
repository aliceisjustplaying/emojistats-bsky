export type NormalizedPost = {
  repoDid: string;
  authorDid: string;
  rkey: string;
  postUri: string;
  seq: number;
  createdAt: Date;
  receivedAt: Date;
  langCodes: string[];
  primaryLang: string;
  clientIdentifier: string | null;
  replyRootUri: string | null;
  replyParentUri: string | null;
  emojiGlyphs: string[];
};
