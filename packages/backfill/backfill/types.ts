export type NormalizedEmojiPost = {
  repoDid: string;
  authorDid: string;
  collection: string;
  rkey: string;
  cid: string;
  postUri: string;
  seq: number;
  createdAt: Date;
  receivedAt: Date;
  langCodes: string[];
  primaryLang: string;
  clientIdentifier: string | null;
  replyRootUri: string | null;
  replyParentUri: string | null;
  text: string;
  emojiGlyphs: string[];
};

export type PreparedEmojiRow = {
  postUri: string;
  repoDid: string;
  rkey: string;
  seq: number;
  createdAt: Date;
  receivedAt: Date;
  langId: number;
  clientId: number | null;
  emojiIds: number[];
  authorDid: string;
  replyRootUri: string | null;
  replyParentUri: string | null;
};

export type RepoDescriptor = {
  did: string;
  pds: string;
};
