import { LanguageStat } from './types.js';

export interface EmojiCount {
  emoji: string;
  count: number;
}

/** Payload shape of the 'emojiStats' Socket.IO event. */
export interface EmojiStats {
  processedPosts: number;
  processedEmojis: number;
  postsWithEmojis: number;
  postsWithoutEmojis: number;
  ratio: string;
  topEmojis: EmojiCount[];
}

/**
 * Read-side contract for the stats socket server (src/index.ts). The
 * ClickHouse implementation lives in lib/clickhouse.ts; a future provider is
 * a new file implementing this interface, not a fork of the server.
 */
export interface StatsProvider {
  /** Connectivity check; the server refuses to start if this rejects. */
  ping(): Promise<void>;
  getEmojiStats(): Promise<EmojiStats>;
  getTopLanguages(): Promise<LanguageStat[]>;
  getTopEmojisForLanguage(language: string): Promise<EmojiCount[]>;
  close(): Promise<void>;
}
