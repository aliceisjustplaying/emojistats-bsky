import { createClient } from "redis";
import type { NormalizedPost } from "./types.js";
import { logger } from "./logger.js";

export type RedisClient = ReturnType<typeof createClient>;

export async function createRedisClient(url: string): Promise<RedisClient> {
  const client = createClient({ url });
  client.on("error", (err) => logger.error({ err }, "Redis error"));
  await client.connect();
  return client;
}

export class RedisEmojiStore {
  constructor(
    private readonly client: RedisClient,
    private readonly prefix: string,
  ) {}

  async increment(post: NormalizedPost) {
    if (post.emojiGlyphs.length === 0) return;
    const multi = this.client.multi();
    for (const glyph of post.emojiGlyphs) {
      multi.hIncrBy(this.globalTotalsKey(), glyph, 1);
      multi.zIncrBy(this.globalSortedKey(), 1, glyph);
      for (const lang of post.langCodes) {
        multi.hIncrBy(this.langTotalsKey(lang), glyph, 1);
        multi.zIncrBy(this.langSortedKey(lang), 1, glyph);
      }
    }
    multi.hIncrBy(this.langVolumeKey(), post.primaryLang, 1);
    await multi.exec();
  }

  async seedGlobal(records: Array<{ glyph: string; posts: number }>) {
    const totalsKey = this.globalTotalsKey();
    const sortedKey = this.globalSortedKey();
    await this.client.del([totalsKey, sortedKey]);
    if (records.length === 0) return;
    const hash = Object.fromEntries(
      records.map((r) => [r.glyph, r.posts.toString()]),
    ) as Record<string, string>;
    await this.client.hSet(totalsKey, hash);
    await this.client.zAdd(
      sortedKey,
      records.map((r) => ({ score: r.posts, value: r.glyph })),
    );
  }

  async seedByLanguage(
    records: Array<{ glyph: string; lang: string; posts: number }>,
  ) {
    const langGroups = new Map<
      string,
      Array<{ glyph: string; posts: number }>
    >();
    for (const record of records) {
      const list = langGroups.get(record.lang) ?? [];
      list.push({ glyph: record.glyph, posts: record.posts });
      langGroups.set(record.lang, list);
    }
    const tasks: Array<Promise<unknown>> = [];
    for (const [lang, entries] of langGroups) {
      const totalsKey = this.langTotalsKey(lang);
      const sortedKey = this.langSortedKey(lang);
      tasks.push(this.client.del([totalsKey, sortedKey]));
      const hash = Object.fromEntries(
        entries.map((entry) => [entry.glyph, entry.posts.toString()]),
      ) as Record<string, string>;
      tasks.push(this.client.hSet(totalsKey, hash));
      tasks.push(
        this.client.zAdd(
          sortedKey,
          entries.map((entry) => ({ score: entry.posts, value: entry.glyph })),
        ),
      );
    }
    await Promise.all(tasks);
  }

  private globalTotalsKey() {
    return `${this.prefix}:global:totals`;
  }
  private globalSortedKey() {
    return `${this.prefix}:global:sorted`;
  }
  private langTotalsKey(lang: string) {
    return `${this.prefix}:lang:${lang}:totals`;
  }
  private langSortedKey(lang: string) {
    return `${this.prefix}:lang:${lang}:sorted`;
  }
  private langVolumeKey() {
    return `${this.prefix}:lang:volume`;
  }
}
