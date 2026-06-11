import { ClickHouseClient, createClient } from '@clickhouse/client';

import {
  CLICKHOUSE_DATABASE,
  CLICKHOUSE_PASSWORD,
  CLICKHOUSE_URL,
  CLICKHOUSE_USER,
  MAX_EMOJIS,
  MAX_TOP_LANGUAGES,
} from '../config.js';

import { EmojiCount, EmojiStats, StatsProvider } from './stats.js';
import { LanguageStat } from './types.js';

/*
 * ClickHouse-backed StatsProvider. All aggregate tables are
 * SummingMergeTree caches maintained by the ingest worker's materialized
 * views; merges settle lazily, so every read MUST sum() + GROUP BY instead
 * of trusting raw rows (see packages/ingest/src/clickhouse/schema.sql).
 *
 * Counting semantics carry over from the retired Redis path: emoji and
 * language counts are per-occurrence (repeats within a post count).
 */

export function createClickHouseClient(): ClickHouseClient {
  return createClient({
    url: CLICKHOUSE_URL,
    username: CLICKHOUSE_USER,
    password: CLICKHOUSE_PASSWORD,
    database: CLICKHOUSE_DATABASE,
    application: 'emojistats-api',
    request_timeout: 30_000,
  });
}

export const clickhouse = createClickHouseClient();

export async function pingClickHouse(): Promise<void> {
  try {
    const result = await clickhouse.query({
      query: 'SELECT 1',
      format: 'JSONEachRow',
    });
    await result.json();
  } catch (err) {
    throw new Error(
      `ClickHouse unreachable at ${CLICKHOUSE_URL} (database "${CLICKHOUSE_DATABASE}"). Is the ingest stack up?`,
      { cause: err },
    );
  }
}

export interface GlobalCounters {
  processedPosts: number;
  processedEmojis: number;
  postsWithEmojis: number;
  postsWithoutEmojis: number;
}

/* ClickHouse quotes UInt64 values as strings in JSON output
   (output_format_json_quote_64bit_integers), so rows come back with string
   counts and we convert with Number() — counts stay far below 2^53. */

async function queryRows<T>(
  query: string,
  query_params?: Record<string, unknown>,
): Promise<T[]> {
  const result = await clickhouse.query({
    query,
    query_params,
    format: 'JSONEachRow',
  });
  return result.json<T>();
}

/**
 * Top emojis across all posts, by summed occurrences. Mirrors the Redis
 * 'emojiStats' sorted set (ZINCRBY 1 per occurrence).
 */
export async function fetchTopEmojis(): Promise<EmojiCount[]> {
  const rows = await queryRows<{ emoji: string; count: string }>(
    `SELECT emoji, sum(occurrences) AS count
     FROM emoji_total
     GROUP BY emoji
     ORDER BY count DESC
     LIMIT {limit: UInt32}`,
    { limit: MAX_EMOJIS },
  );
  return rows.map(({ emoji, count }) => ({ emoji, count: Number(count) }));
}

/**
 * Global counters derived from posts_hourly sums. Mirrors the Redis keys
 * processedPosts / postsWithEmojis / postsWithoutEmojis / processedEmojis.
 */
export async function fetchGlobalCounters(): Promise<GlobalCounters> {
  const rows = await queryRows<{
    processedPosts: string;
    processedEmojis: string;
    postsWithEmojis: string;
    postsWithoutEmojis: string;
  }>(
    `SELECT
       sum(posts) AS processedPosts,
       sum(emoji_occurrences) AS processedEmojis,
       sum(posts_with_emojis) AS postsWithEmojis,
       sum(posts) - sum(posts_with_emojis) AS postsWithoutEmojis
     FROM posts_hourly`,
  );
  const row = rows[0];
  return {
    processedPosts: Number(row?.processedPosts) || 0,
    processedEmojis: Number(row?.processedEmojis) || 0,
    postsWithEmojis: Number(row?.postsWithEmojis) || 0,
    postsWithoutEmojis: Number(row?.postsWithoutEmojis) || 0,
  };
}

/**
 * Top languages by summed emoji occurrences. Mirrors the Redis
 * 'languageStats' sorted set, which is incremented once per emoji occurrence
 * per language (not once per post).
 */
export async function fetchTopLanguages(): Promise<LanguageStat[]> {
  const rows = await queryRows<{ language: string; count: string }>(
    `SELECT lang AS language, sum(occurrences) AS count
     FROM lang_total
     GROUP BY lang
     ORDER BY count DESC
     LIMIT {limit: UInt32}`,
    { limit: MAX_TOP_LANGUAGES },
  );
  return rows.map(({ language, count }) => ({
    language,
    count: Number(count),
  }));
}

/**
 * Top emojis for one language, by summed occurrences. Mirrors the Redis
 * per-language sorted sets (key = lang code). The language is always passed
 * as a bound query parameter, never interpolated.
 */
export async function fetchTopEmojisForLanguage(
  language: string,
): Promise<EmojiCount[]> {
  const rows = await queryRows<{ emoji: string; count: string }>(
    `SELECT emoji, sum(occurrences) AS count
     FROM emoji_total_by_lang
     WHERE lang = {lang: String}
     GROUP BY emoji
     ORDER BY count DESC
     LIMIT {limit: UInt32}`,
    { lang: language, limit: MAX_EMOJIS },
  );
  return rows.map(({ emoji, count }) => ({ emoji, count: Number(count) }));
}

/* 1s TTL cache: one entry per query key ('global', 'langs', 'lang:<code>').
   The pending promise is cached so concurrent sockets share one query;
   failures are evicted immediately so the next tick retries. */
const CACHE_TTL_MS = 1000;
const cache = new Map<string, { at: number; value: Promise<unknown> }>();

function cached<T>(key: string, fetcher: () => Promise<T>): Promise<T> {
  const hit = cache.get(key);
  if (hit && Date.now() - hit.at < CACHE_TTL_MS) {
    return hit.value as Promise<T>;
  }
  const value = fetcher();
  cache.set(key, { at: Date.now(), value });
  value.catch(() => {
    if (cache.get(key)?.value === value) cache.delete(key);
  });
  return value;
}

/* clients can request arbitrary 'lang:<code>' keys, so sweep expired entries
   periodically to keep the map bounded */
const cacheSweeper = setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of cache) {
    if (now - entry.at >= CACHE_TTL_MS) cache.delete(key);
  }
}, 30 * 1000);
cacheSweeper.unref();

export const clickHouseStatsProvider: StatsProvider = {
  ping: pingClickHouse,

  getEmojiStats(): Promise<EmojiStats> {
    return cached('global', async () => {
      const [topEmojis, counters] = await Promise.all([
        fetchTopEmojis(),
        fetchGlobalCounters(),
      ]);

      const ratio =
        counters.postsWithoutEmojis > 0
          ? (
              (counters.postsWithEmojis || 0) /
              (counters.postsWithoutEmojis || 1)
            ).toFixed(4)
          : 'N/A';

      return {
        processedPosts: counters.processedPosts,
        processedEmojis: counters.processedEmojis,
        postsWithEmojis: counters.postsWithEmojis,
        postsWithoutEmojis: counters.postsWithoutEmojis,
        ratio,
        topEmojis,
      };
    });
  },

  getTopLanguages(): Promise<LanguageStat[]> {
    return cached('langs', fetchTopLanguages);
  },

  getTopEmojisForLanguage(language: string): Promise<EmojiCount[]> {
    return cached(`lang:${language}`, () =>
      fetchTopEmojisForLanguage(language),
    );
  },

  async close(): Promise<void> {
    clearInterval(cacheSweeper);
    await clickhouse.close();
  },
};
