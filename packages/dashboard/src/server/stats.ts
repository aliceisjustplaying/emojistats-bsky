import { createServerFn } from '@tanstack/react-start';

import { chDatabase, chQuery, num } from './clickhouse';

// All aggregate tables are SummingMergeTree: merges settle lazily, so every
// query against them must aggregate with sum() + GROUP BY (see
// packages/ingest/src/clickhouse/schema.sql).

export interface LiveStats {
  generatedAt: string;
  postsPerSec1m: number;
  postsPerSec15m: number;
  freshnessSeconds: number;
  totals: {
    posts: number;
    postsWithEmojis: number;
    emojiShare: number;
    emojiOccurrences: number;
    distinctGlyphs: number;
  };
  topEmojis: Array<{ emoji: string; occurrences: number; posts: number }>;
  languages: Array<{ lang: string; occurrences: number; posts: number }>;
}

export interface StorageStats {
  hourly: Array<{ hour: string; posts: number; postsWithEmojis: number }>;
  tables: Array<{ table: string; parts: number; bytesOnDisk: number }>;
  totalParts: number;
  totalBytes: number;
}

interface RatesRow {
  posts_1m: string;
  posts_15m: string;
}
interface FreshnessRow {
  freshness_s: string | number;
}
interface TotalsRow {
  posts: string;
  posts_with_emojis: string;
  emoji_occurrences: string;
  distinct_glyphs: string;
}
interface EmojiRow {
  emoji: string;
  occurrences: string;
  posts: string;
}
interface LangRow {
  lang: string;
  occurrences: string;
  posts: string;
}
interface HourlyRow {
  hour: string;
  posts: string;
  posts_with_emojis: string;
}
interface PartsRow {
  table: string;
  parts: string;
  bytes_on_disk: string;
}

export const getLiveStats = createServerFn().handler(
  async (): Promise<LiveStats> => {
    const [rates, freshness, totals, topEmojis, languages] = await Promise.all([
      // Ingest-time aggregate: keeps the public dashboard off raw posts scans.
      // Source-filtered to the Jetstream/live path so recrawls cannot mask a
      // stalled live worker.
      chQuery<RatesRow>(`
        SELECT
          sumIf(posts, second >= now() - INTERVAL 1 MINUTE) AS posts_1m,
          sumIf(posts, second >= now() - INTERVAL 15 MINUTE) AS posts_15m
        FROM live_ingest_second
        WHERE second >= now() - INTERVAL 15 MINUTE
      `),
      chQuery<FreshnessRow>(`
        SELECT greatest(dateDiff('second', max(second), now()), 0) AS freshness_s
        FROM live_ingest_second
      `),
      chQuery<TotalsRow>(`
        SELECT
          sum(posts) AS posts,
          sum(posts_with_emojis) AS posts_with_emojis,
          sum(emoji_occurrences) AS emoji_occurrences,
          (SELECT uniqExact(emoji) FROM emoji_total) AS distinct_glyphs
        FROM posts_hourly
      `),
      chQuery<EmojiRow>(`
        SELECT emoji, sum(occurrences) AS occurrences, sum(posts) AS posts
        FROM emoji_total
        GROUP BY emoji
        ORDER BY occurrences DESC
        LIMIT 10
      `),
      chQuery<LangRow>(`
        SELECT lang, sum(occurrences) AS occurrences, sum(posts) AS posts
        FROM lang_total
        GROUP BY lang
        ORDER BY occurrences DESC
        LIMIT 8
      `),
    ]);

    const posts = num(totals[0]?.posts);
    const postsWithEmojis = num(totals[0]?.posts_with_emojis);

    return {
      generatedAt: new Date().toISOString(),
      postsPerSec1m: num(rates[0]?.posts_1m) / 60,
      postsPerSec15m: num(rates[0]?.posts_15m) / (15 * 60),
      freshnessSeconds: num(freshness[0]?.freshness_s),
      totals: {
        posts,
        postsWithEmojis,
        emojiShare: posts > 0 ? postsWithEmojis / posts : 0,
        emojiOccurrences: num(totals[0]?.emoji_occurrences),
        distinctGlyphs: num(totals[0]?.distinct_glyphs),
      },
      topEmojis: topEmojis.map((row) => ({
        emoji: row.emoji,
        occurrences: num(row.occurrences),
        posts: num(row.posts),
      })),
      languages: languages.map((row) => ({
        lang: row.lang,
        occurrences: num(row.occurrences),
        posts: num(row.posts),
      })),
    };
  },
);

export const getStorageStats = createServerFn().handler(
  async (): Promise<StorageStats> => {
    const [hourly, parts] = await Promise.all([
      chQuery<HourlyRow>(`
        WITH toStartOfHour(now()) AS end_hour
        SELECT
          hour,
          toUInt64(coalesce(sum(posts), 0)) AS posts,
          toUInt64(coalesce(sum(posts_with_emojis), 0)) AS posts_with_emojis
        FROM
        (
          SELECT end_hour - number * 3600 AS hour
          FROM numbers(24)
        ) AS hours
        LEFT JOIN
        (
          SELECT
            hour,
            sum(posts) AS posts,
            sum(posts_with_emojis) AS posts_with_emojis
          FROM posts_hourly
          WHERE
            hour >= end_hour - INTERVAL 23 HOUR
            AND hour <= end_hour
          GROUP BY hour
        ) AS aggregates USING hour
        GROUP BY hour
        ORDER BY hour
      `),
      chQuery<PartsRow>(
        `
        SELECT table, count() AS parts, sum(bytes_on_disk) AS bytes_on_disk
        FROM system.parts
        WHERE active AND database = {db:String}
        GROUP BY table
        ORDER BY bytes_on_disk DESC
      `,
        { db: chDatabase() },
      ),
    ]);

    const tables = parts.map((row) => ({
      table: row.table,
      parts: num(row.parts),
      bytesOnDisk: num(row.bytes_on_disk),
    }));

    return {
      hourly: hourly.map((row) => ({
        hour: row.hour,
        posts: num(row.posts),
        postsWithEmojis: num(row.posts_with_emojis),
      })),
      tables,
      totalParts: tables.reduce((acc, t) => acc + t.parts, 0),
      totalBytes: tables.reduce((acc, t) => acc + t.bytesOnDisk, 0),
    };
  },
);
