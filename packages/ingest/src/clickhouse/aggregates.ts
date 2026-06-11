/**
 * Single source of the aggregate-table SQL (plan 0001).
 *
 * Each Summing table has exactly one aggregation SELECT, defined here once and
 * consumed twice: migrate.ts creates the live materialized view from it, and
 * rebuild.ts re-runs it over `posts FINAL` to re-derive the table from truth.
 * schema.sql keeps only the table DDL — change an aggregation here and the MV
 * and the rebuild can never drift apart.
 *
 * "Cannot drift" needs one more piece: a live database created before the
 * change still runs the OLD view. Each view therefore carries a content hash
 * of its DDL in its comment (mvSpecComment); migrate.ts compares it against
 * this file and recreates any view whose hash no longer matches.
 */

import { createHash } from 'node:crypto';

export interface AggregateSpec {
  /** Destination SummingMergeTree table (declared in schema.sql). */
  readonly table: string;
  /** The materialized view that feeds the table on live inserts. */
  readonly mvName: string;
  /** Hour-keyed tables support windowed --recent repair; totals do not. */
  readonly hourly: boolean;
  /** Summed measure columns, logged before/after as rebuild evidence. */
  readonly measures: readonly string[];
  /**
   * The aggregation SELECT. `whereClause` narrows the spine scan (rebuild
   * --recent); omitted means all of history. `final` reads `posts FINAL` so a
   * rebuild collapses the ReplacingMergeTree duplicates the MV double-counted;
   * the MV itself reads plain `posts` (it only ever sees fresh insert blocks).
   */
  readonly select: (whereClause?: string, final?: boolean) => string;
}

function posts(final: boolean | undefined): string {
  return final === true ? 'posts FINAL' : 'posts';
}

function emojiWhere(whereClause: string | undefined): string {
  return whereClause === undefined
    ? 'WHERE notEmpty(emojis)'
    : `WHERE notEmpty(emojis) AND ${whereClause}`;
}

export const AGGREGATES: readonly AggregateSpec[] = [
  {
    table: 'emoji_hourly',
    mvName: 'mv_emoji_hourly',
    hourly: true,
    measures: ['occurrences', 'posts'],
    select: (whereClause, final) => `
SELECT hour, emoji, sum(occ) AS occurrences, toUInt64(count()) AS posts
FROM (
  SELECT
    toStartOfHour(created_at) AS hour,
    arrayJoin(arrayDistinct(emojis)) AS emoji,
    toUInt64(countEqual(emojis, emoji)) AS occ
  FROM ${posts(final)}
  ${emojiWhere(whereClause)}
)
GROUP BY hour, emoji`,
  },
  {
    table: 'emoji_hourly_by_lang',
    mvName: 'mv_emoji_hourly_by_lang',
    hourly: true,
    measures: ['occurrences', 'posts'],
    select: (whereClause, final) => `
SELECT lang, hour, emoji, sum(occ) AS occurrences, toUInt64(count()) AS posts
FROM (
  SELECT
    lang,
    toStartOfHour(created_at) AS hour,
    arrayJoin(arrayDistinct(emojis)) AS emoji,
    toUInt64(countEqual(emojis, emoji)) AS occ
  FROM (
    SELECT arrayJoin(langs) AS lang, created_at, emojis
    FROM ${posts(final)}
    ${emojiWhere(whereClause)}
  )
)
GROUP BY lang, hour, emoji`,
  },
  {
    table: 'posts_hourly',
    mvName: 'mv_posts_hourly',
    hourly: true,
    measures: ['posts', 'posts_with_emojis', 'emoji_occurrences'],
    select: (whereClause, final) => `
SELECT
  toStartOfHour(created_at) AS hour,
  toUInt64(count()) AS posts,
  toUInt64(countIf(notEmpty(emojis))) AS posts_with_emojis,
  toUInt64(sum(length(emojis))) AS emoji_occurrences
FROM ${posts(final)}
${whereClause === undefined ? '' : `WHERE ${whereClause}\n`}GROUP BY hour`,
  },
  {
    table: 'emoji_total',
    mvName: 'mv_emoji_total',
    hourly: false,
    measures: ['occurrences', 'posts'],
    select: (whereClause, final) => `
SELECT emoji, sum(occ) AS occurrences, toUInt64(count()) AS posts
FROM (
  SELECT
    arrayJoin(arrayDistinct(emojis)) AS emoji,
    toUInt64(countEqual(emojis, emoji)) AS occ
  FROM ${posts(final)}
  ${emojiWhere(whereClause)}
)
GROUP BY emoji`,
  },
  {
    table: 'emoji_total_by_lang',
    mvName: 'mv_emoji_total_by_lang',
    hourly: false,
    measures: ['occurrences', 'posts'],
    select: (whereClause, final) => `
SELECT lang, emoji, sum(occ) AS occurrences, toUInt64(count()) AS posts
FROM (
  SELECT
    lang,
    arrayJoin(arrayDistinct(emojis)) AS emoji,
    toUInt64(countEqual(emojis, emoji)) AS occ
  FROM (
    SELECT arrayJoin(langs) AS lang, emojis
    FROM ${posts(final)}
    ${emojiWhere(whereClause)}
  )
)
GROUP BY lang, emoji`,
  },
  {
    table: 'lang_total',
    mvName: 'mv_lang_total',
    hourly: false,
    measures: ['occurrences', 'posts'],
    select: (whereClause, final) => `
SELECT lang, sum(occ) AS occurrences, toUInt64(count()) AS posts
FROM (
  SELECT arrayJoin(langs) AS lang, toUInt64(length(emojis)) AS occ
  FROM ${posts(final)}
  ${emojiWhere(whereClause)}
)
GROUP BY lang`,
  },
];

/**
 * The CREATE statement migrate.ts applies after schema.sql's tables exist.
 *
 * IF NOT EXISTS only protects a fresh create against a concurrent migrate; it
 * never updates an existing view. Updating is migrate.ts's job: it compares
 * mvSpecComment against the live view and DROPs first when they differ —
 * ClickHouse has no ALTER for a TO-form view's SELECT.
 */
export function mvCreateSql(spec: AggregateSpec): string {
  return `CREATE MATERIALIZED VIEW IF NOT EXISTS ${spec.mvName} TO ${spec.table} AS\n${spec.select().trim()}`;
}

/**
 * Version stamp migrate.ts stores in the view's comment and reads back from
 * system.tables. Hashes the full CREATE (SELECT + TO target), so retargeting a
 * view reads as drift just like editing its SELECT. 16 hex chars of sha256:
 * collision-proof for a handful of views, short enough to eyeball.
 */
export function mvSpecComment(spec: AggregateSpec): string {
  const hash = createHash('sha256')
    .update(mvCreateSql(spec).trim())
    .digest('hex')
    .slice(0, 16);
  return `spec:${hash}`;
}

/**
 * Attaches the version stamp. A separate ALTER because ClickHouse 25.x rejects
 * a COMMENT clause anywhere inside a TO-form CREATE MATERIALIZED VIEW
 * (verified against 25.3.14). Benign if migrate dies between CREATE and this:
 * the comment-less view reads as drift and the next run recreates it.
 */
export function mvCommentSql(spec: AggregateSpec): string {
  return `ALTER TABLE ${spec.mvName} MODIFY COMMENT '${mvSpecComment(spec)}'`;
}
