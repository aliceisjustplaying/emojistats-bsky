import { clickhouseText, type StoragePolicy } from 'archive/policy';
import type { ArchiveRow } from 'archive/types';

import type { NormalizedPost, PostRow, Source } from './types.js';

/**
 * THE NormalizedPost → row conversion — live writer, backfill pipeline and
 * archive appends all go through here; never copy it. The archive form is a
 * strict superset (toArchiveRow); ClickHouse rows must go through
 * toClickhouseRow so the metadata columns can never leak into JSONEachRow.
 */
export function toPostRow(post: NormalizedPost, src: Source): PostRow {
  return {
    did: post.did,
    rkey: post.rkey,
    // ClickHouse DateTime('UTC') over JSONEachRow wants 'YYYY-MM-DD HH:MM:SS' — no T, Z, or millis.
    created_at: post.createdAt.toISOString().slice(0, 19).replace('T', ' '),
    text: post.text,
    langs: post.langs,
    emojis: post.emojis,
    src,
  };
}

/** '' = field absent on the record; the archive never stores the string "null". */
function extraJson(value: unknown): string {
  if (value === undefined || value === null) return '';
  return JSON.stringify(value);
}

/** Full-fidelity archive form: PostRow plus the record metadata as raw JSON. */
export function toArchiveRow(post: NormalizedPost, src: Source): ArchiveRow {
  return {
    ...toPostRow(post, src),
    facets_json: extraJson(post.extras.facets),
    reply_json: extraJson(post.extras.reply),
    embed_json: extraJson(post.extras.embed),
    labels_json: extraJson(post.extras.labels),
  };
}

/**
 * The exact JSONEachRow shape for ClickHouse `posts` — an explicit pick, not a
 * spread, so widening ArchiveRow can never silently grow the insert payload.
 */
export function toClickhouseRow(row: PostRow, policy: StoragePolicy): PostRow {
  return applyTextPolicy(
    {
      did: row.did,
      rkey: row.rkey,
      created_at: row.created_at,
      text: row.text,
      langs: row.langs,
      emojis: row.emojis,
      src: row.src,
    },
    policy,
  );
}

/**
 * The ClickHouse form of a full-text row: under the 'emoji' policy, emoji-less
 * posts store '' — their text lives only in the Parquet archive (plan 0001).
 */
export function applyTextPolicy(row: PostRow, policy: StoragePolicy): PostRow {
  if (policy.textInClickhouse === 'all') return row;
  return {
    ...row,
    text: clickhouseText(policy, row.text, row.emojis.length > 0),
  };
}
