import { clickhouseText, type StoragePolicy } from 'archive/policy';

import type { NormalizedPost, PostRow, Source } from './types.js';

/**
 * THE NormalizedPost → row conversion — live writer, backfill pipeline and
 * archive appends all go through here; never copy it. The full-text form is
 * also the ArchiveRow shape (same fields, same created_at contract).
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
