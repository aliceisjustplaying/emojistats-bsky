import { normalizePost } from 'ingest/normalizer';
import { toPostRow } from 'ingest/rows';
import type { PostRow, RawPostEvent } from 'ingest/types';

import type { ParsedRepo } from './parser.js';

/**
 * Streams parsed.posts as full-text PostRows in MST walk order, one row alive
 * at a time — the CAR buffer must stay resident (the MST reader needs random
 * block access), so it has to remain the repo's ONLY full in-memory copy or
 * whale repos co-scheduled under GLOBAL_CONCURRENCY blow the heap.
 * parsed.rev / parsed.recordsTotal are final once this is exhausted; rev is
 * already set by the first yield whenever the commit scanner succeeded (see
 * ParsedRepo.rev).
 */
export async function* repoPostRows(
  did: string,
  parsed: ParsedRepo,
  fetchTimeUs: number,
): AsyncGenerator<PostRow, void, undefined> {
  for await (const { rkey, record } of parsed.posts) {
    const event: RawPostEvent = {
      did,
      rkey,
      text: typeof record.text === 'string' ? record.text : '',
      langs: Array.isArray(record.langs)
        ? record.langs.filter(
            (lang): lang is string => typeof lang === 'string',
          )
        : undefined,
      createdAt:
        typeof record.createdAt === 'string' ? record.createdAt : undefined,
      timeUs: fetchTimeUs,
    };
    yield toPostRow(normalizePost(event), 'backfill');
  }
}
