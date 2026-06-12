import type { ArchiveRow } from 'archive/types';
import { normalizePost } from 'ingest/normalizer';
import { toArchiveRow } from 'ingest/rows';
import type { RawPostEvent } from 'ingest/types';

import type { ParsedRepo } from './parser.js';

/**
 * Streams parsed.posts as full-fidelity ArchiveRows in MST walk order, one
 * row alive at a time — the CAR buffer must stay resident (the MST reader
 * needs random block access), so it has to remain the repo's ONLY full
 * in-memory copy or whale repos co-scheduled under GLOBAL_CONCURRENCY blow
 * the heap. The ClickHouse insert strips back down via toClickhouseRow.
 * parsed.rev / parsed.recordsTotal are final once this is exhausted; rev is
 * already set by the first yield whenever the commit scanner succeeded (see
 * ParsedRepo.rev).
 */
export async function* repoPostRows(
  did: string,
  parsed: ParsedRepo,
  fetchTimeUs: number,
): AsyncGenerator<ArchiveRow, void, undefined> {
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
      extras: {
        facets: record.facets,
        reply: record.reply,
        embed: record.embed,
        labels: record.labels,
      },
    };
    yield toArchiveRow(normalizePost(event), 'backfill');
  }
}
