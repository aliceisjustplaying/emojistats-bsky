import { setTimeout as sleep } from 'node:timers/promises';

import type { ClickHouseClient } from '@clickhouse/client';
import type { PostRow } from 'ingest/types';

import { LOADER_CHUNK_ROWS } from './config.js';
import logger from './logger.js';
import type { RepoLoad, RepoLoader } from './types.js';

const MAX_INSERT_ATTEMPTS = 3;
const INSERT_BACKOFF_BASE_MS = 1_000;

/**
 * ClickHouse server error codes where retrying the identical insert cannot
 * succeed (schema/auth/data-shape problems). Everything else — socket resets,
 * timeouts, TOO_MANY_SIMULTANEOUS_QUERIES, memory pressure — is treated as
 * transient and retried with backoff.
 */
const PERMANENT_CH_ERROR_CODES = new Set([
  '6', // CANNOT_PARSE_TEXT
  '26', // CANNOT_PARSE_ESCAPE_SEQUENCE
  '27', // CANNOT_PARSE_INPUT_ASSERTION_FAILED
  '53', // TYPE_MISMATCH
  '60', // UNKNOWN_TABLE
  '81', // UNKNOWN_DATABASE
  '117', // INCORRECT_DATA
  '497', // ACCESS_DENIED
  '516', // AUTHENTICATION_FAILED
]);

function isPermanentInsertError(err: unknown): boolean {
  const code = (err as { code?: unknown })?.code;
  return typeof code === 'string' && PERMANENT_CH_ERROR_CODES.has(code);
}

/**
 * Streams one parsed repo's PostRows into `posts` (plan 0001 "Load" stage).
 *
 * openRepo hands back a per-repo handle: addRow buffers up to LOADER_CHUNK_ROWS
 * and inserts each full chunk as JSONEachRow with insert_deduplication_token =
 * `${did}:${rev}:${chunkRows}:${chunkIdx}`; finish flushes the final partial
 * chunk. Rows arrive in MST walk order — deterministic for a given commit — so
 * re-fetching the same (did, rev) at the same chunk size (crash recovery,
 * retry waves) re-sends byte-identical chunks under identical tokens and the
 * server drops them instead of creating duplicate parts. The chunk size lives
 * in the token because it shapes the chunks: without it, retrying a partial
 * load under a different LOADER_CHUNK_ROWS would reuse a token for a chunk
 * covering MORE rows, and the server would silently skip rows it has never
 * seen. Changing the size mid-crawl therefore changes every token — harmless
 * re-inserts that ReplacingMergeTree collapses, instead of silent skips.
 * NOTE: token dedup on a
 * non-replicated MergeTree requires the table setting
 * non_replicated_deduplication_window > 0 (it is 0 by default); without it
 * duplicates still collapse structurally via ReplacingMergeTree(did, rkey) at
 * merge / FINAL time, so loads remain idempotent either way — the token just
 * avoids the interim raw duplicates.
 *
 * addRow/finish throw after MAX_INSERT_ATTEMPTS on a chunk (or immediately on
 * permanent errors); the caller decides what that means for the ledger. The
 * ledger must flip to 'loaded' only after finish() resolves.
 */
export class ClickHouseRepoLoader implements RepoLoader {
  readonly #client: ClickHouseClient;
  readonly #chunkRows: number;
  readonly #table: string;

  constructor(
    client: ClickHouseClient,
    options: { chunkRows?: number; table?: string } = {},
  ) {
    this.#client = client;
    this.#chunkRows = options.chunkRows ?? LOADER_CHUNK_ROWS;
    this.#table = options.table ?? 'posts';
    if (this.#chunkRows < 1)
      throw new Error(`chunkRows must be >= 1, got ${this.#chunkRows}`);
  }

  openRepo(did: string, rev: string | null): RepoLoad {
    let chunk: PostRow[] = [];
    let chunkIdx = 0;
    let rowsTotal = 0;

    const flush = async (): Promise<void> => {
      const full = chunk;
      chunk = [];
      await this.#insertChunk(did, rev, chunkIdx, full);
      chunkIdx += 1;
    };

    return {
      addRow: async (row: PostRow): Promise<void> => {
        chunk.push(row);
        rowsTotal += 1;
        if (chunk.length >= this.#chunkRows) await flush();
      },
      finish: async (): Promise<void> => {
        if (chunk.length > 0) await flush();
        if (rowsTotal === 0) return; // empty repo: nothing to insert, success
        logger.debug(
          { did, rev, rows: rowsTotal, chunks: chunkIdx },
          'repo rows inserted',
        );
      },
    };
  }

  async #insertChunk(
    did: string,
    rev: string | null,
    chunkIdx: number,
    chunk: PostRow[],
  ): Promise<void> {
    const token = `${did}:${rev}:${this.#chunkRows}:${chunkIdx}`;
    for (let attempt = 1; ; attempt += 1) {
      try {
        await this.#client.insert({
          table: this.#table,
          values: chunk,
          format: 'JSONEachRow',
          clickhouse_settings: {
            insert_deduplicate: 1,
            insert_deduplication_token: token,
          },
        });
        return;
      } catch (err) {
        const permanent = isPermanentInsertError(err);
        if (permanent || attempt >= MAX_INSERT_ATTEMPTS) {
          throw new Error(
            `ClickHouse insert failed for ${did} (chunk ${chunkIdx + 1}, ${chunk.length} rows, ` +
              `${permanent ? 'permanent error' : `${attempt} attempts`})`,
            { cause: err },
          );
        }
        const delayMs = INSERT_BACKOFF_BASE_MS * 2 ** (attempt - 1);
        logger.warn(
          {
            did,
            chunkIdx,
            attempt,
            delayMs,
            err: err instanceof Error ? err.message : String(err),
          },
          'transient ClickHouse insert failure, retrying',
        );
        await sleep(delayMs);
      }
    }
  }
}
