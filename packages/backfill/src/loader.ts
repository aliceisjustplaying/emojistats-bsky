import { createHash } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';

import type { ClickHouseClient } from '@clickhouse/client';
import type { PostRow } from 'ingest/types';

import { LOADER_BATCH_ROWS, LOADER_FLUSH_MS } from './config.js';
import logger from './logger.js';
import type { RepoLoad, RepoLoader } from './types.js';

const MAX_INSERT_ATTEMPTS = 3;
const INSERT_BACKOFF_BASE_MS = 1_000;

/**
 * How many flushed generations keep their outcome around for late finish()
 * lookups. A repo touches a generation, then looks it up at its very next
 * addRow/finish — minutes at most. 128 generations × LOADER_FLUSH_MS is hours
 * of slack; a lookup past that bound means the flush settled long ago and a
 * failure would already have parked every involved repo via other paths.
 */
const GEN_RETENTION = 128;

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

interface Waiter {
  resolve: () => void;
  reject: (err: Error) => void;
}

/**
 * Streams parsed repos' PostRows into `posts` (plan 0001 "Load" stage).
 *
 * Rows from MANY repos accumulate in one shared buffer and flush as a single
 * insert when the buffer reaches LOADER_BATCH_ROWS or the oldest buffered row
 * is LOADER_FLUSH_MS old. This exists because the average repo is ~64 posts
 * spread across its account's whole lifetime: inserting per repo meant ~24
 * tiny inserts/s fleet-wide, each shattering into one part per month
 * partition touched (~46 at full history) — a parts storm that buried the
 * serving box (244k parts for 208 MiB of data, merge starvation, the
 * OvercommitTracker shooting dashboard queries). Batching divides part
 * creation by the number of repos per flush.
 *
 * Durability bookkeeping: the buffer carries a generation number that
 * increments at every flush swap. Each repo handle records the generations
 * its rows landed in; finish() resolves only after every one of those
 * generations' flushes succeeded — the pipeline's "ledger flips to 'loaded'
 * only after finish() resolves" contract is unchanged, it just resolves for
 * many repos at once. A flush failure rejects every repo with rows in that
 * batch — batch-mates of a poison row park as retryable and their re-fetch
 * collapses into ReplacingMergeTree like any other at-least-once replay.
 *
 * The insert_deduplication_token is a digest of the batch's (did, rev) pairs
 * plus row count: the in-process retry loop resends the identical payload
 * under the identical token, so transient mid-insert failures cannot
 * double-load (within non_replicated_deduplication_window). Across crashes,
 * batch composition differs, tokens differ, and the re-inserted rows collapse
 * structurally via ReplacingMergeTree(did, rkey) — loads stay idempotent
 * either way.
 */
export class ClickHouseRepoLoader implements RepoLoader {
  readonly #client: ClickHouseClient;
  readonly #batchRows: number;
  readonly #flushMs: number;
  readonly #table: string;

  #buffer: PostRow[] = [];
  #bufferTags: string[] = [];
  #waiters: Waiter[] = [];
  #gen = 0;
  #runByGen = new Map<number, Promise<void>>();
  #flushTimer: NodeJS.Timeout | null = null;
  // Serializes flushes so a slow insert delays the next batch instead of
  // racing it (and so buffer swaps stay atomic between awaits).
  #flushChain: Promise<void> = Promise.resolve();

  constructor(
    client: ClickHouseClient,
    options: { batchRows?: number; flushMs?: number; table?: string } = {},
  ) {
    this.#client = client;
    this.#batchRows = options.batchRows ?? LOADER_BATCH_ROWS;
    this.#flushMs = options.flushMs ?? LOADER_FLUSH_MS;
    this.#table = options.table ?? 'posts';
    if (this.#batchRows < 1)
      throw new Error(`batchRows must be >= 1, got ${this.#batchRows}`);
    if (this.#flushMs < 1)
      throw new Error(`flushMs must be >= 1, got ${this.#flushMs}`);
  }

  openRepo(did: string, rev: string | null): RepoLoad {
    let rowsTotal = 0;
    const gensTouched = new Set<number>();
    const tag = `${did}:${rev}`;

    return {
      addRow: async (row: PostRow): Promise<void> => {
        if (!gensTouched.has(this.#gen)) {
          this.#bufferTags.push(tag);
          gensTouched.add(this.#gen);
        }
        this.#buffer.push(row);
        rowsTotal += 1;
        if (this.#buffer.length >= this.#batchRows) {
          // Size-triggered flush is awaited: this is the backpressure that
          // stalls fetching when ClickHouse is slower than the fleet.
          await this.#scheduleFlush();
        } else {
          this.#armTimer();
        }
      },
      finish: async (): Promise<void> => {
        if (rowsTotal === 0) return; // empty repo: nothing to insert, success
        const pending: Array<Promise<void>> = [];
        for (const gen of gensTouched) {
          if (gen === this.#gen) {
            // Tail rows still sit in the live buffer: resolve with its flush.
            pending.push(
              new Promise<void>((resolve, reject) => {
                this.#waiters.push({ resolve, reject });
              }),
            );
            this.#armTimer();
          } else {
            // Generation already swapped out; awaiting a missing entry means
            // it settled successfully more than GEN_RETENTION flushes ago.
            const run = this.#runByGen.get(gen);
            if (run !== undefined) pending.push(run);
          }
        }
        await Promise.all(pending);
        logger.debug({ did, rev, rows: rowsTotal }, 'repo rows durable');
      },
    };
  }

  #armTimer(): void {
    if (this.#flushTimer !== null) return;
    this.#flushTimer = setTimeout(() => {
      void this.#scheduleFlush().catch(() => {
        // Rejection is delivered through every involved repo's finish();
        // nothing to handle here.
      });
    }, this.#flushMs);
  }

  /** Swap out the current buffer + waiters and flush them as one insert. */
  #scheduleFlush(): Promise<void> {
    if (this.#flushTimer !== null) {
      clearTimeout(this.#flushTimer);
      this.#flushTimer = null;
    }
    const rows = this.#buffer;
    const tags = this.#bufferTags;
    const waiters = this.#waiters;
    const gen = this.#gen;
    if (rows.length === 0) return this.#flushChain;
    this.#buffer = [];
    this.#bufferTags = [];
    this.#waiters = [];
    this.#gen += 1;
    this.#runByGen.delete(gen - GEN_RETENTION);

    const run = this.#flushChain.then(async () => {
      try {
        await this.#insertBatch(rows, tags);
        for (const w of waiters) w.resolve();
        return undefined;
      } catch (err) {
        const wrapped = err instanceof Error ? err : new Error(String(err));
        for (const w of waiters) w.reject(wrapped);
        throw wrapped;
      }
    });
    this.#runByGen.set(gen, run);
    // The chain must survive a failed flush (every involved repo received the
    // rejection through its finish/addRow path).
    this.#flushChain = run.catch(() => undefined);
    return run;
  }

  async #insertBatch(rows: PostRow[], tags: string[]): Promise<void> {
    const token = `batch:${createHash('sha1')
      .update(tags.join('\n'))
      .update(`:${rows.length}`)
      .digest('hex')}`;
    for (let attempt = 1; ; attempt += 1) {
      try {
        await this.#client.insert({
          table: this.#table,
          values: rows,
          format: 'JSONEachRow',
          clickhouse_settings: {
            insert_deduplicate: 1,
            insert_deduplication_token: token,
          },
        });
        logger.debug(
          { rows: rows.length, repos: tags.length },
          'batch inserted',
        );
        return;
      } catch (err) {
        const permanent = isPermanentInsertError(err);
        if (permanent || attempt >= MAX_INSERT_ATTEMPTS) {
          throw new Error(
            `ClickHouse batch insert failed (${rows.length} rows from ${tags.length} repos, ` +
              `${permanent ? 'permanent error' : `${attempt} attempts`})`,
            { cause: err },
          );
        }
        const delayMs = INSERT_BACKOFF_BASE_MS * 2 ** (attempt - 1);
        logger.warn(
          {
            rows: rows.length,
            repos: tags.length,
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
