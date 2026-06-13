import { createHash } from 'node:crypto';

import type { ClickHouseClient } from '@clickhouse/client';
import type { StoragePolicy } from 'archive/policy';

import logger from './logger.js';
import { applyTextPolicy, toPostRow } from './rows.js';
import type { NormalizedPost, PostRow, Source } from './types.js';

// Backoff covers ClickHouse outages only; the flush cadence itself never stretches (plan 0001).
// A failed batch parks in pendingBatch and retries verbatim — same rows, same
// dedup token — never merged back into the live buffer. A phantom failure
// (server committed the insert, response lost) therefore re-sends an identical
// block: the server's dedup window drops it AND skips the dependent MV pushes,
// so the SummingMergeTree aggregates cannot double-count.
const BACKOFF_INITIAL_MS = 2_000;
const BACKOFF_MAX_MS = 30_000;
const STOP_FLUSH_RETRIES = 3;
const STOP_RETRY_DELAY_MS = 1_000;

export interface ClickHouseWriterOptions {
  flushIntervalMs: number;
  maxBufferRows: number;
  /** Resolved once at startup (archive/policy); decides which rows keep text in ClickHouse. */
  policy: StoragePolicy;
  /** Sampled at buffer swap time so a persisted cursor never runs ahead of inserted rows. */
  getCursor?: () => number | undefined;
  /**
   * Sampled at buffer swap time; the returned promise settles once every
   * archive append in flight at that moment has settled. Awaited after a
   * successful insert, before onFlushSuccess — acks must not outrun the
   * archive (plan 0001: it is the only durable home of non-emoji text).
   */
  commitBarrier?: () => Promise<void>;
  /** Fires only once the batch is durable: insert succeeded AND the commit barrier passed. */
  onFlushSuccess?: (cursorAtSwap: number | undefined, rows: PostRow[]) => void;
}

export interface WriterStats {
  enqueued: number;
  written: number;
  failedFlushes: number;
  /** Every unacked row in memory: live buffer plus any parked retry batch. */
  bufferSize: number;
}

/**
 * A batch that failed its insert, parked for an identical retry. Everything
 * sampled at the buffer swap rides along: the rows (never mutated after the
 * swap), the dedup token derived from them, the cursor, and the commit-barrier
 * promise. Re-sampling any of these on retry would defeat the token (different
 * rows ⇒ different block) or loosen the durability ordering (plan 0001).
 */
interface PendingBatch {
  rows: PostRow[];
  token: string;
  cursorAtSwap: number | undefined;
  barrier: Promise<void> | undefined;
}

export class ClickHouseWriter {
  private buffer: PostRow[] = [];
  private pendingBatch: PendingBatch | null = null;
  private interval: NodeJS.Timeout | undefined;
  private inFlight: Promise<boolean> | null = null;
  private backoffMs = 0;
  private nextFlushAt = 0;
  private stopping = false;
  private enqueuedCount = 0;
  private writtenCount = 0;
  private failedFlushCount = 0;

  constructor(
    private readonly client: ClickHouseClient,
    private readonly opts: ClickHouseWriterOptions,
  ) {}

  enqueue(post: NormalizedPost, src: Source): void {
    this.buffer.push(applyTextPolicy(toPostRow(post, src), this.opts.policy));
    this.enqueuedCount += 1;

    // No-drop crash guard counts every unacked row — the live buffer AND the
    // parked retry batch. During a long outage the buffer keeps growing while
    // pendingBatch retries, and both are rows we refuse to shed (plan 0001).
    const held = this.buffer.length + (this.pendingBatch?.rows.length ?? 0);
    if (held > this.opts.maxBufferRows) {
      logger.fatal(
        `Writer holding ${held} unacked rows (buffer + pending batch), over the ${this.opts.maxBufferRows} cap; refusing to drop data (plan 0001), crashing instead`,
      );
      throw new Error(
        `ClickHouseWriter buffer exceeded ${this.opts.maxBufferRows} rows`,
      );
    }
  }

  start(): void {
    if (this.interval) return;
    this.interval = setInterval(() => {
      void this.flush();
    }, this.opts.flushIntervalMs);
  }

  async flush(): Promise<void> {
    if (this.inFlight !== null || this.stopping) return;
    if (this.pendingBatch === null && this.buffer.length === 0) return;
    if (Date.now() < this.nextFlushAt) return;

    this.inFlight = this.flushOnce();
    try {
      await this.inFlight;
    } finally {
      this.inFlight = null;
    }
  }

  async stop(): Promise<void> {
    this.stopping = true;
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = undefined;
    }
    if (this.inFlight !== null) await this.inFlight;

    // Drain is up to two batches now (the parked retry, then the live
    // buffer), so failures bound the loop instead of total iterations: a
    // success costs no retry, each failure burns one of STOP_FLUSH_RETRIES.
    let failedAttempts = 0;
    while (this.pendingBatch !== null || this.buffer.length > 0) {
      if (await this.flushOnce()) continue;
      failedAttempts += 1;
      if (failedAttempts >= STOP_FLUSH_RETRIES) break;
      await new Promise((resolve) => {
        setTimeout(resolve, STOP_RETRY_DELAY_MS);
      });
    }

    const remaining =
      this.buffer.length + (this.pendingBatch?.rows.length ?? 0);
    if (remaining > 0) {
      throw new Error(
        `ClickHouseWriter.stop: ${remaining} rows still buffered after ${STOP_FLUSH_RETRIES} flush attempts`,
      );
    }
  }

  get stats(): WriterStats {
    return {
      enqueued: this.enqueuedCount,
      written: this.writtenCount,
      failedFlushes: this.failedFlushCount,
      bufferSize: this.buffer.length + (this.pendingBatch?.rows.length ?? 0),
    };
  }

  /**
   * Deduplication token for one swapped batch. Stable across retries because
   * the batch's row array is frozen at the swap, so a phantom-failure retry
   * re-sends the exact block under the exact token and the server drops it
   * (table has non_replicated_deduplication_window = 10000, and a token-dropped
   * block also skips its MV pushes, protecting the aggregates).
   *
   * Hashing every row identity makes different batches collision-resistant
   * even when they share length and boundaries. The `live:` prefix keeps these
   * disjoint from the backfill loader's token space (loader.ts).
   */
  private static batchToken(rows: PostRow[]): string {
    const hash = createHash('sha1');
    for (const row of rows) {
      hash.update(row.did);
      hash.update('\0');
      hash.update(row.rkey);
      hash.update('\0');
    }
    return `live:${rows.length}:${hash.digest('hex')}`;
  }

  private async flushOnce(): Promise<boolean> {
    // A parked batch always goes first, verbatim. Only once it lands does a
    // later flush swap the live buffer into a new batch — swapping early
    // would re-batch the same rows under a new token and reopen the
    // phantom-failure double-count this token exists to close.
    if (this.pendingBatch === null) {
      if (this.buffer.length === 0) return true;
      const rows = this.buffer;
      this.buffer = [];
      // Cursor must be captured at the swap, not after the insert: events enqueued during the
      // insert advance the source cursor past rows that are not in this batch.
      const cursorAtSwap = this.opts.getCursor?.();
      // Barrier captured at the swap too: every append for this batch starts
      // before its row is enqueued, so a swap-time barrier covers all of them
      // (plus, harmlessly, appends for newer rows). It rides in pendingBatch
      // so every retry awaits the same settlement promise — correctness no
      // longer depends on the barrier provider tolerating re-sampling after
      // arbitrary delay.
      const barrier = this.opts.commitBarrier?.();
      this.pendingBatch = {
        rows,
        token: ClickHouseWriter.batchToken(rows),
        cursorAtSwap,
        barrier,
      };
    }
    const batch = this.pendingBatch;

    try {
      await this.client.insert({
        table: 'posts',
        values: batch.rows,
        format: 'JSONEachRow',
        clickhouse_settings: {
          insert_deduplicate: 1,
          insert_deduplication_token: batch.token,
        },
      });
      this.writtenCount += batch.rows.length;
      this.backoffMs = 0;
      this.nextFlushAt = 0;
      // Insert success alone is not durability: the batch's archive appends
      // must settle before anyone marks dedupe or advances the cursor.
      if (batch.barrier) await batch.barrier;
      this.pendingBatch = null;
      this.opts.onFlushSuccess?.(batch.cursorAtSwap, batch.rows);
      return true;
    } catch (error) {
      // The batch stays parked untouched; rows enqueued meanwhile keep
      // accumulating in the buffer behind it, so insert order still holds.
      this.failedFlushCount += 1;
      this.backoffMs =
        this.backoffMs === 0
          ? BACKOFF_INITIAL_MS
          : Math.min(this.backoffMs * 2, BACKOFF_MAX_MS);
      this.nextFlushAt = Date.now() + this.backoffMs;
      logger.error(
        `ClickHouse insert failed (${batch.rows.length} rows parked for identical retry, ${this.buffer.length} more buffered, next attempt in ${this.backoffMs}ms): ${(error as Error).message}`,
      );
      return false;
    }
  }
}
