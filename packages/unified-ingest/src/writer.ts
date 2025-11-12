import type { Pool } from "pg";
import { insertEmojiRows } from "./db.js";
import { DimensionCache } from "./dimensions.js";
import { ParquetSink } from "./parquetSink.js";
import type { NormalizedEmojiPost, PreparedEmojiRow } from "./types.js";
import { timescaleRows } from "./metrics.js";

export class EmojiPostWriter {
  private batch: PreparedEmojiRow[] = [];
  private readonly parquetCountByRepo = new Map<string, number>();
  private readonly insertedCountByRepo = new Map<string, number>();
  private flushTimer?: NodeJS.Timeout;
  // Track pending flush promise - multiple events can await the same flush
  private pendingFlushPromise: Promise<void> | null = null;
  private pendingFlushResolver: (() => void) | null = null;

  constructor(
    private readonly pool: Pool,
    private readonly dimensions: DimensionCache,
    private readonly parquet: ParquetSink,
    private readonly batchSize = 500,
    private readonly flushIntervalMs = 60000, // Flush every 60 seconds
  ) {
    // Start periodic flush timer
    this.flushTimer = setInterval(() => {
      this.flush().catch((error) => {
        console.error("Periodic flush failed:", error);
      });
    }, this.flushIntervalMs);
  }

  async enqueue(post: NormalizedEmojiPost) {
    const langId = await this.dimensions.getLanguageId(post.primaryLang);
    const clientId = await this.dimensions.getClientId(post.clientIdentifier);
    const emojiIds = await Promise.all(
      post.emojiGlyphs.map((glyph) => this.dimensions.getEmojiId(glyph)),
    );

    this.batch.push({
      postUri: post.postUri,
      repoDid: post.repoDid,
      rkey: post.rkey,
      seq: post.seq,
      createdAt: post.createdAt,
      receivedAt: post.receivedAt,
      langId,
      clientId,
      emojiIds,
      authorDid: post.authorDid,
      replyRootUri: post.replyRootUri,
      replyParentUri: post.replyParentUri,
    });

    await this.parquet.append(post, emojiIds);
    const current = this.parquetCountByRepo.get(post.repoDid) ?? 0;
    this.parquetCountByRepo.set(post.repoDid, current + 1);

    // Create flush promise for this batch if it doesn't exist
    if (!this.pendingFlushPromise) {
      this.pendingFlushPromise = new Promise<void>((resolve) => {
        this.pendingFlushResolver = resolve;
      });
    }

    // If batch is full, flush immediately
    if (this.batch.length >= this.batchSize) {
      await this.flush();
    }
  }

  /**
   * Returns a promise that resolves when the batch containing the most recently
   * enqueued item has been flushed. Multiple events can await the same promise,
   * allowing batching while ensuring durability before ack.
   */
  async waitForFlush(): Promise<void> {
    // If batch is empty, there's nothing to wait for
    if (this.batch.length === 0 && !this.pendingFlushPromise) {
      return;
    }
    // If there's a pending flush promise, await it
    if (this.pendingFlushPromise) {
      await this.pendingFlushPromise;
    }
  }

  async flush() {
    if (this.batch.length === 0) {
      // Resolve any pending flush promise even if batch is empty
      if (this.pendingFlushResolver) {
        this.pendingFlushResolver();
        this.pendingFlushResolver = null;
        this.pendingFlushPromise = null;
      }
      return;
    }

    const rows = this.batch;
    this.batch = [];

    // Capture the resolver before clearing it
    const resolver = this.pendingFlushResolver;
    this.pendingFlushResolver = null;
    this.pendingFlushPromise = null;

    try {
      // Perform the actual flush
      const inserted = await insertEmojiRows(this.pool, rows);
      if (inserted) {
        let totalInserted = 0;
        for (const [repoDid, count] of inserted) {
          const current = this.insertedCountByRepo.get(repoDid) ?? 0;
          this.insertedCountByRepo.set(repoDid, current + count);
          totalInserted += count;
        }
        // Update metrics
        timescaleRows.inc(totalInserted);
      }
    } finally {
      // CRITICAL: Always resolve the promise, even if flush failed
      // This prevents all pending waitForFlush() calls from hanging forever
      // The error will still propagate, but events can ack and retry
      if (resolver) {
        resolver();
      }
    }
  }

  async close() {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = undefined;
    }
    await this.flush();
    await this.parquet.close();
  }

  consumeParquetCount(repoDid: string) {
    const count = this.parquetCountByRepo.get(repoDid) ?? 0;
    this.parquetCountByRepo.delete(repoDid);
    return count;
  }

  getCurrentSnapshotPath() {
    return this.parquet.filePath;
  }

  consumeInsertedCount(repoDid: string) {
    const count = this.insertedCountByRepo.get(repoDid) ?? 0;
    this.insertedCountByRepo.delete(repoDid);
    return count;
  }

  resetRepo(repoDid: string) {
    this.parquetCountByRepo.delete(repoDid);
    this.insertedCountByRepo.delete(repoDid);
  }
}
