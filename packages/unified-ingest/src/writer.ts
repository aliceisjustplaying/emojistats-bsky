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
  private activeFlush: Promise<void> | null = null;

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
    while (true) {
      if (!this.activeFlush) {
        if (this.batch.length === 0) {
          return;
        }
        this.activeFlush = this.flushOnce();
      }

      await this.activeFlush;

      if (this.batch.length === 0) {
        return;
      }
    }
  }

  async flush(): Promise<void> {
    while (true) {
      if (!this.activeFlush) {
        if (this.batch.length === 0) {
          return;
        }
        this.activeFlush = this.flushOnce();
      }

      await this.activeFlush;

      if (this.batch.length === 0) {
        return;
      }
    }
  }

  private async flushOnce(): Promise<void> {
    const rows = this.batch;
    if (rows.length === 0) {
      this.activeFlush = null;
      return;
    }
    this.batch = [];

    try {
      const inserted = await insertEmojiRows(this.pool, rows);
      if (inserted) {
        let totalInserted = 0;
        for (const [repoDid, count] of inserted) {
          const current = this.insertedCountByRepo.get(repoDid) ?? 0;
          this.insertedCountByRepo.set(repoDid, current + count);
          totalInserted += count;
        }
        timescaleRows.inc(totalInserted);
      }
    } finally {
      this.activeFlush = null;
    }
  }

  async close() {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = undefined;
    }
    await this.flush();
    if (this.activeFlush) {
      await this.activeFlush;
    }
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
