import type { Pool } from "pg";
import { insertEmojiRows } from "./db.js";
import { DimensionCache } from "./dimensions.js";
import { ParquetSink } from "./parquetSink.js";
import type { NormalizedEmojiPost, PreparedEmojiRow } from "./types.js";

export class EmojiPostWriter {
  private batch: PreparedEmojiRow[] = [];

  constructor(
    private readonly pool: Pool,
    private readonly dimensions: DimensionCache,
    private readonly parquet: ParquetSink,
    private readonly batchSize = 500,
  ) {}

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

    if (this.batch.length >= this.batchSize) {
      await this.flush();
    }
  }

  async flush() {
    if (this.batch.length === 0) return;
    const rows = this.batch;
    this.batch = [];
    await insertEmojiRows(this.pool, rows);
  }

  async close() {
    await this.flush();
    await this.parquet.close();
  }
}
