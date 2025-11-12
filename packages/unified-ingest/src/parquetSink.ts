import { mkdir } from "node:fs/promises";
import path from "node:path";
import { createRequire } from "node:module";
import type { NormalizedEmojiPost } from "./types.js";

const require = createRequire(import.meta.url);
const { ParquetWriter, ParquetSchema } = require("parquetjs-lite");

const parquetSchema = new ParquetSchema({
  post_uri: { type: "UTF8" },
  repo_did: { type: "UTF8" },
  author_did: { type: "UTF8" },
  rkey: { type: "UTF8" },
  cid: { type: "UTF8" },
  created_at: { type: "UTF8" },
  received_at: { type: "UTF8" },
  lang_primary: { type: "UTF8" },
  langs: { type: "UTF8", repeated: true },
  client_identifier: { type: "UTF8", optional: true },
  emoji_glyphs: { type: "UTF8", repeated: true },
  emoji_ids: { type: "INT32", repeated: true },
  emoji_count: { type: "INT32" },
  reply_root_uri: { type: "UTF8", optional: true },
  reply_parent_uri: { type: "UTF8", optional: true },
  text: { type: "UTF8", optional: true },
});

export class ParquetSink {
  private constructor(
    private readonly writer: any,
    public readonly filePath: string,
  ) {}

  static async create(outputDir: string) {
    await mkdir(outputDir, { recursive: true });
    const filename = `emoji_posts_${new Date().toISOString().replace(/[:.]/g, "-")}.parquet`;
    const filePath = path.join(outputDir, filename);
    const writer = await ParquetWriter.openFile(parquetSchema, filePath);
    return new ParquetSink(writer, filePath);
  }

  async append(record: NormalizedEmojiPost, emojiIds: number[]) {
    await this.writer.appendRow({
      post_uri: record.postUri,
      repo_did: record.repoDid,
      author_did: record.authorDid,
      rkey: record.rkey,
      cid: record.cid,
      created_at: record.createdAt.toISOString(),
      received_at: record.receivedAt.toISOString(),
      lang_primary: record.primaryLang,
      langs: record.langCodes,
      client_identifier: record.clientIdentifier ?? undefined,
      emoji_glyphs: record.emojiGlyphs,
      emoji_ids: emojiIds,
      emoji_count: record.emojiGlyphs.length,
      reply_root_uri: record.replyRootUri ?? undefined,
      reply_parent_uri: record.replyParentUri ?? undefined,
      text: record.text || undefined,
    });
  }

  async close() {
    await this.writer.close();
  }
}
