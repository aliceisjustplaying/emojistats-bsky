import { parse as parseTid } from "@atcute/tid";
import emojiRegexFactory from "emoji-regex";
import { batchNormalizeEmojis } from "emoji-normalization";
import type { NormalizedEmojiPost } from "./types.js";
import { logger } from "./logger.js";

const emojiRegex = emojiRegexFactory();

const MAX_INVALID_TID_WARNINGS = 20;
let invalidTidWarningCount = 0;

export function normalizeRepoRecord(params: {
  did: string;
  collection: string;
  rkey: string;
  cid: string;
  record: unknown;
}): NormalizedEmojiPost | null {
  if (params.collection !== "app.bsky.feed.post") return null;
  const { did, collection, rkey, cid, record } = params;
  if (!record || typeof record !== "object") return null;
  const text =
    typeof (record as any).text === "string" ? (record as any).text : "";
  if (!text) return null;
  const emojiMatches = text.match(emojiRegex) ?? [];
  if (emojiMatches.length === 0) return null;
  const normalizedEmojis = batchNormalizeEmojis(emojiMatches).filter(
    (e) => e && e.trim().length > 0,
  );
  if (normalizedEmojis.length === 0) return null;

  const resolved = resolveCreatedAt(record, rkey, did);
  if (!resolved) return null;
  const { createdAt, seq } = resolved;
  const langCodes = extractLangs(record);

  return {
    repoDid: did,
    authorDid: did,
    collection,
    rkey,
    cid,
    postUri: `at://${did}/${collection}/${rkey}`,
    seq,
    createdAt,
    receivedAt: new Date(),
    langCodes,
    primaryLang: langCodes[0] ?? "und",
    clientIdentifier: null,
    replyRootUri: extractReplyUri(record, "root"),
    replyParentUri: extractReplyUri(record, "parent"),
    text,
    emojiGlyphs: normalizedEmojis,
  };
}

function resolveCreatedAt(
  record: any,
  rkey: string,
  did: string,
): { createdAt: Date; seq: number } | null {
  const candidate =
    typeof record?.createdAt === "string" ? new Date(record.createdAt) : null;
  if (candidate && !Number.isNaN(candidate.getTime())) {
    return { createdAt: candidate, seq: candidate.getTime() * 1000 };
  }
  try {
    const tid = parseTid(rkey);
    return { createdAt: new Date(tid.timestamp), seq: tid.timestamp };
  } catch (error) {
    if (invalidTidWarningCount < MAX_INVALID_TID_WARNINGS) {
      logger.warn({ did, rkey, err: error }, "Invalid TID encountered");
      invalidTidWarningCount++;
    }
    return null;
  }
}

function extractLangs(record: any): string[] {
  if (Array.isArray(record?.langs) && record.langs.length > 0) {
    return record.langs.map((lang: unknown) =>
      typeof lang === "string" && lang.length > 0 ? lang.toLowerCase() : "und",
    );
  }
  return ["und"];
}

function extractReplyUri(record: any, key: "root" | "parent"): string | null {
  const uri = record?.reply?.[key]?.uri;
  return typeof uri === "string" ? uri : null;
}
