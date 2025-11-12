import emojiRegexFactory from "emoji-regex";
import { batchNormalizeEmojis } from "emoji-normalization";
import { parse as parseTid, validate as validateTid } from "@atcute/tid";
import type { UnifiedEvent } from "./adapters/types.js";
import type { NormalizedEmojiPost } from "./types.js";

const emojiRegex = emojiRegexFactory();
const MAX_INVALID_WARNINGS = 20;
let invalidTidWarnings = 0;

export function normalizeUnifiedEvent(
  event: UnifiedEvent,
): NormalizedEmojiPost | null {
  if (event.collection !== "app.bsky.feed.post") return null;

  const record = event.record as any;
  if (!record || typeof record !== "object") return null;

  const text = typeof record.text === "string" ? record.text : "";
  if (!text) return null;

  const emojiMatches = text.match(emojiRegex) ?? [];
  if (emojiMatches.length === 0) return null;

  const normalizedEmojis = batchNormalizeEmojis(emojiMatches).filter(
    (glyph) => glyph && glyph.trim().length > 0,
  );
  if (normalizedEmojis.length === 0) return null;

  const langCodes = extractLanguages(record);
  const primaryLang = langCodes[0] ?? "und";

  const { createdAt, seq } = resolveTimestamps(
    record,
    event.rkey,
    event.repoDid,
  );
  if (!createdAt) return null;

  return {
    repoDid: event.repoDid,
    authorDid: event.repoDid,
    collection: event.collection,
    rkey: event.rkey,
    cid: typeof record.cid === "string" ? record.cid : "",
    postUri: `at://${event.repoDid}/${event.collection}/${event.rkey}`,
    seq,
    createdAt,
    receivedAt: event.receivedAt,
    langCodes,
    primaryLang,
    clientIdentifier: typeof record?.app === "string" ? record.app : null,
    replyRootUri: extractReplyUri(record, "root"),
    replyParentUri: extractReplyUri(record, "parent"),
    text,
    emojiGlyphs: normalizedEmojis,
  };
}

function extractLanguages(record: any): string[] {
  if (Array.isArray(record?.langs) && record.langs.length > 0) {
    return record.langs.map((lang: unknown) =>
      typeof lang === "string" && lang.length > 0 ? lang.toLowerCase() : "und",
    );
  }
  return ["und"];
}

// Valid date range: 2000-01-01 to 2100-01-01 (reasonable bounds for Bluesky posts)
const MIN_VALID_DATE = new Date("2000-01-01T00:00:00Z").getTime();
const MAX_VALID_DATE = new Date("2100-01-01T00:00:00Z").getTime();

function isValidDate(date: Date): boolean {
  const time = date.getTime();
  return (
    !Number.isNaN(time) &&
    time >= MIN_VALID_DATE &&
    time <= MAX_VALID_DATE &&
    date.toISOString().match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/) !== null
  );
}

function resolveTimestamps(
  record: any,
  rkey: string,
  did: string,
): {
  createdAt: Date | null;
  seq: number;
} {
  // Try createdAt field first
  const createdAtField =
    typeof record?.createdAt === "string" ? new Date(record.createdAt) : null;
  if (createdAtField && isValidDate(createdAtField)) {
    return { createdAt: createdAtField, seq: createdAtField.getTime() * 1000 };
  }

  // Fall back to TID parsing
  try {
    if (validateTid(rkey)) {
      const tid = parseTid(rkey);
      const tidDate = new Date(tid.timestamp);
      if (isValidDate(tidDate)) {
        return { createdAt: tidDate, seq: tid.timestamp };
      }
    }
  } catch {}

  // Log invalid timestamp for investigation
  if (invalidTidWarnings < MAX_INVALID_WARNINGS) {
    const invalidValue = record?.createdAt ?? rkey;
    console.warn(
      `Invalid timestamp for ${did} rkey=${rkey} createdAt=${invalidValue}`,
    );
    invalidTidWarnings++;
  }

  // Last resort: use current time (but return null to signal invalid record)
  return { createdAt: null, seq: Date.now() * 1000 };
}

function extractReplyUri(record: any, key: "root" | "parent"): string | null {
  const uri = record?.reply?.[key]?.uri;
  return typeof uri === "string" ? uri : null;
}
