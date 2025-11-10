import { CommitCreateEvent } from "@skyware/jetstream";
import emojiRegexFactory from "emoji-regex";
import { batchNormalizeEmojis } from "emoji-normalization";
import { parse as parseTid, validate as validateTid } from "@atcute/tid";
import type { NormalizedPost } from "./types.js";

const emojiRegex = emojiRegexFactory();
const MAX_INVALID_WARNINGS = 20;
let invalidTidWarnings = 0;

export function normalizeEvent(
  event: CommitCreateEvent<"app.bsky.feed.post">,
): NormalizedPost | null {
  const { did } = event;
  const record = event.commit.record as any;
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

  const { createdAt, seq } = resolveTimestamps(record, event.commit.rkey, did);
  if (!createdAt) return null;

  return {
    repoDid: did,
    authorDid: did,
    rkey: event.commit.rkey,
    postUri: `at://${did}/app.bsky.feed.post/${event.commit.rkey}`,
    seq,
    createdAt,
    receivedAt: new Date(),
    langCodes,
    primaryLang,
    clientIdentifier: typeof record?.app === "string" ? record.app : null,
    replyRootUri: extractReplyUri(record, "root"),
    replyParentUri: extractReplyUri(record, "parent"),
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

function resolveTimestamps(
  record: any,
  rkey: string,
  did: string,
): {
  createdAt: Date | null;
  seq: number;
} {
  const createdAtField =
    typeof record?.createdAt === "string" ? new Date(record.createdAt) : null;
  if (createdAtField && !Number.isNaN(createdAtField.getTime())) {
    return { createdAt: createdAtField, seq: createdAtField.getTime() * 1000 };
  }
  try {
    if (validateTid(rkey)) {
      const tid = parseTid(rkey);
      return { createdAt: new Date(tid.timestamp), seq: tid.timestamp };
    }
  } catch {}
  if (invalidTidWarnings < MAX_INVALID_WARNINGS) {
    console.warn(`Live ingest invalid rkey for ${did} rkey=${rkey}`);
    invalidTidWarnings++;
  }
  return { createdAt: new Date(), seq: Date.now() * 1000 };
}

function extractReplyUri(record: any, key: "root" | "parent"): string | null {
  const uri = record?.reply?.[key]?.uri;
  return typeof uri === "string" ? uri : null;
}
