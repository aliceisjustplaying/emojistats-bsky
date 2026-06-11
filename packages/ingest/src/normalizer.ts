import { parse as parseTid } from '@atcute/tid';
import { batchNormalizeEmojis } from 'emoji-normalization';
import emojiRegexFunc from 'emoji-regex';

import {
  CREATED_AT_FUTURE_SLACK_MS,
  EMOJI_MAX_PER_POST,
  MIN_CREATED_AT_MS,
} from './config.js';
import type { Anomaly, NormalizedPost, RawPostEvent } from './types.js';

// Shared instance is safe: String#match with /g resets lastIndex; matches backend usage exactly.
const emojiRegex: RegExp = emojiRegexFunc();

/**
 * Resolve event time, preferring the record's createdAt, then the rkey TID, then receive time.
 * Anything outside [MIN_CREATED_AT_MS, receive + slack] is treated as a lie and skipped.
 */
function resolveCreatedAt(event: RawPostEvent, anomalies: Anomaly[]): Date {
  const receiveMs = event.timeUs / 1000;
  const maxMs = receiveMs + CREATED_AT_FUTURE_SLACK_MS;

  if (event.createdAt !== undefined) {
    const parsedMs = Date.parse(event.createdAt);
    if (
      Number.isFinite(parsedMs) &&
      parsedMs >= MIN_CREATED_AT_MS &&
      parsedMs <= maxMs
    ) {
      return new Date(parsedMs);
    }
  }

  try {
    // TID timestamps are epoch microseconds. rkeys are not guaranteed to be TIDs, hence the catch.
    const tidMs = parseTid(event.rkey).timestamp / 1000;
    if (tidMs >= MIN_CREATED_AT_MS && tidMs <= maxMs) {
      anomalies.push('createdat-tid-fallback');
      return new Date(tidMs);
    }
  } catch {
    // fall through to receive time
  }

  anomalies.push('createdat-receive-fallback');
  return new Date(receiveMs);
}

function normalizeLangs(langs: string[] | undefined): string[] {
  const distinct = new Set<string>();
  for (const lang of langs ?? []) {
    if (typeof lang !== 'string') continue;
    const trimmed = lang.trim();
    if (trimmed !== '') distinct.add(trimmed);
  }
  return distinct.size > 0 ? [...distinct] : ['unknown'];
}

export function normalizePost(event: RawPostEvent): NormalizedPost {
  const anomalies: Anomaly[] = [];

  const createdAt = resolveCreatedAt(event, anomalies);
  const langs = normalizeLangs(event.langs);

  const matches = event.text.match(emojiRegex) ?? [];
  let emojis = batchNormalizeEmojis(matches);
  if (emojis.length > EMOJI_MAX_PER_POST) {
    emojis = emojis.slice(0, EMOJI_MAX_PER_POST);
    anomalies.push('emoji-truncated');
  }

  return {
    did: event.did,
    rkey: event.rkey,
    createdAt,
    text: event.text,
    langs,
    emojis,
    anomalies,
  };
}
