import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { createRaw } from '@atcute/tid';

import { CREATED_AT_FUTURE_SLACK_MS, EMOJI_MAX_PER_POST } from './config.js';
import { normalizePost } from './normalizer.js';
import type { RawPostEvent } from './types.js';

const RECEIVE_MS = Date.UTC(2025, 0, 15, 12, 0, 0);

// Default rkey is deliberately not a valid TID so createdAt tests exercise exactly the path they name.
function makeEvent(overrides: Partial<RawPostEvent> = {}): RawPostEvent {
  return {
    did: 'did:plc:test',
    rkey: 'not-a-tid!',
    text: '',
    timeUs: RECEIVE_MS * 1000,
    createdAt: new Date(RECEIVE_MS - 5_000).toISOString(),
    ...overrides,
  };
}

void describe('emojis', () => {
  void it('extracts a plain emoji', () => {
    const post = normalizePost(makeEvent({ text: 'hello 😀 world' }));
    assert.deepEqual(post.emojis, ['😀']);
    assert.deepEqual(post.anomalies, []);
  });

  void it('normalizes text-style and emoji-style hearts to the same glyph', () => {
    const textStyle = normalizePost(makeEvent({ text: 'I ❤ you' }));
    const emojiStyle = normalizePost(makeEvent({ text: 'I ❤️ you' }));
    assert.deepEqual(textStyle.emojis, ['❤️']);
    assert.deepEqual(textStyle.emojis, emojiStyle.emojis);
  });

  void it('keeps a skin-tone modifier sequence as one glyph', () => {
    const post = normalizePost(makeEvent({ text: '\u{1F44D}\u{1F3FD}' }));
    assert.deepEqual(post.emojis, ['\u{1F44D}\u{1F3FD}']);
  });

  void it('keeps a ZWJ sequence as one glyph', () => {
    const family = '\u{1F468}‍\u{1F469}‍\u{1F467}‍\u{1F466}';
    const post = normalizePost(makeEvent({ text: `our ${family}` }));
    assert.deepEqual(post.emojis, [family]);
  });

  void it('keeps repeated emoji as separate occurrences', () => {
    const post = normalizePost(makeEvent({ text: '😀 and 😀' }));
    assert.deepEqual(post.emojis, ['😀', '😀']);
  });

  void it('returns [] for emoji-less text', () => {
    const post = normalizePost(makeEvent({ text: 'just words :) <3' }));
    assert.deepEqual(post.emojis, []);
  });

  void it('truncates past EMOJI_MAX_PER_POST and flags the anomaly', () => {
    const post = normalizePost(
      makeEvent({ text: '😀'.repeat(EMOJI_MAX_PER_POST + 10) }),
    );
    assert.equal(post.emojis.length, EMOJI_MAX_PER_POST);
    assert.ok(post.emojis.every((e) => e === '😀'));
    assert.deepEqual(post.anomalies, ['emoji-truncated']);
  });
});

void describe('langs', () => {
  void it('falls back to [unknown] when langs is missing', () => {
    assert.deepEqual(normalizePost(makeEvent({ langs: undefined })).langs, [
      'unknown',
    ]);
  });

  void it('falls back to [unknown] when langs is empty or all blank', () => {
    assert.deepEqual(normalizePost(makeEvent({ langs: [] })).langs, [
      'unknown',
    ]);
    assert.deepEqual(normalizePost(makeEvent({ langs: ['', '  '] })).langs, [
      'unknown',
    ]);
  });

  void it('trims, drops empties, and dedupes', () => {
    const post = normalizePost(makeEvent({ langs: ['en', ' en ', '', 'ja'] }));
    assert.deepEqual(post.langs, ['en', 'ja']);
  });
});

void describe('createdAt', () => {
  void it('uses a valid in-window createdAt with no anomaly', () => {
    const createdMs = RECEIVE_MS - 60_000;
    const post = normalizePost(
      makeEvent({ createdAt: new Date(createdMs).toISOString() }),
    );
    assert.equal(post.createdAt.getTime(), createdMs);
    assert.deepEqual(post.anomalies, []);
  });

  void it('falls back to the rkey TID when createdAt is unparseable', () => {
    const tidMs = RECEIVE_MS - 30_000;
    const post = normalizePost(
      makeEvent({ createdAt: 'garbage', rkey: createRaw(tidMs * 1000, 0) }),
    );
    assert.equal(post.createdAt.getTime(), tidMs);
    assert.deepEqual(post.anomalies, ['createdat-tid-fallback']);
  });

  void it('falls back to the rkey TID when createdAt is missing', () => {
    const tidMs = RECEIVE_MS - 30_000;
    const post = normalizePost(
      makeEvent({ createdAt: undefined, rkey: createRaw(tidMs * 1000, 0) }),
    );
    assert.equal(post.createdAt.getTime(), tidMs);
    assert.deepEqual(post.anomalies, ['createdat-tid-fallback']);
  });

  void it('falls back to receive time when createdAt is invalid and the rkey is not a TID', () => {
    const post = normalizePost(makeEvent({ createdAt: 'garbage' }));
    assert.equal(post.createdAt.getTime(), RECEIVE_MS);
    assert.deepEqual(post.anomalies, ['createdat-receive-fallback']);
  });

  void it('rejects createdAt beyond the future slack', () => {
    const future = new Date(
      RECEIVE_MS + CREATED_AT_FUTURE_SLACK_MS + 60_000,
    ).toISOString();
    const post = normalizePost(makeEvent({ createdAt: future }));
    assert.equal(post.createdAt.getTime(), RECEIVE_MS);
    assert.deepEqual(post.anomalies, ['createdat-receive-fallback']);
  });

  void it('accepts createdAt within the future slack', () => {
    const createdMs = RECEIVE_MS + CREATED_AT_FUTURE_SLACK_MS - 60_000;
    const post = normalizePost(
      makeEvent({ createdAt: new Date(createdMs).toISOString() }),
    );
    assert.equal(post.createdAt.getTime(), createdMs);
    assert.deepEqual(post.anomalies, []);
  });

  void it('rejects an out-of-window TID and falls back to receive time', () => {
    // TID claims a timestamp 10 days past receive: outside the slack window.
    const tidMs = RECEIVE_MS + 10 * 86_400_000;
    const post = normalizePost(
      makeEvent({ createdAt: 'garbage', rkey: createRaw(tidMs * 1000, 0) }),
    );
    assert.equal(post.createdAt.getTime(), RECEIVE_MS);
    assert.deepEqual(post.anomalies, ['createdat-receive-fallback']);
  });
});
