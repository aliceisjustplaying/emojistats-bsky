import assert from 'node:assert/strict';
import { describe, it, mock } from 'node:test';

import { parseRateLimitHeaders } from './fetcher.js';

void describe('rate-limit headers', () => {
  void it('parses mushroom ratelimit headers', () => {
    const now = mock.method(Date, 'now', () => 1_781_387_271_000);
    try {
      const headers = new Headers({
        'ratelimit-limit': '3000',
        'ratelimit-remaining': '2999',
        'ratelimit-reset': '1781387571',
        'ratelimit-policy': '3000;w=300',
      });

      assert.deepEqual(parseRateLimitHeaders(headers), {
        limit: 3000,
        remaining: 2999,
        resetAtMs: 1_781_387_571_000,
        windowMs: 300_000,
        retryAfterMs: undefined,
      });
    } finally {
      now.mock.restore();
    }
  });

  void it('accepts x-ratelimit aliases and delta reset seconds', () => {
    const now = mock.method(Date, 'now', () => 1_000_000);
    try {
      const headers = new Headers({
        'x-ratelimit-limit': '10',
        'x-ratelimit-remaining': '0',
        'x-ratelimit-reset': '5',
        'retry-after': '7',
      });

      assert.deepEqual(parseRateLimitHeaders(headers), {
        limit: 10,
        remaining: 0,
        resetAtMs: 1_005_000,
        windowMs: undefined,
        retryAfterMs: 7000,
      });
    } finally {
      now.mock.restore();
    }
  });
});
