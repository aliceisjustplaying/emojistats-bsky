import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { shouldRetireWorkerAfterError } from './parse-pool.js';
import type { RepoJobError } from './parse-worker.js';

void describe('parse worker retirement policy', () => {
  void it('retires workers after typed stall retry failures', () => {
    const stall: RepoJobError = {
      kind: 'retryable',
      message: 'stalled: no progress for 60000ms during body',
      transient: true,
      retryAfterMs: undefined,
      rateLimit: undefined,
    };
    const ordinaryRetry: RepoJobError = {
      kind: 'retryable',
      message: 'socket hang up',
      transient: true,
      retryAfterMs: undefined,
      rateLimit: undefined,
    };
    const terminal: RepoJobError = {
      kind: 'terminal',
      message: 'getRepo failed',
      status: 'failed',
      rateLimit: undefined,
    };

    assert.equal(shouldRetireWorkerAfterError(stall), true);
    assert.equal(shouldRetireWorkerAfterError(ordinaryRetry), false);
    assert.equal(shouldRetireWorkerAfterError(terminal), false);
  });
});
