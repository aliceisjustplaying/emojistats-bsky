import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  nextClaimWakeDelay,
  shouldDropRetainedBacklog,
  shouldExcludeHostFromClaimScan,
} from './scheduler.js';

void describe('scheduler retained backlog policy', () => {
  void it('drops a huge retained tail when only a tiny batch was scheduled', () => {
    assert.equal(shouldDropRetainedBacklog(12, 150_000, 8_192), true);
  });

  void it('keeps a retained tail when the pass filled a useful batch', () => {
    assert.equal(shouldDropRetainedBacklog(512, 150_000, 8_192), false);
  });

  void it('keeps small retained tails', () => {
    assert.equal(shouldDropRetainedBacklog(12, 20_000, 8_192), false);
  });
});

void describe('scheduler claim wake delay', () => {
  void it('uses the idle delay when no host wake is known', () => {
    assert.equal(nextClaimWakeDelay(undefined, 1_000), 1_000);
  });

  void it('clamps near wakes to the scheduler floor', () => {
    assert.equal(nextClaimWakeDelay(1_010, 1_000), 250);
  });

  void it('clamps distant wakes to the scheduler ceiling', () => {
    assert.equal(nextClaimWakeDelay(20_000, 1_000), 5_000);
  });
});

void describe('scheduler claim scan host exclusion', () => {
  void it('excludes true backoff but not short rate-limit pacing', () => {
    assert.equal(shouldExcludeHostFromClaimScan(0, 16, 5_000, false), true);
    assert.equal(shouldExcludeHostFromClaimScan(0, 16, 0, false), false);
  });

  void it('excludes saturated and dead hosts', () => {
    assert.equal(shouldExcludeHostFromClaimScan(16, 16, 0, false), true);
    assert.equal(shouldExcludeHostFromClaimScan(0, 16, 0, true), true);
  });
});
