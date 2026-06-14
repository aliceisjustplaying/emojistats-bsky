import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  exactDonePathForInput,
  nextClaimWakeDelay,
  shouldDropRetainedBacklog,
  shouldExcludeHostFromClaimScan,
  shouldWaitForUnreachableRetry,
} from './scheduler.js';

function isDeadHost(host: string): boolean {
  return host === 'dead.example';
}

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

void describe('scheduler exact DID checkpoint path', () => {
  void it('scopes exact DID checkpoints by sanitized run id', () => {
    const oldRunId = process.env.BACKFILL_RUN_ID;
    process.env.BACKFILL_RUN_ID = 'loose/round:1';
    try {
      assert.equal(
        exactDonePathForInput('data/loose.dids'),
        'data/loose.dids.done.loose_round_1',
      );
    } finally {
      if (oldRunId === undefined) {
        delete process.env.BACKFILL_RUN_ID;
      } else {
        process.env.BACKFILL_RUN_ID = oldRunId;
      }
    }
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

void describe('scheduler unreachable idle policy', () => {
  void it('waits only for non-dead unreachable rows with retry budget left', () => {
    assert.equal(
      shouldWaitForUnreachableRetry(4, 'alive.example', isDeadHost),
      true,
    );
    assert.equal(
      shouldWaitForUnreachableRetry(4, 'dead.example', isDeadHost),
      false,
    );
    assert.equal(
      shouldWaitForUnreachableRetry(5, 'alive.example', isDeadHost),
      false,
    );
  });
});
