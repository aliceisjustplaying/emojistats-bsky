import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';

import { withProgressTimeout } from './fetcher.js';

void describe('withProgressTimeout', () => {
  void it('passes through a value that arrives before the deadline', async () => {
    let aborted = false;
    const value = await withProgressTimeout(
      Promise.resolve(42),
      1_000,
      'body',
      () => {
        aborted = true;
      },
    );
    assert.equal(value, 42);
    assert.equal(aborted, false);
  });

  void it('propagates a rejection that arrives before the deadline', async () => {
    await assert.rejects(
      withProgressTimeout(
        Promise.reject(new Error('upstream boom')),
        1_000,
        'body',
        () => undefined,
      ),
      /upstream boom/,
    );
  });

  // The wedge guard: a promise that never settles (a half-open socket's hung
  // read) MUST be rejected by our own timer and MUST fire onTimeout, so the
  // caller's concurrency slot is freed instead of leaking.
  void it('rejects a never-settling promise and fires the abort', async () => {
    let aborted = false;
    const never = new Promise<never>(() => {
      /* never settles */
    });
    await assert.rejects(
      withProgressTimeout(never, 20, 'body', () => {
        aborted = true;
      }),
      /stalled: no progress for 20ms during body/,
    );
    assert.equal(aborted, true);
  });

  // A late rejection from the hung promise (e.g. an AbortError that arrives
  // after the timeout already won) must not surface as an unhandled rejection.
  void it('swallows a late rejection from the losing promise', async () => {
    let rejectLate!: (err: Error) => void;
    const late = new Promise<never>((_resolve, reject) => {
      rejectLate = reject;
    });
    await assert.rejects(
      withProgressTimeout(late, 20, 'connect/headers', () => undefined),
      /stalled: no progress/,
    );
    // Reject the wrapped promise after the race already settled; Promise.race's
    // internal handler absorbs it. If it were unhandled, the test runner would
    // flag an unhandledRejection.
    rejectLate(new Error('late abort'));
    await sleep(30);
  });
});
