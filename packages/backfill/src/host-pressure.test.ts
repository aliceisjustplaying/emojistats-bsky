import assert from 'node:assert/strict';
import { describe, it, mock } from 'node:test';

import { createHostPressure, hostCapFor } from './host-pressure.js';

void describe('host pressure', () => {
  void it('counts concurrent 429s as one cooldown burst', () => {
    const now = mock.method(Date, 'now', () => 1_000_000);
    try {
      const pressure = createHostPressure();
      pressure.record429('morel.us-east.host.bsky.network');
      const firstWake = pressure.nextWake();

      for (let i = 0; i < 20; i += 1)
        pressure.record429('morel.us-east.host.bsky.network');

      assert.equal(pressure.nextWake(), firstWake);
      assert.equal(pressure.isCooling('morel.us-east.host.bsky.network'), true);

      now.mock.mockImplementation(() => 1_031_000);
      assert.equal(
        pressure.isCooling('morel.us-east.host.bsky.network'),
        false,
      );
    } finally {
      now.mock.restore();
    }
  });

  void it('AIMD: 429 bursts halve the cap, sustained successes raise it back', () => {
    let clock = 1_000_000;
    const now = mock.method(Date, 'now', () => clock);
    try {
      const host = 'morel.us-east.host.bsky.network';
      const staticCap = hostCapFor(host);
      const pressure = createHostPressure();
      assert.equal(pressure.effectiveCap(host), staticCap);

      pressure.record429(host); // one burst → half
      assert.equal(pressure.effectiveCap(host), Math.floor(staticCap / 2));

      clock += 10_000; // past the 5s burst window, within strike decay
      pressure.record429(host); // second burst → quarter
      assert.equal(pressure.effectiveCap(host), Math.floor(staticCap / 4));

      // Additive recovery: +1 per 20 successes, never past the static cap.
      clock += 10_000;
      for (let i = 0; i < 40; i += 1) pressure.recordSuccess(host);
      assert.equal(pressure.effectiveCap(host), Math.floor(staticCap / 4) + 2);

      // 10 quiet minutes: full amnesty.
      clock += 600_000;
      pressure.recordSuccess(host);
      assert.equal(pressure.effectiveCap(host), staticCap);
    } finally {
      now.mock.restore();
    }
  });

  void it('cap floors at 1 instead of going dark', () => {
    let clock = 1_000_000;
    const now = mock.method(Date, 'now', () => clock);
    try {
      const pressure = createHostPressure();
      for (let i = 0; i < 12; i += 1) {
        pressure.record429('tiny.example');
        clock += 130_000; // outside each cooldown, inside nothing — fresh bursts
      }
      assert.equal(pressure.effectiveCap('tiny.example'), 1);
      assert.equal(pressure.isCooling('tiny.example'), false);
    } finally {
      now.mock.restore();
    }
  });
});
