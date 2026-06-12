import assert from 'node:assert/strict';
import { describe, it, mock } from 'node:test';

import { createHostPressure } from './host-pressure.js';

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
});
