import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { shouldDropRetainedBacklog } from './scheduler.js';

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
