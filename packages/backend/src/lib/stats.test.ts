import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { formatEmojiPostRatio } from './stats.js';

void describe('formatEmojiPostRatio', () => {
  void it('formats the share of total posts that contain at least one emoji', () => {
    assert.equal(formatEmojiPostRatio(2_280_932_032, 420_050_497), '0.1842');
  });

  void it('does not divide posts with emojis by posts without emojis', () => {
    assert.notEqual(formatEmojiPostRatio(2_280_932_032, 420_050_497), '0.2257');
  });

  void it('returns N/A before any posts have been counted', () => {
    assert.equal(formatEmojiPostRatio(0, 0), 'N/A');
  });
});
