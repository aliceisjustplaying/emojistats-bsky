import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import Database from 'better-sqlite3';

import {
  promoteLoadedReposByDid,
  promotionResultListSql,
} from './verify-promotion.js';

void describe('verifier promotion policy', () => {
  void it('allows loose only on the explicit loose promotion path', () => {
    assert.equal(promotionResultListSql(false), "('exact')");
    assert.equal(promotionResultListSql(true), "('exact', 'loose')");
  });

  void it('promotes only staged loaded DIDs present in the allowlist', () => {
    const db = new Database(':memory:');
    db.exec(`
      CREATE TABLE repos (
        did TEXT PRIMARY KEY,
        status TEXT NOT NULL
      );
      CREATE TEMP TABLE verify_staged_loaded (
        did TEXT PRIMARY KEY
      ) WITHOUT ROWID;
      INSERT INTO repos (did, status) VALUES
        ('did:plc:exact', 'loaded'),
        ('did:plc:mismatch', 'loaded'),
        ('did:plc:unstaged', 'loaded'),
        ('did:plc:already', 'verified');
      INSERT INTO verify_staged_loaded (did) VALUES
        ('did:plc:exact'),
        ('did:plc:mismatch'),
        ('did:plc:already');
    `);

    const promoted = promoteLoadedReposByDid(db, ['did:plc:exact']);
    const rows = db
      .prepare('SELECT did, status FROM repos ORDER BY did')
      .all() as Array<{ did: string; status: string }>;

    assert.equal(promoted, 1);
    assert.deepEqual(rows, [
      { did: 'did:plc:already', status: 'verified' },
      { did: 'did:plc:exact', status: 'verified' },
      { did: 'did:plc:mismatch', status: 'loaded' },
      { did: 'did:plc:unstaged', status: 'loaded' },
    ]);
    db.close();
  });
});
