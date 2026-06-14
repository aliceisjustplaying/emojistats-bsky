import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import Database from 'better-sqlite3';

import {
  createLoadedPromotionStage,
  type LoadedPromotionStage,
  promoteLoadedReposByDid,
  promotionResultListSql,
  stageLoadedReposForPromotion,
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
      INSERT INTO repos (did, status) VALUES
        ('did:plc:exact', 'loaded'),
        ('did:plc:mismatch', 'loaded'),
        ('did:plc:unstaged', 'loaded'),
        ('did:plc:already', 'verified');
    `);
    const stage = createLoadedPromotionStage(db);
    stageLoadedReposForPromotion(db, stage, [
      'did:plc:exact',
      'did:plc:mismatch',
      'did:plc:already',
    ]);

    const promoted = promoteLoadedReposByDid(db, ['did:plc:exact'], stage);
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

  void it('requires an explicit valid staging table boundary', () => {
    const db = new Database(':memory:');
    db.exec(`
      CREATE TABLE repos (
        did TEXT PRIMARY KEY,
        status TEXT NOT NULL
      );
    `);
    const stage: LoadedPromotionStage = {
      stagedLoadedTable: 'verify_staged_loaded; DROP TABLE repos',
    };

    assert.throws(
      () => promoteLoadedReposByDid(db, [], stage),
      /Unsafe sqlite identifier/,
    );
    db.close();
  });
});
