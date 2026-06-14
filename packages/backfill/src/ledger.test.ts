import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { describe, it } from 'node:test';

import { SqliteLedger } from './ledger.js';

function tempLedger(): SqliteLedger {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'emojistats-ledger-'));
  return new SqliteLedger(path.join(dir, 'ledger.sqlite'));
}

void describe('SqliteLedger terminal status errors', () => {
  void it('clears stale retry errors when a repo is successfully empty', () => {
    const ledger = tempLedger();
    ledger.upsertPending('did:plc:empty', 'example.com');
    ledger.markRetry('did:plc:empty', 'old timeout', 1_000);

    ledger.markTerminal('did:plc:empty', 'empty');

    const repo = ledger.getRepo('did:plc:empty');
    assert.equal(repo?.status, 'empty');
    assert.equal(repo?.error, null);
  });

  void it('preserves the last retry error when a repo exhausts as failed', () => {
    const ledger = tempLedger();
    ledger.upsertPending('did:plc:failed', 'example.com');
    ledger.markRetry('did:plc:failed', 'diagnostic timeout', 1_000);

    ledger.markTerminal('did:plc:failed', 'failed');

    const repo = ledger.getRepo('did:plc:failed');
    assert.equal(repo?.status, 'failed');
    assert.equal(repo?.error, 'diagnostic timeout');
  });
});
