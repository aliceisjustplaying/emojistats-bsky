import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { ClickHouseClient } from '@clickhouse/client';
import type { PostRow } from 'ingest/types';

import { ClickHouseRepoLoader } from './loader.js';

function row(did: string, rkey: string): PostRow {
  return {
    did,
    rkey,
    created_at: '2026-01-01T00:00:00.000Z',
    text: '',
    langs: [],
    emojis: [],
    src: 'backfill',
  };
}

// A permanent ClickHouse error short-circuits the insert retry loop, so flushes
// fail immediately without the multi-second backoff (keeps the test fast).
function permanentInsertError(): Error {
  return Object.assign(new Error('insert boom'), { code: '53' });
}

void describe('ClickHouseRepoLoader durability', () => {
  void it('resolves finish() once the batch insert succeeds', async () => {
    let inserted = 0;
    const client = {
      insert: async () => {
        inserted += 1;
      },
    } as unknown as ClickHouseClient;

    const loader = new ClickHouseRepoLoader(client, {
      batchRows: 10,
      flushMs: 5,
    });
    const repo = loader.openRepo('did:A', 'rev');
    await repo.addRow(row('did:A', 'a1'));
    await repo.finish();

    assert.equal(inserted, 1);
  });

  void it('rejects finish() when the live-buffer flush fails', async () => {
    const client = {
      insert: async () => {
        throw permanentInsertError();
      },
    } as unknown as ClickHouseClient;

    const loader = new ClickHouseRepoLoader(client, {
      batchRows: 10,
      flushMs: 5,
    });
    const repo = loader.openRepo('did:A', 'rev');
    await repo.addRow(row('did:A', 'a1'));
    await assert.rejects(repo.finish(), /ClickHouse batch insert failed/);
  });

  // The regression guard: a generation whose flush FAILED must still reject a
  // late finish() even after many later generations have flushed past it. The
  // old time-windowed eviction dropped the failed generation's outcome after
  // GEN_RETENTION (128) swaps, so a repo that finished later saw "no entry" and
  // was wrongly marked durable despite its rows never landing in ClickHouse.
  void it('rejects a late finish() over a failed, swapped-past generation', async () => {
    let calls = 0;
    const client = {
      insert: async () => {
        calls += 1;
        // Only the very first flush (generation 0, holding A's row) fails.
        if (calls === 1) throw permanentInsertError();
      },
    } as unknown as ClickHouseClient;

    const loader = new ClickHouseRepoLoader(client, {
      batchRows: 2,
      flushMs: 1_000_000, // effectively disable the timer; drive flushes by size
    });

    // A puts one row in generation 0 but does not fill the batch yet.
    const a = loader.openRepo('did:A', 'rev');
    await a.addRow(row('did:A', 'a1'));

    // B's row fills generation 0's batch and triggers its (failing) flush; the
    // size-triggered flush surfaces the error through B's addRow.
    const b = loader.openRepo('did:B', 'rev');
    await assert.rejects(
      b.addRow(row('did:B', 'b1')),
      /ClickHouse batch insert failed/,
    );

    // Drive well past the old 128-generation retention window with successful
    // flushes, so the failed generation 0 would have been evicted under the
    // old eviction policy.
    for (let i = 0; i < 130; i += 1) {
      const filler = loader.openRepo(`did:F${i}`, 'rev');
      await filler.addRow(row(`did:F${i}`, 'x'));
      await filler.addRow(row(`did:F${i}`, 'y'));
      await filler.finish();
    }

    // A touched only the failed generation 0: its finish() MUST reject so the
    // pipeline parks the repo retryable instead of marking it loaded.
    await assert.rejects(a.finish(), /ClickHouse batch insert failed/);
  });
});
