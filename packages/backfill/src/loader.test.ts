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

  // A poisoned socket pool (stale keepalive → ECONNRESET on every insert) must
  // not jam forever: a connection-level failure rebuilds the client so the retry
  // runs on a fresh pool and the flush succeeds. The injected client is NOT the
  // loader's to close (clientIsOwn starts false), so it needs no close().
  void it('rebuilds the client on a connection error, then the retry succeeds', async () => {
    let rebuilds = 0;
    const dead = {
      insert: async () => {
        throw Object.assign(new Error('socket hang up'), {
          code: 'ECONNRESET',
        });
      },
    } as unknown as ClickHouseClient;
    let freshInserts = 0;
    const fresh = {
      insert: async () => {
        freshInserts += 1;
      },
      close: async () => undefined,
    } as unknown as ClickHouseClient;

    const loader = new ClickHouseRepoLoader(dead, {
      batchRows: 10,
      flushMs: 5,
      recreateClient: () => {
        rebuilds += 1;
        return fresh;
      },
    });
    const repo = loader.openRepo('did:A', 'rev');
    await repo.addRow(row('did:A', 'a1'));
    await repo.finish(); // resolves only if the rebuilt client's insert lands

    assert.equal(rebuilds, 1);
    assert.equal(freshInserts, 1);
  });

  // The incident's exact symptom was a code-less error whose MESSAGE is
  // "Timeout exceeded while reading from socket (... 30000 ms)". It must be
  // classified as a connection error (regex path, no err.code) and rebuild.
  void it('treats the code-less "reading from socket" timeout as a connection error', async () => {
    let rebuilds = 0;
    const dead = {
      insert: async () => {
        throw new Error(
          'Timeout exceeded while reading from socket (peer: 1.2.3.4:8123, local: 5.6.7.8:9000, 30000 ms).',
        );
      },
    } as unknown as ClickHouseClient;
    let freshInserts = 0;
    const fresh = {
      insert: async () => {
        freshInserts += 1;
      },
      close: async () => undefined,
    } as unknown as ClickHouseClient;

    const loader = new ClickHouseRepoLoader(dead, {
      batchRows: 10,
      flushMs: 5,
      recreateClient: () => {
        rebuilds += 1;
        return fresh;
      },
    });
    const repo = loader.openRepo('did:A', 'rev');
    await repo.addRow(row('did:A', 'a1'));
    await repo.finish();

    assert.equal(rebuilds, 1);
    assert.equal(freshInserts, 1);
  });

  // Multi-rebuild ownership: the injected client is NEVER closed by the loader
  // (the caller's at shutdown); each superseded REBUILT client is closed on the
  // next rebuild; the final rebuilt client is closed by loader.close().
  void it('closes superseded rebuilt clients but never the injected one', async () => {
    const closed: string[] = [];
    const connErr = () =>
      Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' });
    const mk = (name: string, insert: () => Promise<void>) =>
      ({
        insert,
        close: async () => {
          closed.push(name);
        },
      }) as unknown as ClickHouseClient;

    const injected = mk('injected', async () => {
      throw connErr();
    });
    const r1 = mk('r1', async () => {
      throw connErr();
    });
    let r2inserts = 0;
    const r2 = mk('r2', async () => {
      r2inserts += 1;
    });
    const queue = [r1, r2];

    const loader = new ClickHouseRepoLoader(injected, {
      batchRows: 10,
      flushMs: 5,
      recreateClient: () => queue.shift()!,
    });
    const repo = loader.openRepo('did:A', 'rev');
    await repo.addRow(row('did:A', 'a1'));
    await repo.finish(); // injected→r1 (no close), r1→r2 (close r1), r2 succeeds

    assert.equal(r2inserts, 1);
    await loader.close(); // closes the final owned client (r2)
    // Let the background close() of r1 settle.
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.ok(closed.includes('r1'), 'superseded rebuilt client r1 closed');
    assert.ok(
      closed.includes('r2'),
      'final rebuilt client r2 closed on loader.close()',
    );
    assert.ok(
      !closed.includes('injected'),
      'injected client never closed by loader',
    );
  });

  // A *server* error (permanent or transient-non-connection) must NOT rebuild —
  // the socket is fine, only the payload/server is unhappy.
  void it('does not rebuild the client on a ClickHouse server error', async () => {
    let rebuilds = 0;
    const client = {
      insert: async () => {
        throw permanentInsertError();
      },
    } as unknown as ClickHouseClient;

    const loader = new ClickHouseRepoLoader(client, {
      batchRows: 10,
      flushMs: 5,
      recreateClient: () => {
        rebuilds += 1;
        return client;
      },
    });
    const repo = loader.openRepo('did:A', 'rev');
    await repo.addRow(row('did:A', 'a1'));
    await assert.rejects(repo.finish(), /ClickHouse batch insert failed/);

    assert.equal(rebuilds, 0);
  });
});
