import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';

import type { ClickHouseClient } from '@clickhouse/client';

import { CrawlTelemetry } from './telemetry.js';

function snapshot(pending: number) {
  return {
    statusCounts: { pending },
    postsLoaded: 0,
    bytesDownloaded: 0,
    rowsPerSec: 0,
    inFlight: 0,
  };
}

async function waitFor(fn: () => boolean): Promise<void> {
  const deadline = Date.now() + 500;
  while (!fn()) {
    assert.ok(Date.now() < deadline, 'timed out waiting for condition');
    await sleep(5);
  }
}

void describe('crawl telemetry', () => {
  void it('retries progress snapshots instead of dropping them', async () => {
    let pending = 0;
    let failProgress = true;
    const progressRows: Array<{ pending: number }> = [];
    const client = {
      insert: async (params: { table: string; values: unknown[] }) => {
        if (params.table !== 'backfill_progress') return;
        if (failProgress) {
          failProgress = false;
          throw new Error('slow clickhouse');
        }
        progressRows.push(...(params.values as Array<{ pending: number }>));
      },
    } as unknown as ClickHouseClient;
    const telemetry = new CrawlTelemetry(client, {
      runId: 'test',
      shard: 'shard0',
      intervalMs: 5,
    });

    telemetry.start(() => snapshot(++pending));
    await waitFor(() => progressRows.length > 0);
    await telemetry.stop();

    assert.ok(progressRows[0].pending > 1);
  });

  void it('does not let a stuck event insert block progress snapshots', async () => {
    let releaseEvents: (() => void) | undefined;
    const progressRows: unknown[] = [];
    let eventInsertStarted = false;
    const client = {
      insert: async (params: { table: string; values: unknown[] }) => {
        if (params.table === 'backfill_progress') {
          progressRows.push(...params.values);
          return;
        }
        eventInsertStarted = true;
        await new Promise<void>((release) => {
          releaseEvents = release;
        });
      },
    } as unknown as ClickHouseClient;
    const telemetry = new CrawlTelemetry(client, {
      runId: 'test',
      shard: 'shard0',
      intervalMs: 5,
    });

    telemetry.start(() => snapshot(progressRows.length + 1));
    telemetry.recordEvent({
      did: 'did:plc:test',
      pdsHost: 'example.com',
      event: 'empty',
    });
    await waitFor(() => eventInsertStarted);
    await waitFor(() => progressRows.length >= 2);
    releaseEvents?.();
    await telemetry.stop();
  });

  void it('uses separate clients and chunks event inserts', async () => {
    const progressTables: string[] = [];
    const eventBatchSizes: number[] = [];
    const eventRows: Array<{ run_id: string; shard: string }> = [];
    const progressClient = {
      query: async () => ({
        json: async () => [],
      }),
      insert: async (params: { table: string; values: unknown[] }) => {
        progressTables.push(params.table);
      },
    } as unknown as ClickHouseClient;
    const eventClient = {
      insert: async (params: { table: string; values: unknown[] }) => {
        assert.equal(params.table, 'backfill_repo_events');
        eventBatchSizes.push(params.values.length);
        eventRows.push(
          ...(params.values as Array<{ run_id: string; shard: string }>),
        );
      },
    } as unknown as ClickHouseClient;
    const telemetry = new CrawlTelemetry(
      { progress: progressClient, events: eventClient },
      {
        runId: 'test',
        shard: 'shard0',
        intervalMs: 5,
      },
    );

    for (let i = 0; i < 1001; i += 1) {
      telemetry.recordEvent({
        did: `did:plc:${i}`,
        pdsHost: 'example.com',
        event: 'empty',
      });
    }
    telemetry.start(() => snapshot(1));
    await waitFor(() => eventBatchSizes.length === 2);
    await telemetry.stop();

    assert.deepEqual(eventBatchSizes, [1000, 1]);
    assert.equal(eventRows[0]?.run_id, 'test');
    assert.equal(eventRows[0]?.shard, 'shard0');
    assert.ok(eventRows.every((row) => row.run_id === 'test'));
    assert.ok(eventRows.every((row) => row.shard === 'shard0'));
    assert.ok(progressTables.every((table) => table === 'backfill_progress'));
  });

  void it('fails startup when backfill_repo_events is missing run scope columns', async () => {
    const telemetry = new CrawlTelemetry(
      {
        progress: {
          insert: async () => undefined,
        } as unknown as ClickHouseClient,
        events: {
          query: async (params: { query: string; format: string }) => {
            assert.equal(params.query, 'DESCRIBE TABLE backfill_repo_events');
            assert.equal(params.format, 'JSONEachRow');
            return {
              json: async () => [{ name: 'ts' }, { name: 'did' }],
            };
          },
          insert: async () => undefined,
        } as unknown as ClickHouseClient,
      },
      {
        runId: 'test',
        shard: 'shard0',
        intervalMs: 5,
      },
    );

    await assert.rejects(
      () => telemetry.assertEventColumns(),
      /missing run_id, shard/,
    );
  });

  void it('accepts backfill_repo_events with run scope columns', async () => {
    const telemetry = new CrawlTelemetry(
      {
        progress: {
          insert: async () => undefined,
        } as unknown as ClickHouseClient,
        events: {
          query: async () => ({
            json: async () => [
              { name: 'ts' },
              { name: 'run_id' },
              { name: 'shard' },
              { name: 'did' },
            ],
          }),
          insert: async () => undefined,
        } as unknown as ClickHouseClient,
      },
      {
        runId: 'test',
        shard: 'shard0',
        intervalMs: 5,
      },
    );

    await telemetry.assertEventColumns();
  });

  void it('rebuilds the progress client after a connection error', async () => {
    let oldClosed = 0;
    const replacementRows: unknown[] = [];
    const progressClient = {
      insert: async () => {
        const err = new Error('socket hang up') as Error & { code: string };
        err.code = 'ECONNRESET';
        throw err;
      },
      close: async () => {
        oldClosed += 1;
      },
    } as unknown as ClickHouseClient;
    const eventClient = {
      insert: async () => undefined,
      close: async () => undefined,
    } as unknown as ClickHouseClient;
    const telemetry = new CrawlTelemetry(
      { progress: progressClient, events: eventClient },
      {
        runId: 'test',
        shard: 'shard0',
        intervalMs: 5,
        recreateProgressClient: () =>
          ({
            insert: async (params: { values: unknown[] }) => {
              replacementRows.push(...params.values);
            },
            close: async () => undefined,
          }) as unknown as ClickHouseClient,
      },
    );

    telemetry.start(() => snapshot(replacementRows.length + 1));
    await waitFor(() => replacementRows.length > 0);
    await telemetry.stop();

    assert.equal(oldClosed, 1);
  });
});
