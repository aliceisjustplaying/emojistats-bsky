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
});
