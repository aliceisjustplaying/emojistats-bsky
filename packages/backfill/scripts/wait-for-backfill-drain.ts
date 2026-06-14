import { parseArgs } from 'node:util';

import { createClickHouseClient } from '../src/clickhouse.js';

const { values } = parseArgs({
  options: {
    run: { type: 'string' },
    shard: { type: 'string', multiple: true },
    'poll-seconds': { type: 'string', default: '60' },
    'stable-polls': { type: 'string', default: '2' },
  },
});

const run = values.run;
if (run === undefined || run === '') throw new Error('--run is required');

const shards = values.shard ?? [];
if (shards.length === 0) throw new Error('--shard is required');

const pollSeconds = Number(values['poll-seconds']);
if (!Number.isFinite(pollSeconds) || pollSeconds <= 0) {
  throw new Error(
    `--poll-seconds must be positive, got ${values['poll-seconds']}`,
  );
}

const stablePolls = Number(values['stable-polls']);
if (!Number.isInteger(stablePolls) || stablePolls <= 0) {
  throw new Error(
    `--stable-polls must be a positive integer, got ${values['stable-polls']}`,
  );
}

interface DrainRow {
  remaining: string;
  newest_ts: string;
  oldest_ts: string;
  shards: string;
}

const client = createClickHouseClient('emojistats-backfill-drain-waiter');

async function remainingWork(): Promise<{
  remaining: number;
  newestTs: string;
  oldestTs: string;
  shards: number;
}> {
  const result = await client.query({
    query: `
      SELECT
        sum(pending + fetching) AS remaining,
        max(latest_ts) AS newest_ts,
        min(latest_ts) AS oldest_ts,
        count() AS shards
      FROM (
        SELECT
          shard,
          max(ts) AS latest_ts,
          argMax(pending, ts) AS pending,
          argMax(fetching, ts) AS fetching
        FROM backfill_progress
        WHERE run_id = {run:String}
          AND shard IN {shards:Array(String)}
        GROUP BY shard
      )
    `,
    query_params: { run, shards },
    format: 'JSONEachRow',
  });
  const rows = await result.json<DrainRow>();
  const row = rows[0];
  if (row === undefined || Number(row.shards) !== shards.length) {
    throw new Error(
      `expected ${shards.length} shards, saw ${row?.shards ?? 0}`,
    );
  }
  return {
    remaining: Number(row.remaining),
    newestTs: row.newest_ts,
    oldestTs: row.oldest_ts,
    shards: Number(row.shards),
  };
}

let stable = 0;
try {
  for (;;) {
    const state = await remainingWork();
    console.log(JSON.stringify({ at: new Date().toISOString(), ...state }));
    if (state.remaining === 0) {
      stable += 1;
      if (stable >= stablePolls) break;
    } else {
      stable = 0;
    }
    await new Promise<void>((resolve) => {
      setTimeout(resolve, pollSeconds * 1000);
    });
  }
} finally {
  await client.close();
}
