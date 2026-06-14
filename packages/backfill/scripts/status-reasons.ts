import Database from 'better-sqlite3';

import { createClickHouseClient } from '../src/clickhouse.js';

interface Args {
  ledger: string;
  shardIndex: number;
  shardLabel: string;
  shards: number;
  snapshotId: string;
}

interface ReasonRow {
  status: string;
  reason: string;
  count: number;
}

const UNREACHABLE_REASON_SQL = `
  CASE
    WHEN error = 'host dead: bulk-parked for final sweep'
      THEN 'bulk parked dead host (generic)'
    WHEN error LIKE '%pds.trump.com%'
      THEN 'pds.trump.com dead/DNS'
    WHEN error LIKE 'final sweep parked: referendumapp%'
      THEN 'referendumapp 429/502 tail'
    WHEN error LIKE 'host dead:%'
      THEN 'other host-dead parked'
    WHEN error LIKE '%http 429%'
      THEN 'http 429/rate limited'
    WHEN error LIKE '%ENOTFOUND%'
      THEN 'dns ENOTFOUND'
    WHEN error LIKE '%timed out%' OR error LIKE '%stalled:%'
      THEN 'timeout/stall'
    ELSE 'other/unclassified'
  END
`;

const QUARANTINED_REASON_SQL = `
  CASE
    WHEN error LIKE 'malformed car: decoded value contains remainder%'
      THEN 'malformed CAR: decoded remainder'
    WHEN error LIKE 'malformed car: invalid argument encoding%'
      THEN 'malformed CAR: invalid arg encoding'
    WHEN error LIKE 'malformed car: unexpected eof while decoding varint%'
      THEN 'malformed CAR: EOF varint'
    WHEN error LIKE 'malformed car: unexpected eof while reading data%'
      THEN 'malformed CAR: EOF data'
    WHEN error LIKE 'post record % missing from the car%'
      THEN 'post record missing from CAR'
    WHEN error LIKE 'car is missing a mst-node block%'
      THEN 'missing MST node block'
    WHEN error LIKE 'car is missing a commit block%'
      THEN 'missing commit block'
    WHEN error LIKE 'malformed car: expected mst node block%'
      THEN 'expected MST node block'
    WHEN error LIKE 'malformed car: expected commit block%'
      THEN 'expected commit block'
    WHEN error LIKE 'malformed car: invalid binary cid%'
      THEN 'invalid binary CID'
    ELSE 'other/unclassified'
  END
`;

const FAILED_REASON_SQL = `
  CASE
    WHEN error LIKE 'not in % listRepos (PLC-only DID)'
      THEN 'PLC-only DID missing from host listRepos'
    WHEN error LIKE '%RepoNotFound%'
      THEN 'getRepo RepoNotFound'
    WHEN error LIKE '%http 404%'
      THEN 'http 404'
    WHEN error LIKE '%http 4%'
      THEN 'other http 4xx'
    WHEN error IS NULL OR error = ''
      THEN 'missing error text'
    ELSE 'other/unclassified'
  END
`;

function usage(): never {
  throw new Error(
    'usage: status-reasons --ledger <path> --shard-index <n> [--shards 6] [--shard-label shardN] [--snapshot-id id]',
  );
}

function parseArgs(argv: string[]): Args {
  let ledger: string | undefined;
  let shardIndex: number | undefined;
  let shards = 6;
  let shardLabel: string | undefined;
  let snapshotId = new Date().toISOString().replace(/[:.]/g, '');

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    if (arg === '--ledger' && next !== undefined) {
      ledger = next;
      i += 1;
    } else if (arg === '--shard-index' && next !== undefined) {
      shardIndex = Number(next);
      i += 1;
    } else if (arg === '--shards' && next !== undefined) {
      shards = Number(next);
      i += 1;
    } else if (arg === '--shard-label' && next !== undefined) {
      shardLabel = next;
      i += 1;
    } else if (arg === '--snapshot-id' && next !== undefined) {
      snapshotId = next;
      i += 1;
    } else {
      usage();
    }
  }

  if (ledger === undefined || shardIndex === undefined) usage();
  if (!Number.isInteger(shardIndex) || shardIndex < 0) usage();
  if (!Number.isInteger(shards) || shards <= shardIndex) usage();

  return {
    ledger,
    shardIndex,
    shardLabel: shardLabel ?? `shard${shardIndex}`,
    shards,
    snapshotId,
  };
}

function rowsForStatus(
  db: Database.Database,
  status: string,
  reasonSql: string,
  args: Args,
): ReasonRow[] {
  return db
    .prepare(
      `
      SELECT
        status,
        ${reasonSql} AS reason,
        count(*) AS count
      FROM repos
      WHERE bucket % ? = ? AND status = ?
      GROUP BY status, reason
      ORDER BY count DESC
    `,
    )
    .all(args.shards, args.shardIndex, status) as ReasonRow[];
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const db = new Database(args.ledger, { readonly: true, fileMustExist: true });
  const ch = createClickHouseClient('emojistats-backfill-status-reasons');

  try {
    const rows = [
      ...rowsForStatus(db, 'unreachable', UNREACHABLE_REASON_SQL, args),
      ...rowsForStatus(db, 'quarantined', QUARANTINED_REASON_SQL, args),
      ...rowsForStatus(db, 'failed', FAILED_REASON_SQL, args),
    ];
    await ch.command({
      query: `
        CREATE TABLE IF NOT EXISTS backfill_status_reason_counts (
          ts DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
          snapshot_id LowCardinality(String),
          shard LowCardinality(String),
          status LowCardinality(String),
          reason LowCardinality(String),
          count UInt64
        ) ENGINE = ReplacingMergeTree(ts)
        PARTITION BY toYYYYMM(ts)
        ORDER BY (snapshot_id, shard, status, reason)
        TTL ts + INTERVAL 6 MONTH DELETE
      `,
    });
    await ch.insert({
      table: 'backfill_status_reason_counts',
      format: 'JSONEachRow',
      values: rows.map((row) => ({
        ts: new Date().toISOString().slice(0, 19).replace('T', ' '),
        snapshot_id: args.snapshotId,
        shard: args.shardLabel,
        status: row.status,
        reason: row.reason,
        count: row.count,
      })),
    });
    console.log(
      `${args.shardLabel}: wrote ${rows.length} reason rows (${args.snapshotId})`,
    );
  } finally {
    db.close();
    await ch.close();
  }
}

await main();
