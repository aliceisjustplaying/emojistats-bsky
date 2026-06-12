/**
 * Aggregate rebuild CLI (plan 0001: aggregates are disposable caches).
 *
 * `posts` is the only truth. The Summing tables fed by the materialized views
 * over-count whenever the same post is inserted twice outside the dedup window
 * (backfill/live overlap, replays, re-loads); this tool re-derives them from
 * `posts FINAL` so a counting bug never requires a re-crawl.
 *
 *   --full       shadow-build every aggregate table, then EXCHANGE TABLES —
 *                serving never sees an empty or half-filled table
 *   --recent N   self-heal: re-derive the last N days of the hourly tables in
 *                place; totals tables are shadow-rebuilt in full (see below)
 *   --dry-run    print the SQL instead of executing it
 */
import { parseArgs } from 'node:util';

import {
  createClient,
  type ClickHouseClient,
  type ClickHouseSettings,
} from '@clickhouse/client';

import {
  CLICKHOUSE_DATABASE,
  CLICKHOUSE_PASSWORD,
  CLICKHOUSE_URL,
  CLICKHOUSE_USER,
} from '../config.js';
import logger from '../logger.js';

import { AGGREGATES, type AggregateSpec } from './aggregates.js';

// Spill GROUP BY state to disk past 2 GiB so a full rebuild survives an 8 GB box.
const HEAVY_GROUP_BY: ClickHouseSettings = {
  max_bytes_before_external_group_by: String(2 * 1024 ** 3),
};

// Wait for the DELETE to materialize before re-inserting, so a reader after
// this run can never see the window double-counted.
const SYNC_MUTATION: ClickHouseSettings = { mutations_sync: '1' };

// Every rebuild reads `posts FINAL`: it collapses the ReplacingMergeTree
// duplicates the MVs double-counted. The SELECTs themselves come from
// aggregates.ts — the same source migrate.ts builds the MVs from.
const FINAL = true;

interface Ctx {
  client: ClickHouseClient;
  dryRun: boolean;
}

async function exec(
  ctx: Ctx,
  sql: string,
  settings?: ClickHouseSettings,
): Promise<void> {
  if (ctx.dryRun) {
    logger.info(`[dry-run] would execute:\n${sql.trim()}`);
    return;
  }
  logger.debug(sql.trim());
  await ctx.client.command({ query: sql, clickhouse_settings: settings });
}

type Summary = Record<string, number>;

async function summarize(
  client: ClickHouseClient,
  spec: AggregateSpec,
): Promise<Summary> {
  const sums = spec.measures.map((m) => `sum(${m}) AS ${m}`).join(', ');
  const result = await client.query({
    query: `SELECT toUInt64(count()) AS rows, ${sums} FROM ${spec.table}`,
    format: 'JSONEachRow',
  });
  const [row] = await result.json<Record<string, string>>();
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [key, Number(value)]),
  );
}

function diff(before: Summary, after: Summary): Summary {
  return Object.fromEntries(
    Object.keys(after).map((key) => [key, after[key] - (before[key] ?? 0)]),
  );
}

async function showCreate(
  client: ClickHouseClient,
  table: string,
): Promise<string> {
  const result = await client.query({
    query: `SHOW CREATE TABLE ${table}`,
    format: 'JSONEachRow',
  });
  const [row] = await result.json<{ statement: string }>();
  return row.statement;
}

// SHOW CREATE + name swap instead of repeating DDL here: the clone (shadow or
// staging) can never drift from the live table's actual schema.
async function cloneDdl(
  client: ClickHouseClient,
  table: string,
  clone: string,
): Promise<string> {
  const ddl = await showCreate(client, table);
  const cloned = ddl.replace(
    /^CREATE TABLE \S+/,
    `CREATE TABLE ${CLICKHOUSE_DATABASE}.${clone}`,
  );
  if (cloned === ddl)
    throw new Error(`Cannot derive ${clone} DDL for ${table} from: ${ddl}`);
  return cloned;
}

async function rebuildFull(ctx: Ctx, spec: AggregateSpec): Promise<void> {
  const shadow = `${spec.table}_rebuild`;
  const shadowDdl = await cloneDdl(ctx.client, spec.table, shadow);
  const before = ctx.dryRun ? undefined : await summarize(ctx.client, spec);
  await exec(ctx, `DROP TABLE IF EXISTS ${shadow}`);
  await exec(ctx, shadowDdl);
  await exec(
    ctx,
    `INSERT INTO ${shadow}\n${spec.select(undefined, FINAL).trim()}`,
    HEAVY_GROUP_BY,
  );
  // Atomic swap. Live posts that arrive during the INSERT scan are missing
  // from the shadow (their MV rows went to the table being replaced) — a gap
  // of one scan's duration, repaired by the next scheduled --recent pass.
  await exec(ctx, `EXCHANGE TABLES ${spec.table} AND ${shadow}`);
  await exec(ctx, `DROP TABLE ${shadow}`);
  if (before !== undefined) {
    const after = await summarize(ctx.client, spec);
    logger.info(
      { table: spec.table, before, after, delta: diff(before, after) },
      'full rebuild done',
    );
  }
}

/**
 * In-place self-heal of an hourly table, snapshot-first: the long `posts
 * FINAL` scan lands in a staging table BEFORE the live window is touched, so
 * the scan never overlaps an inconsistent state. The other order (DELETE,
 * then scan-insert into the live table) double-counts every post that arrives
 * during the scan — its MV row lands after the DELETE already finished AND
 * the scan's read snapshot includes it — and a SummingMergeTree never sheds
 * an overcount, so a daily --recent run manufactures permanent drift.
 *
 * Live posts vs. this run, by arrival time:
 *   - before the staging snapshot: counted once — they are in staging, and
 *     their MV rows die in the DELETE
 *   - between snapshot end and DELETE end (a short gap: the mutation is the
 *     only thing between them): counted zero times until the next scheduled
 *     repair re-derives them from `posts` — a bounded, self-healing
 *     undercount, strictly better than an overcount Summing keeps forever
 *   - after the DELETE: counted once, by the MV
 * rebuildFull's EXCHANGE has the same one-scan gap, healed the same way (see
 * the comment on its swap).
 */
async function repairRecent(
  ctx: Ctx,
  spec: AggregateSpec,
  cutoff: string,
): Promise<void> {
  const staging = `${spec.table}_repair`;
  const cutoffExpr = `toDateTime('${cutoff}', 'UTC')`;
  const stagingDdl = await cloneDdl(ctx.client, spec.table, staging);
  const before = ctx.dryRun ? undefined : await summarize(ctx.client, spec);
  await exec(ctx, `DROP TABLE IF EXISTS ${staging}`);
  await exec(ctx, stagingDdl);
  await exec(
    ctx,
    `INSERT INTO ${staging}\n${spec.select(`created_at >= ${cutoffExpr}`, FINAL).trim()}`,
    HEAVY_GROUP_BY,
  );
  await exec(
    ctx,
    `ALTER TABLE ${spec.table} DELETE WHERE hour >= ${cutoffExpr}`,
    SYNC_MUTATION,
  );
  // Pre-aggregated rows, no GROUP BY — a plain copy needs no spill settings.
  await exec(ctx, `INSERT INTO ${spec.table} SELECT * FROM ${staging}`);
  await exec(ctx, `DROP TABLE ${staging}`);
  if (before !== undefined) {
    const after = await summarize(ctx.client, spec);
    logger.info(
      { table: spec.table, cutoff, before, after, delta: diff(before, after) },
      'recent repair done',
    );
  }
}

/**
 * 'YYYY-MM-DD HH:00:00' UTC, floored to the hour so the hour-keyed DELETE and
 * the created_at-keyed INSERT cover exactly the same posts.
 */
function hourAlignedCutoff(daysBack: number): string {
  return `${new Date(Date.now() - daysBack * 86_400_000).toISOString().slice(0, 13).replace('T', ' ')}:00:00`;
}

async function main(): Promise<void> {
  const { values } = parseArgs({
    options: {
      full: { type: 'boolean', default: false },
      recent: { type: 'string' },
      'dry-run': { type: 'boolean', default: false },
    },
  });

  const recentDays =
    values.recent === undefined ? undefined : Number(values.recent);
  if (values.full === (recentDays !== undefined)) {
    throw new Error('Usage: rebuild --full | --recent <days> [--dry-run]');
  }
  if (
    recentDays !== undefined &&
    (!Number.isInteger(recentDays) || recentDays < 1)
  ) {
    throw new Error(
      `--recent expects a positive integer day count, got: ${values.recent}`,
    );
  }

  const ctx: Ctx = {
    // Not the shared ingest client: its 30s request_timeout would abort a
    // full-history INSERT…SELECT mid-flight.
    client: createClient({
      url: CLICKHOUSE_URL,
      username: CLICKHOUSE_USER,
      password: CLICKHOUSE_PASSWORD,
      database: CLICKHOUSE_DATABASE,
      application: 'emojistats-rebuild',
      keep_alive: { eagerly_destroy_stale_sockets: true },
      request_timeout: 4 * 3_600_000,
      // Keeps the HTTP socket alive across a full-history INSERT…SELECT.
      clickhouse_settings: {
        send_progress_in_http_headers: 1,
        http_headers_progress_interval_ms: '50000',
      },
    }),
    dryRun: values['dry-run'],
  };

  try {
    if (recentDays === undefined) {
      logger.info(
        { dryRun: ctx.dryRun },
        'full rebuild of all aggregate tables',
      );
      for (const spec of AGGREGATES) await rebuildFull(ctx, spec);
    } else {
      const cutoff = hourAlignedCutoff(recentDays);
      logger.info(
        { cutoff, recentDays, dryRun: ctx.dryRun },
        'self-heal of recent aggregates',
      );
      for (const spec of AGGREGATES) {
        // Totals tables cannot be partially rebuilt: a Summing row keyed only
        // by emoji/lang blends contributions from all of history, so no
        // predicate can isolate the recent share for deletion. The full
        // shadow+exchange path is the only correct refresh — and stays cheap,
        // being a scan of the no-text spine columns.
        if (spec.hourly) await repairRecent(ctx, spec, cutoff);
        else await rebuildFull(ctx, spec);
      }
    }
    logger.info('rebuild complete');
  } finally {
    await ctx.client.close();
  }
}

try {
  await main();
} catch (err) {
  logger.error({ err }, 'rebuild failed');
  process.exitCode = 1;
}
