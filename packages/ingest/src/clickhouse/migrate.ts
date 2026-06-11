import fs from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

import type { ClickHouseClient } from '@clickhouse/client';

import logger from '../logger.js';

import {
  AGGREGATES,
  mvCommentSql,
  mvCreateSql,
  mvSpecComment,
  type AggregateSpec,
} from './aggregates.js';
import { createClickHouseClient } from './client.js';

const SCHEMA_PATH = fileURLToPath(new URL('schema.sql', import.meta.url));

/** First table/view identifier, for log lines. */
function statementName(statement: string): string {
  const match = statement.match(
    /(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)/i,
  );
  return match?.[1] ?? statement.slice(0, 40);
}

function parseStatements(sql: string): string[] {
  return sql
    .split('\n')
    .filter((line) => !line.trimStart().startsWith('--'))
    .join('\n')
    .split(/;\s*$/m)
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0);
}

/** The live view's comment, or undefined when the view does not exist. */
async function liveMvComment(
  client: ClickHouseClient,
  mvName: string,
): Promise<string | undefined> {
  const result = await client.query({
    query: `SELECT comment FROM system.tables
WHERE database = currentDatabase() AND name = {name:String}`,
    query_params: { name: mvName },
    format: 'JSONEachRow',
  });
  const rows = await result.json<{ comment: string }>();
  return rows[0]?.comment;
}

/**
 * Create / skip / recreate one materialized view (plan 0001).
 *
 * The view's comment carries a hash of its DDL (mvSpecComment); comparing it
 * against aggregates.ts is what keeps live views and rebuild.ts on the same
 * SELECT. Three cases: missing → create; hash matches → skip; anything else
 * (including the pre-hash empty comment) → DROP + CREATE, since ClickHouse
 * cannot alter a TO-form view's SELECT in place. The recreate leaves the
 * Summing table dirty — old-SELECT rows plus a hole for posts that arrived
 * while the view was down — so it warns loudly with the rebuild command, but
 * still exits zero: drift detected and repaired is the success path here.
 */
async function ensureMv(
  client: ClickHouseClient,
  spec: AggregateSpec,
): Promise<void> {
  const expected = mvSpecComment(spec);
  const live = await liveMvComment(client, spec.mvName);
  if (live === expected) {
    logger.info(`${spec.mvName} current (${expected})`);
    return;
  }
  if (live !== undefined) {
    await client.command({ query: `DROP VIEW IF EXISTS ${spec.mvName}` });
  }
  await client.command({ query: mvCreateSql(spec) });
  await client.command({ query: mvCommentSql(spec) });
  if (live === undefined) {
    logger.info(`Applied ${spec.mvName} (${expected})`);
    return;
  }
  logger.warn(
    {
      mv: spec.mvName,
      table: spec.table,
      was: live || '(none)',
      now: expected,
    },
    `${spec.mvName} drifted from aggregates.ts and was recreated. ` +
      `${spec.table} is now DIRTY: it mixes rows from the old SELECT and ` +
      `misses posts inserted while the view was down. Re-derive it from posts ` +
      'with `bun run rebuild -- --full` (packages/ingest) — or the scheduled ' +
      'weekly full rebuild will heal it.',
  );
}

async function migrate(): Promise<void> {
  // Tables first (schema.sql), then the materialized views from aggregates.ts —
  // the single source the rebuild CLI shares. Tables are append-only IF NOT
  // EXISTS; views are version-checked by ensureMv so a changed SELECT reaches
  // existing databases instead of being silently skipped.
  const statements = parseStatements(await fs.readFile(SCHEMA_PATH, 'utf8'));
  const client = createClickHouseClient();
  try {
    for (const statement of statements) {
      try {
        await client.command({ query: statement });
      } catch (err) {
        logger.error({ err }, `Migration failed on statement:\n${statement}`);
        process.exitCode = 1;
        return;
      }
      logger.info(`Applied ${statementName(statement)}`);
    }
    for (const spec of AGGREGATES) {
      try {
        await ensureMv(client, spec);
      } catch (err) {
        logger.error({ err }, `Migration failed on view ${spec.mvName}`);
        process.exitCode = 1;
        return;
      }
    }
    logger.info(
      `Migration complete: ${statements.length} statements, ${AGGREGATES.length} views.`,
    );
  } finally {
    await client.close();
  }
}

await migrate();
