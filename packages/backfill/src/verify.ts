/**
 * Backfill acceptance-criteria engine (plan 0001). Run with `bun run verify`.
 *
 *   (a) Per-repo reconciliation, tiered: ledger posts_total + rkey_digest vs
 *       ClickHouse count() + groupBitXor rkey digest. The verifier stages the
 *       ledger expectations into ClickHouse, then runs one set-based join
 *       against deduped (did, rkey) rows. EXACT = digests match and counts are equal; LOOSE
 *       = CH count >= ledger (today's lower-bound semantics); CH count < ledger
 *       is loss and fails the run. EXACT rows promote to 'verified'; LOOSE rows
 *       promote only after an explicit exhaustive --sample-loose all pass.
 *   (b) Terminal-state report: counts per status plus the explicit
 *       unreachable/quarantined/failed DID lists (capped at 50 each).
 *   (c) --sample N: re-fetch N random loaded/verified repos end to end and
 *       compare the extracted (did, rkey) post sets against ClickHouse. The
 *       invariant is superset, not equality: nothing is ever deleted from CH
 *       (emojitracker semantics), so only rkeys MISSING from CH fail; rkeys CH
 *       has that the fresh CAR lacks are posts deleted since the crawl.
 *   (d) --sample-loose N | all: DIRECTED re-fetch. Reconciliation (a) can only
 *       lower-bound the LOOSE class (CH count >= ledger with a divergent digest)
 *       — precisely the repos where a lost backfill row could be masked by an
 *       offsetting live arrival, the one thing a 64-bit XOR cannot rule out.
 *       This draws the re-fetch sample from THAT set, not at random, so every
 *       repo checked is one the digest could not clear; `all` visits the whole
 *       set (no random gaps), for small/medium runs — for millions use
 *       --emit-loose with the crawler instead.
 *       NOT A PROOF: a re-fetch only checks "every post STILL in the repo is in
 *       ClickHouse". A post deleted upstream since the crawl is absent from both
 *       the fresh CAR and (possibly) CH, so this cannot prove the ORIGINAL CAR
 *       ⊆ CH — the residual (a row both lost from CH AND deleted upstream since)
 *       is unrecoverable and undetectable by any re-fetch. It is the strongest
 *       PRACTICAL integrity check, not a formal proof. Run it post-drain (CH
 *       flushed, no new LOOSE repos racing reconciliation) for the cleanest read.
 *   (e) --emit-loose PATH: write the LOOSE DID list (one per line) so the
 *       at-scale convergence loop can re-fetch them at full concurrency:
 *       `crawl --did-file PATH` re-loads them (a shell-expanded `--did $(cat)`
 *       breaks — --did is repeatable so extras become rejected positionals, and
 *       millions exceed ARG_MAX), then re-run verify; repeat until LOOSE shrinks
 *       to the genuinely-live tail (deletes-since-crawl).
 *   (f) --orphans: optional ClickHouse-only DID report. This scans raw posts
 *       for DISTINCT backfill DIDs absent from the staged expectations for
 *       the current VERIFY_RUN_ID and is deliberately off by default. For a
 *       whole-fleet orphan audit, stage every shard with the same run id first.
 *
 * Exit codes: 0 clean, 1 mismatches found, 2 could not run.
 *
 * `--loaded-only` restricts reconciliation to rows still in ledger status
 * loaded, for non-canonical post-recrawl checks. Use a non-canonical
 * VERIFY_SHARD label such as fail-shard0 so dashboard full-shard totals are not
 * replaced by a partial run.
 *
 * The ledger is accessed directly over its frozen schema (src/ledger.sql) so
 * this CLI stays runnable independently of the crawler's ledger.ts module;
 * the only write it performs is the loaded → verified promotion, mirroring
 * Ledger.markVerified.
 */
import { once } from 'node:events';
import { createWriteStream, existsSync } from 'node:fs';
import path from 'node:path';
import { parseArgs } from 'node:util';

import type { ClickHouseClient, ClickHouseSettings } from '@clickhouse/client';
import Database from 'better-sqlite3';
import type { PostRow } from 'ingest/types';

import { createClickHouseClient, pingClickHouse } from './clickhouse.js';
import { LEDGER_DB_PATH } from './config.js';
import { CH_RKEY_DIGEST_EXPR, normalizeDigestHex } from './digest.js';
import { repoPostRows } from './extract.js';
import { fetchRepoCar } from './fetcher.js';
import logger from './logger.js';
import { parseRepoCar } from './parser.js';
import type { RepoStatus } from './types.js';
import {
  createLoadedPromotionStage,
  type LoadedPromotionStage,
  promoteLoadedReposByDid,
  promotionResultListSql,
  stageLoadedReposForPromotion,
} from './verify-promotion.js';
import { VerifyTelemetry, type VerifyProgress } from './verify-telemetry.js';

const DID_LIST_CAP = 50;
// Compile-checked subset of THE status registry (types.ts REPO_STATUSES).
const TERMINAL_REPORT_STATUSES = [
  'unreachable',
  'quarantined',
  'failed',
] as const satisfies readonly RepoStatus[];

interface LedgerRepo {
  did: string;
  status: string;
  posts_total: number | null;
  pds_host: string;
  rev: string | null;
  rkey_digest: string | null;
}

interface Mismatch {
  did: string;
  status: string;
  ledgerPostsTotal: number;
  clickhousePosts: number;
  // 'count-short': CH holds fewer rows than the ledger recorded — unambiguous
  // loss. 'digest-mismatch': counts are equal but the rkey SETS differ, which
  // means a dropped backfill row was numerically masked by an offsetting
  // live-path arrival (or a since-crawl delete + arrival). XOR digests can't
  // say which, and the contract is to never let live traffic paper over a lost
  // backfill row — so this fails the run for --sample / re-crawl adjudication
  // instead of promoting to 'verified'.
  reason: 'count-short' | 'digest-mismatch';
  ledgerDigest?: string;
  clickhouseDigest?: string;
}

interface ReconcileResult {
  exact: number;
  loose: number;
  promotionStage: LoadedPromotionStage;
  // The LOOSE class itself (CH count >= ledger, divergent digest, expected > 0):
  // the only repos a digest reconciliation cannot prove, and therefore the only
  // ones a re-fetch needs to visit. Carried out so --sample-loose / --emit-loose
  // can target them directly. Empty-ledger (expected = 0) repos are excluded —
  // there is nothing there to have lost.
  looseRepos: LedgerRepo[];
  looseEmitted: number;
  mismatchCount: number;
  mismatches: Mismatch[];
}

interface VerifyExpectedInsertRow {
  inserted_at: string;
  run_id: string;
  shard: string;
  did: string;
  status: string;
  expected_posts: number;
  expected_digest: string;
  pds_host: string;
  rev: string | null;
}

interface ReconcileOptions {
  emitLoosePath?: string;
  collectLooseRepos: boolean;
  includeOrphans: boolean;
  loadedOnly: boolean;
  promoteLoose: boolean;
}

interface StageExpectedReposResult {
  reposTotal: number;
  promotionStage: LoadedPromotionStage;
}

function positiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed <= 0)
    throw new Error(`${name} must be a positive integer, got ${raw}`);
  return parsed;
}

const VERIFY_INSERT_BATCH_ROWS = positiveIntEnv(
  'VERIFY_INSERT_BATCH_ROWS',
  50_000,
);
const VERIFY_EXTERNAL_GROUP_BY_BYTES = positiveIntEnv(
  'VERIFY_EXTERNAL_GROUP_BY_BYTES',
  512 * 1024 ** 2,
);
const VERIFY_EXTERNAL_SORT_BYTES = positiveIntEnv(
  'VERIFY_EXTERNAL_SORT_BYTES',
  512 * 1024 ** 2,
);
const VERIFY_CLICKHOUSE_REQUEST_TIMEOUT_MS = positiveIntEnv(
  'VERIFY_CLICKHOUSE_REQUEST_TIMEOUT_MS',
  60 * 60_000,
);
const VERIFY_MAX_THREADS = positiveIntEnv('VERIFY_MAX_THREADS', 2);
const VERIFY_MAX_MEMORY_USAGE_BYTES = positiveIntEnv(
  'VERIFY_MAX_MEMORY_USAGE_BYTES',
  8 * 1024 ** 3,
);
const VERIFY_QUERY_SETTINGS: ClickHouseSettings = {
  send_progress_in_http_headers: 0,
  wait_end_of_query: 1,
  max_bytes_before_external_group_by: String(VERIFY_EXTERNAL_GROUP_BY_BYTES),
  max_bytes_before_external_sort: String(VERIFY_EXTERNAL_SORT_BYTES),
  max_threads: VERIFY_MAX_THREADS,
  max_memory_usage: String(VERIFY_MAX_MEMORY_USAGE_BYTES),
  optimize_aggregation_in_order: 1,
};
const VERIFY_EXPECTED_TABLE = 'backfill_verify_expected';
const VERIFY_RESULT_TABLE = 'backfill_verify_result';

function canonicalShardFromIndex(value: string): string {
  return /^\d+$/.test(value) ? `shard${value}` : value;
}

const VERIFY_RUN_ID_EXPLICIT = process.env.VERIFY_RUN_ID !== undefined;
const VERIFY_RUN_ID = VERIFY_RUN_ID_EXPLICIT
  ? process.env.VERIFY_RUN_ID!
  : `verify-${new Date().toISOString().slice(0, 19).replace(/[-:T]/g, '')}`;
const VERIFY_SHARD =
  process.env.VERIFY_SHARD ??
  (process.env.CRAWL_SHARD_INDEX === undefined
    ? undefined
    : canonicalShardFromIndex(process.env.CRAWL_SHARD_INDEX)) ??
  path.basename(LEDGER_DB_PATH, '.sqlite');

/** 'YYYY-MM-DD HH:MM:SS' UTC, the JSONEachRow-friendly DateTime form. */
function chDateTime(ms: number): string {
  return new Date(ms).toISOString().slice(0, 19).replace('T', ' ');
}

function verifyQueryParams() {
  return { run_id: VERIFY_RUN_ID, shard: VERIFY_SHARD };
}

async function ensureExpectedTable(ch: ClickHouseClient): Promise<void> {
  await ch.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${VERIFY_EXPECTED_TABLE} (
        inserted_at     DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
        run_id          LowCardinality(String),
        shard           LowCardinality(String),
        did             String CODEC(ZSTD(1)),
        status          LowCardinality(String),
        expected_posts  UInt64,
        expected_digest String,
        pds_host        String CODEC(ZSTD(1)),
        rev             Nullable(String)
      ) ENGINE = MergeTree
      PARTITION BY toYYYYMM(inserted_at)
      ORDER BY (run_id, shard, did)
      TTL inserted_at + INTERVAL 7 DAY DELETE
    `,
  });
}

async function ensureResultTable(ch: ClickHouseClient): Promise<void> {
  await ch.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${VERIFY_RESULT_TABLE} (
        inserted_at     DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
        run_id          LowCardinality(String),
        shard           LowCardinality(String),
        did             String CODEC(ZSTD(1)),
        status          LowCardinality(String),
        expected_posts  UInt64,
        actual_posts    UInt64,
        expected_digest String,
        actual_digest   String,
        pds_host        String CODEC(ZSTD(1)),
        rev             Nullable(String),
        result          LowCardinality(String)
      ) ENGINE = MergeTree
      PARTITION BY toYYYYMM(inserted_at)
      ORDER BY (run_id, shard, result, did)
      TTL inserted_at + INTERVAL 7 DAY DELETE
    `,
  });
}

async function resetExpectedRows(ch: ClickHouseClient): Promise<void> {
  await ch.command({
    query: `
      ALTER TABLE ${VERIFY_EXPECTED_TABLE}
      DELETE WHERE run_id = {run_id:String} AND shard = {shard:String}
    `,
    query_params: verifyQueryParams(),
    clickhouse_settings: { mutations_sync: '1' },
  });
}

async function resetResultRows(ch: ClickHouseClient): Promise<void> {
  await ch.command({
    query: `
      ALTER TABLE ${VERIFY_RESULT_TABLE}
      DELETE WHERE run_id = {run_id:String} AND shard = {shard:String}
    `,
    query_params: verifyQueryParams(),
    clickhouse_settings: { mutations_sync: '1' },
  });
}

async function stageExpectedRepos(
  db: Database.Database,
  ch: ClickHouseClient,
  telemetry: VerifyTelemetry,
  loadedOnly: boolean,
): Promise<StageExpectedReposResult> {
  await ensureExpectedTable(ch);
  await resetExpectedRows(ch);
  const statusWhere = loadedOnly
    ? "status = 'loaded'"
    : "status IN ('loaded', 'verified')";

  const total = (
    db
      .prepare(`SELECT COUNT(*) AS n FROM repos WHERE ${statusWhere}`)
      .get() as {
      n: number;
    }
  ).n;

  await telemetry.record(
    {
      phase: 'staging-ledger',
      reposTotal: total,
      reposChecked: 0,
      exact: 0,
      loose: 0,
      mismatches: 0,
      looseEmitted: 0,
      sampleChecked: 0,
      sampleFailures: 0,
      done: false,
    },
    true,
  );

  const stmt = db.prepare(
    `SELECT did, status, posts_total, pds_host, rev, rkey_digest
     FROM repos WHERE ${statusWhere}`,
  );
  const promotionStage = createLoadedPromotionStage(db);
  const stagedLoadedDids: string[] = [];
  let staged = 0;
  let batch: VerifyExpectedInsertRow[] = [];
  const flush = async () => {
    if (batch.length === 0) return;
    await ch.insert({
      table: VERIFY_EXPECTED_TABLE,
      values: batch,
      format: 'JSONEachRow',
    });
    staged += batch.length;
    batch = [];
    await telemetry.record({
      phase: 'staging-ledger',
      reposTotal: total,
      reposChecked: staged,
      exact: 0,
      loose: 0,
      mismatches: 0,
      looseEmitted: 0,
      sampleChecked: 0,
      sampleFailures: 0,
      done: false,
    });
  };

  const insertedAt = chDateTime(Date.now());
  for (const repo of stmt.iterate() as IterableIterator<LedgerRepo>) {
    batch.push({
      inserted_at: insertedAt,
      run_id: VERIFY_RUN_ID,
      shard: VERIFY_SHARD,
      did: repo.did,
      status: repo.status,
      expected_posts: repo.posts_total ?? 0,
      expected_digest:
        repo.rkey_digest === null ? '' : normalizeDigestHex(repo.rkey_digest),
      pds_host: repo.pds_host,
      rev: repo.rev,
    });
    if (repo.status === 'loaded') stagedLoadedDids.push(repo.did);
    if (batch.length >= VERIFY_INSERT_BATCH_ROWS) await flush();
  }
  await flush();

  stageLoadedReposForPromotion(db, promotionStage, stagedLoadedDids);

  await telemetry.record(
    {
      phase: 'staging-ledger',
      reposTotal: total,
      reposChecked: staged,
      exact: 0,
      loose: 0,
      mismatches: 0,
      looseEmitted: 0,
      sampleChecked: 0,
      sampleFailures: 0,
      done: false,
    },
    true,
  );
  return { reposTotal: staged, promotionStage };
}

function classifiedSql(selectSql: string): string {
  return `
    WITH
      expected AS (
        SELECT did, status, expected_posts, expected_digest, pds_host, rev
        FROM ${VERIFY_EXPECTED_TABLE}
        WHERE run_id = {run_id:String} AND shard = {shard:String}
      ),
      actual AS (
        SELECT
          did,
          toUInt64(count()) AS actual_posts,
          leftPad(lower(hex(${CH_RKEY_DIGEST_EXPR})), 16, '0') AS actual_digest
        FROM
        (
          SELECT did, rkey
          FROM posts
          WHERE did IN (SELECT did FROM expected)
          GROUP BY did, rkey
        )
        GROUP BY did
      ),
      classified AS (
        SELECT
          *,
          multiIf(
            expected_digest != ''
              AND actual_posts = expected_posts
              AND actual_digest = expected_digest,
            'exact',
            expected_digest != ''
              AND expected_posts > 0
              AND actual_posts = expected_posts
              AND actual_digest != expected_digest,
            'digest-mismatch',
            actual_posts < expected_posts,
            'count-short',
            'loose'
          ) AS result
        FROM
        (
          SELECT
            e.did,
            e.status,
            e.expected_posts,
            e.expected_digest,
            e.pds_host,
            e.rev,
            ifNull(a.actual_posts, toUInt64(0)) AS actual_posts,
            ifNull(a.actual_digest, '') AS actual_digest
          FROM expected AS e
          LEFT JOIN actual AS a USING did
        ) AS joined
      )
    ${selectSql}
  `;
}

async function materializeClassification(ch: ClickHouseClient): Promise<void> {
  await ensureResultTable(ch);
  await resetResultRows(ch);
  await ch.command({
    query: `
      INSERT INTO ${VERIFY_RESULT_TABLE}
        (inserted_at, run_id, shard, did, status, expected_posts, actual_posts,
         expected_digest, actual_digest, pds_host, rev, result)
      ${classifiedSql(`
        SELECT
          now() AS inserted_at,
          {run_id:String} AS run_id,
          {shard:String} AS shard,
          did,
          status,
          expected_posts,
          actual_posts,
          expected_digest,
          actual_digest,
          pds_host,
          rev,
          result
        FROM classified
      `)}
    `,
    query_params: verifyQueryParams(),
    clickhouse_settings: VERIFY_QUERY_SETTINGS,
  });
}

async function queryClassificationCounts(ch: ClickHouseClient): Promise<{
  checked: number;
  exact: number;
  loose: number;
  mismatchCount: number;
  passedLoaded: number;
  alreadyVerified: number;
}> {
  const result = await ch.query({
    query: `
      SELECT
        count() AS checked,
        countIf(result = 'exact') AS exact,
        countIf(result = 'loose') AS loose,
        countIf(result IN ('count-short', 'digest-mismatch')) AS mismatches,
        countIf(status = 'loaded' AND result IN ('exact', 'loose')) AS passed_loaded,
        countIf(status = 'verified' AND result IN ('exact', 'loose')) AS already_verified
      FROM ${VERIFY_RESULT_TABLE}
      WHERE run_id = {run_id:String} AND shard = {shard:String}
    `,
    query_params: verifyQueryParams(),
    format: 'JSONEachRow',
  });
  const [row] = await result.json<{
    checked: string;
    exact: string;
    loose: string;
    mismatches: string;
    passed_loaded: string;
    already_verified: string;
  }>();
  return {
    checked: Number(row?.checked ?? 0),
    exact: Number(row?.exact ?? 0),
    loose: Number(row?.loose ?? 0),
    mismatchCount: Number(row?.mismatches ?? 0),
    passedLoaded: Number(row?.passed_loaded ?? 0),
    alreadyVerified: Number(row?.already_verified ?? 0),
  };
}

async function queryMismatchExamples(
  ch: ClickHouseClient,
): Promise<Mismatch[]> {
  const result = await ch.query({
    query: `
      SELECT
        did,
        status,
        expected_posts AS ledger_posts_total,
        actual_posts AS clickhouse_posts,
        result AS reason,
        expected_digest AS ledger_digest,
        actual_digest AS clickhouse_digest
      FROM ${VERIFY_RESULT_TABLE}
      WHERE
        run_id = {run_id:String}
        AND shard = {shard:String}
        AND result IN ('count-short', 'digest-mismatch')
      ORDER BY did
      LIMIT ${DID_LIST_CAP}
    `,
    query_params: verifyQueryParams(),
    format: 'JSONEachRow',
  });
  const rows = await result.json<{
    did: string;
    status: string;
    ledger_posts_total: string;
    clickhouse_posts: string;
    reason: 'count-short' | 'digest-mismatch';
    ledger_digest: string;
    clickhouse_digest: string;
  }>();
  return rows.map((row) => ({
    did: row.did,
    status: row.status,
    ledgerPostsTotal: Number(row.ledger_posts_total),
    clickhousePosts: Number(row.clickhouse_posts),
    reason: row.reason,
    ledgerDigest: row.ledger_digest || undefined,
    clickhouseDigest: row.clickhouse_digest || undefined,
  }));
}

async function collectLooseRepos(ch: ClickHouseClient): Promise<LedgerRepo[]> {
  const result = await ch.query({
    query: `
      SELECT
        did,
        status,
        expected_posts AS posts_total,
        pds_host,
        rev,
        expected_digest AS rkey_digest
      FROM ${VERIFY_RESULT_TABLE}
      WHERE
        run_id = {run_id:String}
        AND shard = {shard:String}
        AND result = 'loose'
        AND expected_posts > 0
      ORDER BY did
    `,
    query_params: verifyQueryParams(),
    format: 'JSONEachRow',
  });
  const rows = await result.json<{
    did: string;
    status: string;
    posts_total: string;
    pds_host: string;
    rev: string | null;
    rkey_digest: string;
  }>();
  return rows.map((row) => ({
    did: row.did,
    status: row.status,
    posts_total: Number(row.posts_total),
    pds_host: row.pds_host,
    rev: row.rev,
    rkey_digest: row.rkey_digest,
  }));
}

async function collectPromotableLoadedDids(
  ch: ClickHouseClient,
  includeLoose: boolean,
): Promise<string[]> {
  const result = await ch.query({
    query: `
      SELECT did
      FROM ${VERIFY_RESULT_TABLE}
      WHERE
        run_id = {run_id:String}
        AND shard = {shard:String}
        AND status = 'loaded'
        AND result IN ${promotionResultListSql(includeLoose)}
      ORDER BY did
    `,
    query_params: verifyQueryParams(),
    format: 'JSONEachRow',
  });
  const rows = await result.json<{ did: string }>();
  return rows.map((row) => row.did);
}

async function emitLooseDids(
  ch: ClickHouseClient,
  emitLoosePath: string,
): Promise<number> {
  const result = await ch.query({
    query: `
      SELECT did
      FROM ${VERIFY_RESULT_TABLE}
      WHERE
        run_id = {run_id:String}
        AND shard = {shard:String}
        AND result = 'loose'
        AND expected_posts > 0
      ORDER BY did
    `,
    query_params: verifyQueryParams(),
    format: 'JSONEachRow',
  });
  const out = createWriteStream(emitLoosePath, { encoding: 'utf8' });
  let emitted = 0;
  try {
    for await (const rows of result.stream<{ did: string }>()) {
      for (const row of rows) {
        if (!out.write(`${row.json().did}\n`)) await once(out, 'drain');
        emitted += 1;
      }
    }
  } catch (err) {
    out.destroy();
    throw err;
  }
  out.end();
  await once(out, 'finish');
  return emitted;
}

async function orphanExamples(ch: ClickHouseClient): Promise<{
  count: number;
  examples: Array<{ did: string; posts: number }>;
}> {
  const countResult = await ch.query({
    query: `
      WITH expected AS (
        SELECT DISTINCT did
        FROM ${VERIFY_EXPECTED_TABLE}
        WHERE run_id = {run_id:String}
      )
      SELECT count() AS n
      FROM
      (
        SELECT DISTINCT did
        FROM posts
        WHERE src = 'backfill'
      ) AS backfilled
      LEFT ANTI JOIN expected USING did
    `,
    query_params: verifyQueryParams(),
    clickhouse_settings: VERIFY_QUERY_SETTINGS,
    format: 'JSONEachRow',
  });
  const [countRow] = await countResult.json<{ n: string }>();
  const examplesResult = await ch.query({
    query: `
      WITH expected AS (
        SELECT DISTINCT did
        FROM ${VERIFY_EXPECTED_TABLE}
        WHERE run_id = {run_id:String}
      )
      SELECT did, toUInt64(count()) AS posts
      FROM
      (
        SELECT did, rkey
        FROM posts
        INNER JOIN
        (
          SELECT did
          FROM
          (
            SELECT DISTINCT did
            FROM posts
            WHERE src = 'backfill'
          ) AS backfilled
          LEFT ANTI JOIN expected USING did
          LIMIT ${DID_LIST_CAP}
        ) AS orphan_dids USING did
        GROUP BY did, rkey
      )
      GROUP BY did
      ORDER BY did
    `,
    query_params: verifyQueryParams(),
    clickhouse_settings: VERIFY_QUERY_SETTINGS,
    format: 'JSONEachRow',
  });
  const examples = await examplesResult.json<{ did: string; posts: string }>();
  return {
    count: Number(countRow?.n ?? 0),
    examples: examples.map((row) => ({
      did: row.did,
      posts: Number(row.posts),
    })),
  };
}

async function reconcile(
  db: Database.Database,
  ch: ClickHouseClient,
  telemetry: VerifyTelemetry,
  options: ReconcileOptions,
): Promise<ReconcileResult> {
  const { reposTotal, promotionStage } = await stageExpectedRepos(
    db,
    ch,
    telemetry,
    options.loadedOnly,
  );

  await telemetry.record(
    {
      phase: 'querying-clickhouse',
      reposTotal,
      reposChecked: 0,
      exact: 0,
      loose: 0,
      mismatches: 0,
      looseEmitted: 0,
      sampleChecked: 0,
      sampleFailures: 0,
      done: false,
    },
    true,
  );

  await materializeClassification(ch);
  const counts = await queryClassificationCounts(ch);
  if (counts.checked !== reposTotal) {
    throw new Error(
      `verification classified ${counts.checked} of ${reposTotal} staged repos; refusing to promote ledger rows. This usually means the ClickHouse INSERT ... SELECT failed mid-query; inspect system.query_log for the underlying error.`,
    );
  }

  await telemetry.record(
    {
      phase: 'classifying',
      reposTotal,
      reposChecked: counts.checked,
      exact: counts.exact,
      loose: counts.loose,
      mismatches: counts.mismatchCount,
      looseEmitted: 0,
      sampleChecked: 0,
      sampleFailures: 0,
      done: false,
    },
    true,
  );

  const mismatches = await queryMismatchExamples(ch);
  const looseRepos = options.collectLooseRepos
    ? await collectLooseRepos(ch)
    : [];
  const looseEmitted =
    options.emitLoosePath === undefined
      ? 0
      : await emitLooseDids(ch, options.emitLoosePath);

  await telemetry.record(
    {
      phase: 'promoting',
      reposTotal,
      reposChecked: counts.checked,
      exact: counts.exact,
      loose: counts.loose,
      mismatches: counts.mismatchCount,
      looseEmitted,
      sampleChecked: 0,
      sampleFailures: 0,
      done: false,
    },
    true,
  );

  const promotableDids = await collectPromotableLoadedDids(
    ch,
    options.promoteLoose,
  );
  const promoted = promoteLoadedReposByDid(db, promotableDids, promotionStage);

  const orphans = options.includeOrphans
    ? await orphanExamples(ch)
    : { count: 'skipped' as const, examples: [] };

  logger.info(
    {
      reposChecked: counts.checked,
      exact: counts.exact,
      loose: counts.loose,
      promotedToVerified: promoted,
      alreadyVerified: counts.alreadyVerified,
      mismatches: counts.mismatchCount,
      clickhouseOnlyDids: orphans.count,
    },
    'reconciliation: exact = counts and rkey digests match; loose = CH count >= ledger only (usual benign cause: live-only posts arriving during/after the crawl; pre-digest ledger rows can never exceed loose)',
  );
  for (const orphan of orphans.examples) {
    logger.warn(
      { did: orphan.did, clickhousePosts: orphan.posts },
      'ClickHouse has backfill rows for a DID the ledger has not marked loaded',
    );
  }
  for (const mismatch of mismatches) {
    logger.error(
      mismatch,
      mismatch.reason === 'count-short'
        ? 'count mismatch: ClickHouse count() < ledger posts_total (lost rows)'
        : 'digest mismatch: counts equal but rkey sets differ (possible lost ' +
            'backfill row masked by a live arrival) — run --sample on this DID',
    );
  }
  return {
    exact: counts.exact,
    loose: counts.loose,
    looseRepos,
    looseEmitted,
    mismatchCount: counts.mismatchCount,
    mismatches,
    promotionStage,
  };
}

/** (b) per-status counts + explicit terminal DID lists. */
function terminalStateReport(db: Database.Database): void {
  const counts = db
    .prepare(
      'SELECT status, COUNT(*) AS n FROM repos GROUP BY status ORDER BY n DESC',
    )
    .all() as Array<{
    status: string;
    n: number;
  }>;
  const total = counts.reduce((acc, row) => acc + row.n, 0);
  logger.info(
    {
      total,
      byStatus: Object.fromEntries(counts.map((row) => [row.status, row.n])),
    },
    'ledger status counts',
  );

  const listStmt = db.prepare(
    'SELECT did, error FROM repos WHERE status = ? ORDER BY did LIMIT ?',
  );
  for (const status of TERMINAL_REPORT_STATUSES) {
    const totalForStatus = counts.find((row) => row.status === status)?.n ?? 0;
    if (totalForStatus === 0) continue;
    const dids = listStmt.all(status, DID_LIST_CAP) as Array<{
      did: string;
      error: string | null;
    }>;
    logger.warn(
      { status, total: totalForStatus, listed: dids.length, dids },
      `${status} repos (first ${DID_LIST_CAP} of ${totalForStatus})`,
    );
  }
}

// --- (c) sample re-fetch -----------------------------------------------------

type RefetchPostRows = (did: string, pdsHost: string) => Promise<PostRow[]>;

const refetchPostRows: RefetchPostRows = async (did, pdsHost) => {
  const fetched = await fetchRepoCar(pdsHost, did);
  const parsed = parseRepoCar(fetched.body);
  // Materializing is fine here: --sample touches a handful of repos, one at a
  // time, and the rkey-set comparison below needs the whole repo anyway.
  const rows: PostRow[] = [];
  for await (const row of repoPostRows(did, parsed, Date.now() * 1000)) {
    rows.push(row);
  }
  return rows;
};

/** Random draw across all loaded/verified repos (the undirected --sample pool). */
function selectRandomRepos(
  db: Database.Database,
  sampleSize: number,
): LedgerRepo[] {
  return db
    .prepare(
      "SELECT did, status, posts_total, pds_host, rev, rkey_digest FROM repos WHERE status IN ('loaded', 'verified') ORDER BY RANDOM() LIMIT ?",
    )
    .all(sampleSize) as LedgerRepo[];
}

/**
 * Unbiased pick of up to `n` from a pool (partial Fisher-Yates). Returns the
 * whole pool when n >= size — so `--sample-loose all` (n = Infinity) visits the
 * entire ambiguous set with no random gaps (the strongest practical check; see
 * the re-fetch caveat in the header — not a formal proof). Sampled DIDs are
 * emitted in the per-repo log lines below, so any run is auditable.
 */
function pickSample(pool: LedgerRepo[], n: number): LedgerRepo[] {
  if (n >= pool.length) return pool;
  const arr = pool.slice();
  for (let i = 0; i < n; i += 1) {
    const j = i + Math.floor(Math.random() * (arr.length - i));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr.slice(0, n);
}

async function sampleVerifyRepos(
  ch: ClickHouseClient,
  repos: LedgerRepo[],
  pool: string,
  telemetry: VerifyTelemetry,
  base: VerifyProgress,
): Promise<number> {
  const refetch = refetchPostRows;
  logger.info(
    { sampled: repos.length, pool },
    'sample verification: re-fetching repos for exact rkey-superset check',
  );

  let failures = 0;
  for (let i = 0; i < repos.length; i += 1) {
    const repo = repos[i];
    try {
      const rows = await refetch(repo.did, repo.pds_host);
      const fetchedRkeys = new Set(rows.map((row) => row.rkey));

      const result = await ch.query({
        // Src-agnostic, same reasoning as reconciliation: live can win the merge.
        query: `
          SELECT rkey
          FROM posts
          WHERE did = {did:String}
          GROUP BY rkey
          ORDER BY rkey
        `,
        query_params: { did: repo.did },
        clickhouse_settings: VERIFY_QUERY_SETTINGS,
        format: 'JSONEachRow',
      });
      const chRkeys = new Set(
        (await result.json<{ rkey: string }>()).map((row) => row.rkey),
      );

      const missingInCh = [...fetchedKeysDiff(fetchedRkeys, chRkeys)];
      const extraInCh = [...fetchedKeysDiff(chRkeys, fetchedRkeys)];
      // The invariant: CH must be a SUPERSET of any later CAR fetch, because
      // nothing is ever deleted (emojitracker semantics — posts count as they
      // happened). The two divergence directions therefore mean opposite
      // things and only one of them is loss.
      if (extraInCh.length > 0) {
        // In CH but gone from the fresh CAR = deleted from the repo since the
        // crawl. Keeping those rows is the design, not a defect.
        logger.info(
          {
            did: repo.did,
            refetched: fetchedRkeys.size,
            clickhouse: chRkeys.size,
            deletedSinceCrawl: extraInCh.length,
            extraInClickhouse: extraInCh.slice(0, 20),
          },
          'sample repo: ClickHouse keeps posts deleted from the repo since the crawl',
        );
      }
      if (missingInCh.length === 0) {
        logger.info(
          { did: repo.did, posts: chRkeys.size },
          extraInCh.length === 0
            ? 'sample repo post sets identical'
            : 'sample repo: ClickHouse is a superset of the fresh CAR',
        );
      } else {
        failures += 1;
        // In the fresh CAR but absent from CH = potential loss; the one benign
        // edge is a post created seconds before sampling that still sits in
        // the live writer's 1s flush buffer — judge a tiny tail by eye.
        logger.error(
          {
            did: repo.did,
            refetched: fetchedRkeys.size,
            clickhouse: chRkeys.size,
            missingInClickhouse: missingInCh.slice(0, 20),
          },
          'sample repo posts missing from ClickHouse',
        );
      }
    } catch (err) {
      failures += 1;
      logger.error(
        {
          did: repo.did,
          err: err instanceof Error ? err.message : String(err),
        },
        'sample re-fetch failed',
      );
    }
    await telemetry.record({
      ...base,
      phase: `sampling-${pool}`,
      sampleChecked: i + 1,
      sampleFailures: failures,
      done: false,
    });
  }
  return failures;
}

function* fetchedKeysDiff(a: Set<string>, b: Set<string>): Generator<string> {
  for (const key of a) if (!b.has(key)) yield key;
}

// --- entrypoint ---------------------------------------------------------------

async function main(): Promise<void> {
  const { values } = parseArgs({
    options: {
      sample: { type: 'string' },
      'sample-loose': { type: 'string' },
      'emit-loose': { type: 'string' },
      orphans: { type: 'boolean', default: false },
      // Skip the full ledger↔CH reconciliation (a) — for a fast, CH-light random
      // --sample run while the crawl is still loading (reconcile still scans
      // ClickHouse for every staged DID). Incompatible
      // with --sample-loose / --emit-loose, which are derived FROM reconciliation.
      'no-reconcile': { type: 'boolean', default: false },
      'terminal-report': { type: 'boolean', default: false },
      'loaded-only': { type: 'boolean', default: false },
    },
  });
  const noReconcile = values['no-reconcile'] ?? false;
  const loadedOnly = values['loaded-only'] ?? false;
  const sampleSize = values.sample === undefined ? null : Number(values.sample);
  if (
    sampleSize !== null &&
    (!Number.isInteger(sampleSize) || sampleSize < 1)
  ) {
    logger.error(
      { sample: values.sample },
      '--sample expects a positive integer',
    );
    process.exitCode = 2;
    return;
  }
  // --sample-loose N | all: directed re-fetch of the ambiguous LOOSE set.
  // 'all' (=> Infinity) makes pickSample return the whole set (exhaustive).
  let sampleLooseSize: number | null = null;
  if (values['sample-loose'] !== undefined) {
    if (values['sample-loose'] === 'all') {
      sampleLooseSize = Number.POSITIVE_INFINITY;
    } else {
      sampleLooseSize = Number(values['sample-loose']);
      // Number.isInteger(Infinity) is false, so 'Infinity'/'1e309' fall through
      // to this error — only the literal 'all' opts into the exhaustive pass.
      if (!Number.isInteger(sampleLooseSize) || sampleLooseSize < 1) {
        logger.error(
          { 'sample-loose': values['sample-loose'] },
          "--sample-loose expects a positive integer or 'all'",
        );
        process.exitCode = 2;
        return;
      }
    }
  }
  const emitLoose = values['emit-loose'];
  if (noReconcile && (sampleLooseSize !== null || emitLoose !== undefined)) {
    logger.error(
      {},
      '--no-reconcile cannot be combined with --sample-loose/--emit-loose (both derive from the reconciliation pass)',
    );
    process.exitCode = 2;
    return;
  }
  if (noReconcile && sampleSize === null) {
    logger.error({}, '--no-reconcile needs --sample N (nothing else to do)');
    process.exitCode = 2;
    return;
  }
  if (loadedOnly && !/^fail-shard\d+$/.test(VERIFY_SHARD)) {
    logger.error(
      { shard: VERIFY_SHARD },
      "--loaded-only is a partial run; set VERIFY_SHARD to a non-canonical label matching 'fail-shardN' so the dashboard does not replace the full-shard verification total",
    );
    process.exitCode = 2;
    return;
  }
  if (loadedOnly && (values.orphans ?? false)) {
    logger.error(
      {},
      '--loaded-only cannot be combined with --orphans because a partial expected set would report the rest of the backfill as orphaned',
    );
    process.exitCode = 2;
    return;
  }
  if (!VERIFY_RUN_ID_EXPLICIT) {
    logger.warn(
      { runId: VERIFY_RUN_ID },
      'VERIFY_RUN_ID was not set; generated a per-process run id, which is unsafe for multi-shard/orphan audits',
    );
  }

  if (!existsSync(LEDGER_DB_PATH)) {
    logger.error(
      { ledger: LEDGER_DB_PATH },
      'ledger database not found — nothing to verify',
    );
    process.exitCode = 2;
    return;
  }

  const db = new Database(LEDGER_DB_PATH);
  db.pragma('busy_timeout = 5000');
  const ch = createClickHouseClient(
    'emojistats-backfill-verify',
    VERIFY_CLICKHOUSE_REQUEST_TIMEOUT_MS,
  );
  const telemetry = new VerifyTelemetry(
    ch,
    VERIFY_RUN_ID,
    VERIFY_SHARD,
    LEDGER_DB_PATH,
  );
  let latestProgress: VerifyProgress = {
    phase: 'starting',
    reposTotal: 0,
    reposChecked: 0,
    exact: 0,
    loose: 0,
    mismatches: 0,
    looseEmitted: 0,
    sampleChecked: 0,
    sampleFailures: 0,
    done: false,
  };

  try {
    await pingClickHouse(ch);
    await telemetry.ensureTable();
    await telemetry.record(latestProgress, true);

    let exact = 0;
    let loose = 0;
    let promotionStage: LoadedPromotionStage | null = null;
    let looseRepos: LedgerRepo[] = [];
    let mismatchCount = 0;
    if (!noReconcile) {
      let looseEmitted = 0;
      ({
        exact,
        loose,
        promotionStage,
        looseRepos,
        looseEmitted,
        mismatchCount,
      } = await reconcile(db, ch, telemetry, {
        emitLoosePath: emitLoose,
        collectLooseRepos: sampleLooseSize !== null,
        includeOrphans: values.orphans ?? false,
        loadedOnly,
        promoteLoose: false,
      }));
      latestProgress = {
        phase: 'reconciled',
        reposTotal: exact + loose + mismatchCount,
        reposChecked: exact + loose + mismatchCount,
        exact,
        loose,
        mismatches: mismatchCount,
        looseEmitted,
        sampleChecked: 0,
        sampleFailures: 0,
        done: false,
      };
      if (values['terminal-report'] ?? false) terminalStateReport(db);

      if (emitLoose !== undefined) {
        logger.info(
          { path: emitLoose, dids: looseEmitted },
          'wrote LOOSE DID list — re-fetch at scale with: crawl --did-file <path>, then re-run verify',
        );
        latestProgress = {
          ...latestProgress,
          phase: 'loose-emitted',
          looseEmitted,
        };
        await telemetry.record(latestProgress, true);
      }
    }

    let sampleFailures = 0;
    if (sampleSize !== null)
      sampleFailures += await sampleVerifyRepos(
        ch,
        selectRandomRepos(db, sampleSize),
        'random',
        telemetry,
        latestProgress,
      );
    if (sampleLooseSize !== null) {
      if (looseRepos.length === 0)
        logger.info(
          {},
          'no LOOSE repos to directed-sample — reconciliation left nothing ambiguous',
        );
      else
        sampleFailures += await sampleVerifyRepos(
          ch,
          pickSample(looseRepos, sampleLooseSize),
          'loose',
          telemetry,
          latestProgress,
        );
    }
    if (
      !noReconcile &&
      sampleLooseSize === Number.POSITIVE_INFINITY &&
      mismatchCount === 0 &&
      sampleFailures === 0
    ) {
      await telemetry.record(
        {
          ...latestProgress,
          phase: 'promoting-loose',
          sampleFailures,
          done: false,
        },
        true,
      );
      if (promotionStage === null) {
        throw new Error('cannot promote LOOSE repos without reconciliation');
      }
      const promoted = promoteLoadedReposByDid(
        db,
        await collectPromotableLoadedDids(ch, true),
        promotionStage,
      );
      logger.info(
        { promotedToVerified: promoted },
        'exhaustive loose sample passed; promoted remaining loaded repos that classified exact or loose',
      );
    } else if (!noReconcile && sampleLooseSize !== null && loose > 0) {
      logger.warn(
        { loose, sampleLoose: values['sample-loose'] },
        "LOOSE repos were not promoted during reconciliation; only '--sample-loose all' with zero sample failures promotes them",
      );
    } else if (!noReconcile && emitLoose !== undefined && loose > 0) {
      logger.info(
        { loose },
        'LOOSE repos were emitted for recrawl and left unpromoted until a follow-up verify clears them',
      );
    } else if (!noReconcile && loose > 0) {
      logger.warn(
        { loose },
        "LOOSE repos were not promoted; only '--sample-loose all' with zero sample failures promotes them",
      );
    }
    latestProgress = {
      ...latestProgress,
      phase: 'finished',
      sampleFailures,
    };

    if (mismatchCount > 0 || sampleFailures > 0) {
      logger.error(
        { exact, loose, failed: mismatchCount, sampleFailures },
        'verification FAILED',
      );
      await telemetry.finish(latestProgress);
      process.exitCode = 1;
    } else if (noReconcile) {
      // Reconciliation was skipped — do NOT claim ledger/CH agreement.
      logger.info(
        { sampleFailures: 0 },
        'sample check passed: every re-fetched repo is a subset of ClickHouse — NOTE --no-reconcile skipped the full ledger↔CH pass, so this is an early indicator, not a verify',
      );
      await telemetry.finish(latestProgress);
    } else {
      logger.info(
        { exact, loose, failed: 0 },
        'verification passed: reconciliation found no CH<ledger or balanced-divergent losses (LOOSE is a lower bound — use --sample-loose for the directed re-fetch check)',
      );
      await telemetry.finish(latestProgress);
    }
  } catch (err) {
    await telemetry.fail(err).catch(() => undefined);
    throw err;
  } finally {
    db.close();
    await ch.close();
  }
}

main().catch((err: unknown) => {
  logger.error(
    { err: err instanceof Error ? (err.stack ?? err.message) : String(err) },
    'verify crashed',
  );
  process.exitCode = 2;
});
