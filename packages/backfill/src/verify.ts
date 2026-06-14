/**
 * Backfill acceptance-criteria engine (plan 0001). Run with `bun run verify`.
 *
 *   (a) Per-repo reconciliation, tiered: ledger posts_total + rkey_digest vs
 *       ClickHouse count() + groupBitXor rkey digest, batched per DID over
 *       every repo in status loaded/verified. EXACT = digests match and counts
 *       are equal; LOOSE = CH count >= ledger (today's lower-bound semantics);
 *       CH count < ledger is loss and fails the run. Both tiers promote to
 *       'verified' — tiering is report-level only, never a new status.
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
 *       for DISTINCT backfill DIDs and is deliberately off by default.
 *
 * Exit codes: 0 clean, 1 mismatches found, 2 could not run.
 *
 * The ledger is accessed directly over its frozen schema (src/ledger.sql) so
 * this CLI stays runnable independently of the crawler's ledger.ts module;
 * the only write it performs is the loaded → verified promotion, mirroring
 * Ledger.markVerified.
 */
import { existsSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { parseArgs } from 'node:util';

import type { ClickHouseClient } from '@clickhouse/client';
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
  // The LOOSE class itself (CH count >= ledger, divergent digest, expected > 0):
  // the only repos a digest reconciliation cannot prove, and therefore the only
  // ones a re-fetch needs to visit. Carried out so --sample-loose / --emit-loose
  // can target them directly. Empty-ledger (expected = 0) repos are excluded —
  // there is nothing there to have lost.
  looseRepos: LedgerRepo[];
  mismatches: Mismatch[];
}

interface ChDidStats {
  posts: number;
  digest: string;
}

const RECONCILE_CHUNK = 1000;

const VERIFY_RUN_ID =
  process.env.VERIFY_RUN_ID ??
  `verify-${new Date().toISOString().slice(0, 19).replace(/[-:T]/g, '')}`;
const VERIFY_SHARD =
  process.env.VERIFY_SHARD ??
  process.env.CRAWL_SHARD_INDEX ??
  path.basename(LEDGER_DB_PATH, '.sqlite');

interface VerifyProgress {
  phase: string;
  reposTotal: number;
  reposChecked: number;
  exact: number;
  loose: number;
  mismatches: number;
  looseEmitted: number;
  sampleChecked: number;
  sampleFailures: number;
  done: boolean;
  error?: string;
}

/** 'YYYY-MM-DD HH:MM:SS' UTC, the JSONEachRow-friendly DateTime form. */
function chDateTime(ms: number): string {
  return new Date(ms).toISOString().slice(0, 19).replace('T', ' ');
}

class VerifyTelemetry {
  #lastInsertMs = 0;
  #lastProgress: VerifyProgress | undefined;
  readonly #ch: ClickHouseClient;
  readonly #runId: string;
  readonly #shard: string;
  readonly #ledgerPath: string;

  constructor(
    ch: ClickHouseClient,
    runId: string,
    shard: string,
    ledgerPath: string,
  ) {
    this.#ch = ch;
    this.#runId = runId;
    this.#shard = shard;
    this.#ledgerPath = ledgerPath;
  }

  async ensureTable(): Promise<void> {
    await this.#ch.command({
      query: `
        CREATE TABLE IF NOT EXISTS backfill_verify_progress (
          ts              DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
          run_id          LowCardinality(String),
          shard           LowCardinality(String),
          ledger_path     String CODEC(ZSTD(1)),
          phase           LowCardinality(String),
          repos_total     UInt64,
          repos_checked   UInt64,
          exact           UInt64,
          loose           UInt64,
          mismatches      UInt64,
          loose_emitted   UInt64,
          sample_checked  UInt64,
          sample_failures UInt64,
          done            UInt8,
          error           String CODEC(ZSTD(3))
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(ts)
        ORDER BY (run_id, shard, ts)
        TTL ts + INTERVAL 6 MONTH DELETE
      `,
    });
  }

  async record(progress: VerifyProgress, force = false): Promise<void> {
    this.#lastProgress = progress;
    const now = Date.now();
    if (!force && now - this.#lastInsertMs < 5_000) return;
    this.#lastInsertMs = now;
    await this.#ch.insert({
      table: 'backfill_verify_progress',
      values: [
        {
          ts: chDateTime(now),
          run_id: this.#runId,
          shard: this.#shard,
          ledger_path: this.#ledgerPath,
          phase: progress.phase,
          repos_total: progress.reposTotal,
          repos_checked: progress.reposChecked,
          exact: progress.exact,
          loose: progress.loose,
          mismatches: progress.mismatches,
          loose_emitted: progress.looseEmitted,
          sample_checked: progress.sampleChecked,
          sample_failures: progress.sampleFailures,
          done: progress.done ? 1 : 0,
          error: progress.error ?? '',
        },
      ],
      format: 'JSONEachRow',
    });
  }

  async finish(progress: VerifyProgress): Promise<void> {
    await this.record({ ...progress, done: true }, true);
  }

  async fail(err: unknown): Promise<void> {
    await this.record(
      {
        ...(this.#lastProgress ?? {
          phase: 'failed',
          reposTotal: 0,
          reposChecked: 0,
          exact: 0,
          loose: 0,
          mismatches: 0,
          looseEmitted: 0,
          sampleChecked: 0,
          sampleFailures: 0,
          done: false,
        }),
        phase: 'failed',
        done: true,
        error: err instanceof Error ? err.message : String(err),
      },
      true,
    );
  }
}

// Src-agnostic on purpose: a post created during the crawl arrives via BOTH the
// live path and the repo CAR; whichever inserts later wins the ReplacingMergeTree
// merge and keeps its src label. Filtering on src='backfill' undercounts active
// repos (live can lag and win). The acceptance contract is "every post the CAR
// contained is in ClickHouse", regardless of which path carried it.
//
// The digest is the same 64-bit XOR fold pipeline.ts wrote to the ledger —
// digest.ts holds both sides and the bit-identity proof, so equal sets produce
// equal digests.
async function chStatsForDids(
  ch: ClickHouseClient,
  dids: string[],
  onProgress?: (reposChecked: number) => Promise<void>,
): Promise<Map<string, ChDidStats>> {
  const stats = new Map<string, ChDidStats>();
  for (let i = 0; i < dids.length; i += RECONCILE_CHUNK) {
    const result = await ch.query({
      query: `
        SELECT did, toUInt64(count()) AS posts,
               hex(${CH_RKEY_DIGEST_EXPR}) AS digest
        FROM posts FINAL WHERE did IN ({dids:Array(String)}) GROUP BY did
      `,
      query_params: { dids: dids.slice(i, i + RECONCILE_CHUNK) },
      format: 'JSONEachRow',
    });
    const rows = await result.json<{
      did: string;
      posts: string;
      digest: string;
    }>();
    for (const row of rows) {
      stats.set(row.did, {
        posts: Number(row.posts),
        digest: normalizeDigestHex(row.digest),
      });
    }
    await onProgress?.(Math.min(i + RECONCILE_CHUNK, dids.length));
  }
  return stats;
}

/** (a) ledger ↔ ClickHouse reconciliation; promotes both pass tiers to 'verified'. */
async function chBackfillDids(ch: ClickHouseClient): Promise<Set<string>> {
  const result = await ch.query({
    query: "SELECT DISTINCT did FROM posts WHERE src = 'backfill'",
    format: 'JSONEachRow',
  });
  return new Set((await result.json<{ did: string }>()).map((row) => row.did));
}

async function reconcile(
  db: Database.Database,
  ch: ClickHouseClient,
  telemetry: VerifyTelemetry,
  backfillDids?: Set<string>,
): Promise<ReconcileResult> {
  const repos = db
    .prepare(
      `SELECT did, status, posts_total, pds_host, rev, rkey_digest
       FROM repos WHERE status IN ('loaded', 'verified')`,
    )
    .all() as LedgerRepo[];

  await telemetry.record(
    {
      phase: 'querying-clickhouse',
      reposTotal: repos.length,
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

  const chStats = await chStatsForDids(
    ch,
    repos.map((repo) => repo.did),
    (reposChecked) =>
      telemetry.record({
        phase: 'querying-clickhouse',
        reposTotal: repos.length,
        reposChecked,
        exact: 0,
        loose: 0,
        mismatches: 0,
        looseEmitted: 0,
        sampleChecked: 0,
        sampleFailures: 0,
        done: false,
      }),
  );

  const mismatches: Mismatch[] = [];
  const passedLoadedDids: string[] = [];
  const looseRepos: LedgerRepo[] = [];
  let exact = 0;
  let loose = 0;
  let alreadyVerified = 0;

  await telemetry.record(
    {
      phase: 'classifying',
      reposTotal: repos.length,
      reposChecked: repos.length,
      exact,
      loose,
      mismatches: mismatches.length,
      looseEmitted: 0,
      sampleChecked: 0,
      sampleFailures: 0,
      done: false,
    },
    true,
  );

  for (const repo of repos) {
    const expected = repo.posts_total ?? 0;
    const stats = chStats.get(repo.did);
    const actual = stats?.posts ?? 0;
    const ledgerDigest =
      repo.rkey_digest === null ? null : normalizeDigestHex(repo.rkey_digest);

    if (
      ledgerDigest !== null &&
      stats !== undefined &&
      actual === expected &&
      stats.digest === ledgerDigest
    ) {
      // EXACT: ClickHouse holds precisely the rkey set the CAR contained — a
      // count alone can't say that (one lost CAR post + one live-only arrival
      // still balances; the digest does not).
      exact += 1;
    } else if (
      ledgerDigest !== null &&
      stats !== undefined &&
      expected > 0 &&
      actual === expected
    ) {
      // Counts balance exactly but the rkey sets differ (we're here, not in the
      // EXACT branch, so the digests are unequal). A dropped backfill row that
      // happened to be offset by a live-path arrival looks EXACTLY like this —
      // and so does a since-crawl delete paired with an arrival. XOR digests
      // can't separate the two, and we refuse to let live traffic mask a lost
      // backfill row, so this FAILS the run rather than promoting. The DID is
      // surfaced for `--sample` (exact rkey-set superset check) or re-crawl.
      mismatches.push({
        did: repo.did,
        status: repo.status,
        ledgerPostsTotal: expected,
        clickhousePosts: actual,
        reason: 'digest-mismatch',
        ledgerDigest,
        clickhouseDigest: stats.digest,
      });
      continue;
    } else if (actual >= expected) {
      // LOOSE: the CAR is a lower bound — posts created after the fetch
      // legitimately push the CH count above posts_total (live path), which
      // also perturbs the digest. CH count > ledger can't be set-checked from a
      // 64-bit XOR digest alone, so it stays a lower-bound pass; only CH <
      // ledger (handled below) or the balanced-but-divergent case (above) fail.
      // This is the residual blind spot — collect it (when there is data to
      // have lost) so --sample-loose / --emit-loose can re-fetch exactly here.
      loose += 1;
      if (expected > 0) looseRepos.push(repo);
    } else {
      mismatches.push({
        did: repo.did,
        status: repo.status,
        ledgerPostsTotal: expected,
        clickhousePosts: actual,
        reason: 'count-short',
      });
      continue;
    }
    if (repo.status === 'loaded') passedLoadedDids.push(repo.did);
    else alreadyVerified += 1;
  }

  await telemetry.record(
    {
      phase: 'promoting',
      reposTotal: repos.length,
      reposChecked: repos.length,
      exact,
      loose,
      mismatches: mismatches.length,
      looseEmitted: 0,
      sampleChecked: 0,
      sampleFailures: 0,
      done: false,
    },
    true,
  );

  const markVerified = db.prepare(
    "UPDATE repos SET status = 'verified' WHERE did = ? AND status = 'loaded'",
  );
  const promote = db.transaction((dids: string[]) => {
    let changed = 0;
    for (const did of dids) changed += markVerified.run(did).changes;
    return changed;
  });
  const promoted = promote(passedLoadedDids);

  // Repos that wrote backfill rows to ClickHouse but are not loaded/verified in
  // the ledger (crash between insert ack and markLoaded — harmless, the repo
  // will be re-fetched and its rows collapse, but worth surfacing). Restricted
  // to backfill-attributed DIDs: the ledger query above only covers its own.
  const ledgerDids = new Set(repos.map((repo) => repo.did));
  const orphans =
    backfillDids === undefined
      ? []
      : [...backfillDids].filter((did) => !ledgerDids.has(did));
  const orphanStats =
    orphans.length === 0
      ? new Map<string, ChDidStats>()
      : await chStatsForDids(ch, orphans.slice(0, DID_LIST_CAP));

  logger.info(
    {
      reposChecked: repos.length,
      exact,
      loose,
      promotedToVerified: promoted,
      alreadyVerified,
      mismatches: mismatches.length,
      clickhouseOnlyDids:
        backfillDids === undefined ? 'skipped' : orphans.length,
    },
    'reconciliation: exact = counts and rkey digests match; loose = CH count >= ledger only (usual benign cause: live-only posts arriving during/after the crawl; pre-digest ledger rows can never exceed loose)',
  );
  for (const orphan of orphans.slice(0, DID_LIST_CAP)) {
    logger.warn(
      { did: orphan, clickhousePosts: orphanStats.get(orphan)?.posts },
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
  return { exact, loose, looseRepos, mismatches };
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
        query: 'SELECT rkey FROM posts FINAL WHERE did = {did:String}',
        query_params: { did: repo.did },
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
      // --sample run while the crawl is still loading (reconcile fires ~1 query
      // per 1000 loaded DIDs with FINAL, heavy on a busy ClickHouse). Incompatible
      // with --sample-loose / --emit-loose, which are derived FROM reconciliation.
      'no-reconcile': { type: 'boolean', default: false },
    },
  });
  const noReconcile = values['no-reconcile'] ?? false;
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
  const ch = createClickHouseClient();
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
    let looseRepos: LedgerRepo[] = [];
    let mismatches: Mismatch[] = [];
    if (!noReconcile) {
      const backfillDids = values.orphans
        ? await chBackfillDids(ch)
        : undefined;
      ({ exact, loose, looseRepos, mismatches } = await reconcile(
        db,
        ch,
        telemetry,
        backfillDids,
      ));
      latestProgress = {
        phase: 'reconciled',
        reposTotal: exact + loose + mismatches.length,
        reposChecked: exact + loose + mismatches.length,
        exact,
        loose,
        mismatches: mismatches.length,
        looseEmitted: 0,
        sampleChecked: 0,
        sampleFailures: 0,
        done: false,
      };
      terminalStateReport(db);

      if (emitLoose !== undefined) {
        writeFileSync(
          emitLoose,
          looseRepos.map((repo) => repo.did).join('\n') + '\n',
        );
        logger.info(
          { path: emitLoose, dids: looseRepos.length },
          'wrote LOOSE DID list — re-fetch at scale with: crawl --did-file <path>, then re-run verify',
        );
        latestProgress = {
          ...latestProgress,
          phase: 'loose-emitted',
          looseEmitted: looseRepos.length,
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
    latestProgress = {
      ...latestProgress,
      phase: 'finished',
      sampleFailures,
    };

    if (mismatches.length > 0 || sampleFailures > 0) {
      logger.error(
        { exact, loose, failed: mismatches.length, sampleFailures },
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
