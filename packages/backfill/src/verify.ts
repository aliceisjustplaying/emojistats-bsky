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
 *
 * Exit codes: 0 clean, 1 mismatches found, 2 could not run.
 *
 * The ledger is accessed directly over its frozen schema (src/ledger.sql) so
 * this CLI stays runnable independently of the crawler's ledger.ts module;
 * the only write it performs is the loaded → verified promotion, mirroring
 * Ledger.markVerified.
 */
import { existsSync } from 'node:fs';
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
}

interface ReconcileResult {
  exact: number;
  loose: number;
  mismatches: Mismatch[];
}

interface ChDidStats {
  posts: number;
  digest: string;
}

const RECONCILE_CHUNK = 1000;

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
  backfillDids: Set<string>,
): Promise<ReconcileResult> {
  const repos = db
    .prepare(
      `SELECT did, status, posts_total, pds_host, rev, rkey_digest
       FROM repos WHERE status IN ('loaded', 'verified')`,
    )
    .all() as LedgerRepo[];

  const chStats = await chStatsForDids(
    ch,
    repos.map((repo) => repo.did),
  );

  const mismatches: Mismatch[] = [];
  const passedLoadedDids: string[] = [];
  let exact = 0;
  let loose = 0;
  let alreadyVerified = 0;

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
    } else if (actual >= expected) {
      // LOOSE: the CAR is a lower bound — posts created after the fetch
      // legitimately push the CH count above posts_total (live path), which
      // also perturbs the digest. Only CH < ledger is loss.
      loose += 1;
      if (ledgerDigest !== null && actual === expected) {
        // Counts balance but the sets differ: either an offsetting lost-post /
        // live-arrival pair (the exact failure mode the digest exists to
        // catch) or live deletes. Promoted per the lower-bound contract, but
        // loudly — eyes on these.
        logger.warn(
          {
            did: repo.did,
            posts: actual,
            ledgerDigest,
            clickhouseDigest: stats?.digest,
          },
          'counts equal but rkey digests differ: set changed since the CAR fetch',
        );
      }
    } else {
      mismatches.push({
        did: repo.did,
        status: repo.status,
        ledgerPostsTotal: expected,
        clickhousePosts: actual,
      });
      continue;
    }
    if (repo.status === 'loaded') passedLoadedDids.push(repo.did);
    else alreadyVerified += 1;
  }

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
  const orphans = [...backfillDids].filter((did) => !ledgerDids.has(did));
  const orphanStats = await chStatsForDids(ch, orphans.slice(0, DID_LIST_CAP));

  logger.info(
    {
      reposChecked: repos.length,
      exact,
      loose,
      promotedToVerified: promoted,
      alreadyVerified,
      mismatches: mismatches.length,
      clickhouseOnlyDids: orphans.length,
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
      'count mismatch: ClickHouse count() < ledger posts_total',
    );
  }
  return { exact, loose, mismatches };
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

async function sampleVerify(
  db: Database.Database,
  ch: ClickHouseClient,
  sampleSize: number,
): Promise<number> {
  const refetch = refetchPostRows;
  const sampled = db
    .prepare(
      "SELECT did, status, posts_total, pds_host, rev FROM repos WHERE status IN ('loaded', 'verified') ORDER BY RANDOM() LIMIT ?",
    )
    .all(sampleSize) as LedgerRepo[];
  logger.info(
    { requested: sampleSize, sampled: sampled.length },
    'sample verification: re-fetching random repos',
  );

  let failures = 0;
  for (const repo of sampled) {
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
  }
  return failures;
}

function* fetchedKeysDiff(a: Set<string>, b: Set<string>): Generator<string> {
  for (const key of a) if (!b.has(key)) yield key;
}

// --- entrypoint ---------------------------------------------------------------

async function main(): Promise<void> {
  const { values } = parseArgs({ options: { sample: { type: 'string' } } });
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

  try {
    await pingClickHouse(ch);

    const backfillDids = await chBackfillDids(ch);
    const { exact, loose, mismatches } = await reconcile(db, ch, backfillDids);
    terminalStateReport(db);

    let sampleFailures = 0;
    if (sampleSize !== null)
      sampleFailures = await sampleVerify(db, ch, sampleSize);

    if (mismatches.length > 0 || sampleFailures > 0) {
      logger.error(
        { exact, loose, failed: mismatches.length, sampleFailures },
        'verification FAILED',
      );
      process.exitCode = 1;
    } else {
      logger.info(
        { exact, loose, failed: 0 },
        'verification passed: ledger and ClickHouse agree',
      );
    }
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
