import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import Database from 'better-sqlite3';

import { LEDGER_DB_PATH, MAX_ATTEMPTS } from './config.js';
import type { Ledger, RepoCounts, RepoRow, RepoStatus } from './types.js';

const SCHEMA_PATH = fileURLToPath(new URL('./ledger.sql', import.meta.url));

interface RepoTableRow {
  did: string;
  pds_host: string;
  status: string;
  rev: string | null;
  car_bytes: number | null;
  records_total: number | null;
  posts_total: number | null;
  posts_with_emojis: number | null;
  emoji_occurrences: number | null;
  rkey_digest: string | null;
  attempts: number;
  error: string | null;
  enumerated_at: number;
  fetched_at: number | null;
  loaded_at: number | null;
  retry_after: number | null;
}

function toRepoRow(row: RepoTableRow): RepoRow {
  return {
    did: row.did,
    pdsHost: row.pds_host,
    status: row.status as RepoStatus,
    rev: row.rev,
    carBytes: row.car_bytes,
    recordsTotal: row.records_total,
    postsTotal: row.posts_total,
    postsWithEmojis: row.posts_with_emojis,
    emojiOccurrences: row.emoji_occurrences,
    rkeyDigest: row.rkey_digest,
    attempts: row.attempts,
    error: row.error,
    enumeratedAt: row.enumerated_at,
    fetchedAt: row.fetched_at,
    loadedAt: row.loaded_at,
    retryAfter: row.retry_after,
  };
}

export interface SqliteLedgerOptions {
  /** Total shard count; 1 (default) disables shard filtering entirely. */
  shards?: number;
  /** This process's shard, 0-based; rows hashing elsewhere are invisible — neither claimable nor counted. */
  shardIndex?: number;
}

/**
 * Deterministic DID → shard bucket. Mixes three character positions of the
 * DID's base32 tail (single positions skew on power-of-two shard counts; odd
 * multipliers keep every position contributing); guards degenerate short DIDs
 * so no row can hash to nowhere. The result is PERSISTED in repos.bucket —
 * launch night taught us that evaluating this per row inside the claim query
 * is a full-table scan that grows with enumeration and eats the main thread.
 *
 * The modulus is fixed at write time: changing the fleet's shard count means
 * `UPDATE repos SET bucket = ...` with the new modulus (minutes), and the
 * constructor refuses mismatched shard counts so it can't happen silently.
 */
export const BUCKET_MODULUS = 6;

export function bucketOf(did: string): number {
  const n = did.length;
  const c1 = n >= 1 ? did.charCodeAt(n - 1) : 0;
  const c2 = n >= 2 ? did.charCodeAt(n - 2) : 0;
  const c3 = n >= 3 ? did.charCodeAt(n - 3) : 0;
  return (c1 + c2 * 31 + c3 * 961) % BUCKET_MODULUS;
}

/** SQL twin of bucketOf, used once per ledger open to backfill missing buckets. */
const BUCKET_BACKFILL_SQL = `
  UPDATE repos SET bucket = (
    COALESCE(unicode(substr(did, -1)), 0)
    + COALESCE(unicode(substr(did, -2, 1)), 0) * 31
    + COALESCE(unicode(substr(did, -3, 1)), 0) * 961
  ) % ${BUCKET_MODULUS}
  WHERE bucket IS NULL
`;

export class SqliteLedger implements Ledger {
  private readonly db: Database.Database;
  private readonly shardAnd: string;
  private readonly stmtUpsertPending: Database.Statement;
  private readonly stmtMarkTombstoned: Database.Statement;
  private readonly stmtGetMeta: Database.Statement;
  private readonly stmtSetMeta: Database.Statement;
  private readonly stmtListClaimablePending: Database.Statement;
  private readonly stmtListClaimableRetry: Database.Statement;
  private readonly stmtMarkFetching: Database.Statement;
  private readonly stmtMarkLoaded: Database.Statement;
  private readonly stmtMarkRetry: Database.Statement;
  private readonly stmtMarkThrottled: Database.Statement;
  private readonly stmtMarkTerminal: Database.Statement;
  private readonly stmtMarkVerified: Database.Statement;
  private readonly stmtStatusCounts: Database.Statement;
  private readonly stmtLoadedSince: Database.Statement;
  private readonly stmtTotalPosts: Database.Statement;
  private readonly stmtLastError: Database.Statement;
  private readonly stmtGetRepo: Database.Statement;
  private readonly stmtByStatus: Database.Statement;
  private readonly stmtResetUnreachable: Database.Statement;
  private readonly claimableExclusionStatements = new Map<
    number,
    { pending: Database.Statement; retry: Database.Statement }
  >();

  constructor(
    dbPath: string = LEDGER_DB_PATH,
    options: SqliteLedgerOptions = {},
  ) {
    const shards = options.shards ?? 1;
    const shardIndex = options.shardIndex ?? 0;
    if (!Number.isInteger(shards) || shards < 1) {
      throw new Error(`shards must be a positive integer, got ${shards}`);
    }
    if (
      !Number.isInteger(shardIndex) ||
      shardIndex < 0 ||
      shardIndex >= shards
    ) {
      throw new Error(
        `shardIndex must be in [0, ${shards}), got ${shardIndex}`,
      );
    }
    if (shards > 1 && shards !== BUCKET_MODULUS) {
      throw new Error(
        `shards=${shards} does not match the persisted bucket modulus ` +
          `${BUCKET_MODULUS}; recompute repos.bucket before changing the fleet size`,
      );
    }
    // Validated integers baked into the prepared statements. The filter scopes
    // claims AND reporting (statusCounts / totalPostsLoaded / loadedSince /
    // iterateByStatus): a sharded instance sees only its own slice, so per-shard
    // telemetry sums exactly across the fleet and the idle policy can never be
    // pinned open by other shards' rows. Writes stay unfiltered — they are
    // keyed by DID. shards=1 (the default; status/verify/enumerate construct it
    // that way) compiles the filter away, keeping global tools global.
    const shardPredicate = `bucket = ${shardIndex}`;
    const shardAnd = shards > 1 ? `AND ${shardPredicate}` : '';
    const shardWhere = shards > 1 ? `WHERE ${shardPredicate}` : '';
    this.shardAnd = shardAnd;

    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.exec(fs.readFileSync(SCHEMA_PATH, 'utf8'));

    // Additive migration for ledgers from before the persisted bucket column;
    // the backfill UPDATE is a one-time full scan (seconds), then a no-op.
    const hasBucket = (
      this.db.pragma('table_info(repos)') as Array<{ name: string }>
    ).some((col) => col.name === 'bucket');
    if (!hasBucket) this.db.exec('ALTER TABLE repos ADD COLUMN bucket INTEGER');
    this.db.exec(BUCKET_BACKFILL_SQL);
    // (status, bucket, did): claims become an index seek in did order — DIDs
    // are random base32, so did order doubles as statistically fair host
    // rotation. (bucket, status): shard-scoped status aggregation.
    this.db.exec(
      'CREATE INDEX IF NOT EXISTS idx_repos_claim ON repos (status, bucket, did)',
    );
    this.db.exec(
      'CREATE INDEX IF NOT EXISTS idx_repos_bucket_status ON repos (bucket, status)',
    );

    // Account migrations during enumeration: a later op may move a still-pending
    // DID to a new PDS. Rows already past 'pending' are never clobbered.
    this.stmtUpsertPending = this.db.prepare(`
      INSERT INTO repos (did, pds_host, status, enumerated_at, bucket) VALUES (?, ?, 'pending', ?, ?)
      ON CONFLICT (did) DO UPDATE SET
        pds_host = excluded.pds_host,
        enumerated_at = excluded.enumerated_at
      WHERE repos.status = 'pending' AND repos.pds_host <> excluded.pds_host
    `);

    // Already-loaded rows keep their data (emojitracker semantics: posts count
    // as they happened, even if the account is later deleted at the PLC layer).
    this.stmtMarkTombstoned = this.db.prepare(`
      INSERT INTO repos (did, pds_host, status, enumerated_at, bucket) VALUES (?, '', 'tombstoned', ?, ?)
      ON CONFLICT (did) DO UPDATE SET status = 'tombstoned', retry_after = NULL
      WHERE repos.status NOT IN ('loaded', 'verified')
    `);

    this.stmtGetMeta = this.db.prepare('SELECT value FROM meta WHERE key = ?');
    this.stmtSetMeta = this.db.prepare(`
      INSERT INTO meta (key, value) VALUES (?, ?)
      ON CONFLICT (key) DO UPDATE SET value = excluded.value
    `);

    // Claims are an index seek on (status, bucket, did) in did order — DIDs
    // are random base32, so a did-ordered batch is a statistically fair host
    // mix and the per-host limiters do the actual anti-hogging. (The previous
    // strict-rotation window function ranked EVERY claimable row per call: a
    // full-table scan that grew with enumeration and ate the main thread.)
    // Unreachable rows are offered only while attempts < MAX_ATTEMPTS — past
    // the budget they park (never flipped to 'failed': host down ≠ data gone)
    // as the explicit final-sweep list, and the scheduler's idle policy ends
    // the run instead of hammering dead hosts.
    this.stmtListClaimablePending = this.db.prepare(`
      SELECT * FROM repos
      WHERE status = 'pending' ${shardAnd}
      ORDER BY did
      LIMIT ?
    `);
    this.stmtListClaimableRetry = this.db.prepare(`
      SELECT * FROM repos
      WHERE status = 'unreachable' AND retry_after <= ? AND attempts < ? ${shardAnd}
      ORDER BY did
      LIMIT ?
    `);

    this.stmtMarkFetching = this.db.prepare(`
      UPDATE repos SET status = 'fetching', fetched_at = ?
      WHERE did = ? AND status IN ('pending', 'unreachable')
    `);

    this.stmtMarkLoaded = this.db.prepare(`
      UPDATE repos SET
        status = 'loaded', rev = ?, car_bytes = ?, records_total = ?, posts_total = ?,
        posts_with_emojis = ?, emoji_occurrences = ?, rkey_digest = ?, loaded_at = ?,
        error = NULL, retry_after = NULL
      WHERE did = ?
    `);

    this.stmtMarkRetry = this.db.prepare(`
      UPDATE repos SET status = 'unreachable', attempts = attempts + 1, error = ?, retry_after = ?
      WHERE did = ?
    `);

    // 429 path: parks for the backoff like markRetry but does NOT burn an
    // attempt — rate limiting is evidence of our pressure (handled by the
    // host cooldown), not of the repo being gone. Burning attempts during a
    // 429 storm mass-parked repos behind the final-sweep fence.
    this.stmtMarkThrottled = this.db.prepare(`
      UPDATE repos SET status = 'unreachable', error = ?, retry_after = ?
      WHERE did = ?
    `);

    // COALESCE keeps the last recorded error when markTerminal is called without one
    // (e.g. 'failed' after exhausting retries — the markRetry error is the diagnosis).
    this.stmtMarkTerminal = this.db.prepare(`
      UPDATE repos SET status = ?, error = COALESCE(?, error), retry_after = NULL
      WHERE did = ?
    `);

    this.stmtMarkVerified = this.db.prepare(
      "UPDATE repos SET status = 'verified' WHERE did = ?",
    );

    // Final sweep (--final-sweep): parked unreachables become claimable again by
    // zeroing the budget listClaimable checks. Shard-scoped like every other
    // read/claim path; error text is kept for the post-sweep report.
    this.stmtResetUnreachable = this.db.prepare(`
      UPDATE repos SET attempts = 0, retry_after = 0
      WHERE status = 'unreachable' ${shardAnd}
    `);

    this.stmtStatusCounts = this.db.prepare(
      `SELECT status, COUNT(*) AS n FROM repos ${shardWhere} GROUP BY status`,
    );
    this.stmtLoadedSince = this.db.prepare(
      `SELECT COUNT(*) AS n FROM repos WHERE loaded_at >= ? ${shardAnd}`,
    );
    this.stmtTotalPosts = this.db.prepare(
      `SELECT COALESCE(SUM(posts_total), 0) AS n FROM repos ${shardWhere}`,
    );

    // No updated_at column; retry_after/fetched_at/enumerated_at approximate write recency
    // well enough for a one-glance "what broke last" readout.
    this.stmtLastError = this.db.prepare(`
      SELECT did, error FROM repos WHERE error IS NOT NULL
      ORDER BY COALESCE(retry_after, fetched_at, enumerated_at) DESC
      LIMIT 1
    `);

    this.stmtGetRepo = this.db.prepare('SELECT * FROM repos WHERE did = ?');
    this.stmtByStatus = this.db.prepare(
      `SELECT * FROM repos WHERE status = ? ${shardAnd}`,
    );
  }

  /** Not part of the Ledger contract: batches many writes (e.g. one export page) into one commit. */
  transaction<T>(fn: () => T): T {
    return this.db.transaction(fn)();
  }

  upsertPending(did: string, pdsHost: string): void {
    this.stmtUpsertPending.run(did, pdsHost, Date.now(), bucketOf(did));
  }

  markTombstoned(did: string): void {
    this.stmtMarkTombstoned.run(did, Date.now(), bucketOf(did));
  }

  getMeta(key: string): string | undefined {
    const row = this.stmtGetMeta.get(key) as { value: string } | undefined;
    return row?.value;
  }

  setMeta(key: string, value: string): void {
    this.stmtSetMeta.run(key, value);
  }

  private claimableStatementsForExclusions(count: number): {
    pending: Database.Statement;
    retry: Database.Statement;
  } {
    const cached = this.claimableExclusionStatements.get(count);
    if (cached !== undefined) return cached;
    const placeholders = Array.from({ length: count }, () => '?').join(', ');
    const hostExclusion =
      count === 0 ? '' : `AND pds_host NOT IN (${placeholders})`;
    const statements = {
      pending: this.db.prepare(`
        SELECT * FROM repos
        WHERE status = 'pending' ${this.shardAnd} ${hostExclusion}
        ORDER BY did
        LIMIT ?
      `),
      retry: this.db.prepare(`
        SELECT * FROM repos
        WHERE status = 'unreachable' AND retry_after <= ? AND attempts < ? ${this.shardAnd} ${hostExclusion}
        ORDER BY did
        LIMIT ?
      `),
    };
    this.claimableExclusionStatements.set(count, statements);
    return statements;
  }

  listClaimable(
    limit: number,
    excludedHosts: readonly string[] = [],
  ): RepoRow[] {
    const uniqueExcludedHosts = [...new Set(excludedHosts)].filter(
      (host) => host.length > 0,
    );
    const statements =
      uniqueExcludedHosts.length === 0
        ? {
            pending: this.stmtListClaimablePending,
            retry: this.stmtListClaimableRetry,
          }
        : this.claimableStatementsForExclusions(uniqueExcludedHosts.length);
    const rows = statements.pending.all(
      ...uniqueExcludedHosts,
      limit,
    ) as RepoTableRow[];
    if (rows.length < limit) {
      rows.push(
        ...(statements.retry.all(
          Date.now(),
          MAX_ATTEMPTS,
          ...uniqueExcludedHosts,
          limit - rows.length,
        ) as RepoTableRow[]),
      );
    }
    return rows.map(toRepoRow);
  }

  markFetching(did: string): boolean {
    return this.stmtMarkFetching.run(Date.now(), did).changes === 1;
  }

  markLoaded(did: string, counts: RepoCounts): void {
    this.stmtMarkLoaded.run(
      counts.rev,
      counts.carBytes,
      counts.recordsTotal,
      counts.postsTotal,
      counts.postsWithEmojis,
      counts.emojiOccurrences,
      counts.rkeyDigest,
      Date.now(),
      did,
    );
  }

  markRetry(did: string, error: string, retryAfterMs: number): void {
    this.stmtMarkRetry.run(error, Date.now() + retryAfterMs, did);
  }

  markThrottled(did: string, error: string, retryAfterMs: number): void {
    this.stmtMarkThrottled.run(error, Date.now() + retryAfterMs, did);
  }

  // Rare path (retries only) — not worth a prepared-statement field.
  updateHost(did: string, pdsHost: string): void {
    this.db
      .prepare('UPDATE repos SET pds_host = ? WHERE did = ?')
      .run(pdsHost, did);
  }

  markTerminal(
    did: string,
    status: Extract<
      RepoStatus,
      | 'empty'
      | 'tombstoned'
      | 'deactivated'
      | 'takendown'
      | 'quarantined'
      | 'failed'
    >,
    error?: string,
  ): void {
    this.stmtMarkTerminal.run(status, error ?? null, did);
  }

  markVerified(did: string): void {
    this.stmtMarkVerified.run(did);
  }

  resetUnreachableAttempts(): number {
    return this.stmtResetUnreachable.run().changes;
  }

  statusCounts(): Partial<Record<RepoStatus, number>> {
    const rows = this.stmtStatusCounts.all() as {
      status: RepoStatus;
      n: number;
    }[];
    const counts: Partial<Record<RepoStatus, number>> = {};
    for (const row of rows) counts[row.status] = row.n;
    return counts;
  }

  loadedSince(sinceMs: number): number {
    return (this.stmtLoadedSince.get(sinceMs) as { n: number }).n;
  }

  totalPostsLoaded(): number {
    return (this.stmtTotalPosts.get() as { n: number }).n;
  }

  lastError(): { did: string; error: string } | null {
    const row = this.stmtLastError.get() as
      | { did: string; error: string }
      | undefined;
    return row ?? null;
  }

  getRepo(did: string): RepoRow | undefined {
    const row = this.stmtGetRepo.get(did) as RepoTableRow | undefined;
    return row === undefined ? undefined : toRepoRow(row);
  }

  *iterateByStatus(status: RepoStatus): IterableIterator<RepoRow> {
    for (const row of this.stmtByStatus.iterate(status)) {
      yield toRepoRow(row as RepoTableRow);
    }
  }

  close(): void {
    this.db.close();
  }
}
