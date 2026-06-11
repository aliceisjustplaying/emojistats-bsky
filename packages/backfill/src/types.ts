import type { PostRow } from 'ingest/types';

/**
 * Repo lifecycle (plan 0001):
 *   pending → fetching → loaded → verified
 * Terminal states:
 *   empty       — repo exists but has zero app.bsky.feed.post records
 *   tombstoned  — PLC tombstone (account deleted at identity layer)
 *   deactivated / takendown — PDS refused with the corresponding error
 *   quarantined — CAR/CBOR was malformed or exceeded caps; never crash the worker
 *   failed      — exhausted MAX_ATTEMPTS on errors that aren't clearly transient
 * Retryable parking state:
 *   unreachable — PDS down/timeout; retried in waves while attempts <
 *                 MAX_ATTEMPTS, then parked as the explicit final-sweep list.
 *                 Never flips to 'failed' on attempts exhaustion: host down ≠
 *                 data gone, and 'failed' would hide the repo from the sweep.
 */
/**
 * THE status registry — telemetry columns, dashboard panels and the ledger all
 * derive from this list; never copy it.
 */
export const REPO_STATUSES = [
  'pending',
  'fetching',
  'loaded',
  'verified',
  'empty',
  'tombstoned',
  'deactivated',
  'takendown',
  'unreachable',
  'quarantined',
  'failed',
] as const;

export type RepoStatus = (typeof REPO_STATUSES)[number];

export interface RepoRow {
  did: string;
  pdsHost: string;
  status: RepoStatus;
  rev: string | null;
  carBytes: number | null;
  recordsTotal: number | null;
  postsTotal: number | null;
  postsWithEmojis: number | null;
  emojiOccurrences: number | null;
  rkeyDigest: string | null;
  attempts: number;
  error: string | null;
  enumeratedAt: number;
  fetchedAt: number | null;
  loadedAt: number | null;
  retryAfter: number | null;
}

/** Counts a fetched-and-parsed repo reports into the ledger when marked loaded. */
export interface RepoCounts {
  rev: string | null;
  carBytes: number;
  recordsTotal: number;
  postsTotal: number;
  postsWithEmojis: number;
  emojiOccurrences: number;
  /**
   * Order-independent fingerprint of the loaded rkey set: 64-bit XOR fold of
   * sha256(rkey) bytes 0..7 read little-endian, stored as fixed-width lowercase
   * hex. A bare count() can pass while one CAR post is lost and one live-only
   * post arrives (plan 0001 cares about exactly this); the digest catches the
   * swap. Hex string rather than a number: sqlite INTEGER is signed 64-bit and
   * JS numbers lose precision past 2^53. Null on rows loaded before the column
   * existed — verify treats those as loose-eligible only.
   */
  rkeyDigest: string | null;
}

/**
 * The ledger is the crawl's only checkpoint: kill the process at any moment and
 * nothing is lost — in-flight repos simply re-fetch (loads are idempotent).
 * Implemented over better-sqlite3, fully synchronous, schema in ledger.sql.
 */
export interface Ledger {
  // enumeration
  upsertPending(did: string, pdsHost: string): void;
  markTombstoned(did: string): void;
  getMeta(key: string): string | undefined;
  setMeta(key: string, value: string): void;

  // crawl
  /**
   * Claimable = pending, or unreachable whose retryAfter is due AND whose
   * attempts are still within MAX_ATTEMPTS — past the budget the row parks
   * (still 'unreachable') for the final sweep. Ordered to spread hosts.
   */
  listClaimable(limit: number): RepoRow[];
  /** Guarded transition to 'fetching'; false if someone else claimed it. */
  markFetching(did: string): boolean;
  markLoaded(did: string, counts: RepoCounts): void;
  markRetry(did: string, error: string, retryAfterMs: number): void;
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
  ): void;
  markVerified(did: string): void;
  /** Final sweep: zero attempts on parked unreachable rows (shard-scoped); returns rows reset. */
  resetUnreachableAttempts(): number;
  /** Follow an account migration discovered after enumeration (stale PLC pointer). */
  updateHost(did: string, pdsHost: string): void;

  // reporting (dashboard + verify read these) — a sharded ledger instance
  // reports only its own shard slice, so per-shard numbers sum cleanly; with
  // shards=1 (the default) these are ledger-wide.
  statusCounts(): Partial<Record<RepoStatus, number>>;
  loadedSince(sinceMs: number): number;
  totalPostsLoaded(): number;
  lastError(): { did: string; error: string } | null;
  getRepo(did: string): RepoRow | undefined;
  iterateByStatus(status: RepoStatus): IterableIterator<RepoRow>;
  close(): void;
}

/**
 * One repo's in-flight load: addRow buffers and inserts a chunk whenever
 * LOADER_CHUNK_ROWS accumulate; finish flushes the final partial chunk. The
 * ledger must flip to 'loaded' only after finish() resolves.
 */
export interface RepoLoad {
  addRow(row: PostRow): Promise<void>;
  finish(): Promise<void>;
}

/**
 * Streaming chunked ClickHouse inserts with per-chunk
 * insert_deduplication_token = `${did}:${rev}:${chunkRows}:${chunkIdx}` —
 * which is why rev must be known when the handle opens, before the first chunk
 * insert. Tokens are stable across re-fetches: rows arrive in MST walk order
 * (deterministic for a given commit), so identical (did, rev) re-fetches at
 * the same chunk size produce byte-identical chunks under identical tokens.
 * The chunk size is part of the token because it shapes the chunks: changing
 * it mid-crawl changes every token, causing harmless re-inserts (collapsed by
 * ReplacingMergeTree) instead of silently skipping chunks that now cover
 * different rows.
 */
export interface RepoLoader {
  openRepo(did: string, rev: string | null): RepoLoad;
}
