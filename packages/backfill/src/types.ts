import type { PostRow } from 'ingest/types';

/**
 * Repo lifecycle (plan 0001):
 *   pending → fetching → loaded → verified
 * Terminal states:
 *   empty       — repo exists but has zero app.bsky.feed.post records
 *   tombstoned  — PLC tombstone (account deleted at identity layer)
 *   deactivated / takendown — PDS refused with the corresponding error
 *   quarantined — CAR/CBOR was malformed or exceeded a configured safety valve;
 *                 never crash the worker
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
  /**
   * Exact-DID recrawls of already loaded/verified rows must refresh successful
   * loads without downgrading durable good ledger state on a transient failure.
   */
  preserveExisting?: boolean;
  /** Scheduler-internal: this repo already consumed a host rate-limit slot. */
  rateLimitReserved?: boolean;
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
  listClaimable(limit: number, excludedHosts?: readonly string[]): RepoRow[];
  /** Guarded transition to 'fetching'; false if someone else claimed it. */
  markFetching(did: string): boolean;
  markLoaded(did: string, counts: RepoCounts): void;
  markRetry(did: string, error: string, retryAfterMs: number): void;
  /** Parks like markRetry but without burning an attempt — the 429 path. */
  markThrottled(did: string, error: string, retryAfterMs: number): void;
  /**
   * Moves one repo to out-of-budget 'unreachable' immediately. Used when a host
   * is already dead for this run, or when final sweep refuses an absurd
   * Retry-After horizon.
   */
  parkUnreachable(did: string, error: string): void;
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
  /**
   * Final sweep: zero attempts on parked unreachable rows (shard-scoped);
   * dead-host rows stay parked by passing the dead-host registry as exclusions.
   * Returns rows reset.
   */
  resetUnreachableAttempts(excludedHosts?: readonly string[]): number;
  /**
   * Dead-host bulk park: moves up to `limit` PENDING rows on the host to
   * out-of-budget 'unreachable' (the final-sweep list). Returns rows changed;
   * callers loop until a short chunk, yielding between chunks.
   */
  parkDeadHostChunk(host: string, error: string, limit: number): number;
  /** Companion one-shot: zeroes the budget on the host's in-budget unreachable rows. */
  parkDeadHostUnreachable(host: string, error: string): number;
  /** Cross-process dead-host registry (ledger meta), written by the crawler, read by enumeration. */
  addDeadHost(host: string): void;
  getDeadHosts(): string[];
  /**
   * Final-sweep-only dead-host registry scoped to one BACKFILL_RUN_ID. These
   * hosts hit the sweep stop-loss and must stay parked across restarts of the
   * same run, but must not leak into later runs or enumeration.
   */
  addFinalSweepDeadHost(host: string, runId: string): void;
  getFinalSweepDeadHosts(runId: string): string[];
  /**
   * Revive path (--revive-host): drop a host from the dead registry so startup
   * re-seeding and the scan filter stop excluding it. No-op if absent. MUST be
   * paired with resetUnreachableForHost — removing the verdict alone leaves the
   * host's rows parked out-of-budget (the final-sweep gap this closes).
   */
  removeDeadHost(host: string): void;
  /**
   * Companion to removeDeadHost: zero attempts/retry_after on the host's parked
   * 'unreachable' rows (shard-scoped) so they become claimable again. Returns
   * rows reset. Scoped to one host — never the blanket resetUnreachableAttempts,
   * which would also revive genuinely-dead DNS/legal hosts.
   */
  resetUnreachableForHost(host: string): number;
  /** Drop one host from the current run's final-sweep dead-host registry. */
  removeFinalSweepDeadHost(host: string, runId: string): void;
  /** Enumeration insert path for dead-host rows: born parked (final-sweep list). */
  upsertParked(did: string, pdsHost: string, error: string): void;
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
 * One repo's in-flight load: addRow hands rows to the loader's shared
 * cross-repo buffer; finish resolves only once every batch that carried this
 * repo's rows is durably inserted. The ledger must flip to 'loaded' only
 * after finish() resolves.
 */
export interface RepoLoad {
  addRow(row: PostRow): Promise<void>;
  finish(): Promise<void>;
}

/**
 * Batched cross-repo ClickHouse inserts (see loader.ts for the durability
 * bookkeeping and why per-repo inserts were a parts storm). Loads are
 * idempotent: re-fetches re-insert rows that ReplacingMergeTree(did, rkey)
 * collapses, and within one process identical retried batches dedup via
 * insert_deduplication_token.
 */
export interface RepoLoader {
  openRepo(did: string, rev: string | null): RepoLoad;
  /** Release any loader-owned ClickHouse client (one rebuilt after a poisoned pool). */
  close(): Promise<void>;
}
