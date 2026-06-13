/** Retry / host-refresh policy + post-crash reconciliation. */

import type { ClickHouseClient } from '@clickhouse/client';

import {
  MAX_ATTEMPTS,
  PLC_DIRECTORY_URL,
  RETRY_BASE_MS,
  RETRY_MAX_MS,
  USER_AGENT,
} from './config.js';
import { CH_RKEY_DIGEST_EXPR, normalizeDigestHex } from './digest.js';
import {
  isStallMessage,
  pdsHostFromEndpoint,
  QuarantineError,
  RetryableError,
  TerminalFetchError,
} from './fetcher.js';
import type { HostHealth } from './host-health.js';
import type { HostPressure } from './host-pressure.js';
import logger from './logger.js';
import type { CrawlStats } from './run-state.js';
import type { CrawlTelemetry } from './telemetry.js';
import type { Ledger, RepoRow } from './types.js';

const CRASH_RECONCILE_WINDOW_MS = 3_600_000;

/**
 * After an unclean shutdown, repos marked loaded shortly before the crash may
 * have lost their ClickHouse rows (insert acked into the OS page cache, never
 * flushed before the power went). Compare the recent window against CH and
 * requeue any mismatch — loads are idempotent, so requeuing is always safe.
 */
export async function reconcileRecentLoads(
  ledger: Ledger,
  ch: ClickHouseClient,
): Promise<void> {
  let maxLoadedAt = 0;
  for (const row of ledger.iterateByStatus('loaded')) {
    if (row.loadedAt !== null && row.loadedAt > maxLoadedAt)
      maxLoadedAt = row.loadedAt;
  }
  if (maxLoadedAt === 0) return;

  const cutoff = maxLoadedAt - CRASH_RECONCILE_WINDOW_MS;
  const recent: RepoRow[] = [];
  for (const row of ledger.iterateByStatus('loaded')) {
    if (row.loadedAt !== null && row.loadedAt >= cutoff) recent.push(row);
  }
  logger.warn(
    { recent: recent.length },
    'unclean shutdown detected: reconciling recently loaded repos against ClickHouse',
  );

  // Src-agnostic on purpose (same contract as verify.ts): a post created
  // during the crawl arrives via BOTH the live path and the repo CAR; whichever
  // inserts later wins the ReplacingMergeTree merge and keeps its src label, so
  // filtering on src='backfill' undercounts active repos.
  //
  // STRICT, unlike verify: 'loaded' survives only when the counts are exactly
  // equal AND the rkey digests match (digest.ts holds both sides). A bare >=
  // count is blind to the offset case — one lost CAR row balanced by one
  // live-only arrival — which is precisely what a crash can produce. Requeue is
  // cheap and idempotent, and this window is minutes of recent loads after a
  // crash, so strictness is free here; verify is where live-only arrivals must
  // pass loosely, over the whole ledger. Null ledger digest (should not happen
  // on fresh ledgers) falls back to requeue only when CH < ledger.
  let requeued = 0;
  for (let i = 0; i < recent.length; i += 1000) {
    const chunk = recent.slice(i, i + 1000);
    const result = await ch.query({
      query: `SELECT did, toUInt64(count()) AS posts, hex(${CH_RKEY_DIGEST_EXPR}) AS digest FROM posts FINAL WHERE did IN ({dids:Array(String)}) GROUP BY did`,
      query_params: { dids: chunk.map((row) => row.did) },
      format: 'JSONEachRow',
    });
    const stats = new Map(
      (await result.json<{ did: string; posts: string; digest: string }>()).map(
        (r) => [
          r.did,
          { posts: Number(r.posts), digest: normalizeDigestHex(r.digest) },
        ],
      ),
    );
    for (const row of chunk) {
      const actual = stats.get(row.did);
      const expected = row.postsTotal ?? 0;
      const intact =
        row.rkeyDigest === null
          ? (actual?.posts ?? 0) >= expected
          : actual !== undefined &&
            actual.posts === expected &&
            actual.digest === normalizeDigestHex(row.rkeyDigest);
      if (!intact) {
        ledger.markRetry(
          row.did,
          'post-crash reconcile: ClickHouse count/digest mismatch',
          0,
        );
        requeued += 1;
      }
    }
  }
  logger.warn(
    { checked: recent.length, requeued },
    'post-crash reconciliation done',
  );
}

export interface RetryPolicy {
  /** Classifies a pipeline failure: terminal, quarantined, failed or retry wave. */
  handleRepoError(repo: RepoRow, err: unknown): void;
  /** Re-resolves the DID's current PDS before a retry; 'tombstoned' is terminal. */
  refreshHost(repo: RepoRow): Promise<'ok' | 'tombstoned'>;
}

export interface RetryPolicyDeps {
  ledger: Ledger;
  telemetry: CrawlTelemetry;
  stats: CrawlStats;
  hostPressure: HostPressure;
  hostHealth: HostHealth;
}

export function createRetryPolicy(deps: RetryPolicyDeps): RetryPolicy {
  const { ledger, telemetry, stats, hostPressure, hostHealth } = deps;

  function handleRepoError(repo: RepoRow, err: unknown): void {
    const message = err instanceof Error ? err.message : String(err);

    if (err instanceof TerminalFetchError) {
      // The host answered (404/tombstone/takedown are host SUCCESSES).
      hostHealth.recordSuccess(repo.pdsHost);
      ledger.markTerminal(repo.did, err.status, message);
      stats.terminal += 1;
      telemetry.recordEvent({
        did: repo.did,
        pdsHost: repo.pdsHost,
        event: err.status,
        error: message,
      });
      logger.debug(
        { did: repo.did, status: err.status, err: message },
        'repo terminal',
      );
      return;
    }
    if (err instanceof QuarantineError) {
      // Hitting CAR_MAX_BYTES means the host delivered a >1GiB body — loud
      // proof of life that must clear any stall/deadness streak.
      hostHealth.recordSuccess(repo.pdsHost);
      ledger.markTerminal(repo.did, 'quarantined', message);
      stats.terminal += 1;
      telemetry.recordEvent({
        did: repo.did,
        pdsHost: repo.pdsHost,
        event: 'quarantined',
        error: message,
      });
      logger.warn({ did: repo.did, err: message }, 'repo quarantined');
      return;
    }

    // Everything else retries. Clearly-transient failures (429/5xx/network/timeout,
    // loader outages) wave while within the attempts budget; past it the repo
    // stays parked as 'unreachable' for a later run / final sweep — never
    // flipped to 'failed', because host down ≠ data gone, and listClaimable
    // simply stops offering it. Anything else — unknown HTTP errors — gets
    // MAX_ATTEMPTS total tries, then 'failed'. repo.attempts predates this try
    // (markRetry is what increments it in the ledger).
    const transient = err instanceof RetryableError && err.transient;
    const attempts = repo.attempts + 1;
    if (!transient && attempts >= MAX_ATTEMPTS) {
      ledger.markTerminal(repo.did, 'failed', message);
      stats.terminal += 1;
      telemetry.recordEvent({
        did: repo.did,
        pdsHost: repo.pdsHost,
        event: 'failed',
        error: message,
      });
      logger.warn(
        { did: repo.did, attempts, err: message },
        'repo failed: max attempts on a non-transient error',
      );
      return;
    }

    const backoff = Math.min(RETRY_BASE_MS * 2 ** repo.attempts, RETRY_MAX_MS);
    const retryAfterHint =
      err instanceof RetryableError ? (err.retryAfterMs ?? 0) : 0;
    const retryAfterMs = Math.max(backoff, retryAfterHint);
    // 429 is evidence of OUR pressure, not of the repo being unreachable:
    // it arms the host cooldown and must not burn the attempts budget —
    // during the morel storm, attempts-burning 429s mass-parked repos that
    // a politer pace fetched fine. The repo still backs off (flat base; the
    // host cooldown is what actually meters the pressure).
    if (/http 429/.test(message)) {
      // Rate limiting is proof of life — never deadness evidence.
      hostHealth.recordSuccess(repo.pdsHost);
      hostPressure.record429(repo.pdsHost);
      ledger.markThrottled(
        repo.did,
        message,
        Math.max(RETRY_BASE_MS, retryAfterHint),
      );
      stats.retried += 1;
      telemetry.recordEvent({
        did: repo.did,
        pdsHost: repo.pdsHost,
        event: 'retry',
        error: message,
      });
      return;
    }
    if (isStallMessage(message)) {
      // A half-open/silent host (see fetcher progress timeout). Soft: cool it
      // so the box stops burning slots on it. Hard: count it toward the
      // stall-deadness park so a host that ONLY stalls (a rate-limited bridge
      // owning a drained shard's tail) gets parked and the box moves on.
      // Like 429 this is host pressure, not the repo's fault, so markThrottled
      // backs the repo off WITHOUT burning its attempts budget.
      hostPressure.recordStall(repo.pdsHost);
      hostHealth.recordFailure(repo.pdsHost, message);
      ledger.markThrottled(
        repo.did,
        message,
        Math.max(RETRY_BASE_MS, retryAfterHint),
      );
      stats.retried += 1;
      telemetry.recordEvent({
        did: repo.did,
        pdsHost: repo.pdsHost,
        event: 'retry',
        error: message,
      });
      return;
    }
    hostHealth.recordFailure(repo.pdsHost, message);
    ledger.markRetry(repo.did, message, retryAfterMs);
    stats.retried += 1;
    telemetry.recordEvent({
      did: repo.did,
      pdsHost: repo.pdsHost,
      event: 'retry',
      error: message,
    });
    logger.debug(
      { did: repo.did, attempts, retryAfterMs, err: message },
      'repo retry scheduled',
    );
  }

  // Stale-host self-healing: the ledger's PDS pointer reflects whichever PLC ops
  // enumeration had seen by its cursor — accounts that migrated later (or migrate
  // mid-crawl) would fail against the old host forever. Every retry re-resolves
  // the DID's current document and follows it; tombstones discovered here are
  // terminal. Resolution is best-effort: on PLC hiccups the old host is kept and
  // the fetch below classifies the real failure.
  async function refreshHost(repo: RepoRow): Promise<'ok' | 'tombstoned'> {
    try {
      const res = await fetch(
        `${PLC_DIRECTORY_URL}/${encodeURIComponent(repo.did)}`,
        {
          headers: { 'user-agent': USER_AGENT },
          signal: AbortSignal.timeout(10_000),
        },
      );
      if (res.status === 410) return 'tombstoned';
      if (!res.ok) return 'ok';
      const doc = (await res.json()) as {
        service?: Array<{ type?: string; serviceEndpoint?: string }>;
      };
      const endpoint = doc.service?.find(
        (s) => s.type === 'AtprotoPersonalDataServer',
      )?.serviceEndpoint;
      if (typeof endpoint !== 'string') return 'ok';
      // Same normalization as enumeration (pdsHostFromEndpoint) so the two
      // writers of pds_host can never drift — including the scheme-prefixed
      // form for the rare http PDS, which also self-heals rows enumerated
      // before that form existed.
      const host = pdsHostFromEndpoint(endpoint);
      if (host !== undefined && host !== repo.pdsHost) {
        logger.info(
          { did: repo.did, from: repo.pdsHost, to: host },
          'stale PDS pointer: following current DID doc',
        );
        ledger.updateHost(repo.did, host);
        repo.pdsHost = host;
      }
    } catch {
      // best-effort; the fetch below classifies the real failure
    }
    return 'ok';
  }

  return { handleRepoError, refreshHost };
}
