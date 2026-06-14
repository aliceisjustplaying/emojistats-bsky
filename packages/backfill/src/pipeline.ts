/** Per-repo pipeline: fetch+parse in a worker (its own loop and core) → archive + load (I/O here). */

import type { StoragePolicy } from 'archive/policy';
import type { ArchiveSink } from 'archive/types';
import { toClickhouseRow } from 'ingest/rows';

import { RetryableError } from './fetcher.js';
import type { HostHealth } from './host-health.js';
import type { HostPressure } from './host-pressure.js';
import logger from './logger.js';
import type { ParsePool } from './parse-pool.js';
import type { RetryPolicy } from './retry.js';
import type { CrawlControl, CrawlStats } from './run-state.js';
import type { CrawlTelemetry } from './telemetry.js';
import type {
  Ledger,
  RepoCounts,
  RepoLoad,
  RepoLoader,
  RepoRow,
} from './types.js';

export interface RepoPipelineDeps {
  ledger: Ledger;
  loader: RepoLoader;
  parsePool: ParsePool;
  archiveSink: ArchiveSink | null;
  policy: StoragePolicy;
  telemetry: CrawlTelemetry;
  retry: RetryPolicy;
  stats: CrawlStats;
  control: CrawlControl;
  hostPressure: HostPressure;
  hostHealth: HostHealth;
}

export function createRepoPipeline(
  deps: RepoPipelineDeps,
): (repo: RepoRow) => Promise<void> {
  const {
    ledger,
    loader,
    parsePool,
    archiveSink,
    policy,
    telemetry,
    retry,
    stats,
    control,
    hostPressure,
    hostHealth,
  } = deps;

  // ClickHouse being down fails every repo post-download; trip after a few in a
  // row so we stop burning bandwidth and drain (repos re-fetch on the next run).
  const LOADER_TRIP_THRESHOLD = 5;
  let consecutiveLoaderFailures = 0;
  // Archive doctrine: full text lives ONLY in the parquet archive, so a single
  // failed append trips the whole run (unlike telemetry, which is lossy).
  let archiveFailed = false;

  // A failed append means data loss, so it trips the run: stop claiming, drain,
  // exit non-zero. The repo parks as retryable, so the next run re-fetches and
  // re-appends it (at-least-once; the archive dedupes by (did, rkey) at mining
  // time).
  const archiveAppendFailed = (did: string, err: unknown): RetryableError => {
    if (!archiveFailed) {
      archiveFailed = true;
      control.stopClaiming = true;
      process.exitCode = 1;
      logger.fatal(
        {
          did,
          err: err instanceof Error ? err.message : String(err),
        },
        'archive append failed; archive rows are irreplaceable — stopping claims and draining',
      );
    }
    return new RetryableError(
      `archive: ${err instanceof Error ? err.message : String(err)}`,
      {
        transient: true,
        cause: err,
      },
    );
  };

  const loaderChunkFailed = (err: unknown): RetryableError => {
    consecutiveLoaderFailures += 1;
    if (
      consecutiveLoaderFailures >= LOADER_TRIP_THRESHOLD &&
      !control.stopClaiming
    ) {
      control.stopClaiming = true;
      logger.fatal(
        { consecutiveLoaderFailures },
        'ClickHouse loads failing repeatedly; stopping claims and draining',
      );
    }
    return new RetryableError(
      `loader: ${err instanceof Error ? err.message : String(err)}`,
      {
        transient: true,
        cause: err,
      },
    );
  };

  return async function processRepo(repo: RepoRow): Promise<void> {
    const startedAt = Date.now();
    const preserveExisting = repo.preserveExisting === true;
    try {
      // bsky.social is the entryway, never a real sync host — a ledger pointer at
      // it is always a stale pre-mushroom op; resolve before wasting a request.
      const staleKnown = repo.pdsHost === 'bsky.social';
      if (
        (repo.attempts > 0 || staleKnown) &&
        (await retry.refreshHost(repo)) === 'tombstoned'
      ) {
        if (preserveExisting) {
          logger.warn(
            { did: repo.did },
            'recrawl saw PLC tombstone; preserving existing loaded/verified ledger state',
          );
          telemetry.recordEvent({
            did: repo.did,
            pdsHost: repo.pdsHost,
            event: 'retry',
            error: 'PLC tombstone discovered during preserved recrawl',
          });
          return;
        }
        ledger.markTerminal(
          repo.did,
          'tombstoned',
          'PLC tombstone discovered at retry',
        );
        stats.terminal += 1;
        telemetry.recordEvent({
          did: repo.did,
          pdsHost: repo.pdsHost,
          event: 'tombstoned',
          error: 'PLC tombstone discovered at retry',
        });
        return;
      }
      // Fetch AND parse happen inside a repo worker; row batches come back with
      // backpressure so whale repos never materialize a second full row array
      // on this thread.
      const fetchTimeUs = Date.now() * 1000;
      const streamed = {
        postsTotal: 0,
        load: null as RepoLoad | null,
      };
      const parsed = await parsePool.run(
        repo.did,
        repo.pdsHost,
        fetchTimeUs,
        async ({ rev, rows }) => {
          if (rows.length === 0) return;
          let batchLoad = streamed.load;
          if (batchLoad === null) {
            batchLoad = loader.openRepo(repo.did, rev);
            streamed.load = batchLoad;
          }
          streamed.postsTotal += rows.length;
          for (const row of rows) {
            // Full rows (text always included) go to the parquet archive first
            // — it is the only durable home of non-emoji post text.
            if (archiveSink !== null) {
              try {
                await archiveSink.append(row);
              } catch (err) {
                throw archiveAppendFailed(repo.did, err);
              }
            }

            // Cost-revised storage (plan 0001): ClickHouse keeps text for emoji
            // posts only; rows without emojis ship with text stripped. The pick
            // in toClickhouseRow also drops the archive-only metadata columns.
            try {
              await batchLoad.addRow(toClickhouseRow(row, policy));
            } catch (err) {
              throw loaderChunkFailed(err);
            }
          }
        },
      );
      const postsTotal = streamed.postsTotal;
      if (postsTotal !== parsed.postsTotal) {
        throw new RetryableError(
          `worker streamed ${postsTotal} rows but reported ${parsed.postsTotal}`,
          { transient: true },
        );
      }
      // The CAR arrived: feed the AIMD cap raise and reset deadness evidence.
      hostPressure.observeRateLimit(repo.pdsHost, parsed.rateLimit);
      hostPressure.recordSuccess(repo.pdsHost);
      hostHealth.recordSuccess(repo.pdsHost);

      const repoCounts: RepoCounts = {
        rev: parsed.rev,
        carBytes: parsed.carBytes,
        recordsTotal: parsed.recordsTotal,
        postsTotal,
        postsWithEmojis: parsed.postsWithEmojis,
        emojiOccurrences: parsed.emojiOccurrences,
        rkeyDigest: parsed.rkeyDigestHex,
      };
      stats.bytes += repoCounts.carBytes;
      if (parsed.duplicatePostsSkipped > 0) {
        logger.warn(
          { did: repo.did, skipped: parsed.duplicatePostsSkipped },
          'byte-identical duplicate posts skipped',
        );
      }

      try {
        if (streamed.load !== null) await streamed.load.finish();
        consecutiveLoaderFailures = 0;
      } catch (err) {
        throw loaderChunkFailed(err);
      }

      if (postsTotal === 0) {
        if (preserveExisting) {
          telemetry.recordEvent({
            did: repo.did,
            pdsHost: repo.pdsHost,
            event: 'retry',
            records: repoCounts.recordsTotal,
            carBytes: repoCounts.carBytes,
            error:
              'recrawl returned zero posts; preserving existing loaded/verified ledger state',
          });
          logger.warn(
            { did: repo.did, ...repoCounts },
            'recrawl returned zero posts; preserving existing loaded/verified ledger state',
          );
          return;
        }
        // Zero posts is terminal 'empty', not 'loaded': the loaded→empty double
        // transition would leave a misleading loaded_at, and 'empty' rows carry
        // no counts by design — what the repo did contain is logged here.
        ledger.markTerminal(repo.did, 'empty');
        stats.empty += 1;
        telemetry.recordEvent({
          did: repo.did,
          pdsHost: repo.pdsHost,
          event: 'empty',
          records: repoCounts.recordsTotal,
          carBytes: repoCounts.carBytes,
        });
        logger.debug(
          { did: repo.did, ...repoCounts },
          'repo has zero posts; marked empty',
        );
      } else {
        ledger.markLoaded(repo.did, repoCounts);
        stats.loaded += 1;
        stats.postRows += postsTotal;
        telemetry.recordEvent({
          did: repo.did,
          pdsHost: repo.pdsHost,
          event: 'loaded',
          posts: repoCounts.postsTotal,
          records: repoCounts.recordsTotal,
          carBytes: repoCounts.carBytes,
        });
        logger.debug(
          {
            did: repo.did,
            posts: postsTotal,
            records: parsed.recordsTotal,
            ms: Date.now() - startedAt,
          },
          'repo loaded',
        );
      }
    } catch (err) {
      // Parse failures (quarantine included) surface before any row is
      // appended anywhere — the worker materializes the full repo first. Rows
      // can still partially land if archive/loader I/O fails mid-loop; those
      // park as retryable and the re-fetch collapses into ReplacingMergeTree
      // + the dedup tokens like any other at-least-once replay.
      if ((await retry.handleRepoError(repo, err)) === 'retry-now') {
        await processRepo(repo);
      }
    }
  };
}
