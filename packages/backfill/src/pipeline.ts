/** Per-repo pipeline: fetch (I/O) → parse in a worker (CPU) → archive + load (I/O). */

import type { StoragePolicy } from 'archive/policy';
import type { ArchiveSink } from 'archive/types';
import { applyTextPolicy } from 'ingest/rows';

import { fetchRepoCar, QuarantineError, RetryableError } from './fetcher.js';
import logger from './logger.js';
import type { ParsePool } from './parse-pool.js';
import type { RetryPolicy } from './retry.js';
import type { CrawlControl, CrawlStats } from './run-state.js';
import type { CrawlTelemetry } from './telemetry.js';
import type { Ledger, RepoCounts, RepoLoader, RepoRow } from './types.js';

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
}

/** Drains a (CAR_MAX_BYTES-guarded) body stream into one transferable buffer. */
async function bufferCar(
  body: ReadableStream<Uint8Array>,
): Promise<ArrayBuffer> {
  const chunks: Uint8Array[] = [];
  let total = 0;
  const reader = body.getReader();
  while (true) {
    const result = await reader.read();
    if (result.done) break;
    chunks.push(result.value);
    total += result.value.byteLength;
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out.buffer;
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
    try {
      // bsky.social is the entryway, never a real sync host — a ledger pointer at
      // it is always a stale pre-mushroom op; resolve before wasting a request.
      const staleKnown = repo.pdsHost === 'bsky.social';
      if (
        (repo.attempts > 0 || staleKnown) &&
        (await retry.refreshHost(repo)) === 'tombstoned'
      ) {
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
      const fetched = await fetchRepoCar(repo.pdsHost, repo.did);
      const fetchTimeUs = Date.now() * 1000;
      // Buffer the (CAR_MAX_BYTES-guarded) body and hand it to a parse worker
      // by transfer — all CPU happens off-thread, this thread only does I/O.
      // The CAR is resident either way (the MST reader needs random access);
      // the worker frees it when the job ends. A quarantine inside the worker
      // means NO rows have been written anywhere — materializing rows before
      // any append removed the old partial-coverage caveat.
      const car = await bufferCar(fetched.body);
      const parsed = await parsePool.parse(repo.did, car, fetchTimeUs);

      const postsTotal = parsed.rows.length;
      const load =
        postsTotal > 0 ? loader.openRepo(repo.did, parsed.rev) : null;
      for (const row of parsed.rows) {
        // Full rows (text always included) go to the parquet archive first — it
        // is the only durable home of non-emoji post text.
        if (archiveSink !== null) {
          try {
            await archiveSink.append(row);
          } catch (err) {
            throw archiveAppendFailed(repo.did, err);
          }
        }

        // Cost-revised storage (plan 0001): ClickHouse keeps text for emoji
        // posts only; rows without emojis ship with text stripped. 'all' is the
        // upgrade path (applyTextPolicy passes rows through untouched).
        try {
          await load!.addRow(applyTextPolicy(row, policy));
        } catch (err) {
          throw loaderChunkFailed(err);
        }
      }

      const repoCounts: RepoCounts = {
        rev: parsed.rev,
        carBytes: fetched.bytesRead(),
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
        if (load !== null) await load.finish();
        consecutiveLoaderFailures = 0;
      } catch (err) {
        throw loaderChunkFailed(err);
      }

      if (postsTotal === 0) {
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
        logger.info(
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
      retry.handleRepoError(repo, err);
    }
  };
}
