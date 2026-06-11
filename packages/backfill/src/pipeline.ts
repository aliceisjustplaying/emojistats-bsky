/** Per-repo pipeline: fetch → parse → stream each row through archive + load in one pass. */

import type { StoragePolicy } from 'archive/policy';
import type { ArchiveSink } from 'archive/types';
import { applyTextPolicy } from 'ingest/rows';
import type { PostRow } from 'ingest/types';

import { rkeyHash64 } from './digest.js';
import { repoPostRows } from './extract.js';
import { fetchRepoCar, QuarantineError, RetryableError } from './fetcher.js';
import logger from './logger.js';
import { parseRepoCar } from './parser.js';
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
  archiveSink: ArchiveSink | null;
  policy: StoragePolicy;
  telemetry: CrawlTelemetry;
  retry: RetryPolicy;
  stats: CrawlStats;
  control: CrawlControl;
}

export function createRepoPipeline(
  deps: RepoPipelineDeps,
): (repo: RepoRow) => Promise<void> {
  const {
    ledger,
    loader,
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
    // Hoisted out of the try so the quarantine path in the catch below can
    // report how many rows had already streamed when the walk gave up.
    let postsTotal = 0;
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
      const parsed = parseRepoCar(fetched.body);

      // Single pass in MST walk order: each row goes archive → ClickHouse chunk
      // before the next is normalized, so peak memory per repo stays the CAR
      // buffer plus one loader chunk — never a second full copy of the repo.
      // The interleave means partial chunks may land before an archive failure;
      // that is fine: the repo re-fetches entirely on the next run and
      // ReplacingMergeTree + the dedup tokens collapse whatever already landed.
      let postsWithEmojis = 0;
      let emojiOccurrences = 0;
      // Folded over the same post-normalization rkeys that get loaded, so the
      // ledger digest and ClickHouse's recomputation describe the same set.
      let rkeyDigest = 0n;

      // The dedup token (`${did}:${rev}:${chunkIdx}`) needs rev before the
      // first chunk insert, so ClickHouse rows buffer here until the commit
      // scanner has it. In practice that is zero rows: the MST reader cannot
      // yield a record before consuming the commit block (see ParsedRepo.rev).
      let load: RepoLoad | null = null;
      const pending: PostRow[] = [];
      const openAndFlushPending = async (): Promise<RepoLoad> => {
        const opened = loader.openRepo(repo.did, parsed.rev);
        for (const buffered of pending) await opened.addRow(buffered);
        pending.length = 0;
        return opened;
      };

      for await (const row of repoPostRows(repo.did, parsed, fetchTimeUs)) {
        postsTotal += 1;
        rkeyDigest ^= rkeyHash64(row.rkey);
        if (row.emojis.length > 0) {
          postsWithEmojis += 1;
          emojiOccurrences += row.emojis.length;
        }

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
        const chRow = applyTextPolicy(row, policy);
        try {
          if (load === null && parsed.rev !== null)
            load = await openAndFlushPending();
          if (load === null) pending.push(chRow);
          else await load.addRow(chRow);
        } catch (err) {
          throw loaderChunkFailed(err);
        }
      }

      // Drain complete: rev, recordsTotal and carBytes are final from here on.
      const repoCounts: RepoCounts = {
        rev: parsed.rev,
        carBytes: fetched.bytesRead(),
        recordsTotal: parsed.recordsTotal,
        postsTotal,
        postsWithEmojis,
        emojiOccurrences,
        rkeyDigest: rkeyDigest.toString(16).padStart(16, '0'),
      };
      stats.bytes += repoCounts.carBytes;
      if (parsed.duplicatePostsSkipped > 0) {
        logger.warn(
          { did: repo.did, skipped: parsed.duplicatePostsSkipped },
          'byte-identical duplicate posts skipped',
        );
      }

      try {
        // rev-less drains quarantine inside the parser today, so this flush is
        // belt-and-suspenders: if rows are still pending, load them under the
        // old `${did}:null:${chunkIdx}` token shape rather than drop them.
        if (load === null && pending.length > 0)
          load = await openAndFlushPending();
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
      // A mid-walk quarantine (malformed CBOR, caps) leaves whatever already
      // streamed — archive appends plus full ClickHouse chunks — in place while
      // the ledger goes 'quarantined'. Accepted consequence of the single-pass
      // stream: buffering rows until the walk completes would undo the memory
      // win. It is also benign: every row written was an individually valid
      // post, 'quarantined' already records incomplete coverage, and verify's
      // orphan check flags CH rows for non-loaded DIDs. The count is appended
      // to the message so the operator sees the partial write in the ledger.
      if (err instanceof QuarantineError && postsTotal > 0) {
        err.message += `; ${postsTotal} rows already in posts/archive (valid posts, incomplete coverage)`;
      }
      retry.handleRepoError(repo, err);
    }
  };
}
