import { createArchiveSink } from 'archive';
import { resolveStoragePolicy } from 'archive/policy';
import type { ArchiveSink } from 'archive/types';

import { JetstreamSource } from './adapters/jetstream.js';
import { createClickHouseClient, pingClickHouse } from './clickhouse/client.js';
import {
  ARCHIVE_DIR,
  ARCHIVE_ENABLED,
  ARCHIVE_MAX_FILE_AGE_MS,
  ARCHIVE_MAX_ROWS_PER_FILE,
  ARCHIVE_SYNC_COMMAND,
  CURSOR_FILE_PATH,
  CURSOR_OVERRIDE_PATH,
  CURSOR_REWIND_US,
  DEDUPE_CLEANUP_INTERVAL_MS,
  DEDUPE_DB_PATH,
  DEDUPE_RETENTION_HOURS,
  FLUSH_INTERVAL_MS,
  JETSTREAM_ENDPOINT,
  STATS_LOG_INTERVAL_MS,
  TEXT_IN_CLICKHOUSE,
  WRITER_MAX_BUFFER_ROWS,
} from './config.js';
import { CursorStore } from './cursor.js';
import { DedupeStore } from './dedupe.js';
import logger from './logger.js';
import { normalizePost } from './normalizer.js';
import { toArchiveRow } from './rows.js';
import type { Anomaly } from './types.js';
import { ClickHouseWriter } from './writer.js';

// Stack traces, not just .message: a late WebSocket error once crashed the
// worker with the unreadable line "Uncaught exception: " — never again.
process.on('unhandledRejection', (reason) => {
  logger.fatal(
    `Unhandled rejection: ${reason instanceof Error ? (reason.stack ?? reason.message) : String(reason)}`,
  );
  process.exit(1);
});

process.on('uncaughtException', (error) => {
  logger.fatal(`Uncaught exception: ${error.stack ?? error.message}`);
  process.exit(1);
});

/* Storage policy: resolved exactly once, before anything connects. Refusing to
   start here enforces the archive-required invariant — TEXT_IN_CLICKHOUSE='emoji'
   without the archive would silently discard non-emoji text forever. */
const storagePolicy = (() => {
  try {
    return resolveStoragePolicy({
      textInClickhouse: TEXT_IN_CLICKHOUSE,
      archiveEnabled: ARCHIVE_ENABLED,
    });
  } catch (error) {
    logger.fatal(`Storage policy rejected: ${(error as Error).message}`);
    return process.exit(1);
  }
})();
/* End storage policy */

/* ClickHouse initialization */
const client = createClickHouseClient();
try {
  await pingClickHouse(client);
} catch (error) {
  logger.fatal(`ClickHouse ping failed: ${(error as Error).message}`);
  process.exit(1);
}
/* End ClickHouse initialization */

const dedupe = new DedupeStore(DEDUPE_DB_PATH);
const cursorStore = new CursorStore(CURSOR_FILE_PATH, CURSOR_OVERRIDE_PATH);

/* Cursor resolution: always rewind so the reconnect window is replayed; dedupe absorbs the overlap. */
const loaded = cursorStore.load();
if (loaded?.fromOverride) {
  logger.warn(
    `CURSOR OVERRIDE IN EFFECT: starting from ${loaded.cursorUs} (${CURSOR_OVERRIDE_PATH})`,
  );
}
const initialCursor =
  loaded?.cursorUs !== undefined
    ? loaded.cursorUs - CURSOR_REWIND_US
    : Date.now() * 1000 - CURSOR_REWIND_US;
logger.info(
  `Initial cursor ${initialCursor} (${new Date(initialCursor / 1000).toISOString()})`,
);
/* End cursor resolution */

const source = new JetstreamSource(JETSTREAM_ENDPOINT, initialCursor);

/* Commit barrier (plan 0001): every in-flight archive append parks its
   settlement promise here and removes itself once settled. The writer samples
   the set at buffer swap and waits for it after a successful insert, so dedupe
   marks and cursor saves never outrun the archive. Over-inclusive on purpose —
   it may also wait on appends for rows newer than the batch, which is harmless
   and keeps the barrier intact across insert retries (a snapshot-and-clear
   design would lose it for retried batches). */
const pendingAppends = new Set<Promise<void>>();

const writer = new ClickHouseWriter(client, {
  flushIntervalMs: FLUSH_INTERVAL_MS,
  maxBufferRows: WRITER_MAX_BUFFER_ROWS,
  policy: storagePolicy,
  getCursor: () => source.cursor,
  commitBarrier: () => Promise.all(pendingAppends).then(() => undefined),
  onFlushSuccess: (cursorAtSwap, rows) => {
    // Mark-first: a crash between the two suppresses replay only for rows
    // that are already durable — safe in either order, but strictly tighter.
    dedupe.markSeenBatch(rows, Date.now());
    if (cursorAtSwap !== undefined) cursorStore.save(cursorAtSwap);
  },
});
writer.start();

/* Archive spool (plan 0001): the Parquet archive is the ONLY durable home of
   non-emoji post text, so the sink must exist before the first event flows. */
let archive: ArchiveSink | undefined;
if (storagePolicy.archiveEnabled) {
  archive = await createArchiveSink({
    dir: ARCHIVE_DIR,
    prefix: 'live',
    maxRowsPerFile: ARCHIVE_MAX_ROWS_PER_FILE,
    maxFileAgeMs: ARCHIVE_MAX_FILE_AGE_MS,
    syncCommand: ARCHIVE_SYNC_COMMAND,
  });
}
/* End archive spool */

const stats = {
  eventsSeen: 0,
  duplicates: 0,
  anomalies: {} as Partial<Record<Anomaly, number>>,
};

await source.start((event) => {
  stats.eventsSeen++;
  // Lookup only — marking happens in onFlushSuccess, once the row is durable.
  // Marking at receive time would let a crash-replay find the mark and drop
  // rows that never reached ClickHouse or the archive. The trade: the same
  // (did, rkey) arriving twice within one flush window is enqueued twice;
  // ReplacingMergeTree collapses it.
  if (dedupe.isSeen(event.did, event.rkey)) {
    stats.duplicates++;
    return;
  }
  const post = normalizePost(event);
  for (const anomaly of post.anomalies) {
    stats.anomalies[anomaly] = (stats.anomalies[anomaly] ?? 0) + 1;
  }
  if (archive) {
    // Full text always — TEXT_IN_CLICKHOUSE only governs the ClickHouse row.
    // Append failure is fatal: the archive is the only durable home of
    // non-emoji text, so crashing (cursor un-advanced, restart replays the
    // window) beats acking rows whose text was never spooled.
    //
    // Settlement feeds the writer's commit barrier: dedupe marks and cursor
    // saves wait for every append in flight at buffer swap, so a crash at
    // any point replays un-acked rows on restart (at-least-once; RMT and
    // the archive's dedupe-on-read absorb the dupes).
    //
    // Honest caveat: append resolution means the row reached the duckdb
    // staging file, not fsync — a hard power cut can still lose the staging
    // tail after the cursor advanced. Bounded and accepted, and strictly
    // smaller than the old window, which acked at receive time.
    const settled: Promise<void> = archive
      .append(toArchiveRow(post, 'live'))
      .catch((error: unknown) => {
        logger.fatal(
          `Archive append failed, refusing to outlive the only home of full text: ${(error as Error).message}`,
        );
        process.exit(1);
      });
    pendingAppends.add(settled);
    void settled.finally(() => pendingAppends.delete(settled));
  }
  writer.enqueue(post, 'live');
});

/* logging stats to the console */
const statsInterval = setInterval(() => {
  const cursor = source.cursor;
  logger.info(
    {
      eventsSeen: stats.eventsSeen,
      duplicates: stats.duplicates,
      anomalies: stats.anomalies,
      writer: writer.stats,
      archive: archive?.stats,
      cursorLagSeconds:
        cursor === undefined
          ? null
          : Math.round((Date.now() * 1000 - cursor) / 1e4) / 100,
    },
    'ingest stats',
  );
}, STATS_LOG_INTERVAL_MS);
/* End logging stats */

/* dedupe retention */
const cleanupInterval = setInterval(() => {
  try {
    const deleted = dedupe.cleanup(DEDUPE_RETENTION_HOURS);
    logger.info(
      `Dedupe cleanup deleted ${deleted} rows older than ${DEDUPE_RETENTION_HOURS}h.`,
    );
  } catch (error) {
    logger.error(`Dedupe cleanup failed: ${(error as Error).message}`);
  }
}, DEDUPE_CLEANUP_INTERVAL_MS);
/* End dedupe retention */

let isShuttingDown = false;

async function shutdown(signal: NodeJS.Signals) {
  logger.info(`Received ${signal}, shutting down gracefully...`);

  try {
    await source.stop();
  } catch (error) {
    logger.error(
      `Error stopping Jetstream source: ${(error as Error).message}`,
    );
  }

  let writerDrained = false;
  try {
    await writer.stop();
    writerDrained = true;
    logger.info('Writer flushed and stopped.');
  } catch (error) {
    logger.error(`Error stopping writer: ${(error as Error).message}`);
  }

  /* Archive close ordering: after writer.stop() so the sink outlives every
     producer (source.stop() above already ended appends; keeping the sink
     open through the writer's final flushes keeps the rule simple), and
     BEFORE the cursor save: a failed finalize exits non-zero with the
     shutdown cursor un-advanced, so the restart replays the window — the
     same at-least-once recovery as any crash now that dedupe marks and
     cursor saves only follow CH insert success plus archive append
     settlement — instead of sealing a silent gap in the only home of
     non-emoji text. */
  if (archive) {
    try {
      await archive.close();
      logger.info('Archive sink closed.');
    } catch (error) {
      logger.fatal(`Archive close failed: ${(error as Error).message}`);
      process.exit(1);
    }
  }

  /* Final cursor save only when the writer drained: if rows are still
     buffered after stop()'s retries, the last flush-success save already
     points before the lost rows — saving the live cursor here would seal
     them into a gap. The always-applied rewind plus replay covers the rest. */
  if (writerDrained) {
    try {
      const cursor = source.cursor;
      if (cursor !== undefined) {
        cursorStore.save(cursor);
        logger.info(`Saved cursor ${cursor}.`);
      }
    } catch (error) {
      logger.error(`Error saving cursor: ${(error as Error).message}`);
    }
  }

  try {
    dedupe.close();
  } catch (error) {
    logger.error(`Error closing dedupe store: ${(error as Error).message}`);
  }

  try {
    await client.close();
  } catch (error) {
    logger.error(
      `Error closing ClickHouse client: ${(error as Error).message}`,
    );
  }

  clearInterval(statsInterval);
  clearInterval(cleanupInterval);

  process.exit(0);
}

function onSignal(signal: NodeJS.Signals) {
  if (isShuttingDown) {
    logger.warn(`Received ${signal} during shutdown — forcing exit.`);
    process.exit(1);
  }
  isShuttingDown = true;
  shutdown(signal).catch((error: unknown) => {
    logger.error(`Shutdown failed: ${(error as Error).message}`);
    process.exit(1);
  });
}

process.on('SIGINT', onSignal);
process.on('SIGTERM', onSignal);
