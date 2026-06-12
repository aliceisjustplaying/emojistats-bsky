import type { ClickHouseClient } from '@clickhouse/client';

import logger from './logger.js';
import { REPO_STATUSES, type RepoStatus } from './types.js';

/**
 * Crawl telemetry → ClickHouse (backfill_progress / backfill_repo_events),
 * the dashboard's data source across processes and boxes.
 *
 * DOCTRINE DIFFERENCE from the archive: repo events are NOT precious. A failed
 * event insert logs a warning and drops the batch — it never crashes or stalls
 * the crawl. Progress snapshots are the dashboard's current status surface, so
 * the newest one is retained and retried until ClickHouse accepts it. The
 * ledger remains the durable accounting.
 */

export interface ProgressSnapshot {
  statusCounts: Partial<Record<RepoStatus, number>>;
  postsLoaded: number;
  bytesDownloaded: number;
  rowsPerSec: number;
  inFlight: number;
}

export interface RepoEvent {
  did: string;
  pdsHost: string;
  event: string;
  posts?: number;
  records?: number;
  carBytes?: number;
  error?: string;
}

interface RepoEventRow {
  ts: string;
  did: string;
  pds_host: string;
  event: string;
  posts: number;
  records: number;
  car_bytes: number;
  error: string;
}

export interface CrawlTelemetryOptions {
  runId: string;
  shard: string;
  intervalMs: number;
}

/** 'YYYY-MM-DD HH:MM:SS' UTC, the JSONEachRow-friendly DateTime form. */
function chDateTime(ms: number): string {
  return new Date(ms).toISOString().slice(0, 19).replace('T', ' ');
}

export class CrawlTelemetry {
  readonly #client: ClickHouseClient;
  readonly #runId: string;
  readonly #shard: string;
  readonly #intervalMs: number;
  #events: RepoEventRow[] = [];
  #pendingProgress: Record<string, string | number> | undefined;
  #getSnapshot: (() => ProgressSnapshot) | undefined;
  #timer: NodeJS.Timeout | undefined;
  #progressFlushing = false;
  #eventFlushing = false;

  constructor(client: ClickHouseClient, options: CrawlTelemetryOptions) {
    this.#client = client;
    this.#runId = options.runId;
    this.#shard = options.shard;
    this.#intervalMs = options.intervalMs;
  }

  /** One backfill_progress row per tick; buffered repo events flush alongside it. */
  start(getSnapshot: () => ProgressSnapshot): void {
    if (this.#timer !== undefined)
      throw new Error('CrawlTelemetry already started');
    this.#getSnapshot = getSnapshot;
    this.#timer = setInterval(() => {
      void this.#tick();
    }, this.#intervalMs);
    void this.#tick();
    // Telemetry must never keep the process alive on its own.
    this.#timer.unref();
  }

  /**
   * Warn-only startup check: backfill_progress must carry a column per status
   * in REPO_STATUSES, or progress inserts will fail and retry forever. Never
   * throws because a schema problem should be visible without crashing a crawl.
   */
  async assertProgressColumns(): Promise<void> {
    try {
      const result = await this.#client.query({
        query: 'DESCRIBE TABLE backfill_progress',
        format: 'JSONEachRow',
      });
      const columns = new Set(
        (await result.json<{ name: string }>()).map((row) => row.name),
      );
      const missing = REPO_STATUSES.filter((status) => !columns.has(status));
      if (missing.length > 0) {
        logger.warn(
          { missing },
          'telemetry: backfill_progress is missing columns for REPO_STATUSES; progress ticks will be dropped',
        );
      }
    } catch (err) {
      logger.warn(
        { err: err instanceof Error ? err.message : String(err) },
        'telemetry: could not verify backfill_progress columns',
      );
    }
  }

  /** Buffers; the row rides out with the next tick (or the final flush). */
  recordEvent(event: RepoEvent): void {
    this.#events.push({
      ts: chDateTime(Date.now()),
      did: event.did,
      pds_host: event.pdsHost,
      event: event.event,
      posts: event.posts ?? 0,
      records: event.records ?? 0,
      car_bytes: event.carBytes ?? 0,
      error: event.error ?? '',
    });
  }

  /** Stops the timer and performs the final flush. */
  async stop(): Promise<void> {
    if (this.#timer !== undefined) {
      clearInterval(this.#timer);
      this.#timer = undefined;
    }
    this.#captureProgress();
    void this.#flushProgress();
    void this.#flushEvents();
    // Let in-flight flushes finish so the final flush doesn't interleave.
    while (this.#progressFlushing || this.#eventFlushing) {
      await new Promise((resolve) => {
        setTimeout(resolve, 50);
      });
    }
    await Promise.all([this.#flushProgress(), this.#flushEvents()]);
  }

  async #tick(): Promise<void> {
    this.#captureProgress();
    void this.#flushProgress();
    void this.#flushEvents();
  }

  #captureProgress(): void {
    const snapshot = this.#getSnapshot?.();
    if (snapshot !== undefined)
      this.#pendingProgress = this.#progressRow(snapshot);
  }

  async #flushProgress(): Promise<void> {
    if (this.#progressFlushing) return;
    this.#progressFlushing = true;
    try {
      while (this.#pendingProgress !== undefined) {
        const progress = this.#pendingProgress;
        try {
          await this.#client.insert({
            table: 'backfill_progress',
            values: [progress],
            format: 'JSONEachRow',
          });
          if (this.#pendingProgress === progress)
            this.#pendingProgress = undefined;
        } catch (err) {
          logger.warn(
            { err: err instanceof Error ? err.message : String(err) },
            'telemetry: backfill_progress insert failed; will retry latest snapshot',
          );
          return;
        }
      }
    } finally {
      this.#progressFlushing = false;
    }
  }

  async #flushEvents(): Promise<void> {
    if (this.#eventFlushing) return;
    if (this.#events.length === 0) return;
    this.#eventFlushing = true;
    const events = this.#events;
    this.#events = [];
    try {
      try {
        await this.#client.insert({
          table: 'backfill_repo_events',
          values: events,
          format: 'JSONEachRow',
        });
      } catch (err) {
        logger.warn(
          {
            dropped: events.length,
            err: err instanceof Error ? err.message : String(err),
          },
          'telemetry: backfill_repo_events insert failed; dropping batch',
        );
      }
    } finally {
      this.#eventFlushing = false;
    }
  }

  #progressRow(snapshot: ProgressSnapshot): Record<string, string | number> {
    const counts = snapshot.statusCounts;
    return {
      ts: chDateTime(Date.now()),
      run_id: this.#runId,
      shard: this.#shard,
      // One column per status in THE registry (types.ts); zero-filled so every
      // snapshot row is complete even before a status first occurs.
      ...Object.fromEntries(
        REPO_STATUSES.map((status) => [status, counts[status] ?? 0]),
      ),
      posts_loaded: snapshot.postsLoaded,
      bytes_downloaded: snapshot.bytesDownloaded,
      rows_per_sec: Math.round(snapshot.rowsPerSec * 100) / 100,
      in_flight: snapshot.inFlight,
    };
  }
}
