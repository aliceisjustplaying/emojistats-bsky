/**
 * The Parquet archive is the ONLY durable home of full post text under the
 * cost-revised storage plan (plan 0001): ClickHouse keeps text for emoji posts
 * only. Losing archive rows therefore loses data forever — sinks must fail
 * loud, never drop silently.
 */

export interface ArchiveRow {
  did: string;
  rkey: string;
  /** 'YYYY-MM-DD HH:MM:SS' UTC — identical to PostRow.created_at. */
  created_at: string;
  langs: string[];
  emojis: string[];
  text: string;
  src: 'live' | 'backfill';
}

export interface ArchiveStats {
  appended: number;
  finalizedFiles: number;
  openRows: number;
  /** Finalized-but-unsynced files drained by the startup sweep in create(). */
  sweptFiles: number;
  /** Stale `.parquet.tmp` partials from a crash mid-COPY deleted at startup. */
  removedTmpFiles: number;
}

export interface ArchiveSinkOptions {
  /** Spool directory; finalized files land in `${dir}/finalized/`. */
  dir: string;
  /** File name prefix, e.g. 'live' or 'backfill-shard0'. */
  prefix: string;
  /** Rotate after this many rows (default 1_000_000). */
  maxRowsPerFile?: number;
  /** Rotate after this many ms regardless of rows (default 3_600_000). */
  maxFileAgeMs?: number;
  /**
   * Optional shell command run after each finalize with {file} substituted by
   * the finalized path — the deploy-time hook for rclone/scp to the Storage Box.
   * Runs in the background (uploads take minutes; appends must not wait), one
   * at a time in finalize order. Non-zero exit must surface as an error (the
   * archive is the only text home) — it is thrown from the next sink call, and
   * close() drains all pending syncs before resolving.
   *
   * Startup re-runs the command for every finalized file still on disk (the
   * sweep that drains files a sync-failed run left behind), so the command
   * must either remove the local file once it is safely remote (`rclone
   * move`) or tolerate re-running on an already-synced file (idempotent
   * copy) — at-least-once, like everything else in the archive.
   */
  syncCommand?: string;
}

/**
 * Rotating zstd-Parquet writer. Every finalized file is appended to
 * `${dir}/finalized/manifest.jsonl` as
 * { file, rows, bytes, minCreatedAt, maxCreatedAt, finalizedAt } —
 * the manifest is the completeness accounting for later mining/restore.
 */
export interface ArchiveSink {
  append(row: ArchiveRow): Promise<void>;
  /** Finalize the currently open file (no-op when empty). */
  rotate(): Promise<void>;
  /** rotate() + flush manifest; the sink is unusable afterwards. */
  close(): Promise<void>;
  readonly stats: ArchiveStats;
}
