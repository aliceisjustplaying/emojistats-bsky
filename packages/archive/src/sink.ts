/**
 * Rotating zstd-Parquet archive sink backed by a file-backed DuckDB staging
 * database.
 *
 * Durability model — the archive is AT-LEAST-ONCE; readers dedupe on
 * (did, rkey):
 * - append() stages rows into `${dir}/staging.duckdb` through a DuckDB
 *   appender flushed every APPENDER_FLUSH_EVERY rows, so a hard crash loses
 *   at most the unflushed appender buffer, never the whole open file's rows.
 * - rotate() COPYs staged rows to a temp parquet (zstd), renames it into
 *   `${dir}/finalized/`, appends the manifest line with fsync, and only then
 *   truncates staging. A crash anywhere before the truncate leaves the rows
 *   staged; the next startup rotates them out again, which may produce a
 *   duplicate parquet file — accepted, per the at-least-once contract.
 * - create() sweeps `finalized/` before accepting rows: stale `.parquet.tmp`
 *   partials from a crash mid-COPY are deleted (their rows are still staged —
 *   see sweepFinalized), and when syncCommand is set every finalized file
 *   still on disk is queued for re-sync oldest-first. The queue runs on the
 *   same background sync chain as live finalizes, so crawl startup does not
 *   wait behind a large storage backlog before telemetry can begin.
 * - syncCommand runs on its own serialized chain (one upload at a time, in
 *   finalize order), never on the append chain: an upload to a slow Storage
 *   Box takes minutes and would otherwise stall every append() behind
 *   enqueue(). A sync failure is recorded and thrown from the NEXT
 *   append()/rotate()/close() instead of the finalize that enqueued it;
 *   close() drains the chain before resolving, so a clean shutdown is
 *   synced-or-loud. Deferring is crash-safe: a crash mid-upload leaves the
 *   file in finalized/ and the create() sweep retries it.
 * - Every failure (staging write, COPY, manifest append, syncCommand) is
 *   thrown to the caller — for a background sync, the next caller. Nothing
 *   is dropped silently (doctrine in types.ts).
 */

import { execFile } from 'node:child_process';
import {
  mkdir,
  open,
  readdir,
  readFile,
  rename,
  rm,
  stat,
} from 'node:fs/promises';
import { join } from 'node:path';
import { promisify } from 'node:util';

import {
  DuckDBAppender,
  DuckDBConnection,
  DuckDBInstance,
  LIST,
  VARCHAR,
  listValue,
} from '@duckdb/node-api';

import type {
  ArchiveRow,
  ArchiveSink,
  ArchiveSinkOptions,
  ArchiveStats,
} from './types.js';

const execFileAsync = promisify(execFile);

const DEFAULT_MAX_ROWS_PER_FILE = 1_000_000;
const DEFAULT_MAX_FILE_AGE_MS = 3_600_000;
/** Bounds the rows a hard crash can lose to one appender buffer. */
const APPENDER_FLUSH_EVERY = 1024;
const SEQ_PAD = 6;
const LIST_VARCHAR = LIST(VARCHAR);

export const STAGING_DB_FILE = 'staging.duckdb';
export const STAGING_TABLE_DDL = `CREATE TABLE IF NOT EXISTS staging (
  did VARCHAR NOT NULL,
  rkey VARCHAR NOT NULL,
  created_at VARCHAR NOT NULL,
  langs VARCHAR[] NOT NULL,
  emojis VARCHAR[] NOT NULL,
  text VARCHAR NOT NULL,
  src VARCHAR NOT NULL,
  facets_json VARCHAR,
  reply_json VARCHAR,
  embed_json VARCHAR,
  labels_json VARCHAR
)`;
/**
 * Widens a staging table created before the metadata columns existed (the
 * appender binds by column position, so the order here must stay: original
 * seven, then these, matching the CREATE above).
 *
 * Re-crawl accounting contract: NULL = row archived before widening (extras
 * were never looked at — ALTER backfills NULL, and old parquet files read as
 * NULL under union_by_name); '' = extras captured and the record had no such
 * field. New appends always write non-NULL. The repo-level marker for
 * re-crawls is the ledger meta key 'archive_extras_since' (see crawl.ts).
 */
export const STAGING_TABLE_MIGRATIONS = [
  `ALTER TABLE staging ADD COLUMN IF NOT EXISTS facets_json VARCHAR`,
  `ALTER TABLE staging ADD COLUMN IF NOT EXISTS reply_json VARCHAR`,
  `ALTER TABLE staging ADD COLUMN IF NOT EXISTS embed_json VARCHAR`,
  `ALTER TABLE staging ADD COLUMN IF NOT EXISTS labels_json VARCHAR`,
];

interface ManifestEntry {
  file: string;
  rows: number;
  bytes: number;
  minCreatedAt: string;
  maxCreatedAt: string;
  finalizedAt: string;
  /** Archive schema version; absent = v1 (text-only, no metadata columns). */
  v: number;
}

/** v2 = facets/reply/embed/labels columns present (2026-06-13). */
const ARCHIVE_SCHEMA_VERSION = 2;

function sqlQuote(path: string): string {
  return path.replaceAll("'", "''");
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

class DuckDBArchiveSink implements ArchiveSink {
  private appender: DuckDBAppender | undefined;
  private unflushed = 0;
  private rowsInStaging = 0;
  private openedAt: number | undefined;
  private nextSeq = 1;
  private appended = 0;
  private finalizedFiles = 0;
  private sweptFiles = 0;
  private removedTmpFiles = 0;
  private closed = false;
  private chain: Promise<void> = Promise.resolve();
  private syncChain: Promise<void> = Promise.resolve();
  private syncFailure: Error | undefined;

  private readonly maxRowsPerFile: number;
  private readonly maxFileAgeMs: number;
  private readonly finalizedDir: string;
  private readonly manifestPath: string;
  /**
   * `${prefix}-NNNNNN.parquet` — prefix-scoped so multi-shard sinks sharing a
   * dir never seed from or sweep each other's files.
   */
  private readonly fileNamePattern: RegExp;

  private constructor(
    private readonly opts: ArchiveSinkOptions,
    private readonly instance: DuckDBInstance,
    private readonly connection: DuckDBConnection,
  ) {
    this.maxRowsPerFile = opts.maxRowsPerFile ?? DEFAULT_MAX_ROWS_PER_FILE;
    this.maxFileAgeMs = opts.maxFileAgeMs ?? DEFAULT_MAX_FILE_AGE_MS;
    this.finalizedDir = join(opts.dir, 'finalized');
    this.manifestPath = join(this.finalizedDir, 'manifest.jsonl');
    this.fileNamePattern = new RegExp(
      `^${escapeRegExp(opts.prefix)}-(\\d+)\\.parquet$`,
    );
  }

  static async create(opts: ArchiveSinkOptions): Promise<DuckDBArchiveSink> {
    const finalizedDir = join(opts.dir, 'finalized');
    await mkdir(finalizedDir, { recursive: true });

    const instance = await DuckDBInstance.create(
      join(opts.dir, STAGING_DB_FILE),
    );
    let connection: DuckDBConnection;
    try {
      connection = await instance.connect();
    } catch (err) {
      instance.closeSync();
      throw err;
    }

    const sink = new DuckDBArchiveSink(opts, instance, connection);
    try {
      await connection.run(STAGING_TABLE_DDL);
      for (const migration of STAGING_TABLE_MIGRATIONS) {
        await connection.run(migration);
      }
      await sink.seedSequence();
      // Drain what a previous run left behind (stale partials, unsynced
      // files) before the recovery rotate adds this run's first file, so
      // the backlog goes out oldest-first. Only the local cleanup blocks
      // startup; uploads ride the background sync chain.
      await sink.sweepFinalized();
      // Startup recovery: rows left by a crashed run go out as their own
      // file before any new rows mix in.
      await sink.finalizeStaged();
      sink.appender = await connection.createAppender('staging');
    } catch (err) {
      connection.closeSync();
      instance.closeSync();
      throw err;
    }
    return sink;
  }

  get stats(): ArchiveStats {
    return {
      appended: this.appended,
      finalizedFiles: this.finalizedFiles,
      openRows: this.rowsInStaging,
      sweptFiles: this.sweptFiles,
      removedTmpFiles: this.removedTmpFiles,
    };
  }

  append(row: ArchiveRow): Promise<void> {
    return this.enqueue(async () => {
      this.assertOpen();
      if (
        this.openedAt !== undefined &&
        this.rowsInStaging > 0 &&
        Date.now() - this.openedAt >= this.maxFileAgeMs
      ) {
        await this.finalizeStaged();
      }
      this.appendToStaging(row);
      this.appended += 1;
      if (this.rowsInStaging >= this.maxRowsPerFile) {
        await this.finalizeStaged();
      }
    });
  }

  rotate(): Promise<void> {
    return this.enqueue(async () => {
      this.assertOpen();
      await this.finalizeStaged();
    });
  }

  close(): Promise<void> {
    return this.enqueue(async () => {
      // Not assertOpen: a recorded sync failure must not skip the finalize
      // (staged rows still deserve their parquet) — it is rethrown after the
      // drain below.
      if (this.closed) {
        throw new Error('archive sink is closed');
      }
      try {
        await this.finalizeStaged();
        // Drain in-flight uploads: a clean shutdown means synced-or-loud,
        // never synced-maybe-later.
        await this.syncChain;
        if (this.syncFailure) {
          throw this.syncFailure;
        }
      } finally {
        this.closed = true;
        this.appender?.closeSync();
        this.connection.closeSync();
        this.instance.closeSync();
      }
    });
  }

  /** Serializes append/rotate/close so staging, COPY and truncate never interleave. */
  private enqueue<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.chain.then(fn);
    this.chain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private assertOpen(): void {
    if (this.closed) {
      throw new Error('archive sink is closed');
    }
    // A background sync failure is fatal to the run: it surfaces here, on
    // the first sink call after it was recorded, and stays sticky so the
    // crawler's drain path cannot miss it.
    if (this.syncFailure) {
      throw this.syncFailure;
    }
  }

  private appendToStaging(row: ArchiveRow): void {
    const appender = this.appender;
    if (!appender) {
      throw new Error('archive sink staging appender is not initialized');
    }
    appender.appendVarchar(row.did);
    appender.appendVarchar(row.rkey);
    appender.appendVarchar(row.created_at);
    appender.appendList(listValue(row.langs), LIST_VARCHAR);
    appender.appendList(listValue(row.emojis), LIST_VARCHAR);
    appender.appendVarchar(row.text);
    appender.appendVarchar(row.src);
    appender.appendVarchar(row.facets_json);
    appender.appendVarchar(row.reply_json);
    appender.appendVarchar(row.embed_json);
    appender.appendVarchar(row.labels_json);
    appender.endRow();
    this.unflushed += 1;
    this.rowsInStaging += 1;
    this.openedAt ??= Date.now();
    if (this.unflushed >= APPENDER_FLUSH_EVERY) {
      this.flushAppender();
    }
  }

  private flushAppender(): void {
    if (this.appender && this.unflushed > 0) {
      this.appender.flushSync();
      this.unflushed = 0;
    }
  }

  /**
   * Seeds nextSeq from the directory listing AND the manifest. The manifest
   * matters: a move-style syncCommand (`rclone move`) removes synced files
   * locally, so after a clean run the listing alone would reseed at 1 and the
   * next finalize would re-emit live-000001.parquet — and the sync would
   * overwrite the already-synced remote file of the same name. The manifest
   * never leaves disk, so it remembers every sequence this prefix ever used.
   */
  private async seedSequence(): Promise<void> {
    for (const name of await readdir(this.finalizedDir)) {
      this.noteSequence(name);
    }

    let manifest: string;
    try {
      manifest = await readFile(this.manifestPath, 'utf8');
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
        return;
      }
      throw err;
    }
    for (const line of manifest.split('\n')) {
      if (line.length === 0) {
        continue;
      }
      try {
        const entry = JSON.parse(line) as Partial<ManifestEntry>;
        if (typeof entry.file === 'string') {
          this.noteSequence(entry.file);
        }
      } catch {
        // A crash mid-append can tear the trailing line. Its file was
        // renamed before the manifest write, so the listing pass above
        // already counted it.
      }
    }
  }

  private noteSequence(fileName: string): void {
    const match = this.fileNamePattern.exec(fileName);
    if (match) {
      this.nextSeq = Math.max(this.nextSeq, Number(match[1]) + 1);
    }
  }

  /**
   * Startup sweep, prefix-scoped like seedSequence:
   *
   * - Deletes stale `.parquet.tmp` partials. finalizeStaged orders COPY →
   *   rename → manifest → DELETE FROM staging → CHECKPOINT, then enqueues
   *   the sync, so a surviving .tmp means the rename never happened: its
   *   rows were never manifested and never deleted from staging. The
   *   recovery rotate that follows re-emits them; the partial itself is
   *   pure litter.
   * - When syncCommand is set, re-runs it for every finalized file still on
   *   disk, oldest-first. A failed or never-started background sync leaves
   *   the file finalized locally (loud, not lossy) — this sweep is the
   *   retry. Re-uploads join the normal background sync chain so startup can
   *   reach telemetry/crawling immediately; any failure stays sticky and
   *   surfaces on the next sink call or close().
   */
  private async sweepFinalized(): Promise<void> {
    const backlog: { seq: number; path: string }[] = [];
    for (const name of await readdir(this.finalizedDir)) {
      if (
        name.endsWith('.tmp') &&
        this.fileNamePattern.test(name.slice(0, -'.tmp'.length))
      ) {
        await rm(join(this.finalizedDir, name), { force: true });
        this.removedTmpFiles += 1;
        continue;
      }
      const match = this.fileNamePattern.exec(name);
      if (match && this.opts.syncCommand) {
        backlog.push({
          seq: Number(match[1]),
          path: join(this.finalizedDir, name),
        });
      }
    }

    backlog.sort((a, b) => a.seq - b.seq);
    for (const { path } of backlog) {
      this.enqueueSync(path, { swept: true });
    }
  }

  private async finalizeStaged(): Promise<void> {
    this.flushAppender();

    const reader = await this.connection.runAndReadAll(
      'SELECT count(*) AS rows, min(created_at) AS min_ca, max(created_at) AS max_ca FROM staging',
    );
    const summary = reader.getRowObjects()[0];
    const rows = Number(summary['rows']);
    if (rows === 0) {
      return;
    }

    const fileName = `${this.opts.prefix}-${String(this.nextSeq).padStart(SEQ_PAD, '0')}.parquet`;
    this.nextSeq += 1;
    const finalPath = join(this.finalizedDir, fileName);
    const tmpPath = `${finalPath}.tmp`;

    await rm(tmpPath, { force: true });
    await this.connection.run(
      `COPY (SELECT did, rkey, created_at, langs, emojis, text, src, ` +
        `facets_json, reply_json, embed_json, labels_json FROM staging) ` +
        `TO '${sqlQuote(tmpPath)}' (FORMAT PARQUET, COMPRESSION ZSTD)`,
    );
    await rename(tmpPath, finalPath);

    const { size } = await stat(finalPath);
    await this.appendManifest({
      file: fileName,
      rows,
      bytes: size,
      minCreatedAt: String(summary['min_ca']),
      maxCreatedAt: String(summary['max_ca']),
      finalizedAt: new Date().toISOString(),
      v: ARCHIVE_SCHEMA_VERSION,
    });

    await this.connection.run('DELETE FROM staging');
    await this.connection.run('CHECKPOINT');
    this.rowsInStaging = 0;
    this.openedAt = undefined;
    this.finalizedFiles += 1;

    this.enqueueSync(finalPath);
  }

  /**
   * Hands a finalized file to the background sync chain. The failure is
   * captured into syncFailure rather than rejecting the chain so later
   * uploads still queue behind it (their files would otherwise sit until
   * the next startup sweep for no reason); first failure wins because the
   * run is dead either way once assertOpen rethrows it.
   */
  private enqueueSync(
    finalPath: string,
    options: { swept?: boolean } = {},
  ): void {
    this.syncChain = this.syncChain.then(async () => {
      try {
        await this.runSyncCommand(finalPath);
        if (options.swept) this.sweptFiles += 1;
      } catch (err) {
        this.syncFailure ??= err as Error;
      }
      return undefined;
    });
  }

  private async appendManifest(entry: ManifestEntry): Promise<void> {
    const handle = await open(this.manifestPath, 'a');
    try {
      await handle.writeFile(`${JSON.stringify(entry)}\n`, 'utf8');
      await handle.sync();
    } finally {
      await handle.close();
    }
  }

  private async runSyncCommand(finalPath: string): Promise<void> {
    const { syncCommand } = this.opts;
    if (!syncCommand) {
      return;
    }
    const command = syncCommand.replaceAll('{file}', finalPath);
    try {
      await execFileAsync('/bin/sh', ['-c', command]);
    } catch (cause) {
      throw new Error(
        `archive syncCommand failed for ${finalPath}: ${command}` +
          ` — the file is finalized locally but NOT synced`,
        { cause },
      );
    }
  }
}

export async function createArchiveSink(
  opts: ArchiveSinkOptions,
): Promise<ArchiveSink> {
  return DuckDBArchiveSink.create(opts);
}
