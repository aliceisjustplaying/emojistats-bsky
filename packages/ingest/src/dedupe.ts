import fs from 'node:fs';
import path from 'node:path';

import Database from 'better-sqlite3';

/**
 * Cross-restart post dedupe backed by SQLite. Marks land only after the batch
 * is durable (ClickHouse insert success + archive append settlement), so a
 * mark here always means "safe to drop on replay". WAL + synchronous=NORMAL
 * can drop the last few transactions on a crash; the cost is re-ingesting a
 * handful of posts, which ReplacingMergeTree collapses anyway.
 */
export class DedupeStore {
  private readonly db: Database.Database;
  private readonly insertSeen: Database.Statement;
  private readonly selectSeen: Database.Statement;
  private readonly deleteExpired: Database.Statement;
  private readonly insertSeenBatch: Database.Transaction<
    (keys: ReadonlyArray<{ did: string; rkey: string }>, nowMs: number) => void
  >;

  constructor(dbPath: string) {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.exec(
      'CREATE TABLE IF NOT EXISTS seen_posts (post_id TEXT PRIMARY KEY, seen_at_ms INTEGER NOT NULL)',
    );
    this.insertSeen = this.db.prepare(
      'INSERT OR IGNORE INTO seen_posts (post_id, seen_at_ms) VALUES (?, ?)',
    );
    this.selectSeen = this.db.prepare(
      'SELECT 1 FROM seen_posts WHERE post_id = ?',
    );
    this.deleteExpired = this.db.prepare(
      'DELETE FROM seen_posts WHERE seen_at_ms < ?',
    );
    this.insertSeenBatch = this.db.transaction((keys, nowMs) => {
      for (const { did, rkey } of keys) {
        this.insertSeen.run(`${did}/${rkey}`, nowMs);
      }
    });
  }

  /** Pure lookup — the receive path checks here but never writes. */
  isSeen(did: string, rkey: string): boolean {
    return this.selectSeen.get(`${did}/${rkey}`) !== undefined;
  }

  /** Records a durable batch in one transaction (INSERT OR IGNORE per key). */
  markSeenBatch(
    keys: ReadonlyArray<{ did: string; rkey: string }>,
    nowMs: number,
  ): void {
    this.insertSeenBatch(keys, nowMs);
  }

  /** Drops entries older than the retention window; returns the number deleted. */
  cleanup(retentionHours: number): number {
    const cutoffMs = Date.now() - retentionHours * 3_600_000;
    return this.deleteExpired.run(cutoffMs).changes;
  }

  close(): void {
    this.db.close();
  }
}
