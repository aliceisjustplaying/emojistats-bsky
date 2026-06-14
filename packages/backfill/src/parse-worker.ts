/**
 * Repo worker: fetches a repo CAR and parses it, entirely off the main
 * thread. Each worker owns its fetch buffers and its parse CPU on its own
 * event loop/heap — launch night proved that both whale-CAR parsing AND
 * concurrent whale-stream buffering on the main thread starve every socket
 * and timer in the process. The main thread dispatches jobs (politeness
 * limiters live there) and receives acknowledged row batches.
 *
 * Inside one worker, fetches overlap on the event loop while parses
 * serialize naturally (sync CPU) — so N workers give N parallel parses and
 * plenty of download concurrency without any extra coordination.
 */
import { parentPort } from 'node:worker_threads';

import type { ArchiveRow } from 'archive/types';

import { PARSE_ROW_BATCH_ROWS } from './config.js';
import { rkeyHash64 } from './digest.js';
import { repoPostRows } from './extract.js';
import {
  fetchRepoCar,
  QuarantineError,
  RetryableError,
  TerminalFetchError,
  type RateLimitHint,
  type TerminalFetchStatus,
} from './fetcher.js';
import { parseRepoCar } from './parser.js';

export interface RepoJob {
  seq: number;
  did: string;
  pdsHost: string;
  fetchTimeUs: number;
}

export interface RepoJobResult {
  rev: string | null;
  carBytes: number;
  rateLimit: RateLimitHint;
  recordsTotal: number;
  duplicatePostsSkipped: number;
  postsTotal: number;
  postsWithEmojis: number;
  emojiOccurrences: number;
  /** 16-hex-digit XOR fold of rkeyHash64 over rows, zero-padded. */
  rkeyDigestHex: string;
}

export type RepoJobError =
  | { kind: 'quarantine'; message: string }
  | {
      kind: 'retryable';
      message: string;
      transient: boolean;
      retryAfterMs: number | undefined;
      rateLimit: RateLimitHint | undefined;
    }
  | {
      kind: 'terminal';
      message: string;
      status: TerminalFetchStatus;
      rateLimit: RateLimitHint | undefined;
    }
  | { kind: 'error'; message: string };

export type RepoJobReply =
  | { seq: number; batch: number; rev: string | null; rows: ArchiveRow[] }
  | { seq: number; ok: RepoJobResult }
  | { seq: number; err: RepoJobError };

type RepoWorkerMessage = RepoJob | { seq: number; batch: number; ack: true };

function describeError(err: unknown): RepoJobError {
  const message = err instanceof Error ? err.message : String(err);
  if (err instanceof QuarantineError) return { kind: 'quarantine', message };
  if (err instanceof TerminalFetchError)
    return {
      kind: 'terminal',
      message,
      status: err.status,
      rateLimit: err.rateLimit,
    };
  if (err instanceof RetryableError)
    return {
      kind: 'retryable',
      message,
      transient: err.transient,
      retryAfterMs: err.retryAfterMs,
      rateLimit: err.rateLimit,
    };
  return { kind: 'error', message };
}

const ackWaiters = new Map<string, () => void>();

function ackKey(seq: number, batch: number): string {
  return `${seq}:${batch}`;
}

async function postRowsAndWait(
  seq: number,
  batch: number,
  rev: string | null,
  rows: ArchiveRow[],
): Promise<void> {
  await new Promise<void>((resolve) => {
    ackWaiters.set(ackKey(seq, batch), resolve);
    port.postMessage({ seq, batch, rev, rows });
  });
}

async function handle(job: RepoJob): Promise<RepoJobReply> {
  try {
    const fetched = await fetchRepoCar(job.pdsHost, job.did);
    const parsed = parseRepoCar(fetched.body);
    let rows: ArchiveRow[] = [];
    let rowBatches = 0;
    let postsTotal = 0;
    let postsWithEmojis = 0;
    let emojiOccurrences = 0;
    let digest = 0n;
    for await (const row of repoPostRows(job.did, parsed, job.fetchTimeUs)) {
      rows.push(row);
      postsTotal += 1;
      digest ^= rkeyHash64(row.rkey);
      if (row.emojis.length > 0) {
        postsWithEmojis += 1;
        emojiOccurrences += row.emojis.length;
      }
      if (rows.length >= PARSE_ROW_BATCH_ROWS) {
        rowBatches += 1;
        await postRowsAndWait(job.seq, rowBatches, parsed.rev, rows);
        rows = [];
      }
    }
    if (rows.length > 0) {
      rowBatches += 1;
      await postRowsAndWait(job.seq, rowBatches, parsed.rev, rows);
    }
    return {
      seq: job.seq,
      ok: {
        rev: parsed.rev,
        carBytes: fetched.bytesRead(),
        rateLimit: fetched.rateLimit,
        recordsTotal: parsed.recordsTotal,
        duplicatePostsSkipped: parsed.duplicatePostsSkipped,
        postsTotal,
        postsWithEmojis,
        emojiOccurrences,
        rkeyDigestHex: digest.toString(16).padStart(16, '0'),
      },
    };
  } catch (err) {
    return { seq: job.seq, err: describeError(err) };
  }
}

if (parentPort === null) {
  throw new Error('parse-worker must run inside a worker thread');
}
const port = parentPort;
port.on('message', (message: RepoWorkerMessage) => {
  if ('ack' in message) {
    const waiter = ackWaiters.get(ackKey(message.seq, message.batch));
    if (waiter === undefined) return;
    ackWaiters.delete(ackKey(message.seq, message.batch));
    waiter();
    return;
  }
  void handle(message).then((reply) => {
    port.postMessage(reply);
    return undefined;
  });
});
