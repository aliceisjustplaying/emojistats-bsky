/**
 * Repo worker: fetches a repo CAR and parses it, entirely off the main
 * thread. Each worker owns its fetch buffers and its parse CPU on its own
 * event loop/heap — launch night proved that both whale-CAR parsing AND
 * concurrent whale-stream buffering on the main thread starve every socket
 * and timer in the process. The main thread dispatches jobs (politeness
 * limiters live there) and receives materialized rows.
 *
 * Inside one worker, fetches overlap on the event loop while parses
 * serialize naturally (sync CPU) — so N workers give N parallel parses and
 * plenty of download concurrency without any extra coordination.
 */
import { parentPort } from 'node:worker_threads';

import type { PostRow } from 'ingest/types';

import { rkeyHash64 } from './digest.js';
import { repoPostRows } from './extract.js';
import {
  fetchRepoCar,
  QuarantineError,
  RetryableError,
  TerminalFetchError,
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
  recordsTotal: number;
  duplicatePostsSkipped: number;
  rows: PostRow[];
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
    }
  | { kind: 'terminal'; message: string; status: TerminalFetchStatus }
  | { kind: 'error'; message: string };

export type RepoJobReply =
  | { seq: number; ok: RepoJobResult }
  | { seq: number; err: RepoJobError };

function describeError(err: unknown): RepoJobError {
  const message = err instanceof Error ? err.message : String(err);
  if (err instanceof QuarantineError) return { kind: 'quarantine', message };
  if (err instanceof TerminalFetchError)
    return { kind: 'terminal', message, status: err.status };
  if (err instanceof RetryableError)
    return {
      kind: 'retryable',
      message,
      transient: err.transient,
      retryAfterMs: err.retryAfterMs,
    };
  return { kind: 'error', message };
}

async function handle(job: RepoJob): Promise<RepoJobReply> {
  try {
    const fetched = await fetchRepoCar(job.pdsHost, job.did);
    const parsed = parseRepoCar(fetched.body);
    const rows: PostRow[] = [];
    let postsWithEmojis = 0;
    let emojiOccurrences = 0;
    let digest = 0n;
    for await (const row of repoPostRows(job.did, parsed, job.fetchTimeUs)) {
      rows.push(row);
      digest ^= rkeyHash64(row.rkey);
      if (row.emojis.length > 0) {
        postsWithEmojis += 1;
        emojiOccurrences += row.emojis.length;
      }
    }
    return {
      seq: job.seq,
      ok: {
        rev: parsed.rev,
        carBytes: fetched.bytesRead(),
        recordsTotal: parsed.recordsTotal,
        duplicatePostsSkipped: parsed.duplicatePostsSkipped,
        rows,
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
port.on('message', (job: RepoJob) => {
  void handle(job).then((reply) => {
    port.postMessage(reply);
    return undefined;
  });
});
