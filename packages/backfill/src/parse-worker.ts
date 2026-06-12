/**
 * Parse worker: receives a fully-buffered CAR (zero-copy ArrayBuffer transfer),
 * walks it with the existing parser and sends back materialized post rows plus
 * the counters the pipeline needs. All CPU lives here so the main thread stays
 * pure I/O — launch night proved one whale repo on the main thread starves
 * every socket and timer in the process.
 */
import { parentPort } from 'node:worker_threads';

import type { PostRow } from 'ingest/types';

import { rkeyHash64 } from './digest.js';
import { repoPostRows } from './extract.js';
import { QuarantineError, RetryableError } from './fetcher.js';
import { parseRepoCar } from './parser.js';

export interface ParseJob {
  seq: number;
  did: string;
  car: ArrayBuffer;
  fetchTimeUs: number;
}

export interface ParseResult {
  rev: string | null;
  recordsTotal: number;
  duplicatePostsSkipped: number;
  rows: PostRow[];
  postsWithEmojis: number;
  emojiOccurrences: number;
  /** 16-hex-digit XOR fold of rkeyHash64 over rows, zero-padded. */
  rkeyDigestHex: string;
}

export type ParseReply =
  | { seq: number; ok: ParseResult }
  | {
      seq: number;
      err: { kind: 'quarantine' | 'retryable' | 'error'; message: string };
    };

function singleChunkStream(car: ArrayBuffer): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new Uint8Array(car));
      controller.close();
    },
  });
}

async function handle(job: ParseJob): Promise<ParseReply> {
  try {
    const parsed = parseRepoCar(singleChunkStream(job.car));
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
        recordsTotal: parsed.recordsTotal,
        duplicatePostsSkipped: parsed.duplicatePostsSkipped,
        rows,
        postsWithEmojis,
        emojiOccurrences,
        rkeyDigestHex: digest.toString(16).padStart(16, '0'),
      },
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const kind =
      err instanceof QuarantineError
        ? 'quarantine'
        : err instanceof RetryableError
          ? 'retryable'
          : 'error';
    return { seq: job.seq, err: { kind, message } };
  }
}

if (parentPort === null) {
  throw new Error('parse-worker must run inside a worker thread');
}
const port = parentPort;
port.on('message', (job: ParseJob) => {
  void handle(job).then((reply) => {
    port.postMessage(reply);
  });
});
