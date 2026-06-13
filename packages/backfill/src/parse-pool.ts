/**
 * Router over the repo workers (parse-worker.ts). Jobs are dispatched
 * round-robin immediately — a worker runs many concurrent fetches on its own
 * event loop while its parses serialize on its own core. Total in-flight jobs
 * are capped by the caller (the scheduler's GLOBAL_CONCURRENCY slot is held
 * for the whole fetch→parse→load span), so the pool needs no queue of its own.
 */
import os from 'node:os';
import { Worker } from 'node:worker_threads';

import { PARSE_WORKERS, REPO_FETCH_TIMEOUT_MS } from './config.js';
import {
  QuarantineError,
  RetryableError,
  TerminalFetchError,
} from './fetcher.js';
import logger from './logger.js';
import type {
  RepoJobError,
  RepoJobReply,
  RepoJobResult,
} from './parse-worker.js';

interface PendingJob {
  resolve: (result: RepoJobResult) => void;
  reject: (err: Error) => void;
  timer: NodeJS.Timeout;
}

/**
 * Backstop for a dispatched job whose worker never posts a reply, leaking its
 * GLOBAL_CONCURRENCY slot forever; enough leaks wedge the scheduler (inFlight
 * pinned, 0 CPU, telemetry frozen — recurring on crawl1/crawl2, 2026-06-13).
 * fetcher.ts already caps the getRepo fetch at REPO_FETCH_TIMEOUT_MS, so a
 * stalled fetch normally rejects there and frees the slot cleanly; the leak is
 * the residual case where that abort does NOT fire (dead socket the runtime
 * never surfaces). This ceiling sits a full fetch-budget ABOVE that timeout so
 * it never races the fetcher. Two cases can reach this ceiling: (a) a residual
 * fetch stall where the fetcher's own 300s abort failed to fire (the leak this
 * targets), or (b) a successful fetch (<= 300s) followed by a parse/materialize
 * that exceeds the remaining ~300s. (b) is the only theoretical false-kill, and
 * it is pathological — CBOR decode + MST walk + row build is CPU-bound seconds
 * even for GB-scale CARs — and harmless besides, since the repo just requeues.
 * On trip the job rejects RetryableError (repo requeues, at-least-once) and any
 * late reply is dropped by the unknown-seq guard.
 */
const REPLY_TIMEOUT_MS = REPO_FETCH_TIMEOUT_MS + 300_000;

function rehydrate(err: RepoJobError): Error {
  switch (err.kind) {
    case 'quarantine':
      return new QuarantineError(err.message);
    case 'terminal':
      return new TerminalFetchError(err.status, err.message);
    case 'retryable':
      return new RetryableError(err.message, {
        transient: err.transient,
        retryAfterMs: err.retryAfterMs,
      });
    case 'error':
      return new Error(err.message);
    default: {
      const exhaustive: never = err;
      return exhaustive;
    }
  }
}

export interface ParsePool {
  run(
    did: string,
    pdsHost: string,
    fetchTimeUs: number,
  ): Promise<RepoJobResult>;
  close(): Promise<void>;
}

export function createParsePool(): ParsePool {
  const size =
    PARSE_WORKERS > 0
      ? PARSE_WORKERS
      : Math.max(1, os.availableParallelism() - 2);

  const workers: Worker[] = [];
  const pendingByWorker = new Map<Worker, Map<number, PendingJob>>();
  let seq = 0;
  let rr = 0;
  let closed = false;

  const failWorker = (
    worker: Worker,
    cause: string,
    terminate = false,
  ): void => {
    const pending = pendingByWorker.get(worker);
    pendingByWorker.delete(worker);
    const i = workers.indexOf(worker);
    for (const job of pending?.values() ?? []) {
      clearTimeout(job.timer);
      job.reject(
        new RetryableError(`repo worker died: ${cause}`, {
          transient: true,
        }),
      );
    }
    if (!closed && i !== -1) {
      logger.error({ cause }, 'repo worker died; respawning');
      workers[i] = spawn();
    }
    if (terminate) void worker.terminate();
  };

  const spawn = (): Worker => {
    // The service runs under tsx; workers need the loader spelled out or the
    // .ts entry fails to resolve inside the thread.
    const worker = new Worker(new URL('./parse-worker.ts', import.meta.url), {
      execArgv: ['--import', 'tsx'],
    });
    pendingByWorker.set(worker, new Map());
    worker.on('message', (reply: RepoJobReply) => {
      const pending = pendingByWorker.get(worker);
      const job = pending?.get(reply.seq);
      if (job === undefined) return;
      pending!.delete(reply.seq);
      clearTimeout(job.timer);
      if ('ok' in reply) job.resolve(reply.ok);
      else job.reject(rehydrate(reply.err));
    });
    worker.on('error', (err) => failWorker(worker, err.message));
    worker.on('exit', (code) => {
      if (code !== 0) failWorker(worker, `exit code ${code}`);
    });
    return worker;
  };

  for (let i = 0; i < size; i += 1) workers.push(spawn());
  logger.info({ workers: size }, 'repo worker pool up');

  return {
    run(did, pdsHost, fetchTimeUs) {
      return new Promise<RepoJobResult>((resolve, reject) => {
        rr = (rr + 1) % workers.length;
        const worker = workers[rr];
        seq += 1;
        const jobSeq = seq;
        const pending = pendingByWorker.get(worker)!;
        // If a worker never replies, the worker itself is suspect: reject every
        // job on it, terminate it, and respawn so hidden work cannot pile up.
        const timer = setTimeout(() => {
          if (!pending.has(jobSeq)) return;
          failWorker(worker, `reply timeout after ${REPLY_TIMEOUT_MS}ms`, true);
        }, REPLY_TIMEOUT_MS);
        timer.unref();
        pending.set(jobSeq, {
          resolve: (result) => {
            resolve(result);
          },
          reject: (err) => {
            reject(err);
          },
          timer,
        });
        worker.postMessage({ seq: jobSeq, did, pdsHost, fetchTimeUs });
      });
    },
    async close() {
      closed = true;
      await Promise.all(workers.map((worker) => worker.terminate()));
    },
  };
}
