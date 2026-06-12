/**
 * Router over the repo workers (parse-worker.ts). Jobs are dispatched
 * round-robin immediately — a worker runs many concurrent fetches on its own
 * event loop while its parses serialize on its own core. Total in-flight jobs
 * are capped by the caller (the scheduler's GLOBAL_CONCURRENCY slot is held
 * for the whole fetch→parse→load span), so the pool needs no queue of its own.
 */
import os from 'node:os';
import { Worker } from 'node:worker_threads';

import { PARSE_WORKERS } from './config.js';
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
}

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
    default:
      return new Error(err.message);
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
      if ('ok' in reply) job.resolve(reply.ok);
      else job.reject(rehydrate(reply.err));
    });
    const fail = (cause: string) => {
      const pending = pendingByWorker.get(worker);
      pendingByWorker.delete(worker);
      const i = workers.indexOf(worker);
      for (const job of pending?.values() ?? []) {
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
    };
    worker.on('error', (err) => fail(err.message));
    worker.on('exit', (code) => {
      if (code !== 0) fail(`exit code ${code}`);
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
        pendingByWorker.get(worker)!.set(seq, { resolve, reject });
        worker.postMessage({ seq, did, pdsHost, fetchTimeUs });
      });
    },
    async close() {
      closed = true;
      await Promise.all(workers.map((worker) => worker.terminate()));
    },
  };
}
