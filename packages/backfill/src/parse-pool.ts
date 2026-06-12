/**
 * Fixed pool of parse workers (parse-worker.ts). The pool deliberately has no
 * queue-depth limit of its own: callers hold a GLOBAL_CONCURRENCY slot for the
 * whole fetch→parse→load span, so at most that many jobs (and their buffered
 * CARs) can exist at once — the same resident-CAR memory envelope the old
 * in-process parser had.
 */
import os from 'node:os';
import { Worker } from 'node:worker_threads';

import { PARSE_WORKERS } from './config.js';
import { QuarantineError, RetryableError } from './fetcher.js';
import logger from './logger.js';
import type { ParseReply, ParseResult } from './parse-worker.js';

interface PendingJob {
  resolve: (result: ParseResult) => void;
  reject: (err: Error) => void;
}

interface QueuedJob {
  did: string;
  car: ArrayBuffer;
  fetchTimeUs: number;
  pending: PendingJob;
}

function rehydrate(err: {
  kind: 'quarantine' | 'retryable' | 'error';
  message: string;
}): Error {
  if (err.kind === 'quarantine') return new QuarantineError(err.message);
  if (err.kind === 'retryable')
    return new RetryableError(err.message, { transient: true });
  return new Error(err.message);
}

export interface ParsePool {
  parse(
    did: string,
    car: ArrayBuffer,
    fetchTimeUs: number,
  ): Promise<ParseResult>;
  close(): Promise<void>;
}

export function createParsePool(): ParsePool {
  const size =
    PARSE_WORKERS > 0
      ? PARSE_WORKERS
      : Math.max(1, os.availableParallelism() - 2);

  const idle: Worker[] = [];
  const queue: QueuedJob[] = [];
  const pendingByWorker = new Map<Worker, Map<number, PendingJob>>();
  const all = new Set<Worker>();
  let seq = 0;
  let closed = false;

  const dispatch = (worker: Worker, job: QueuedJob): void => {
    seq += 1;
    pendingByWorker.get(worker)!.set(seq, job.pending);
    worker.postMessage(
      { seq, did: job.did, car: job.car, fetchTimeUs: job.fetchTimeUs },
      [job.car],
    );
  };

  const release = (worker: Worker): void => {
    const next = queue.shift();
    if (next !== undefined) dispatch(worker, next);
    else idle.push(worker);
  };

  const spawn = (): void => {
    // The service runs under tsx; workers need the loader spelled out or the
    // .ts entry fails to resolve inside the thread.
    const worker = new Worker(new URL('./parse-worker.ts', import.meta.url), {
      execArgv: ['--import', 'tsx'],
    });
    all.add(worker);
    pendingByWorker.set(worker, new Map());
    worker.on('message', (reply: ParseReply) => {
      const pending = pendingByWorker.get(worker)!;
      const job = pending.get(reply.seq);
      if (job === undefined) return;
      pending.delete(reply.seq);
      if ('ok' in reply) job.resolve(reply.ok);
      else job.reject(rehydrate(reply.err));
      release(worker);
    });
    const fail = (cause: string) => {
      const pending = pendingByWorker.get(worker);
      pendingByWorker.delete(worker);
      all.delete(worker);
      const i = idle.indexOf(worker);
      if (i !== -1) idle.splice(i, 1);
      for (const job of pending?.values() ?? []) {
        job.reject(
          new RetryableError(`parse worker died: ${cause}`, {
            transient: true,
          }),
        );
      }
      if (!closed) {
        logger.error({ cause }, 'parse worker died; respawning');
        spawn();
      }
    };
    worker.on('error', (err) => fail(err.message));
    worker.on('exit', (code) => {
      if (code !== 0) fail(`exit code ${code}`);
    });
    idle.push(worker);
  };

  for (let i = 0; i < size; i += 1) spawn();
  logger.info({ workers: size }, 'parse worker pool up');

  return {
    parse(did, car, fetchTimeUs) {
      return new Promise<ParseResult>((resolve, reject) => {
        const job: QueuedJob = {
          did,
          car,
          fetchTimeUs,
          pending: { resolve, reject },
        };
        const worker = idle.pop();
        if (worker !== undefined) dispatch(worker, job);
        else queue.push(job);
      });
    },
    async close() {
      closed = true;
      await Promise.all([...all].map((worker) => worker.terminate()));
    },
  };
}
