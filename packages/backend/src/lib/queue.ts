import { Queue, Worker } from 'bullmq';

import { BULLMQ_CONCURRENCY, REDIS_URL } from '../config.js';
import { flushBatchToDatabase } from './batchProcessor.js';
import logger from './logger.js';

// Separate batch processing logic

export const postQueue = new Queue('post-processing', {
  connection: {
    url: REDIS_URL,
  },
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 1000,
    },
    removeOnComplete: true,
    removeOnFail: 1000,
  },
});

// Batch processing parameters
const BATCH_SIZE = 1000;
const BATCH_TIMEOUT_MS = 1000;

/**
 * Creates a Worker instance with its own batching context.
 * @param id - Identifier for the worker instance.
 * @returns A Worker instance.
 */
const createWorker = (id: number) => {
  let currentBatch: { postData: any[]; emojiData: any[] }[] = [];
  let batchTimer: NodeJS.Timeout | null = null;
  let isProcessing = false;

  /**
   * Processes the current batch by flushing it to the database.
   */
  const processBatch = async () => {
    if (isProcessing || currentBatch.length === 0) return;
    isProcessing = true;

    const batchToProcess = [...currentBatch];
    currentBatch = [];
    batchTimer = null;

    try {
      await flushBatchToDatabase(batchToProcess);
      logger.info(`Worker ${id}: Processed batch of ${batchToProcess.length} posts.`);
    } catch (error) {
      logger.error(`Worker ${id}: Error processing batch: ${(error as Error).message}`);
      // Re-enqueue failed jobs for retry
      for (const jobData of batchToProcess) {
        await postQueue.add('process-post', jobData);
      }
      logger.info(`Worker ${id}: Re-enqueued ${batchToProcess.length} failed jobs.`);
    } finally {
      isProcessing = false;
    }
  };

  const worker = new Worker<{ postData: any[]; emojiData: any[] }>(
    'post-processing',
    async (job) => {
      currentBatch.push(job.data);

      if (currentBatch.length >= BATCH_SIZE && !batchTimer) {
        await processBatch();
      } else if (!batchTimer) {
        batchTimer = setTimeout(processBatch, BATCH_TIMEOUT_MS);
      }
    },
    {
      connection: {
        url: REDIS_URL,
      },
      concurrency: 1,
    },
  );

  worker.on('completed', (job) => {
    logger.debug(`Worker ${id}: Job ${job.id} completed`);
  });

  worker.on('failed', (job, error) => {
    logger.error(`Worker ${id}: Job ${job?.id} failed: ${error.message}`);
  });

  return worker;
};

const workers: Worker[] = [];
for (let i = 0; i < BULLMQ_CONCURRENCY; i++) {
  const worker = createWorker(i + 1);
  workers.push(worker);
}

export { workers };
