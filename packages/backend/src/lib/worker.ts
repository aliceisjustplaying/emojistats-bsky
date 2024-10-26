import { Worker } from 'bullmq';
import { Insertable } from 'kysely';

import { BULLMQ_CONCURRENCY, REDIS_URL } from '../config.js';
import { flushBatchToDatabase } from './batchProcessor.js';
import logger from './logger.js';
import { postQueue } from './queue.js';
import { Emojis, Posts } from './schema.js';

const BATCH_SIZE = 1000;

const createWorker = (id: number) => {
  let currentBatch: { postData: Insertable<Posts>; emojiData: Insertable<Emojis>[] }[] = [];
  let isProcessing = false;

  const processBatch = async () => {
    if (isProcessing || currentBatch.length === 0) {
      process.stdout.write('.');
      return;
    }
    isProcessing = true;

    const batchToProcess = [...currentBatch];
    currentBatch = [];

    try {
      await flushBatchToDatabase(batchToProcess);
      logger.debug(`Worker ${id}: Processed batch of ${batchToProcess.length} posts.`);
    } catch (error) {
      logger.error(`Worker ${id}: Error processing batch: ${(error as Error).message}`);
      for (const jobData of batchToProcess) {
        await postQueue.add('process-post', jobData);
      }
      logger.info(`Worker ${id}: Re-enqueued ${batchToProcess.length} failed jobs.`);
    } finally {
      isProcessing = false;
    }
  };

  const worker = new Worker<{ postData: Insertable<Posts>; emojiData: Insertable<Emojis>[] }>(
    'post-processing',
    async (job) => {
      currentBatch.push(job.data);

      // Count total emojis and posts in current batch
      const totalEmojis = currentBatch.reduce((sum, item) => sum + item.emojiData.length, 0);
      const totalPosts = currentBatch.length;

      if (totalEmojis >= BATCH_SIZE || totalPosts >= BATCH_SIZE) {
        await processBatch();
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
