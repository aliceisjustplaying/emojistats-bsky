import { CommitCreateEvent } from '@skyware/jetstream';
import { Queue, Worker } from 'bullmq';

import { BULLMQ_CONCURRENCY, REDIS_URL } from '../config.js';
import { handleCreate } from './emojiStats.js';
import logger from './logger.js';

export const postQueue = new Queue<CommitCreateEvent<'app.bsky.feed.post'>>('post-processing', {
  connection: {
    url: REDIS_URL,
  },
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 1000,
    },
  },
});

const worker = new Worker<CommitCreateEvent<'app.bsky.feed.post'>>(
  'post-processing',
  async (job) => {
    try {
      await handleCreate(job.data);
    } catch (error) {
      logger.error(`Error processing job ${job.id}: ${(error as Error).message}`);
      throw error;
    }
  },
  {
    connection: {
      url: REDIS_URL,
    },
    concurrency: BULLMQ_CONCURRENCY,
  },
);

worker.on('completed', (job) => {
  logger.debug(`Job ${job.id} completed`);
});

worker.on('failed', (job, error) => {
  logger.error(`Job ${job?.id} failed: ${error.message}`);
});

export { worker };
