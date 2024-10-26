import { Queue } from 'bullmq';

import { REDIS_URL } from '../config.js';

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
