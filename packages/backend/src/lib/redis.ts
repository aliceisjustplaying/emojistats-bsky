import { createClient } from 'redis';

import { REDIS_URL } from '../config.js';
import logger from './logger.js';

const redis = createClient({ url: REDIS_URL });

redis.on('error', (err: Error) => {
  logger.error('Redis Client Error', { error: err });
});

redis.on('connect', () => {
  logger.info('Connected to Redis.');
});

redis.on('ready', () => {
  logger.info('Redis client ready.');
});

redis.on('end', () => {
  logger.info('Redis client disconnected.');
});

export { redis };
