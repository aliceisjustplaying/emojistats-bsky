import fs from 'fs';
import { createClient } from 'redis';

import { REDIS_URL } from '../config.js';
import logger from '../logger.js';

const redis = createClient({ url: REDIS_URL });

redis.on('error', (err: Error) => {
  logger.error('Redis Client Error', { error: err });
});

redis.on('connect', () => {
  logger.info('Connected to Redis');
});

redis.on('ready', () => {
  logger.info('Redis client ready');
});

redis.on('end', () => {
  logger.info('Redis client disconnected');
});

let SCRIPT_SHA: string;

const loadRedisScripts = async () => {
  const scriptPath = new URL('lua/incrementEmojis.lua', import.meta.url);
  const incrementEmojisScript = fs.readFileSync(scriptPath, 'utf8');
  SCRIPT_SHA = await redis.scriptLoad(incrementEmojisScript);
  logger.info(`Loaded Redis script with SHA: ${SCRIPT_SHA}`);
};

export { redis, loadRedisScripts, SCRIPT_SHA };
