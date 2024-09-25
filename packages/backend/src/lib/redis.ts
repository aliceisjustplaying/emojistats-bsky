import fs from 'fs';
import { createClient } from 'redis';

import { REDIS_URL } from '../config.js';
import logger from '../logger.js';

const redisClient = createClient({ url: REDIS_URL });

redisClient.on('error', (err: Error) => {
  logger.error('Redis Client Error', { error: err });
});

redisClient.on('connect', () => {
  logger.info('Connected to Redis');
});

redisClient.on('ready', () => {
  logger.info('Redis client ready');
});

redisClient.on('end', () => {
  logger.info('Redis client disconnected');
});

let SCRIPT_SHA: string;

const loadRedisScripts = async () => {
  const scriptPath = new URL('lua/incrementEmojis.lua', import.meta.url);
  const incrementEmojisScript = fs.readFileSync(scriptPath, 'utf8');
  SCRIPT_SHA = await redisClient.scriptLoad(incrementEmojisScript);
  console.log(`Loaded Redis script with SHA: ${SCRIPT_SHA}`);
};

export { redisClient, loadRedisScripts, SCRIPT_SHA };
