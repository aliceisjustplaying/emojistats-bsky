import { CURSOR_UPDATE_INTERVAL } from '../config.js';
import logger from '../logger.js';
import { redisClient } from './redis.js';

let latestCursor: string;

export function getLatestCursor(): string {
  return latestCursor;
}

export function setLatestCursor(value: string): void {
  latestCursor = value;
}

export async function getLastCursor(): Promise<string> {
  logger.debug('Getting last cursor...');
  const result = await redisClient.get('cursor');
  if (!result) {
    logger.info('No cursor found, initializing with current epoch in microseconds...');
    const currentEpochMicroseconds = BigInt(Date.now()) * 1000n;
    await redisClient.set('cursor', currentEpochMicroseconds.toString());
    logger.info(
      `Initialized cursor with value: ${currentEpochMicroseconds} (${new Date(Number(currentEpochMicroseconds.toString()) / 1000).toISOString()})`,
    );
    return currentEpochMicroseconds.toString();
  }
  logger.info(`Returning cursor from Redis: ${result} (${new Date(Number(result) / 1000).toISOString()})`);
  return result;
}

export async function updateLastCursor(newCursor: string): Promise<void> {
  try {
    await redisClient.set('cursor', newCursor);
    logger.info(`Updated last cursor to ${newCursor} (${new Date(Number(newCursor) / 1000).toISOString()})`);
  } catch (error: unknown) {
    logger.error(`Error updating cursor: ${(error as Error).message}`);
  }
}

export let cursorUpdateInterval: NodeJS.Timeout | undefined;

export function initializeCursorUpdate(cursor: string) {
  latestCursor = cursor;
  cursorUpdateInterval = setInterval(() => {
    updateLastCursor(latestCursor).catch((error: unknown) => {
      logger.error(`Error updating cursor: ${(error as Error).message}`);
    });
  }, CURSOR_UPDATE_INTERVAL);
}
