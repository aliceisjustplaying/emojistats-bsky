import { EMIT_INTERVAL, LOG_INTERVAL, METRICS_PORT, PORT } from './config.js';
import { getEmojiStats, getTopLanguages, initiateShutdown, logEmojiStats } from './lib/emojiStats.js';
import { flushPostgresBatch } from './lib/emojiStats.js';
import { initializeJetstream, jetstream } from './lib/jetstream.js';
import logger from './lib/logger.js';
import { startMetricsServer } from './lib/metrics.js';
import { run } from './lib/mqui.js';
import { pool } from './lib/postgres.js';
import { postQueue, worker } from './lib/queue.js';
import { loadRedisScripts, redis } from './lib/redis.js';
import { io, startSocketServer } from './lib/socket.io.js';

/* redis initialization */
await redis.connect();
await loadRedisScripts();
/* End Redis initialization */

/* Jetstream initialization */
await initializeJetstream();
jetstream.start();
/* End Jetstream initialization */

/* socket.io server initialization */
startSocketServer(Number(PORT));
/* End socket.io server initialization */

/* BullMQ UI */
await run();
/* End BullMQ UI */

/* emitting data for frontend */
setInterval(() => {
  Promise.all([getEmojiStats(), getTopLanguages()])
    .then(([stats, languages]) => {
      io.emit('emojiStats', stats);
      io.emit('languageStats', languages);
    })
    .catch((error: unknown) => {
      logger.error(`Error emitting or logging emoji stats: ${(error as Error).message}`);
    });
}, EMIT_INTERVAL);
/* End emitting data for frontend */

/* metrics server */
const metricsServer = startMetricsServer(Number(METRICS_PORT));
/* End metrics server */

/* logging stats to the console */
setInterval(() => {
  getEmojiStats()
    .then(() => {
      return logEmojiStats();
    })
    .catch((error: unknown) => {
      logger.error(`Error emitting or logging emoji stats: ${(error as Error).message}`);
    });
}, LOG_INTERVAL);
/* End logging stats */

let isShuttingDown = false;

async function shutdown() {
  if (isShuttingDown) {
    logger.info('Shutdown called but one is already in progress.');
    return;
  }

  isShuttingDown = true;

  logger.info('Shutting down gracefully...');
  try {
    jetstream.close();
  } catch (error) {
    logger.error(`Error closing Jetstream: ${(error as Error).message}`);
  }

  try {
    await worker.close();
    await postQueue.close();
    logger.info('BullMQ worker and queue closed.');
  } catch (error) {
    logger.error(`Error closing BullMQ: ${(error as Error).message}`);
  }

  try {
    await initiateShutdown();
  } catch (error) {
    logger.error(`Error initiating shutdown: ${(error as Error).message}`);
  }

  try {
    await flushPostgresBatch();
    logger.info('Flushed remaining PostgreSQL batch.');
  } catch (error) {
    logger.error(`Error flushing PostgreSQL batch during shutdown: ${(error as Error).message}`);
  }

  try {
    await io.close();
  } catch (error) {
    logger.error(`Error closing Socket.io server: ${(error as Error).message}`);
  }

  try {
    metricsServer.close();
  } catch (error) {
    logger.error(`Error closing Metrics server: ${(error as Error).message}`);
  }

  try {
    await redis.quit();
  } catch (error) {
    logger.error(`Error disconnecting Redis client: ${(error as Error).message}`);
  }

  try {
    await pool.end();
    logger.info('PostgreSQL pool disconnected.');
  } catch (error) {
    logger.error(`Error disconnecting PostgreSQL pool: ${(error as Error).message}`);
  }

  process.exit(0);
}

process.on('SIGINT', () => {
  shutdown().catch((error: unknown) => {
    logger.error(`Shutdown failed: ${(error as Error).message}`);
    process.exit(1);
  });
});

process.on('SIGTERM', () => {
  shutdown().catch((error: unknown) => {
    logger.error(`Shutdown failed: ${(error as Error).message}`);
    process.exit(1);
  });
});
