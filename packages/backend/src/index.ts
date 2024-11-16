import { EMIT_INTERVAL, LOG_INTERVAL, METRICS_PORT, PORT } from './config.js';
import { getEmojiStats, getTopLanguages, logEmojiStats } from './lib/emojiStats.js';
import { initializeJetstream, jetstream } from './lib/jetstream.js';
import logger from './lib/logger.js';
import { startMetricsServer } from './lib/metrics.js';
import { startBullMQUI } from './lib/mqui.js';
import { pool } from './lib/postgres.js';
import { postQueue } from './lib/queue.js';
import { redis } from './lib/redis.js';
import { io, startSocketServer } from './lib/socket.io.js';
import { worker, workers } from './lib/worker.js';

/* redis initialization */
await redis.connect();
/* End Redis initialization */

/* Jetstream initialization */
await initializeJetstream();
jetstream.start();
/* End Jetstream initialization */

/* socket.io server initialization */
startSocketServer(Number(PORT));
/* End socket.io server initialization */

/* BullMQ UI */
const bullMQUI = await startBullMQUI();
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
    await Promise.all(workers.map((worker) => worker.close()));
    // await worker.close();
    await postQueue.close();
    logger.info('BullMQ worker and queue closed.');
  } catch (error) {
    logger.error(`Error closing BullMQ: ${(error as Error).message}`);
  }

  try {
    await bullMQUI.close();
  } catch (error) {
    logger.error(`Error closing BullMQ UI: ${(error as Error).message}`);
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
    await pool.end();
    logger.info('PostgreSQL pool disconnected.');
  } catch (error) {
    logger.error(`Error disconnecting PostgreSQL pool: ${(error as Error).message}`);
  }

  try {
    await redis.quit();
  } catch (error) {
    logger.error(`Error disconnecting Redis client: ${(error as Error).message}`);
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
