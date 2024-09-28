import { EMIT_INTERVAL, LOG_INTERVAL, METRICS_PORT, PORT } from './config.js';
import { cursorUpdateInterval, getLastCursor } from './lib/cursor.js';
import { getEmojiStats, getTopLanguages, logEmojiStats } from './lib/emojiStats.js';
import { initializeJetstream, jetstream } from './lib/jetstream.js';
import logger from './lib/logger.js';
import { startMetricsServer } from './lib/metrics.js';
import { loadRedisScripts, redis } from './lib/redis.js';
import { io, startSocketServer } from './lib/socket.io.js';

/* redis initialization */
await redis.connect();
await loadRedisScripts();
/* End Redis initialization */

/* cursor initialization */
const cursor = await getLastCursor();
/* End cursor initialization */

/* Jetstream initialization */
initializeJetstream(cursor);
jetstream.start();
/* End Jetstream initialization */

/* socket.io server initialization */
startSocketServer(Number(PORT));
/* End socket.io server initialization */

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

function shutdown() {
  logger.info('Shutting down gracefully...');

  setTimeout(() => {
    logger.error('Forcing shutdown.');
    process.exit(1);
  }, 60000);

  clearInterval(cursorUpdateInterval);
  void io.close();
  jetstream.close();
  metricsServer.close();
  redis
    .quit()
    .catch((error: unknown) => {
      logger.error('Error disconnecting Redis client:', error);
    })
    .finally(() => {
      process.exit(0);
    });
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
