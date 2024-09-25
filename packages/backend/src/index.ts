import { EMIT_INTERVAL, LOG_INTERVAL, PORT } from './config.js';
import { cursorUpdateInterval, getLastCursor } from './lib/cursor.js';
import { getEmojiStats, getLanguageStats, logEmojiStats } from './lib/emojiStats.js';
import { initializeJetstream, jetstream } from './lib/jetstream.js';
import { loadRedisScripts, redisClient } from './lib/redis.js';
import { io, startSocketServer } from './lib/socket.io.js';
import logger from './logger.js';

/* redis initialization */
await redisClient.connect();
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
  Promise.all([getEmojiStats(), getLanguageStats()])
    .then(([stats, languages]) => {
      io.emit('emojiStats', stats);
      io.emit('languageStats', languages);
    })
    .catch((error: unknown) => {
      logger.error(`Error emitting or logging emoji stats: ${(error as Error).message}`);
    });
}, EMIT_INTERVAL);
/* End emitting data for frontend */

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

  redisClient
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
