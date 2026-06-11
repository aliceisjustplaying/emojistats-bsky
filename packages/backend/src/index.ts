import { createServer } from 'http';

import { emojiToCodePoint } from 'emoji-normalization';
import { Server, Socket } from 'socket.io';

import { EMIT_INTERVAL, LOG_INTERVAL, ORIGINS, PORT } from './config.js';
import { clickHouseStatsProvider } from './lib/clickhouse.js';
import logger from './lib/logger.js';
import { StatsProvider } from './lib/stats.js';

/*
 * The stats socket server. Reads come from a StatsProvider — currently the
 * ClickHouse aggregate tables filled by packages/ingest. Swapping the
 * backing store means implementing StatsProvider in a new lib/ file and
 * wiring it here; the Socket.IO contract stays put.
 */
const stats: StatsProvider = clickHouseStatsProvider;

/* provider connectivity check before accepting clients */
await stats.ping();

/* socket.io server initialization */
const httpServer = createServer();
const io = new Server(httpServer, {
  cors: {
    origin: ORIGINS,
    methods: ['GET', 'POST'],
  },
});

io.on('connection', (socket: Socket) => {
  logger.info(`A user connected from ${socket.handshake.address}`);

  socket.on('getTopEmojisForLanguage', async (language: string) => {
    try {
      const topEmojis = await stats.getTopEmojisForLanguage(language);
      socket.emit('topEmojisForLanguage', { language, topEmojis });
    } catch (error) {
      logger.error(
        `Error fetching top emojis for language ${language}: ${(error as Error).message}`,
      );
      socket.emit(
        'error',
        `Error fetching top emojis for language ${language}`,
      );
    }
  });

  socket.on('getEmojiInfo', (emoji: string) => {
    // change to noop for now
    console.log(emojiToCodePoint(emoji));
  });

  socket.on('disconnect', (reason) => {
    logger.info(`A user disconnected. Reason: ${reason}`);
  });
});

io.on('close', () => {
  logger.info('Socket.io server closed.');
});

httpServer.listen(Number(PORT), () => {
  logger.info(`Socket.io server (ClickHouse-backed) listening on port ${PORT}`);
});
/* End socket.io server initialization */

/* emitting data for frontend */
const emitTimer = setInterval(() => {
  Promise.all([stats.getEmojiStats(), stats.getTopLanguages()])
    .then(([emojiStats, languages]) => {
      io.emit('emojiStats', emojiStats);
      io.emit('languageStats', languages);
      return;
    })
    .catch((error: unknown) => {
      logger.error(`Error emitting emoji stats: ${(error as Error).message}`);
    });
}, EMIT_INTERVAL);
/* End emitting data for frontend */

/* logging stats to the console */
const logTimer = setInterval(() => {
  stats
    .getEmojiStats()
    .then((emojiStats) => {
      logger.info(`Processed ${emojiStats.processedPosts} posts`);
      logger.info(`Processed ${emojiStats.processedEmojis} emojis`);
      logger.info(`Posts with: ${emojiStats.postsWithEmojis}`);
      logger.info(`Posts without: ${emojiStats.postsWithoutEmojis}`);
      logger.info(`Ratio: ${emojiStats.ratio}`);
      logger.info('Top emojis:');
      emojiStats.topEmojis.slice(0, 5).forEach(({ emoji, count }) => {
        logger.info(`${emoji}: ${count}`);
      });
      logger.info('---');
      return;
    })
    .catch((error: unknown) => {
      logger.error(`Error logging emoji stats: ${(error as Error).message}`);
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

  clearInterval(emitTimer);
  clearInterval(logTimer);

  try {
    await io.close();
  } catch (error) {
    logger.error(`Error closing Socket.io server: ${(error as Error).message}`);
  }

  try {
    await stats.close();
    logger.info('Stats provider closed.');
  } catch (error) {
    logger.error(`Error closing stats provider: ${(error as Error).message}`);
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
