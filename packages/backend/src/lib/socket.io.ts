import { createServer } from 'http';
import { Server, Socket } from 'socket.io';

import { ORIGINS } from '../config.js';
import { emojis, getTopEmojisForLanguage } from './emojiStats.js';
import logger from './logger.js';

const httpServer = createServer();
export const io = new Server(httpServer, {
  cors: {
    origin: ORIGINS,
    methods: ['GET', 'POST'],
  },
});

io.on('connection', (socket: Socket) => {
  logger.info(`A user connected from ${socket.handshake.address}`);

  socket.on('getTopEmojisForLanguage', async (language: string) => {
    try {
      const topEmojis = await getTopEmojisForLanguage(language);
      socket.emit('topEmojisForLanguage', { language, topEmojis });
    } catch (error) {
      logger.error(`Error fetching top emojis for language ${language}: ${(error as Error).message}`);
      socket.emit('error', `Error fetching top emojis for language ${language}`);
    }
  });

  socket.on('getEmojiInfo', (emoji: string) => {
    logger.info(`Getting emoji info for ${emoji}`);
    const emojiInfo = emojis.find((e) => e.char === emoji);
    socket.emit('emojiInfo', emojiInfo);
  });

  socket.on('disconnect', () => {
    logger.info('A user disconnected');
  });
});

export const startSocketServer = (port: number) => {
  httpServer.listen(port, () => {
    logger.info(`Socket.io server listening on port ${port}`);
  });
};
