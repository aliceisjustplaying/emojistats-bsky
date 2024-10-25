import { emojiToCodePoint } from 'emoji-normalization';
import { createServer } from 'http';
import { Server, Socket } from 'socket.io';

import { ORIGINS } from '../config.js';
import { getTopEmojisForLanguage } from './emojiStats.js';
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
    // change to noop for now
    console.log(emojiToCodePoint(emoji));
    // logger.info(`Getting emoji info for ${emoji}`);
    // const emojiInfo = emojis.find((e) => e.unified === emojiToCodePoint(emoji));
    // socket.emit('emojiInfo', emojiInfo);
  });

  socket.on('disconnect', (reason) => {
    logger.info(`A user disconnected. Reason: ${reason}`);
  });
});

io.on('close', () => {
  logger.info('Socket.io server closed.');
});

export const startSocketServer = (port: number) => {
  httpServer.listen(port, () => {
    logger.info(`Socket.io server listening on port ${port}`);
  });
};
