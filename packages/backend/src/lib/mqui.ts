import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js';
import { FastifyAdapter } from '@bull-board/fastify';
import fastify from 'fastify';

import { BULLMQ_UI_PORT } from '../config.js';
import logger from './logger.js';
import { postQueue } from './queue.js';

const startBullMQUI = async () => {
  const app = fastify();
  const serverAdapter = new FastifyAdapter();

  createBullBoard({
    queues: [new BullMQAdapter(postQueue)],
    serverAdapter,
  });

  serverAdapter.setBasePath('/');
  app.register(serverAdapter.registerPlugin());

  const port = Number(BULLMQ_UI_PORT);
  await app.listen({ host: '0.0.0.0', port });

  logger.info(`BullMQ UI available at http://localhost:${port}/`);
  return app;
};

export { startBullMQUI };
