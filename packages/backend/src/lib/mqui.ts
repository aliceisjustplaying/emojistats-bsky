import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js';
import { FastifyAdapter } from '@bull-board/fastify';
import fastify from 'fastify';

import { BULLMQ_UI_PORT } from '../config.js';
import { postQueue } from './queue.js';

const run = async () => {
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

  console.log(`For the UI, open http://localhost:${port}/`);
};

export { run };
