import { pino } from 'pino';

import { LOG_LEVEL } from './config.js';

const logger = pino({
  level: LOG_LEVEL,
  transport: process.stdout.isTTY
    ? { target: 'pino-pretty', options: { colorize: true } }
    : undefined,
});

export default logger;
