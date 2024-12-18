import dotenv from 'dotenv';

dotenv.config();

export const FIREHOSE_URL = process.env.FIREHOSE_URL ?? 'wss://jetstream.atproto.tools/subscribe';
export const MAX_EMOJIS = 3790; // Per Unicode 16.0
export const MAX_TOP_LANGUAGES = 30;
export const EMIT_INTERVAL = 1000;
export const LOG_INTERVAL = process.env.LOG_INTERVAL ? parseInt(process.env.LOG_INTERVAL, 10) : 10 * 1000;
export const CURSOR_UPDATE_INTERVAL =
  process.env.CURSOR_UPDATE_INTERVAL ? parseInt(process.env.CURSOR_UPDATE_INTERVAL, 10) : 10 * 1000;
export const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
export const PORT = process.env.PORT ?? '3100';
export const METRICS_PORT = process.env.METRICS_PORT ?? '3101';
export const BULLMQ_UI_PORT = process.env.BULLMQ_UI_PORT ?? '3102';
export const ORIGINS = process.env.ORIGINS?.split(',') ?? ['http://localhost:5173'];
export const BULLMQ_CONCURRENCY = 50;
