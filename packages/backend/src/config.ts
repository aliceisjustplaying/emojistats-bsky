import dotenv from 'dotenv';

dotenv.config();

export const FIREHOSE_URL = process.env.FIREHOSE_URL ?? 'wss://jetstream.atproto.tools/subscribe';
export const MAX_EMOJIS = 3790; // Per Unicode 16.0
export const EMIT_INTERVAL = 1000;
export const LOG_INTERVAL = 10 * 1000;
export const TRIM_LANGUAGE_CODES = false;
export const CURSOR_UPDATE_INTERVAL = 10 * 1000;
export const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
export const PORT = process.env.PORT ?? 3000;
