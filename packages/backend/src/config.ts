import dotenv from 'dotenv';

dotenv.config();

export const MAX_EMOJIS = 3790; // Per Unicode 16.0
export const MAX_TOP_LANGUAGES = 30;
export const EMIT_INTERVAL = 1000;
export const LOG_INTERVAL = process.env.LOG_INTERVAL
  ? parseInt(process.env.LOG_INTERVAL, 10)
  : 10 * 1000;
export const PORT = process.env.PORT ?? '3100';
export const ORIGINS = process.env.ORIGINS?.split(',') ?? [
  'http://localhost:5173',
];
export const CLICKHOUSE_URL =
  process.env.CLICKHOUSE_URL ?? 'http://localhost:8123';
export const CLICKHOUSE_DATABASE =
  process.env.CLICKHOUSE_DATABASE ?? 'emojistats';
export const CLICKHOUSE_USER = process.env.CLICKHOUSE_USER ?? 'emojistats';
export const CLICKHOUSE_PASSWORD =
  process.env.CLICKHOUSE_PASSWORD ?? 'emojistats';
