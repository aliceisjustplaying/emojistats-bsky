import { monitorPgPool } from '@christiangalsterer/node-postgres-prometheus-exporter';
import express from 'express';
import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

import logger from './logger.js';
import { pool } from './postgres.js';

const register = new Registry();
collectDefaultMetrics({ register });

export const totalPostsProcessed = new Counter({
  name: 'bluesky_total_posts_processed_for_emojis',
  help: 'Total number of posts processed for emojis',
  registers: [register],
});

export const totalPostsWithEmojis = new Counter({
  name: 'bluesky_total_posts_with_emojis',
  help: 'Total number of posts with emojis',
  registers: [register],
});

export const totalPostsWithoutEmojis = new Counter({
  name: 'bluesky_total_posts_without_emojis',
  help: 'Total number of posts without emojis',
  registers: [register],
});

export const totalEmojis = new Counter({
  name: 'bluesky_total_emojis',
  help: 'Total number of emojis processed',
  registers: [register],
});

export const postsPerSecond = new Gauge({
  name: 'bluesky_processed_posts_per_second_for_emojis',
  help: 'Number of posts processed per second for emojis',
  registers: [register],
});

export const emojisPerSecond = new Gauge({
  name: 'bluesky_processed_emojis_per_second',
  help: 'Number of emojis processed per second',
  registers: [register],
});

export const postProcessingDuration = new Histogram({
  name: 'bluesky_post_processing_duration_seconds',
  help: 'Duration of post processing in seconds',
  buckets: [
    0.0001, 0.0002, 0.0003, 0.0004, 0.0005, 0.0006, 0.0007, 0.0008, 0.0009, 0.001, 0.002, 0.003, 0.004, 0.005, 0.01,
    0.02, 0.03, 0.04, 0.05, 0.1, 0.25, 0.5, 1,
  ],
  registers: [register],
});

export const concurrentHandleCreates = new Gauge({
  name: 'bluesky_concurrent_handle_create',
  help: 'Number of handleCreate functions running concurrently',
  registers: [register],
});

export const concurrentPostgresInserts = new Gauge({
  name: 'bluesky_concurrent_postgres_inserts',
  help: 'Number of concurrent Postgres inserts',
  registers: [register],
});

export const concurrentRedisInserts = new Gauge({
  name: 'bluesky_concurrent_redis_inserts',
  help: 'Number of concurrent Redis inserts',
  registers: [register],
});

monitorPgPool(pool, register);

let postsLastInterval = 0;
let emojisLastInterval = 0;

export function incrementTotalPosts(count = 1) {
  totalPostsProcessed.inc(count);
  postsLastInterval += count;
}

export function incrementTotalEmojis(count = 1) {
  totalEmojis.inc(count);
  emojisLastInterval += count;
}

setInterval(() => {
  postsPerSecond.set(postsLastInterval);
  postsLastInterval = 0;
}, 1000);

setInterval(() => {
  emojisPerSecond.set(emojisLastInterval);
  emojisLastInterval = 0;
}, 1000);

const app = express();

app.get('/metrics', (req, res) => {
  register
    .metrics()
    .then((metrics) => {
      res.set('Content-Type', register.contentType);
      res.send(metrics);
    })
    .catch((ex: unknown) => {
      logger.error(`Error serving metrics: ${(ex as Error).message}`);
      res.status(500).end((ex as Error).message);
    });
});

export const startMetricsServer = (port: number, host = '127.0.0.1') => {
  const server = app.listen(port, host, () => {
    logger.info(`Metrics server listening on port ${port}`);
  });

  server.on('close', () => {
    logger.info('Metrics server closed.');
  });

  return server;
};
