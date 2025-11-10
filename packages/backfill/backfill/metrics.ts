import http from "node:http";
import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
} from "prom-client";
import { logger } from "./logger.js";

const registry = new Registry();
collectDefaultMetrics({ register: registry });

export const reposCompleted = new Counter({
  name: "emoji_backfill_repos_completed_total",
  help: "Number of repos fully processed",
  labelNames: ["pds_host"],
  registers: [registry],
});

export const terminalSkips = new Counter({
  name: "emoji_backfill_terminal_skips_total",
  help: "Repos skipped permanently (deactivated/taken down etc.)",
  labelNames: ["reason", "pds_host"],
  registers: [registry],
});

export const transientFailures = new Counter({
  name: "emoji_backfill_transient_failures_total",
  help: "Repos skipped temporarily due to transient errors",
  labelNames: ["reason", "pds_host"],
  registers: [registry],
});

export const unknownFailures = new Counter({
  name: "emoji_backfill_unknown_failures_total",
  help: "Repos skipped for unknown reasons",
  labelNames: ["reason", "pds_host"],
  registers: [registry],
});

export const postsInserted = new Counter({
  name: "emoji_backfill_posts_total",
  help: "Emoji-bearing posts inserted",
  labelNames: ["pds_host"],
  registers: [registry],
});

export const emojisProcessed = new Counter({
  name: "emoji_backfill_emojis_total",
  help: "Total emoji glyphs processed",
  labelNames: ["pds_host"],
  registers: [registry],
});

export const repoProcessingDurationSeconds = new Histogram({
  name: "emoji_backfill_repo_processing_duration_seconds",
  help: "Time to process individual repos",
  labelNames: ["pds_host"],
  buckets: [1, 5, 10, 30, 60, 120, 300, 600],
  registers: [registry],
});

export const rateLimiterWaitSeconds = new Histogram({
  name: "emoji_backfill_rate_limiter_wait_seconds",
  help: "Delay introduced by rate limiters before requests proceed",
  labelNames: ["scope", "pds_host"],
  buckets: [0.001, 0.01, 0.05, 0.1, 0.5, 1, 2, 5],
  registers: [registry],
});

export const queueSizeGauge = new Gauge({
  name: "emoji_backfill_queue_size",
  help: "Number of repo jobs waiting in queue",
  registers: [registry],
});

export const queuePendingGauge = new Gauge({
  name: "emoji_backfill_queue_pending",
  help: "Number of repo jobs currently running",
  registers: [registry],
});

export function startMetricsServer(port: number) {
  const server = http.createServer(async (req, res) => {
    if (!req.url) {
      res.statusCode = 404;
      return res.end();
    }
    if (req.url === "/metrics") {
      res.statusCode = 200;
      res.setHeader("Content-Type", registry.contentType);
      res.end(await registry.metrics());
      return;
    }
    res.statusCode = 404;
    res.end("Not Found");
  });

  server.listen(port, () => {
    logger.info({ port }, "Backfill metrics server listening");
  });

  return server;
}
