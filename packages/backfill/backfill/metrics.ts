import http from "node:http";
import { Counter, Gauge, Registry, collectDefaultMetrics } from "prom-client";

const registry = new Registry();
collectDefaultMetrics({ register: registry });

export const reposCompleted = new Counter({
  name: "emoji_backfill_repos_completed_total",
  help: "Number of repos fully processed",
  registers: [registry],
});

export const terminalSkips = new Counter({
  name: "emoji_backfill_terminal_skips_total",
  help: "Repos skipped permanently (deactivated/taken down etc.)",
  registers: [registry],
});

export const transientFailures = new Counter({
  name: "emoji_backfill_transient_failures_total",
  help: "Repos skipped temporarily due to transient errors",
  registers: [registry],
});

export const unknownFailures = new Counter({
  name: "emoji_backfill_unknown_failures_total",
  help: "Repos skipped for unknown reasons",
  registers: [registry],
});

export const postsInserted = new Counter({
  name: "emoji_backfill_posts_total",
  help: "Emoji-bearing posts inserted",
  registers: [registry],
});

export const emojisProcessed = new Counter({
  name: "emoji_backfill_emojis_total",
  help: "Total emoji glyphs processed",
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
    console.info(`Backfill metrics server listening on :${port}`);
  });

  return server;
}
