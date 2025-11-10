import http from "node:http";
import { Counter, Gauge, Registry, collectDefaultMetrics } from "prom-client";

const registry = new Registry();
collectDefaultMetrics({ register: registry });

export const postsCounter = new Counter({
  name: "emoji_live_posts_total",
  help: "Posts processed by the live ingest service",
  registers: [registry],
});

export const emojisCounter = new Counter({
  name: "emoji_live_emojis_total",
  help: "Total emoji glyphs observed in live traffic",
  registers: [registry],
});

export const errorCounter = new Counter({
  name: "emoji_live_errors_total",
  help: "Errors encountered while processing live events",
  labelNames: ["type"],
  registers: [registry],
});

export const queueSizeGauge = new Gauge({
  name: "emoji_live_queue_size",
  help: "Number of events waiting to be processed",
  registers: [registry],
});

export const queuePendingGauge = new Gauge({
  name: "emoji_live_queue_pending",
  help: "Number of events currently in-flight",
  registers: [registry],
});

export function startMetricsServer(port: number) {
  const server = http.createServer(async (req, res) => {
    if (req.url !== "/metrics") {
      res.statusCode = 404;
      return res.end("Not Found");
    }
    res.statusCode = 200;
    res.setHeader("Content-Type", registry.contentType);
    res.end(await registry.metrics());
  });

  server.listen(port, () => {
    console.info(`Live ingest metrics listening on :${port}`);
  });

  return server;
}
