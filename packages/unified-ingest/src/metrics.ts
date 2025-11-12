import { Registry, Counter, Histogram } from "prom-client";
import { createServer } from "node:http";

const registry = new Registry();

export const eventsReceived = new Counter({
  name: "emoji_ingest_events_total",
  help: "Total events received from source",
  labelNames: ["source", "is_live"],
  registers: [registry],
});

export const eventsProcessed = new Counter({
  name: "emoji_ingest_events_processed_total",
  help: "Events successfully written to Timescale",
  labelNames: ["source"],
  registers: [registry],
});

export const eventsFailed = new Counter({
  name: "emoji_ingest_events_failed_total",
  help: "Failed events",
  labelNames: ["source", "reason"],
  registers: [registry],
});

export const ackLagSeconds = new Histogram({
  name: "emoji_ingest_ack_lag_seconds",
  help: "Time between receive and ack",
  labelNames: ["source"],
  buckets: [0.1, 0.5, 1, 2, 5, 10, 30],
  registers: [registry],
});

export const repoCompletions = new Counter({
  name: "emoji_ingest_repo_completions_total",
  help: "Repos completing backfill",
  labelNames: ["source"],
  registers: [registry],
});

export const parquetRows = new Counter({
  name: "emoji_ingest_parquet_rows_total",
  help: "Rows written to Parquet",
  registers: [registry],
});

export const timescaleRows = new Counter({
  name: "emoji_ingest_timescale_rows_total",
  help: "Rows inserted to Timescale",
  registers: [registry],
});

export const validationErrors = new Counter({
  name: "emoji_ingest_validation_errors_total",
  help: "Validation failures",
  labelNames: ["repo_did"],
  registers: [registry],
});

export function startMetricsServer(port: number) {
  if (port === 0) {
    return { close: () => {} };
  }

  const server = createServer(async (req, res) => {
    if (req.url === "/metrics") {
      res.setHeader("Content-Type", registry.contentType);
      res.end(await registry.metrics());
    } else if (req.url === "/health") {
      res.statusCode = 200;
      res.end("OK");
    } else {
      res.statusCode = 404;
      res.end("Not found");
    }
  });

  server.listen(port, () => {
    console.log(`Metrics server listening on port ${port}`);
  });

  return {
    close: () => {
      server.close();
    },
  };
}
