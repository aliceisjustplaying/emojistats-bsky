import pino from "pino";

export const logger = pino({
  name: "live-ingest",
  level: process.env.LOG_LEVEL ?? "info",
});
