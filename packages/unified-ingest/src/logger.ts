import pino from "pino";

export const logger = pino({
  name: "unified-ingest",
  level: process.env.LOG_LEVEL ?? "info",
});
