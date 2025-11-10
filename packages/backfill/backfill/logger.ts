import pino from "pino";

export const logger = pino({
  name: "emoji-backfill",
  level: process.env.LOG_LEVEL ?? "info",
});
