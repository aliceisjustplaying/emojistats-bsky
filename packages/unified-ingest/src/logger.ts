import pino from "pino";

const allowedLevels = new Set([
  "fatal",
  "error",
  "warn",
  "info",
  "debug",
  "trace",
  "silent",
]);

const envLevel = process.env.LOG_LEVEL?.toLowerCase();
const level = allowedLevels.has(envLevel ?? "")
  ? (envLevel as pino.LevelWithSilent)
  : ("info" as const);

export const logger = pino({
  name: "unified-ingest",
  level,
});
