import { CommitCreateEvent, Jetstream } from "@skyware/jetstream";
import { logger } from "./logger.js";
import { config } from "./config.js";
import type { RedisClient } from "./redisStore.js";

export function createJetstream(cursor?: number) {
  return new Jetstream({
    endpoint: config.jetstreamEndpoint,
    cursor,
    wantedCollections: ["app.bsky.feed.post"],
  });
}

export async function loadCursor(
  redis: RedisClient,
): Promise<number | undefined> {
  if (config.cursorOverridePath) {
    try {
      const override = await Bun.file(config.cursorOverridePath).text();
      const parsed = Number(override.trim());
      if (!Number.isNaN(parsed)) {
        logger.info({ cursor: parsed }, "Using cursor override");
        return parsed;
      }
    } catch (err) {
      logger.warn({ err }, "Unable to read cursor override");
    }
  }
  const stored = await redis.get(config.jetstreamCursorKey);
  if (!stored) return undefined;
  const parsed = Number(stored);
  return Number.isNaN(parsed) ? undefined : parsed;
}

export async function persistCursor(redis: RedisClient, cursor?: number) {
  if (!cursor) return;
  await redis.set(config.jetstreamCursorKey, cursor.toString());
}

export function attachCursorPersistence(
  jetstream: Jetstream,
  redis: RedisClient,
) {
  jetstream.on("open", () => logger.info("Connected to Jetstream"));
  jetstream.on("error", (err) => logger.error({ err }, "Jetstream error"));
  jetstream.on("close", () => logger.warn("Jetstream connection closed"));
  setInterval(() => {
    if (jetstream.cursor) {
      persistCursor(redis, jetstream.cursor).catch((err) => {
        logger.error({ err }, "Failed to persist cursor");
      });
    }
  }, 10000).unref();
}
