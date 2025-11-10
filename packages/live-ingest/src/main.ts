import "dotenv/config";
import PQueue from "p-queue";
import { CommitCreateEvent } from "@skyware/jetstream";
import { config } from "./config.js";
import { logger } from "./logger.js";
import { createPool, insertEmojiRows } from "./db.js";
import { DimensionCache } from "./dimensions.js";
import { createRedisClient, RedisEmojiStore } from "./redisStore.js";
import { normalizeEvent } from "./normalizer.js";
import {
  createJetstream,
  attachCursorPersistence,
  loadCursor,
} from "./jetstream.js";
import {
  emojisCounter,
  errorCounter,
  postsCounter,
  queuePendingGauge,
  queueSizeGauge,
  startMetricsServer,
} from "./metrics.js";
import type { NormalizedPost } from "./types.js";
import { PreparedEmojiRow } from "./db.js";

async function main() {
  const pool = createPool(config.databaseUrl, config.databaseSchema);
  const dimensions = new DimensionCache(pool);
  await dimensions.hydrate();

  const redisClient = await createRedisClient(config.redisUrl);
  const redisStore = new RedisEmojiStore(redisClient, config.redisKeyPrefix);

  const cursor = await loadCursor(redisClient);
  const jetstream = createJetstream(cursor);
  attachCursorPersistence(jetstream, redisClient);

  const queue = new PQueue({ concurrency: config.concurrency });
  const metricsServer = startMetricsServer(config.metricsPort);

  const processEvent = async (
    event: CommitCreateEvent<"app.bsky.feed.post">,
  ) => {
    try {
      const normalized = normalizeEvent(event);
      if (!normalized) return;
      if (normalized.emojiGlyphs.length > config.emojiMaxPerPost) {
        return;
      }
      await handleNormalized(normalized, pool, dimensions, redisStore);
    } catch (err) {
      errorCounter.inc({ type: err instanceof Error ? err.name : "unknown" });
      logger.error({ err }, "Failed to process live event");
    }
  };

  jetstream.onCreate("app.bsky.feed.post", (event) => {
    queue
      .add(() => processEvent(event))
      .catch((err: unknown) => logger.error({ err }, "Queue task failed"))
      .finally(() => updateQueueMetrics(queue));
    updateQueueMetrics(queue);
  });

  jetstream.start();
  logger.info("Live ingest started");

  const handleSignals = async () => {
    logger.info("Shutting down live ingest...");
    metricsServer.close();
    await queue.onIdle();
    jetstream.close();
    await redisClient.disconnect();
    await pool.end();
    process.exit(0);
  };

  process.on("SIGINT", handleSignals);
  process.on("SIGTERM", handleSignals);
}

async function handleNormalized(
  post: NormalizedPost,
  pool: ReturnType<typeof createPool>,
  dimensions: DimensionCache,
  redisStore: RedisEmojiStore,
) {
  const langId = await dimensions.getLanguageId(post.primaryLang);
  const clientId = await dimensions.getClientId(post.clientIdentifier);
  const emojiIds = await Promise.all(
    post.emojiGlyphs.map(async (glyph) => await dimensions.getEmojiId(glyph)),
  );

  const row: PreparedEmojiRow = {
    postUri: post.postUri,
    repoDid: post.repoDid,
    rkey: post.rkey,
    seq: post.seq,
    createdAt: post.createdAt,
    receivedAt: post.receivedAt,
    langId,
    clientId,
    emojiIds,
    authorDid: post.authorDid,
    replyRootUri: post.replyRootUri,
    replyParentUri: post.replyParentUri,
  };

  await insertEmojiRows(pool, [row]);
  await redisStore.increment(post);
  postsCounter.inc();
  if (post.emojiGlyphs.length > 0) {
    emojisCounter.inc(post.emojiGlyphs.length);
  }
}

function updateQueueMetrics(queue: PQueue) {
  queueSizeGauge.set(queue.size);
  queuePendingGauge.set(queue.pending);
}

await main().catch((err) => {
  logger.error({ err }, "Live ingest crashed");
  process.exit(1);
});
