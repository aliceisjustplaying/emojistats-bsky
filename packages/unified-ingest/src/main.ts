import { config } from "./config.js";
import { createPool } from "./db.js";
import { DimensionCache } from "./dimensions.js";
import { ParquetSink } from "./parquetSink.js";
import { EmojiPostWriter } from "./writer.js";
import { logger } from "./logger.js";
import { normalizeUnifiedEvent } from "./normalizer.js";
import { NexusAdapter } from "./adapters/nexus.js";
import { JetstreamAdapter } from "./adapters/jetstream.js";
import type { UnifiedEvent } from "./adapters/types.js";
import {
  startMetricsServer,
  eventsReceived,
  eventsProcessed,
  eventsFailed,
  ackLagSeconds,
  repoCompletions,
  parquetRows,
  timescaleRows,
  validationErrors,
} from "./metrics.js";
import { createClient } from "redis";
import type { RedisClient } from "./adapters/jetstream.js";
import type { Pool } from "pg";
import {
  countRepoEmojiPosts,
  markRepoPending,
  markRepoComplete,
  recordRepoValidation,
  type RepoValidationRecord,
} from "./db.js";

async function main() {
  const pool = createPool({
    databaseUrl: config.databaseUrl,
    schema: config.databaseSchema,
  });

  const dimensions = new DimensionCache(pool);
  await dimensions.hydrate();

  const parquet = await ParquetSink.create(config.parquetDir);
  const writer = new EmojiPostWriter(pool, dimensions, parquet);

  // Track repo state for completion detection (Nexus backfill -> live transition)
  const repoState = new Map<
    string,
    { lastWasLive: boolean; processedCount: number; firstSeenAt: Date }
  >();

  const adapters: Array<{
    adapter: import("./adapters/types.js").EventAdapter;
    name: string;
  }> = [];

  // Initialize adapters based on config
  if (config.ingestSource === "nexus" || config.ingestSource === "both") {
    const nexusAdapter = new NexusAdapter({
      url: config.nexusUrl,
      ackTimeout: config.nexusAckTimeout,
    });
    adapters.push({ adapter: nexusAdapter, name: "nexus" });
  }

  if (config.ingestSource === "jetstream" || config.ingestSource === "both") {
    const redisClient: RedisClient = createClient({ url: config.redisUrl });
    redisClient.on("error", (err) => logger.error({ err }, "Redis error"));
    await redisClient.connect();

    const jetstreamAdapter = new JetstreamAdapter({
      endpoint: config.jetstreamEndpoint,
      cursorKey: config.jetstreamCursorKey,
      redisClient,
      cursorOverride: config.cursorOverride,
    });
    adapters.push({ adapter: jetstreamAdapter, name: "jetstream" });
  }

  if (adapters.length === 0) {
    throw new Error(
      "No adapters configured. Set INGEST_SOURCE to 'nexus', 'jetstream', or 'both'",
    );
  }

  // Set up event handler
  const handleEvent = async (event: UnifiedEvent) => {
    const ackStartTime = Date.now();
    let shouldAck = false;
    let hasEmoji = false;

    try {
      eventsReceived.inc({
        source: event.source,
        is_live: event.isLive ? "true" : "false",
      });

      // Track repo state for Nexus backfill completion detection
      if (event.source === "nexus") {
        const repoStateEntry = repoState.get(event.repoDid);
        if (!repoStateEntry) {
          // First time seeing this repo
          await markRepoPending(pool, event.repoDid);
          repoState.set(event.repoDid, {
            lastWasLive: event.isLive,
            processedCount: 0,
            firstSeenAt: new Date(),
          });
        } else {
          // Check for backfill completion: transition from live:false to live:true
          if (!repoStateEntry.lastWasLive && event.isLive) {
            await validateAndCompleteRepo(event.repoDid, writer, pool);
            repoCompletions.inc({ source: event.source });
          }
          repoStateEntry.lastWasLive = event.isLive;
        }
      }

      const normalized = normalizeUnifiedEvent(event);
      if (!normalized) {
        // Not a post with emoji - ack immediately in finally block
        shouldAck = true;
        return;
      }

      if (normalized.emojiGlyphs.length > config.emojiMaxPerPost) {
        // Too many emojis - ack immediately in finally block
        shouldAck = true;
        return;
      }

      // Post has emoji - process it
      hasEmoji = true;
      await writer.enqueue(normalized);
      eventsProcessed.inc({ source: event.source });

      // Track metrics
      const repoStateEntry = repoState.get(event.repoDid);
      if (repoStateEntry) {
        repoStateEntry.processedCount++;
      }
      parquetRows.inc();
      // Note: timescaleRows is incremented in writer.flush() via inserted count

      // CRITICAL: Wait for the batch containing this event to be flushed before acking
      // Multiple events can await the same flush promise, maintaining batching performance
      // while ensuring durability before ack
      await writer.waitForFlush();

      // Now safe to ack - data is durable
      // Use Promise.allSettled to ack all adapters concurrently, preventing slow Redis
      // writes from blocking Nexus acks when multiple sources are configured
      shouldAck = true;
      const ackResults = await Promise.allSettled(
        adapters.map(({ adapter }) => adapter.ack(event)),
      );

      // Log any failed acks for observability
      for (let i = 0; i < ackResults.length; i++) {
        const result = ackResults[i];
        if (result.status === "rejected") {
          logger.error(
            {
              err: result.reason,
              source: event.source,
              adapter: adapters[i].adapter.constructor.name,
            },
            "Ack failed for adapter",
          );
        }
      }

      // Record ack lag
      const ackDuration = (Date.now() - ackStartTime) / 1000;
      ackLagSeconds.observe({ source: event.source }, ackDuration);
    } catch (error) {
      logger.error(
        { err: error, source: event.source },
        "Failed to process event",
      );
      eventsFailed.inc({
        source: event.source,
        reason: error instanceof Error ? error.name : "unknown",
      });
      // Don't ack on error - let adapter retry
      shouldAck = false;
    } finally {
      // Always ack non-emoji events (even if they were filtered out)
      // This prevents Nexus from stalling and ensures Jetstream cursor advances
      if (shouldAck && !hasEmoji) {
        // Use concurrent acks for consistency with emoji event path
        const ackResults = await Promise.allSettled(
          adapters.map(({ adapter }) => adapter.ack(event)),
        );

        // Log any failed acks for observability
        for (let i = 0; i < ackResults.length; i++) {
          const result = ackResults[i];
          if (result.status === "rejected") {
            logger.error(
              {
                err: result.reason,
                source: event.source,
                adapter: adapters[i].adapter.constructor.name,
              },
              "Failed to ack filtered event",
            );
          }
        }

        const ackDuration = (Date.now() - ackStartTime) / 1000;
        ackLagSeconds.observe({ source: event.source }, ackDuration);
      }
    }
  };

  // Validate repo and mark as complete
  async function validateAndCompleteRepo(
    repoDid: string,
    writer: EmojiPostWriter,
    pool: Pool,
  ) {
    try {
      // Get existing count before flushing
      const existingCount = await countRepoEmojiPosts(pool, repoDid);

      // Ensure writer is flushed before validation
      await writer.flush();

      const parquetCount = writer.consumeParquetCount(repoDid);
      const insertedCount = writer.consumeInsertedCount(repoDid);
      const dbCount = await countRepoEmojiPosts(pool, repoDid);
      const snapshotPath = writer.getCurrentSnapshotPath();

      const processedCount = repoState.get(repoDid)?.processedCount ?? 0;
      const expectedDbCount = existingCount + insertedCount;
      const extrasDetected = dbCount > expectedDbCount;

      // Validation checks
      if (parquetCount !== processedCount) {
        logger.error(
          {
            repoDid,
            parquetCount,
            processedCount,
          },
          "Parquet count mismatch during validation",
        );
        validationErrors.inc({ repo_did: repoDid });
      }

      if (dbCount < expectedDbCount) {
        logger.error(
          {
            repoDid,
            expectedDbCount,
            dbCount,
          },
          "Timescale shortfall during validation",
        );
        validationErrors.inc({ repo_did: repoDid });
      }

      // Record validation
      const validationRecord: RepoValidationRecord = {
        repoDid,
        processedRows: processedCount,
        insertedRows: insertedCount,
        parquetRows: parquetCount,
        existingRows: existingCount,
        totalRows: dbCount,
        snapshotPath,
        extrasDetected,
      };

      await recordRepoValidation(pool, validationRecord);

      // Mark repo as complete
      await markRepoComplete(
        pool,
        repoDid,
        dbCount,
        snapshotPath,
        parquetCount,
      );

      logger.info(
        {
          repoDid,
          processedCount,
          insertedCount,
          parquetCount,
          dbCount,
          extrasDetected,
        },
        "Repo backfill completed and validated",
      );

      // Clean up repo state
      repoState.delete(repoDid);
      writer.resetRepo(repoDid);
    } catch (error) {
      logger.error(
        { err: error, repoDid },
        "Failed to validate and complete repo",
      );
      validationErrors.inc({ repo_did: repoDid });
    }
  }

  // Register event handlers
  for (const { adapter, name } of adapters) {
    adapter.onEvent(handleEvent);
    await adapter.start();
    logger.info({ adapter: name }, "Adapter started");
  }

  // Start metrics server
  const metricsServer =
    config.metricsPort > 0 ? startMetricsServer(config.metricsPort) : null;

  logger.info(
    {
      source: config.ingestSource,
      adapters: adapters.map((a) => a.name),
    },
    "Unified ingest worker started",
  );

  // Graceful shutdown
  const shutdown = async () => {
    logger.info("Shutting down unified ingest worker...");

    for (const { adapter, name } of adapters) {
      try {
        await adapter.stop();
        logger.info({ adapter: name }, "Adapter stopped");
      } catch (error) {
        logger.error({ err: error, adapter: name }, "Error stopping adapter");
      }
    }

    await writer.close();
    await pool.end();
    metricsServer?.close();

    logger.info("Shutdown complete");
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

await main().catch((error) => {
  logger.error({ err: error }, "Unified ingest worker failed");
  process.exit(1);
});
