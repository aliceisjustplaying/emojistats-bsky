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
  validationErrors,
} from "./metrics.js";
import { createClient } from "redis";
import type { RedisClient } from "./adapters/jetstream.js";
import type { Pool } from "pg";
import {
  countRepoEmojiPosts,
  markRepoPending,
  markRepoComplete,
  markRepoCarComplete,
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

  type RepoRuntimeState = {
    lastWasLive: boolean;
    processedCount: number;
    filteredCount: number;
    firstSeenAt: Date;
    lastProgressLogMs: number;
  };

  // Track repo state for completion detection (Nexus backfill -> live transition)
  const repoState = new Map<string, RepoRuntimeState>();

  const ensureRepoState = (event: UnifiedEvent): RepoRuntimeState => {
    let state = repoState.get(event.repoDid);
    if (!state) {
      state = {
        lastWasLive: event.isLive,
        processedCount: 0,
        filteredCount: 0,
        firstSeenAt: new Date(),
        lastProgressLogMs: Date.now(),
      };
      repoState.set(event.repoDid, state);
    }
    return state;
  };

  const logRepoProgress = (
    repoDid: string,
    state: RepoRuntimeState,
    progressType: "filtered" | "emoji" | "periodic",
  ) => {
    const now = Date.now();
    const reachedCountThreshold =
      (progressType === "filtered" &&
        (state.filteredCount === 1 ||
          state.filteredCount % config.progressLogEvery === 0)) ||
      (progressType === "emoji" &&
        (state.processedCount === 1 ||
          state.processedCount % config.progressLogEvery === 0));

    const reachedTimeThreshold =
      now - state.lastProgressLogMs >= config.progressLogIntervalMs;

    if (!reachedCountThreshold && !reachedTimeThreshold) {
      return;
    }

    state.lastProgressLogMs = now;
    logger.info(
      {
        repoDid,
        progressType,
        emojiEvents: state.processedCount,
        filteredEvents: state.filteredCount,
        elapsedSeconds: Math.round((now - state.firstSeenAt.getTime()) / 1000),
      },
      "Repo ingest progress",
    );
  };

  const adapters: Array<{
    adapter: import("./adapters/types.js").EventAdapter;
    name: string;
  }> = [];
  const adapterBySource: Partial<
    Record<
      UnifiedEvent["source"],
      { adapter: import("./adapters/types.js").EventAdapter; name: string }
    >
  > = {};

  // Initialize adapters based on config
  if (config.ingestSource === "nexus" || config.ingestSource === "both") {
    const nexusAdapter = new NexusAdapter({
      url: config.nexusUrl,
      ackTimeout: config.nexusAckTimeout,
    });
    adapters.push({ adapter: nexusAdapter, name: "nexus" });
    adapterBySource.nexus = { adapter: nexusAdapter, name: "nexus" };
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
    adapterBySource.jetstream = {
      adapter: jetstreamAdapter,
      name: "jetstream",
    };
  }

  if (adapters.length === 0) {
    throw new Error(
      "No adapters configured. Set INGEST_SOURCE to 'nexus', 'jetstream', or 'both'",
    );
  }

  const ackEvent = async (
    event: UnifiedEvent,
    ackStartTime: number,
    context: "filtered" | "processed",
  ) => {
    const target = adapterBySource[event.source];
    if (target) {
      try {
        await target.adapter.ack(event);
      } catch (error) {
        logger.error(
          {
            err: error,
            adapter: target.name,
            context,
            source: event.source,
            nexusEventId: event.nexusEventId,
          },
          "Ack failed",
        );
        throw new Error(`Ack failed during ${context} ack`);
      }
    }

    const ackDuration = (Date.now() - ackStartTime) / 1000;
    ackLagSeconds.observe({ source: event.source }, ackDuration);
  };

  // Set up event handler
  const handleEvent = async (event: UnifiedEvent) => {
    const ackStartTime = Date.now();
    const hasExistingState = repoState.has(event.repoDid);
    const repoStateEntry = ensureRepoState(event);

    try {
      eventsReceived.inc({
        source: event.source,
        is_live: event.isLive ? "true" : "false",
      });

      // Track repo state for Nexus backfill completion detection
      if (event.source === "nexus") {
        if (!hasExistingState) {
          // First time seeing this repo
          await markRepoPending(pool, event.repoDid);
        } else if (!repoStateEntry.lastWasLive && event.isLive) {
          // Backfill finished, live stream has started
          await markRepoCarComplete(pool, event.repoDid);
          await validateAndCompleteRepo(event.repoDid, writer, pool);
          repoCompletions.inc({ source: event.source });
        }
        repoStateEntry.lastWasLive = event.isLive;
      }

      const normalized = normalizeUnifiedEvent(event);
      if (
        !normalized ||
        normalized.emojiGlyphs.length > config.emojiMaxPerPost
      ) {
        logger.debug(
          {
            repoDid: event.repoDid,
            rkey: event.rkey,
            source: event.source,
            nexusEventId: event.nexusEventId,
          },
          "Event filtered out by normalizer (no emoji, invalid timestamp, or emoji limit exceeded)",
        );
        repoStateEntry.filteredCount++;
        logRepoProgress(event.repoDid, repoStateEntry, "filtered");
        await ackEvent(event, ackStartTime, "filtered");
        return;
      }

      // Post has emoji - process it
      await writer.enqueue(normalized);
      eventsProcessed.inc({ source: event.source });

      // Track metrics
      repoStateEntry.processedCount++;
      logRepoProgress(event.repoDid, repoStateEntry, "emoji");
      parquetRows.inc();
      // Note: timescaleRows is incremented in writer.flush() via inserted count

      // CRITICAL: Wait for the batch containing this event to be flushed before acking
      // Multiple events can await the same flush promise, maintaining batching performance
      // while ensuring durability before ack
      await writer.waitForFlush();

      // Now safe to ack - data is durable
      await ackEvent(event, ackStartTime, "processed");
    } catch (error) {
      logger.error(
        { err: error, source: event.source },
        "Failed to process event",
      );
      eventsFailed.inc({
        source: event.source,
        reason: error instanceof Error ? error.name : "unknown",
      });
      // Ack failures propagate through ackEvent, so no ack in catch/finally
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

  // Periodic progress logs to show repo states even when counts aren't moving
  setInterval(() => {
    for (const [repoDid, state] of repoState.entries()) {
      logRepoProgress(repoDid, state, "periodic");
    }
  }, config.progressLogIntervalMs).unref();

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
