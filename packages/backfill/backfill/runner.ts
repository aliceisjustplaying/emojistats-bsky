import PQueue from "p-queue";
import { RepoReader } from "@atcute/car/v4";
import { ClientResponseError } from "@atcute/client";
import type { Pool } from "pg";
import { fetchAllDids } from "./util/fetch.js";
import { XRPCManager, RetryError } from "./util/xrpc.js";
import { normalizeRepoRecord } from "./postExtractor.js";
import type { BackfillConfig } from "./config.js";
import { EmojiPostWriter } from "./writer.js";
import { loadAllowlist } from "./allowlist.js";
import {
  countRepoEmojiPosts,
  isRepoComplete,
  markRepoComplete,
  recordRepoValidation,
} from "./db.js";
import type { RepoDescriptor } from "./types.js";
import {
  queuePendingGauge,
  queueSizeGauge,
  reposCompleted,
  terminalSkips,
  transientFailures as transientCounter,
  unknownFailures as unknownCounter,
  postsInserted as postsCounter,
  emojisProcessed as emojisCounter,
  repoProcessingDurationSeconds,
  rateLimiterWaitSeconds,
} from "./metrics.js";
import { RateLimiter } from "./util/rateLimiter.js";
import { logger } from "./logger.js";

const TERMINAL_ERROR_REASON = new Map<string, string>([
  ["RepoDeactivated", "repo deactivated"],
  ["RepoTakendown", "repo taken down"],
  ["RepoTakedown", "repo taken down"],
  ["RepoNotFound", "repo not found"],
  ["RepoSuspended", "repo suspended"],
  ["Tombstone", "repo tombstoned"],
  ["NotFound", "repo not found"],
]);

export class BackfillRunner {
  private readonly xrpc = new XRPCManager();
  private readonly stats = {
    scheduled: 0,
    completed: 0,
    skippedTerminal: 0,
    transientFailures: 0,
    unknownFailures: 0,
    postsInserted: 0,
    emojisProcessed: 0,
  };
  private statsTimer?: NodeJS.Timeout;
  private readonly limiterByPds = new Map<string, RateLimiter>();
  private lastStatsLogMs = 0;
  private readonly inFlight = new Set<string>();

  constructor(
    private readonly config: BackfillConfig,
    private readonly pool: Pool,
    private readonly writer: EmojiPostWriter,
  ) {}

  async run() {
    const allowlist = await loadAllowlist(this.config.allowlistPath);
    const queue = new PQueue({ concurrency: this.config.repoConcurrency });
    this.statsTimer = setInterval(() => this.printStats(queue), 10000);

    let scheduled = 0;

    for await (const [did, pds] of fetchAllDids()) {
      if (allowlist && !allowlist.has(did)) continue;
      if (this.inFlight.has(did)) continue;
      if (await isRepoComplete(this.pool, did)) continue;
      this.inFlight.add(did);

      const descriptor: RepoDescriptor = { did, pds };
      queue.add(async () => {
        try {
          await this.processRepo(descriptor, queue);
        } finally {
          this.inFlight.delete(descriptor.did);
        }
      });
      scheduled++;
      this.stats.scheduled++;

      if (this.config.didLimit && scheduled >= this.config.didLimit) {
        break;
      }
    }

    await queue.onIdle();
    if (this.statsTimer) {
      clearInterval(this.statsTimer);
      this.printStats(queue, true);
    }
  }

  private async processRepo(descriptor: RepoDescriptor, queue: PQueue) {
    let inserted = 0;
    const startTime = Date.now();
    const deadline = Date.now() + this.config.repoProcessingTimeoutMs;
    const existingCount = await countRepoEmojiPosts(this.pool, descriptor.did);
    try {
      const stream = await this.fetchRepoStreamWithBackoff(
        descriptor,
        deadline,
      );
      const repo = RepoReader.fromStream(stream);
      try {
        for await (const { record, collection, rkey, cid } of repo) {
          if (Date.now() > deadline) {
            throw new RepoTimeoutError(descriptor.did);
          }
          const cidString = typeof cid?.$link === "string" ? cid.$link : "";
          const normalized = normalizeRepoRecord({
            did: descriptor.did,
            collection,
            rkey,
            cid: cidString,
            record,
          });
          if (!normalized) continue;
          if (normalized.emojiGlyphs.length > this.config.emojiMaxPerPost) {
            continue;
          }
          await this.writer.enqueue(normalized);
          const emojiCount = normalized.emojiGlyphs.length;
          this.stats.emojisProcessed += emojiCount;
          if (emojiCount > 0) {
            emojisCounter.inc({ pds_host: descriptor.pds }, emojiCount);
          }
          inserted++;
        }
      } catch (err) {
        if (isCarStreamError(err)) {
          logger.warn(
            {
              did: descriptor.did,
              pds: descriptor.pds,
              err,
            },
            "CAR stream error",
          );
          this.stats.transientFailures++;
          transientCounter.inc({
            reason: "car_stream_error",
            pds_host: descriptor.pds,
          });
          this.printStats(queue);
          return;
        }
        throw err;
      }

      const snapshotPath = this.writer.getCurrentSnapshotPath();
      const validation = await this.validateRepo(
        descriptor,
        inserted,
        existingCount,
      );
      await recordRepoValidation(this.pool, {
        repoDid: descriptor.did,
        processedRows: validation.processedRows,
        insertedRows: validation.insertedRows,
        parquetRows: validation.parquetRows,
        existingRows: validation.existingRows,
        totalRows: validation.totalRows,
        snapshotPath,
        extrasDetected: validation.extrasDetected,
      });
      await markRepoComplete(
        this.pool,
        descriptor.did,
        validation.totalRows,
        snapshotPath,
        validation.parquetRows,
      );
      reposCompleted.inc({ pds_host: descriptor.pds });
      repoProcessingDurationSeconds.observe(
        { pds_host: descriptor.pds },
        (Date.now() - startTime) / 1000,
      );
      this.stats.completed++;
      this.stats.postsInserted += inserted;
      if (inserted > 0) {
        postsCounter.inc({ pds_host: descriptor.pds }, inserted);
      }
      if (process.env.EMOJI_BACKFILL_VERBOSE?.toLowerCase() === "true") {
        logger.info(
          { did: descriptor.did, pds: descriptor.pds, inserted },
          "Repo finished",
        );
      }
    } catch (error) {
      if (error instanceof RepoValidationError) {
        logger.error(
          {
            did: descriptor.did,
            pds: descriptor.pds,
            err: error,
          },
          "Repo validation failed",
        );
        this.stats.unknownFailures++;
        unknownCounter.inc({
          reason: "validation_failed",
          pds_host: descriptor.pds,
        });
        this.printStats(queue);
        return;
      }
      if (error instanceof RepoTimeoutError) {
        logger.warn(
          {
            did: descriptor.did,
            pds: descriptor.pds,
            timeoutMs: this.config.repoProcessingTimeoutMs,
          },
          "Repo processing timed out",
        );
        this.stats.transientFailures++;
        transientCounter.inc({
          reason: "repo_timeout",
          pds_host: descriptor.pds,
        });
        this.printStats(queue);
        return;
      }
      if (isConnectionError(error)) {
        logger.warn(
          { did: descriptor.did, pds: descriptor.pds, err: error },
          "Network error fetching repo",
        );
        this.stats.transientFailures++;
        transientCounter.inc({
          reason: "network_error",
          pds_host: descriptor.pds,
        });
        this.printStats(queue);
        return;
      }
      if (error instanceof ClientResponseError) {
        const classification = classifyClientError(error);
        if (classification.type === "terminal") {
          if (process.env.EMOJI_BACKFILL_VERBOSE?.toLowerCase() === "true") {
            logger.info(
              {
                did: descriptor.did,
                pds: descriptor.pds,
                reason: classification.reason,
              },
              "Skipping repo (terminal)",
            );
          }
          await markRepoComplete(this.pool, descriptor.did, 0, null, null);
          this.stats.skippedTerminal++;
          terminalSkips.inc({
            reason: classification.reason,
            pds_host: descriptor.pds,
          });
          this.printStats(queue);
          return;
        }
        if (classification.type === "transient") {
          logger.warn(
            {
              did: descriptor.did,
              pds: descriptor.pds,
              reason: classification.reason,
            },
            "Transient repo failure",
          );
          this.stats.transientFailures++;
          transientCounter.inc({
            reason: classification.reason,
            pds_host: descriptor.pds,
          });
          this.printStats(queue);
          return;
        }
      }
      this.stats.unknownFailures++;
      unknownCounter.inc({
        reason:
          error instanceof Error
            ? (error.name ?? "unknown_error")
            : "unknown_error",
        pds_host: descriptor.pds,
      });
      logger.error(
        { did: descriptor.did, pds: descriptor.pds, err: error },
        "Failed to process repo",
      );
      this.printStats(queue);
    } finally {
      this.writer.resetRepo(descriptor.did);
    }
  }

  private async validateRepo(
    descriptor: RepoDescriptor,
    processedRows: number,
    existingRows: number,
  ): Promise<ValidationStats> {
    const parquetCount = this.writer.consumeParquetCount(descriptor.did);
    if (parquetCount !== processedRows) {
      throw new RepoValidationError(
        descriptor.did,
        `Parquet mismatch: processed ${processedRows}, got ${parquetCount}`,
      );
    }
    await this.writer.flush();
    const insertedCount = this.writer.consumeInsertedCount(descriptor.did);
    const dbCount = await countRepoEmojiPosts(this.pool, descriptor.did);
    const expectedDbCount = existingRows + insertedCount;
    if (dbCount < expectedDbCount) {
      throw new RepoValidationError(
        descriptor.did,
        `Timescale shortfall: expected at least ${expectedDbCount}, counted ${dbCount}`,
      );
    }
    if (dbCount > expectedDbCount) {
      logger.info(
        {
          did: descriptor.did,
          pds: descriptor.pds,
          processed: processedRows,
          inserted: insertedCount,
          existing: existingRows,
          extras: dbCount - expectedDbCount,
        },
        "Repo gained additional rows during validation window",
      );
    }
    const extrasDetected = dbCount > expectedDbCount;
    if (insertedCount !== processedRows) {
      const duplicates = processedRows - insertedCount;
      logger.info(
        {
          did: descriptor.did,
          pds: descriptor.pds,
          duplicates,
          processed: processedRows,
          inserted: insertedCount,
          existing: existingRows,
        },
        "Repo contained duplicate records; stored unique rows",
      );
    }
    return {
      processedRows,
      insertedRows: insertedCount,
      parquetRows: parquetCount,
      existingRows,
      totalRows: dbCount,
      extrasDetected,
    };
  }

  private printStats(queue: PQueue, final = false) {
    const now = Date.now();
    if (!final && now - this.lastStatsLogMs < STATS_LOG_INTERVAL_MS) {
      return;
    }
    this.lastStatsLogMs = now;
    const label = final ? "progress (final)" : "progress";
    queueSizeGauge.set(queue.size);
    queuePendingGauge.set(queue.pending);
    logger.info(
      {
        label,
        queueSize: queue.size,
        queuePending: queue.pending,
        stats: this.stats,
      },
      "Backfill progress",
    );
  }

  private async fetchRepoStream(
    descriptor: RepoDescriptor,
    attempt = 0,
  ): Promise<any> {
    try {
      const limiter = this.getLimiter(descriptor.pds);
      await limiter.take();
      return await this.xrpc.query(
        descriptor.pds,
        async (client) =>
          await client.get("com.atproto.sync.getRepo", {
            params: { did: descriptor.did as any },
            as: "stream",
          }),
        attempt,
        { skipGlobalLimiter: true },
      );
    } catch (error) {
      if (error instanceof RetryError) {
        await error.wait();
        return this.fetchRepoStream(descriptor, attempt + 1);
      }
      throw error;
    }
  }

  private getLimiter(pds: string) {
    let limiter = this.limiterByPds.get(pds);
    if (!limiter) {
      limiter = new RateLimiter({
        capacity: 20,
        refillPerSec: 20,
        defaultContext: { scope: "pds", pds_host: pds },
        onWait: (waitMs, context) => {
          rateLimiterWaitSeconds.observe(
            {
              scope: String(context?.scope ?? "pds"),
              pds_host: String(context?.pds_host ?? pds),
            },
            waitMs / 1000,
          );
        },
      });
      this.limiterByPds.set(pds, limiter);
    }
    return limiter;
  }

  private async fetchRepoStreamWithBackoff(
    descriptor: RepoDescriptor,
    deadline: number,
  ) {
    let lastError: unknown;
    let attempts = 0;
    let hadConnectionError = false;
    for (const delaySeconds of NETWORK_BACKOFF_SECONDS) {
      if (Date.now() > deadline) {
        break;
      }
      if (delaySeconds > 0) {
        await new Promise((resolve) =>
          setTimeout(resolve, delaySeconds * 1000),
        );
        if (Date.now() > deadline) {
          break;
        }
      }
      attempts++;
      try {
        const stream = await this.fetchRepoStream(descriptor);
        if (hadConnectionError) {
          logger.info(
            {
              did: descriptor.did,
              pds: descriptor.pds,
              attempts,
            },
            "Connection error resolved",
          );
        }
        return stream;
      } catch (error) {
        if (!isConnectionError(error)) {
          throw error;
        }
        hadConnectionError = true;
        lastError = error;
        logger.warn(
          {
            did: descriptor.did,
            pds: descriptor.pds,
            attempt: attempts,
            delaySeconds,
            err: error,
          },
          "Connection error fetching repo; will retry",
        );
      }
    }
    throw lastError ?? new RepoTimeoutError(descriptor.did);
  }
}

class RepoTimeoutError extends Error {
  constructor(public readonly did: string) {
    super(`Repo ${did} processing timed out`);
  }
}

class RepoValidationError extends Error {
  constructor(
    public readonly did: string,
    message: string,
  ) {
    super(message);
  }
}

const NETWORK_BACKOFF_SECONDS = [0, 1, 2, 4, 8, 16, 32, 64, 128];
const STATS_LOG_INTERVAL_MS = 20_000;

type ValidationStats = {
  processedRows: number;
  insertedRows: number;
  parquetRows: number;
  existingRows: number;
  totalRows: number;
  extrasDetected: boolean;
};

type ClientErrorClassification =
  | { type: "terminal"; reason: string }
  | { type: "transient"; reason: string }
  | { type: "unknown"; reason: string };

function classifyClientError(
  error: ClientResponseError,
): ClientErrorClassification {
  const mapped = TERMINAL_ERROR_REASON.get(error.error ?? "");
  if (mapped) {
    return { type: "terminal", reason: mapped };
  }
  if (error.description && /has been deactivated/i.test(error.description)) {
    return { type: "terminal", reason: "repo deactivated" };
  }
  if (error.error === "UnknownXRPCError" && error.status === 404) {
    return { type: "terminal", reason: "repo not found" };
  }
  if (error.status && error.status >= 500) {
    return { type: "transient", reason: `server responded ${error.status}` };
  }
  if (error.status === 429) {
    return { type: "transient", reason: "rate limited" };
  }
  return { type: "unknown", reason: error.error ?? "unknown" };
}

const CONNECTION_ERROR_CODES = [
  "ECONNREFUSED",
  "ENOTFOUND",
  "ECONNRESET",
  "ETIMEDOUT",
  "CONNECTIONREFUSED",
  "FAILEDTOOPENSOCKET",
];

function isConnectionError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const code = (error as any).code ?? (error as any).errno;
  const message = (error as any).message ?? "";
  if (typeof code === "string") {
    const upperCode = code.toUpperCase();
    if (CONNECTION_ERROR_CODES.some((token) => upperCode.includes(token))) {
      return true;
    }
  }
  if (typeof message === "string") {
    const lowerMessage = message.toLowerCase();
    return (
      lowerMessage.includes("unable to connect") ||
      lowerMessage.includes("timed out")
    );
  }
  return false;
}

function isCarStreamError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const message = (error as any).message ?? "";
  if (typeof message !== "string") return false;
  return (
    /unexpected eof while decoding varint/i.test(message) ||
    /invalid varint/i.test(message) ||
    /invalid block/i.test(message)
  );
}
