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
import { isRepoComplete, markRepoComplete } from "./db.js";
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
} from "./metrics.js";

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
      if (await isRepoComplete(this.pool, did)) continue;

      const descriptor: RepoDescriptor = { did, pds };
      queue.add(() => this.processRepo(descriptor, queue));
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
    try {
      const stream = await this.fetchRepoStream(descriptor);
      const repo = RepoReader.fromStream(stream);
      try {
        for await (const { record, collection, rkey, cid } of repo) {
          const cidString = typeof cid?.$link === "string" ? cid.$link : "";
          const normalized = normalizeRepoRecord({
            did: descriptor.did,
            collection,
            rkey,
            cid: cidString,
            record,
          });
          if (!normalized) continue;
          await this.writer.enqueue(normalized);
          const emojiCount = normalized.emojiGlyphs.length;
          this.stats.emojisProcessed += emojiCount;
          if (emojiCount > 0) {
            emojisCounter.inc(emojiCount);
          }
          inserted++;
        }
      } catch (err) {
        if (isCarStreamError(err)) {
          console.warn(
            `CAR stream error for ${descriptor.did}: ${(err as Error).message}`,
          );
          this.stats.transientFailures++;
          this.printStats(queue);
          return;
        }
        throw err;
      }

      await markRepoComplete(this.pool, descriptor.did);
      reposCompleted.inc();
      this.stats.completed++;
      this.stats.postsInserted += inserted;
      if (inserted > 0) {
        postsCounter.inc(inserted);
      }
      if (process.env.EMOJI_BACKFILL_VERBOSE?.toLowerCase() === "true") {
        console.info(
          `Finished ${descriptor.did}: inserted ${inserted} emoji posts`,
        );
      }
    } catch (error) {
      if (isConnectionError(error)) {
        console.warn(
          `Network error fetching ${descriptor.did}: ${(error as Error).message ?? error}`,
        );
        this.stats.transientFailures++;
        transientCounter.inc();
        this.printStats(queue);
        return;
      }
      if (error instanceof ClientResponseError) {
        const classification = classifyClientError(error);
        if (classification.type === "terminal") {
          console.warn(
            `Skipping ${descriptor.did}: ${classification.reason} (${error.message ?? error.error})`,
          );
          await markRepoComplete(this.pool, descriptor.did);
          this.stats.skippedTerminal++;
          terminalSkips.inc();
          this.printStats(queue);
          return;
        }
        if (classification.type === "transient") {
          console.warn(
            `Transient failure for ${descriptor.did}: ${classification.reason}. Will retry on a later run.`,
          );
          this.stats.transientFailures++;
          transientCounter.inc();
          this.printStats(queue);
          return;
        }
      }
      this.stats.unknownFailures++;
      unknownCounter.inc();
      console.error(`Failed to process ${descriptor.did}:`, error);
      this.printStats(queue);
    }
  }

  private printStats(queue: PQueue, final = false) {
    const label = final ? "progress (final)" : "progress";
    queueSizeGauge.set(queue.size);
    queuePendingGauge.set(queue.pending);
    // ensure counters are registered even if zero
    postsCounter.inc(0);
    emojisCounter.inc(0);
    console.info(
      `[${label}] scheduled=${this.stats.scheduled} completed=${this.stats.completed} ` +
        `terminal_skips=${this.stats.skippedTerminal} transient=${this.stats.transientFailures} ` +
        `unknown=${this.stats.unknownFailures} posts=${this.stats.postsInserted} ` +
        `emojis=${this.stats.emojisProcessed} queue=${queue.size} pending=${queue.pending}`,
    );
  }

  private async fetchRepoStream(
    descriptor: RepoDescriptor,
    attempt = 0,
  ): Promise<any> {
    try {
      return await this.xrpc.query(
        descriptor.pds,
        async (client) =>
          await client.get("com.atproto.sync.getRepo", {
            params: { did: descriptor.did as any },
            as: "stream",
          }),
      );
    } catch (error) {
      if (error instanceof RetryError) {
        await error.wait();
        return this.fetchRepoStream(descriptor, attempt + 1);
      }
      throw error;
    }
  }
}

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

function isConnectionError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const code = (error as any).code ?? (error as any).errno;
  const message = (error as any).message ?? "";
  if (typeof code === "string") {
    return [
      "ECONNREFUSED",
      "ENOTFOUND",
      "ECONNRESET",
      "ETIMEDOUT",
      "ConnectionRefused",
    ].some((token) => code.toUpperCase().includes(token));
  }
  if (typeof message === "string") {
    return /unable to connect/i.test(message) || /timed out/i.test(message);
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
