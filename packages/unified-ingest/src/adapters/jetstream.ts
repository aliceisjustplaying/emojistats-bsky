import { Jetstream } from "@skyware/jetstream";
import type { CommitCreateEvent } from "@skyware/jetstream";
import type { UnifiedEvent, EventAdapter } from "./types.js";
import { logger } from "../logger.js";
import { parse as parseTid } from "@atcute/tid";
import { createClient } from "redis";

export type RedisClient = ReturnType<typeof createClient>;

export interface JetstreamAdapterConfig {
  endpoint: string;
  cursorKey: string;
  redisClient: RedisClient;
  cursorOverride?: number;
}

export class JetstreamAdapter implements EventAdapter {
  private jetstream: Jetstream | null = null;
  private eventCallback?: (event: UnifiedEvent) => Promise<void>;
  private cursorSaveInterval?: NodeJS.Timeout;
  private isStopped = false;

  constructor(private readonly config: JetstreamAdapterConfig) {}

  async start(): Promise<void> {
    if (this.isStopped) {
      throw new Error("Adapter has been stopped");
    }

    const cursor = await this.loadCursor();
    this.jetstream = new Jetstream({
      endpoint: this.config.endpoint,
      cursor: cursor,
      wantedCollections: ["app.bsky.feed.post"],
    });

    this.jetstream.on("open", () => {
      logger.info("Connected to Jetstream");
    });

    this.jetstream.on("error", (err) => {
      logger.error({ err }, "Jetstream error");
    });

    this.jetstream.on("close", () => {
      logger.warn("Jetstream connection closed");
      if (!this.isStopped) {
        // Jetstream will attempt to reconnect automatically
        logger.info("Waiting for Jetstream reconnection...");
      }
    });

    this.jetstream.onCreate("app.bsky.feed.post", (event) => {
      // Wrap in promise handler to catch errors and prevent unhandled rejections
      this.handleEvent(event).catch((error) => {
        logger.error(
          { err: error },
          "Unhandled error in Jetstream event handler",
        );
        // Don't rethrow - we've logged it, let Jetstream continue
      });
    });

    // Save cursor periodically
    this.cursorSaveInterval = setInterval(() => {
      if (this.jetstream?.cursor) {
        this.persistCursor(this.jetstream.cursor).catch((err) => {
          logger.error({ err }, "Failed to persist cursor");
        });
      }
    }, 10000);

    this.jetstream.start();
  }

  async stop(): Promise<void> {
    this.isStopped = true;

    if (this.cursorSaveInterval) {
      clearInterval(this.cursorSaveInterval);
      this.cursorSaveInterval = undefined;
    }

    if (this.jetstream?.cursor) {
      await this.persistCursor(this.jetstream.cursor);
    }

    if (this.jetstream) {
      this.jetstream.close();
      this.jetstream = null;
    }
  }

  onEvent(callback: (event: UnifiedEvent) => Promise<void>): void {
    this.eventCallback = callback;
  }

  async ack(event: UnifiedEvent): Promise<void> {
    if (event.source !== "jetstream") {
      return;
    }

    // For Jetstream, we persist the cursor after successful write
    // This is handled in the main worker after write completion
    if (event.jetstreamCursor && this.jetstream) {
      await this.persistCursor(event.jetstreamCursor);
    }
  }

  private async handleEvent(
    event: CommitCreateEvent<"app.bsky.feed.post">,
  ): Promise<void> {
    const unifiedEvent = this.mapJetstreamEvent(event);
    if (!unifiedEvent) {
      return;
    }

    if (this.eventCallback) {
      await this.eventCallback(unifiedEvent);
    }
  }

  private mapJetstreamEvent(
    event: CommitCreateEvent<"app.bsky.feed.post">,
  ): UnifiedEvent | null {
    const { did } = event;
    const record = event.commit.record as any;
    if (!record || typeof record !== "object") {
      return null;
    }

    const rkey = event.commit.rkey;
    const { createdAt, seq } = this.resolveTimestamps(record, rkey, did);

    return {
      repoDid: did,
      collection: "app.bsky.feed.post",
      rkey,
      record,
      seq,
      createdAt,
      receivedAt: new Date(),
      source: "jetstream",
      isLive: true, // Jetstream events are always live
      jetstreamCursor: this.jetstream?.cursor,
    };
  }

  private resolveTimestamps(
    record: any,
    rkey: string,
    did: string,
  ): { createdAt: Date; seq: number } {
    // Valid date range: 2000-01-01 to 2100-01-01
    const MIN_VALID_DATE = new Date("2000-01-01T00:00:00Z").getTime();
    const MAX_VALID_DATE = new Date("2100-01-01T00:00:00Z").getTime();

    const isValidDate = (date: Date): boolean => {
      const time = date.getTime();
      return (
        !Number.isNaN(time) &&
        time >= MIN_VALID_DATE &&
        time <= MAX_VALID_DATE &&
        date.toISOString().match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/) !==
          null
      );
    };

    // Try createdAt field first
    const createdAtField =
      typeof record?.createdAt === "string" ? new Date(record.createdAt) : null;
    if (createdAtField && isValidDate(createdAtField)) {
      return {
        createdAt: createdAtField,
        seq: createdAtField.getTime() * 1000,
      };
    }

    // Fall back to TID parsing
    try {
      const tid = parseTid(rkey);
      const tidDate = new Date(tid.timestamp);
      if (isValidDate(tidDate)) {
        return { createdAt: tidDate, seq: tid.timestamp };
      }
    } catch {
      // TID parsing failed
    }

    // Last resort: use current time (but log the issue)
    logger.warn(
      { did, rkey, createdAt: record?.createdAt },
      "Invalid timestamp, using current time as fallback",
    );
    const now = new Date();
    return { createdAt: now, seq: now.getTime() * 1000 };
  }

  private async loadCursor(): Promise<number | undefined> {
    if (this.config.cursorOverride !== undefined) {
      logger.info(
        { cursor: this.config.cursorOverride },
        "Using cursor override",
      );
      return this.config.cursorOverride;
    }

    try {
      const stored = await this.config.redisClient.get(this.config.cursorKey);
      if (!stored) return undefined;
      const parsed = Number(stored);
      return Number.isNaN(parsed) ? undefined : parsed;
    } catch (error) {
      logger.warn({ err: error }, "Failed to load cursor from Redis");
      return undefined;
    }
  }

  private async persistCursor(cursor: number): Promise<void> {
    try {
      await this.config.redisClient.set(
        this.config.cursorKey,
        cursor.toString(),
      );
    } catch (error) {
      logger.error({ err: error }, "Failed to persist cursor to Redis");
      throw error;
    }
  }
}
