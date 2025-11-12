import WebSocket from "ws";
import type {
  UnifiedEvent,
  NexusOutboxEvent,
  NexusRecordEvent,
  EventAdapter,
} from "./types.js";
import { logger } from "../logger.js";
import { parse as parseTid } from "@atcute/tid";

export interface NexusAdapterConfig {
  url: string;
  ackTimeout?: number; // milliseconds, default 10000
  reconnectBackoff?: number[]; // backoff delays in ms
}

const DEFAULT_ACK_TIMEOUT = 10000;
const DEFAULT_RECONNECT_BACKOFF = [1000, 2000, 4000, 8000, 16000, 30000];

export class NexusAdapter implements EventAdapter {
  private ws: WebSocket | null = null;
  private eventCallback?: (event: UnifiedEvent) => Promise<void>;
  private reconnectAttempt = 0;
  private reconnectTimer?: NodeJS.Timeout;
  private inFlightAcks = new Map<number, NodeJS.Timeout>();
  private isStopped = false;

  constructor(private readonly config: NexusAdapterConfig) {}

  async start(): Promise<void> {
    if (this.isStopped) {
      throw new Error("Adapter has been stopped");
    }
    await this.connect();
  }

  async stop(): Promise<void> {
    this.isStopped = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    for (const timeout of this.inFlightAcks.values()) {
      clearTimeout(timeout);
    }
    this.inFlightAcks.clear();
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  onEvent(callback: (event: UnifiedEvent) => Promise<void>): void {
    this.eventCallback = callback;
  }

  async ack(event: UnifiedEvent): Promise<void> {
    if (event.source !== "nexus" || !event.nexusEventId) {
      return;
    }

    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      logger.warn(
        { eventId: event.nexusEventId },
        "Cannot ack: WebSocket not open",
      );
      return;
    }

    // Clear any pending timeout for this event
    const timeout = this.inFlightAcks.get(event.nexusEventId);
    if (timeout) {
      clearTimeout(timeout);
      this.inFlightAcks.delete(event.nexusEventId);
    }

    try {
      const ackMessage = JSON.stringify({ id: event.nexusEventId });
      this.ws.send(ackMessage);
    } catch (error) {
      logger.error(
        { err: error, eventId: event.nexusEventId },
        "Failed to send ack",
      );
    }
  }

  private async connect(): Promise<void> {
    if (this.isStopped) {
      return;
    }

    return new Promise((resolve, reject) => {
      try {
        logger.info({ url: this.config.url }, "Connecting to Nexus WebSocket");
        const ws = new WebSocket(this.config.url);

        ws.on("open", () => {
          logger.info("Connected to Nexus WebSocket");
          this.reconnectAttempt = 0;
          this.ws = ws;
          resolve();
        });

        ws.on("message", (data: Buffer) => {
          this.handleMessage(data);
        });

        ws.on("error", (error) => {
          logger.error({ err: error }, "Nexus WebSocket error");
          if (this.ws === ws) {
            this.ws = null;
          }
          if (!this.isStopped) {
            this.scheduleReconnect();
          }
          reject(error);
        });

        ws.on("close", () => {
          logger.warn("Nexus WebSocket closed");
          if (this.ws === ws) {
            this.ws = null;
          }
          if (!this.isStopped) {
            this.scheduleReconnect();
          }
        });
      } catch (error) {
        logger.error({ err: error }, "Failed to create WebSocket connection");
        if (!this.isStopped) {
          this.scheduleReconnect();
        }
        reject(error);
      }
    });
  }

  private async handleMessage(data: Buffer): Promise<void> {
    try {
      const event: NexusOutboxEvent = JSON.parse(data.toString());

      if (event.type !== "record" || !event.record) {
        // Ignore user events and non-record events
        return;
      }

      const unifiedEvent = this.mapNexusEvent(event);
      if (!unifiedEvent) {
        return;
      }

      // Set up ack timeout
      if (unifiedEvent.nexusEventId) {
        const timeout = setTimeout(() => {
          logger.warn(
            { eventId: unifiedEvent.nexusEventId },
            "Ack timeout for Nexus event",
          );
          this.inFlightAcks.delete(unifiedEvent.nexusEventId!);
        }, this.config.ackTimeout ?? DEFAULT_ACK_TIMEOUT);
        this.inFlightAcks.set(unifiedEvent.nexusEventId, timeout);
      }

      if (this.eventCallback) {
        await this.eventCallback(unifiedEvent);
      }
    } catch (error) {
      logger.error({ err: error }, "Failed to handle Nexus message");
    }
  }

  private mapNexusEvent(event: NexusOutboxEvent): UnifiedEvent | null {
    if (!event.record) {
      return null;
    }

    const record = event.record;
    if (record.collection !== "app.bsky.feed.post") {
      return null;
    }

    // Extract createdAt and seq from record or rkey
    const { createdAt, seq } = this.resolveTimestamps(
      record.record as any,
      record.rkey,
      record.did,
    );

    return {
      repoDid: record.did,
      collection: record.collection,
      rkey: record.rkey,
      record: record.record ?? {},
      seq,
      createdAt,
      receivedAt: new Date(),
      source: "nexus",
      isLive: record.live,
      nexusEventId: event.id,
    };
  }

  private resolveTimestamps(
    record: any,
    rkey: string,
    did: string,
  ): { createdAt: Date; seq: number } {
    // Try createdAt field first
    const createdAtField =
      typeof record?.createdAt === "string" ? new Date(record.createdAt) : null;
    if (createdAtField && !Number.isNaN(createdAtField.getTime())) {
      return {
        createdAt: createdAtField,
        seq: createdAtField.getTime() * 1000,
      };
    }

    // Fall back to TID parsing
    try {
      const tid = parseTid(rkey);
      return { createdAt: new Date(tid.timestamp), seq: tid.timestamp };
    } catch {
      // Last resort: use current time
      const now = new Date();
      return { createdAt: now, seq: now.getTime() * 1000 };
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) {
      return; // Already scheduled
    }

    const backoffDelays =
      this.config.reconnectBackoff ?? DEFAULT_RECONNECT_BACKOFF;
    const delay =
      backoffDelays[Math.min(this.reconnectAttempt, backoffDelays.length - 1)];

    logger.info(
      { attempt: this.reconnectAttempt + 1, delayMs: delay },
      "Scheduling Nexus reconnection",
    );

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.reconnectAttempt++;
      this.connect().catch((error) => {
        logger.error({ err: error }, "Reconnection failed");
      });
    }, delay);
  }
}
