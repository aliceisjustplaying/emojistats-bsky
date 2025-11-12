import WebSocket from "ws";
import type { UnifiedEvent, NexusOutboxEvent, EventAdapter } from "./types.js";
import { logger } from "../logger.js";
import { parse as parseTid } from "@atcute/tid";

export interface NexusAdapterConfig {
  url: string;
  ackTimeout?: number; // milliseconds, default 90000 (must be > flush interval 60s)
  reconnectBackoff?: number[]; // backoff delays in ms
}

// Default ack timeout must be longer than flush interval (60s) to prevent false timeouts
// Events wait for batch flush before acking, which can take up to 60s (timer flush)
const DEFAULT_ACK_TIMEOUT = 90000; // 90 seconds (flush interval 60s + buffer)
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
    await this.ackById(event.nexusEventId);
  }

  /**
   * Ack a Nexus event by ID directly. Used internally for filtered events
   * that don't need to go through the full event processing pipeline.
   */
  private async ackById(eventId: number): Promise<void> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      logger.warn(
        { eventId, wsState: this.ws?.readyState },
        "Cannot ack: WebSocket not open",
      );
      throw new Error(`WebSocket not open for event ${eventId}`);
    }

    // Clear any pending timeout for this event
    const timeout = this.inFlightAcks.get(eventId);
    if (timeout) {
      clearTimeout(timeout);
      this.inFlightAcks.delete(eventId);
    }

    try {
      const ackMessage = JSON.stringify({ id: eventId });
      this.ws.send(ackMessage);
      logger.trace({ eventId }, "Sent ack for filtered event");
    } catch (error) {
      logger.error({ err: error, eventId }, "Failed to send ack");
      throw error; // Re-throw so caller knows ack failed
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
        // Event was filtered out (invalid timestamp, wrong collection, etc.)
        // Still need to track repo state and ack it
        // Create minimal UnifiedEvent for state tracking, handler will ack it
        const filteredEvent: UnifiedEvent = {
          repoDid: event.record.did,
          collection: event.record.collection,
          rkey: event.record.rkey,
          record: event.record.record ?? {},
          seq: 0,
          createdAt: new Date(), // Won't be used since event is filtered
          receivedAt: new Date(),
          source: "nexus",
          isLive: event.record.live,
          nexusEventId: event.id,
        };

        // Set up ack timeout for filtered events too
        if (filteredEvent.nexusEventId) {
          const timeout = setTimeout(() => {
            logger.warn(
              { eventId: filteredEvent.nexusEventId },
              "Ack timeout for filtered Nexus event",
            );
            this.inFlightAcks.delete(filteredEvent.nexusEventId!);
          }, this.config.ackTimeout ?? DEFAULT_ACK_TIMEOUT);
          this.inFlightAcks.set(filteredEvent.nexusEventId, timeout);
        }

        // Pass to handler for repo state tracking and acking
        // Handler will ack it in finally block, but we need to ensure errors don't stop processing
        try {
          if (this.eventCallback) {
            await this.eventCallback(filteredEvent);
          } else {
            // No callback set, ack directly
            await this.ackById(event.id);
          }
        } catch (error) {
          logger.error(
            { err: error, eventId: event.id, repoDid: filteredEvent.repoDid },
            "Error processing filtered event - acking to prevent stall",
          );
          // Handler failed, ack directly to prevent Nexus stalling
          await this.ackById(event.id).catch((ackError) => {
            logger.error(
              { err: ackError, eventId: event.id },
              "Failed to ack filtered event after handler error",
            );
          });
        }
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
    // Validate timestamp BEFORE creating UnifiedEvent - if invalid, return null
    // The event will still be acked by the handler (it's a filtered event)
    const { createdAt, seq, isValid } = this.resolveTimestamps(
      record.record as any,
      record.rkey,
      record.did,
    );

    // If timestamp is invalid (used fallback), don't create UnifiedEvent
    // This prevents processing records with corrupted timestamps
    if (!isValid) {
      return null;
    }

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
  ): { createdAt: Date; seq: number; isValid: boolean } {
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
        isValid: true,
      };
    }

    // Fall back to TID parsing
    try {
      const tid = parseTid(rkey);
      const tidDate = new Date(tid.timestamp);
      if (isValidDate(tidDate)) {
        return { createdAt: tidDate, seq: tid.timestamp, isValid: true };
      }
    } catch {
      // TID parsing failed
    }

    // Invalid timestamp - log and return invalid flag
    // Don't use fallback - let the event be filtered out
    logger.warn(
      { did, rkey, createdAt: record?.createdAt },
      "Invalid timestamp, filtering out event",
    );
    const now = new Date();
    return { createdAt: now, seq: now.getTime() * 1000, isValid: false };
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
