/**
 * Unified event interface that abstracts Nexus and Jetstream sources
 */
export interface UnifiedEvent {
  // Common fields
  repoDid: string;
  collection: string;
  rkey: string;
  record: unknown;
  seq: number;
  createdAt: Date;
  receivedAt: Date;

  // Source metadata
  source: "nexus" | "jetstream";
  isLive: boolean; // true for Jetstream, false for Nexus backfill

  // Source-specific ack/cursor handling
  nexusEventId?: number; // For Nexus acks
  jetstreamCursor?: number; // For Jetstream cursor persistence
}

/**
 * Nexus event format as received from WebSocket
 */
export interface NexusOutboxEvent {
  id: number;
  type: "record" | "user";
  record?: NexusRecordEvent;
  user?: NexusUserEvent;
}

export interface NexusRecordEvent {
  live: boolean;
  did: string;
  rev: string;
  collection: string;
  rkey: string;
  action: string;
  record?: Record<string, unknown>;
  cid?: string;
}

export interface NexusUserEvent {
  did: string;
  handle: string;
  is_active: boolean;
  status: string;
}

/**
 * Ack message sent back to Nexus
 */
export interface NexusAckMessage {
  id: number;
}

/**
 * Event adapter interface
 */
export interface EventAdapter {
  start(): Promise<void>;
  stop(): Promise<void>;
  onEvent(callback: (event: UnifiedEvent) => Promise<void>): void;
  ack(event: UnifiedEvent): Promise<void>;
}
