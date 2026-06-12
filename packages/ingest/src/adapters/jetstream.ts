import { type CommitCreateEvent, Jetstream } from '@skyware/jetstream';

import logger from '../logger.js';
import type { IngestSource, RawPostEvent } from '../types.js';

const RECONNECT_MIN_DELAY_MS = 1_000;
const RECONNECT_MAX_DELAY_MS = 30_000;

interface BskyPostRecord {
  text?: string;
  langs?: string[];
  createdAt?: string;
}

function epochUsToDateTime(cursor: number): string {
  return new Date(cursor / 1000).toISOString();
}

export class JetstreamSource implements IngestSource {
  private jetstream: Jetstream<'app.bsky.feed.post'> | undefined;
  private onEvent: ((event: RawPostEvent) => void) | undefined;
  private startResolve: (() => void) | undefined;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private reconnectDelayMs = RECONNECT_MIN_DELAY_MS;
  private stopped = false;

  constructor(
    private readonly endpoint: string,
    private readonly initialCursor: number | undefined,
  ) {}

  get cursor(): number | undefined {
    // A closed instance keeps its last consumed cursor, so this stays valid across reconnects.
    return this.jetstream?.cursor ?? this.initialCursor;
  }

  start(onEvent: (event: RawPostEvent) => void): Promise<void> {
    this.onEvent = onEvent;
    return new Promise((resolve) => {
      this.startResolve = resolve;
      this.connect();
    });
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.reconnectTimer !== undefined) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    const jetstream = this.jetstream;
    if (!jetstream) return;
    const ws = jetstream.ws;
    if (!ws || ws.readyState === ws.CLOSED) {
      jetstream.close();
      return;
    }
    await new Promise<void>((resolve) => {
      jetstream.once('close', () => resolve());
      jetstream.close();
    });
  }

  private connect(): void {
    const jetstream = new Jetstream({
      wantedCollections: ['app.bsky.feed.post'],
      endpoint: this.endpoint,
      cursor: this.cursor,
    });
    this.jetstream = jetstream;

    jetstream.on('open', () => {
      this.reconnectDelayMs = RECONNECT_MIN_DELAY_MS;
      logger.info('Connected to Jetstream');
      this.startResolve?.();
      this.startResolve = undefined;
    });

    jetstream.on('close', () => {
      logger.info('Jetstream connection closed.');
      if (this.stopped || jetstream !== this.jetstream) return;
      this.scheduleReconnect(jetstream);
    });

    jetstream.on('error', (error) => {
      logger.error(`Jetstream error: ${error.message}`);
    });

    jetstream.onCreate(
      'app.bsky.feed.post',
      (event: CommitCreateEvent<'app.bsky.feed.post'>) => {
        const record = event.commit.record as BskyPostRecord;
        this.onEvent?.({
          did: event.did,
          rkey: event.commit.rkey,
          text: record.text ?? '',
          langs: record.langs,
          createdAt: record.createdAt,
          timeUs: event.time_us,
        });
      },
    );

    jetstream.start();
  }

  private scheduleReconnect(previous: Jetstream<'app.bsky.feed.post'>): void {
    // partysocket (inside Jetstream) schedules its own retry before emitting 'close';
    // close() defuses it so this backoff is the only reconnect driver.
    previous.close();
    // Jetstream extends EventEmitter at runtime but its declared type stopped
    // surfacing the inherited members under newer tsc — cast for this call.
    (previous as unknown as NodeJS.EventEmitter).removeAllListeners();
    // The dying socket can still emit a late 'error' (often with an empty
    // message), and an EventEmitter with no 'error' listener turns that into
    // an uncaught exception — which took the whole worker down. Keep a drain
    // attached for the instance's remaining lifetime.
    previous.on('error', (error) => {
      logger.debug(
        `Late error from closed Jetstream socket: ${error.message || '(empty)'}`,
      );
    });

    const delayMs = this.reconnectDelayMs;
    this.reconnectDelayMs = Math.min(
      this.reconnectDelayMs * 2,
      RECONNECT_MAX_DELAY_MS,
    );
    const cursor = this.cursor;
    logger.warn(
      `Reconnecting to Jetstream in ${delayMs}ms` +
        (cursor !== undefined
          ? ` from cursor ${cursor} (${epochUsToDateTime(cursor)})`
          : ''),
    );
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.connect();
    }, delayMs);
  }
}
