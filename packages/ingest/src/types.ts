/** A post-create event as delivered by an ingest source (Jetstream now, backfill crawler later). */
export interface RawPostEvent {
  did: string;
  rkey: string;
  /** Record text; may be empty. */
  text: string;
  langs?: string[];
  /** record.createdAt as found on the wire — client-supplied, may lie. */
  createdAt?: string;
  /** Receive-time proxy in epoch microseconds (Jetstream time_us). */
  timeUs: number;
}

export type Source = 'live' | 'backfill';

export type Anomaly =
  | 'createdat-tid-fallback'
  | 'createdat-receive-fallback'
  | 'emoji-truncated';

export interface NormalizedPost {
  did: string;
  rkey: string;
  /** Event time after validation/clamping — never assume this came straight off the record. */
  createdAt: Date;
  text: string;
  /** Distinct, never empty; ['unknown'] when the record carries no langs. */
  langs: string[];
  /** Normalized glyphs, one entry per occurrence (repeats kept), capped at EMOJI_MAX_PER_POST. */
  emojis: string[];
  anomalies: Anomaly[];
}

/** Row shape for ClickHouse `posts` (JSONEachRow). created_at is 'YYYY-MM-DD HH:MM:SS' in UTC. */
export interface PostRow {
  did: string;
  rkey: string;
  created_at: string;
  text: string;
  langs: string[];
  emojis: string[];
  src: Source;
}

export interface IngestSource {
  /** Resolves once the connection is up; events flow to the callback until stop(). */
  start(onEvent: (event: RawPostEvent) => void): Promise<void>;
  stop(): Promise<void>;
  /** Latest consumed cursor (epoch µs), if the source has one. */
  readonly cursor: number | undefined;
}
