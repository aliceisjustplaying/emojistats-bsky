import { createClient, type ClickHouseClient } from '@clickhouse/client';

import {
  CLICKHOUSE_DATABASE,
  CLICKHOUSE_PASSWORD,
  CLICKHOUSE_REQUEST_TIMEOUT_MS,
  CLICKHOUSE_URL,
  CLICKHOUSE_USER,
} from './config.js';

/**
 * Mirrors packages/ingest/src/clickhouse/client.ts — same server and database,
 * tagged 'emojistats-backfill' so the two writers are distinguishable in
 * system.query_log / system.processes.
 */
export function createClickHouseClient(
  application = 'emojistats-backfill',
  requestTimeoutMs = CLICKHOUSE_REQUEST_TIMEOUT_MS,
): ClickHouseClient {
  return createClient({
    url: CLICKHOUSE_URL,
    username: CLICKHOUSE_USER,
    password: CLICKHOUSE_PASSWORD,
    database: CLICKHOUSE_DATABASE,
    application,
    request_timeout: requestTimeoutMs,
    // JSONEachRow post batches are very repetitive but large. Compressing the
    // request body cuts upload time and avoids ClickHouse seeing truncated
    // HTTP bodies when a client-side socket resets mid-upload.
    compression: { request: true },
    // Keeps the HTTP socket alive while ClickHouse works through large
    // JSONEachRow inserts; otherwise the load balancer can close an idle
    // request and force an identical retry.
    clickhouse_settings: {
      send_progress_in_http_headers: 1,
      http_headers_progress_interval_ms: '10000',
    },
    // CAR parsing blocks the event loop past the 2.5s socket TTL; without
    // this the client reuses server-closed sockets and inserts hang forever
    // (launch night: fetching=128, zero completions, telemetry frozen).
    keep_alive: { eagerly_destroy_stale_sockets: true },
  });
}

export async function pingClickHouse(client: ClickHouseClient): Promise<void> {
  try {
    const result = await client.query({
      query: 'SELECT 1',
      format: 'JSONEachRow',
    });
    await result.json();
  } catch (err) {
    throw new Error(
      `ClickHouse unreachable at ${CLICKHOUSE_URL} (database "${CLICKHOUSE_DATABASE}"). ` +
        'Is it up? Start it with `docker compose up` and apply the schema with `bun run db:migrate` in packages/ingest.',
      { cause: err },
    );
  }
}
