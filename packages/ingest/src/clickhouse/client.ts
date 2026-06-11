import { createClient, type ClickHouseClient } from '@clickhouse/client';

import {
  CLICKHOUSE_DATABASE,
  CLICKHOUSE_PASSWORD,
  CLICKHOUSE_URL,
  CLICKHOUSE_USER,
} from '../config.js';

export function createClickHouseClient(): ClickHouseClient {
  return createClient({
    url: CLICKHOUSE_URL,
    username: CLICKHOUSE_USER,
    password: CLICKHOUSE_PASSWORD,
    database: CLICKHOUSE_DATABASE,
    application: 'emojistats-ingest',
    request_timeout: 30_000,
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
        'Is it up? Start it with `docker compose up` and apply the schema with `bun run db:migrate`.',
      { cause: err },
    );
  }
}
