// Server-only ClickHouse access over the HTTP interface (no client library).
// All queries go through `chQuery`, which appends FORMAT JSON and uses basic
// auth from the environment (see packages/dashboard/.env).

interface ChJsonResponse<T> {
  data: Array<T>;
  rows: number;
  statistics?: { elapsed: number; rows_read: number; bytes_read: number };
}

function config() {
  return {
    url: process.env.CLICKHOUSE_URL ?? 'http://localhost:8123',
    database: process.env.CLICKHOUSE_DATABASE ?? 'emojistats',
    user: process.env.CLICKHOUSE_USER ?? 'default',
    password: process.env.CLICKHOUSE_PASSWORD ?? '',
  };
}

export function chDatabase(): string {
  return config().database;
}

/**
 * Run a SQL query and return the parsed `data` rows from FORMAT JSON output.
 * `params` are passed as ClickHouse query parameters ({name:String} syntax).
 *
 * Note: ClickHouse serializes UInt64/Int64 as JSON strings; coerce with
 * `num()` when reading numeric fields.
 */
export async function chQuery<T>(
  sql: string,
  params?: Record<string, string>,
): Promise<Array<T>> {
  const { url, database, user, password } = config();
  const endpoint = new URL(url);
  endpoint.searchParams.set('database', database);
  // The backfill insert load keeps the server pinned at its memory cap, and
  // the OvercommitTracker kills whichever aggregation asks next — which was
  // these dashboard queries (the public page 500'd mid-crawl). Cap our
  // appetite and spill big GROUP BYs to disk instead of competing for RAM.
  endpoint.searchParams.set('max_memory_usage', '1200000000');
  endpoint.searchParams.set('max_bytes_before_external_group_by', '600000000');
  endpoint.searchParams.set('max_bytes_before_external_sort', '600000000');
  for (const [key, value] of Object.entries(params ?? {})) {
    endpoint.searchParams.set(`param_${key}`, value);
  }

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${Buffer.from(`${user}:${password}`).toString('base64')}`,
      'Content-Type': 'text/plain',
    },
    body: `${sql.trim().replace(/;\s*$/, '')}\nFORMAT JSON`,
  });

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `ClickHouse query failed (HTTP ${response.status}): ${detail.slice(0, 500)}`,
    );
  }

  const payload = (await response.json()) as ChJsonResponse<T>;
  return payload.data;
}

/** Coerce a ClickHouse JSON number (UInt64 arrives as a string) to a number. */
export function num(value: string | number | null | undefined): number {
  if (value === null || value === undefined) return 0;
  return typeof value === 'number' ? value : Number(value);
}
