import { parseArgs } from 'node:util';

import { PLC_DIRECTORY_URL, USER_AGENT } from './config.js';
import { pdsHostFromEndpoint } from './fetcher.js';
import { SqliteLedger } from './ledger.js';
import logger from './logger.js';

/**
 * PLC directory enumeration: streams the /export feed into the ledger.
 *
 * Since the January 2026 export change (bluesky-social/atproto discussion #4508),
 * /export paginates by sequence number: pass ?after=<seq> (numeric) and every line
 * carries a top-level `seq`, strictly ascending, with per-DID order preserved —
 * so "last op wins" holds when replaying. Timestamp cursors are deprecated; we
 * always send a numeric seq (0 on first run). Verified empirically 2026-06-11:
 * pages cap at 1000 lines regardless of count, caught-up = HTTP 200 empty body.
 */

const CURSOR_META_KEY = 'plc_cursor';
const EXPORT_PAGE_SIZE = 1000;
// plc.directory's export endpoint exists to be consumed; 429/backoff (below) is the
// governor, not artificial pacing (operator experience: Alice).
const PAGE_DELAY_MS = Number(process.env.PLC_PAGE_DELAY_MS ?? 25);
const FETCH_TIMEOUT_MS = 60_000;
const MAX_FETCH_RETRIES = 5;
const PROGRESS_EVERY_PAGES = 5;

interface PlcExportLine {
  did: string;
  seq?: number;
  nullified?: boolean;
  operation: {
    type: string;
    /** legacy genesis op ('create'): PDS endpoint as a bare string */
    service?: string;
    /** modern op ('plc_operation') */
    services?: Record<string, { type?: string; endpoint?: string } | undefined>;
  };
}

interface DidDocument {
  service?: { id: string; type: string; serviceEndpoint: unknown }[];
}

// Object, not a bare boolean: the signal handler mutates it while loops poll it.
const stop = { requested: false };

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function parseRetryAfterMs(header: string | null): number | undefined {
  if (header === null) return undefined;
  const seconds = Number(header);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
  const dateMs = Date.parse(header);
  if (!Number.isNaN(dateMs)) return Math.max(0, dateMs - Date.now());
  return undefined;
}

/**
 * Fetch with UA, timeout, and backoff on 429/5xx/network errors.
 * Non-retryable 4xx responses are returned for the caller to interpret
 * (404 = unregistered DID, 410 = tombstoned DID).
 */
async function politeFetch(url: string): Promise<Response> {
  for (let attempt = 1; ; attempt++) {
    let response: Response | undefined;
    let failure: string | undefined;
    try {
      response = await fetch(url, {
        headers: { 'user-agent': USER_AGENT },
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
      });
    } catch (error) {
      failure = (error as Error).message;
    }
    if (
      response !== undefined &&
      response.status !== 429 &&
      response.status < 500
    )
      return response;

    let retryAfterMs: number | undefined;
    if (response !== undefined) {
      failure = `HTTP ${response.status}`;
      retryAfterMs = parseRetryAfterMs(response.headers.get('retry-after'));
      await response.arrayBuffer().catch(() => undefined);
    }
    if (attempt >= MAX_FETCH_RETRIES)
      throw new Error(
        `GET ${url} failed after ${attempt} attempts: ${failure}`,
      );
    const delayMs = Math.min(
      Math.max(retryAfterMs ?? 2 ** attempt * 1000, 1000),
      120_000,
    );
    logger.warn(
      { url, attempt, failure, delayMs },
      'plc request failed, backing off',
    );
    await sleep(delayMs);
  }
}

function endpointFromOperation(
  op: PlcExportLine['operation'],
): string | undefined {
  if (op.type === 'create') return op.service;
  if (op.type === 'plc_operation')
    return op.services?.['atproto_pds']?.endpoint;
  return undefined;
}

async function runExport(
  ledger: SqliteLedger,
  limit: number | undefined,
): Promise<void> {
  const rawCursor = ledger.getMeta(CURSOR_META_KEY) ?? '0';
  let cursor = Number(rawCursor);
  if (!Number.isSafeInteger(cursor) || cursor < 0) {
    throw new Error(`Corrupt ${CURSOR_META_KEY} in ledger meta: ${rawCursor}`);
  }
  logger.info(
    { cursor, limit: limit ?? null },
    'starting PLC export enumeration',
  );

  const touched = new Set<string>();
  const stats = {
    pages: 0,
    ops: 0,
    upserted: 0,
    tombstoned: 0,
    skipped: 0,
    parked: 0,
  };
  let caughtUp = false;

  // Crawler-published dead-host verdicts, refreshed once a minute so a host
  // tripping mid-run starts diverting within a page or two.
  let deadHosts = new Set(ledger.getDeadHosts());
  let deadHostsReadAt = Date.now();

  while (!stop.requested) {
    if (Date.now() - deadHostsReadAt > 60_000) {
      deadHosts = new Set(ledger.getDeadHosts());
      deadHostsReadAt = Date.now();
    }
    const response = await politeFetch(
      `${PLC_DIRECTORY_URL}/export?count=${EXPORT_PAGE_SIZE}&after=${cursor}`,
    );
    if (!response.ok)
      throw new Error(`export page failed: HTTP ${response.status}`);
    const lines = (await response.text())
      .split('\n')
      .filter((line) => line.length > 0);
    if (lines.length === 0) {
      caughtUp = true;
      break;
    }

    let limitReached = false;
    ledger.transaction(() => {
      for (const raw of lines) {
        const line = JSON.parse(raw) as PlcExportLine;
        if (typeof line.seq !== 'number') {
          throw new Error(
            `export line missing seq (did the API change again?): ${raw.slice(0, 200)}`,
          );
        }
        cursor = line.seq;
        stats.ops++;
        if (line.nullified === true) {
          stats.skipped++;
          continue;
        }
        if (line.operation.type === 'plc_tombstone') {
          ledger.markTombstoned(line.did);
          touched.add(line.did);
          stats.tombstoned++;
        } else {
          const endpoint = endpointFromOperation(line.operation);
          const host =
            endpoint === undefined ? undefined : pdsHostFromEndpoint(endpoint);
          if (host === undefined) {
            stats.skipped++;
            continue;
          }
          // Hosts on the crawler's dead list (meta 'dead_hosts'): rows are
          // born parked. Upserting them 'pending' made the crawler's bulk
          // park race enumeration forever on spam waves (pds.trump.com:
          // 17.9M rows) — the park drained 'pending' at ~18k rows/s while
          // this loop refilled it, pinning the crawl main thread.
          if (deadHosts.has(host)) {
            ledger.upsertParked(
              line.did,
              host,
              `host dead: ${host} (enumerated onto final-sweep list)`,
            );
            stats.parked++;
          } else {
            ledger.upsertPending(line.did, host);
          }
          touched.add(line.did);
          stats.upserted++;
        }
        if (limit !== undefined && touched.size >= limit) {
          limitReached = true;
          break;
        }
      }
      // Cursor commits atomically with the page's rows: a crash never loses writes
      // nor skips ops, and replaying a page is idempotent anyway.
      ledger.setMeta(CURSOR_META_KEY, String(cursor));
    });

    stats.pages++;
    if (stats.pages % PROGRESS_EVERY_PAGES === 0) {
      logger.info({ ...stats, cursor, dids: touched.size }, 'export progress');
    }
    if (limitReached) {
      logger.info({ limit, dids: touched.size }, 'distinct-DID limit reached');
      break;
    }
    if (lines.length < EXPORT_PAGE_SIZE) {
      caughtUp = true;
      break;
    }
    await sleep(PAGE_DELAY_MS);
  }

  logger.info(
    {
      ...stats,
      cursor,
      dids: touched.size,
      caughtUp,
      interrupted: stop.requested,
    },
    'export enumeration finished',
  );
}

async function runDids(ledger: SqliteLedger, dids: string[]): Promise<void> {
  for (const did of dids) {
    if (stop.requested) break;
    const response = await politeFetch(`${PLC_DIRECTORY_URL}/${did}`);
    if (response.status === 404) {
      logger.warn({ did }, 'DID not registered in PLC');
      continue;
    }
    if (response.status === 410) {
      ledger.markTombstoned(did);
      logger.info({ did }, 'DID is tombstoned');
      continue;
    }
    if (!response.ok)
      throw new Error(`resolving ${did} failed: HTTP ${response.status}`);
    const doc = (await response.json()) as DidDocument;
    const service = doc.service?.find(
      (s) =>
        s.id.endsWith('#atproto_pds') && s.type === 'AtprotoPersonalDataServer',
    );
    const host =
      typeof service?.serviceEndpoint === 'string'
        ? pdsHostFromEndpoint(service.serviceEndpoint)
        : undefined;
    if (host === undefined) {
      logger.warn(
        { did },
        'DID document has no usable atproto_pds service, skipping',
      );
      continue;
    }
    ledger.upsertPending(did, host);
    logger.info(
      { did, pdsHost: host, status: ledger.getRepo(did)?.status },
      'enumerated DID',
    );
  }
}

function onSignal(signal: NodeJS.Signals): void {
  if (stop.requested) {
    logger.warn({ signal }, 'second signal — forcing exit');
    process.exit(1);
  }
  stop.requested = true;
  logger.info({ signal }, 'finishing current page, then stopping');
}

process.on('SIGINT', onSignal);
process.on('SIGTERM', onSignal);

// bun/npm may forward the `--` separator from `bun run enumerate -- --limit N`.
const argv = process.argv.slice(2).filter((arg) => arg !== '--');
const { values } = parseArgs({
  args: argv,
  options: {
    limit: { type: 'string' },
    did: { type: 'string', multiple: true },
  },
});

let limit: number | undefined;
if (values.limit !== undefined) {
  limit = Number(values.limit);
  if (!Number.isSafeInteger(limit) || limit <= 0) {
    logger.fatal({ limit: values.limit }, '--limit must be a positive integer');
    process.exit(1);
  }
}

const ledger = new SqliteLedger();
try {
  if (values.did !== undefined && values.did.length > 0) {
    await runDids(ledger, values.did);
  } else {
    await runExport(ledger, limit);
  }
} catch (error) {
  logger.fatal({ err: error }, 'enumeration failed');
  process.exitCode = 1;
} finally {
  ledger.close();
}
