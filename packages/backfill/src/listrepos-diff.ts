/**
 * Host listRepos diff (operator tool) — the anti-spam classifier learned from
 * microcosm's hubble/lightrail: a PDS's own com.atproto.sync.listRepos is the
 * ground truth of which repos EXIST there. PLC-spam DIDs (registered in the
 * directory, repo never created) are absent from it and would each cost one
 * rate-limited getRepo just to learn "RepoNotFound". One listRepos walk —
 * morel's ~490k repos is ~500 pages, about two minutes at the 10 rps
 * microcosm proved safe — classifies them all for free.
 *
 *   bun run listrepos-diff -- --host morel.us-east.host.bsky.network          # report only
 *   bun run listrepos-diff -- --host morel.us-east.host.bsky.network --apply  # classify
 *
 * With --apply, pending/unreachable rows for the host whose DID is NOT in
 * the listing are marked terminal 'failed' — the exact status the per-DID
 * path produces for getRepo 400 RepoNotFound (fetcher.ts) — and listed-but-
 * inactive repos get their listed status (takendown/deactivated). Rows that
 * ARE listed and active stay pending and crawl normally. Nothing is applied
 * unless the walk completed cleanly and listed at least one repo: a partial
 * or empty listing must never condemn rows. Safe alongside a running
 * crawler: writes are chunked transactions, and each batch re-checks row
 * status inside its transaction so a repo the crawler claimed mid-diff
 * settles through the normal fetch path instead of being clobbered.
 */

import { parseArgs } from 'node:util';

import { LEDGER_DB_PATH, USER_AGENT } from './config.js';
import { SqliteLedger } from './ledger.js';
import logger from './logger.js';

const PAGE_LIMIT = 1000;
const PAGE_DELAY_MS = 100; // ~10 rps, microcosm's field-proven mushroom limit
const FETCH_TIMEOUT_MS = 30_000;
const MAX_PAGE_RETRIES = 5;
const READ_CHUNK = 50_000;
const WRITE_CHUNK = 10_000;
/** Hosts walked in parallel during --all; paging within a host stays 10 rps. */
const HOST_CONCURRENCY = 10;

/**
 * Hosts whose listRepos is broken or lying (operator-curated; brid.gy
 * doesn't implement it, the eurosky demo PDSes and tz2at DID farms aren't
 * trustworthy listings). Extend per run with --skip. A failed walk never
 * classifies anything anyway — the skip list just saves the retries.
 */
const SKIP_HOSTS_DEFAULT = [
  'atproto.brid.gy',
  'live2025demo.eurosky.social',
  'berlin-demo.eurosky.social',
  'pds-test-9879187213.eurosky.social',
  'user.eurosky.social',
  'berlin-user.eurosky.social',
  'wallets.tz2at.store',
  'contracts.tz2at.store',
];

interface ListedRepo {
  did: string;
  active?: boolean;
  status?: string;
}

interface ListReposPage {
  cursor?: string;
  repos: ListedRepo[];
}

const sleep = (ms: number) =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

/** Full listRepos walk; throws unless the listing completed cleanly. */
async function walkListRepos(
  host: string,
  maxPageRetries: number = MAX_PAGE_RETRIES,
): Promise<Map<string, ListedRepo>> {
  const base = host.includes('://') ? host : `https://${host}`;
  const listed = new Map<string, ListedRepo>();
  let cursor: string | undefined;
  let pages = 0;
  for (;;) {
    const url =
      `${base}/xrpc/com.atproto.sync.listRepos?limit=${PAGE_LIMIT}` +
      (cursor === undefined ? '' : `&cursor=${encodeURIComponent(cursor)}`);
    let page: ListReposPage | undefined;
    for (let attempt = 1; page === undefined; attempt += 1) {
      try {
        const res = await fetch(url, {
          headers: { 'user-agent': USER_AGENT },
          signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
        });
        if (!res.ok) throw new Error(`http ${res.status}`);
        page = (await res.json()) as ListReposPage;
      } catch (err) {
        if (attempt >= maxPageRetries) throw err;
        const delayMs = 1_000 * 2 ** (attempt - 1);
        logger.warn(
          { host, pages, attempt, delayMs },
          'listRepos page failed, backing off',
        );
        await sleep(delayMs);
      }
    }
    for (const repo of page.repos) listed.set(repo.did, repo);
    pages += 1;
    if (pages % 50 === 0)
      logger.info({ host, pages, repos: listed.size }, 'listRepos progress');
    // Stuck-cursor guard (both microcosm crawlers carry one): a server
    // re-serving the same cursor would loop us forever.
    if (page.cursor === undefined || page.repos.length === 0) break;
    if (page.cursor === cursor)
      throw new Error(`listRepos cursor stuck at ${cursor}`);
    cursor = page.cursor;
    await sleep(PAGE_DELAY_MS);
  }
  logger.info({ host, pages, repos: listed.size }, 'listRepos walk complete');
  return listed;
}

/** Listed-but-inactive repos map to the status the per-DID fetch would yield. */
function terminalStatusFor(
  repo: ListedRepo | undefined,
): 'failed' | 'takendown' | 'deactivated' | null {
  if (repo === undefined) return 'failed'; // PLC-only DID: getRepo would 400 RepoNotFound
  if (repo.active !== false) return null; // real, live repo: crawl it
  if (repo.status === 'takendown' || repo.status === 'suspended')
    return 'takendown';
  if (repo.status === 'deactivated') return 'deactivated';
  return 'failed';
}

interface HostDiffResult {
  host: string;
  listedOnHost: number;
  listedInactive: number;
  scanned: number;
  plcOnly: number;
  classified: number;
}

async function diffHost(
  ledger: SqliteLedger,
  host: string,
  apply: boolean,
  pageRetries: number,
): Promise<HostDiffResult | null> {
  // Rows enumerated after this instant are never condemned: an account
  // created mid-walk could be missing from already-fetched pages.
  const walkStartedAt = Date.now();
  let listed: Map<string, ListedRepo>;
  try {
    listed = await walkListRepos(host, pageRetries);
  } catch (err) {
    logger.warn(
      { host, err: err instanceof Error ? err.message : String(err) },
      'listRepos walk failed; host skipped (nothing classified)',
    );
    return null;
  }
  if (listed.size === 0) {
    logger.warn(
      { host },
      'listRepos returned zero repos; refusing to classify against an empty listing',
    );
    return null;
  }

  const counts: HostDiffResult = {
    host,
    listedOnHost: listed.size,
    listedInactive: 0,
    scanned: 0,
    plcOnly: 0,
    classified: 0,
  };
  for (const repo of listed.values())
    if (repo.active === false) counts.listedInactive += 1;

  for (const status of ['pending', 'unreachable'] as const) {
    let afterRowid = 0;
    for (;;) {
      const rows = ledger.listHostStatusDids(
        host,
        status,
        afterRowid,
        READ_CHUNK,
        walkStartedAt,
      );
      if (rows.length === 0) break;
      afterRowid = rows.at(-1)!.rowid;
      counts.scanned += rows.length;

      const condemned: Array<{
        did: string;
        terminal: 'failed' | 'takendown' | 'deactivated';
        error: string;
      }> = [];
      for (const { did } of rows) {
        const repo = listed.get(did);
        const terminal = terminalStatusFor(repo);
        if (terminal === null) continue;
        if (repo === undefined) counts.plcOnly += 1;
        condemned.push({
          did,
          terminal,
          error:
            repo === undefined
              ? `not in ${host} listRepos (PLC-only DID)`
              : `${host} listRepos: active=false (${repo.status ?? 'unknown'})`,
        });
      }

      if (apply) {
        for (let i = 0; i < condemned.length; i += WRITE_CHUNK) {
          const batch = condemned.slice(i, i + WRITE_CHUNK);
          ledger.transaction(() => {
            for (const { did, terminal, error } of batch) {
              // Re-check inside the transaction: a row the crawler claimed
              // (status now 'fetching') settles through the normal path.
              const current = ledger.getRepo(did);
              if (
                current?.status !== 'pending' &&
                current?.status !== 'unreachable'
              )
                continue;
              ledger.markTerminal(did, terminal, error);
              counts.classified += 1;
            }
          });
          await sleep(50);
        }
      } else {
        counts.classified += condemned.length;
      }
      await sleep(25);
    }
  }

  logger.info(
    { apply, ...counts },
    apply
      ? 'listrepos-diff applied'
      : 'listrepos-diff report (re-run with --apply to classify)',
  );
  return counts;
}

async function main(): Promise<void> {
  const { values } = parseArgs({
    options: {
      host: { type: 'string', multiple: true },
      skip: { type: 'string', multiple: true },
      apply: { type: 'boolean', default: false },
      all: { type: 'boolean', default: false },
      report: { type: 'string' },
    },
  });
  const skipHosts = new Set([...SKIP_HOSTS_DEFAULT, ...(values.skip ?? [])]);

  // shards=1 like every operator tool: the verdict is per-host, the whole
  // ledger file should agree with it regardless of which shard owns a row.
  const ledger = new SqliteLedger(LEDGER_DB_PATH, { busyTimeoutMs: 60_000 });

  // --all sweeps every host that still owns pending rows, deepest first so
  // the spam-bloated mushrooms land early. Dead hosts fail fast (2 page
  // attempts instead of 5) — a failed walk classifies nothing by design.
  const hosts = (
    values.all
      ? ledger.pendingHostCounts().map((h) => h.host)
      : (values.host ?? [])
  ).filter((host) => {
    if (!skipHosts.has(host)) return true;
    logger.warn({ host }, 'host is on the skip list; not diffing');
    return false;
  });
  if (hosts.length === 0)
    throw new Error(
      'usage: listrepos-diff (--host <pds_host> [--host ...] | --all) [--skip <host>] [--apply] [--report <file.jsonl>]',
    );
  const pageRetries = values.all ? 2 : MAX_PAGE_RETRIES;

  const results: HostDiffResult[] = [];
  let cursor = 0;
  let walked = 0;
  await Promise.all(
    Array.from(
      { length: Math.min(HOST_CONCURRENCY, hosts.length) },
      async () => {
        for (;;) {
          const host = hosts[cursor];
          cursor += 1;
          if (host === undefined) return;
          const result = await diffHost(
            ledger,
            host,
            values.apply,
            pageRetries,
          );
          walked += 1;
          if (result !== null) results.push(result);
          if (walked % 250 === 0)
            logger.info(
              {
                walked,
                of: hosts.length,
                classified: results.reduce((a, r) => a + r.classified, 0),
              },
              'sweep progress',
            );
        }
      },
    ),
  );

  const total = results.reduce(
    (acc, r) => ({
      hostsDiffed: acc.hostsDiffed + 1,
      listedOnHost: acc.listedOnHost + r.listedOnHost,
      scanned: acc.scanned + r.scanned,
      plcOnly: acc.plcOnly + r.plcOnly,
      classified: acc.classified + r.classified,
    }),
    { hostsDiffed: 0, listedOnHost: 0, scanned: 0, plcOnly: 0, classified: 0 },
  );
  logger.info(
    { apply: values.apply, hostsRequested: hosts.length, ...total },
    'listrepos-diff sweep complete',
  );
  if (values.report !== undefined) {
    const { writeFileSync } = await import('node:fs');
    writeFileSync(
      values.report,
      results
        .toSorted((a, b) => b.classified - a.classified)
        .map((r) => JSON.stringify(r))
        .join('\n') + '\n',
    );
    logger.info({ report: values.report }, 'per-host report written');
  }
  ledger.close();
}

main().catch((err: unknown) => {
  logger.fatal(
    { err: err instanceof Error ? (err.stack ?? err.message) : String(err) },
    'listrepos-diff crashed',
  );
  process.exitCode = 1;
});
