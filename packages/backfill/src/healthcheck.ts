/**
 * One-shot PDS healthcheck sweep (operator tool).
 *
 * The organic dead-host detector (host-health.ts) only trips hosts the claim
 * path actually hammers; a long tail of small dead PDSes instead burns five
 * retry attempts per repo across hours of waves. This sweep probes every
 * distinct host that still owns pending rows and, with --park, bulk-parks the
 * provably-dead ones (DNS NXDOMAIN / HTTP 451) onto the final-sweep list up
 * front — the healthcheck-first step the original backfill design had.
 *
 * Probe = GET /xrpc/com.atproto.server.describeServer. ANY HTTP response
 * counts as alive (404/429/500 prove a listener); only ENOTFOUND and 451
 * qualify as dead, mirroring host-health.ts. Timeouts/refusals are reported
 * but never parked: a wedged host can come back, and retry waves already
 * handle it.
 *
 * Run it on a crawl box from packages/backfill:
 *   bunx tsx src/healthcheck.ts            # report only
 *   bunx tsx src/healthcheck.ts --park     # report + park dead hosts
 * Safe alongside a running crawler (chunked writes, busy-timeout tolerant),
 * cheapest during a rollout stop window.
 */

import { parseArgs } from 'node:util';

import { LEDGER_DB_PATH, USER_AGENT } from './config.js';
import { SqliteLedger } from './ledger.js';
import logger from './logger.js';

const PROBE_CONCURRENCY = 50;
const PROBE_TIMEOUT_MS = 10_000;
const PARK_CHUNK = 20_000;

type Verdict = 'alive' | 'dns-dead' | 'legal-dead' | 'unresponsive';

async function probe(host: string): Promise<Verdict> {
  // Ledger hosts are bare hostnames except the rare scheme-prefixed http PDS.
  const base = host.includes('://') ? host : `https://${host}`;
  try {
    const res = await fetch(`${base}/xrpc/com.atproto.server.describeServer`, {
      headers: { 'user-agent': USER_AGENT },
      signal: AbortSignal.timeout(PROBE_TIMEOUT_MS),
      redirect: 'manual',
    });
    return res.status === 451 ? 'legal-dead' : 'alive';
  } catch (err) {
    const cause = err instanceof Error ? err.cause : undefined;
    const message =
      err instanceof Error
        ? `${err.message} ${cause instanceof Error ? cause.message : ''}`
        : '';
    if (message.includes('ENOTFOUND')) return 'dns-dead';
    return 'unresponsive';
  }
}

async function main(): Promise<void> {
  const { values } = parseArgs({
    options: { park: { type: 'boolean', default: false } },
  });

  // shards=1 on purpose: probing is per-host, parking should cover the whole
  // ledger file — foreign-shard rows are equally dead and every box reaches
  // the same per-host verdicts, so cross-box ledgers stay consistent.
  const ledger = new SqliteLedger(LEDGER_DB_PATH, { busyTimeoutMs: 60_000 });
  const hosts = ledger.pendingHostCounts();
  logger.info(
    { hosts: hosts.length, pending: hosts.reduce((a, h) => a + h.pending, 0) },
    'probing every host with pending rows',
  );

  const verdicts = new Map<string, Verdict>();
  let cursor = 0;
  await Promise.all(
    Array.from({ length: PROBE_CONCURRENCY }, async () => {
      for (;;) {
        const next = hosts[cursor];
        cursor += 1;
        if (next === undefined) return;
        verdicts.set(next.host, await probe(next.host));
      }
    }),
  );

  const byVerdict = (v: Verdict) =>
    hosts.filter((h) => verdicts.get(h.host) === v);
  const dead = [...byVerdict('dns-dead'), ...byVerdict('legal-dead')];
  for (const v of [
    'alive',
    'unresponsive',
    'dns-dead',
    'legal-dead',
  ] as const) {
    const matched = byVerdict(v);
    logger.info(
      {
        verdict: v,
        hosts: matched.length,
        pending: matched.reduce((a, h) => a + h.pending, 0),
        top: matched.slice(0, 5).map((h) => `${h.host}:${h.pending}`),
      },
      'healthcheck verdict',
    );
  }

  if (!values.park) {
    if (dead.length > 0)
      logger.warn(
        { hosts: dead.length },
        'dead hosts found; re-run with --park to move their rows to the final-sweep list',
      );
    ledger.close();
    return;
  }

  for (const { host } of dead) {
    const error = `host dead: ${host} (healthcheck sweep)`;
    let parked = 0;
    for (;;) {
      const changes = ledger.parkDeadHostChunk(host, error, PARK_CHUNK);
      parked += changes;
      if (changes < PARK_CHUNK) break;
      await new Promise((resolve) => {
        setTimeout(resolve, 50);
      });
    }
    parked += ledger.parkDeadHostUnreachable(host, error);
    logger.warn({ host, parked }, 'healthcheck: dead host parked');
  }
  ledger.close();
}

main().catch((err: unknown) => {
  logger.fatal(
    { err: err instanceof Error ? (err.stack ?? err.message) : String(err) },
    'healthcheck crashed',
  );
  process.exitCode = 1;
});
