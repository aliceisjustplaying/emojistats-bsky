import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { RetryableError, TerminalFetchError } from './fetcher.js';
import { createRetryPolicy } from './retry.js';
import type { CrawlStats } from './run-state.js';
import type { CrawlTelemetry } from './telemetry.js';
import type { Ledger, RepoRow } from './types.js';

const repo = (preserveExisting: boolean): RepoRow => ({
  did: 'did:plc:test',
  pdsHost: 'example.com',
  status: 'verified',
  rev: null,
  carBytes: null,
  recordsTotal: null,
  postsTotal: 1,
  postsWithEmojis: 1,
  emojiOccurrences: 1,
  rkeyDigest: null,
  attempts: 0,
  error: null,
  enumeratedAt: 0,
  fetchedAt: null,
  loadedAt: 0,
  retryAfter: null,
  preserveExisting,
});

void describe('retry policy preserved recrawls', () => {
  void it('does not downgrade loaded/verified rows on recrawl failures', async () => {
    const ledgerCalls: string[] = [];
    const policy = createRetryPolicy({
      ledger: {
        markRetry: () => ledgerCalls.push('retry'),
        markThrottled: () => ledgerCalls.push('throttled'),
        parkUnreachable: () => ledgerCalls.push('park'),
        markTerminal: () => ledgerCalls.push('terminal'),
      } as unknown as Ledger,
      telemetry: {
        recordEvent: () => undefined,
      } as unknown as CrawlTelemetry,
      stats: { retried: 0 } as CrawlStats,
      hostPressure: {
        record429: () => undefined,
        recordStall: () => undefined,
      } as never,
      hostHealth: {
        recordSuccess: () => undefined,
        recordFailure: () => undefined,
        isDead: () => false,
      } as never,
    });

    await policy.handleRepoError(
      repo(true),
      new RetryableError('socket hang up', { transient: true }),
    );
    await policy.handleRepoError(
      repo(true),
      new TerminalFetchError('deactivated', 'deactivated'),
    );
    await policy.handleRepoError(
      repo(true),
      new RetryableError('http 429', { transient: true }),
    );

    assert.deepEqual(ledgerCalls, []);
  });

  void it('retries preserved RepoNotFound recrawls against a migrated host', async () => {
    const originalFetch = globalThis.fetch;
    const ledgerCalls: string[] = [];
    const testRepo = repo(true);
    testRepo.pdsHost = 'old.example';
    globalThis.fetch = async () =>
      new Response(
        JSON.stringify({
          service: [
            {
              type: 'AtprotoPersonalDataServer',
              serviceEndpoint: 'https://new.bsky.network',
            },
          ],
        }),
        { status: 200 },
      );
    try {
      const policy = createRetryPolicy({
        ledger: {
          updateHost: (did: string, host: string) => {
            ledgerCalls.push(`update:${did}:${host}`);
          },
          markRetry: () => ledgerCalls.push('retry'),
          markThrottled: () => ledgerCalls.push('throttled'),
          parkUnreachable: () => ledgerCalls.push('park'),
          markTerminal: () => ledgerCalls.push('terminal'),
        } as unknown as Ledger,
        telemetry: {
          recordEvent: () => undefined,
        } as unknown as CrawlTelemetry,
        stats: { retried: 0 } as CrawlStats,
        hostPressure: {
          record429: () => undefined,
          recordStall: () => undefined,
        } as never,
        hostHealth: {
          recordSuccess: () => undefined,
          recordFailure: () => undefined,
          isDead: () => false,
        } as never,
      });

      const result = await policy.handleRepoError(
        testRepo,
        new TerminalFetchError(
          'failed',
          'getRepo did:plc:test@old.example: http 400 RepoNotFound',
        ),
      );

      assert.equal(result, 'retry-now');
      assert.equal(testRepo.pdsHost, 'new.bsky.network');
      assert.deepEqual(ledgerCalls, ['update:did:plc:test:new.bsky.network']);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  void it('refuses refreshed PDS pointers to private hosts', async () => {
    const originalFetch = globalThis.fetch;
    const ledgerCalls: string[] = [];
    const healthCalls: string[] = [];
    const testRepo = repo(false);
    testRepo.attempts = 1;
    testRepo.pdsHost = 'old.bsky.network';
    globalThis.fetch = async () =>
      new Response(
        JSON.stringify({
          service: [
            {
              type: 'AtprotoPersonalDataServer',
              serviceEndpoint: 'http://127.0.0.1:2583',
            },
          ],
        }),
        { status: 200 },
      );
    try {
      const policy = createRetryPolicy({
        ledger: {
          addDeadHost: (host: string) => ledgerCalls.push(`dead:${host}`),
          parkUnreachable: (_did: string, error: string) =>
            ledgerCalls.push(`park:${error}`),
          updateHost: (_did: string, host: string) =>
            ledgerCalls.push(`update:${host}`),
          markRetry: () => ledgerCalls.push('retry'),
          markThrottled: () => ledgerCalls.push('throttled'),
          markTerminal: () => ledgerCalls.push('terminal'),
        } as unknown as Ledger,
        telemetry: {
          recordEvent: () => undefined,
        } as unknown as CrawlTelemetry,
        stats: { retried: 0 } as CrawlStats,
        hostPressure: {
          record429: () => undefined,
          recordStall: () => undefined,
        } as never,
        hostHealth: {
          recordSuccess: () => undefined,
          recordFailure: () => undefined,
          markDead: (host: string) => healthCalls.push(`dead:${host}`),
          isDead: () => false,
        } as never,
      });

      const result = await policy.refreshHost(testRepo);

      assert.equal(result, 'host-parked');
      assert.equal(testRepo.pdsHost, 'old.bsky.network');
      assert.deepEqual(healthCalls, ['dead:http://127.0.0.1:2583']);
      assert.deepEqual(ledgerCalls, [
        'dead:http://127.0.0.1:2583',
        'park:host dead: http://127.0.0.1:2583 (non-public PDS address: loopback)',
      ]);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  void it('parks absurd final-sweep retry-after hints immediately', async () => {
    const ledgerCalls: string[] = [];
    const policy = createRetryPolicy({
      ledger: {
        markRetry: () => ledgerCalls.push('retry'),
        markThrottled: () => ledgerCalls.push('throttled'),
        parkUnreachable: () => ledgerCalls.push('park'),
        markTerminal: () => ledgerCalls.push('terminal'),
      } as unknown as Ledger,
      telemetry: {
        recordEvent: () => undefined,
      } as unknown as CrawlTelemetry,
      stats: { retried: 0 } as CrawlStats,
      hostPressure: {
        record429: () => undefined,
        recordStall: () => undefined,
        observeRateLimit: () => undefined,
      } as never,
      hostHealth: {
        recordSuccess: () => undefined,
        recordFailure: () => undefined,
        isDead: () => false,
      } as never,
      finalSweepStopLoss: true,
    });

    await policy.handleRepoError(
      repo(false),
      new RetryableError('getRepo did:plc:test@example.com: http 503', {
        transient: true,
        retryAfterMs: 72 * 60 * 60 * 1000,
      }),
    );

    assert.deepEqual(ledgerCalls, ['park']);
  });

  void it('parks in-flight retry failures once the host is dead for this run', async () => {
    const ledgerCalls: string[] = [];
    let dead = false;
    const policy = createRetryPolicy({
      ledger: {
        markRetry: () => ledgerCalls.push('retry'),
        markThrottled: () => ledgerCalls.push('throttled'),
        parkUnreachable: () => ledgerCalls.push('park'),
        markTerminal: () => ledgerCalls.push('terminal'),
      } as unknown as Ledger,
      telemetry: {
        recordEvent: () => undefined,
      } as unknown as CrawlTelemetry,
      stats: { retried: 0 } as CrawlStats,
      hostPressure: {
        record429: () => undefined,
        recordStall: () => undefined,
        observeRateLimit: () => undefined,
      } as never,
      hostHealth: {
        recordSuccess: () => undefined,
        recordFailure: () => {
          dead = true;
        },
        isDead: () => dead,
      } as never,
    });

    await policy.handleRepoError(
      repo(false),
      new RetryableError('getRepo did:plc:test@example.com: http 503', {
        transient: true,
      }),
    );

    assert.deepEqual(ledgerCalls, ['park']);
  });
});
