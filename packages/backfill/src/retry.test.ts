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
              serviceEndpoint: 'https://new.example',
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
      assert.equal(testRepo.pdsHost, 'new.example');
      assert.deepEqual(ledgerCalls, ['update:did:plc:test:new.example']);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
