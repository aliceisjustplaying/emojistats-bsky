import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { MAX_ATTEMPTS } from './config.js';
import { classifyDeadness, createHostHealth } from './host-health.js';
import { bucketOf, SqliteLedger } from './ledger.js';

void describe('deadness classification', () => {
  void it('matches only failure modes that cannot be per-repo transient', () => {
    assert.equal(
      classifyDeadness(
        'getRepo did@pds.trump.com: fetch failed: getaddrinfo ENOTFOUND pds.trump.com',
      ),
      'dns',
    );
    assert.equal(
      classifyDeadness('getRepo did@plc.surge.sh: http 451'),
      'legal',
    );
    // Resolver trouble is OUR problem, not the host's.
    assert.equal(
      classifyDeadness('fetch failed: getaddrinfo EAI_AGAIN x.example'),
      null,
    );
    assert.equal(classifyDeadness('http 404 RepoNotFound'), null);
    assert.equal(classifyDeadness('http 429'), null);
    assert.equal(classifyDeadness('socket hang up'), null);
  });
});

void describe('host health tripping', () => {
  void it('trips only after 30 consecutive classified failures spanning 30s', (t) => {
    const health = createHostHealth();
    let now = 1_000_000;
    t.mock.method(Date, 'now', () => now);

    for (let i = 0; i < 40; i += 1)
      health.recordFailure('dead.example', 'ENOTFOUND dead.example');
    // 40 consecutive but zero elapsed span: must not trip.
    assert.equal(health.isDead('dead.example'), false);

    now += 31_000;
    health.recordFailure('dead.example', 'ENOTFOUND dead.example');
    assert.equal(health.isDead('dead.example'), true);
    assert.deepEqual(health.takeNewlyTripped(), [
      { host: 'dead.example', kind: 'dns', persist: true },
    ]);
    // Drained: a second take returns nothing.
    assert.deepEqual(health.takeNewlyTripped(), []);
    assert.deepEqual(health.deadHosts(), ['dead.example']);
  });

  void it('any success resets the consecutive count', (t) => {
    const health = createHostHealth();
    let now = 1_000_000;
    t.mock.method(Date, 'now', () => now);

    for (let i = 0; i < 29; i += 1)
      health.recordFailure('flaky.example', 'ENOTFOUND flaky.example');
    health.recordSuccess('flaky.example');
    now += 60_000;
    health.recordFailure('flaky.example', 'ENOTFOUND flaky.example');
    assert.equal(health.isDead('flaky.example'), false);
    assert.deepEqual(health.takeNewlyTripped(), []);
  });

  void it('unclassified failures never count toward deadness', () => {
    const health = createHostHealth();
    for (let i = 0; i < 100; i += 1)
      health.recordFailure('slow.example', 'fetch timeout after 300000ms');
    assert.equal(health.isDead('slow.example'), false);
  });

  void it('final sweep stop-loss parks a host after repeated generic failures over 5 minutes', (t) => {
    const health = createHostHealth({ finalSweepStopLoss: true });
    let now = 1_000_000;
    t.mock.method(Date, 'now', () => now);

    for (let i = 0; i < 4; i += 1)
      health.recordFailure(
        'stuck.example',
        'getRepo did@stuck.example: http 503',
      );
    assert.equal(health.isDead('stuck.example'), false);

    now += 301_000;
    health.recordFailure(
      'stuck.example',
      'getRepo did@stuck.example: http 503',
    );
    assert.equal(health.isDead('stuck.example'), true);
    assert.deepEqual(health.takeNewlyTripped(), [
      {
        host: 'stuck.example',
        kind: 'final-sweep',
        persist: false,
      },
    ]);
  });

  void it('final sweep stop-loss resets on proof of life', (t) => {
    const health = createHostHealth({ finalSweepStopLoss: true });
    let now = 1_000_000;
    t.mock.method(Date, 'now', () => now);

    for (let i = 0; i < 4; i += 1)
      health.recordFailure(
        'recovering.example',
        'fetch timeout after 300000ms',
      );
    health.recordSuccess('recovering.example');
    now += 301_000;
    health.recordFailure('recovering.example', 'fetch timeout after 300000ms');
    assert.equal(health.isDead('recovering.example'), false);
  });

  void it('final sweep stop-loss counts repeated 429s against the host budget', (t) => {
    const health = createHostHealth({ finalSweepStopLoss: true });
    let now = 1_000_000;
    t.mock.method(Date, 'now', () => now);

    for (let i = 0; i < 4; i += 1)
      health.recordFailure(
        'ratelimited.example',
        'getRepo did@ratelimited.example: http 429',
      );
    now += 301_000;
    health.recordFailure(
      'ratelimited.example',
      'getRepo did@ratelimited.example: http 429',
    );
    assert.equal(health.isDead('ratelimited.example'), true);
  });

  void it('classifies a progress-timeout stall as a stall', () => {
    assert.equal(
      classifyDeadness(
        'getRepo did@brid.gy: stalled: no progress for 60000ms during body',
      ),
      'stall',
    );
    assert.equal(
      classifyDeadness(
        'getRepo did@brid.gy: stalled: no progress for 60000ms during connect/headers',
      ),
      'stall',
    );
  });

  void it('parks a host on sustained stalls (6 over 120s), span-guarded', (t) => {
    const health = createHostHealth();
    let now = 1_000_000;
    t.mock.method(Date, 'now', () => now);
    const stall =
      'getRepo did@brid.gy: stalled: no progress for 60000ms during body';

    // 6 stalls but zero elapsed span: meets the count, not the span — no trip.
    for (let i = 0; i < 6; i += 1) health.recordFailure('brid.gy', stall);
    assert.equal(health.isDead('brid.gy'), false);

    // Cross the 120s span: now it parks.
    now += 121_000;
    health.recordFailure('brid.gy', stall);
    assert.equal(health.isDead('brid.gy'), true);
    assert.deepEqual(health.takeNewlyTripped(), [
      { host: 'brid.gy', kind: 'stall', persist: true },
    ]);
  });

  void it('a success resets a stall streak before it can park', (t) => {
    const health = createHostHealth();
    let now = 1_000_000;
    t.mock.method(Date, 'now', () => now);
    const stall = 'stalled: no progress for 60000ms during connect/headers';

    for (let i = 0; i < 6; i += 1)
      health.recordFailure('slowish.example', stall);
    health.recordSuccess('slowish.example'); // proof of life clears the streak
    now += 121_000;
    health.recordFailure('slowish.example', stall);
    assert.equal(health.isDead('slowish.example'), false);
  });

  void it('a kind change restarts the streak (dns and stall do not combine)', (t) => {
    const health = createHostHealth();
    let now = 1_000_000;
    t.mock.method(Date, 'now', () => now);
    // 5 DNS failures spanning >120s, then one stall: the stall opens a fresh
    // streak, so this is NOT 6 consecutive stalls and must not park under the
    // lower stall threshold.
    for (let i = 0; i < 5; i += 1)
      health.recordFailure('mixed.example', 'ENOTFOUND mixed.example');
    now += 121_000;
    health.recordFailure(
      'mixed.example',
      'stalled: no progress for 60000ms during body',
    );
    assert.equal(health.isDead('mixed.example'), false);
  });

  void it('an HTTP response resets a DNS/451 streak; pure network errors do not', (t) => {
    const health = createHostHealth();
    let now = 1_000_000;
    t.mock.method(Date, 'now', () => now);
    for (let i = 0; i < 29; i += 1)
      health.recordFailure('flap.example', 'ENOTFOUND flap.example');
    // 5xx = a server answered: the streak must restart from zero.
    health.recordFailure('flap.example', 'getRepo did@flap.example: http 503');
    now += 60_000;
    health.recordFailure('flap.example', 'ENOTFOUND flap.example');
    assert.equal(health.isDead('flap.example'), false);
    // A timeout proves nothing: streak keeps building across it.
    const health2 = createHostHealth();
    for (let i = 0; i < 29; i += 1)
      health2.recordFailure('dead.example', 'ENOTFOUND dead.example');
    health2.recordFailure('dead.example', 'fetch timeout after 300000ms');
    now += 60_000;
    health2.recordFailure('dead.example', 'ENOTFOUND dead.example');
    assert.equal(health2.isDead('dead.example'), true);
  });

  void it('deadness is sticky against late stray successes', (t) => {
    const health = createHostHealth();
    let now = 1_000_000;
    t.mock.method(Date, 'now', () => now);
    for (let i = 0; i < 30; i += 1)
      health.recordFailure('dead.example', 'ENOTFOUND dead.example');
    now += 31_000;
    health.recordFailure('dead.example', 'ENOTFOUND dead.example');
    assert.equal(health.isDead('dead.example'), true);
    health.recordSuccess('dead.example');
    assert.equal(health.isDead('dead.example'), true);
  });
});

void describe('ledger.parkDeadHostChunk', () => {
  let dir: string;
  beforeEach(() => {
    dir = mkdtempSync(path.join(tmpdir(), 'ledger-park-'));
  });
  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  void it('parks pending and in-budget unreachable rows of the host, nothing else', () => {
    const ledger = new SqliteLedger(path.join(dir, 'ledger.sqlite'));
    ledger.upsertPending('did:plc:aaa1', 'dead.example');
    ledger.upsertPending('did:plc:aaa2', 'dead.example');
    ledger.upsertPending('did:plc:aaa3', 'dead.example');
    ledger.upsertPending('did:plc:bbb1', 'alive.example');
    // In-budget unreachable on the dead host: parks too.
    ledger.markFetching('did:plc:aaa2');
    ledger.markRetry('did:plc:aaa2', 'timeout', 60_000);
    // Loaded rows are untouchable.
    ledger.markFetching('did:plc:aaa3');
    ledger.markLoaded('did:plc:aaa3', {
      rev: 'r',
      carBytes: 1,
      recordsTotal: 1,
      postsTotal: 1,
      postsWithEmojis: 0,
      emojiOccurrences: 0,
      rkeyDigest: null,
    });

    let parked = 0;
    for (;;) {
      const changes = ledger.parkDeadHostChunk('dead.example', 'host dead', 1);
      parked += changes;
      if (changes < 1) break;
    }
    parked += ledger.parkDeadHostUnreachable('dead.example', 'host dead');
    assert.equal(parked, 2);

    const aaa1 = ledger.getRepo('did:plc:aaa1')!;
    assert.equal(aaa1.status, 'unreachable');
    assert.equal(aaa1.attempts, MAX_ATTEMPTS);
    assert.equal(aaa1.retryAfter, null);
    assert.equal(ledger.getRepo('did:plc:aaa2')!.attempts, MAX_ATTEMPTS);
    assert.equal(ledger.getRepo('did:plc:aaa3')!.status, 'loaded');
    assert.equal(ledger.getRepo('did:plc:bbb1')!.status, 'pending');
    // Parked rows are out of budget: never offered to claims again this run.
    assert.deepEqual(
      ledger.listClaimable(10).map((r) => r.did),
      ['did:plc:bbb1'],
    );
    // Conditional classify: claimable rows flip, progressed rows are immune.
    assert.equal(
      ledger.markTerminalIfClaimable('did:plc:bbb1', 'failed', 'spam'),
      true,
    );
    assert.equal(
      ledger.markTerminalIfClaimable('did:plc:aaa3', 'failed', 'spam'),
      false,
    );
    assert.equal(ledger.getRepo('did:plc:aaa3')!.status, 'loaded');
    ledger.close();
  });

  void it('dead-host registry round-trips and enumeration inserts born-parked rows', () => {
    const ledger = new SqliteLedger(path.join(dir, 'registry.sqlite'));
    assert.deepEqual(ledger.getDeadHosts(), []);
    ledger.addDeadHost('dead.example');
    ledger.addDeadHost('dead.example');
    ledger.addDeadHost('another.example');
    assert.deepEqual(ledger.getDeadHosts(), [
      'another.example',
      'dead.example',
    ]);

    ledger.upsertParked('did:plc:born1', 'dead.example', 'host dead');
    const born = ledger.getRepo('did:plc:born1')!;
    assert.equal(born.status, 'unreachable');
    assert.equal(born.attempts, MAX_ATTEMPTS);
    // Existing pending rows convert; progressed rows are untouchable.
    ledger.upsertPending('did:plc:was-pending', 'dead.example');
    ledger.upsertParked('did:plc:was-pending', 'dead.example', 'host dead');
    assert.equal(ledger.getRepo('did:plc:was-pending')!.status, 'unreachable');
    ledger.markFetching('did:plc:born1');
    assert.equal(ledger.getRepo('did:plc:born1')!.status, 'fetching');
    ledger.upsertParked('did:plc:born1', 'dead.example', 'host dead');
    assert.equal(ledger.getRepo('did:plc:born1')!.status, 'fetching');
    assert.deepEqual(ledger.listClaimable(10), []);
    ledger.close();
  });

  void it('final-sweep dead-host registry is scoped to one run id', () => {
    const ledger = new SqliteLedger(
      path.join(dir, 'final-sweep-registry.sqlite'),
    );
    assert.deepEqual(ledger.getFinalSweepDeadHosts('run-1'), []);
    ledger.addFinalSweepDeadHost('stall.example', 'run-1');
    ledger.addFinalSweepDeadHost('stall.example', 'run-1');
    ledger.addFinalSweepDeadHost('retry.example', 'run-1');
    assert.deepEqual(ledger.getFinalSweepDeadHosts('run-1'), [
      'retry.example',
      'stall.example',
    ]);
    assert.deepEqual(ledger.getFinalSweepDeadHosts('run-2'), []);

    ledger.removeFinalSweepDeadHost('stall.example', 'run-2');
    assert.deepEqual(ledger.getFinalSweepDeadHosts('run-1'), [
      'retry.example',
      'stall.example',
    ]);

    ledger.removeFinalSweepDeadHost('stall.example', 'run-1');
    assert.deepEqual(ledger.getFinalSweepDeadHosts('run-1'), ['retry.example']);

    ledger.addFinalSweepDeadHost('fresh.example', 'run-2');
    assert.deepEqual(ledger.getFinalSweepDeadHosts('run-1'), []);
    assert.deepEqual(ledger.getFinalSweepDeadHosts('run-2'), ['fresh.example']);
    ledger.close();
  });

  void it('revive drops a host from the registry and re-arms only its rows', () => {
    const ledger = new SqliteLedger(path.join(dir, 'revive.sqlite'));
    ledger.addDeadHost('brid.gy');
    ledger.addDeadHost('dns.dead');
    // Two hosts' rows parked out-of-budget (born parked, attempts = MAX).
    ledger.upsertParked('did:plc:bridge1', 'brid.gy', 'host skipped');
    ledger.upsertParked('did:plc:dns1', 'dns.dead', 'host dead');
    assert.equal(ledger.getRepo('did:plc:bridge1')!.attempts, MAX_ATTEMPTS);
    assert.deepEqual(ledger.listClaimable(10), []);

    // Revive brid.gy only: drop the verdict, then re-arm its rows.
    ledger.removeDeadHost('brid.gy');
    assert.deepEqual(ledger.getDeadHosts(), ['dns.dead']);
    assert.equal(ledger.resetUnreachableForHost('brid.gy'), 1);

    const bridge = ledger.getRepo('did:plc:bridge1')!;
    assert.equal(bridge.status, 'unreachable');
    assert.equal(bridge.attempts, 0);
    // Re-armed → claimable again; the genuinely-dead host is untouched.
    assert.deepEqual(
      ledger.listClaimable(10).map((r) => r.did),
      ['did:plc:bridge1'],
    );
    assert.equal(ledger.getRepo('did:plc:dns1')!.attempts, MAX_ATTEMPTS);

    // Reviving a host with no parked rows resets nothing; removing an absent
    // host from the registry is a no-op.
    assert.equal(ledger.resetUnreachableForHost('no.such.host'), 0);
    ledger.removeDeadHost('never.there');
    assert.deepEqual(ledger.getDeadHosts(), ['dns.dead']);
    ledger.close();
  });

  void it('final sweep re-arms parked rows except dead-host registry rows', () => {
    const ledger = new SqliteLedger(path.join(dir, 'final-sweep.sqlite'));
    const runId = 'run-1';
    ledger.addDeadHost('dns.dead');
    ledger.addFinalSweepDeadHost('stall.dead', runId);
    ledger.upsertParked('did:plc:dns1', 'dns.dead', 'host dead');
    ledger.upsertParked('did:plc:stall1', 'stall.dead', 'host stalled');
    ledger.upsertParked('did:plc:alive1', 'alive.example', 'timed out');

    const reset = ledger.resetUnreachableAttempts([
      ...ledger.getDeadHosts(),
      ...ledger.getFinalSweepDeadHosts(runId),
    ]);
    assert.equal(reset, 1);

    const dead = ledger.getRepo('did:plc:dns1')!;
    assert.equal(dead.status, 'unreachable');
    assert.equal(dead.attempts, MAX_ATTEMPTS);
    assert.equal(dead.retryAfter, null);

    const stalled = ledger.getRepo('did:plc:stall1')!;
    assert.equal(stalled.status, 'unreachable');
    assert.equal(stalled.attempts, MAX_ATTEMPTS);
    assert.equal(stalled.retryAfter, null);

    const alive = ledger.getRepo('did:plc:alive1')!;
    assert.equal(alive.status, 'unreachable');
    assert.equal(alive.attempts, 0);
    assert.equal(alive.retryAfter, 0);
    assert.deepEqual(
      ledger.listClaimable(10).map((r) => r.did),
      ['did:plc:alive1'],
    );
    ledger.close();
  });

  void it('is shard-scoped like every other claim-path write', () => {
    // Find dids landing in bucket 0 and elsewhere so the test is deterministic.
    const dids: string[] = [];
    for (let i = 0; dids.length < 2 && i < 1000; i += 1) {
      const did = `did:plc:shardtest${i}`;
      if (bucketOf(did) === 0 && dids.length === 0) dids.push(did);
      else if (bucketOf(did) === 1 && dids.length === 1) dids.push(did);
    }
    const [inShard, outShard] = dids;
    const dbPath = path.join(dir, 'sharded.sqlite');
    const writer = new SqliteLedger(dbPath);
    writer.upsertPending(inShard, 'dead.example');
    writer.upsertPending(outShard, 'dead.example');
    writer.close();

    const shard0 = new SqliteLedger(dbPath, { shards: 6, shardIndex: 0 });
    assert.equal(shard0.parkDeadHostChunk('dead.example', 'host dead', 100), 1);
    assert.equal(shard0.getRepo(inShard)!.status, 'unreachable');
    assert.equal(shard0.getRepo(outShard)!.status, 'pending');
    shard0.close();
  });
});
