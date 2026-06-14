import { createServerFn } from '@tanstack/react-start';
import { REPO_STATUSES, type RepoStatus } from 'backfill/types';

import { chQuery, num } from './clickhouse';

// Crawl telemetry lives in ClickHouse (backfill_progress / backfill_repo_events,
// schema in packages/ingest/src/clickhouse/schema.sql): the crawler runs on its
// own box and writes snapshots every TELEMETRY_INTERVAL_MS, so ClickHouse is the
// shared bus — there is no local ledger file to read.
//
// backfill_progress rows are cumulative per (run_id, shard): the current state
// is the latest row per shard summed across shards; rates are deltas over time.
//
// Statuses come from THE registry (backfill/types REPO_STATUSES), which the
// backfill_progress column set also derives from.

export type BackfillRepoStatus = RepoStatus;

const ISSUE_EVENTS = [
  'failed',
  'quarantined',
  'unreachable',
  'takendown',
  'deactivated',
] as const;
const TERMINAL_EVENTS = ['loaded', 'empty', ...ISSUE_EVENTS] as const;
const V1_RECRAWL_TARGET_REPOS = 3_208_370;
const V1_RECRAWL_TARGET_POSTS = 478_676_984;
const V1_RECRAWL_RUN_PREFIX = 'v1-recrawl';

/**
 * Window for the rolling repos-per-minute estimate. Must span multiple shard
 * flush cycles: a single shard silent for the whole window zeroes its rate
 * contribution while its remaining repos still count, understating the rate
 * and inflating the ETA.
 */
const RATE_WINDOW_MINUTES = 10;
/** Telemetry older than this means the crawl is idle (interval is ~10s). */
const IDLE_AFTER_SECONDS = 60;
/** Target point count for the throughput timeline. */
const TIMELINE_POINTS = 200;

/** Compact crawl snapshot for the front-page panel. */
export interface BackfillStatus {
  statusCounts: Record<BackfillRepoStatus, number>;
  /** Repos resolved per minute (rolling, last RATE_WINDOW_MINUTES). */
  reposPerMin: number;
  /** Posts written to ClickHouse by the backfill so far. */
  postsLoaded: number;
  /** Estimated hours until the crawl completes, null when unknown. */
  etaHours: number | null;
  /** Most recent crawler error, null when none recorded. */
  lastError: string | null;
}

/** Full crawl snapshot for the /backfill hero + status breakdown. */
export interface BackfillOverview {
  generatedAt: string;
  runId: string;
  shards: number;
  /** Seconds since the stalest shard's newest telemetry row. */
  freshnessSeconds: number;
  /** Seconds since the freshest shard's newest telemetry row. */
  latestFreshnessSeconds: number;
  /** True while telemetry is arriving (freshness below the idle cutoff). */
  active: boolean;
  /** Seconds since each shard's newest telemetry row, stalest first. */
  shardFreshness: Array<{ shard: string; ageSeconds: number }>;
  totalEnumerated: number;
  /** Repos out of pending/fetching — every terminal status incl. unreachable. */
  resolved: number;
  /** Unreachable repos parked for retry waves + the final sweep. */
  parkedUnreachable: number;
  postsLoaded: number;
  bytesDownloaded: number;
  reposPerMin: number;
  rowsPerSec: number;
  inFlight: number;
  etaHours: number | null;
  lastError: string | null;
  statusCounts: Record<BackfillRepoStatus, number>;
}

type SnapshotRow = Record<BackfillRepoStatus, string> & {
  shard: string;
  latest_ts: string;
  in_flight: string;
};

function chTsToDate(chUtcDateTime: string): Date {
  return new Date(`${chUtcDateTime.replace(' ', 'T')}Z`);
}

/** One argMax column per status, derived from the registry. */
const STATUS_ARGMAX_SQL = REPO_STATUSES.map(
  (status) => `argMax(${status}, ts) AS ${status}`,
).join(',\n        ');

/**
 * Statuses that still represent work. Unreachable PARKS for retry waves
 * rather than resolving, so it stays out of the throughput rate. The hero
 * progress is about active crawl budget, so parked unreachable rows count as
 * done there and pending + fetching are the only remaining statuses.
 */
const UNRESOLVED_STATUSES = new Set<RepoStatus>([
  'pending',
  'fetching',
  'unreachable',
]);

/** Sum of every genuinely settled status — the "resolved" measure. */
const RESOLVED_SUM_SQL = REPO_STATUSES.filter(
  (status) => !UNRESOLVED_STATUSES.has(status),
).join(' + ');

async function fetchOverview(): Promise<BackfillOverview | null> {
  const runRows = await chQuery<{ run_id: string }>(`
    SELECT run_id FROM backfill_progress ORDER BY ts DESC LIMIT 1
  `);
  const runId = runRows[0]?.run_id;
  if (runId === undefined) return null;

  const [snapshot, totals, rate, lastErrorRows] = await Promise.all([
    chQuery<SnapshotRow>(
      `
      SELECT
        shard,
        -- aliased away from plain "ts": the alias would shadow the column
        -- inside the argMax calls (ILLEGAL_AGGREGATION)
        max(ts) AS latest_ts,
        ${STATUS_ARGMAX_SQL},
        argMax(in_flight, ts) AS in_flight
      FROM backfill_progress
      WHERE run_id = {run:String}
      GROUP BY shard
    `,
      { run: runId },
    ),
    // Posts/bytes/row-rate come from the append-only events table, NOT the
    // crawler's in-process gauges: those reset to zero on every service
    // restart (a fleet-tuning afternoon made "data downloaded" lurch
    // backwards repeatedly), and since batched loading the instantaneous
    // rows_per_sec gauge reads 0 between 15 s flushes. Events survive both.
    // Aliases must not collide with source columns (posts, car_bytes): the
    // alias would shadow the column inside the sibling aggregates
    // (ILLEGAL_AGGREGATION), same trap as latest_ts above.
    chQuery<{
      posts_total: string;
      bytes_total: string;
      recent_posts: string;
    }>(`
      SELECT
        sumIf(posts, event = 'loaded') AS posts_total,
        sum(car_bytes) AS bytes_total,
        sumIf(
          posts,
          event = 'loaded' AND ts >= now() - INTERVAL ${RATE_WINDOW_MINUTES} MINUTE
        ) AS recent_posts
      FROM backfill_repo_events
    `),
    // Per-shard rate = delta / the shard's ACTUAL covered minutes, not the
    // nominal window. Progress snapshots are replace-and-retry under load,
    // so a "10-minute" window typically holds 7-9.5 minutes of rows per
    // shard — dividing the delta by the flat window under-read the fleet
    // rate by 20-30% and inflated the ETA accordingly (observed 22.3k vs
    // true 26.2k repos/min, 2026-06-12 evening).
    chQuery<{ repos_per_min: string }>(
      `
      SELECT sum(rpm) AS repos_per_min
      FROM (
        SELECT
          (argMax(${RESOLVED_SUM_SQL}, ts) - argMin(${RESOLVED_SUM_SQL}, ts))
          / greatest(dateDiff('second', min(ts), max(ts)) / 60, 1) AS rpm
        FROM backfill_progress
        WHERE run_id = {run:String}
          AND ts >= now() - INTERVAL ${RATE_WINDOW_MINUTES} MINUTE
        GROUP BY shard
      )
    `,
      { run: runId },
    ),
    chQuery<{ did: string; error: string }>(`
      SELECT did, error
      FROM backfill_repo_events
      WHERE event IN ('failed', 'quarantined') AND error != ''
      ORDER BY ts DESC
      LIMIT 1
    `),
  ]);
  if (snapshot.length === 0) return null;

  const statusCounts = Object.fromEntries(
    REPO_STATUSES.map((status) => [status, 0]),
  ) as Record<BackfillRepoStatus, number>;
  let inFlight = 0;
  for (const row of snapshot) {
    for (const status of REPO_STATUSES)
      statusCounts[status] += num(row[status]);
    inFlight += num(row.in_flight);
  }
  const postsLoaded = num(totals[0]?.posts_total);
  const bytesDownloaded = num(totals[0]?.bytes_total);
  const rowsPerSec = num(totals[0]?.recent_posts) / (RATE_WINDOW_MINUTES * 60);

  const totalEnumerated = REPO_STATUSES.reduce(
    (acc, status) => acc + statusCounts[status],
    0,
  );
  // Parked unreachable repos are out of the active crawl budget; they still
  // display separately because the final sweep/retry wave may revisit them.
  const remaining = statusCounts.pending + statusCounts.fetching;
  const resolved = totalEnumerated - remaining;
  const reposPerMin = num(rate[0]?.repos_per_min);
  const now = Date.now();
  const shardFreshness = snapshot
    .map((row) => ({
      shard: row.shard,
      ageSeconds: Math.max(
        0,
        Math.round((now - chTsToDate(row.latest_ts).getTime()) / 1000),
      ),
    }))
    .toSorted((a, b) => b.ageSeconds - a.ageSeconds);
  const freshnessSeconds = shardFreshness[0]?.ageSeconds ?? 0;
  const latestFreshnessSeconds =
    shardFreshness[shardFreshness.length - 1]?.ageSeconds ?? freshnessSeconds;

  return {
    generatedAt: new Date().toISOString(),
    runId,
    shards: snapshot.length,
    freshnessSeconds,
    latestFreshnessSeconds,
    active: latestFreshnessSeconds < IDLE_AFTER_SECONDS,
    shardFreshness,
    totalEnumerated,
    resolved,
    parkedUnreachable: statusCounts.unreachable,
    postsLoaded,
    bytesDownloaded,
    reposPerMin,
    rowsPerSec,
    inFlight,
    etaHours: reposPerMin > 0 ? remaining / reposPerMin / 60 : null,
    lastError: lastErrorRows[0]
      ? `${lastErrorRows[0].did}: ${lastErrorRows[0].error}`
      : null,
    statusCounts,
  };
}

export const getBackfillStatus = createServerFn().handler(
  async (): Promise<BackfillStatus | null> => {
    const overview = await fetchOverview();
    if (overview === null) return null;
    return {
      statusCounts: overview.statusCounts,
      reposPerMin: overview.reposPerMin,
      postsLoaded: overview.postsLoaded,
      etaHours: overview.etaHours,
      lastError: overview.lastError,
    };
  },
);

export const getBackfillOverview = createServerFn().handler(
  (): Promise<BackfillOverview | null> => fetchOverview(),
);

export interface BackfillTimelinePoint {
  ts: string;
  postsPerMin: number;
  bytesPerMin: number;
  rowsPerSec: number;
}

export interface BackfillTimeline {
  stepSeconds: number;
  points: Array<BackfillTimelinePoint>;
}

export const getBackfillTimeline = createServerFn().handler(
  async (): Promise<BackfillTimeline | null> => {
    // Two steps, not a scalar subquery: ClickHouse throws on scalar subqueries
    // over an empty table, and "no telemetry yet" must render, not 500.
    const latest = await chQuery<{ run_id: string }>(`
      SELECT run_id FROM backfill_progress ORDER BY ts DESC LIMIT 1
    `);
    if (latest[0] === undefined) return null;
    const runRows = await chQuery<{
      run_id: string;
      min_ts: string;
      max_ts: string;
    }>(
      `
      SELECT run_id, min(ts) AS min_ts, max(ts) AS max_ts
      FROM backfill_progress
      WHERE run_id = {run:String}
      GROUP BY run_id
    `,
      { run: latest[0].run_id },
    );
    const run = runRows[0];
    if (run === undefined) return null;

    const spanSeconds = Math.max(
      0,
      (chTsToDate(run.max_ts).getTime() - chTsToDate(run.min_ts).getTime()) /
        1000,
    );
    const stepSeconds = Math.max(10, Math.ceil(spanSeconds / TIMELINE_POINTS));

    // Cumulative counters per shard → bucket per shard (max within bucket),
    // sum across shards, then deltas between consecutive buckets in JS.
    // Rates are normalized by the latest sample ts per bucket, not the bucket
    // start: buckets hold a varying number of telemetry samples whenever the
    // step isn't a multiple of the report interval, and dividing by the bucket
    // width would alias that into a sawtooth.
    const buckets = await chQuery<{
      bucket: string;
      latest_ts: string;
      posts_loaded: string;
      bytes_downloaded: string;
      rows_per_sec: number;
    }>(
      `
      SELECT
        bucket,
        max(shard_latest_ts) AS latest_ts,
        sum(p) AS posts_loaded,
        sum(b) AS bytes_downloaded,
        sum(r) AS rows_per_sec
      FROM (
        SELECT
          toStartOfInterval(ts, toIntervalSecond({step:UInt32})) AS bucket,
          shard,
          max(ts) AS shard_latest_ts,
          max(posts_loaded) AS p,
          max(bytes_downloaded) AS b,
          avg(rows_per_sec) AS r
        FROM backfill_progress
        WHERE run_id = {run:String}
        GROUP BY bucket, shard
      )
      GROUP BY bucket
      ORDER BY bucket
    `,
      { run: run.run_id, step: String(stepSeconds) },
    );

    const points: Array<BackfillTimelinePoint> = [];
    for (let i = 1; i < buckets.length; i += 1) {
      const prev = buckets[i - 1];
      const curr = buckets[i];
      const minutes =
        (chTsToDate(curr.latest_ts).getTime() -
          chTsToDate(prev.latest_ts).getTime()) /
        60_000;
      if (minutes <= 0) continue;
      points.push({
        ts: curr.bucket,
        postsPerMin: Math.max(
          0,
          (num(curr.posts_loaded) - num(prev.posts_loaded)) / minutes,
        ),
        bytesPerMin: Math.max(
          0,
          (num(curr.bytes_downloaded) - num(prev.bytes_downloaded)) / minutes,
        ),
        rowsPerSec: num(curr.rows_per_sec),
      });
    }
    return { stepSeconds, points };
  },
);

export interface BackfillHistogram {
  months: Array<{ month: string; posts: number }>;
  totalPosts: number;
}

export const getBackfillHistogram = createServerFn().handler(
  async (): Promise<BackfillHistogram> => {
    // Src-agnostic on purpose: live can win the ReplacingMergeTree merge for
    // posts that exist in both paths, so a src filter undercounts recovered
    // history at the crawl boundary. "What's in the database per month" IS the
    // coverage this chart exists to show — historical months only ever fill
    // via the crawl anyway.
    // Reads posts_hourly, not raw posts: a month GROUP BY over the raw table
    // is a full scan that grows with the crawl (it OOM'd mid-backfill at 8M
    // rows; it would never survive 2.9B). Aggregate counts over-count dups
    // until the rebuild timers settle them — the footer says as much.
    const rows = await chQuery<{ month: string; posts: string }>(`
      SELECT toStartOfMonth(hour) AS month, sum(posts) AS posts
      FROM posts_hourly
      WHERE hour >= toDateTime('2023-01-01 00:00:00', 'UTC')
      GROUP BY month
      ORDER BY month
    `);

    // Zero-fill 2023-01 → current month so gaps render as gaps, not jumps.
    const byMonth = new Map(rows.map((row) => [row.month, num(row.posts)]));
    const months: Array<{ month: string; posts: number }> = [];
    const now = new Date();
    const last = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1);
    for (
      let cursor = Date.UTC(2023, 0, 1);
      cursor <= last;
      cursor = new Date(cursor).setUTCMonth(new Date(cursor).getUTCMonth() + 1)
    ) {
      const key = new Date(cursor).toISOString().slice(0, 10);
      months.push({ month: key, posts: byMonth.get(key) ?? 0 });
    }

    return {
      months,
      totalPosts: months.reduce((acc, m) => acc + m.posts, 0),
    };
  },
);

export interface BackfillHost {
  host: string;
  total: number;
  loaded: number;
  empty: number;
  issues: number;
  bytes: number;
  avgPostsPerRepo: number;
}

export const getBackfillHosts = createServerFn().handler(
  async (): Promise<Array<BackfillHost>> => {
    const rows = await chQuery<{
      host: string;
      total: string;
      loaded: string;
      empty: string;
      issues: string;
      bytes: string;
      avg_posts: number | null;
    }>(`
      SELECT
        pds_host AS host,
        countIf(event IN (${TERMINAL_EVENTS.map((e) => `'${e}'`).join(', ')})) AS total,
        countIf(event = 'loaded') AS loaded,
        countIf(event = 'empty') AS empty,
        countIf(event IN (${ISSUE_EVENTS.map((e) => `'${e}'`).join(', ')})) AS issues,
        sum(car_bytes) AS bytes,
        avgIf(posts, event = 'loaded') AS avg_posts
      FROM backfill_repo_events
      GROUP BY pds_host
      ORDER BY total DESC, loaded DESC
      LIMIT 12
    `);
    return rows.map((row) => ({
      host: row.host,
      total: num(row.total),
      loaded: num(row.loaded),
      empty: num(row.empty),
      issues: num(row.issues),
      bytes: num(row.bytes),
      avgPostsPerRepo: num(row.avg_posts),
    }));
  },
);

export interface BackfillRecrawlStatus {
  generatedAt: string;
  targetRepos: number;
  targetPosts: number;
  runId: string | null;
  active: boolean;
  shards: number;
  freshnessSeconds: number | null;
  reposProcessed: number;
  remainingRepos: number;
  reposPerMin: number;
  etaHours: number | null;
}

export interface BackfillVerifyStatus {
  generatedAt: string;
  runId: string | null;
  active: boolean;
  shards: number;
  freshnessSeconds: number | null;
  phase: string;
  reposTotal: number;
  reposChecked: number;
  exact: number;
  loose: number;
  mismatches: number;
  looseEmitted: number;
  sampleChecked: number;
  sampleFailures: number;
  doneShards: number;
  failedShards: number;
  error: string | null;
}

export const getBackfillVerifyStatus = createServerFn().handler(
  async (): Promise<BackfillVerifyStatus> => {
    const exists = await chQuery<{ n: string }>(`
      SELECT count() AS n
      FROM system.tables
      WHERE database = currentDatabase()
        AND name = 'backfill_verify_progress'
    `);
    if (num(exists[0]?.n) === 0) {
      return {
        generatedAt: new Date().toISOString(),
        runId: null,
        active: false,
        shards: 0,
        freshnessSeconds: null,
        phase: 'ready',
        reposTotal: 0,
        reposChecked: 0,
        exact: 0,
        loose: 0,
        mismatches: 0,
        looseEmitted: 0,
        sampleChecked: 0,
        sampleFailures: 0,
        doneShards: 0,
        failedShards: 0,
        error: null,
      };
    }

    const runRows = await chQuery<{ run_id: string }>(`
      SELECT run_id
      FROM backfill_verify_progress
      ORDER BY ts DESC
      LIMIT 1
    `);
    const runId = runRows[0]?.run_id;
    if (runId === undefined) {
      return {
        generatedAt: new Date().toISOString(),
        runId: null,
        active: false,
        shards: 0,
        freshnessSeconds: null,
        phase: 'ready',
        reposTotal: 0,
        reposChecked: 0,
        exact: 0,
        loose: 0,
        mismatches: 0,
        looseEmitted: 0,
        sampleChecked: 0,
        sampleFailures: 0,
        doneShards: 0,
        failedShards: 0,
        error: null,
      };
    }

    const rows = await chQuery<{
      shard: string;
      latest_ts: string;
      phase: string;
      repos_total: string;
      repos_checked: string;
      exact: string;
      loose: string;
      mismatches: string;
      loose_emitted: string;
      sample_checked: string;
      sample_failures: string;
      done: string;
      error: string;
    }>(
      `
      SELECT
        shard,
        max(ts) AS latest_ts,
        argMax(phase, ts) AS phase,
        argMax(repos_total, ts) AS repos_total,
        argMax(repos_checked, ts) AS repos_checked,
        argMax(exact, ts) AS exact,
        argMax(loose, ts) AS loose,
        argMax(mismatches, ts) AS mismatches,
        argMax(loose_emitted, ts) AS loose_emitted,
        argMax(sample_checked, ts) AS sample_checked,
        argMax(sample_failures, ts) AS sample_failures,
        argMax(done, ts) AS done,
        argMax(error, ts) AS error
      FROM backfill_verify_progress
      WHERE run_id = {run:String}
      GROUP BY shard
    `,
      { run: runId },
    );

    let newestTs = 0;
    let reposTotal = 0;
    let reposChecked = 0;
    let exact = 0;
    let loose = 0;
    let mismatches = 0;
    let looseEmitted = 0;
    let sampleChecked = 0;
    let sampleFailures = 0;
    let doneShards = 0;
    let failedShards = 0;
    const phases = new Set<string>();
    const errors: string[] = [];
    for (const row of rows) {
      newestTs = Math.max(newestTs, chTsToDate(row.latest_ts).getTime());
      reposTotal += num(row.repos_total);
      reposChecked += num(row.repos_checked);
      exact += num(row.exact);
      loose += num(row.loose);
      mismatches += num(row.mismatches);
      looseEmitted += num(row.loose_emitted);
      sampleChecked += num(row.sample_checked);
      sampleFailures += num(row.sample_failures);
      if (num(row.done) > 0) doneShards += 1;
      if (row.phase === 'failed' || row.error !== '') failedShards += 1;
      phases.add(row.phase);
      if (row.error !== '') errors.push(`${row.shard}: ${row.error}`);
    }
    const freshnessSeconds =
      newestTs > 0
        ? Math.max(0, Math.round((Date.now() - newestTs) / 1000))
        : null;
    const active =
      freshnessSeconds !== null &&
      freshnessSeconds < IDLE_AFTER_SECONDS &&
      doneShards < rows.length;
    return {
      generatedAt: new Date().toISOString(),
      runId,
      active,
      shards: rows.length,
      freshnessSeconds,
      phase:
        phases.size === 1
          ? [...phases][0]
          : active
            ? 'mixed'
            : doneShards === rows.length
              ? 'finished'
              : 'idle',
      reposTotal,
      reposChecked,
      exact,
      loose,
      mismatches,
      looseEmitted,
      sampleChecked,
      sampleFailures,
      doneShards,
      failedShards,
      error: errors[0] ?? null,
    };
  },
);

export const getBackfillRecrawlStatus = createServerFn().handler(
  async (): Promise<BackfillRecrawlStatus> => {
    const runRows = await chQuery<{ run_id: string }>(
      `
      SELECT run_id
      FROM backfill_progress
      WHERE startsWith(run_id, {prefix:String})
      ORDER BY ts DESC
      LIMIT 1
    `,
      { prefix: V1_RECRAWL_RUN_PREFIX },
    );
    const runId = runRows[0]?.run_id;
    if (runId === undefined) {
      return {
        generatedAt: new Date().toISOString(),
        targetRepos: V1_RECRAWL_TARGET_REPOS,
        targetPosts: V1_RECRAWL_TARGET_POSTS,
        runId: null,
        active: false,
        shards: 0,
        freshnessSeconds: null,
        reposProcessed: 0,
        remainingRepos: V1_RECRAWL_TARGET_REPOS,
        reposPerMin: 0,
        etaHours: null,
      };
    }

    const [snapshot, rate] = await Promise.all([
      chQuery<SnapshotRow>(
        `
        SELECT
          shard,
          max(ts) AS latest_ts,
          ${STATUS_ARGMAX_SQL},
          argMax(in_flight, ts) AS in_flight
        FROM backfill_progress
        WHERE run_id = {run:String}
        GROUP BY shard
      `,
        { run: runId },
      ),
      chQuery<{ repos_per_min: string }>(
        `
        SELECT sum(rpm) AS repos_per_min
        FROM (
          SELECT
            (argMax(${RESOLVED_SUM_SQL}, ts) - argMin(${RESOLVED_SUM_SQL}, ts))
            / greatest(dateDiff('second', min(ts), max(ts)) / 60, 1) AS rpm
          FROM backfill_progress
          WHERE run_id = {run:String}
            AND ts >= now() - INTERVAL ${RATE_WINDOW_MINUTES} MINUTE
          GROUP BY shard
        )
      `,
        { run: runId },
      ),
    ]);

    const statusCounts = Object.fromEntries(
      REPO_STATUSES.map((status) => [status, 0]),
    ) as Record<BackfillRepoStatus, number>;
    let newestTs = 0;
    for (const row of snapshot) {
      for (const status of REPO_STATUSES)
        statusCounts[status] += num(row[status]);
      newestTs = Math.max(newestTs, chTsToDate(row.latest_ts).getTime());
    }

    const totalEnumerated = REPO_STATUSES.reduce(
      (acc, status) => acc + statusCounts[status],
      0,
    );
    const remaining = statusCounts.pending + statusCounts.fetching;
    const processed = Math.max(0, totalEnumerated - remaining);
    const reposPerMin = num(rate[0]?.repos_per_min);
    const freshnessSeconds =
      newestTs > 0
        ? Math.max(0, Math.round((Date.now() - newestTs) / 1000))
        : null;
    return {
      generatedAt: new Date().toISOString(),
      targetRepos: V1_RECRAWL_TARGET_REPOS,
      targetPosts: V1_RECRAWL_TARGET_POSTS,
      runId,
      active:
        freshnessSeconds !== null &&
        freshnessSeconds < IDLE_AFTER_SECONDS &&
        remaining > 0,
      shards: snapshot.length,
      freshnessSeconds,
      reposProcessed: processed,
      remainingRepos: Math.max(0, totalEnumerated - processed),
      reposPerMin,
      etaHours: reposPerMin > 0 ? remaining / reposPerMin / 60 : null,
    };
  },
);

export interface BackfillIssue {
  ts: string;
  did: string;
  host: string;
  event: string;
  error: string;
}

export interface BackfillIssues {
  generatedAt: string;
  issues: Array<BackfillIssue>;
}

export const getBackfillIssues = createServerFn().handler(
  async (): Promise<BackfillIssues> => {
    const rows = await chQuery<BackfillIssue>(`
      SELECT
        ts,
        did,
        pds_host AS host,
        event,
        substring(error, 1, 160) AS error
      FROM backfill_repo_events
      WHERE event IN (${ISSUE_EVENTS.map((e) => `'${e}'`).join(', ')})
      ORDER BY ts DESC
      LIMIT 20
    `);
    return { generatedAt: new Date().toISOString(), issues: rows };
  },
);

export interface BackfillFun {
  generatedAt: string;
  topEmojis: Array<{ emoji: string; occurrences: number }>;
  emojiPosts: number;
  oldestPostAt: string | null;
}

// The top-emoji query arrayJoins every emoji post (src-agnostic: live can win
// the ReplacingMergeTree merge, so filtering by src would undercount recovered
// rows); cheap at dry-run scale but memoized so pollers share one scan.
let funCache: { at: number; data: BackfillFun } | null = null;
const FUN_CACHE_MS = 60_000;

export const getBackfillFun = createServerFn().handler(
  async (): Promise<BackfillFun> => {
    if (funCache !== null && Date.now() - funCache.at < FUN_CACHE_MS) {
      return funCache.data;
    }
    // Aggregate tables only — arrayJoin over raw posts is a full scan that
    // grows with the crawl (OOM'd mid-backfill; unsurvivable at 2.9B rows).
    const [topEmojis, oldest] = await Promise.all([
      chQuery<{ emoji: string; occurrences: string }>(`
        SELECT emoji, sum(occurrences) AS occurrences
        FROM emoji_total
        GROUP BY emoji
        ORDER BY occurrences DESC
        LIMIT 10
      `),
      chQuery<{ oldest: string; n: string; emoji_posts: string }>(`
        SELECT
          min(hour) AS oldest,
          sum(posts) AS n,
          sum(posts_with_emojis) AS emoji_posts
        FROM posts_hourly
      `),
    ]);
    const data: BackfillFun = {
      generatedAt: new Date().toISOString(),
      topEmojis: topEmojis.map((row) => ({
        emoji: row.emoji,
        occurrences: num(row.occurrences),
      })),
      emojiPosts: num(oldest[0]?.emoji_posts),
      oldestPostAt: num(oldest[0]?.n) > 0 ? (oldest[0]?.oldest ?? null) : null,
    };
    funCache = { at: Date.now(), data };
    return data;
  },
);
