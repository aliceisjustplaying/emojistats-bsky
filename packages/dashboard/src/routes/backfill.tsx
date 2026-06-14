import { queryOptions, useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  ComposedChart,
  Line,
  XAxis,
  YAxis,
} from 'recharts';

import { Badge } from '#/components/ui/badge';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '#/components/ui/card';
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from '#/components/ui/chart';
import type { ChartConfig } from '#/components/ui/chart';
import {
  getBackfillFun,
  getBackfillHistogram,
  getBackfillHosts,
  getBackfillIssues,
  getBackfillLooseRecrawlStatus,
  getBackfillOverview,
  getBackfillRecrawlStatus,
  getBackfillStatusReasons,
  getBackfillTimeline,
  getBackfillVerifyStatus,
} from '#/server/backfill';
import type {
  BackfillOverview,
  BackfillLooseRecrawlStatus,
  BackfillRecrawlStatus,
  BackfillRepoStatus,
  BackfillStatusReasons,
  BackfillVerifyStatus,
} from '#/server/backfill';

const overviewQueryOptions = queryOptions({
  queryKey: ['backfill-overview'],
  queryFn: () => getBackfillOverview(),
  refetchInterval: 5_000,
});

const timelineQueryOptions = queryOptions({
  queryKey: ['backfill-timeline'],
  queryFn: () => getBackfillTimeline(),
  refetchInterval: 30_000,
});

const histogramQueryOptions = queryOptions({
  queryKey: ['backfill-histogram'],
  queryFn: () => getBackfillHistogram(),
  refetchInterval: 30_000,
});

const hostsQueryOptions = queryOptions({
  queryKey: ['backfill-hosts'],
  queryFn: () => getBackfillHosts(),
  refetchInterval: 60_000,
});

const recrawlQueryOptions = queryOptions({
  queryKey: ['backfill-recrawl'],
  queryFn: () => getBackfillRecrawlStatus(),
  refetchInterval: 15_000,
});

const looseRecrawlQueryOptions = queryOptions({
  queryKey: ['backfill-loose-recrawl'],
  queryFn: () => getBackfillLooseRecrawlStatus(),
  refetchInterval: 10_000,
});

const verifyQueryOptions = queryOptions({
  queryKey: ['backfill-verify'],
  queryFn: () => getBackfillVerifyStatus(),
  refetchInterval: 10_000,
});

const issuesQueryOptions = queryOptions({
  queryKey: ['backfill-issues'],
  queryFn: () => getBackfillIssues(),
  refetchInterval: 15_000,
});

const funQueryOptions = queryOptions({
  queryKey: ['backfill-fun'],
  queryFn: () => getBackfillFun(),
  refetchInterval: 60_000,
});

const statusReasonsQueryOptions = queryOptions({
  queryKey: ['backfill-status-reasons'],
  queryFn: () => getBackfillStatusReasons(),
  refetchInterval: 60_000,
});

export const Route = createFileRoute('/backfill')({
  head: () => ({
    meta: [{ title: 'emojistats · backfill' }],
  }),
  loader: async ({ context }) => {
    await Promise.all([
      context.queryClient.ensureQueryData(overviewQueryOptions),
      context.queryClient.ensureQueryData(timelineQueryOptions),
      context.queryClient.ensureQueryData(histogramQueryOptions),
      context.queryClient.ensureQueryData(hostsQueryOptions),
      context.queryClient.ensureQueryData(recrawlQueryOptions),
      context.queryClient.ensureQueryData(looseRecrawlQueryOptions),
      context.queryClient.ensureQueryData(verifyQueryOptions),
      context.queryClient.ensureQueryData(issuesQueryOptions),
      context.queryClient.ensureQueryData(funQueryOptions),
      context.queryClient.ensureQueryData(statusReasonsQueryOptions),
    ]);
  },
  errorComponent: ({ error }) => (
    <main className="mx-auto max-w-3xl p-6">
      <h1 className="text-lg font-semibold">emojistats backfill</h1>
      <p className="mt-4 text-sm text-destructive-foreground">
        Failed to load backfill stats: {error.message}
      </p>
    </main>
  ),
  component: BackfillPage,
});

const integer = new Intl.NumberFormat('en-US');
const compact = new Intl.NumberFormat('en-US', {
  notation: 'compact',
  maximumFractionDigits: 1,
});
const compactMobile = new Intl.NumberFormat('en-US', {
  notation: 'compact',
  maximumFractionDigits: 0,
});

function CompactHostValue({ value }: { value: number }) {
  return (
    <>
      <span className="sm:hidden">{compactMobile.format(value)}</span>
      <span className="hidden sm:inline">{compact.format(value)}</span>
    </>
  );
}

const MONTH_NAMES = [
  'January',
  'February',
  'March',
  'April',
  'May',
  'June',
  'July',
  'August',
  'September',
  'October',
  'November',
  'December',
] as const;

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${Math.round(bytes)} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(1)} ${units[unit]}`;
}

function chTsToDate(chUtcDateTime: string): Date {
  return new Date(`${chUtcDateTime.replace(' ', 'T')}Z`);
}

function formatClock(chUtcDateTime: string): string {
  const date = chTsToDate(chUtcDateTime);
  return `${date.getHours().toString().padStart(2, '0')}:${date
    .getMinutes()
    .toString()
    .padStart(2, '0')}`;
}

function formatTimelineLabel(chUtcDateTime: string): string {
  const date = chTsToDate(chUtcDateTime);
  return `${date.getMonth() + 1}/${date.getDate()} ${formatClock(chUtcDateTime)}`;
}

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3_600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86_400) return `${(seconds / 3_600).toFixed(1)}h`;
  if (seconds < 365.25 * 86_400) return `${(seconds / 86_400).toFixed(1)}d`;
  return `${(seconds / (365.25 * 86_400)).toFixed(1)}y`;
}

/** Relative time computed against the payload's generatedAt so SSR and
 * hydration render identically. */
function formatAgo(generatedAt: string, chUtcDateTime: string): string {
  const seconds =
    (new Date(generatedAt).getTime() - chTsToDate(chUtcDateTime).getTime()) /
    1000;
  return `${formatDuration(Math.max(0, seconds))} ago`;
}

function formatEta(hours: number | null): string {
  if (hours === null) return '—';
  if (hours < 1) return `${Math.max(1, Math.round(hours * 60))}m`;
  if (hours < 48) return `${hours.toFixed(1)}h`;
  return `${(hours / 24).toFixed(1)}d`;
}

function BackfillPage() {
  const { data: overview } = useSuspenseQuery(overviewQueryOptions);
  const { data: timeline } = useSuspenseQuery(timelineQueryOptions);
  const { data: histogram } = useSuspenseQuery(histogramQueryOptions);
  const { data: hosts } = useSuspenseQuery(hostsQueryOptions);
  const { data: recrawl } = useSuspenseQuery(recrawlQueryOptions);
  const { data: looseRecrawl } = useSuspenseQuery(looseRecrawlQueryOptions);
  const { data: verify } = useSuspenseQuery(verifyQueryOptions);
  const { data: issues } = useSuspenseQuery(issuesQueryOptions);
  const { data: fun } = useSuspenseQuery(funQueryOptions);
  const { data: statusReasons } = useSuspenseQuery(statusReasonsQueryOptions);

  return (
    <main className="mx-auto max-w-6xl space-y-4 p-4 pb-10">
      <header className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h1 className="font-mono text-lg font-semibold tracking-tight">
            emojistats backfill
          </h1>
          <p className="text-xs text-muted-foreground">
            crawling every Bluesky repo to rebuild emoji history since 2023
          </p>
        </div>
        <div className="flex flex-col items-end gap-1.5">
          <div className="flex items-center gap-2">
            <CrawlBadge overview={overview} />
            {overview ? (
              <span
                className="text-xs text-muted-foreground tabular-nums"
                suppressHydrationWarning
              >
                {new Date(overview.generatedAt).toLocaleTimeString()}
              </span>
            ) : null}
          </div>
          <ShardFreshnessStrip overview={overview} />
        </div>
      </header>

      <Hero overview={overview} />
      <div className="grid gap-4 lg:grid-cols-3">
        <VerificationStatus status={verify} />
        <LooseRecrawlStatus status={looseRecrawl} />
        <RecrawlStatus status={recrawl} overview={overview} />
      </div>

      {/* the payoff goes right under the hero — this is the fun part */}
      <div className="grid gap-4 lg:grid-cols-2">
        <TopEmojis topEmojis={fun.topEmojis} emojiPosts={fun.emojiPosts} />
        <OldestPost
          oldestPostAt={fun.oldestPostAt}
          generatedAt={fun.generatedAt}
        />
      </div>

      <HistoryHistogram
        months={histogram.months}
        totalPosts={histogram.totalPosts}
      />
      <ThroughputTimeline points={timeline?.points ?? []} />
      <StatusBreakdown overview={overview} />
      <StatusReasonBreakdown breakdown={statusReasons} />

      {/* items-start: the issues feed is far taller than the hosts table */}
      <div className="grid items-start gap-4 lg:grid-cols-2">
        <HostsTable hosts={hosts} />
        <IssuesFeed generatedAt={issues.generatedAt} issues={issues.issues} />
      </div>

      <footer className="pt-2 text-center text-xs text-muted-foreground">
        deleted posts are gone forever — these curves show surviving history,
        counted as it happened
      </footer>
    </main>
  );
}

function CrawlBadge({ overview }: { overview: BackfillOverview | null }) {
  if (overview === null) {
    return <Badge variant="outline">awaiting crawl</Badge>;
  }
  return overview.active ? (
    <Badge variant="outline" className="tabular-nums">
      <span className="size-1.5 rounded-full bg-emerald-600" />
      crawling · updated {formatDuration(overview.latestFreshnessSeconds)} ago
    </Badge>
  ) : (
    <Badge variant="secondary" className="tabular-nums">
      <span className="size-1.5 rounded-full bg-muted-foreground" />
      idle · newest snapshot {formatDuration(
        overview.latestFreshnessSeconds,
      )}{' '}
      ago
    </Badge>
  );
}

// These are logical ledger snapshot ages. Recovery runs can refresh an old
// shard after the original crawler host is gone, so this must not be phrased
// as crawler-host liveness.
const SHARD_AMBER_SECONDS = 60;
const SHARD_RED_SECONDS = 300;

function ShardFreshnessStrip({
  overview,
}: {
  overview: BackfillOverview | null;
}) {
  if (overview === null || overview.shardFreshness.length === 0) return null;
  const frozen = overview.shardFreshness.filter(
    (s) => s.ageSeconds > SHARD_RED_SECONDS,
  );
  return (
    <div className="space-y-1">
      <div className="flex flex-wrap justify-end gap-1.5">
        {overview.shardFreshness.map((s) => (
          <span
            key={s.shard}
            className={`rounded-full border px-2 py-0.5 font-mono text-[10px] tabular-nums ${
              s.ageSeconds > SHARD_RED_SECONDS
                ? 'border-red-600/50 text-red-600'
                : s.ageSeconds > SHARD_AMBER_SECONDS
                  ? 'border-amber-600/50 text-amber-600'
                  : 'text-muted-foreground'
            }`}
          >
            {s.shard} snapshot · {formatDuration(s.ageSeconds)}
          </span>
        ))}
      </div>
      {frozen.length > 0 ? (
        <p className="text-right text-xs text-red-600/90">
          {frozen.length === 1
            ? `${frozen[0].shard} snapshot is stale — counts below use its latest telemetry row`
            : `${frozen.length} shard snapshots are stale — counts below use each shard's latest telemetry row`}
        </p>
      ) : null}
    </div>
  );
}

function Hero({ overview }: { overview: BackfillOverview | null }) {
  if (overview === null) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Crawl progress</CardTitle>
          <CardDescription>
            no crawl telemetry yet — progress appears here the moment a run
            starts reporting
          </CardDescription>
        </CardHeader>
      </Card>
    );
  }

  const pct =
    overview.totalEnumerated > 0
      ? (overview.resolved / overview.totalEnumerated) * 100
      : 0;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Crawl progress</CardTitle>
        <CardDescription>
          {integer.format(overview.resolved)} of{' '}
          {integer.format(overview.totalEnumerated)} enumerated repos resolved
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
        <div className="space-y-1.5">
          <div className="h-3 w-full overflow-hidden rounded-full bg-secondary">
            <div
              className="h-full rounded-full bg-primary transition-[width] duration-700"
              style={{ width: `${Math.min(100, pct)}%` }}
            />
          </div>
          <p className="text-right text-xs text-muted-foreground tabular-nums">
            {pct.toFixed(1)}%
          </p>
        </div>
        <div className="grid grid-cols-2 gap-x-4 gap-y-5 sm:grid-cols-3 lg:grid-cols-6">
          <Stat
            label="posts loaded"
            value={compact.format(overview.postsLoaded)}
            sub={integer.format(overview.postsLoaded)}
          />
          <Stat
            label="data downloaded"
            value={formatBytes(overview.bytesDownloaded)}
          />
          <Stat
            label="repos/min"
            value={integer.format(Math.round(overview.reposPerMin))}
            sub={overview.active ? 'rolling 10 min' : 'crawl idle'}
          />
          <Stat
            label="rows/s"
            value={integer.format(Math.round(overview.rowsPerSec))}
            sub={overview.active ? 'into ClickHouse' : 'last reported'}
          />
          <Stat
            label="ETA"
            value={formatEta(overview.etaHours)}
            sub="pending + fetching"
          />
          <Stat
            label="in flight"
            value={integer.format(overview.inFlight)}
            sub={`${integer.format(overview.shards)} shard${overview.shards === 1 ? '' : 's'}`}
          />
        </div>
        {overview.parkedUnreachable > 0 ? (
          <p className="text-xs text-muted-foreground tabular-nums">
            {integer.format(overview.parkedUnreachable)} unreachable parked
            (retry waves + final sweep) — outside the ETA
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}

const histogramChartConfig = {
  posts: { label: 'posts recovered', color: 'var(--chart-1)' },
} satisfies ChartConfig;

function HistoryHistogram({
  months,
  totalPosts,
}: {
  months: Array<{ month: string; posts: number }>;
  totalPosts: number;
}) {
  const data = months.map((row) => {
    const date = new Date(`${row.month}T00:00:00Z`);
    const monthName = MONTH_NAMES[date.getUTCMonth()];
    return {
      ...row,
      label: `${monthName.slice(0, 3)} ’${String(date.getUTCFullYear()).slice(2)}`,
      full: `${monthName} ${date.getUTCFullYear()}`,
    };
  });
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Filling in history</CardTitle>
        <CardDescription>
          {compact.format(totalPosts)} surviving posts recovered so far, by
          month written — the bars grow as the crawl reaches deeper into the
          past
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer
          config={histogramChartConfig}
          className="aspect-auto h-64 w-full sm:h-80"
        >
          <BarChart data={data} margin={{ left: 4, right: 4 }}>
            <CartesianGrid vertical={false} strokeOpacity={0.3} />
            <XAxis
              dataKey="label"
              tickLine={false}
              axisLine={false}
              tickMargin={6}
              fontSize={10}
              minTickGap={24}
            />
            <ChartTooltip
              content={
                <ChartTooltipContent
                  labelFormatter={(_, payload) =>
                    (payload?.[0]?.payload as { full?: string } | undefined)
                      ?.full ?? ''
                  }
                />
              }
            />
            <Bar
              dataKey="posts"
              fill="var(--color-posts)"
              radius={[3, 3, 0, 0]}
            />
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}

const throughputChartConfig = {
  postsPerMin: { label: 'posts/min', color: 'var(--chart-2)' },
  rowsPerSec: { label: 'posts/s', color: 'var(--chart-1)' },
} satisfies ChartConfig;

const downloadChartConfig = {
  mibPerMin: { label: 'MiB/min', color: 'var(--chart-3)' },
} satisfies ChartConfig;

function ThroughputTimeline({
  points,
}: {
  points: Array<{
    ts: string;
    postsPerMin: number;
    bytesPerMin: number;
    rowsPerSec: number;
  }>;
}) {
  if (points.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Throughput</CardTitle>
          <CardDescription>
            no telemetry to chart yet — the timeline appears once the crawl
            reports a few snapshots
          </CardDescription>
        </CardHeader>
      </Card>
    );
  }

  const data = points.map((p) => ({
    label: formatTimelineLabel(p.ts),
    postsPerMin: Math.round(p.postsPerMin),
    rowsPerSec: Math.round(p.rowsPerSec),
    mibPerMin: Number((p.bytesPerMin / 1024 / 1024).toFixed(1)),
  }));

  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Posts throughput</CardTitle>
          <CardDescription>
            project lifetime · posts loaded per minute and per second
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ChartContainer
            config={throughputChartConfig}
            className="aspect-auto h-44 w-full"
          >
            <ComposedChart data={data} margin={{ left: 4, right: 4 }}>
              <CartesianGrid vertical={false} strokeOpacity={0.3} />
              <XAxis
                dataKey="label"
                tickLine={false}
                axisLine={false}
                tickMargin={6}
                fontSize={10}
                minTickGap={32}
              />
              <YAxis yAxisId="posts" hide />
              <YAxis yAxisId="rows" orientation="right" hide />
              <ChartTooltip content={<ChartTooltipContent />} />
              <Area
                yAxisId="posts"
                dataKey="postsPerMin"
                type="monotone"
                fill="var(--color-postsPerMin)"
                fillOpacity={0.2}
                stroke="var(--color-postsPerMin)"
                strokeWidth={1.5}
              />
              <Line
                yAxisId="rows"
                dataKey="rowsPerSec"
                type="monotone"
                stroke="var(--color-rowsPerSec)"
                strokeWidth={1.5}
                dot={false}
              />
            </ComposedChart>
          </ChartContainer>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Download rate</CardTitle>
          <CardDescription>
            project lifetime · repo archive data fetched per minute
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ChartContainer
            config={downloadChartConfig}
            className="aspect-auto h-44 w-full"
          >
            <AreaChart data={data} margin={{ left: 4, right: 4 }}>
              <CartesianGrid vertical={false} strokeOpacity={0.3} />
              <XAxis
                dataKey="label"
                tickLine={false}
                axisLine={false}
                tickMargin={6}
                fontSize={10}
                minTickGap={32}
              />
              <ChartTooltip content={<ChartTooltipContent />} />
              <Area
                dataKey="mibPerMin"
                type="monotone"
                fill="var(--color-mibPerMin)"
                fillOpacity={0.2}
                stroke="var(--color-mibPerMin)"
                strokeWidth={1.5}
              />
            </AreaChart>
          </ChartContainer>
        </CardContent>
      </Card>
    </div>
  );
}

// Display order: active work first, then happy terminals, then the morgue.
const STATUS_ORDER: Array<{
  key: BackfillRepoStatus;
  tone: 'normal' | 'warn' | 'bad';
}> = [
  { key: 'pending', tone: 'normal' },
  { key: 'fetching', tone: 'normal' },
  { key: 'loaded', tone: 'normal' },
  { key: 'verified', tone: 'normal' },
  { key: 'empty', tone: 'normal' },
  { key: 'tombstoned', tone: 'normal' },
  { key: 'deactivated', tone: 'normal' },
  { key: 'takendown', tone: 'normal' },
  { key: 'unreachable', tone: 'warn' },
  { key: 'quarantined', tone: 'bad' },
  { key: 'failed', tone: 'bad' },
];

function StatusBreakdown({ overview }: { overview: BackfillOverview | null }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Repo status breakdown</CardTitle>
        <CardDescription>
          loaded is every fetched repo with post rows; verified is the subset
          whose post rows have passed the digest check. residual
          pending/fetching here is retained canonical telemetry residue from
          retired/stale shard snapshots, not active backfill work.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {overview === null ? (
          <p className="text-sm text-muted-foreground">
            no crawl telemetry yet
          </p>
        ) : (
          <div className="grid grid-cols-3 gap-x-4 gap-y-5 sm:grid-cols-4 lg:grid-cols-6">
            {STATUS_ORDER.map(({ key, tone }) => {
              const count =
                key === 'loaded'
                  ? overview.statusCounts.loaded +
                    overview.statusCounts.verified
                  : overview.statusCounts[key];
              return (
                <div key={key} className="space-y-0.5">
                  <p className="text-xs text-muted-foreground">{key}</p>
                  <p
                    className={`text-lg leading-tight font-semibold tabular-nums ${
                      count === 0
                        ? 'text-muted-foreground/60'
                        : tone === 'bad'
                          ? 'text-red-600'
                          : tone === 'warn'
                            ? 'text-amber-600'
                            : ''
                    }`}
                  >
                    {integer.format(count)}
                  </p>
                </div>
              );
            })}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

const REASON_TITLES: Record<
  BackfillStatusReasons['groups'][number]['status'],
  string
> = {
  unreachable: 'unreachable',
  quarantined: 'quarantined',
  failed: 'failed',
};

function StatusReasonBreakdown({
  breakdown,
}: {
  breakdown: BackfillStatusReasons | null;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Repo issue reasons</CardTitle>
        <CardDescription>
          latest 6-shard ledger rollup · grouped from SQLite error text
        </CardDescription>
      </CardHeader>
      <CardContent>
        {breakdown === null ? (
          <p className="text-sm text-muted-foreground">
            no reason rollup has been published yet
          </p>
        ) : (
          <div className="grid gap-4 lg:grid-cols-3">
            {breakdown.groups.map((group) => (
              <div key={group.status} className="space-y-2">
                <div>
                  <p className="text-sm font-medium">
                    {REASON_TITLES[group.status]}
                  </p>
                  <p className="font-mono text-lg font-semibold tabular-nums">
                    {integer.format(group.total)}
                  </p>
                </div>
                <div className="space-y-1.5">
                  {group.reasons.map((row) => {
                    const pct =
                      group.total > 0 ? (row.count / group.total) * 100 : 0;
                    return (
                      <div key={row.reason} className="space-y-0.5">
                        <div className="flex items-start justify-between gap-3 text-xs">
                          <span className="text-muted-foreground">
                            {row.reason}
                          </span>
                          <span className="font-mono tabular-nums">
                            {integer.format(row.count)}
                          </span>
                        </div>
                        <div className="h-1.5 overflow-hidden rounded-full bg-secondary">
                          <div
                            className="h-full rounded-full bg-primary"
                            style={{ width: `${Math.max(1, pct)}%` }}
                          />
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function HostsTable({
  hosts,
}: {
  hosts: Array<{
    host: string;
    total: number;
    loaded: number;
    empty: number;
    issues: number;
    bytes: number;
    avgPostsPerRepo: number;
  }>;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Top PDS hosts</CardTitle>
        <CardDescription>by terminal repos seen</CardDescription>
      </CardHeader>
      <CardContent>
        {hosts.length === 0 ? (
          <p className="text-sm text-muted-foreground">no repo events yet</p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-xs text-muted-foreground">
                <th className="pb-2 font-medium">host</th>
                <th className="pb-2 text-right font-medium">total</th>
                <th className="pb-2 text-right font-medium">loaded</th>
                <th className="pb-2 text-right font-medium">empty</th>
                <th className="pb-2 text-right font-medium">issues</th>
              </tr>
            </thead>
            <tbody className="tabular-nums">
              {hosts.map((row) => (
                <tr key={row.host} className="border-b border-border/50">
                  <td className="max-w-32 truncate py-1.5 font-mono text-xs sm:max-w-48">
                    <p className="truncate">{row.host}</p>
                    <p className="truncate font-sans text-[11px] text-muted-foreground">
                      {formatBytes(row.bytes)} · avg{' '}
                      {integer.format(Math.round(row.avgPostsPerRepo))} posts
                    </p>
                  </td>
                  <td className="py-1.5 text-right font-medium">
                    <CompactHostValue value={row.total} />
                  </td>
                  <td className="py-1.5 text-right">
                    <CompactHostValue value={row.loaded} />
                  </td>
                  <td className="py-1.5 text-right text-muted-foreground">
                    <CompactHostValue value={row.empty} />
                  </td>
                  <td
                    className={`py-1.5 text-right ${
                      row.issues > 0
                        ? 'text-amber-600'
                        : 'text-muted-foreground'
                    }`}
                  >
                    <CompactHostValue value={row.issues} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </CardContent>
    </Card>
  );
}

function RecrawlStatus({
  status,
  overview,
}: {
  status: BackfillRecrawlStatus;
  overview: BackfillOverview | null;
}) {
  const prepared = status.runId === null;
  const currentRemaining =
    overview === null
      ? null
      : overview.statusCounts.pending + overview.statusCounts.fetching;
  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <CardTitle className="text-sm">v2 metadata recrawl</CardTitle>
            <CardDescription>
              {prepared
                ? 'prepared; waiting for current crawl + final sweep'
                : `${status.runId} · ${status.active ? 'active' : 'idle'}`}
            </CardDescription>
          </div>
          <Badge variant={status.active ? 'outline' : 'secondary'}>
            {prepared ? 'ready' : status.active ? 'running' : 'seen'}
          </Badge>
        </div>
      </CardHeader>
      <CardContent>
        <div className="grid gap-3 text-sm sm:grid-cols-4">
          <RecrawlMetric
            label="target repos"
            value={integer.format(status.targetRepos)}
          />
          <RecrawlMetric
            label="target posts"
            value={integer.format(status.targetPosts)}
          />
          <RecrawlMetric
            label={prepared ? 'current crawl left' : 'recrawl left'}
            value={
              prepared
                ? currentRemaining === null
                  ? '—'
                  : integer.format(currentRemaining)
                : integer.format(status.remainingRepos)
            }
          />
          <RecrawlMetric
            label={prepared ? 'recrawl eta' : 'eta'}
            value={prepared ? 'not started' : formatEta(status.etaHours)}
          />
        </div>
        {prepared ? null : (
          <p className="mt-3 text-xs text-muted-foreground tabular-nums">
            {integer.format(status.reposProcessed)} repos processed ·{' '}
            {integer.format(Math.round(status.reposPerMin))}/min ·{' '}
            {status.freshnessSeconds === null
              ? 'no telemetry age'
              : `updated ${formatDuration(status.freshnessSeconds)} ago`}
          </p>
        )}
      </CardContent>
    </Card>
  );
}

function LooseRecrawlStatus({
  status,
}: {
  status: BackfillLooseRecrawlStatus;
}) {
  const prepared = status.runId === null;
  const pct =
    status.targetRepos > 0 ? (status.loaded / status.targetRepos) * 100 : 0;
  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <CardTitle className="text-sm">loose recrawl</CardTitle>
            <CardDescription>
              {prepared
                ? 'waiting for loose DID files'
                : `${status.runId} · ${status.active ? 'active' : 'idle'}`}
            </CardDescription>
          </div>
          <Badge variant={status.active ? 'outline' : 'secondary'}>
            {prepared ? 'ready' : status.active ? 'running' : 'seen'}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-1.5">
          <div className="h-2.5 w-full overflow-hidden rounded-full bg-secondary">
            <div
              className="h-full rounded-full bg-primary transition-[width] duration-700"
              style={{ width: `${Math.min(100, pct)}%` }}
            />
          </div>
          <p className="text-right text-xs text-muted-foreground tabular-nums">
            {prepared
              ? 'not started'
              : `${integer.format(status.loaded)} / ${integer.format(status.targetRepos)} loaded from loose files · ${pct.toFixed(1)}%`}
          </p>
        </div>
        <div className="grid grid-cols-2 gap-3 text-sm">
          <RecrawlMetric
            label="active shards"
            value={`${integer.format(status.activeShards)} / ${integer.format(status.shards)}`}
          />
          <RecrawlMetric
            label="in flight"
            value={integer.format(status.inFlight)}
          />
          <RecrawlMetric
            label="repos/min"
            value={integer.format(Math.round(status.reposPerMin))}
          />
          <RecrawlMetric label="eta" value={formatEta(status.etaHours)} />
          <RecrawlMetric
            label="rows/sec"
            value={integer.format(Math.round(status.rowsPerSec))}
          />
        </div>
        {status.runId !== null ? (
          <p className="text-xs text-muted-foreground tabular-nums">
            {status.freshnessSeconds === null
              ? 'no telemetry age'
              : `updated ${formatDuration(status.freshnessSeconds)} ago`}
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}

function VerificationStatus({ status }: { status: BackfillVerifyStatus }) {
  const prepared = status.runId === null;
  const failed = status.failedShards > 0;
  const hasOpenRecheck = status.recheckLoadedOpen > 0;
  const needsAttention = failed || hasOpenRecheck;
  const classified = status.exact + status.loose + status.mismatches;
  const pct =
    status.reposTotal > 0 ? (classified / status.reposTotal) * 100 : 0;
  const runLabel =
    status.runIds.length > 1
      ? `${integer.format(status.runIds.length)} shard runs`
      : status.runId;
  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <CardTitle className="text-sm">verification</CardTitle>
            <CardDescription>
              {prepared
                ? 'ready; waiting for manual kickoff'
                : `${runLabel} · ${status.phase}`}
            </CardDescription>
          </div>
          <Badge
            variant={
              needsAttention
                ? 'destructive'
                : status.active
                  ? 'outline'
                  : 'secondary'
            }
          >
            {prepared
              ? 'ready'
              : failed
                ? status.active
                  ? 'failing'
                  : 'failed'
                : hasOpenRecheck
                  ? 'loaded check'
                  : status.active
                    ? 'running'
                    : 'seen'}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-1.5">
          <div className="h-2.5 w-full overflow-hidden rounded-full bg-secondary">
            <div
              className="h-full rounded-full bg-primary transition-[width] duration-700"
              style={{ width: `${Math.min(100, pct)}%` }}
            />
          </div>
          <p className="text-right text-xs text-muted-foreground tabular-nums">
            {prepared
              ? 'not started'
              : `${integer.format(classified)} / ${integer.format(status.reposTotal)} repos classified across latest reporting shard runs · ${pct.toFixed(1)}%`}
          </p>
        </div>
        <div className="grid grid-cols-3 gap-3 text-sm">
          <RecrawlMetric label="exact" value={integer.format(status.exact)} />
          <RecrawlMetric label="loose" value={integer.format(status.loose)} />
          <RecrawlMetric
            label="loaded open"
            value={integer.format(status.recheckLoadedOpen)}
          />
          <RecrawlMetric
            label="historic diff"
            value={integer.format(status.mismatches)}
          />
          <RecrawlMetric
            label="loose file"
            value={integer.format(status.looseEmitted)}
          />
          <RecrawlMetric
            label="reporting shards"
            value={`${integer.format(status.doneShards)} / ${integer.format(status.shards)}`}
          />
        </div>
        {needsAttention && !status.active ? (
          <p className="text-xs text-muted-foreground">
            {hasOpenRecheck
              ? `${integer.format(status.recheckLoadedOpen)} post-recrawl repos remain loaded pending loaded-only verification`
              : 'verification needs operator attention'}
          </p>
        ) : null}
        {status.recheckRunIds.length > 0 ? (
          <p className="text-xs text-muted-foreground tabular-nums">
            recheck: {status.recheckRunIds.join(', ')}
          </p>
        ) : null}
        {status.runId !== null ? (
          <p className="text-xs text-muted-foreground tabular-nums">
            {status.freshnessSeconds === null
              ? 'no telemetry age'
              : `updated ${formatDuration(status.freshnessSeconds)} ago`}
          </p>
        ) : null}
        {status.error !== null ? (
          <p className="line-clamp-2 text-xs break-all text-red-600/90">
            last attempt error: {status.error}
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}

function RecrawlMetric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="font-mono text-base font-semibold tabular-nums">{value}</p>
    </div>
  );
}

function IssuesFeed({
  generatedAt,
  issues,
}: {
  generatedAt: string;
  issues: Array<{
    ts: string;
    did: string;
    host: string;
    event: string;
    error: string;
  }>;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Recent issues</CardTitle>
        <CardDescription>
          failed, quarantined, unreachable, takendown, deactivated
        </CardDescription>
      </CardHeader>
      <CardContent>
        {issues.length === 0 ? (
          <p className="text-sm text-muted-foreground">no issues recorded</p>
        ) : (
          <ul className="max-h-80 space-y-2.5 overflow-y-auto pr-1">
            {issues.map((issue) => (
              <li
                key={`${issue.did}-${issue.ts}`}
                className="border-b border-border/50 pb-2.5 last:border-0 last:pb-0"
              >
                <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                  <Badge
                    variant={
                      issue.event === 'failed' || issue.event === 'quarantined'
                        ? 'destructive'
                        : 'secondary'
                    }
                    className="px-1.5 py-0 text-[10px]"
                  >
                    {issue.event}
                  </Badge>
                  <span className="max-w-44 truncate font-mono text-xs text-muted-foreground">
                    {issue.host}
                  </span>
                  <span className="ml-auto text-xs text-muted-foreground tabular-nums">
                    {formatAgo(generatedAt, issue.ts)}
                  </span>
                </div>
                <p className="mt-0.5 truncate font-mono text-xs text-muted-foreground">
                  {issue.did}
                </p>
                {issue.error ? (
                  <p className="mt-0.5 line-clamp-2 text-xs break-all text-red-600/90">
                    {issue.error}
                  </p>
                ) : null}
              </li>
            ))}
          </ul>
        )}
      </CardContent>
    </Card>
  );
}

function TopEmojis({
  topEmojis,
  emojiPosts,
}: {
  topEmojis: Array<{ emoji: string; occurrences: number }>;
  emojiPosts: number;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Emojis of the recovered past</CardTitle>
        <CardDescription>
          top 10 across {compact.format(emojiPosts)} backfilled posts with
          emojis
        </CardDescription>
      </CardHeader>
      <CardContent>
        {topEmojis.length === 0 ? (
          <p className="text-sm text-muted-foreground">no emojis yet</p>
        ) : (
          <div className="grid grid-cols-5 gap-x-2 gap-y-4 text-center">
            {topEmojis.map((row) => (
              <div key={row.emoji} className="space-y-0.5">
                <p className="text-2xl">{row.emoji}</p>
                <p className="text-xs text-muted-foreground tabular-nums">
                  {compact.format(row.occurrences)}
                </p>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function OldestPost({
  oldestPostAt,
  generatedAt,
}: {
  oldestPostAt: string | null;
  generatedAt: string;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Oldest post recovered</CardTitle>
        <CardDescription>
          how deep into history the crawl has reached
        </CardDescription>
      </CardHeader>
      <CardContent>
        {oldestPostAt === null ? (
          <p className="text-sm text-muted-foreground">nothing crawled yet</p>
        ) : (
          <div className="space-y-0.5">
            <p className="text-xl leading-tight font-semibold tabular-nums">
              {chTsToDate(oldestPostAt).toISOString().slice(0, 10)}
            </p>
            <p className="text-xs text-muted-foreground">
              {formatAgo(generatedAt, oldestPostAt)} · around the first Bluesky
              sandbox posts
            </p>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function Stat({
  label,
  value,
  sub,
}: {
  label: string;
  value: string;
  sub?: string;
}) {
  return (
    <div className="space-y-0.5">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="text-xl leading-tight font-semibold tabular-nums">
        {value}
      </p>
      {sub ? (
        <p className="text-xs text-muted-foreground tabular-nums">{sub}</p>
      ) : null}
    </div>
  );
}
