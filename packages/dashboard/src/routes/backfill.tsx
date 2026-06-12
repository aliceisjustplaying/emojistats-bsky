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
  getBackfillOverview,
  getBackfillTimeline,
} from '#/server/backfill';
import type { BackfillOverview, BackfillRepoStatus } from '#/server/backfill';

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
  refetchInterval: 15_000,
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
      context.queryClient.ensureQueryData(issuesQueryOptions),
      context.queryClient.ensureQueryData(funQueryOptions),
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
  const { data: issues } = useSuspenseQuery(issuesQueryOptions);
  const { data: fun } = useSuspenseQuery(funQueryOptions);

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
      </header>

      <Hero overview={overview} />
      <HistoryHistogram
        months={histogram.months}
        totalPosts={histogram.totalPosts}
      />
      <ThroughputTimeline points={timeline?.points ?? []} />
      <StatusBreakdown overview={overview} />

      {/* items-start: the issues feed is far taller than the hosts table */}
      <div className="grid items-start gap-4 lg:grid-cols-2">
        <HostsTable hosts={hosts} />
        <IssuesFeed generatedAt={issues.generatedAt} issues={issues.issues} />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <TopEmojis topEmojis={fun.topEmojis} emojiPosts={fun.emojiPosts} />
        <OldestPost
          oldestPostAt={fun.oldestPostAt}
          generatedAt={fun.generatedAt}
        />
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
      crawling · updated {formatDuration(overview.freshnessSeconds)} ago
    </Badge>
  ) : (
    <Badge variant="secondary" className="tabular-nums">
      <span className="size-1.5 rounded-full bg-muted-foreground" />
      idle · last update {formatDuration(overview.freshnessSeconds)} ago
    </Badge>
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
            sub={overview.active ? 'rolling 5 min' : 'crawl idle'}
          />
          <Stat
            label="rows/s"
            value={integer.format(Math.round(overview.rowsPerSec))}
            sub={overview.active ? 'into ClickHouse' : 'last reported'}
          />
          <Stat label="ETA" value={formatEta(overview.etaHours)} />
          <Stat
            label="in flight"
            value={integer.format(overview.inFlight)}
            sub={`${integer.format(overview.shards)} shard${overview.shards === 1 ? '' : 's'}`}
          />
        </div>
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
  rowsPerSec: { label: 'rows/s', color: 'var(--chart-1)' },
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
    label: formatClock(p.ts),
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
            posts loaded per minute · insert rows/s (local time)
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
            repo archive data fetched per minute (local time)
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
          every enumerated repo walks the ledger to a terminal status — nothing
          is silently skipped
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
              const count = overview.statusCounts[key];
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

function HostsTable({
  hosts,
}: {
  hosts: Array<{
    host: string;
    loaded: number;
    errors: number;
    bytes: number;
    avgPostsPerRepo: number;
  }>;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Top PDS hosts</CardTitle>
        <CardDescription>by repos loaded</CardDescription>
      </CardHeader>
      <CardContent>
        {hosts.length === 0 ? (
          <p className="text-sm text-muted-foreground">no repo events yet</p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-xs text-muted-foreground">
                <th className="pb-2 font-medium">host</th>
                <th className="pb-2 text-right font-medium">loaded</th>
                <th className="pb-2 text-right font-medium">errors</th>
                <th className="hidden pb-2 text-right font-medium sm:table-cell">
                  avg posts
                </th>
                <th className="pb-2 text-right font-medium">data</th>
              </tr>
            </thead>
            <tbody className="tabular-nums">
              {hosts.map((row) => (
                <tr key={row.host} className="border-b border-border/50">
                  <td className="max-w-36 truncate py-1.5 font-mono text-xs sm:max-w-48">
                    {row.host}
                  </td>
                  <td className="py-1.5 text-right">
                    {integer.format(row.loaded)}
                  </td>
                  <td
                    className={`py-1.5 text-right ${
                      row.errors > 0
                        ? 'text-amber-600'
                        : 'text-muted-foreground'
                    }`}
                  >
                    {integer.format(row.errors)}
                  </td>
                  <td className="hidden py-1.5 text-right sm:table-cell">
                    {integer.format(Math.round(row.avgPostsPerRepo))}
                  </td>
                  <td className="py-1.5 text-right">
                    {formatBytes(row.bytes)}
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
