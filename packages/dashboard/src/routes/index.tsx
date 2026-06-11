import { queryOptions, useSuspenseQuery } from '@tanstack/react-query';
import { Link, createFileRoute } from '@tanstack/react-router';
import { REPO_STATUSES, type RepoStatus } from 'backfill/types';
import { Bar, BarChart, CartesianGrid, XAxis } from 'recharts';

import { Badge } from '#/components/ui/badge';
import {
  Card,
  CardAction,
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
import { getBackfillStatus } from '#/server/backfill';
import type { BackfillStatus } from '#/server/backfill';
import { getLiveStats, getStorageStats } from '#/server/stats';

const FRESHNESS_RED_SECONDS = 10;

const liveStatsQueryOptions = queryOptions({
  queryKey: ['live-stats'],
  queryFn: () => getLiveStats(),
  refetchInterval: 2_000,
});

const storageStatsQueryOptions = queryOptions({
  queryKey: ['storage-stats'],
  queryFn: () => getStorageStats(),
  refetchInterval: 30_000,
});

const backfillQueryOptions = queryOptions({
  queryKey: ['backfill-status'],
  queryFn: () => getBackfillStatus(),
  refetchInterval: 15_000,
});

export const Route = createFileRoute('/')({
  loader: async ({ context }) => {
    await Promise.all([
      context.queryClient.ensureQueryData(liveStatsQueryOptions),
      context.queryClient.ensureQueryData(storageStatsQueryOptions),
      context.queryClient.ensureQueryData(backfillQueryOptions),
    ]);
  },
  errorComponent: ({ error }) => (
    <main className="mx-auto max-w-3xl p-6">
      <h1 className="text-lg font-semibold">emojistats ops</h1>
      <p className="mt-4 text-sm text-destructive-foreground">
        Failed to reach ClickHouse: {error.message}
      </p>
    </main>
  ),
  component: OpsDashboard,
});

const integer = new Intl.NumberFormat('en-US');
const compact = new Intl.NumberFormat('en-US', {
  notation: 'compact',
  maximumFractionDigits: 1,
});

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(1)} ${units[unit]}`;
}

function formatHour(chUtcDateTime: string): string {
  // ClickHouse returns "YYYY-MM-DD HH:MM:SS" in UTC; render local hour.
  const date = new Date(`${chUtcDateTime.replace(' ', 'T')}Z`);
  return `${date.getHours().toString().padStart(2, '0')}h`;
}

function OpsDashboard() {
  const { data: live } = useSuspenseQuery(liveStatsQueryOptions);
  const { data: storage } = useSuspenseQuery(storageStatsQueryOptions);
  const { data: backfill } = useSuspenseQuery(backfillQueryOptions);

  const stalled = live.freshnessSeconds > FRESHNESS_RED_SECONDS;

  return (
    <main className="mx-auto max-w-6xl space-y-4 p-4 pb-10">
      <header className="flex flex-wrap items-center justify-between gap-2">
        <h1 className="font-mono text-lg font-semibold tracking-tight">
          emojistats ops
        </h1>
        <div className="flex items-center gap-2">
          <Badge
            variant={stalled ? 'destructive' : 'outline'}
            className="tabular-nums"
          >
            <span
              className={`size-1.5 rounded-full ${stalled ? 'bg-white' : 'bg-emerald-600'}`}
            />
            {stalled
              ? `stalled · ${integer.format(live.freshnessSeconds)}s behind`
              : `live · ${integer.format(live.freshnessSeconds)}s`}
          </Badge>
          <span
            className="text-xs text-muted-foreground tabular-nums"
            suppressHydrationWarning
          >
            {new Date(live.generatedAt).toLocaleTimeString()}
          </span>
        </div>
      </header>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Live ingest</CardTitle>
          <CardDescription>Jetstream worker, all-time totals</CardDescription>
        </CardHeader>
        <CardContent className="grid grid-cols-2 gap-x-4 gap-y-5 sm:grid-cols-4">
          <Stat
            label="posts/s · 1m"
            value={live.postsPerSec1m.toFixed(1)}
            sub={`${integer.format(Math.round(live.postsPerSec1m * 60))}/min`}
          />
          <Stat
            label="posts/s · 15m"
            value={live.postsPerSec15m.toFixed(1)}
            sub={`${integer.format(Math.round(live.postsPerSec15m * 60))}/min`}
          />
          <Stat
            label="freshness"
            value={`${integer.format(live.freshnessSeconds)}s`}
            sub={stalled ? 'worker stalled?' : 'max ingested_at lag'}
            alert={stalled}
          />
          <Stat
            label="posts total"
            value={compact.format(live.totals.posts)}
            sub={integer.format(live.totals.posts)}
          />
          <Stat
            label="with emojis"
            value={compact.format(live.totals.postsWithEmojis)}
            sub={`${(live.totals.emojiShare * 100).toFixed(1)}% of posts`}
          />
          <Stat
            label="emoji occurrences"
            value={compact.format(live.totals.emojiOccurrences)}
            sub={integer.format(live.totals.emojiOccurrences)}
          />
          <Stat
            label="distinct glyphs"
            value={integer.format(live.totals.distinctGlyphs)}
          />
        </CardContent>
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Top emojis</CardTitle>
            <CardDescription>
              by occurrences — divergence from posts marks spam
            </CardDescription>
          </CardHeader>
          <CardContent>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-xs text-muted-foreground">
                  <th className="pb-2 font-medium">emoji</th>
                  <th className="pb-2 text-right font-medium">occurrences</th>
                  <th className="pb-2 text-right font-medium">posts</th>
                  <th className="pb-2 text-right font-medium">occ/post</th>
                </tr>
              </thead>
              <tbody className="tabular-nums">
                {live.topEmojis.map((row) => {
                  const ratio = row.posts > 0 ? row.occurrences / row.posts : 0;
                  return (
                    <tr key={row.emoji} className="border-b border-border/50">
                      <td className="py-1.5 text-base">{row.emoji}</td>
                      <td className="py-1.5 text-right">
                        {integer.format(row.occurrences)}
                      </td>
                      <td className="py-1.5 text-right">
                        {integer.format(row.posts)}
                      </td>
                      <td
                        className={`py-1.5 text-right ${
                          ratio >= 5
                            ? 'font-semibold text-amber-600'
                            : 'text-muted-foreground'
                        }`}
                      >
                        {ratio.toFixed(1)}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Languages</CardTitle>
            <CardDescription>top 8 by emoji occurrences</CardDescription>
          </CardHeader>
          <CardContent>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-xs text-muted-foreground">
                  <th className="pb-2 font-medium">lang</th>
                  <th className="pb-2 text-right font-medium">occurrences</th>
                  <th className="pb-2 text-right font-medium">posts</th>
                </tr>
              </thead>
              <tbody className="tabular-nums">
                {live.languages.map((row) => (
                  <tr key={row.lang} className="border-b border-border/50">
                    <td className="py-1.5 font-mono text-xs">
                      {row.lang || '∅'}
                    </td>
                    <td className="py-1.5 text-right">
                      {integer.format(row.occurrences)}
                    </td>
                    <td className="py-1.5 text-right">
                      {integer.format(row.posts)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </CardContent>
        </Card>
      </div>

      <HourlyChart hourly={storage.hourly} />

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">ClickHouse health</CardTitle>
            <CardDescription>
              {integer.format(storage.totalParts)} active parts ·{' '}
              {formatBytes(storage.totalBytes)} on disk
            </CardDescription>
          </CardHeader>
          <CardContent>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-xs text-muted-foreground">
                  <th className="pb-2 font-medium">table</th>
                  <th className="pb-2 text-right font-medium">parts</th>
                  <th className="pb-2 text-right font-medium">on disk</th>
                </tr>
              </thead>
              <tbody className="tabular-nums">
                {storage.tables.map((row) => (
                  <tr key={row.table} className="border-b border-border/50">
                    <td className="py-1.5 font-mono text-xs">{row.table}</td>
                    <td className="py-1.5 text-right">
                      {integer.format(row.parts)}
                    </td>
                    <td className="py-1.5 text-right">
                      {formatBytes(row.bytesOnDisk)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </CardContent>
        </Card>

        <BackfillPanel status={backfill} />
      </div>
    </main>
  );
}

function Stat({
  label,
  value,
  sub,
  alert = false,
}: {
  label: string;
  value: string;
  sub?: string;
  alert?: boolean;
}) {
  return (
    <div className="space-y-0.5">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p
        className={`text-xl leading-tight font-semibold tabular-nums ${
          alert ? 'text-red-600' : ''
        }`}
      >
        {value}
      </p>
      {sub ? (
        <p
          className={`text-xs tabular-nums ${
            alert ? 'text-red-600/80' : 'text-muted-foreground'
          }`}
        >
          {sub}
        </p>
      ) : null}
    </div>
  );
}

const hourlyChartConfig = {
  posts: { label: 'posts', color: 'var(--chart-2)' },
  postsWithEmojis: { label: 'with emojis', color: 'var(--chart-1)' },
} satisfies ChartConfig;

function HourlyChart({
  hourly,
}: {
  hourly: Array<{ hour: string; posts: number; postsWithEmojis: number }>;
}) {
  const data = hourly.map((row) => ({ ...row, label: formatHour(row.hour) }));
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Last 24h</CardTitle>
        <CardDescription>posts per hour (local time)</CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer
          config={hourlyChartConfig}
          className="aspect-auto h-48 w-full"
        >
          <BarChart data={data} margin={{ left: 4, right: 4 }}>
            <CartesianGrid vertical={false} strokeOpacity={0.3} />
            <XAxis
              dataKey="label"
              tickLine={false}
              axisLine={false}
              tickMargin={6}
              fontSize={10}
            />
            <ChartTooltip content={<ChartTooltipContent />} />
            <Bar
              dataKey="posts"
              fill="var(--color-posts)"
              radius={[2, 2, 0, 0]}
            />
            <Bar
              dataKey="postsWithEmojis"
              fill="var(--color-postsWithEmojis)"
              radius={[2, 2, 0, 0]}
            />
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}

// Full RepoStatus set from THE registry (backfill/types), reordered for
// display only: 'unreachable' (still retried) renders with the live statuses,
// ahead of the terminal ones. New statuses append automatically.
const BACKFILL_STATUS_LEAD: ReadonlyArray<RepoStatus> = [
  'pending',
  'fetching',
  'loaded',
  'verified',
  'empty',
  'unreachable',
];
const BACKFILL_STATUS_ORDER: ReadonlyArray<RepoStatus> = [
  ...BACKFILL_STATUS_LEAD,
  ...REPO_STATUSES.filter((status) => !BACKFILL_STATUS_LEAD.includes(status)),
];

function BackfillPanel({ status }: { status: BackfillStatus | null }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Backfill</CardTitle>
        <CardDescription>full-network crawl telemetry</CardDescription>
        <CardAction>
          <Link
            to="/backfill"
            className="text-xs font-medium text-muted-foreground underline-offset-4 hover:text-foreground hover:underline"
          >
            details →
          </Link>
        </CardAction>
      </CardHeader>
      <CardContent>
        {status === null ? (
          <p className="text-sm text-muted-foreground">
            no crawl telemetry yet
          </p>
        ) : (
          <div className="space-y-4">
            <div className="flex flex-wrap gap-1.5">
              {BACKFILL_STATUS_ORDER.map((key) => (
                <Badge key={key} variant="secondary" className="tabular-nums">
                  {key} {integer.format(status.statusCounts[key])}
                </Badge>
              ))}
            </div>
            <div className="grid grid-cols-2 gap-x-4 gap-y-5 sm:grid-cols-3">
              <Stat
                label="repos/min"
                value={integer.format(status.reposPerMin)}
              />
              <Stat
                label="posts loaded"
                value={compact.format(status.postsLoaded)}
                sub={integer.format(status.postsLoaded)}
              />
              <Stat
                label="ETA"
                value={
                  status.etaHours === null
                    ? '—'
                    : `${status.etaHours.toFixed(1)}h`
                }
              />
            </div>
            {status.lastError ? (
              <p className="text-xs break-all text-red-600">
                last error: {status.lastError}
              </p>
            ) : null}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
