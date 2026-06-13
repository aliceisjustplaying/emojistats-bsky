# Dashboard `getLiveStats` runs full-table scans on raw `posts` at live cadence

**Severity:** High (from `docs/adversarial-code-review-2026-06-13.md`)
**File:** `packages/dashboard/src/server/stats.ts` (`getLiveStats`)

## What's wrong

`getLiveStats` is polled live (~2s per the review) and fires five queries in
parallel. Two already hit aggregate tables and are fine (`topEmojis` ←
`emoji_total`, `languages` ← `lang_total`). The other **three scan raw `posts`**,
which is a `ReplacingMergeTree ORDER BY (did, rkey)` at backfill scale (billions
of rows):

- **rates:** `countIf(ingested_at >= now()-1m), count() FROM posts WHERE ingested_at >= now()-15m`
  — filters on `ingested_at`, but the sort key is `(did, rkey)`, so there's no
  sort-key support; it reads the `ingested_at` column across every part.
- **freshness:** `SELECT now() - max(ingested_at) FROM posts` — `max` over the
  whole table, same lack of sort-key help.
- **totals:** `count(), countIf(notEmpty(emojis)), sum(length(emojis)) FROM posts`
  — no `WHERE` at all; full-table aggregate every poll.

## Impact

Every dashboard client poll competes with ingest/backfill for ClickHouse memory
and CPU. Under load this causes 500s or OvercommitTracker (memory-overcommit)
kills — and it's worst exactly during backfill, when the table is largest and
ingest is hottest.

## Existing infra the fix should reuse

The schema already has SummingMergeTree aggregate tables fed by MVs at insert
time — `posts_hourly(hour, posts, posts_with_emojis, emoji_occurrences)`,
`emoji_total`, `lang_total`, etc. Pattern: query them with `sum() + GROUP BY`
(merges settle lazily; never `FINAL`).

## Recommended fix

- **totals** → derive from `posts_hourly` (`sum(posts)`, `sum(posts_with_emojis)`,
  `sum(emoji_occurrences)` across all hours); `distinct_glyphs` already comes from
  `emoji_total`. No raw-`posts` read needed.
- **rates** → `posts_hourly` is hour-granular, too coarse for 1m/15m. Add a
  minute-bucketed SummingMergeTree MV (e.g. `posts_minutely(minute, posts)` keyed
  on `toStartOfMinute(ingested_at)`) and read recent buckets from it.
- **freshness** → track `max(ingested_at)` cheaply: an `AggregatingMergeTree` with
  `maxState(ingested_at)` fed by the same MV (or read the latest minute bucket).
  Avoid scanning `posts`.
- Keep raw-`posts` scans for offline verify/rebuild only.

Alternative if you'd rather not add tables: a ClickHouse `PROJECTION` on `posts`
ordered by `ingested_at` would let the rates/freshness queries use a sorted
projection instead of a full scan — but that adds storage + merge cost on a
billion-row table, and cuts against the codebase's "aggregates are separate
SummingMergeTree tables" convention. The MV route is preferred.

## Semantic caveat

The current `totals` query deliberately accepts raw `count()` inflation
(transient at-least-once dupes that merges erase) — see its inline comment,
"ops-precision, not accounting-precision." `posts_hourly` sums carry a *slightly
different* inflation profile (the MV counts every arrival at insert, including
rkey-reuse re-arrivals that don't merge away until a rebuild). Both are
approximate live numbers; neither should be mistaken for verify-grade exactness.

## Verification

After the change, run the dashboard against a populated ClickHouse and confirm:

- the three queries no longer touch `posts` (`EXPLAIN` / `system.query_log`
  read-rows), and
- the displayed live numbers stay sane vs the old values.
