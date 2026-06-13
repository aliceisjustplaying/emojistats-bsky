# Adversarial code review - 2026-06-13

Scope: whole repository, with emphasis on correctness, performance, and maintainability.

Verification run:

- `bun run lint` passed with warnings.
- `bun run --cwd packages/ingest typecheck` passed.
- `bun run --cwd packages/backfill typecheck` passed.
- `bun run --cwd packages/archive typecheck` passed.
- `bun run --cwd packages/backend typecheck` passed.
- `bun run --cwd packages/dashboard typecheck` passed.
- `bun run --cwd packages/frontend typecheck` passed.
- `bun --filter '*' test` passed for archive, ingest, and backfill package tests.
- `bun run --cwd packages/frontend build` passed.
- `bun run --cwd packages/dashboard build` passed.
- `bun run formatting-check` passed.

## Findings

### High: dashboard polls raw `posts` at production cadence

File: `packages/dashboard/src/server/stats.ts`

`getLiveStats` polls raw `posts` every 2 seconds for rates, freshness, and all-time totals. With `posts` ordered by `(did, rkey)`, filters on `ingested_at` and aggregate reads such as `count()`, `countIf(notEmpty(emojis))`, and `sum(length(emojis))` do not have a useful sort key and become scale-killer scans at backfill size.

Impact: public dashboard requests can compete directly with ingest/backfill ClickHouse memory and CPU, causing 500s or OvercommitTracker kills under load.

Recommended fix: add dedicated aggregate/projection tables for live ingest rates, freshness, and totals, then query those from the dashboard. Keep raw `posts` scans for offline verification only.

### High: backfill loader can forget failed generation outcomes

File: `packages/backfill/src/loader.ts`

`finish()` treats a missing generation promise as success once `#runByGen` has evicted it after `GEN_RETENTION`. A repo that spans more than 128 flush generations can miss an earlier failed generation and still resolve `finish()`, allowing the ledger to mark the repo `loaded` despite failed rows.

Impact: rare but real durability violation for very large or long-running repos during ClickHouse insert failures.

Recommended fix: store the touched generation promises directly on each repo handle, or refcount generations and retain each generation result until all handles that touched it have finished.

### High: parse worker reply timeout does not cancel underlying work

File: `packages/backfill/src/parse-pool.ts`

The reply timeout rejects the scheduler-side promise, but the worker job continues running. If the root cause is a stuck fetch, dead socket, or pathological parse, the worker still holds that work while the scheduler frees the slot and dispatches more jobs.

Impact: a liveness fix can turn into unbounded worker backlog and memory/socket accumulation under repeated stalls.

Recommended fix: on timeout, terminate and respawn the worker that owns the timed-out job, rejecting all jobs assigned to that worker as retryable.

### High: parse workers materialize whole repos before loading

File: `packages/backfill/src/parse-worker.ts`

The worker collects every parsed post into `rows: ArchiveRow[]` and posts the full array back to the main thread. With `CAR_MAX_BYTES` at 1 GiB and multiple concurrent worker jobs, this can double memory through row materialization plus structured cloning.

Impact: whale repos can cause memory spikes or OOMs, especially when several workers complete around the same time.

Recommended fix: stream row batches from worker to main, or move archive/load handling into the worker with explicit batch acknowledgements.

### Medium: backfill event telemetry is not run-scoped

Files:

- `packages/ingest/src/clickhouse/schema.sql`
- `packages/backfill/src/telemetry.ts`
- `packages/dashboard/src/server/backfill.ts`

`backfill_progress` includes `run_id` and `shard`, but `backfill_repo_events` does not. The dashboard picks the latest run from `backfill_progress`, then combines it with all-time event totals from `backfill_repo_events`.

Impact: a second run will show latest-run status counts mixed with all prior posts/bytes/errors, making dashboard totals and recent issue feeds misleading.

Recommended fix: add `run_id` and `shard` to `backfill_repo_events`, write them from telemetry, and filter event queries by the selected latest run.

### Medium: live insert deduplication token is too weak

File: `packages/ingest/src/writer.ts`

The live writer dedup token is based on `rows.length`, first `(did,rkey)`, and last `(did,rkey)`. Two different batches with the same boundary tuple and length inside ClickHouse's dedup window would be treated as the same insert.

Impact: a distinct live batch can be dropped if a collision occurs. The current comment says this is unrealistic, but the token is cheap to make collision-resistant.

Recommended fix: hash all `(did,rkey)` pairs, or hash the serialized row payload, and use that digest in the token.

### Medium: archive sync command uses shell interpolation

File: `packages/archive/src/sink.ts`

`syncCommand.replaceAll('{file}', finalPath)` is executed through `/bin/sh -c`. If the archive directory or filename ever contains shell metacharacters, or if the command is configured incorrectly, the shell can execute unintended syntax.

Impact: operational footgun in the only durable home for full-text archive data.

Recommended fix: model sync as argv templating, or pass the file path through an environment variable and execute a fixed command without shell interpolation.

### Medium: frontend language tabs are uncontrolled

File: `packages/frontend/src/components/LanguageTabs.tsx`

`LanguageTabs` accepts `selectedLanguage` but does not use it. The component uses `defaultIndex`, so tab UI state can drift from app state when language rows refresh or reorder.

Impact: users can see one selected tab while the grid is showing another language's emoji data.

Recommended fix: make `Tabs` controlled with `selectedIndex` derived from `selectedLanguage`, and handle missing/reordered language entries.

### Low: selected-language emoji requests poll once per second per client

File: `packages/frontend/src/App.tsx`

The frontend emits `getTopEmojisForLanguage` every second while a language is selected. The backend has a 1-second cache, but each connected client still causes repeated Socket.IO traffic and cache churn.

Impact: avoidable request noise that scales with connected clients.

Recommended fix: request on selection and refresh from a shared server cadence, or raise the interval to match the global emit cadence.

### Low: emoji normalization map construction is O(n^2)

File: `packages/emoji-normalization/emojiNormalization.ts`

`normalizationMap` uses object spread inside a reducer. That copies the accumulated object on every iteration.

Impact: avoidable module-load cost.

Recommended fix: mutate the accumulator, matching the `nonQualifiedMap` pattern below it.
