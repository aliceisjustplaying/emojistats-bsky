# Backfill Rewrite Plan (2025-11-10T21:30:02Z)

Goal: adopt a Bluesky-style producer/consumer pipeline backed by Redis streams so each DID is enqueued exactly once, we get durable retries, and listRepos enumeration is decoupled from repo processing.

## 1. Establish Redis queue primitives

- Introduce a small queue module (e.g., `packages/backfill/queue/redisStream.ts`) that wraps `@redis/client`.
- Functions required:
  - `ensureStream({stream, group})` – create stream + consumer group if missing.
  - `appendRepoJob({did, pds, rev?, batchId?})` – add messages with payload JSON.
  - `readJobs({group, consumer, count, blockMs})` – XREADGROUP wrapper.
  - `ackJobs(ids)` and `claimStalledJobs({minIdleMs})`.
- Track stream length via `xlen` to support backpressure.

## 2. Producer service (listRepos → Redis)

- New entrypoint (e.g., `scripts/backfill-producer.ts`) that:
  - Loads existing `fetchPdses` + cursor cache logic.
  - For each PDS, streams repos using `fetchPdsDids`, but instead of yielding to an in-process queue, writes `{did, pds}` to Redis via `appendRepoJob`.
  - Honors a stream high-water mark: before enqueueing, call queue helper to check length; if above threshold, wait (e.g., exponential backoff + metrics).
  - Emits Prometheus metrics: repos enqueued, per-PDS cursor, wait time due to backpressure.
  - Stores per-PDS cursor in the existing JSON cache exactly when the enqueue succeeds, so re-runs resume correctly.
  - CLI command: `bun run backfill:producer`.

## 3. Consumer service (Redis → BackfillRunner)

- Refactor current `BackfillRunner.run()` so it no longer calls `fetchAllDids`. Instead:
  - Initialize Redis stream + group once (matching producer).
  - In an async loop, call `readJobs` with `count=repoConcurrency`, `blockMs` to avoid busy wait.
  - For each job, parse payload and call a `processJob` helper that wraps the existing repo processing logic.
  - On success, ACK the Redis IDs; on failure, leave pending (Redis will allow re-claim later) and increment metrics.
  - Use Redis “pending entries list” to reclaim stalled jobs periodically (e.g., worker died). Similar to Bluesky’s backfiller.
  - Keep the current per-repo validation + Timescale writes unchanged; only scheduling changes.
  - CLI command: `bun run backfill:consumer --consumer-name=worker-1`.

## 4. Durable in-flight tracking

- Replace the in-memory `Set` with Redis semantics:
  - Pending messages in a consumer group already act as “in-flight”; no additional tracking needed.
  - Still update `repo_progress` at start (mark `backfill_complete=false`) so non-Redis readers can skip a DID that’s in progress.
  - Add an API to requeue or drop a job if `repo_progress` says it already succeeded (defensive check in case of duplicates).

## 5. Backpressure + monitoring

- Expose metrics for:
  - Stream length (`XLEN`).
  - Pending vs acknowledged jobs.
  - Producer backoff duration.
  - Consumer throughput, failures, reclaim counts.
- Support env vars:
  - `BACKFILL_REDIS_URL`
  - `BACKFILL_STREAM_NAME`, `BACKFILL_GROUP_NAME`
  - `BACKFILL_HIGH_WATER` (default e.g., 50k)
  - `BACKFILL_CONSUMER_NAME`
- Document how to run multiple consumers (set different names) and how to inspect `XPENDING`.

## 6. Transitional strategy

- Keep the old `bun run backfill` path temporarily behind a flag for local tests.
- Provide a migration doc: how to start Redis, run producer + consumer, reset state, and monitor via metrics.
- Add scripts:
  - `scripts/queue/drain.ts` to trim processed entries.
  - `scripts/queue/pending.ts` to list/claim stuck jobs.

## 7. Testing & rollout

- Unit-test queue wrapper functions (mock Redis or use testcontainers).
- End-to-end: run producer + one consumer locally, verify queue length drops, duplicates disappear, `check-validation` quiet on clean DB.
- Once stable, remove the old fetchAllDids/PQueue path.

## Notes to future self

- Before switching, clear `packages/backfill/pds-cursor-cache.json` or ensure producers resume from the right cursor; otherwise we may re-enqueue old DIDs.
- Verify `EMOJISTATS_DATABASE_URL` / `BACKFILL_REDIS_URL` in both producer and consumer environments—they must match or we’ll see phantom duplicates again.
- Keep an “escape hatch” flag to fall back to the current pipeline until Redis infra is battle-tested.
