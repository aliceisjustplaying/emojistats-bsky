# Backfill v2 observability launch scaffold

This note bridges the current smoke telemetry to a Prometheus/OpenTelemetry launch path
without changing crawler behavior.

## Current state

- `fetch-one` emits `smoke_telemetry {json}` lines from `failure.rs`.
- Fleet mode emits terminal summaries and per-attempt stderr failures, but has no stable
  metrics surface yet.
- `docs/backfill-v2-design.md` already names the required crawler and derive progress
  vectors. Those vectors are the contract for the first metrics pass.

## First metrics surface

Use counters and histograms for work that has completed, and gauges only for live state
that can go stale.

Labels must stay low-cardinality:

- `run_id`
- `worker_id`
- `shard`
- `host`
- `stage`
- `outcome`
- `pressure_state`
- `backend`

Do not label metrics by DID, rkey, path, manifest hash, receipt hash, or error string.
Those belong in logs and receipts.

## Required launch metrics

Fleet:

- `backfill_fleet_repos_claimed_total`
- `backfill_fleet_attempts_total`
- `backfill_fleet_attempt_duration_seconds`
- `backfill_fleet_active_attempts`
- `backfill_fleet_pressure_state`
- `backfill_fleet_stale_claims_recovered_total`

Derive:

- `backfill_derive_manifest_entries_seen_total`
- `backfill_derive_files_read_total`
- `backfill_derive_rows_verified_total`
- `backfill_derive_clickhouse_batches_committed_total`
- `backfill_derive_batch_duration_seconds`

Storage Box:

- `backfill_storage_box_uploads_total`
- `backfill_storage_box_upload_bytes_total`
- `backfill_storage_box_commit_duration_seconds`
- `backfill_storage_box_backpressure_seconds_total`

ClickHouse:

- `backfill_clickhouse_insert_batches_total`
- `backfill_clickhouse_insert_rows_total`
- `backfill_clickhouse_insert_duration_seconds`
- `backfill_clickhouse_retries_total`
- `backfill_clickhouse_dedupe_conflicts_total`

## Prometheus / OTel shape

The Rust crate should expose an internal metrics adapter with two implementations:

- noop adapter for tests, local smoke, and `--dry-run`;
- OTel adapter that exports Prometheus-compatible names through the OTel SDK.

The runtime path should call typed observation functions at stage boundaries, not construct
metric names inline. That keeps fleet, derive, Storage Box, and ClickHouse labels consistent
when the exporter changes.

## Watchdog contract

Watchdogs should restart only when none of the relevant progress counters advance and no
declared pressure state explains the pause. Logs can explain a stall, but counters decide
whether work advanced.
