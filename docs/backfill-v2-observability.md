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

## Metric contract

All launch metrics use the `backfill_` prefix and Prometheus-compatible names. Each
metric has one owner scope, one instrument type, and a bounded label set.

Allowed `stage` values:

- `claim`
- `fetch`
- `list_records_fetch`
- `fallback_list_records`
- `parse_wait`
- `parse_start`
- `parse_archive`
- `archive_commit`
- `derive_manifest_scan`
- `derive_file_read`
- `derive_receipt_verify`
- `derive_clickhouse_commit`
- `storage_box_upload`
- `storage_box_commit`
- `clickhouse_insert`
- `complete`

Allowed `pressure_state` values:

- `host_pacing`
- `fetch_byte_budget`
- `disk_pressure`
- `parse_backpressure`
- `parse_active`
- `storage_box_backpressure`
- `clickhouse_backpressure`
- `rate_limit_sleep`
- `operator_pause`

Pressure states are gauges or event labels, not error names. A watchdog may treat them as
an intentional pause only while the process is still alive and the state is freshly
observed.

## Required launch metrics

Fleet:

- `backfill_fleet_repos_claimed_total`
  - instrument: counter
  - stage: `claim`
  - labels: `run_id`, `worker_id`, `shard`
- `backfill_fleet_attempts_total`
  - instrument: counter
  - stage: `fetch`, `list_records_fetch`, `parse_archive`, `archive_commit`, `complete`
  - labels: `run_id`, `worker_id`, `shard`, `host`, `stage`, `outcome`
- `backfill_fleet_attempt_duration_seconds`
  - instrument: histogram
  - stage: `complete`
  - labels: `run_id`, `worker_id`, `shard`, `host`, `outcome`
- `backfill_fleet_active_attempts`
  - instrument: gauge
  - stage: active stage value
  - labels: `run_id`, `worker_id`, `shard`, `stage`
- `backfill_fleet_pressure_state`
  - instrument: gauge
  - stage: stage that is paused or constrained
  - labels: `run_id`, `worker_id`, `shard`, `host`, `stage`, `pressure_state`
- `backfill_fleet_stale_claims_recovered_total`
  - instrument: counter
  - stage: `claim`
  - labels: `run_id`, `worker_id`, `shard`, `outcome`

Derive:

- `backfill_derive_manifest_entries_seen_total`
  - instrument: counter
  - stage: `derive_manifest_scan`
  - labels: `run_id`, `shard`
- `backfill_derive_files_read_total`
  - instrument: counter
  - stage: `derive_file_read`
  - labels: `run_id`, `shard`, `backend`
- `backfill_derive_rows_verified_total`
  - instrument: counter
  - stage: `derive_receipt_verify`
  - labels: `run_id`, `shard`, `outcome`
- `backfill_derive_clickhouse_batches_committed_total`
  - instrument: counter
  - stage: `derive_clickhouse_commit`
  - labels: `run_id`, `shard`, `outcome`
- `backfill_derive_batch_duration_seconds`
  - instrument: histogram
  - stage: `derive_file_read`, `derive_receipt_verify`, `derive_clickhouse_commit`
  - labels: `run_id`, `shard`, `stage`, `outcome`

Storage Box:

- `backfill_storage_box_uploads_total`
  - instrument: counter
  - stage: `storage_box_upload`
  - labels: `run_id`, `shard`, `backend`, `outcome`
- `backfill_storage_box_upload_bytes_total`
  - instrument: counter
  - stage: `storage_box_upload`
  - labels: `run_id`, `shard`, `backend`, `outcome`
- `backfill_storage_box_commit_duration_seconds`
  - instrument: histogram
  - stage: `storage_box_commit`
  - labels: `run_id`, `shard`, `backend`, `outcome`
- `backfill_storage_box_backpressure_seconds_total`
  - instrument: counter
  - stage: `storage_box_upload`, `storage_box_commit`
  - labels: `run_id`, `shard`, `backend`, `stage`, `pressure_state`

ClickHouse:

- `backfill_clickhouse_insert_batches_total`
  - instrument: counter
  - stage: `clickhouse_insert`
  - labels: `run_id`, `shard`, `outcome`
- `backfill_clickhouse_insert_rows_total`
  - instrument: counter
  - stage: `clickhouse_insert`
  - labels: `run_id`, `shard`, `outcome`
- `backfill_clickhouse_insert_duration_seconds`
  - instrument: histogram
  - stage: `clickhouse_insert`
  - labels: `run_id`, `shard`, `outcome`
- `backfill_clickhouse_retries_total`
  - instrument: counter
  - stage: `clickhouse_insert`
  - labels: `run_id`, `shard`, `outcome`
- `backfill_clickhouse_dedupe_conflicts_total`
  - instrument: counter
  - stage: `clickhouse_insert`
  - labels: `run_id`, `shard`

## JSONL launch proof

`run-fleet` and `derive-manifest` accept `--metrics-jsonl <path>`. The JSONL
recorder emits the same typed metric names, scopes, kinds, and labels documented here,
one `backfill_metric` event per line.

Use this for canaries and early daemon runs before the final Prometheus/OpenTelemetry
exporter lands:

```bash
emojistats-backfill run-fleet fixtures/scale-smoke.dids \
  --bypass-canary \
  --metrics-jsonl data/canary/fleet-metrics.jsonl

emojistats-backfill derive-manifest data/canary/all-raw-manifests.jsonl \
  --archive-root data/canary/archive \
  --metrics-jsonl data/canary/derive-metrics.jsonl
```

The recorder is intentionally append-only. A launcher should rotate paths per run or
per worker.

## Prometheus / OTel shape

The Rust crate should expose an internal metrics adapter with two implementations:

- noop adapter for tests, local smoke, and `--dry-run`;
- JSONL adapter for canaries and launch dry-runs;
- OTel adapter that exports Prometheus-compatible names through the OTel SDK.

The runtime path should call typed observation functions at stage boundaries, not construct
metric names inline. That keeps fleet, derive, Storage Box, and ClickHouse labels consistent
when the exporter changes.

## Watchdog contract

Watchdogs should restart only when none of the relevant progress counters advance and no
declared pressure state explains the pause. Logs can explain a stall, but counters decide
whether work advanced.
