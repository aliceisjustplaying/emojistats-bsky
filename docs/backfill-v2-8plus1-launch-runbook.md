# Backfill v2 8+1 Launch Runbook

Goal: keep `getRepo` as the only expected bottleneck by overlapping fetch/archive with derive,
while a ninth temporary dedi absorbs ClickHouse ingest and aggregate rebuild.

## Topology

- 8 worker dedis: `run-fleet` fetch/archive plus local derive workers.
- 1 temporary ClickHouse dedi: ClickHouse ingest, aggregate rebuild, export to prod.
- Existing prod VPS: serving only until final validated data is copied over.

The handoff contract is the committed archive manifest. Derive workers only consume manifest
entries after archive objects and receipts have been committed.

## Shared Control Directory

The ClickHouse insert throttle uses POSIX file locks, so the slot directory must be on a shared
filesystem with working advisory locks. Use a tiny NFS export from the temporary ClickHouse dedi;
this carries only lock files and claim ledgers, not archive data.

On the temporary ClickHouse dedi:

```bash
sudo mkdir -p /srv/emojistats-control/insert-slots
sudo chown -R "$USER":"$USER" /srv/emojistats-control
```

On each worker dedi, mount that directory at:

```bash
sudo mkdir -p /mnt/emojistats-control
sudo mount -t nfs -o vers=4.2 TEMP_CH_PRIVATE_IP:/srv/emojistats-control /mnt/emojistats-control
```

## Build

Run on all worker dedis and the temporary ClickHouse dedi:

```bash
cd /srv/emojistats-bsky/rust
GIT_REV="$(git rev-parse --short=12 HEAD)" cargo build --release -p emojistats-backfill
```

## Temporary ClickHouse Setup

On the temporary ClickHouse dedi:

```bash
export CH_DB=emojistats_v2_backfill_$(date -u +%Y%m%d_%H%M%S)
clickhouse client --query "CREATE DATABASE IF NOT EXISTS ${CH_DB} ENGINE = Atomic"
./target/release/emojistats-backfill clickhouse-schema \
  --clickhouse-database "$CH_DB" \
  | clickhouse client --multiquery
```

Probe ingest health during launch:

```bash
clickhouse client --query "
SELECT count() AS active_parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS disk
FROM system.parts
WHERE database = '${CH_DB}' AND active"

clickhouse client --query "
SELECT count() AS active_merges
FROM system.merges
WHERE database = '${CH_DB}'"
```

## Worker Fetch/Archive

Each worker gets one DID shard file and one local archive root. Use release binaries only.

```bash
export RUN_ID=backfill-v2-$(date -u +%Y%m%dT%H%M%SZ)
export WORKER_ID="$(hostname -s)"
export DID_FILE=/srv/emojistats/dids/${WORKER_ID}.dids
export LEDGER=/srv/emojistats/ledger/${WORKER_ID}.sqlite
export ARCHIVE_ROOT=/srv/emojistats/archive
export SPOOL=/srv/emojistats/spool
export METRICS=/srv/emojistats/metrics/${WORKER_ID}.fleet.jsonl
export EMOJISTATS_CANARY_HMAC_KEY="$(cat /home/agent/.secrets/emojistats-canary-hmac-key)"

./target/release/emojistats-backfill run-fleet "$DID_FILE" \
  --ledger-path "$LEDGER" \
  --run-id "$RUN_ID" \
  --claim-limit 100000000 \
  --concurrency 256 \
  --parse-concurrency 4 \
  --spool-dir "$SPOOL" \
  --archive-dir "$ARCHIVE_ROOT" \
  --archive-backend local \
  --canary-evidence /srv/emojistats/canary-signed.jsonl \
  --metrics-jsonl "$METRICS"
```

Tune `--concurrency` per host behavior; do not raise it if PDS 429s or dead-host backoff dominate.

## Overlapped Derive

Run derive loops on workers while `run-fleet` is still appending manifests. Start with one or two
derive loops per worker; the shared slot directory globally caps simultaneous ClickHouse inserts.

```bash
export CH_URL=http://TEMP_CH_PRIVATE_IP:8123
export CH_DB=emojistats_v2_backfill_YYYYMMDD_HHMMSS
export WORKER_ID="$(hostname -s)"
export ARCHIVE_ROOT=/srv/emojistats/archive
export MANIFEST="$(find "$ARCHIVE_ROOT/manifests" -type f -name '*.jsonl' | sort | tail -n 1)"
export CLAIM_LEDGER=/srv/emojistats/derive/${WORKER_ID}.claims.jsonl
export DERIVE_LEDGER=/srv/emojistats/derive/${WORKER_ID}.derive-ledger.jsonl
export SLOT_DIR=/mnt/emojistats-control/insert-slots

test -n "$MANIFEST" && test -f "$MANIFEST"

while true; do
  ./target/release/emojistats-backfill derive-manifest "$MANIFEST" \
    --archive-root "$ARCHIVE_ROOT" \
    --clickhouse-url "$CH_URL" \
    --clickhouse-database "$CH_DB" \
    --derive-ledger-path "$DERIVE_LEDGER" \
    --claim-ledger-path "$CLAIM_LEDGER" \
    --claim-worker-id "${WORKER_ID}-derive-1" \
    --claim-max-rows 5000000 \
    --claim-max-entries 64 \
    --claim-stale-seconds 3600 \
    --clickhouse-insert-slots-dir "$SLOT_DIR" \
    --clickhouse-insert-slots 4 \
    --clickhouse-insert-slot-timeout-secs 300 \
    --metrics-jsonl "/srv/emojistats/metrics/${WORKER_ID}.derive.jsonl" \
    2>&1 | tee -a "/srv/emojistats/logs/${WORKER_ID}.derive.log"

  sleep 10
done
```

Raise `--clickhouse-insert-slots` globally by changing all derive loops to `6` or `8` only after
`system.merges` stays near zero and insert latency remains stable. Lower it if active parts,
active merges, memory, or insert latency climb.

## Drain

When all `run-fleet` processes finish:

```bash
pgrep -af 'emojistats-backfill run-fleet' || true
```

Keep derive loops running until every worker repeatedly prints:

```text
derive_manifest_claim none
```

Then stop derive loops and verify ClickHouse row count is stable:

```bash
clickhouse client --query "SELECT count() FROM ${CH_DB}.v2_post_serving_r3"
```

## Aggregate Rebuild

On the temporary ClickHouse dedi:

```bash
./target/release/emojistats-backfill clickhouse-rebuild-aggregates \
  --clickhouse-url http://localhost:8123 \
  --clickhouse-database "$CH_DB"
```

Sanity checks:

```bash
clickhouse client --query "SELECT count() FROM ${CH_DB}.v2_post_serving_r3"
clickhouse client --query "SELECT count() FROM ${CH_DB}.v2_emoji_total_r3"
clickhouse client --query "SELECT count() FROM ${CH_DB}.v2_posts_hourly_r3"
```

## Copy To Prod VPS

Keep prod serving the old dataset until the temp ClickHouse database is validated.

If both ClickHouse servers can talk over native protocol, copy table-by-table from the temporary
ClickHouse dedi:

```bash
export PROD_HOST=PROD_PRIVATE_IP
export PROD_DB=emojistats_v2_serving
clickhouse client --query "CREATE DATABASE IF NOT EXISTS ${PROD_DB} ENGINE = Atomic"
./target/release/emojistats-backfill clickhouse-schema --clickhouse-database "$PROD_DB" \
  | clickhouse client --host "$PROD_HOST" --multiquery

for table in \
  v2_post_serving_r3 \
  v2_total_post_counters_r3 \
  v2_emoji_total_r3 \
  v2_emoji_total_by_lang_r3 \
  v2_lang_total_r3 \
  v2_posts_hourly_r3
do
  clickhouse client --query "
    INSERT INTO FUNCTION remote('${PROD_HOST}', '${PROD_DB}', '${table}')
    SELECT * FROM ${CH_DB}.${table}"
done
```

Cut the site over only after prod row counts match the temporary ClickHouse counts.
