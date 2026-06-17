# ADR 0002: v2 Serving Projection Is Compact And No-Delete

## Status

Accepted.

## Context

The v2 Rust backfill writes full observed post rows to Storage Box `Parquet` with receipts,
proof metadata, row hashes, and manifests. That archive is the durable corpus and the audit
surface.

The public website needs fast emoji totals, language totals, and time buckets. It does not need
full post text or `extras_json` in `ClickHouse`.

The product semantics match v1 and emojitracker-style counting: once a post has been observed,
absence in a later crawl is not a delete. Bluesky AppView does not expose post edits today, but
AT Protocol can represent record updates; for repeated `(did, collection, rkey)` observations,
the latest observed content wins in the serving projection.

## Decision

`ClickHouse` serving state is a compact projection, not the full observation log.

Storage Box `Parquet` remains the complete observed corpus. It stores `did`, `rkey`, `cid`, raw
and normalized timestamp fields, parse status, full text, languages, emoji sequence, extras,
normalizer identity, receipt hashes, and proof metadata.

`ClickHouse` stores one compact post row per serving identity:

```text
(dataset, fetch_method, completeness_class, did, rkey)
```

The single ingested collection is `app.bsky.feed.post`, so `(did, rkey)` is sufficient inside a
proof lane while that collection invariant is enforced.

The compact row stores no full text and no extras. It stores proof-lane dimensions, normalizer
identity, receipt lineage, `created_at`, timestamp parse status, languages, and the normalized
emoji sequence as an array.

`ClickHouse` uses replacement by observation/version time for repeated keys. A later observation
of the same key replaces the serving row. A later observation that omits the key does nothing.
There is no tombstone path and no delete path.

Aggregate tables and query-facing totals are derived from the compact post-serving table. Rebuild
jobs use `arrayJoin(emojis)` for emoji totals and language-by-emoji totals. Website requests read
the aggregate tables, not the compact post table.

## Consequences

`ClickHouse` stays close to v1 size because it stores one compact post row, not full text and not
one row per emoji occurrence.

Replay and recrawl correctness depends on deterministic serving keys and dedupe tokens, while
auditability depends on Storage Box receipts and manifests.

Aggregate rebuilds are batch compute. Realtime serving reads remain cheap because they hit
precomputed aggregate tables.

Historical observation analysis comes from Storage Box manifests and `Parquet`, not from
`ClickHouse` serving rows.
