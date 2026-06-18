---
license: other
pretty_name: EmojiStats Bluesky Archive
language:
  - multilingual
tags:
  - bluesky
  - atproto
  - emoji
  - parquet
size_categories:
  - 1B<n<10B
---

# EmojiStats Bluesky Archive

This dataset contains parquet exports produced by the EmojiStats Bluesky backfill and live ingest pipeline.

## Contents

- `shard0/` through `shard5/`: original backfill parquet files.
- `v1-recrawl/`: archive-only metadata recrawl overlay. When a post exists in both the original shard parquet and this tree, prefer the `v1-recrawl` row.
- `live/`: post-backfill live ingest parquet files.

The parquet files are intentionally published in their operational layout instead of being physically compacted into one corpus. Consumers that need a unified view should read all three groups and deduplicate by stable post identity, normally `(did, rkey)`, preferring `v1-recrawl` over `shard*` rows for duplicates.

## Query Example

```sql
with all_rows as (
  select 0 as priority, * from read_parquet('shard*/*.parquet', union_by_name = true)
  union all by name
  select 1 as priority, * from read_parquet('v1-recrawl/worker*/*.parquet', union_by_name = true)
)
select *
from all_rows
qualify row_number() over (
  partition by did, rkey
  order by priority desc
) = 1;
```

## Notes

The archive reflects records observable during the crawl and live ingest windows. It does not reconstruct records deleted before observation, and some unavailable or policy-terminal repositories are absent.
