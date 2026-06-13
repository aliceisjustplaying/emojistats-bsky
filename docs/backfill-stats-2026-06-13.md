# Backfill statistics — snapshot 2026-06-13 ~16:00 UTC

Authoritative per-shard counts pulled directly from the six crawl-box ledgers
(`repos` table, `bucket = shard index`), plus ClickHouse and the storagebox
archive inventory. Captured at the start of the crawl1/crawl2 retirement, so
the "remaining" numbers are what a later mop-up pass must finish.

## Fleet totals (95,470,241 repos enumerated)

| Category | Repos | % | Meaning |
|---|---|---|---|
| **Captured (real data)** | **35,892,016** | **37.6%** | crawled, content in ClickHouse + archive |
| — loaded | 9,244,838 | | posts written |
| — verified | 2,570,526 | | loaded + reconciled |
| — empty | 24,076,652 | | crawled, account has 0 posts (normal on Bluesky) |
| **Terminal / no data** | **24,275,332** | **25.4%** | resolved, nothing to capture |
| — failed | 21,060,875 | | non-transient (mostly 404 RepoNotFound) after max attempts |
| — takendown | 2,781,370 | | account taken down |
| — deactivated | 413,830 | | account deactivated |
| — tombstoned | 2,305 | | account tombstoned |
| — quarantined | 16,952 | | CAR exceeded 1 GiB cap, aborted |
| **Parked unreachable** | **30,989,049** | **32.5%** | host down — NOT crawled (see note) |
| **REMAINING (work left)** | **4,313,844** | **4.5%** | pending 4,282,780 + fetching 31,064 |

**Resolved (everything except remaining): 91,156,397 — 95.48% of enumeration.**

Note on *unreachable*: dominated by genuinely-dead hosts — `pds.trump.com`
(~4.7M/shard, DNS NXDOMAIN/ENOTFOUND) and `plc.surge.sh` (http 451 legal block),
plus test/junk PDSes. These are not re-crawlable (the host is gone) and the
final sweep correctly skips them. A smaller slice is budget-exhausted rows on
*alive* hosts (e.g. `morel.us-east.host.bsky.network` ~12k/shard from 429
storms); those are NOT dead-listed and ARE recoverable by `--final-sweep`.

## Per-shard breakdown

| shard | total | loaded | verified | empty | failed | unreachable | takendown | **remaining** |
|---|---|---|---|---|---|---|---|---|
| 0 | 15,908,424 | 1,703,577 | 133,522 | 3,910,467 | 3,533,042 | 5,220,624 | 470,096 | **864,137** |
| 1 | 15,906,298 | 1,905,950 | 437,852 | 4,401,247 | 3,538,350 | 5,048,865 | 485,637 | **14,765** ← bridge tail |
| 2 | 15,906,975 | 1,994,670 | 336,598 | 4,397,211 | 3,556,239 | 5,048,018 | 485,725 | **14,931** ← bridge tail |
| 3 | 15,911,249 | 1,277,481 | 508,564 | 3,824,367 | 3,511,228 | 5,215,399 | 449,155 | **1,054,048** |
| 4 | 15,922,372 | 1,748,881 | 0 | 3,765,603 | 3,405,507 | 5,260,937 | 443,789 | **1,226,855** |
| 5 | 15,914,923 | 614,279 | 1,153,990 | 3,777,757 | 3,516,509 | 5,195,206 | 446,968 | **1,139,108** |

(deactivated/tombstoned/quarantined/fetching columns omitted for width; in the
ledgers. shard4 verified=0 and shard5 loaded<verified reflect that `bun run
verify` was run on some shards and not others — not a data difference.)

## ClickHouse (serving DB, on emoji)

- **2.09 billion** posts
- **384.70 million** posts with ≥1 emoji
- **11.97 million** distinct DIDs (≈ ledger loaded 9.24M + verified 2.57M)

## Archive (full post text, parquet on Hetzner storagebox)

`storagebox:emojistats-archive/shard{0..5}/`, synced via `rclone move`:

| shard | objects | size |
|---|---|---|
| 0 | 635 | 29.74 GiB |
| 1 | 763 | 44.00 GiB |
| 2 | 762 | 45.68 GiB |
| 3 | 618 | 30.09 GiB |
| 4 | 625 | 29.62 GiB |
| 5 | 610 | 29.63 GiB |
| **total** | **3,613** | **≈208.8 GiB** |

## Work left — what a mop-up pass must finish

Total remaining: **4,313,844 repos** (4.5%), per shard:

- **shard1: 14,765 — ALL on `atproto.brid.gy`** (the AT↔Fediverse bridge), 429 rate-limited. crawl1 retired 2026-06-13; finish later on a cheap box (`CRAWL_SHARD_INDEX=1` against the preserved ledger). Bridge throttles hard — this is slow for anyone.
- **shard2: 14,931 — same bridge tail.** crawl2 retiring; same mop-up.
- **shard0: 864,137 / shard3: 1,054,048 / shard4: 1,226,855 / shard5: 1,139,108** — still actively crawling on crawl0/3/4/5 (productive boxes), draining normally.

The remaining repos stay `pending` in the preserved per-shard ledgers; the data
is not lost, only deferred. To finish: restore the shard's ledger, point a
crawler at `CRAWL_SHARD_INDEX=<n>`, run.

## Provenance
Counts: `SELECT status, count(*) FROM repos WHERE bucket=<n> GROUP BY status`
on each box's `/workspace/src/emojistats-bsky/packages/backfill/data/ledger.sqlite`.
crawl1 was stopped (quiescent) at capture; others read live (WAL, consistent).
