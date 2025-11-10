# Futur AppView Backfill Findings

Last updated: 2025-11-10

Findings come from futur.blue’s 3 Oct 2025 write-up “in and out, quick appview adventure” describing the first full-history Bluesky AppView backfill. Source: https://whtwnd.com/futur.blue/3ls7sbvpsqc2w.

## What Futur Ran Into

- **Storage reality check:** A full AppView replica on a Hetzner Ryzen 9 5950X box with 8×3.84 TB SSDs landed near 16 TB after the June 22 2025 snapshot, and RAM requirements stayed modest (<32 GB) outside of the backfill spike. citeturn0view0
- **Timescale/Postgres insertion path:** After trying small-batch CTEs and bulk `INSERT … UNNEST`, the winning recipe was `COPY FROM` → temp table → final table with `ON CONFLICT DO NOTHING`, unlocking tens of thousands of rows/sec without duplicate-induced aborts. citeturn0view0
- **Fetcher throughput:** Listing PDSes via `mary-ext/atproto-scraping`, then fetching ~120 repos/sec (targeting ~50 concurrently per PDS) pushed a 3-day historical sweep, with Bun-backed repo workers parsing ~1.2k repos/sec before a pool of collection/record writers handled DB commits. citeturn0view0
- **Error handling cost:** Imperfect validation lost hundreds of thousands to millions of records—acceptable for tens of billions overall, but still a reminder that fast paths need correctness gates. citeturn0view0
- **Live indexer bottleneck:** The OSS AppView indexer stalled near 200 events/sec; weeks of rewrites ultimately switched from Node to Deno and saw ~4× throughput, enough to keep up with live traffic. citeturn0view0

## Implications for emojistats-bsky

1. **Confirm storage guardrails:** Our emoji-only footprint (<120 GB) is tiny in comparison, but we should keep Parquet + Timescale retention configurable in case scope expands toward richer AppView data.
2. **Evaluate our writer path:** Inspect `packages/backfill`’s Timescale writer; if we still emit row-at-a-time inserts, plan a migration to the COPY→temp-table pattern before increasing repo concurrency.
3. **Decouple fetch vs. normalize vs. write:** Ensure the current token-bucket boost does not simply shift bottlenecks downstream; consider explicit worker tiers (fetcher, normalization, Timescale writer) with queue metrics.
4. **Harden validation:** Tie each repo’s Timescale + Parquet commit to a checksum/row-count verification so “terminal skips” stay true skips, not silent truncations.
5. **Plan for live ingest performance:** When we light up Jetstream, set throughput SLOs (events/sec plus queue depth) and keep runtime flexibility if Node-based workers run into the same ceiling Futur hit.
