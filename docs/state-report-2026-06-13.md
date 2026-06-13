# Backfill state report — 2026-06-13 ~15:30 UTC

Single-day incident + hardening session on the full-network Bluesky emoji
backfill (6 Hetzner crawler dedis crawl0–5, 1 CX33 "emoji" VPS = ClickHouse +
web). Written for a second-opinion review.

## What shipped today (all via git → per-box `reset --hard origin/main`)

1. **Loader silent-loss fix** (`a31b514`, deployed all 6). The ClickHouse
   loader evicted a flush generation's outcome on a fixed 128-flush window and
   `finish()` treated a missing entry as success → a repo whose `finish()`
   raced past the window over a *failed* flush was marked `loaded` despite
   dropped rows (silent loss). Now a generation is evicted ONLY on flush
   success; failed generations are retained so a late `finish()` always
   observes the failure and parks the repo retryable. Codex-reviewed; regression
   test pins it.

2. **verify digest hardening** (same commit). `bun run verify` reconciles
   ledger `posts_total` + rkey XOR-digest vs ClickHouse `count()` + digest for
   EVERY loaded/verified repo. Previously a balanced count with a divergent
   digest (a lost backfill row masked by an offsetting live-path arrival) was
   warned-and-promoted; now it FAILS the run.

3. **Fetcher wedge fix** (`06a13b0`, deployed all 6). Root cause of the
   recurring 0-CPU scheduler wedges: a half-open/silent socket made `fetch()`,
   the body `read()`, the error-body `response.text()`, or `upstream.cancel()`
   hang forever; `AbortSignal.timeout` did not reliably interrupt it. The job
   never settled → its GLOBAL_CONCURRENCY slot leaked → enough leaks froze the
   scheduler → 180s watchdog restarted the box → host-health reset → same hosts
   re-stalled → wedge/restart loop. Added `withProgressTimeout` (Promise.race
   against a self-driven 60s inactivity timer) wrapping all four awaited network
   ops; on stall it best-effort `abort()`s and rejects, so every fetch settles
   within ~60s (under the 180s watchdog) and the slot is always freed.
   Codex-reviewed 3 rounds (caught the error-body + cancel paths). **Validated:
   crawl0 ran ~30 min clean vs the old 5–7 min wedge interval.**

4. **cooling-on-stall** (`7adfdc4`, on `main`; deployed ONLY to crawl1).
   Soft: a stall now drives the same AIMD host-pressure cooldown as a 429.
   Hard: host-health parks a host after 6 sustained stalls over 120s of zero
   successes (own thresholds vs dns/legal's 30/30s); any success/HTTP response
   resets, and a kind change restarts the streak. Stalls use `markThrottled`
   (no attempts burned); parked rows → `unreachable` = the deferred final-sweep
   re-crawl list (not lost). Codex-reviewed 2 rounds (caught kind-carryover +
   quarantine proof-of-life), clean.

## Current fleet state (~15:30 UTC)

| box   | rev      | state  | rps  | shard remaining | note |
|-------|----------|--------|------|-----------------|------|
| crawl0| 06a13b0  | active | 455  | 880k            | productive |
| crawl1| 7adfdc4  | idle   | 0    | 14,870          | bridge 429 tail; cooling-on-stall canary |
| crawl2| 06a13b0  | idle   | 0    | 15,037          | bridge 429 tail |
| crawl3| 06a13b0  | active | 536  | 1.16M           | productive |
| crawl4| 06a13b0  | active | 4655 | 1.34M           | productive |
| crawl5| 06a13b0  | active | 1755 | 1.21M           | productive |

- No wedges since the fetcher fix; all `NRestarts=0`, fresh stats.
- **Version skew: crawl1 on `7adfdc4`, the other five on `06a13b0`.**
- ClickHouse: load ~11, mem 6/15 GB, 2.06B rows, 220 GB free of 300 GB. Healthy.
  (Earlier today it briefly spiked to load 16 with 30s insert-timeouts when all
  6 boxes ramped at once post-deploy → a transient CH-overload wedge on crawl0,
  cleared once the fleet settled. Lesson: deploy one box at a time with gaps.)

## Open issues / decisions

### A. crawl1 & crawl2 — 429 bridge tail (misdiagnosis corrected)
Both shards' entire remaining backlog (~15k each) is on `atproto.brid.gy`, the
AT↔Fediverse bridge. **Direct probe: it returns HTTP 429 in 0.23s** — fast
rate-limiting, NOT a stall. I initially assumed "stall" and built
cooling-on-stall to free them; it does not (0 stall events — the existing 429
AIMD cooling already handles the bridge, which is why both boxes sit idle in
cooldown). The bridge *will* serve, just very slowly under rate limits.
Options to stop paying for two near-idle dedis:
- **Retire them** (recommended). Cheapest, data-safe: the ~30k repos stay
  `pending` in the ledger (not lost), re-crawled by a later targeted/slow run.
- Park-on-sustained-429 — new code, aggressive, against the codebase's
  deliberate "429 = alive, never deadness" principle. Not recommended.
- Let them trickle — many dedi-hours for ~30k repos. Worst on cost.

### B. Ledgers live on the dedis, not emoji
Each crawl box holds its own ~34–35 GB `ledger.sqlite` (full enumeration, but
authoritative only for its own `bucket = shard index`). **emoji has none.** So
retiring a box destroys the only record of its shard's status (what's
pending/loaded) unless the ledger is `scp`'d off first (emoji has 220 GB free).
Same ledgers are what `verify` needs.

### C. cooling-on-stall — held
Correct and reviewed, but NOT urgent: the fetcher fix already prevents wedges;
cooling-on-stall only optimizes throughput on genuinely-silent hosts (none
currently). Holding the rollout to crawl0/2/3/4/5 to avoid 5 more restarts.
crawl1 carries it (harmless; will be retired anyway). Resolves the version skew
either by finishing the rollout later or reverting crawl1.

### D. Zero-data-loss posture
- The loader fix prevents the silent-loss mechanism going forward.
- Evidence the past ~2/3 is intact: zero terminal `batch insert failed` in
  journald; the tens of thousands of loader/insert errors in the ledgers are
  repos correctly re-queued (status fetching/failed/unreachable), not silently
  `loaded`; a `post-crash reconcile: count/digest mismatch` net already parks
  mismatches. This is strong evidence, NOT proof.
- The only *provable* zero is: run full `verify` (count+digest, every repo) +
  re-crawl any discrepancy, looped to convergence. `verify` is CH-bound (run it
  on emoji against a copied ledger; ~30–60 min/fleet). One residual digest
  blind spot: `CH count > ledger` (live growth) can mask a lost backfill row;
  only `--sample` exact-rkey re-fetch closes it — but the loader fix makes that
  moot for the failed-flush class.

## Pending decisions for the operator
1. Retire crawl1/crawl2 (after ledger copy-off)? 
2. Finish or drop the cooling-on-stall rollout (resolve version skew)?
3. When to run the verify→re-crawl convergence pass for provable zero-loss?
