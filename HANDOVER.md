# Handover — emojistats backfill fleet

_As of 2026-06-13 ~21:00 UTC. Written for whoever picks this up next._

This is the operational handover: current state, the **open items in detail**, and a
reference for the tooling/gotchas. The narrative history lives in
`docs/launch-log-2026-06-12.md` (raw ops notes) and `docs/retro.md` (the polished retro,
owned by a separate agent — **do not edit retro.md**).

---

## 1. Current state (the one-screen version)

- **Fleet:** 4 productive crawlers — `crawl0/3/4/5` (Hetzner), sharded by
  `CRAWL_SHARD_INDEX` with `CRAWL_SHARDS=6`. `crawl1`/`crawl2` were retired + **deleted**
  earlier today (their shards 1/2 are paused — bridge-tail only, see §2.5).
- **Serving box:** `emoji` (CX33, **8 cores**) — ClickHouse (`posts` ≈ 2.09B rows) + the
  public web/api (emojistats.mosphere.at) + live Jetstream ingest.
- **Deployed rev:** `48de289` on all 4 boxes == `origin/main`. Git is clean everywhere.
- **Remaining work:** ~2.8M repos across the 4 live shards (s0 ~635k / s3 ~740k /
  s4 ~610k / s5 ~690k as of ~20:30), draining slowly (end-game tail — slow/rate-limited
  hosts). ETA is days at the current tail rate.
- **Monitoring:** a bash watchdog runs as background task **`be1x2c4ml`**
  (`/home/agent/fleet-watchdog.sh`) — auto-restarts wedged boxes + alerts. A 10-min
  babysit loop (ScheduleWakeup) also runs.

### What shipped this session (all deployed, codex-reviewed)
| commit | what |
|---|---|
| `a31b514` | loader silent-loss fix + verify digest hardening |
| `06a13b0` | fetcher wedge fix (`withProgressTimeout`) |
| `7adfdc4` | cooling-on-stall (host-pressure + host-health park) |
| `4c38d0f` | **`--revive-host`** — dead-host exit-ramp |
| `1385aea` | **verify directed sampling** (`--sample-loose`/`--emit-loose`/`--did-file`/`--no-reconcile`) |
| `b825b5c` | **loader CH-client rebuild** — self-heals a poisoned socket pool |
| `6a06ccd` | **listClaimable stack-overflow fix** (`push(...big)` → loop) |

Also done: **Bridgy blacklisted** on all 4 boxes (`atproto.brid.gy` + `fed.brid.gy` in each
ledger's `dead_hosts` meta).

---

## 2. OPEN ITEMS (in detail)

### 2.1 — pix-flake: remove/reschedule `emojistats-verify.timer`  ⚠️ highest-priority infra
**Problem.** A systemd timer `emojistats-verify.timer` fires `emojistats-verify.service`
(= `bun run verify`, a FULL ledger↔ClickHouse reconcile) on a schedule. At 18:20 today it
fired mid-crawl and ran a reconcile against the live CH — hundreds of heavy `FINAL` digest
group-by queries that starved the crawler inserts → 30s insert-timeouts → crawl3 wedged.
CH load spiked 11→20 (8-core box). See the launch-log "18:33–18:50" entry.

**Why it matters.** `verify` belongs **post-drain**, not during the crawl — running both at
once overloads the single CH box. It WILL recur: I only `systemctl stop`ped the timer,
which holds **only until the next reboot / `nixos-rebuild`** because the unit is declared
in the **pix flake** (the NixOS config repo, `/home/agent/workspace/src/pix`).

**What to do.** In the pix flake, either remove `emojistats-verify.timer` or reschedule it
to not run while the backfill is active (e.g. disable it; run verify manually post-drain).
Then `nixos-rebuild switch` on each box. Verify with
`systemctl is-enabled emojistats-verify.timer`.

**Check if it's already crept back:** `ssh crawlN systemctl is-active emojistats-verify.timer`
— if `active`, it re-fired; `systemctl stop` it again and prioritize the flake fix.

---

### 2.2 — #3 verify zero-loss: run the directed convergence pass (post-drain)
**Problem / context.** `verify` reconciles each repo's `(count, XOR-rkey-digest)` from the
ledger against ClickHouse. The residual blind spot: when **CH count ≥ ledger** with a
divergent digest ("LOOSE"), a lost backfill row could be masked by an offsetting live-path
arrival, and a 64-bit XOR can't set-check once counts differ. This is **inherent** to O(1)
digests, not a bug. (Also: the CH `posts` table is `ReplacingMergeTree(ingested_at)` keyed
`(did,rkey)` with `src` NOT in the sort key, so filtering `src='backfill'` is unsafe.)

**Status.** The tooling is **built and deployed** (`1385aea`). An early random sample
(`--sample 300 --no-reconcile`) returned **0 losses**. The loader silent-loss fix
(`a31b514`) prevents the only known drop mechanism going forward.

**What to do (the convergence loop), AFTER the crawl drains + `--final-sweep` (§2.6):**
- `bun run verify --emit-loose loose.txt` — runs the full reconcile and writes the
  ambiguous LOOSE DID list (the only repos the digest can't clear) to `loose.txt`.
- `bun run crawl --did-file loose.txt` — re-fetches exactly those at full concurrency
  (NOT `crawl --did $(cat ...)` — that breaks; `--did` is repeatable and millions of DIDs
  exceed ARG_MAX. An **empty** `--did-file` errors by design, to avoid a full crawl).
- Re-run verify; repeat until LOOSE shrinks to the genuinely-live tail.
- For a measured bound without the full loop: `bun run verify --sample-loose N` (directed
  random sample of the LOOSE set) or `--sample-loose all` (exhaustive).

**Honesty caveat (don't overclaim):** a re-fetch only proves "every post STILL in the repo
is in CH" — a post deleted upstream since the crawl is invisible to both the fresh CAR and
CH. So even `--sample-loose all` is the strongest **practical** check, **not a formal proof**
of original-CAR ⊆ CH. Run it post-drain (CH flushed, no new LOOSE racing reconciliation).

**Run it on `emoji`** against a copied ledger, or on a box — it's CH-bound; expect it to be
heavy on CH (see §2.1 — don't run it while the crawl is hammering CH).

---

### 2.3 — Fix #2: watchdog progress-signal (deferred)
**Problem.** The watchdog (`fleet-watchdog.sh`, task `be1x2c4ml`) keys on **log freshness**
(any journal line) to avoid false-restarting boxes that are alive but failure-heavy (404
churn). Its blind spot: a box that's **logging but not progressing** — e.g. the poisoned-pool
stall (§ crawl4 20:06) where the crawler kept emitting stats while `loaded` was frozen and
`fetching` pegged at the concurrency cap. The log-freshness signal read "healthy."

**What to do.** Add a progress-based restart trigger: restart if `loaded`/resolved is flat
**AND** `fetching` is pegged at `GLOBAL_CONCURRENCY` for >~4 min, even while the box logs.
The watchdog already parses stats lines; add this as a second confirmed-wedge condition
alongside the existing 0-CPU check. Keep the warm-up guard + cooldown.

**Why deferred.** The `b825b5c` loader fix should prevent the poisoned-pool stall at the
source. Manual restart + the telemetry-staleness watch (per-shard `backfill_progress` age)
cover it in the meantime. Worth doing before the next big crawl.

---

### 2.4 — Telemetry ClickHouse client: same poisoning class, NOT yet fixed
**Problem.** `b825b5c` made the **loader's** CH client self-heal a poisoned socket pool
(`isConnectionError` → `#rebuildClient`). But the **telemetry** client (the isolated client
that writes `backfill_progress`, the dashboard's source) is the same `@clickhouse/client`
and can poison the same way — that's why shard4's dashboard row went stale independently of
the loader during the 20:06 incident.

**What to do.** If dashboard-staleness must be fully eliminated, fold the same rebuild-on-
connection-error pattern into the telemetry client (`telemetry.ts` / wherever the
`backfill_progress` insert lives). Lower priority — it only affects the dashboard's
freshness, not data integrity.

---

### 2.5 — Bridgy: re-crawl when it ships getRepo
**Context.** `atproto.brid.gy` (the AT↔Fediverse bridge) returns HTTP 429 fast but **does
not support `getRepo` at all** — those repos can never be crawled until Bridgy adds support.
They're blacklisted (in each box's `dead_hosts` meta) and parked as `unreachable`. Counts:
~15–18k/shard live + the ~14.8k+14.9k preserved from the retired shards 1/2.

**What to do (someday).** When Bridgy supports getRepo:
`bun run crawl --revive-host atproto.brid.gy --revive-host fed.brid.gy` on each box (drops
them from `dead_hosts` + re-arms their parked rows). Procedure in `docs/backfill-runbook.md`
→ "Reviving a blacklisted or dead host". The preserved shard1/2 bridge-DID lists are on the
storagebox at `_meta/bridge-parked/`.

---

### 2.6 — End-game sequence (when the 4 shards drain)
The crawl is in its slow tail. When `pending`+`fetching` per shard approaches ~0:
- **`--final-sweep`** (`bun run crawl --final-sweep`): zeroes the attempt budget on parked
  `unreachable` rows so retry waves resume — recovers repos that exhausted retries on
  **alive** hosts (e.g. 429 storms). Genuinely-dead/blacklisted hosts stay parked (they're
  in `dead_hosts`; `--final-sweep` does NOT re-crawl those — use `--revive-host` for a
  specific recovered host).
- **Then full verify** (§2.2) for the zero-loss number.
- **Then** the v1 metadata re-crawl (§2.7) and the public-site cutover (see retro "Open").

### 2.7 — Re-crawl the ~17% pre-widening (`v:1`) archive metadata
Repos archived before the 2026-06-13 metadata widening (`archive_extras_since` ledger meta)
lack `facets/reply/embed/labels` (v1 parquet stored only text/langs/emojis; CH keeps text
for emoji-posts only). Recovering them is a **network re-fetch**, recoverable only for
still-online repos. Identify via manifest `v:1` vs `v:2` + per-repo `loaded_at`. Deferrable;
additive. Likely fold into `--final-sweep`.

---

## 3. Operational reference

### Deploy (git-only; NEVER rsync)
Per box, staggered (one at a time — synchronized restarts spike the single CH box):
`ssh crawlN 'cd /workspace/src/emojistats-bsky && git fetch origin && git reset --hard origin/main'`
then `ssh crawlN sudo systemctl restart emojistats-crawl`, then watch warm-up (~70s) and
confirm `loaded` advancing before the next box. Verify rev with `git rev-parse --short HEAD`.
CH creds come from `EnvironmentFile=/run/secrets/emojistats-crawl-env` (no `.env` on boxes).

### Codex review (required before deploy)
`cat prompt | /etc/profiles/per-user/agent/bin/codex exec -c model="gpt-5.5" -c model_reasoning_effort="xhigh" --sandbox read-only --cd /home/agent/emojistats-bsky`
Feed the prompt via **STDIN** (positional-arg form hangs).

### Tests / typecheck (run from `packages/backfill`)
`bun run typecheck` (tsgo) and `bunx tsx --test src/*.test.ts`. NOTE: `bun test` fails on
`better-sqlite3` (use `bunx tsx --test`). The pre-commit hook runs `oxfmt`.

### Gotchas that bit this session (save yourself the time)
- **`pgrep -f verify.ts` self-matches** the shell running it (the pattern is in your own
  command line) → false "2 procs". Trust `systemctl is-active` + `system.processes` + CH
  load instead.
- **`systemctl stop` ≠ process gone:** a proc blocked in a 30s CH socket read ignores
  SIGTERM; use `pkill -9 -f "src/verify.ts"` (targets verify, never `src/crawl.ts`).
- **Per-box ledger holds the FULL enumeration (all ~95M repos, every bucket)** but a box
  only owns `bucket = CRAWL_SHARD_INDEX`. A raw `WHERE pds_host=... AND status='pending'`
  count spans ALL shards and lies — always add `AND bucket=<shardIndex>` for per-box truth.
- **`@duckdb/node-api` needs `libstdc++.so.6`** (absent on the NixOS bun/node PATH; bun
  can't load it at all): run with `LD_LIBRARY_PATH=/nix/store/<gcc>-lib/lib`.
- **CH telemetry table is `backfill_progress`**, columns `shard` (e.g. `shard4`) + `ts`.
  Dashboard staleness check:
  `clickhouse-client --database=emojistats -q "SELECT shard, dateDiff('second',max(ts),now()) FROM backfill_progress WHERE ts>now()-3600 GROUP BY shard"`.
- **"box keeps restarting" is a symptom, not a diagnosis** — a 0-CPU hang, an exit-1 crash,
  and an OOM look identical from outside. Read the signature (`systemctl show ... Result`,
  `status=`, the fatal log line, `dmesg` for OOM) before reaching for a fix.

### Key paths
- Repo (boxes): `/workspace/src/emojistats-bsky` ; ledger:
  `packages/backfill/data/ledger.sqlite` ; archive spool: `packages/backfill/data/archive/`.
- Archive (durable): `storagebox:emojistats-archive/` via rclone
  (`--config /run/secrets/emojistats-rclone-conf`); `_meta/` holds ledgers, stats, runbook.
- pix flake (infra/systemd units): `/home/agent/workspace/src/pix`.
- SSH: `ssh -F /home/agent/fleet-ssh-config crawl0|crawl3|crawl4|crawl5|emoji`.
- Watchdog: `/home/agent/fleet-watchdog.sh` (running as task `be1x2c4ml`).
