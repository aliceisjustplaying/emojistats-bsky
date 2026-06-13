# Backfill fleet launch log — night of 2026-06-11 → 12

Raw notes for the eventual writeup. Times are local (CEST). The goal: stand up
one serving box + six crawl boxes from bare Hetzner hardware to a running
full-network Bluesky backfill, overnight, with correctness guarantees built in.

## Current checkpoint — 2026-06-12 13:40 UTC

Stable pause point reached, not final target reached.

- Deployed commit: `90b9de7` on `emoji` and `crawl0..5`.
- Live crawler settings: `GLOBAL_CONCURRENCY=4096`,
  `PER_HOST_CONCURRENCY_BSKY=96`, `PER_HOST_CONCURRENCY=16`,
  `LOADER_BATCH_ROWS=50000`.
- Stable progress-delta sample from `backfill_progress`: 55,689,931 pending,
  ~10,122 terminal repos/min, ETA ~3.82 days.
- 429s were moderate in the same window: 147 from `morel`, 19 from
  `atproto.brid.gy`.
- Remaining rough edge: telemetry/event inserts still show occasional
  `socket hang up`; `backfill_progress` retries the newest snapshot, but
  `backfill_repo_events` is lossy and must not be used for ETA.

Things tried that did not reduce the bottleneck enough:

- `5120/128/20`: too hot. It filled slots, but large ClickHouse inserts crossed
  the client timeout, telemetry froze, and some crawlers restarted.
- `6144/96/16`: not useful. It lowered measured progress-delta throughput to
  ~13.7k/min and worsened upload resets.
- ClickHouse progress headers alone: useful for long server-side work, but not
  sufficient here because the server logged `CANNOT_READ_ALL_DATA`, meaning the
  client upload body was cut mid-request.
- `backfill_repo_events` as a rate source: invalid under write pressure because
  event batches are allowed to drop.

The current safe operating point is 4096 global, 96/16 per-host, 50k post
batches, gzip-compressed ClickHouse uploads. Future tuning should start from
ClickHouse upload stability or shard skew, not from blindly increasing global
concurrency.

Telemetry follow-up: status progress, repo events, and durable post loads now
use separate ClickHouse clients. Repo events also flush in capped chunks
(`TELEMETRY_EVENT_BATCH_ROWS`, default 1000). Event telemetry remains lossy,
but it should no longer share an HTTP connection pool with the durable loader
or block the progress snapshot retry path.

## Chapter 0 — how we got here (pre-launch working notes, added day 2)

The white whale: backfill emojistats from the first Bluesky post and serve
down-to-the-hour historical emoji trends next to the live view. The scar
tissue shaping every decision: a failed November 2025 attempt ("Nexus" —
which a later archaeology dig resolved to be tap's pre-release development
name; we'd been running pre-release tap into Timescale + Parquet). That
attempt died of unverifiability. Hence the prime directive this time:
**correctness must be checkable per repo**, not vibes-checked at the end.

Decisions locked 2026-06-11, before any hardware:

- Custom lean TypeScript crawler (not tap, not a zeppelin fork): we want the
  ledger, verification and emoji extraction fused, and the workload (getRepo
  CAR → posts only) is narrow.
- Emojitracker semantics: posts count as they happened; deletes are ignored
  on purpose. Relays are non-archival, so "history" means surviving posts.
- One store: ClickHouse. Postgres/Timescale, Redis/Valkey and BullMQ from the
  old stack: ripped out (16 deps deleted from the backend the same day).
- Cost-revised storage split: ClickHouse keeps all posts but full text only
  for emoji posts (~tens of GB); the complete text corpus goes to zstd
  Parquet on a €4/mo Storage Box, written during the crawl and spooled hourly
  from live. The Parquet sink was a pre-crawl blocker because the crawl is
  the only time most of that text will ever be fetched.
- Architecture pillars: raw `posts` (ReplacingMergeTree on (did, rkey)) is
  the only truth; every aggregate is a disposable cache rebuildable from it;
  a per-repo SQLite ledger is the only crawl checkpoint; explicit acceptance
  criteria per repo.
- Verification is digest-tiered: a 64-bit XOR fold of sha256(rkey) per repo,
  computed identically in JS and ClickHouse (`groupBitXor(reinterpretAsUInt64
  (substring(SHA256(rkey),1,8)))`), promotes repos to EXACT; count-based
  LOOSE catches live-arrival deltas; everything else FAILS loudly.

Four pre-launch review rounds left war stories the blogpost should keep:
live ingest made truly at-least-once (cursor commits only after the
ClickHouse flush AND the archive-append barrier); a latent parquet
sequence-reuse bug — rclone-move restarts would have silently overwritten
remote files, i.e. silent destruction of the only copy of non-emoji text —
caught by reading the manifest at seed time; insert dedup tokens that encode
chunk size so a config change degrades to harmless re-inserts instead of
silent skips; and a spec dive proving (did, rkey) is only safe as a post key
because both paths hard-filter to `app.bsky.feed.post` (the argument is
pinned as a comment on the schema's ORDER BY — if a second collection is
ever ingested, the key must grow a collection component).

Dry run before buying anything: 2,990 repos crawled, 100% reconciled,
including a hand-verification that the owner's own 13,999 posts came back
byte-exact. Also on the record: the old prod stack was no ground truth at
all — its Postgres had been off for months and its Redis counters have gaps
in unknown places. The crawl IS the source of truth now.

## Hardware

- `emoji` (serving): Hetzner Cloud CX33, 4 vCPU / 8 GB / 80 GB, Debian 13 → NixOS
- `crawl0..5`: Hetzner auction dedis, i7-6700, 8 threads, 2× ~480 GB SSD each
  - crawl0/1: NVMe; crawl2–5: SATA SSD (mixed fleet, lsblk-verified in rescue)
  - crawl3 turned out to be a 32 GB box (rest are 64 GB) — kept it; the crawler
    streams and never needs more than a few GB
- Storage Box for the parquet full-text archive (rclone sftp, port 23)

## ~00:00 — boxes arrive

All five auction boxes activated within the hour; the sixth (crawl5,
138.201.205.59) landed while pre-flight was running. Every host key verified
against the activation emails via ssh-keyscan before first connect.

## ~00:20 — pre-flight findings (each would have been a 3am outage)

- **Hetzner Cloud x86 boots SeaBIOS, not UEFI.** The stock Debian image carries
  an ESP so it *looks* UEFI, but `/sys/firmware/efi` is absent. The planned
  ESP-only disko layout would have installed cleanly and then never booted.
  Switched `emoji` to the legacy-BIOS GPT layout (1M EF02 + GRUB).
- **MagicDNS is off fleet-wide** (`--accept-dns=false` in the tailscale module)
  but the crawl env pointed at `http://emoji:8123`. Crawlers would have failed
  their first ClickHouse write. Fix: pin `emoji` in `networking.hosts` once its
  tailnet IP exists.
- **sops ⇄ tailscale bootstrap deadlock.** SSH was tailnet-only; tailscale needs
  its auth key from sops; sops can't decrypt until the host's key is a
  recipient — which the old runbook only added *after* first boot, over SSH.
  Lockout by design. Fix: pre-generate one age key per host, add all seven as
  recipients up front, inject each via `nixos-anywhere --extra-files` so
  secrets decrypt on the very first boot. Also opened public 22 (key-only,
  no root) for the launch window as a belt-and-suspenders.
- **Initrd storage modules.** No generated hardware-configuration.nix in a
  disko flow, so ahci/nvme/sd_mod had to be spelled out or stage 1 would never
  find the root disk.
- No nix on the laptop (macOS): crawl0's rescue system (62 GB of RAM doing
  nothing) becomes the deploy driver. It installs everyone else, then
  reformats itself last — the rescue system runs from RAM, so it can happily
  disko its own disks.

## Deploy order

driver = crawl0 rescue → `emoji` first (everything depends on ClickHouse),
learn its tailnet IP, bake `networking.hosts`, then crawl1–5 in parallel,
crawl0 self-install last. DNS (`backfill.mosphere.at` → 167.233.121.232,
unproxied, Caddy/ZeroSSL on-box) via Cloudflare API.

## ~01:00–02:10 — the install wave (and its potholes)

- First parallel launch died on a GRUB assertion: disko already derives
  `boot.loader.grub.devices` from the EF02 disk, so listing it explicitly
  duplicated the device. Lesson: with disko, set `disk.main.device` and stop.
- gogcli's go-modules fetch (a fixed-output drv) flaked once and took four of
  six builds down with it — all six shared the driver's nix store. Led to the
  right call anyway: the fleet doesn't need interactive-agent tooling, so the
  whole profile went lean (Alice: "even emoji doesn't need the agent stuff").
- `pkill -f nixos-anywhere` over SSH matches its own command line. Twice.
  `pkill -f "anywhere[.]sh"` does not. Write that one down.
- The tailscale auth key carried tag:emojistats, the module hardcoded
  tag:pix → first enrollment rejected. Tag is an option now.
- systemd splits unquoted `Environment=` values on spaces: the archive
  syncCommand truncated to a bare rclone path and the sink (correctly!)
  refused to run without a durable archive. Auto-quote in agent-service now.
- rclone's sftp backend probes the remote shell and tries to persist the
  result into its config — which is a read-only sops secret. Pinning
  shell_type/md5sum/sha1sum per the Hetzner docs killed both the error spam
  and ~15s of retry latency per invocation.
- bun 1.3 defaults fresh installs to the isolated linker → no root
  node_modules/.bin/tsx where the systemd units look. bunfig.toml pins
  hoisted; the lockfile is now committed (it had been gitignored since the
  project's early days — seven boxes resolving deps independently is how you
  get version drift). Shared dep versions synced across packages while at it.
- crawl0's localhost self-install: rescue lacks `nixos-install` (it ships in
  the kexec image we skipped) → ran it from `nixpkgs#nixos-install-tools`;
  the bootloader step then failed on chroot PATH (`mount: command not found`)
  → re-ran switch-to-configuration boot with the system PATH prefixed. GRUB
  verified in the MBR before reboot. The rescue system formatted its own
  disks and rebooted into the result — RAM-rooted systems are a gift.

## 02:30–04:00 — the night of the four bottlenecks

The hum died within the hour. Repos/min collapsed, ETA ballooned to 70 days,
the dashboard went idle. What followed was a 90-minute onion-peeling session —
four real defects stacked on top of each other, each one masking the next:

1. **bsky.social rated as a third-party PDS.** Every pre-migration account's
   PLC tail points at the entryway; the third-party politeness cap (2 slots
   per box!) gated a 168-deep queue. One classification fix: entryway = bsky
   infra tier.
2. **DNS threadpool starvation.** getaddrinfo runs on libuv's default 4
   threads; retry waves dialing dead PDSes (rip boobee.blue, 4k errors/30min)
   parked all four in DNS timeouts. UV_THREADPOOL_SIZE=64.
3. **Stale keep-alive sockets to ClickHouse.** The client WARNs and reuses a
   server-closed socket; the insert hangs forever; telemetry's single-flight
   tick latches shut. keep_alive.eagerly_destroy_stale_sockets=true on every
   client.
4. **The real boss: whale-repo CAR parsing on the main thread.** repoFromStream
   buffers and indexes the whole CAR synchronously before yielding entry one —
   10-30s of unyieldable CPU per whale, and every restart re-front-loaded 256
   requeued whales at once. The event loop never breathed: sockets starved
   (3 MB/s across "128 active downloads", 21 actual TLS connections), CH
   responses sat unread past their timeouts, setInterval never fired. Each fix
   above was real, and none of them could matter until this one fell.

The fix that fell the boss: **a worker_threads parse pool** (parse-worker.ts +
parse-pool.ts). Fetch buffers the guarded CAR and transfers it zero-copy to a
worker (availableParallelism−2 per box); the worker walks the MST, normalizes
rows, folds the rkey digest; the main thread is pure I/O again. Side effect:
one busy core became seven per box. Correctness check: David's deliberately
cursed repo (retr0.id — stale PLC pointer, byte-identical duplicate records)
re-parsed off-thread to the identical 30,058 posts and the identical rkey
digest 1b3f7ddc33926fd0 it produced in-process. Bonus: rows now materialize
before any append, so a quarantined parse writes nothing anywhere — the old
partial-coverage caveat died with the streaming interleave.

Honorable mention: `pkill -f nixos-anywhere` over SSH matches its own command
line and kills itself. It got me twice. `pkill -f "anywhere[.]sh"`.

## ~04:30 — the boss's true form

Even with parsing AND fetching in workers, the main thread pegged 100% in
native code. SIGUSR1 couldn't even open the inspector — the loop was that
dead. A restart with `--inspect` pre-armed and a hand-rolled CDP profiler
finally produced the confession, and it was none of the four suspects above:

    100.0%  Statement.all ← listClaimable (ledger.ts) ← scheduler.run

The claimable query ran `row_number() OVER (PARTITION BY pds_host)` over
EVERY claimable row — with the shard-bucket polynomial computed per row — on
every claim batch. A full-table scan of a table that enumeration was growing
by ~6,000 rows a second. At dry-run scale: milliseconds, invisible. At 3.7M
rows with dead-host churn re-triggering claims: one to three seconds of
synchronous native CPU, back to back, forever. Every earlier fix was real and
necessary; none of them could matter while the claim path itself was the spin.
The decay curve of the whole night — fine at first, dead by hour two — was the
ledger growing.

Fix: persist the bucket (computed once per row at write time, JS === SQL,
modulus pinned with a constructor guard), additive migration with backfill on
open (~70s per box over ~19M rows), index `(status, bucket, did)`, and claims
became an O(LIMIT) index seek in did order — random base32 DIDs make did order
a statistically fair host shuffle, so the window-function rotation wasn't even
needed. Migration tested for JS/SQL bucket parity (5,001 DIDs, zero
mismatches) and verified to hit the index via EXPLAIN QUERY PLAN.

## ~05:30 — IT ACTUALLY HUMS

First two minutes on one box after the fix: 1,696 claimed, 556 loaded, 725
empty, 268k post rows, 552 MB — main thread at 9%. Fleet-wide: 150–250k
posts/minute into ClickHouse sustained, ~1,300+ repos/minute resolving, boxes
loafing at load 0.7 of 8 cores with zero 429s — so the bsky-infra host cap got
a bump (16 → 32 per box) to let the early host-concentrated era through. The
rate climbs on its own from here as enumeration fans out hosts and the queue
leaves the whale era: the network's average repo is ~64 posts; tonight's were
300+.

Morals, in order of expense: profile before fixing (the CPU profile cost five
minutes and was right; four plausible theories cost two hours and were
upstream of the truth); O(n) on a growing n is a time bomb with a fuse
exactly as long as your dry run; and the dashboard's "idle" badge was the
single most honest component of the entire system all night.

## ~06:30 — shift handover

Steady state as the sun comes up: **~1,450 repos/min resolving, 8.6M posts in
the database, 85k repos terminal, 27M of ~45M DIDs enumerated** (enumeration
finishes mid-morning). One experiment failed honestly: doubling the bsky host
cap to 32 drew 429s from the mushrooms AND pushed ClickHouse's 8 GB box into
refusing inserts — reverted within twenty minutes; 16 is the system's natural
operating point until ClickHouse gets more headroom. The insert pressure also
exposed the dashboard's raw-posts scans (the page 500'd mid-crawl): the
histogram and fun cards now read posts_hourly/emoji_total — 0.2s renders,
immune to the posts table growing 300×.

ETA truth: ~3 weeks at the instantaneous rate, trending down as enumeration
fans out hosts and the queue exits the whale era. The one cheap lever for
single-digit days: more memory for ClickHouse (the CX33 resize is minutes of
downtime) — its 5 GB cap is now the ceiling everything else queues behind.
The night started at 70 days and a frozen dashboard; it ends self-healing,
verified, and 25× faster.

---

# Day 2 — 2026-06-12, daytime

## ~09:30 — the dashboard dies again (bottleneck #6: the parts storm)

Morning report: dashboard 500s with `MEMORY_LIMIT_EXCEEDED ... maximum: 5.00
GiB`. Not the dashboard's queries this time (those were capped at 1.2 GB
yesterday) — the *server-wide* limit. Under the night's insert pressure the
OvercommitTracker shoots whichever query asks next, i.e. whatever the browser
triggers.

Triage detour worth blogging honestly: I first declared BOTH memory and disk
emergencies — disk had gone 31% → 49% in three hours, and I extrapolated
"dead in 8–12 hours, the corpus needs hundreds of GB, we need a volume NOW."
Wrong. Decomposing instead of extrapolating: the entire emojistats database
was **230 MiB** (posts: 208 MiB for 8.6M rows ≈ 25 B/post — the schema
estimate was always fine). The disk eater was the posts table directory
holding **14 GB for 208 MiB of live data**: `du` vs `system.tables.total_bytes`
disagreeing 70×.

The mechanism, and it's a beauty: the loader inserted **per repo** (average
repo: ~64 posts), and `posts` is partitioned by month. A repo's posts span its
account's whole lifetime, so each tiny insert shatters into one part per month
touched — ~46 at full history. ~24 inserts/s fleet-wide × ~46 partitions =
hundreds of parts created per second against a memory-starved merge scheduler.
**244,883 parts** for 208 MiB of data. Parts metadata lives in RAM: that's
what was pinning the 5 GB cap. One pathology, both symptoms — the launch-night
dry run could never see it because dry runs don't run long enough to shatter
a quarter-million parts.

## ~10:00 — the rescale (and honest cost talk)

Decision point with Alice (who is, correctly, watching the bill: the crawl
fleet costs ~€0.54/h): memory is the fire, disk is fine once de-bloated.
Hetzner only offered 16 GB **with** 320 GB disk (€30/mo vs €8) — and a
gotcha for the retro: **Hetzner can never shrink a disk**, so "resize back
later" doesn't exist; the path down is a fresh smaller box + migration. The
end-state ClickHouse data (~75 GB at 25 B/post × 2.9B posts) needs ≥160 GB of
disk anyway, so the realistic endgame is ~€15/mo, not €8.

Shutdown was clean (stop services → stop clickhouse → poweroff); the fleet
was already paused for the parts drain — which, fun fact, made **zero
progress in 3 minutes of idle merging**: a quarter-million parts is too many
for the merge scheduler to even schedule against a 5 GB cap.

## ~10:30 — surgery on the serving box

- Partition grow: no growpart/parted on NixOS — `sfdisk -N 2 --force
  --no-reread` + `partx -u` + `resize2fs`. 75 G → 300 G online.
- CH cap 5 → 12 GiB in the nix flake (pix repo). Trap for the retro:
  `nixos-rebuild switch` does NOT restart clickhouse on extraServerConfig
  changes — verify `system.server_settings`, then restart by hand.
- The 244k-part table: waiting for merges would have taken hours. With 208 MiB
  of live data the right move is a rebuild: `CREATE TABLE posts_rebuild AS
  posts` + `INSERT SELECT` (37 s; MVs don't fire on table-to-table copies) +
  parity gate (`uniqExact(did, rkey)` identical on both sides: 9,619,608 —
  the 503-row count delta was ReplacingMergeTree finally collapsing replay
  duplicates) + `EXCHANGE TABLES` + drop. 244,883 parts → **63**. The 14 GB
  came back ~8 minutes later (Atomic DBs drop lazily). All six MVs survived
  the exchange (`dependencies_table` check) and ticked on live replay.
- Footgun logged: `max(hour)` of the aggregates is useless as a freshness
  signal — clock-skewed clients put posts in the future. Use current-hour
  sums ticking instead.

## ~11:00 — the loader stops being the arsonist

Root fix, not just cleanup: the loader now batches inserts **across repos** —
one shared buffer, flushed at 200k rows or 15 s. Insert rate drops from ~4/s
to ~4/min per box; part creation divides by the number of repos per flush.

The interesting engineering bit: durability bookkeeping. `finish()` must not
resolve until every batch carrying that repo's rows has landed (the ledger
flips to 'loaded' on it). First draft had a race — "buffer empty" ≠ "my rows
are durable" (the flush holding them might be in flight, or MY batch failed
while a LATER one succeeded). Final design: the buffer carries a generation
number; each repo records the generations its rows landed in; finish() awaits
exactly those flushes. A failed flush rejects every batch-mate — they re-park
as retryable and the re-fetch collapses into ReplacingMergeTree like any
at-least-once replay. Verified with a mock-client harness: coalescing, whale
spanning generations, failure attribution, retry-token stability, empty-repo
short-circuit.

## ~11:30 — relaunch, and the canary flies

crawl1 canary: ~1,330 repos/min on one box (5× the whole fleet's overnight
rate). Fleet rollout: 5,393/min → 6,684/min ramping, parts steady ~200, CH
RSS 1.57 GiB of 12, disk back to 25 G, dashboard 200 in 0.3 s.

## ~12:00 — Alice asks the two best questions of the project

**"Last night you said 1–1.5 days. How were you off by almost an order of
magnitude?"** Because every ETA I gave was an extrapolation of a differently
broken system, stated with more confidence than the measurement deserved:
70 d (frozen fleet), 3 wk (ledger fixed, parts storm brewing), 10–20 d (parts
storm active). The original 1–1.5 d was capacity math that assumed healthy
software on day one. Retro rule: ETAs come from sustained measured throughput
only; extrapolations get labeled as extrapolations, out loud.

**"My friends crawl the whole network in 3–4 days with less compute."** The
single most useful debugging input of the day. It meant: nothing about the
network forbids that pace, so the gap is ours. Hunting it found bottleneck #7:
`GLOBAL_CONCURRENCY=128` — the launch-night value from when fetch+parse lived
on the main thread — AND the new batching loader parking each pipeline slot
for up to 15 s awaiting its durability flush. Slots: 50% asleep. The two
fixes (yesterday's worker pool, today's batching) had quietly invalidated the
constant between them. 768 slots: one box → ~3,750 repos/min, load 1.1 of 8
cores. Fleet → **23,646 repos/min**. Baked into nix, not drop-ins.

## ~13:00 — the 33M mystery (bottleneck #8, the silent one)

Alice, back from a break: "dashboard says 33M total repos; the network is
~44M." The dashboard was honest again — the **PLC enumeration service died
during launch night** (twice, last at 03:11, exit 1, in the ledger-contention
era when claim queries were holding the SQLite lock for seconds) and the unit
was `Restart=no`, manual-start. The DID universe had been frozen at
33,311,489 for nine hours while the crawl happily worked the stale set.
Run manually, it resumed from its cursor checkpoint flawlessly at ~3.5k
ops/s. Fix: `Restart=on-failure` in nix (cursor checkpointing makes restarts
free), restarted fleet-wide, catches up to the PLC head in ~2 h.

Bonus finding: the rate had settled to ~10k/min because the fleet is now
**host-diversity bound** — only ~238 of 768 slots downloading, hot mushrooms
(puffball: 899-deep queue) capped at 16 connections of politeness. The
missing 11M DIDs are newer accounts on newer mushrooms: enumeration catching
up IS the throughput fix. Compounding errors note for the retro: the frozen
enumeration also silently capped host diversity all day — bottlenecks #7 and
#8 masked each other.

## ~13:30 — owner's call: crank it

With 429s ambient (~0.1% per shard) and CH at 10% of cap, Alice calls it:
"PDSes are fast." `PER_HOST_CONCURRENCY_BSKY` 16 → 32, global 768 → 1536 (so
the pool can't re-bind when enumeration fans out), via nix, fleet rebuilt.
429 overshoot degrades softly (60 s retry park), so the experiment is cheap
to walk back. Ten-minute verdict pending as of this entry.

## ~14:30 — verdicts and the dashboard learns to remember

The 32/host + 1536-slot verdict: **32,743 repos/min** sustained, 429s at
~0.15% (the politeness margin was real), ClickHouse yawning — 778 inserts in
10 minutes averaging 36k rows, p95 792 ms, zero delayed/rejected inserts, ≤10
parts per partition, zero merge backlog, 1.3 of 12 GiB used, box 80% idle.
The enumeration catch-up compounds it: 35M+ DIDs and climbing, each newly
discovered mushroom adding another 32-wide lane per box. Dashboard ETA
dropped under 16 h while we watched.

The error-rate scare that wasn't: ~17% of claims "erroring" decomposed into
RepoTakendown / RepoNotFound / RepoDeactivated — the newly enumerated tail
being *classified*, which is the system working — plus 2,414 `example.test`
junk DIDs that PLC really does contain.

Dashboard truthfulness round: "data downloaded" and the download rate came
from the crawler's in-process gauges, which reset on every restart (a
fleet-tuning afternoon made the counter lurch backwards) and read 0 between
batched flushes. Both now come from the append-only events table — 336 GiB
fetched all-time, 107.85M posts — and survive any restart. Bonus bug for the
collection: the fix initially aliased `sumIf(posts, ...) AS posts`, and
ClickHouse's alias-shadows-column rule turned it into ILLEGAL_AGGREGATION —
the same trap the adjacent query documents for `ts`. The page 500'd for
four minutes and the comment block grew by one war story.

Also this afternoon, off the hot path: identity hygiene — pix's entire git
history rewritten (git-filter-repo) to scrub a legacy name from config and
emails, force-pushed; the fleet's deploy trees re-synced to match.

## ~16:00 — bottleneck #9: the morel wall (and the false accusation)

Indie-cap verdict came back looking like a disaster: 55k 429s in 8 minutes
(200× ambient). Plot twist on grouping by host: the indie PDSes were
innocent (brid.gy: 188) — **morel**, a bsky mushroom, was throwing 60k.
Mechanism: morel holds ~2× the pending backlog of any other mushroom, so as
smaller queues drain, claims concentrate on it until six boxes × 32
concurrent hit one host's rate limiter. Any static cap eventually walls like
this — the tail always concentrates onto the deepest host. Worse, every 429
burned one of the repo's five retry attempts: the storm was quietly
converting a temporary rate limit into ~158k repos parked behind the
final-sweep fence.

Fix shipped (cause, not symptom): **429-driven per-host cooldown** — each
429 arms an exponential cooldown (30 s → 10 min) for that host, and the
scheduler parks the host's whole queue *inside its per-host limiter*, which
by construction never pins global slots; every other host keeps flowing.
Plus a `markThrottled` ledger path: 429s park the repo for the backoff but
no longer burn attempts — rate limiting is evidence of OUR pressure, not the
repo's reachability. The 158k storm victims got their attempts reset and
rejoined the queue.

Retro note: the "indie caps are too aggressive" hypothesis felt obviously
right for ~10 minutes and was wrong; one GROUP BY pds_host saved a pointless
revert. Group before you blame.

Cooldown verdict (10-min window, fleet-wide): **429s collapsed 55,376 → 124**
— below even the morning's ambient level. The feedback loop replaces cap
whack-a-mole for good: whichever mushroom is deepest next week gets exactly
the pressure it tolerates, automatically.

## ~16:30 — the restart tax (emoji load 24, ETA "4.1 days", both lies)

Minutes after the cooldown deploy, emoji hit load 24 on 8 cores and the
dashboard ETA ballooned to 4.1 days. Cause, found via system.processes in one
query: six concurrent streams of `SELECT … groupBitXor(SHA256(rkey)) FROM
posts FINAL` — the **post-crash reconciliation**. The fleet restart counted
as an unclean shutdown on every box, so all six were digest-re-verifying
their last hour of loads (~1.8M repos in 1,000-DID chunks) before resuming
the crawl. The paranoid-correctness machinery, working as designed, just six
boxes at once against an 8-core warehouse. The ETA spike was pure artifact:
boxes don't crawl while reconciling, so remaining ÷ (paused rate) read 4.1
days for what was really a ~25-minute pause.

Lessons banked: (1) every mid-crawl fleet restart pays a reconcile tax —
stagger restarts box by box instead of all six at once; (2) the batched
loader's shutdown should register as clean so a plain `systemctl restart`
doesn't trigger the crash path at all (queued for post-cutover); (3) when a
load number looks insane, `system.processes` answers in five seconds what
theorizing answers in an hour.

## ~12:45 — restart tax removed from the hot path

The next tuning cycle reproduced the same failure mode: all crawler boxes had
fetch/memory headroom and almost no 429s, but emoji ClickHouse sat at 100% CPU
serving six concurrent `posts FINAL` digest reconciliations. That made the
post-rollout rate sample useless (~18.8k terminal repos/min, ~41h ETA) because
the fleet was mostly proving old loads instead of claiming new repos.

Change: `CRASH_RECONCILE_ON_STARTUP` now defaults false. Startup still requeues
stale `fetching` rows, which is the important at-least-once recovery path, but
loaded-row digest reconciliation moves to explicit verification/offline
operator mode. Normal deploys no longer turn every shard restart into a
warehouse-wide `posts FINAL` scan.

## ~12:55 — dashboard freeze was ClickHouse client timeout

The 5120/128/20 canary filled fetch slots, but the 200k-row batched inserts
started crossing the backfill client's fixed 30s request timeout. That looked
like a dead dashboard because `backfill_progress` and `backfill_repo_events`
also ride the same ClickHouse client and began dropping ticks; it also aborted
some crawler processes after repeated batch-insert failures. Immediate action:
rollback live runtime caps to 4096/96/16 and raise the backfill ClickHouse
request timeout to a configurable 180s default.

Follow-up: repo events stay lossy, but progress snapshots no longer are. The
crawler retains the newest `backfill_progress` row and retries it until
ClickHouse accepts it, and event inserts run separately so a stuck/lossy event
batch cannot freeze status counts. Dashboard freshness now uses the stalest
shard, not the newest shard, so a single fresh crawler cannot hide five stale
status rows.

Second-order fix: five shards were spending minutes in the synchronous
claim/refill loop before the first stats timer got a chance to fire. The
scheduler now yields every 1,000 scanned claim rows and telemetry emits an
initial startup snapshot before `scheduler.run()`, so long host-skewed claim
scans cannot make status counts look dead.

Third-order fix: the first 250k pending rows on every shard had become
dominated by `pds.trump.com`, `morel`, and `plc.surge.sh`. Once those hosts
were capped or cooling, the scheduler could scan a large window and still
under-fill the worker. Claim refills now ask SQLite to exclude hosts that are
already full or cooling, so the next claim window reaches repos that can use
open slots.

Fourth-order fix: the ClickHouse client was still logging idle `socket hang up`
errors on long inserts, including at least one posts batch retry. The backfill
client now enables HTTP progress headers, matching the rebuild client, so
large inserts keep the HTTP connection active while ClickHouse works.

Fifth-order fix: ClickHouse server logs showed `CANNOT_READ_ALL_DATA`, meaning
the HTTP request body was being cut mid-upload. The backfill client now gzip
compresses request bodies, and the live canary lowered post batch size so
uploads are smaller while we monitor retry rate.

## ~17:45 — bottleneck #10: cooldowns that still occupied scheduler slots

The morel cooldown fix did collapse 429s, but it exposed a second-order
scheduler bug: every shard could still pre-claim 3,072 repos from the deepest
host before the first 429 landed. The cooldown sleep lived inside the
per-host limiter, so it did not hold download slots, but each parked repo
still counted against the scheduler's `active` set. Result: every crawler sat
at `inFlight=3072`, `fetching=0`, `rowsPerSec=0`, and `topHosts=[morel:3072]`
while millions of other claimable repos waited behind it.

Fix: claim-time host admission now skips hosts that are cooling or already
queued to their per-host cap, and scans deeper into the ledger (up to 50k rows)
to find runnable work. Concurrent 429s during one active cooldown also count
as one burst instead of immediately escalating strikes to the 10-minute max.
The invariant is now explicit: scheduler `active` slots are for runnable work,
not parked cooldown sleepers.

Canary caught one more edge: if the deeper scan found some runnable work but
not enough to fill all 3,072 outstanding slots, the loop immediately scanned
again. That was a new event-loop starvation mode: only a few hundred fetches
active, but the main thread burning CPU in repeated claim scans and telemetry
silent. The scheduler now yields whenever a scan cannot fill the requested
capacity, even if it scheduled some work.

That still left a core burning on repeated 50k scans. The measured shape of
the ledger was skewed: 50k DID-ordered rows only exposed 14 hosts; 250k exposed
138, enough to fill the active pool under the per-host caps. Final fix for this
round: claim scans are deeper but amortized through an in-memory backlog, so
one ledger scan feeds many scheduler refills.

One final hot-path tax showed up after that: this tail is mostly terminal
classification (RepoNotFound / RepoTakendown / empty repos), and journald was
being asked to ingest a line per repo. Those events remain in
`backfill_repo_events`; the per-repo service logs are debug-only now.

Last canary wrinkle: after the active pool filled, fast terminal repos caused
one-slot refills, which made the scheduler rescan the backlog once per repo.
Refills now wait for one global-concurrency batch of available slots, cutting
claim-loop churn while keeping the downloader pool mostly full.

The actual live measurement was even sharper: the cap was raised to 250k, but
the multiplier still requested only 49k rows for a 3,072-slot refill. That
exposed ~324 runnable slots in this skewed tail. A 250k scan exposes ~2,711
runnable slots in ~1s, so the scheduler now has a hard 250k scan floor.

## ~14:00 — the afternoon recon: five bottlenecks were wearing one trenchcoat

Fresh session, mandate: "ETA under 1 day, telemetry 100% accurate, stop
bleeding €0.54/h." A 9-agent parallel recon (one per crawl box, one on
ClickHouse, one on the crawler code, one on the dashboard) plus live probes
produced a verdict nobody had written down yet: **ClickHouse was innocent all
along.** 91% idle, zero failed inserts in 3h from localhost, p95 246ms. The
"socket hang up" storms were the *crawler's own frozen event loop* letting
server-closed sockets rot — cause and effect had been reversed for a day.

What was actually stacking, in causal order:

1. **The operator restart loop.** 109 crawler service starts on crawl3 in 8
   hours (23 on crawl0, 18 on crawl1, 21 on crawl4 in 3h — every one a clean
   SIGTERM from a tuning session on pix2). Ramp to full slots takes 10-15
   min; restarts came every ~6. The fleet spent the entire day in cold-start.
   Each restart also requeued 2-5k in-flight repos and (on OOM crashes)
   paid ~6 min of silent reconciliation.
2. **The claim-backlog discard.** scheduler.ts dropped the whole 250k-row
   backlog whenever host caps cut a refill short, then immediately re-ran
   the fully synchronous 250k-row better-sqlite3 scan. On a tail where 38%
   of pending sat on 3 hosts, refills were always short — so the main thread
   ran back-to-back scans forever (observed: 99.9% main-thread CPU on three
   boxes, "skipped" +104k in 35s, pino→journald skew of 23s). Same genus as
   launch night's listClaimable boss fight: O(big) on the main thread, hidden
   until the tail concentrated.
3. **Heap OOMs at node's default 4GB** on 64GB boxes: 4096 slots of buffered
   CARs + a loader that couldn't flush through the frozen loop = GC death
   spiral (mutator utilization 0.029), five crashes on crawl4 alone.
4. **The archive freeze.** Every ~1M rows the sink ran DuckDB COPY + an
   inline-awaited rclone upload to the Storage Box ON the shared append
   chain — every pipeline slot in the process blocked behind a minutes-long
   sftp upload. Bursts and stalls, metronome-regular.
5. **The morel duty cycle.** Exponential cooldowns (30s→10min, strikes never
   decaying under sustained pressure) converged to ~1% duty on the deepest
   host: seconds of full-cap fetching, one 429, ten dark minutes. And the
   "4096 fetching" gauge was claims, not connections — `ss` showed **246**
   established TCP sockets under a "full" pool.

And one piece of pure queue arithmetic: **38% of all pending was three
hosts**. morel (~12M fleet-wide, rate-limited), pds.trump.com (~11M, DNS
NXDOMAIN — the domain is just gone), plc.surge.sh (~1.7M, HTTP 451 on every
request). Two of the three could never produce a single byte; their rows
existed only to poison every claim window.

## ~14:20 — the fix wave (deploy `90b9de7`+working-tree, canary crawl1)

- **Backlog keeps its tail** across partial refills; busy/cooling-host rows
  are *retained* for later refills instead of dropped; scans that schedule
  nothing arm a 1s rescan floor. The all-skip regime that melted the main
  thread now costs at most one scan per second.
- **Refill threshold** GLOBAL/16 instead of GLOBAL — the pool refills in
  256-slot sips instead of draining to half before each 4096-slot gulp.
- **AIMD per-host pressure** replaces exponential darkness: a 429 burst
  halves the host's cap (floor 1) and arms a 5s cooldown (max 2 min);
  every 20 successes raise the cap by 1; ten quiet minutes restore it. The
  cap converges to just under what each host actually tolerates — "as close
  to the rate limit as possible while respecting it" is now literal code.
- **Dead-host detection + bulk park**: 30 consecutive ENOTFOUND/451 failures
  over ≥30s with zero successes declares the host dead for the run; its
  claimable rows bulk-move (chunked, event-loop-yielding) to out-of-budget
  `unreachable` — the explicit final-sweep list. pds.trump.com and
  plc.surge.sh tripped within 90 seconds of the canary start. First park
  implementation was quadratic (the `status IN` subselect kept re-visiting
  already-parked rows); phase-split into a shrinking `pending` range +
  one-shot for unreachable stragglers.
- **Retry-arm index**: `(status, bucket, retry_after)` + ORDER BY
  retry_after, killing the planner's habit of walking the entire parked
  unreachable set per scan to satisfy ORDER BY did. 81s build on a 23GB
  ledger, done offline.
- **Archive sync off-chain**: COPY/rename/manifest stay serialized; the
  rclone upload runs on a background chain; failures stay fatal-loud
  (surface on the next sink call; startup sweep already retried unsynced
  files after crashes). Appends no longer freeze during uploads.
- **Loader flush 15s→5s** (slot occupancy ≈ flush latency; CH p95 was 246ms)
  and **--max-old-space-size=12288** (8192 on the 32GB crawl3).
- **WAL checkpoint** while stopped: crawl0's ledger WAL had grown to 7.65GB
  (crawl5: 4GB) under the restart churn.
- **Dashboard honesty round 3**: ETA now covers pending+fetching only, with
  parked unreachable shown separately ("retry waves + final sweep, outside
  the ETA"); per-shard freshness chips (amber >60s, red >300s) replace the
  silent freeze where one dead shard's counts fossilized into the breakdown;
  rate window 5→10 min so one silent shard can't zero the fleet rate while
  its remaining still counts.
- Also found and restarted: the live Jetstream ingest on emoji had been hung
  for 13+ minutes (eventsSeen frozen, cursor lag growing 1:1 with wall
  clock, writer healthy — the websocket died without reconnecting). Cursor
  replay made the restart lossless. A reconnect watchdog is still owed.

## ~15:45 — bottleneck #11: the telemetry tick was eating the event loop

The canary after the fix wave was still wrong: claims at hundreds/min,
"claim pass" instrumentation never even logging — one pass over a 250k
backlog wasn't finishing in NINE MINUTES. Two CPU profiles via the
inspector (the launch-night CDP trick, now a 30-line bun script) closed the
case in four minutes flat:

    91.5%  Statement.get ← totalPostsLoaded ← #captureProgress ← telemetry #tick
     8.5%  Statement.all ← statusCounts    ← #captureProgress ← telemetry #tick

The 10-second telemetry tick was running `SUM(posts_total)` plus a
`GROUP BY status` over the shard's ~11M ledger rows — synchronously, on the
main thread, every 10 seconds, costing ~10s per tick. It had been there
since the telemetry was built: invisible on a small ledger, lethal on a
67M-row one — the same O(n)-on-a-growing-n time bomb as launch night's
claim scan, hiding inside the *reporting* path. It also explains the
fleet-wide decay pattern everyone chased all day (each box degraded as
enumeration grew its ledger), the "ClickHouse socket hang up" red herring
(server idle-closing sockets the frozen loop never serviced), and why every
restart looked briefly healthy: ticks were cheap until the row count bit.

Two accomplices found on the way, both also real:

- The claim pass yielded via setImmediate every 1,000 rows; under I/O load
  each yield parks behind the whole poll queue (seconds), so "politeness"
  turned a millisecond walk into minutes. Yields are time-based now (only
  after 50ms of continuous walking).
- The first bulk-park implementation re-scanned every already-parked row on
  every chunk (status IN ('pending','unreachable') + attempts filter =
  quadratic), and v2 ran the park inside the claim loop, freezing claims for
  its duration. It now runs as a background chain beside the loop, and the
  monsters get parked offline (single bucket-scoped UPDATE, 2m33s for 2.77M
  rows) during rollout stop windows anyway.

Fix for #11: a dedicated ledger-stats worker thread (readonly WAL reader)
computes the aggregates on its own core every 10s; the main thread reads a
cached snapshot. The telemetry contract is unchanged — same columns, same
shard scoping — it just stopped costing the crawl anything.

Also new this round: `pds.test` auto-tripped the dead-host detector (DNS,
30 consecutive) — the machinery generalizes past the two monsters it was
built for. And the `bun run healthcheck` sweep (report `--park`) brings the
original "healthcheck every PDS first" design back as an operator tool:
probe every host owning pending rows, park the provably-dead up front.

## ~16:00–17:00 — rollout, and the registry closes the loop

Canary verdict after #11 fell: claims 46k/min in the first burst, then a
plateau that turned out to be one more layer — the in-process bulk park was
RACING enumeration. The PLC export was still streaming pds.trump.com's spam
wave in at thousands of rows a second; the park drained 'pending' at ~18k
rows/s while enumeration refilled it, so the park never terminated and ate
the main thread (profile #3: 99% parkDeadHostChunk). Structural fix, not a
pacing tweak: the dead-host verdict now persists in ledger meta
(`dead_hosts`), enumeration reads it (refreshing once a minute) and inserts
those hosts' rows BORN parked. Plus adaptive park pacing (pause ≥3× chunk
duration) and a 5-minute sweep for stragglers. Within minutes of the fleet
rollout, crawl2's registry had already self-discovered two junk hosts nobody
had named (`pds.invalid`, `follow-sqky.one-plz.cool`) — the machinery
generalizes.

Rollout mechanics worth keeping for the retro: per box, ~80s offline build
of the retry index, a single bucket-scoped offline UPDATE to park the two
monsters (8s–2.5min depending on slice), WAL checkpoint (crawl0's WAL had
hit 7.65GB under the day's churn), heap drop-in, staggered restarts. The
rollout script kept "failing" with everything actually done — `systemctl
is-active` returns nonzero for an `activating` enumerate, and `set -e` did
the rest. Three boxes of babysitting later the lesson is old and familiar:
exit codes are part of the interface.

Post-rollout per-shard resolution (6-min window, mid-ramp): shard0 3.8k,
shard1 6.9k, shard2 6.4k, shard4 3.3k, shard5 9.1k repos/min with full
in-flight pools — versus ~400/min/box at midday. The morning's "ClickHouse
is flaky" narrative ended the day as: ClickHouse never had a bad minute;
every symptom traced to the crawler's own main thread.

## Retro — what 2026-06-12 actually taught us

1. **O(n) on a growing n is a time bomb, and it ticks twice.** Launch night
   it was the claim scan; today it was the telemetry tick's aggregates. Both
   were invisible at dry-run scale, both grew with enumeration, both
   presented as *other systems* failing (sockets, ClickHouse). The class of
   bug, not the instance, is the lesson: anything per-tick or per-claim that
   touches the ledger must be O(LIMIT) or off-thread.
2. **Profile before fixing — now 3 for 3.** Four plausible theories cost two
   hours on launch night; today the inspector-over-bun-websocket profiler
   (30 lines, kept in /tmp, deserves a home in the repo) cracked each
   regression in under five minutes. The claim-pass instrumentation that
   "proved" the code wasn't running was the tell that found stale assumptions.
3. **Yielding is not free.** setImmediate every 1,000 rows under I/O load =
   the pass parks behind the entire poll queue per yield. Politeness knobs
   need budgets in TIME, not iterations.
4. **The directory is ~40% junk and that's a scheduling problem, not a
   correctness one.** ~45M real users vs ~85M PLC DIDs; one squatted domain
   held 17.9M rows. Host-level verdicts (dead list, born-parked inserts,
   healthcheck sweep) turned 20M+ rows of poison into final-sweep inventory.
5. **Static rate-limit caps always wall on the deepest host.** AIMD per-host
   caps replaced cap whack-a-mole: each host converges to just under its
   tolerance, automatically, including hosts nobody is watching.
6. **Restarts are not free even when they're "clean".** 109 restarts in 8h
   kept the fleet permanently cold. Ramp time (10-15 min) has to be priced
   into any tuning loop, or the measurements that drive the tuning are
   garbage — the day's middle hours were tuning on noise.
7. **Reporting must never share a thread with the work.** The dashboard was
   the only honest component twice over — but the act of *feeding* it was
   what froze the crawl. Telemetry now costs the crawl nothing, and the
   dashboard says when any shard's numbers are stale instead of freezing
   them silently.
8. **Cross-process state wants a registry, not a race.** Two processes with
   opposite opinions about the same rows (enumerate inserting pending,
   crawler parking them) will fight forever at millions of rows. A tiny
   meta-key contract ended it in 20 lines.

## ~19:00 — field trip: what microcosm's hubble + lightrail teach us

Read both repos (tangled.org/microcosm.blue) against our pain points.

- **Hubble is pre-launch**: the main binary is a stub, no serving API exists
  anywhere in the repo — the runbook's "point stragglers at Hubble" stays
  aspirational. But its one-shot tools have RUN a full morel dump, and the
  number is a gift: **"all of morel" was 490,694 CARs / 246.5 GB**. Morel's
  real repo population is half a million — our 18M+ morel-pending rows are
  ~97% PLC-spam DIDs that never created a repo and would all RepoNotFound.
- **The transferable trick, from both repos**: microcosm never crawls PLC.
  Discovery = relay listHosts → per-host `listRepos`; a DID absent from the
  host's listRepos is never fetched (hubble hacking.md even spells it out:
  classify against the relay, "RepoNotFound implies an old inactive repo").
  Inverted for us: ONE `listRepos` walk of morel (~1k pages at their proven
  10 rps = ~2 minutes) yields morel's true repo set; every pending morel DID
  not in it can be classified terminally with zero getRepo calls. The same
  sweep generalizes to any spam-bloated host. This deletes the largest
  remaining ETA unknown.
- Ground-truth politeness numbers from people who crawled everything:
  10 rps self-limit per bsky mushroom (used for the full morel dump), 1 rps
  for brid.gy (whose getRepo they found outright broken — "--skip
  atproto.brid.gy"), 2-consecutive-transient-failures halves per-host
  concurrency with the reduced cap persisted across restarts, and terminal
  400s deliberately never count toward backoff streaks.
- Validations of choices we already made: no-events idle timeout instead of
  ws ping/pong for firehose liveness (lightrail: 180s; ours: 45s), cursor
  replay on reconnect, per-repo `rev` recorded for surgical future resyncs
  (our ledger already stores it), probe-before-crawl host gating (our
  healthcheck sweep), host-fair queues with cooldown skip.
- Their retry ladder is saner than infinite waves: not-found retries at
  6h/24h/24h then STOPS. Worth adopting for the final sweep.

## Running ETA honesty table (for the retro)

| When | Basis | Claim |
|---|---|---|
| pre-launch | capacity napkin | 1–1.5 days |
| launch night, frozen | broken measurement | 70 days |
| post ledger fix | sick measurement | ~3 weeks |
| post parts storm, pre-fix | sick measurement | 10–20 days |
| post batching, 128 slots | healthy but self-capped | 5.8 days |
| post 768 slots | healthy measurement | ~31 hours |
| post 32/host + full enumeration | measurement pending | TBD |
| mid-day restart-churn era | tuning on noise | 3.8–6+ days |
| post #11 + dead-host park, mid-ramp | 6-min windows, 6 boxes | ~29h and falling |
| 17:15 UTC, all 6 boxes converted | 8-min resolved-delta window | **~26h** (35.4k repos/min, 55.6M remaining), AIMD still ramping |

17:15 snapshot per shard (rpm / in-flight): shard0 3.4k/6.9k, shard1
8.4k/7.7k, shard2 4.9k/7.1k, shard3 9.6k/7.2k (the 32GB box, fastest!),
shard4 2.4k/7.0k, shard5 6.7k/8.1k. emoji box load ~3.9 of 4 vCPU with the
post flood — ClickHouse finally has a job. The live Jetstream ingest also
got a liveness watchdog (45s of silence forces a reconnect): it had
silently half-open-died twice today, the second time for three hours —
caught because the dashboard's freshness honesty extends to cursor lag.

One more for the bottleneck ledger, then: #12 was the telemetry tick
(found by profile), and the count now stands at twelve across two days,
every single one of them ours, none of them ClickHouse, none of them the
network, none of them the hardware.

## 02:15 — IT HUMS (first time, briefly)

- emoji: live ingest reconnected to Jetstream, dashboard public behind Caddy
  TLS at backfill.mosphere.at, parquet shipping to the Storage Box live/.
- ~20 minutes after first enumerate page: **522k backfill posts from 558
  repos in ClickHouse**, all six shards loading in parallel, enumeration
  still paging the PLC directory at 25ms/page (~45M DIDs to go).
- Zero-touch first boot proven on crawl0: pre-baked age key → sops decrypts →
  tailscale auto-enrolls with the right tag → emoji pin resolves. No hands.

## Night watch — 2026-06-12 23:30 → (Alice asleep, goal mode)

The all-host sweeps landed while the crawlers were stopped. Four boxes
(crawl0/1/2/5) finished within minutes of one another — once we noticed all
four were independently paging `bsky.network`, the *relay*, whose listRepos
is the entire network (~42M repos, 3+ hours), to classify a grand total of
one stray ledger row. A surgical iptables REJECT aborted the walks (a failed
walk classifies nothing by design); the relay is now first on the skip list
(2c803e5). Lesson for the blogpost: never diff an aggregator, and
deepest-first ordering means the cheapest-looking host in the tail can be
the most expensive walk in the run.

Sweep haul across the four finished boxes: 11,818 hosts diffed, 156.9M
listed rows churned, 18,789,628 condemned beyond morel (8.5M of those
PLC-only ghosts). With morel's 18.4M that is ~37M repos that will never
cost a getRepo. Largest single hauls after the mushroom band: stropharia
(238k), then a long plateau of ~135-140k per mushroom.

Archive widening shipped mid-sweep (8c4be10): facets/reply/embed/labels as
raw-JSON parquet columns, NULL = pre-widening row, '' = field absent,
manifest v2, ledger meta archive_extras_since per shard. Measured cost
basis: 52 B/post text-only; widened estimate ~200 B/post, ~530GB projected
against the 1TiB box. ClickHouse untouched via explicit toClickhouseRow
pick. Live ingest cut over at 23:32 UTC, zero-gap cursor replay.

First incident of the night: the live worker died once at 23:37 on its
first stall-watchdog reconnect — @skyware/jetstream extends TinyEmitter,
and an old cast called the nonexistent removeAllListeners. Fix (59e697f)
detaches only open/close/error; codex review (gpt-5.5, xhigh) caught that
detaching the collection listener too could advance the cursor past an
unprocessed post — kept attached, late posts are at-least-once traffic.
Two review rounds, approved, deployed 23:48.

### Canary verdict — 6144 REJECTED (00:40 UTC)

crawl1 OOM-crashed at 00:09 (SIGABRT 134, V8 heap) and by 00:40 was in a
GC death spiral: 16.2GB RSS against a 12GB heap cap, 278% CPU all in
collection, event loop starved, telemetry silent for 7+ minutes while the
unit read "active" — Alice spotted the silent shard from her phone before
the next scheduled check did. 6144 in-flight on shard1's post-heavy mix
simply does not fit the heap; the two ambiguous resolved/min rounds were
this memory pressure, not repo mix. Both canary boxes reverted to 4096.
Lesson logged twice now: the failure mode of this crawler is never "down",
it is "alive and silent" — so the watchdog must key on stats-line AGE, not
unit state. A persistent fleet watchdog now alerts on >120s staleness,
crash-restarts, and unit failures across all six boxes + ingest.

### 01:04 UTC — post-revert health + ETA

All six boxes 4096, all reporting fresh (<10s), RSS at the documented
stable operating point (crawl0/4 ~10G against 12G caps — off-heap CAR
buffers, not heap pressure; crawl3 ~8G against its 8G cap on the 32GB
box, steady). emoji maximized and healthy: load ~15/8 cores, ClickHouse
~700% CPU, 0 delayed inserts, posts 374 active parts.

Canary fully vindicated: shard1 back at 4096 resolves 8,769/min — nearly
2× the 4,170-5,615/min it managed while OOM-throttled at 6144.

Per-shard resolved/min (00:45-01:04): s0 4842, s1 8769, s2 10830,
s3 5266, s4 3911, s5 4963 = 38,581/min fleet.
Remaining (pending+fetching): 27,062,492.
Honest ETA: ~11.7h → roughly 12:45 UTC. Ahead of the 13h baseline; the
listRepos sweeps (37M+ ghost rows condemned) are why the real-repo
remainder is draining this fast.

### 01:27 UTC — ETA cycle

All six 4096, fresh, RSS steady (crawl0 10.3G / crawl4 9.9G / crawl5 9.7G
against 12G caps — off-heap, stable). emoji healthy (load 14, 0 delayed,
378 parts). Per-shard resolved/min (01:16-01:27): s0 5127, s1 11189,
s2 10811, s3 4931, s4 4869, s5 5148 = 42,075/min fleet (up from 38.6k).
Remaining 26,092,371. ETA ~10.3h → ~11:50 UTC. Trending faster as the
real-repo backlog drains. No box near drain yet (smallest remaining
shard0 at 3.85M).

### 01:49 UTC — ETA cycle

All six 4096, fresh, RSS steady (max crawl0 9.9G). emoji healthy (load
14, 0 delayed, 356 parts). Per-shard resolved/min (01:38-01:49): s0 4330,
s1 12311, s2 9109, s3 4375, s4 4584, s5 4792 = 39,501/min. Remaining
25,193,109. ETA ~10.6h → ~12:25 UTC (steady). shard0 closest to drain at
3.74M, no box near zero yet. shard1 consistently fastest (small-repo mix).

### 02:11 UTC — ETA cycle + crawl4 RSS watch

All six fresh and resolving. Per-shard resolved/min (02:00-02:11): s0 4651,
s1 12009, s2 11027, s3 3877, s4 4406, s5 5585 = 41,555/min. Remaining
24,297,840. ETA ~9.7h → ~11:50 UTC (improving). shard1 dipped just under
3.6M (now smallest with shard0 at 3.64M).
WATCH: crawl4 RSS 11.4G (up from 9.8G), closest to its 12G heap cap all
night — still fresh + resolving, no spiral, but flagged for close watch.
Watchdog bvr8c8nie alerts within 60s on any staleness/restart.

### 02:20 UTC — crawl4 watch closed

crawl4 RSS receded 11.4G → 10.5G with no restart (transient whale-repo
parse, not heap pressure). All six fresh and resolving (crawl0 11.2k/s).
emoji load down to 2.9/8 cores, 0 delayed inserts — ClickHouse fully
caught up with headroom, not starved. Normal 600s cadence resumed.

### 02:31 UTC — ETA cycle

All six 4096, fresh, RSS healthy (crawl4 back to 10.9G normal range).
emoji clean (0 delayed, 384 parts). Per-shard resolved/min (02:20-02:31):
s0 7568, s1 10503, s2 11484, s3 6329, s4 4320, s5 5222 = 45,426/min (up
from 41.6k). Remaining 23,439,443. ETA ~8.6h → ~11:10 UTC (improving).
shard1 smallest at 3.32M, no box near drain.

### 02:42 UTC — crawl4 RSS watch (again)

crawl4 11.6G RSS, second whale spike (02:11 was 11.4G), nearing 12G cap —
fresh + resolving, no restart. crawl4's shard evidently holds several
whale repos. If it OOMs, the hard-exit fix self-heals (systemd restart +
fetching requeue, ~1min blip, watchdog alerts). Other 5 healthy, emoji
clean. Cadence shortened to 7min to watch crawl4.

### 02:50 UTC — crawl4 plateaued (stable)

crawl4 holding 11.6G RSS across two cycles — plateaued, not climbing,
restarts=0, fresh, resolving. Sustainable elevated state (heavier shard
working set; JS heap well under 12G cap, remainder off-heap parse buffers),
not a spiral. Other 5 healthy, emoji clean. Resumed 600s cadence,
crawl4 stays first-check. bvr8c8nie catches any crash <60s.

### 03:01 UTC — ETA cycle; crawl4 at 12.0G cap (riding it out)

crawl4 RSS reached 12.0G (its heap cap line), still fresh + resolving,
restarts=0. Decision: do NOT preemptively restart — the hard-exit fix
self-heals an OOM cleanly (systemd restart + fetching requeue, no data
loss), so a preemptive bounce only wastes 4096 in-flight fetches for the
same downtime; concurrency tuning is off-limits (6144 lesson). Watching
at 7min; bvr8c8nie alerts <60s on any crash. Other 5 healthy, emoji clean.
Per-shard resolved/min (02:50-03:01): s0 4789, s1 10141, s2 9384, s3 5538,
s4 4659, s5 5704 = 40,215/min. Remaining 22,224,065. ETA ~9.2h →
~12:15 UTC. shard1 smallest 3.02M.

### 03:10-03:51 UTC — monitoring-loop gap (honest note)

A malformed tool call at the 03:10 wakeup ended the turn without
scheduling the next wakeup, stalling the 10-min observation cadence for
~40 min until Alice nudged. IMPORTANT: only OBSERVATION paused — the
crawlers, ingest, and the persistent watchdog (bvr8c8nie) all kept
running. Watchdog logged zero alerts through the gap = fleet stayed
healthy. crawl4 rode through its whale and receded 12.0G → 11.5G with
ZERO restarts (never OOMed). Hardening: the persistent watchdog is the
real safety net (alerts <60s on crash/staleness regardless of loop
state); the per-cycle wakeup is for proactive ETA/RSS tracking only.

### 03:51 UTC — ETA cycle (post-gap)

All six healthy, fresh, RSS normal (crawl4 11.5G receded). emoji clean.
Per-shard resolved/min (03:35-03:51): s0 4854, s1 13055, s2 7787,
s3 5153, s4 4373, s5 5620 = 40,842/min. Remaining 20,178,323 (under 20.2M,
down from 22.2M). ETA ~8.2h → ~12:05 UTC. shard1 smallest 2.50M and
dropping fast; still above the 500k retire threshold.

### 04:03 UTC — ETA cycle

All six healthy, zero restarts fleet-wide, crawl4 stable 11.6G. emoji
clean (405 parts, 0 delayed). Per-shard resolved/min (03:51-04:03):
s0 5392, s1 10258, s2 8684, s3 5377, s4 4454, s5 5183 = 39,348/min.
Remaining 19,731,710. ETA ~8.4h → ~12:25 UTC. shard1 smallest 2.39M.

### 04:32 UTC — ETA cycle

All six healthy, zero restarts, crawl4 receded to 11.4G. emoji clean
(368 parts, 0 delayed). Per-shard resolved/min (04:14-04:32): s0 4901,
s1 9698, s2 12101, s3 5239, s4 3912, s5 5051 = 40,902/min. Remaining
18,565,601 (under 18.6M). ETA ~7.6h → ~12:10 UTC. shard1 smallest 2.13M.

### 04:55 UTC — crawl5 RSS spike to 15.8G (watching) + ETA

crawl5 spiked to 15.8G RSS (past 12G cap, near the ~16G OOM line — higher
than crawl4 ever reached), inFlight 7995 (~2x the 4096 fetching = whale-CAR
pileup resident in memory). Held flat 15.8G across two readings 30s apart
= plateaued not runaway; telemetry fresh, still resolving 4188/min,
restarts=0. Hands-off per design: if a new whale tips it >16G the OOM
self-heals (hard-exit + systemd restart, ~8000 in-flight requeue as
pending, at-least-once, zero loss). Tightened to ~5min watch; bvr8c8nie
alerts <60s on crash. Other 5 healthy, emoji clean.
Per-shard resolved/min (04:44-04:55): s0 4815, s1 13586, s2 7810, s3 4887,
s4 4017, s5 4188 = 39,303/min. Remaining 17,699,202. ETA ~7.5h →
~12:25 UTC. shard1 smallest 1.88M dropping fast.

### 05:00 UTC — crawl1 WEDGE (deadlock) + recovery

Watchdog bvr8c8nie alerted crawl1 stats-stale-181s. Diagnosis: process
active, restarts=0, RSS 5.5G (NOT memory), but 0% CPU across 3 samples,
main thread idle in do_epoll_wait, 69 threads in futex_do_wait, telemetry
frozen 191s+. A genuine DEADLOCK — distinct from the crashed-scheduler
zombie the hard-exit fix catches (no exception here; the event loop just
wedged, likely a parse-worker reply that never came leaving the scheduler
waiting at concurrency limit). Manual systemctl restart recovered it
cleanly: unclean-shutdown reconcile → 6 workers → 3,183 stale fetching
repos requeued (at-least-once, zero loss) → claiming within ~30s.
Detection-to-recovery ~6min. Hardening this cycle: watchdog upgraded to
AUTO-RESTART a confirmed wedge (stale>180s AND main-proc CPU<5% AND no
auto-restart of that box in 15min) so a deadlock self-heals even if the
monitoring loop is stranded. Root-cause fix (parse-pool reply timeout) is
a code change left for Alice — too invasive to do unsupervised at 5am.
### 05:08 UTC — crawl1 recovered, crawl5 coping
crawl1 fully healthy post-restart: 12.2k rows/sec, RSS 5.6G, claiming. crawl5 sustained-elevated 15.7G ~13min, resolving 8.3k/sec, restarts=0 — coping (bucket-5 whale cluster); auto-heal watchdog bzc0lok2t covers any OOM/wedge <3min so relaxed to 600s. Other 4 healthy, emoji clean (360 parts, 0 delayed).
### 05:19 UTC — ETA cycle
All six fresh+resolving, r=0. Whale-heavy window: crawl0 12.3G, crawl5 15.5G, crawl4 10.5G (all coping). emoji clean (378 parts, 0 delayed). Per-shard resolved/min (05:07-05:19): s0 3692, s1 13145, s2 6923, s3 5305, s4 4498, s5 4730 = 38,293/min. Remaining 16,902,687. ETA ~7.4h → ~12:40 UTC. shard1 smallest 1.67M.
### 05:21-05:52 UTC — crawl2 OOM self-heal + loop gap + ETA
A malformed ScheduleWakeup at 05:19 stranded the proactive loop ~33min (2nd time; the court/<invoke> tag mangle). Auto-heal watchdog bzc0lok2t covered it fully — it caught crawl2's OOM and alerted. crawl2 hit JS heap limit (status 134) ~05:32, hard-exit + systemd self-healed: 7,935 stale fetching requeued at-least-once, RSS reset 3.7G, claiming 10k/sec. Zero loss — the designed path. Whale-heavy window persists (crawl0 14.1G, crawl5 14.8G, coping). emoji clean (383 parts, 0 delayed). Per-shard resolved/min (05:40-05:52, depressed by crawl2 restart): s0 3131, s1 14243, s2 3684, s3 4831, s4 3947, s5 4536 = 34,372/min. Remaining 15,634,171. ETA ~7.6h → ~13:30 UTC. shard1 smallest 1.29M, will near 500k retire-threshold ~06:50.
### 06:04 UTC — ETA cycle
All six healthy, crawl2 rebuilding post-OOM (RSS 6.4G, claiming). Whale boxes crawl0 14.2G / crawl5 14.5G coping. emoji clean (384 parts, 0 delayed). Per-shard resolved/min (05:52-06:04): s0 4012, s1 10973, s2 9713, s3 4718, s4 3793, s5 4126 = 37,335/min (recovered). Remaining 15,193,485. ETA ~6.8h → ~12:50 UTC. shard1 smallest 1.17M, ~1h from 500k retire-flag.
### 06:26 UTC — ETA cycle
All six healthy, fresh. crawl5 stable 14.7G (coping), others 8-11G. emoji clean (369 parts, 0 delayed). Per-shard resolved/min (06:14-06:26): s0 4073, s1 12684, s2 10428, s3 4717, s4 3807, s5 4233 = 39,942/min. Remaining 14,331,948. ETA ~6.0h → ~12:25 UTC. shard1(crawl1) smallest 913k — APPROACHING 500k retire-flag, ETA to 500k ~07:00.
### 06:37 UTC — ETA cycle
All six healthy, fresh. crawl5 14.2G, crawl4 11.8G coping; others 8-10G. emoji clean (417 parts, 0 delayed). Per-shard resolved/min (06:25-06:37): s0 4047, s1 11578, s2 9971, s3 4607, s4 3321, s5 3992 = 37,516/min. Remaining 13,920,101. ETA ~6.2h → ~12:50 UTC. shard1(crawl1) 788k, dropping ~11.6k/min, crosses 500k ~07:05-07:10.
### 06:49 UTC — crawl1 2nd deadlock + double-restart race fixed
crawl1 wedged AGAIN (same signature: 0 CPU, 69 threads futex-blocked, telemetry frozen). 2nd occurrence (05:01, 06:49) — recurring deadlock on this box (parse-worker reply never returns, scheduler waits at concurrency limit). Manual restart at 06:50:23; but before the warming process emitted its first stats line, watchdog bzc0lok2t saw the OLD stale timestamp (224s)+0 CPU and auto-restarted it AGAIN — a benign-but-wasteful double-restart race (crash-safe, idempotent requeue, but interrupted warm-up). FIX: watchdog v2 (bole1g1gv) adds a 90s warming-guard — skips staleness alert AND auto-restart for any unit whose ActiveEnterTimestamp is <90s ago, so a freshly-restarted process is never judged stale by either me or the watchdog. crawl1 recovered, claiming. RECURRENCE NOTE FOR ALICE: the parse-pool reply-timeout fix should be prioritized — crawl1 deadlocks ~hourly; auto-heal masks it but it costs a restart each time.
### 07:00 UTC — crawl1 3rd wedge; watchdog cooldown lowered 15min→5min
crawl1 wedged a 3rd time (07:00, inFlight=fetching=3150 all stuck, 0 CPU) — root cause pinpointed: a getRepo fetch stalls with no reply, the worker never posts back, that concurrency slot leaks permanently; after enough leaks the scheduler wedges. The watchdog's 15min cooldown couldn't keep pace (crawl1 recurs ~10-15min), so it sat wedged during cooldown until I manually restarted. FIX (conservative, monitoring-only — NOT production code): watchdog v3 (bhuypb90j) cooldown 15min→5min. 5min >> 40s warmup + 90s warming-guard so no thrash. Now every crawl1 wedge auto-heals. ROOT-CAUSE FIX FOR ALICE: add a per-job reply timeout in parse-pool.ts run() (reject RetryableError + delete pending entry if no worker reply in ~300s) so a stalled fetch frees its slot instead of leaking it — prevents the wedge entirely. Surgical (~10 lines), but hot-path across all 6 boxes, so left for supervised deploy.
### 07:14 UTC — ETA cycle
All six healthy; crawl1 recovered from watchdog auto-heal (RSS 3.8G, 14.4k/sec). crawl5 14.9G coping. emoji clean (374 parts, 0 delayed). Per-shard resolved/min (07:02-07:14, crawl1-depressed by 2 restarts this window): s0 3699, s1 3109, s2 11012, s3 4033, s4 3532, s5 3842 = 29,227/min windowed. Remaining 12,910,910. ETA windowed ~7.4h but that understates (crawl1 downtime); at healthy ~37k/min ≈ 5.8h → ~13:00 UTC. shard1(crawl1) 704k approaching 500k retire-flag (~07:40); shard2 936k second-closest.
