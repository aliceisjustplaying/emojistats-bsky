# Backfill fleet launch log — night of 2026-06-11 → 12

Raw notes for the eventual writeup. Times are local (CEST). The goal: stand up
one serving box + six crawl boxes from bare Hetzner hardware to a running
full-network Bluesky backfill, overnight, with correctness guarantees built in.

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
Refills now batch at 512 available slots, cutting claim-loop churn while keeping
the downloader pool mostly full.

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

## 02:15 — IT HUMS (first time, briefly)

- emoji: live ingest reconnected to Jetstream, dashboard public behind Caddy
  TLS at backfill.mosphere.at, parquet shipping to the Storage Box live/.
- ~20 minutes after first enumerate page: **522k backfill posts from 558
  repos in ClickHouse**, all six shards loading in parallel, enumeration
  still paging the PLC directory at 25ms/page (~45M DIDs to go).
- Zero-touch first boot proven on crawl0: pre-baked age key → sops decrypts →
  tailscale auto-enrolls with the right tag → emoji pin resolves. No hands.
