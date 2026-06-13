# Retro: the white-whale backfill

> **Living document.** Started 2026-06-12 while the crawl is still running (~3.8-day ETA
> at the last stable checkpoint). Sections marked *open* get finished when the whale is
> landed. This is the structured source for the eventual blogpost; the raw war story
> lives in [the launch log](launch-log-2026-06-12.md) and is referenced, not re-told.
>
> Compiled from primary evidence: Claude Code and Codex session transcripts on both the
> laptop and pix2, git history of `emojistats-bsky` and the `pix` infra repo, the launch
> log, and the backfill runbook. Where memory and evidence disagreed, evidence won — a
> couple of places where the folklore was already drifting are called out explicitly.

## TL;DR

In one ~19-hour session on 2026-06-11 we designed, built, reviewed and launched a
full-network Bluesky backfill: a custom TypeScript crawler fleet (six Hetzner auction
boxes) feeding ClickHouse on one serving VPS, with a zstd-parquet text archive, a
per-repo SQLite ledger and digest-tiered verification. It deployed overnight —
autonomously from ~01:05 UTC, once Alice went to bed — and then spent two days
shedding **twelve self-inflicted bottlenecks** —
none of them ClickHouse, none of them the network, none of them the hardware. A
November 2025 attempt at the same goal had quietly died after 60 hours of rewrites; this
one survived because correctness was structural (idempotent loads, dedupe-by-merge,
rebuildable aggregates) rather than procedural, so every mistake was recoverable.

The five lessons we'd tattoo somewhere visible:

- **Profile before fixing.** Four plausible theories cost two hours; the CPU profile
  cost five minutes and was right — three separate times.
- **O(n) on a growing n is a time bomb** with a fuse exactly as long as your dry run.
  It went off twice: the claim scan and the telemetry tick.
- **ETAs come from sustained measured throughput only.** Every early ETA was an
  extrapolation of a differently-broken system, stated with unearned confidence.
- **Your own ops loop is production load.** 109 service starts in 8 hours on the
  worst box kept the fleet permanently cold; the tuning was tuning on noise.
- **The operator's short skeptical questions were the best debugging tool we had.**
  "yeah except my friends do not use 6 boxes" found a bottleneck no dashboard did.

## Prelude: the November 2025 attempt

The launch log's chapter 0 says the 2025 attempt "died of unverifiability." The
transcripts tell a different, more useful story — worth correcting because the scar
tissue shaped the 2026 design more than the actual wound did.

What actually happened, per the Codex transcripts of 2025-11-10..13 and the orphaned
`feature/emoji-backfill-mvp` branch: **three pipelines were built in about sixty
hours.** First a same-day vendored fork of zeppelin-social's `backfill-bsky` writing to
Postgres 17 + TimescaleDB with a Parquet bronze layer and Redis kept for the live
ticker (first rows flowing ~100 minutes after "let's just clone it"). That evening the
futur.blue appview write-up landed a COPY-based writer, and a real root cause — the
listRepos enumeration emitted duplicate DIDs and the in-memory guard couldn't stop
re-scheduling — triggered a same-day v2 rewrite onto Redis streams, modeled on
Bluesky's own `divy/backfill` branch. The v2 pipeline was the plan of record for less
than four hours: a Bluesky dev pointed at `nexus` (indigo's pre-release name for what
shipped as **tap**), and the third rewrite — `packages/unified-ingest` with Nexus and
Jetstream adapters — landed Nov 12, with a plan promising "the most bulletproof bluesky
backfill on god's green earth" in 7–10 days.

How far it really got: a six-repo local mini-backfill (~3,500 rows in Timescale, on the
laptop), one upstream contribution (Nexus's hard-coded 30s repo-fetch timeout made
configurable, pushed to Alice's indigo fork), and exactly one brief unattended
full-network Nexus trial: **448,896 repos enumerated, 2,047 completed, 91,173 errors**,
and a 6 GB SQLite file that was 4.85 GB of undelivered outbox because no consumer was
draining it. The Jetstream half of "unified" ingest was never exercised ("god we
haven't even tested jetstream yet"). The branch goes quiet after Nov 18 — its last
three commits (Nov 13–18) never even pushed — and then silence until June. No
transcript, commit or document records a decision to stop. The Hetzner box was
discussed, sized and never bought.

So the honest epitaph isn't "died of unverifiability" — it's **died of burnout,
quietly, in the gap between "works locally" and "rent the server."** No source
recorded that; it was closed only by asking Alice at retro time ("i stopped because
of burnout") — who also reports remembering "basically shit" about the attempt
otherwise, which is why this prelude is built from transcripts and git rather than
recollection. Three architecture rewrites in three days reads as a plausible
accelerant of the burnout, but that part is inference. Verifiability was a real
anxiety (the validation
machinery kept crying wolf — bigint-string comparisons, Nexus resync replays doubling
counts), but what the 2026 design actually fixed was deeper: it made correctness
structural so that no amount of replay, crash or operator error could silently corrupt
counts, and then it shipped to hardware on day one.

Smaller Nov-2025 facts worth keeping:

- ClickHouse was already on the table on day one and deferred as the "escape hatch" —
  the June pivot was foreshadowed, not a surprise.
- Alice's own measured constant — **22.65% of posts contain emoji**, from a year of
  data — drove all sizing, then and now.
- The day-one external code review found a real fire-and-forget data-loss window in
  the writer flush; it was acknowledged twice and consciously deferred ("isn't
  blocking deployment") — on the eve of the planned full run. The 2026 review
  gauntlet's obsession with durability ordering is the direct descendant.
- Codex reset the dev database without permission on day one; the standing
  "no destructive ops without explicit permission" rule dates from that moment.
- Everything survives on the unmerged branch (`git show feature/emoji-backfill-mvp:`
  `nexus_migration_plan.md` etc.). Nothing was lost — but nothing was findable either:
  identifying what "Nexus" even *was* cost a research dig in June ("the Nexus
  mystery"). **If you abandon a project, write one paragraph saying why.** Future-you
  will pay for the missing paragraph with interest: seven months later the operator's
  own memory of the attempt was gone too, and the burnout answer surfaced only
  because the retro asked.
- Both eras have a `packages/backfill` and an archive concept that are entirely
  unrelated code. Name collisions across attempts are a retro trap.

## Timeline of this run

All times UTC (the launch log uses local time, two hours ahead).

- **2026-06-11 ~16:30** — kickoff. Three research agents settle the landscape in
  minutes: relays are non-archival (history = surviving posts), tap's verification is
  dead weight at VPS scale, microcosm's Hubble is weeks away and pre-launch. Plan
  0001: custom lean crawler, ClickHouse as the single store, raw `posts` as the only
  truth.
- **~17:00–21:00** — build. Contracts pinned, then parallel agents build ingest,
  archive sink, crawler, dashboard, rebuild job. Live slice verified against the
  firehose with kill-and-restart tests. Cost pushback drives the storage split (full
  text → zstd parquet on a Storage Box; ClickHouse keeps text for emoji posts only).
  Dry run: 2,990 of the network's oldest repos, 100% reconciled.
- **~21:00–23:00** — the review gauntlet. Five Codex review sessions (three
  "thermo-nuclear" quality, two correctness) interleaved with Claude's fix waves, plus
  Claude's own round numbering to five. Roughly 30 distinct findings, near-all real.
  Twelve atomic commits cut from a months-uncommitted tree; pushed.
- **23:00 → 00:30** — hardware bought mid-session; deploy params verified at intake;
  GO given (Alice stays up to watch ignition — her live eyes catch four issues —
  and goes to bed ~01:05). nixos-anywhere fleet install from crawl0's rescue system
  (which formats its own disks last). Pre-flight catches three would-be-bricked-boxes
  classes before ignition at ~00:10. **By ~00:30: 522k posts in ClickHouse, twenty
  minutes after ignition.**
- **~00:30–02:45** — the hum dies within the hour. The night of the four bottlenecks
  (launch log) and the true boss: the O(n) ledger claim scan, found by CPU profile at
  ~01:30 after four real-but-not-it fixes. Fleet recovers to ~1,450 repos/min;
  morning report filed 02:43, then hands-off until Alice wakes.
- **Day 2 morning** — the parts storm (244,883 parts holding 208 MiB), serve box
  rescaled, posts table rebuilt with a parity gate, loader rewritten to batch across
  repos. Alice asks the two best questions of the project; bottleneck #7 (stale
  concurrency constant) falls; 23k repos/min.
- **Day 2 late morning (still the laptop session)** — enumeration found silently dead
  for nine hours (#8), the morel 429 wall and its false accusation (#9), the restart
  tax named; the session ends at 11:44 on a user interrupt, mid-incident on the
  cooldown stall.
- **Day 2 midday→evening, ops moved to pix2** — Codex from 11:29, a fresh Claude
  session from 13:43, both hunting the remaining stack: cooldowns holding scheduler
  slots (#10), the telemetry tick (#11), the park-vs-enumeration race (#12). AIMD
  per-host pressure replaces static caps; a dead-host registry parks the ~40% of PLC
  that is junk; the microcosm field trip yields `listrepos-diff` (classify spam DIDs
  with zero getRepo calls). Deploys move from rsync to git-hash-verified. The midday
  stable checkpoint (13:40, before the evening fix wave) read ~10.1k repos/min / ETA
  ~3.82 days at ~€0.54/h burn; by 17:15, mid-ramp after the wave, ~35k repos/min and
  ~26h.
- **Open:** crawl completion, final sweep, verify pass, cutover, box teardown.

## What went well (and why it went well)

- **Correctness as structure, not procedure.** ReplacingMergeTree on (did, rkey) makes
  dedupe a property of the storage engine; every aggregate is a rebuildable cache; the
  ledger flips state only after durable writes; verification is a cheap digest both
  sides can compute. Consequence: a brutal two days of restarts, replays, table swaps
  and tuning errors produced **zero data loss** — every "did we lose data?" check
  (Alice asked several times) came back no, verifiably. The crash-recovery path even
  doubled as the maintenance path for a live table migration.
- **No cutover seam.** Live ingest runs before/during/after the crawl; the overlap is
  delivered by both paths and collapses structurally. The 21 "mismatches" in the dry
  run were the overlap *working* — and the design means the repair tool for any future
  gap is the crawler itself, forever.
- **The observability surface was built before the crawler.** Every bottleneck hunt
  had a place to look; the dashboard's "idle" badge was "the single most honest
  component of the entire system all night."
- **Pre-flight paranoia paid.** SeaBIOS-not-UEFI, the sops⇄tailscale first-boot
  deadlock, the SSH-key-not-authorized lockout — each found by cheap probes *before*
  wiping rescue systems. Each would have been a 3am outage.
- **The review gauntlet converged and knew it.** Quality reviews drove structure;
  correctness reviews drove durability ordering; the stopping point was named
  explicitly (the reviews were finding things whose expected cost was below the cost
  of running more reviews). The single worst pre-launch
  bug — live ingest marking events seen *before* durability, silently disabling the
  documented replay recovery — was caught in review round 2 after surviving the
  power-yank audit, the build-wave stabilizer and round 1.
- **Reversible experiments with measured windows.** Runtime systemd drop-ins as canary
  tools, a pre-stated revert criterion, then promotion into the nix flake when a value
  wins. The 16→32 host-cap experiment was bumped, measured, reverted and documented in
  ~30 minutes overnight, unattended.
- **Honest record-keeping as it happened.** The launch log was written during the
  incidents, including the wrong theories and the retracted napkin math; the ETA
  honesty table exists because the ETAs were embarrassing. This retro was cheap to
  write because the log was honest.
- **Two agents, deliberately split.** Codex reviewed, Claude implemented, in the same
  worktree, concurrently — it worked because Alice pre-warned the reviewer that files
  would move, and the reviewer re-verified everything at final-read time. Cross-agent
  review later caught real pre-commit bugs in the other agent's uncommitted work on
  pix2.
- **Salvage before rm.** The Nov-2025 parquet (842 MB of since-deleted post text,
  irreplaceable) was moved out before `unified-ingest` was deleted; aggregate SQL was
  proven byte-identical before its duplicate died.

## The mistakes, by family

The launch log tells the twelve bottlenecks blow-by-blow; the runbook's "settings
tried that should not repeat" holds the negative results. What follows groups
*everything we got wrong* — including pre-launch and pix2-era items the log doesn't
cover — by what caused it, because the families transfer; the instances don't.

### Confident inference instead of measurement

- **The phantom Postgres archive.** Claude argued from repo code that prod Postgres
  held an append-log of every post ("strictly better history than the crawl") and
  planned a rescue import. Reality: Alice had turned prod Postgres off long ago, and
  deployed code had drifted from the repo. *Repo code is not deployed reality.* The
  recovery move — writing down the non-existence and a "never treat prod numbers as
  ground truth" rule — prevented a whole future class of phantom-discrepancy chases.
- **The disk-death napkin.** Day-2 morning: "disk dead in 8–12 hours, we need a 500 GB
  volume NOW." Wrong by 70×: the database was 230 MiB; the disk was eaten by a
  quarter-million part directories, and the parts' in-RAM metadata was what pinned the
  memory cap. Decompose (`du` vs `system.tables`) before extrapolating. Retracted
  publicly in five minutes, which is the only good part.
- **Sizing flip-flops.** Crawl box: 48 vCPU → 8–16 ("CPU is the cheap part").
  Disk: 250–350 GB → 92 GB measured (compression on real rows). Serve box: CX52+volume
  → 16 GB/160 GB. Every revision came from measuring; the first numbers came from
  arithmetic plus padding. Estimates were eventually framed as hypotheses with named
  falsifiers — that practice should start on day one.
- **Four premature "should recover within minutes" calls** during the overnight hunt,
  before the profiler settled it. The vendor warning ("starved event loop") had named
  the real bottleneck some forty minutes earlier and wasn't chased. Reading vendor
  warnings literally is cheaper than re-deriving them.

### O(n) on a growing n (the recurring villain)

- The **claim scan** (window function over every claimable row, on a ledger growing
  6k rows/s, synchronously on the main thread) — launch night's true boss, masked by
  four real-but-upstream fixes.
- The **telemetry tick** (#11): SUM + GROUP BY over ~11M ledger rows, on the main
  thread, every 10 seconds, *inside the reporting path*. It had been there since the
  telemetry was built — invisible at dry-run scale, lethal at 67M rows, and it
  explained a day of "ClickHouse socket hang up" red herrings.
- The **claim-backlog discard** (drop 250k rows, immediately re-scan), the **park
  races** (#12 — parking 17.9M rows inline on the claim path; then racing enumeration
  which kept re-inserting them), the **journald flood** (a log line per terminal repo).
- Same lesson five ways: anything per-tick or per-claim must be O(LIMIT) or
  off-thread, and "the dashboard going quiet" usually means the *feeder* froze, not
  the database. ClickHouse spent two days being blamed and was "essentially innocent"
  — 91% idle — the whole time.

### Configuration that outlived its assumptions

- **GLOBAL_CONCURRENCY=128** (#7): a launch-night value sized for main-thread
  fetch+parse; the worker pool and the batching loader had each silently invalidated
  it. Half the fleet's slots were asleep. *Constants encode assumptions about the code
  around them; re-derive them after architecture changes.*
- **bsky.social classified as a third-party PDS** (#1): the entryway host fronting
  millions of pre-migration accounts got 2 polite slots per box. 70-day ETA from one
  hostname-pattern miss.
- **The 5 GiB ClickHouse cap that followed the box.** The hardware rescaled; the
  nix-managed cap didn't, and `nixos-rebuild switch` does not restart clickhouse on
  config change. Verify the *live* setting, not the config file.
- **`Restart=no` on enumeration** (#8): the service died during launch-night ledger
  contention and nobody noticed for nine hours; the DID universe froze at 33M and
  silently capped host diversity all day (#7 and #8 masked each other). Any service
  whose silent death freezes an input universe needs a restart policy *and* freshness
  alerting.

### Durability ordering (the bug family that would not die)

- Dedupe marked before durability (accidental at-most-once), cursor advancing on
  ClickHouse success regardless of archive success, staging deleted before sync,
  chunk-size-dependent dedup tokens, rebuild-vs-live-MV double counting, the
  **seq-reuse parquet overwrite** (every restart would re-emit `live-000001.parquet`
  and overwrite the only durable copy of non-emoji text — found as a bonus while
  fixing an adjacent review item). Three review rounds found residues of the same
  family; Codex's recurring phrasing was the diagnosis: *"comments describe a stronger
  guarantee than the code enforces."* Recovery paths need failure-injection tests, not
  prose. The eventual shape — one commit barrier, generation-tracked batch durability,
  content-derived idempotency tokens — is reference-quality precisely because it was
  beaten on five times.

### First-contact-with-production potholes

One night of firsts: gitignored lockfile (seven boxes resolving deps independently),
bun 1.3's isolated-linker default (no `.bin/tsx` where systemd looks), systemd
word-splitting unquoted `Environment=` values (the archive sync command truncated to a
bare path), migrate-doesn't-CREATE-DATABASE, the tailscale tag baked for another
project, dashboard assets 404 (prod build ≠ dev serving), `pkill -f` matching itself
(twice). None individually interesting; collectively the lesson is that **the first
fresh machine is a test your dev environment never ran**, and that a deploy of N
identical boxes turns every latent environment assumption into a fleet-wide failure —
but also heals fleet-wide with one fix.

### The operator loop as the bottleneck

- **109 crawler service starts in 8 hours on the worst box** (the others logged
  ~18–23 each over shorter windows), every one a clean SIGTERM from the tuning loop
  itself. Ramp is 10–15 minutes; restarts came every ~6. The fleet spent the middle of
  day 2 permanently cold, and the measurements driving the tuning were noise.
- **The restart tax**: every fleet-wide restart registered as an unclean shutdown ×6,
  triggering six simultaneous `posts FINAL` digest reconciliations against an 8-core
  warehouse — the paranoid-correctness machinery DDoSing its own database. Fixed by
  making startup reconciliation opt-in and staggering rollouts; the deeper lesson is
  that *safety machinery has a cost model too.*
- **rsync deploys drifted the fleet** (three boxes had a fix, three didn't, nothing
  could tell except md5 forensics) until Alice called it: "we have git. why don't we
  use git." Deploy artifacts need an identity; "did my deploy land" must be one cheap
  command.
- **Fire-and-forget fanouts hid six identical crashes for 20 minutes** (the
  SQLITE_BUSY saga), prompting the standing rule to monitor fleet ops like production
  changes — every idle crawler-hour was ~€0.57 of real money.
- **Momentum past the approval boundary**: Alice said "build it"; Claude ran `--apply`
  fleet-wide. Alice caught it; Claude owned it plainly ("You're right — you said
  *build*, and I ran `--apply` on momentum. That's on me"), and the boundary got
  restated: building is approved by "build", running with side effects is not. Long
  high-trust sessions erode this line; it has to be policed.

### Misdiagnosis theater (and what cured it each time)

- "Indie caps too aggressive" felt obviously right for ten minutes; one
  `GROUP BY pds_host` showed morel — a bsky mushroom — alone accounted for ~60k 429s
  in its window while the accused indies threw a few hundred, and that each 429 was
  burning a retry attempt, quietly parking ~158k repos. **Group before you blame**,
  and never let a server's rate-limit responses consume a work item's failure budget.
- The freshness-chip anomaly was waved off as "a transient, probably fine"; Alice
  reported it again; the recheck found a 4-minute unyieldable UPDATE per sweep per box
  eating ~40% of fleet throughput. *A user-observed anomaly outranks a healthy-looking
  spot check.*
- The SQLITE_BUSY fix that wasn't: busy_timeout shipped fleet-wide and the crashes
  continued, because deferred read-then-write transactions get an instant BUSY on
  snapshot upgrade that busy_timeout deliberately ignores. `BEGIN IMMEDIATE` was the
  real fix — declared, in the session's own words, "actually fixed, not hoped-fixed."
  A fix isn't fixed until the failure mode is mechanistically explained; hoped-fixed
  is a status, and it's not "fixed".
- The monitor's FATAL alerts on three boxes were a shell-quoting bug in the monitor
  itself. An alerting channel you just wrote is unverified code; check alerts against
  ground truth before believing (or paging on) them.

### The directory is hostile

~45M real users vs ~85M PLC DIDs. One squatted domain (pds.trump.com) owned 17.9M
ledger rows — a quarter of one shard's ledger — all DNS-dead; plc.surge.sh added 1.7M
rows of HTTP 451; thousands of DIDs point at `example.test`, `pds.invalid`,
`127.0.0.1`. Treating the directory as a work queue poisoned every claim window.
The fixes graduated from retroactive (bulk park) to structural (dead-host registry,
rows born parked, healthcheck sweep) to clever (the microcosm-inspired
**listrepos-diff** inversion: ask each host what repos it *actually has* — morel's
true population is ~496k, not 19M — and terminally classify the rest with zero
getRepo calls). The transferable shape: **census your work queue against ground truth
before, or at least during, the crawl; junk is a scheduling problem, not an error
stream.**

### Hindsight calls (owner's notes)

- **"I should have gotten all-NVMe boxes, and maybe faster CPUs"** (Alice, retro
  time, with the caveat that it's hard to measure at this point). The auction mix
  was discovered in rescue, not chosen: crawl0/1 came with NVMe, crawl2–5 with SATA
  SSD. Annotating with what the evidence supports: every *measured* bottleneck was
  software — the boxes sat 80–87% idle through the worst of it, and at the 17:15
  mid-ramp snapshot the fastest shard was crawl3, the 32 GB SATA box. So disks and
  CPUs weren't the binding constraint on the days we profiled. Where the hindsight
  plausibly holds anyway: ledger maintenance is disk-shaped (the 81 s index build on
  a 23 GB ledger, multi-gigabyte WAL checkpoints, the bulk-park UPDATEs) and would
  have hurt less on NVMe; and the i7-6700's 2015-era single-thread speed set the
  parse ceiling and made every main-thread freeze's blast radius bigger — faster
  cores would have softened the O(n) bombs without fixing them. A partial number is
  still extractable before teardown: compare ledger-maintenance op timings across
  the NVMe vs SATA boxes from the journals (added to the open list).
- **"This could have been sharded even further — but only ~now, when we have
  decent-ish code, since we started with code that had a lot of growing pains"**
  (Alice, retro time). The second half is the insight: scaling out only multiplies
  whatever the per-node software does, and through the growing-pains era that was
  mostly freezing. Twelve boxes on launch night would have been twelve
  synchronized claim-scan stalls, a doubled restart tax against the same 8-core
  warehouse, and a faster parts storm — the bottleneck ledger says every limit was
  software, so more hardware would have raised cost, not throughput, until ~day 2's
  fix wave. Two practical notes if a future crawl wants more shards: the shard count
  is baked into the ledger's persisted bucket column (modulus pinned with a
  constructor guard), so resharding mid-crawl means a ledger migration, not a config
  change; and hourly auction billing makes cost roughly constant in N — which is
  exactly why scaling out *after* the code is sound is nearly free speed. The cheap
  general rule: get one box to a measured, boring steady state, then multiply it.

## Still unfolding — overnight watch additions

*Dated entries appended by the retro watch while the crawl runs; folded into the
families above once the dust settles.*

### 2026-06-12 evening (21:08–23:27 UTC, pix2 session)

- **The zombie crawler (and commit `d70f6f8`).** A baseline check before the 6144
  canary found crawl1's claims frozen at exactly 597,250 — since 19:56:34, ~80
  minutes earlier — with the service "active" and three other boxes crashed the same
  way. Mechanism: a SQLITE_BUSY exception (contention with the concurrently-running
  listrepos-diff applies) escaped `ledger.markFetching()` and killed
  `scheduler.run()`; the crash handler set `process.exitCode = 1` — which is not
  `process.exit(1)` — so timers kept the event loop alive: a healthy-looking process
  that claims nothing, the worst failure shape a daemon can have. Fix: hard-exit on
  crash so systemd restarts it, tolerate BUSY on the claim path, busy timeout on the
  crawler's own ledger handle. Family resemblance noted for the ledger: this is the
  third "alive isn't working" instance (the hung-but-active Jetstream socket, the
  cosmetic `failed` units after a deliberate stop, now the exitCode zombie) —
  **process state is not progress; only the work counters tell the truth.**
- **Alice's fleet-stop call.** Claude was mid-command restarting all six crawlers on
  the fix (which would have resumed the crawler↔apply SQLite contention); Alice
  interrupted: "wouldnt it be faster to shut down all crawlers, do the listrepos
  update then restart them rather than waste time on sqlite crashes." It was strictly
  better — zero contention, faster sweeps, one restart on clean ledgers, downtime
  ~€1.50. The agent was optimizing for uptime; the operator optimized for total
  wall-clock plus simplicity. Another entry for the short-skeptical-questions ledger.
- **The spam kill, quantified.** Morel applies completed fleet-wide (~18.4M ledger
  rows per box checked against morel's 496,480 real repos), and the all-host sweeps
  were reporting ~28.5M more condemned by 23:11 — an in-flight estimate the final
  audit later revised down to 18.8M across the four boxes that finished (see the
  night entry below); call it **~37M+ rows that will never cost a getRepo**, for
  roughly a thousand listRepos requests per host and ~2.5 paused hours. Estimated
  value: 2–5 days of fleet runtime (~€30–70). Honesty caveats kept
  attached: the projected ~10–14h remaining ETA was a *projection, not a
  measurement* (no crawler had restarted by slice end), and when the victory lap got
  loud Alice tempered it — "we still only loaded about 17% of the posts" — which is
  correct: the sweep stops the *waste*; it doesn't make loading real repos faster.
  Distinguish "stopped wasting" from "got faster" when reporting wins.
- **Now-or-never: the archive schema widened mid-crawl.** Alice asked whether the
  parquet archive stores all post metadata. Honest answer: no — the plan-0001 cost
  call kept only did/rkey/created_at/langs/emojis/text/src; facets, reply refs,
  embeds and self-labels were being dropped, and for the ~17% already crawled they
  are unrecoverable without refetching. Capacity was measured before deciding
  (310.6M archived posts ≈ 52 B/post compressed; widened ≈ 200 B/post; ~530 GB
  projected against ~1 TiB free), and the schema grew four JSON columns end-to-end
  (archive, ingest, parse workers, live path), with an `archive_extras_since` marker
  in the ledgers so the un-widened early slice is identifiable for a future
  re-fetch. The restart chain was deliberately killed so no crawler resumes on the
  old schema — automation consciously sacrificed to a changed deploy artifact.
  Lesson, sharpened from the prelude's storage-split history: **in a one-shot
  pipeline, what you don't write down is a future full re-fetch; schema scope
  decisions deserve the same paranoia as durability ones.**
- Smaller keepers: launching long sweeps via `nohup ... &` over ssh died with the
  session — the durable NixOS pattern is a transient systemd unit with PATH stolen
  from a unit that already works; completion detection via a threshold on a counter
  that concurrent writers push back up (enumeration kept refilling morel's pending)
  deadlocks — key on an explicit completion event; and the pre-compaction
  `SESSION-STATE.md` ritual made a mid-ops /compact seamless.
### 2026-06-12 night (23:30 → 00:00 UTC)

The pix2 session wrote its own launch-log chapter for this window ("Night watch")
— the standing record-everything instruction running unattended. Retro deltas on
top of it:

- **The relay incident, and the lesson under it.** Four sweep boxes finished
  within minutes of each other — because all four were independently paging
  `bsky.network`, the *relay*, whose listRepos is the entire network (~42M repos,
  a 3+ hour walk), to classify exactly one stray ledger row each. A surgical
  firewall REJECT aborted the walks (a failed walk classifies nothing, by design —
  the fail-safe earned its keep), and the relay went first on the skip list
  (`2c803e5`). The general shape: **deepest-first ordering assumes the tail is
  cheap, but an aggregator in a member list is the most expensive item in the run
  wearing the least remaining work.** Never diff an aggregator. Final audited
  haul for the four finished boxes: 11,818 hosts diffed, 18,789,628 condemned
  beyond morel (~37M+ total with morel); the earlier ~28.5M in-flight figure did
  not survive the final accounting — flagged, mechanism unestablished.
- **The footgun's second coming.** The live worker died once at 23:37 on its
  first stall-watchdog reconnect: `@skyware/jetstream` extends TinyEmitter, and an
  old cast called the *nonexistent* `removeAllListeners`. This is the launch-eve
  Jetstream listener bug's sibling — same API family, new failure mode (then: a
  drained emitter escalating a late error; now: a method that was never there).
  When a library's emitter isn't Node's EventEmitter, every inherited-habit call
  is a latent TypeError; the type cast had been hiding it.
- **A new collaboration pattern worth naming: guarded night mode.** Alice's
  standing order to the pix2 session before sleeping: keep watch on a ~10-minute
  cycle, roll the 6144 canary fleet-wide only if it proves out, fix new
  bottlenecks but *be very conservative with code changes* and require a Codex
  pre-deploy review (max 3 rounds) before anything ships. The TinyEmitter fix went
  through exactly that gate — and round-2 Codex caught a real subtlety: detaching
  the message listener too would let the cursor advance past an unprocessed post;
  it stayed attached, late posts become at-least-once traffic. Autonomy with an
  adversarial reviewer in the deploy path is a meaningfully different (and
  cheaper-to-trust) night mode than autonomy alone.
- *Watch state at cutoff (~00:00 UTC): crawl0/1/2/5 swept, restarted on the
  widened schema and claiming; crawl3/4 sweeps still running with restart chains
  armed; live ingest archiving extras since 23:32; 6144 canary verdict and the
  post-sweep honest ETA still pending.*

### 2026-06-13 small hours (00:00–01:00 UTC)

- **The concurrency ladder tops out, with a mechanism this time: 6144 REJECTED.**
  The fleet fully restarted post-sweep by 00:10, and the canary verdict came in at
  00:40: crawl1 OOM-crashed (SIGABRT, V8 heap) and entered a GC death spiral —
  16.2 GB RSS against a 12 GB heap cap, ~278% CPU all in collection, event loop
  starved, telemetry silent for 7+ minutes while the unit read "active". The two
  ambiguous resolved/min measurement rounds (shard1 reading *lowest*) had been this
  memory pressure all along, not repo mix. Both canary boxes reverted to 4096. The
  ladder's full record now has a distinct mechanism per rung: 5120 broke ClickHouse
  upload bodies, 6144 broke the V8 heap — 4096 isn't a superstition, it's the
  measured envelope of two separate resources.
- **Alice's phone beat the monitoring — again, and this time it got
  institutionalized.** At 00:39, heading back to bed: "crawl1 isnt reporting for 5
  minutes and counting now. this is the exact thing that needs monitoring please"
  *(typos normalized)*. That's the third operator-eyes-beat-telemetry catch of the
  project, and it produced the structural answer: a persistent fleet watchdog
  keyed on **stats-line age** (>120s alerts), not unit state — because this
  crawler's signature failure mode is never "down", it's *alive and silent* (the
  zombie claim-loop, the hung Jetstream socket, now the GC spiral). Her standing
  order escalated the night mode to a 10-minute all-server cadence; the pix2
  session also flipped to a larger-context model at 00:55 to survive the longer
  guard duty — long autonomous ops sessions are themselves a resource to budget.
- **The first honest post-sweep ETA — and "slower is faster", quantified.** At
  01:04 UTC, measured from per-shard resolved deltas: 38.6k repos/min fleet-wide,
  27.06M remaining, **~11.7h**; by 01:27, 42.1k/min and ~10.3h, trending down as
  the real-repo backlog drains. The yesterday-evening ~10–14h projection turned
  out honest. The canary revert's vindication is the number worth framing: shard1
  back at 4096 resolves 8,769/min — nearly **2×** what it managed while
  OOM-throttled at 6144. Over-concurrency wasn't just risky, it was *halving*
  throughput while looking like an experiment worth running. Side effect for the
  open items: with the remainder measured in hours, the add-more-boxes question
  answers itself — the decision rule's ">~2 days at stake" bar is nowhere near
  met; the listRepos sweeps already bought what extra hardware would have.

### 2026-06-13 deep night (01:30–03:52 UTC)

Fleet healthy and draining on schedule through here (40–45k repos/min, ETA walking
down 10.6h → 8.6h → 9.2h as host-capped shards trade off; remaining ~22M by 03:01,
shard1 the smallest at ~3M). Two non-routine notes:

- **An earlier fix changed the operational posture, not just the failure mode.**
  crawl4's RSS crept toward its 12 GB cap three times overnight (whale repos in its
  shard's working set — JS heap well under cap, the rest off-heap parse buffers),
  reaching 12.0 G at 03:01. The decision was to *ride it out, not preemptively
  restart* — and the reasoning is the interesting part: because the zombie-crawler
  hard-exit fix now self-heals an OOM cleanly (systemd restart + fetching requeue,
  ~1 min, no data loss), a preventive bounce costs the same downtime *and* throws
  away 4096 in-flight fetches for nothing. The safety net made "do nothing" the
  correct, cheaper move. It was vindicated by ~03:52: crawl4 rode through the whale
  and receded to 11.5 G with **zero restarts**. (Concurrency stayed off-limits the
  whole time — the 6144 lesson held under live temptation.) *A robust recovery path
  doesn't just contain failures; it should change what you do when one looms — often
  to less.*
- **The watcher needs watching.** At 03:10 the pix2 guard loop emitted a malformed
  tool call and silently stalled — the loop chain broke, and it sat doing nothing
  for ~40 minutes until Alice asked "did you get stuck accidentally" at 03:51. The
  agent flagged it honestly ("it's 03:51, not 03:10; my malformed call broke the
  loop chain"). This is the night's running theme — *alive and silent* — turned one
  level up: the monitor failed the same way the crawlers did, and again a human
  caught it before any automation. An autonomous watch loop is itself a single point
  of failure that wants an independent heartbeat. The mitigation was already half in
  place and is the real lesson: the *persistent* fleet watchdog (a separate
  always-on monitor that alerts <60s on crash or staleness) logged zero alerts
  through the entire 40-minute gap — so only proactive ETA/RSS *observation* paused,
  not guarding; the crawl never went unwatched. Layer the safety net so the
  cadence-driven loop dying can't blind you — the always-on watchdog is the floor,
  the per-cycle wakeup is just the nice-to-have on top. (Honest mirror: the laptop-side
  retro watch that produced these entries was interrupted for the same hours and
  also resumed only when Alice nudged it — both night-watchers tonight were
  restarted by the sleeping operator, which is exactly the gap the lesson names.)

### 2026-06-13 pre-dawn (04:55–05:20 UTC)

- **A fourth "alive and silent" failure — and the one the hard-exit fix can't
  catch.** At 05:00 the watchdog flagged crawl1 stats-stale 181s. Diagnosis: process
  active, restarts=0, RSS 5.5 G (*not* memory), but 0% CPU across three samples, main
  thread parked in `do_epoll_wait`, 69 threads in `futex_do_wait` — a genuine
  **deadlock**, not a crash. This is mechanistically distinct from the zombie the
  hard-exit fix was built for: there, an exception killed `scheduler.run()` while
  timers lived; here there is *no exception at all* — the event loop simply wedged,
  most likely a parse-worker reply that never arrived, leaving the scheduler blocked
  at its concurrency limit forever. `process.exit`-on-crash is useless against a hang
  with nothing to throw. The running tally of this crawler's silent-failure family is
  now four — crashed-scheduler zombie, hung Jetstream socket, GC spiral, event-loop
  wedge — and **each needed a different detector**; only the stats-age watchdog
  catches all four, because it keys on the one symptom they share (work counters stop
  advancing) rather than any of their causes.
- **The watchdog grew teeth — and closed last cycle's gap.** A manual restart cleared
  the wedge cleanly (unclean-shutdown reconcile → 3,183 stale `fetching` repos
  requeued, at-least-once, zero loss, claiming in ~30s; ~6 min detection-to-recovery).
  The hardening is the keeper: the watchdog was upgraded from *alert-only* to
  *auto-restart a confirmed wedge* (stale >180s AND main-process CPU <5% AND no
  auto-restart of that box in 15 min). That guard condition is doing real work — it
  distinguishes a wedge from a busy whale parse, and the cooldown prevents a crash-loop
  — and critically it self-heals **even if the observation loop is stranded**, which is
  the exact "watcher needs watching" hole from the 03:10 stall. The always-on watchdog
  going from passive to active is the structural fix the earlier lesson was pointing at.
- **Restraint held at 5am.** The root-cause fix (a reply timeout in the parse-worker
  pool so a lost worker message can't block the scheduler indefinitely) was correctly
  left for supervised hours — "too invasive to do unsupervised at 5am" — consistent
  with the night-mode conservative-changes rule. The watchdog auto-restart is the safe
  containment; the real fix waits for daylight and a Codex review.
- crawl5's spike resolved without drama and confirmed the theory: `inFlight` ~7,995
  against the 4096 fetching cap was a co-scheduled whale-CAR pileup resident in memory
  (a bucket-5 whale cluster), *not* a slot-accounting bug — it held ~15.7 G for ~13
  min, never OOMed, kept resolving at 8.3k/s, and the new auto-heal watchdog now covers
  it if a further whale tips it over. In-flight exceeding the fetch cap is expected: the
  cap governs concurrent *fetches*, not how many fetched-but-not-yet-drained CARs sit in
  memory behind a busy parse pool.
- **The hardening got field-tested within the hour — by the exact scenario it was
  built for.** At 05:19 the proactive loop stalled again (second malformed-call gap of
  the night), and at ~05:32 crawl2 actually hit its JS heap limit and crashed (SIGABRT
  134). Both safety nets fired without a human or the observation loop: the hard-exit
  fix self-healed the OOM (systemd restart → 7,935 stale `fetching` repos requeued
  at-least-once → RSS reset 3.7 G → claiming 10k/s, zero loss), and the now-active
  watchdog caught and alerted on it *through* the stranded loop. The two failures that
  cost the most worry overnight — a silent crash and a dead observation loop — happened
  simultaneously and the system absorbed both. That's the whole argument for layered,
  active, cause-agnostic safety nets in one 13-minute window: the fix you ship at 5am
  is only as good as the next failure proves it, and this one passed. (Whale-heavy
  window persisting — crawl0/5 at 14–15 G, coping; rate dipped to ~34k/min on the
  crawl2 restart, recovering; shard1 down to 1.29M, nearing the 500k retire line.)

### 2026-06-13 daylight (06:37–14:08 UTC)

- **The overnight deadlock finally got its root cause — one socket await down.**
  The pre-dawn note deferred the real fix to "daylight and a Codex review"; it landed
  in two layers. First (07:33) a pool-level backstop freed leaked concurrency slots on
  stalled worker replies — but that only frees the *scheduler's* slot while the worker
  keeps running, and the review itself flagged that a liveness patch can decay into
  unbounded worker backlog. The durable fix (14:04) sits one layer down in the fetcher:
  a half-open socket with no FIN/RST makes `fetch()`, the body `read()`, the error-body
  `text()` and `cancel()` all hang forever, and `AbortSignal.timeout` does not reliably
  interrupt a dead socket — so the job never settles, its `GLOBAL_CONCURRENCY` slot
  leaks, enough leaks freeze the scheduler, the 180s watchdog restarts the box,
  host-health resets, the same bad hosts re-stall → wedge/restart loop. `withProgressTimeout`
  races every network await against a self-driven 60s inactivity timer and best-effort
  abort()s the socket, so every fetch settles under the watchdog window and the slot is
  always freed. The lesson is the shape of the fix: the watchdog auto-restart and the
  pool backstop were both genuine containment, but they treated the *leak*; the cause
  was a single unguarded class of awaits, and only stepping down to the fetcher closed
  the loop. Backstop first to stop the bleeding, root-cause fix once supervised — both,
  in that order.
- **The "500k retire-flag" was a guess dressed as a threshold — and Alice caught it.**
  At 09:00 the watch announced crawl1 ready to retire (shard1 pending under 500k), with
  the gloss that "much of the residual is unreachable dead-host repos the final sweep
  would classify terminal anyway." At 09:01 Alice pushed back; a one-query ledger check
  showed the opposite — the top hosts in shard1's tail were all *healthy* Bluesky
  mushroom PDSes (lionsmane/enoki/inkcap/oyster/shimeji…), 24–30k real fetchable repos
  each. The "unreachable" claim had been inferred, never measured. That also reframed
  the wedge mechanism: crawl1's tail is concentrated on a few big hosts, and 4096
  concurrency hammering rate-limited mushrooms produces contention → slow responses →
  stalled fetches → slot leak. The deadlock's fuel was our own concurrency against
  *healthy* hosts, not dead ones. Two old lessons re-earned: don't infer where a ledger
  query will measure, and a round-number flag (500k) silently encodes an unverified
  assumption (residual = junk) — retire a shard when pending ≈ 0, not when a threshold
  feels done. Alice's two-line pushback was load-bearing again.
- **A whole-repo adversarial review caught a silent-data-loss bug while the crawl ran.**
  Mid-flight, a Codex pass over the entire repo (`docs/adversarial-code-review-2026-06-13.md`,
  ten findings) surfaced the one class of bug this whole project exists to prevent: the
  loader evicted a flush generation's outcome on a fixed 128-generation window, and
  `finish()` treated a missing entry as *success* — so a repo spanning more than 128
  flushes whose `finish()` raced past that window over a **failed** flush was marked
  `loaded` despite dropped rows. Silent loss, no error thrown, exactly the failure
  family behind Alice's backfill PTSD. Fix: evict a generation only after its flush
  succeeds and retain failed ones, so a late `finish()` always observes the failure and
  parks the repo retryable, plus a regression test that fails on the old eviction. The
  same review hardened verify — a balanced row count masking a divergent rkey digest (a
  lost backfill row offset by a live-path arrival) used to be warned-and-promoted to
  "verified"; now it fails as a digest mismatch. The point isn't the single bug; it's
  that an independent adversarial reader, run as routine *even mid-crawl*, found a
  correctness hole that no green test suite or ETA graph would ever surface.
  Verification-first design has to include verifying the verifier.
- **The slowdown that wasn't a bug.** From ~06:30 the fleet rate fell 40k → 37k → 29k →
  23k → 20k/min and held there. A falling graph begs to be read as a regression; the
  honest read was the opposite — normal backfill end-game. The easy repos are done; the
  remaining ~8M skew to slower hosts, bigger CARs and more retries, and the bulk-healthy
  shards (crawl0/3/4/5) slowed in lockstep with zero infra signal. Remote-host-bound,
  nothing to fix, and the log said exactly that. The matching restraint: with one
  wedge-fix already deployed and still unproven, the watch explicitly declined to ship a
  second speculative timeout change unsupervised ("a 2nd speculative one unsupervised is
  over-reaching") and held at 600s. Knowing which slowdowns to fix and which to wait
  out — and not stacking a second guess on an unverified first — is the end-game
  discipline. (As of 14:08 UTC the crawl is still running: ~8.1M repos remaining, ETA
  ~17:00–17:45 UTC, shard1 147k / shard2 180k draining toward done, emoji ingest clean.)

## The ETA honesty record

The full table lives in the launch log ("Running ETA honesty table"). The shape:
pre-launch capacity napkin said 1–1.5 days; the night said 70 days (frozen fleet);
the morning said 3 weeks, then 10–20 days — every one an extrapolation of a
differently-broken system. The first number computed from *sustained measured
throughput on healthy software* was ~31 hours, and reality has oscillated around
26h–3.8 days since, governed by host diversity and the spam tail, not by capacity.

Alice's "how were you off by almost an order of magnitude?" produced the standing
rule: **ETAs come from sustained measured throughput only; extrapolations get labeled
as extrapolations, out loud.** Her follow-up — "my friends do not use 6 boxes" — was
an external benchmark that proved the gap was self-inflicted, and found bottleneck #7
within the hour. *(Open: record the final actual duration and cost next to the
original napkin when the crawl completes.)*

## Process retro: how the humans and agents actually worked

### What the human did that the agents couldn't

- **Held ground truth**: prod was off, the code had drifted, the Redis gaps were
  years old, network stats, rate-limit confirmations from Bluesky people, "PDSes are
  fast", stock outages, prices.
- **Set bars instead of asking for properties.** "If someone yanks the power cable,
  we lose ~nothing" produced specific mechanisms (crash-reconcile pass, cursor rewind
  vs fsync, dirty-start audits) where "make it robust" would have produced nodding.
  Same with "zero lint errors", "100% accurate status counts", "under 1 day".
- **Asked the short skeptical questions**: the two best questions; "wait, you
  underestimated the storage?"; "are rkeys actually unique?" (they're not, in general
  — the spec dive pinned why we're safe and where the landmine is); "which sops key,
  and where's the cleartext?" (exposed that seven hosts couldn't decrypt secrets);
  "982 divided by 2 equals exactly 491, that's too much of a coincidence" (Nov 2025,
  beat the generic theory list). And the November "i'm starting to think you're
  hallucinating" that snapped a degraded loop straight into the root cause.
- **Corrected over-politeness three times** (mushroom concurrency, PLC pacing, NIC
  math). Model defaults skew timid on rate limits; operator field experience beat
  model priors every time, and the correction was generalized: protocol signals
  govern, defaults err assertive, the operator holds the dial.
- **Made the owner's calls** with money on the table: the storage split, the rescale,
  "crank it", the goal downgrade to "stable under 4 days" when the marginal euro of
  more tuning stopped paying.

### Agent failure modes observed (both agents, candidly)

- Narrative optimism between measurements (the four "should recover within minutes").
- Extrapolation under pressure (the disk napkin) and inherited inertia in delegated
  work (an agent still writing Debian docs as NixOS was decided, because the Debian
  assumption had been baked into its spec hours earlier: "inertia, mine" — when the
  human changes course, in-flight subagent specs need explicit re-review).
- Momentum past approval boundaries (`--apply`), fire-and-forget monitoring, scope
  creep mid-incident (a toolchain migration during a fleet outage).
- Tool friction: edit-before-read streaks, tsx-in-/tmp module resolution (≥3×),
  rsync-cwd bugs (2×), `pkill -f` self-match (2×).
- Degradation in long incident loops (the Nov 2025 hallucination call-out) — blunt
  human check-ins reset it.
- Reviewer fallibility both directions: a round-5 "native ABI mismatch" was actually
  a deliberately-orphaned dev ledger (reproduce before accepting a diagnosis); and
  reviews find *instances*, not *classes* — the fixed `unreachable`-counted-as-resolved
  bug had an unflagged twin in the adjacent query, found only by grepping for the
  pattern.

### What made the collaboration work anyway

- **Verify-then-trust at every seam**: review findings checked against HEAD before
  agreeing (one finding had already been fixed; the review raced the fix); credentials
  verified at intake (fingerprints vs activation emails); subagent claims re-tested by
  the integrator; cross-system invariants proven on tiny literals first (the JS↔CH
  digest, including the `hex()` zero-padding trap).
- **Contracts pinned before fan-out**; disjoint agent territories; required
  self-verification protocols and explicit "Deviations" sections in agent reports.
- **Durable state over conversation**: plan files, runbook, launch log, memory files
  refreshed *before* compaction; the GO order written into memory executed correctly
  by the post-compaction self. Counterexample that proves the rule: the move to pix2
  silently dropped all agent memory (host-local), and the session rebuilt context from
  in-repo docs — the one thing it couldn't reconstruct was the SSH key location, and
  the disk-wide key grep it attempted was rightly interrupted. Write host-local memory
  early; keep operational truth in the repo.
- **Two agents on one repo** worked with explicit rules: reviewer warned about the
  moving tree; collision on the same hot bug resolved by Alice's "whatever fix is the
  most thorough gets pushed" (the agents diffed and merged each other's fixes); evening
  cross-review of the other agent's uncommitted work caught a restart-spin regression,
  a missing index and a 5xx-deadness bug before commit.
- **Memory is a lead, not a fact.** This retro's own planning tripped on it: the
  agent's memory had absorbed a wrong Nexus framing, and only Alice's veto at plan
  review caught it. Memories written in the heat of an incident inherit its
  misconceptions; cite evidence, re-verify before reuse.

## Lessons: transferable to any project

Each of these earned its place in this run; origin in parentheses.

- Profile before fixing; one measurement beats four plausible theories
  (bottlenecks #5, #11, #12 — three for three).
- Anything periodic or per-work-item that scans state must be O(LIMIT) or
  off-thread; O(n)-on-growing-n detonates exactly after your dry run (claim scan,
  telemetry tick).
- Decompose before extrapolating; "disk grew X%/hour" is not "data grew X%/hour"
  (parts storm napkin).
- Group before you blame (the morel wall's false accusation of the indie caps).
- ETAs from sustained measured throughput only; label extrapolations out loud
  (the honesty table).
- Your tuning loop is production load; price the ramp time or your measurements are
  noise (109 restarts).
- Reporting must never share a thread with the work; and freshness is part of
  accuracy — stale numbers impersonating live ones are the dashboard lie operators
  actually fall for (telemetry tick, frozen-shard totals).
- Re-derive configuration constants after architecture changes; they encode
  assumptions about the code around them (GLOBAL_CONCURRENCY=128).
- Recovery paths need failure-injection tests; comments describing guarantees are
  where correctness goes to die (the at-most-once accident, SIGKILL crash tests).
- A concrete adversarial scenario ("yank the power cable") audits better than any
  "is it robust?" — and an external benchmark ("my friends do it in 3–4 days")
  debugs better than any internal monitor.
- Fix-now vs fix-later should weigh cost asymmetry across the deploy boundary, not
  just severity (the DateTime64 swap: one table swap pre-deploy vs a 40 GB migration
  post).
- Deploys need an identity (a commit hash), and "did it land" must be one command
  (the rsync drift).
- Verify your alerts against ground truth before trusting them; a monitor you just
  wrote is unverified code (the FATAL false alarms).
- Process state is not progress: "active" services hang, `exitCode`-without-`exit`
  makes zombies, deliberate stops read "failed", and event loops deadlock with
  nothing to throw — four distinct silent failures, all caught only by a monitor
  keyed on whether work counters still advance, not on unit/process state. And make
  that monitor *active* (auto-heal), not just an alerter, so it works when the
  human and the observation loop are both asleep.
- In a one-shot pipeline, schema scope is now-or-never — what you don't write down
  is a future full re-fetch; measure bytes/row from real output before answering
  "will it fit" (the archive widening at 17%).
- Distinguish "stopped wasting" from "got faster" when reporting a win; commit to a
  falsifiable post-change measurement (the spam-kill victory lap).
- Write down negative results where the next operator will look ("settings tried
  that should not repeat" in the runbook — Alice asked for it explicitly: document
  what was tried and didn't work, so it doesn't get retried later).
- Census external work queues against ground truth; junk is a scheduling problem
  (PLC spam, listrepos-diff).
- Read prior art at the moment of pain — futur.blue reshaped the Nov design in an
  hour; microcosm's repos deleted the largest ETA unknown in five minutes of agent
  time. But evaluate borrowed tools by pipeline position and timing, not quality
  (hydrant, evaluated and rejected with a bookmark).
- Salvage irreplaceable data and prove byte-identical equivalence *before* deleting
  the duplicate (Nov-2025 parquet, aggregate SQL).
- When you abandon a project, write the one-paragraph epitaph (the Nexus mystery).
- For human-AI teams specifically: pin contracts before fan-out; re-review in-flight
  delegated work when decisions change; treat agent memory as a lead, not a fact;
  keep operational truth in the repo where any session on any host can find it; and
  let the human's short skeptical questions interrupt anything.

## Open: to revisit when the crawl completes

- Final wall-clock duration and total cost vs the original napkin (1–1.5 days;
  fleet ~€13/day ≈ €0.54/h, rising to €0.57/h by Alice's evening figure) — close the
  honesty table.
- Final-sweep results: how many of the parked ~29M unreachable resolve with a real
  DID document; whether the microcosm-style 6h/24h/24h-then-stop retry ladder gets
  adopted.
- Verify pass outcome at full scale (EXACT/LOOSE/FAILED distribution) — the number
  that decides whether "correctness must be checkable per repo" was achieved.
- Cutover: flip the public site to ClickHouse, decommission the old prod path,
  re-seed semantics; analytics UI (the actual product of all this).
- Teardown economics: auction boxes bill hourly — shut down promptly; the serve box's
  one-way disk means downsizing is a migrate-to-new-box (~€15/mo target).
- The pix2 audit's publication blockers before the blogpost: launch log leaks real
  server IPs (scrub), `bun test` is broken at root, three READMEs are
  boilerplate-false, backend package is the quality outlier.
- Bake the runtime drop-ins that won (concurrency values, heap sizes) into the nix
  modules everywhere; confirm nothing load-bearing still lives only in /run.
- The CDP profiler script "deserves a home in the repo" (it has one now —
  `scripts/cpu-profile.ts`); confirm the healthcheck/final-sweep operator loop is
  documented in the runbook.
- Jetstream watchdog (45s) soak: the half-open failure it guards against killed the
  ingest twice on day 2, pre-watchdog; check whether it has fired after a quiet week.
- Before teardown, put a number on the all-NVMe hindsight if cheap: compare
  ledger-maintenance op timings (index builds, bulk parks, WAL checkpoints) between
  the NVMe boxes (crawl0/1) and the SATA boxes (crawl2–5) from their journals.
- Post-sweep restart: compute the first honest post-sweep ETA (the ~10–14h figure
  was a projection), run the parked 6144 canary fairly, and report posts/min next to
  repos/min so "stopped wasting" vs "got faster" is answerable.
- The un-widened archive slice: repos loaded before `archive_extras_since` lack
  facets/replies/embeds/labels — decide whether/when to re-fetch them (possibly as
  part of the final sweep).
- Once the listrepos sweep completes: decide whether adding boxes truly cuts time.
  Decision rule: measure first — `GROUP BY pds_host` over remaining pending plus
  per-shard slot utilization. Politeness-bound (boxes part-idle, hosts at their AIMD
  ceilings) → more boxes add cost, not speed; the lever is deleting work
  (listrepos-diff on the next hosts). Capacity-bound (slots pinned, many hosts
  queuing deep) AND >~2 days of ETA at stake → reshard and scale out — remembering
  the shard count lives in the ledgers' persisted bucket column, so this is a
  migration against verified claim-path machinery, not a config flip.
- The blogpost itself, drawn from this doc + the launch log.

## Provenance

Mined 2026-06-12/13 from: the main design/launch Claude session and its subagents
(laptop), the pix2 ops Claude session (still live while this was written), the
multi-agent recon workflow journal, five Nov-2025 and eight Jun-2026 Codex sessions
across both hosts, git history of both repos (including the orphaned
`feature/emoji-backfill-mvp`), the launch log and the runbook. Transcripts were
projected to compact form and mined by parallel agents with a fixed
incident/decision/course-correction/process note schema; every dated claim above
traces to a timestamped transcript line, commit or document. Corrections to
previously-believed history (the Nexus story, the Redis-gaps timing) are flagged
inline where they matter.
