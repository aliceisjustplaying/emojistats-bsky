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
- **~17:00–21:00** — build. First, toolchain groundwork: oxlint + oxfmt adopted
  repo-wide, Node 24 pinned, TypeScript 7 preview (tsgo) chosen for typechecking
  (`b4f642d`, `323db86`, `2ae6601`) — a bet on an unreleased compiler for a production
  system, traded for significantly faster typechecks across the monorepo. Then contracts
  pinned, and parallel agents build ingest, archive sink, crawler, dashboard, rebuild
  job. Live slice verified against the firehose with kill-and-restart tests. Cost
  pushback drives the storage split (full text → zstd parquet on a Storage Box;
  ClickHouse keeps text for emoji posts only). Dry run: 2,990 of the network's oldest
  repos, 100% reconciled.
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
  with zero getRepo calls). Deploys move from rsync to git-hash-verified. The Jetstream
  45s silence watchdog (`2fd3233`) lands in the same batch — the half-open socket
  failure that killed the ingest twice on day 1 gets a structural guard rather than
  another manual restart. Every outstanding lint error goes to zero (`053970b`, 38 → 0)
  as a quality gate before the overnight autonomous run. The midday stable checkpoint
  (13:40, before the evening fix wave) read ~10.1k repos/min / ETA ~3.82 days at
  ~€0.54/h burn; by 17:15, mid-ramp after the wave, ~35k repos/min and ~26h.
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

### 2026-06-13 mid-afternoon (15:01 UTC)

- **The wedge saga's third act: the fetcher fix exposed a hole one level up — a bad
  host nobody was learning from.** The 14:04 `withProgressTimeout` made every stalled
  fetch *settle* (abort + retry), which stopped the scheduler wedging — but
  settling-as-retryable just re-claims the same silent host on the next pass. A
  half-open host that accepts the socket then never delivers (the rate-limited
  `atproto.brid.gy` bridge, owning a drained shard's tail, was the live culprit) was
  invisible to *both* back-off systems: host-pressure cools only on a 429, host-health
  parks only on DNS `ENOTFOUND` / HTTP 451. So the host produced bounded "stalled: no
  progress" errors forever at ~0 throughput — never cooled, never parked, burning
  dedicated-box hours for nothing. The fix routes stalls into both layers: a soft AIMD
  cool-down (the same `penalize()` path as a 429) and a hard park after 6 sustained
  stalls across 120s of zero successes (its own thresholds, distinct from the DNS/legal
  30/30s; any success or HTTP response resets the streak, and a kind-change restarts it
  so DNS and stall streaks can't fuse). Stalls use `markThrottled` so no attempt budget
  is burned, and parked rows go to `unreachable` — the deferred final-sweep re-crawl
  list — so nothing is lost. The transferable shape: a per-request timeout buys
  *liveness* but not *learning*; if the selector can re-pick a persistently-bad
  resource, you also need per-resource back-off so the system stops choosing it. Three
  layers now stand behind this one failure — restart (watchdog), settle (fetcher
  timeout), avoid (host cool/park) — each catching what the one before it could not.
  (Codex-reviewed, 2 rounds, which caught a kind-carryover and a quarantine-reset gap;
  tests cover stall classification, the 6/120s park, the resets, and the AIMD.)
  **[Corrected at 15:30 — see the next entry. Two claims above are wrong: the
  `atproto.brid.gy` bridge is 429-rate-limited, not a silent stall (the existing
  429 back-off already handles it, and the lone canary box logged zero stalls
  against it); and host-parked rows are *not* auto-re-crawled by the final-sweep,
  so "nothing is lost" overstated it. The general stall fix still stands; the bridge
  was a misdiagnosis and the deferred-re-crawl guarantee does not currently hold.]**

### 2026-06-13 afternoon (15:30 UTC) — a second-opinion review, and two corrections to the entry above

- **The bridge was 429, not a stall; and "park = deferred re-crawl" doesn't hold.**
  Two claims in the entry above need walking back, both surfaced within the hour by
  evidence. First: I cited `atproto.brid.gy` as the stalling host the cool-and-park
  fix was built for. A direct probe settled it — the bridge returns HTTP 429 in 0.23s,
  fast rate-limiting, *not* a half-open stall; crawl1 (the only box carrying
  cooling-on-stall) logged zero stall events against it, because the existing 429 AIMD
  back-off already parks it in cooldown. The general stall fix is still correct — dead
  half-open sockets exist and did wedge the fleet — but the bridge was the operator's
  initial misdiagnosis, corrected by probe, and the cool-and-park code comments that
  cite the bridge as a stall example are themselves misleading. Second, and worse: I
  repeated that hard-parked hosts land in `unreachable` and get re-crawled by the
  deferred final-sweep, "so nothing is lost." A second-opinion review showed that is
  false for *host*-parked rows — `--final-sweep` (`resetUnreachableAttempts`) only
  resets budget-exhausted unreachables, but the scheduler re-seeds and re-parks dead
  hosts on startup, so a host parked dead (DNS/legal — and cooling-on-stall's hard
  park) stays excluded *forever* unless a dead-host-clear path is added. The repos
  aren't dropped from the ledger, but the system does not bring them back on its own.
  "Park = deferred re-crawl" should read "park = excluded until that gap is fixed."
- **The incident worth keeping: an adversarial review of the *narrative*, not the
  diff, caught the overclaims.** The day's code shipped sound — loader silent-loss,
  fetcher wedge, cool-and-park all held up under review. What didn't hold was the
  operator's *state report* (`docs/state-report-2026-06-13.md`), a confident write-up
  of "what's safe to do now." A Codex second-opinion pass over that report, read
  against the actual code, returned three HIGH corrections to the operational story,
  none about the code: (a) host-parked ≠ re-crawled, above; (b) `scp ledger.sqlite` to
  save a retiring box's record is unsafe — the ledger is WAL-mode, a raw copy can miss
  committed WAL pages, so correct prep is stop-crawler → SQLite checkpoint/`.backup` →
  copy; (c) the report omitted the per-dedi local parquet archive entirely — that's the
  durable full-text home, written before the ClickHouse load, and retiring a box
  without verifying its archive-sync state risks losing text. It also walked back
  "verify proves zero-loss" to "verify is strong evidence, not proof" (loose
  `CH count > ledger` passes get promoted, `--sample` is random-only, and re-fetching a
  current CAR can't prove rows deleted since the original crawl). The transferable
  lesson is sharp: review the claims a system makes about *itself* — its runbook, its
  "safe to retire," its "nothing is lost" — with the same adversarial energy you give a
  code diff. Confident summaries overreach exactly on guarantees, and guarantees are
  where a backfill lives or dies. The decision that fell out of it: holding the
  cool-and-park rollout to a single canary box was *right* for a reason not fully in
  hand at the time — its hard-park would strand the bridge repos given the final-sweep
  gap, not defer them — so the version skew (crawl1 ahead of the other five) is a
  deliberate, reversible hold, not drift.
- **A detector keyed on a proxy: the watchdog false-positives on a busy box.**
  Separately, crawl0 spent the afternoon flapping "wedge → RECOVERED" without ever being
  wedged. The auto-heal watchdog keys staleness on the gap since the last `crawl stats`
  log line (>180s = suspect); but a box churning a failure-heavy host — `kt.tngl.oyster.cafe`
  404ing every request — emits stats on a cadence that drifts to 130–180s while it is
  fully alive (newest log line 6–16s old, event loop running), because its repos resolve
  to `failed`, not `loaded`. The decisive test cut through it: `loaded` advanced 0 in 25s
  *not* because the box was frozen but because it was producing failures, not loads — so
  the stale-stats signal and the real-progress signal disagreed, and the stats signal was
  the wrong one. `cpu=0` doesn't disambiguate either (it samples the near-idle main
  thread — the same reason the 6144 canary read cpu=0 while perfectly healthy). And the
  false restart isn't free: it resets host-health and kicks off the very ramp/churn the
  fleet has been fighting. This sharpens the earlier "key the detector on the symptom
  every failure shares — work counters stop advancing" lesson: stats-emission cadence was
  a *proxy* for progress that legitimately varies with workload, so the real fix is to key
  the watchdog on an actual progress delta (`loaded`/`postRows`), not on whether a log line
  showed up. A liveness check is only as good as the signal it trusts.
- **Small and clean: deploy one box at a time.** Earlier in the day all six boxes were
  `reset --hard` to a new build and restarted at once; the simultaneous cold-ramp drove
  ClickHouse to load 16 with 30s insert-timeouts and produced a transient CH-overload
  wedge on crawl0 that cleared only once the fleet settled. A backfill's datastore is a
  shared resource; a fleet-wide synchronized restart is a self-inflicted thundering herd.
  Stagger deploys with gaps. (Fleet at 15:30: crawl0/3/4/5 productive and wedge-free since
  the fetcher fix, crawl1/crawl2 idle on the 429 bridge tail ~15k each and slated to
  retire, ClickHouse healthy at 2.06B rows. The pending operator decisions — retire the
  two idle boxes after a safe ledger copy-off, finish or revert the cool-and-park rollout,
  and when to run the verify→re-crawl convergence pass that is the only *provable* zero —
  are the live edge of the project now.)

### 2026-06-13 late afternoon (16:00 UTC) — the dead-host gap quantified; "retire" ≠ "finish"

- **The "park = excluded forever" gap, measured — and it's the *stall*-park that's the
  landmine, not DNS/legal.** The entry above flagged that hard-parked hosts aren't
  auto-re-crawled; pushed on by Alice ("this seems bad on a first look"), the operator
  read the code and the live ledger instead of trusting the relay, and the picture
  sharpened in a way that both confirms and softens it. The mechanism is real:
  `addDeadHost` persists a host's death, every startup (including a `--final-sweep` run)
  re-seeds the dead list and `unavailableHosts()` excludes it from claim scans at the SQL
  level, while `resetUnreachableAttempts` zeroes only each row's *budget*, never the host
  verdict — so there is no path to un-dead a host. But the *impact* turns entirely on
  which hosts are on that list, and the data says they're the right ones: shard0's 5.2M
  `unreachable` rows are 4.68M on `pds.trump.com` (DNS NXDOMAIN — the domain is gone),
  295k on `plc.surge.sh` (HTTP 451 legal block), a pile of `*.test`/localhost/ngrok junk,
  and ~25k on *alive* `bsky.network` hosts — and those 25k are **under** max-attempts and
  **not** dead-listed, so they aren't excluded and the sweep/retry waves do pick them up.
  Not re-crawling a domain that no longer resolves isn't data loss; it's correct. So
  "unreachable = re-crawl later" was wrong as a *blanket* claim, but only for rows that
  are genuinely uncrawlable by anyone. **Where the gap actually bites is the stall-park
  from two entries up:** DNS and legal deadness are permanent and that's fine, but a
  *stall* can be transient — a host quiet for two minutes may be perfectly alive — and
  `addDeadHost` would persist that as permanent death with no re-probe. Cooling-on-stall's
  hard-park is therefore the one that would strand recoverable data, which is exactly why
  holding its rollout was right, and why the prerequisite for ever shipping it is a
  dead-host *clear* path (periodic re-probe, or TTL'd stall-deadness kept distinct from
  permanent DNS/legal deadness). The lesson sharpened twice: a relayed finding deserves a
  code-and-data check before it's allowed to alarm *or* reassure (the check downgraded
  "the final sweep is broken" to "one un-shipped feature needs a reversal path"), and
  "permanent" is the right default for some failure kinds and a bug for others — the
  back-off system has to tell DNS/legal death apart from a transient stall.
- **The watchdog false-positive fix shipped — keyed on silence, not on the stats line.**
  Item 1 of the queue landed: the auto-heal watchdog now measures staleness by the newest
  log line *of any kind* rather than the `crawl stats` line specifically, thresholds
  raised (alert 120→180s, restart 180→240s). The reasoning is clean — a box that's alive
  but failure-heavy (crawl0 churning 404s) keeps logging *something*, so it no longer
  reads as wedged, while a genuine event-loop freeze emits nothing at all and is still
  caught. It's a slightly different resolution than "key on a `loaded` delta" (which would
  misread a box legitimately producing *failures*, not loads, on a bad-host tail) — total
  log silence is the more faithful proxy for "the process is actually stuck." The right
  liveness signal is the one that goes quiet *only* when the thing is truly dead.
- **"Retire" a near-done shard means "defer to a cheaper mop-up," not "finish."** Alice
  caught the conflation — retiring crawl1/crawl2 doesn't complete their work, it leaves
  ~15k pending repos each with no crawler assigned, so the shard-1/2 backfill stays
  *incomplete* until something finishes it. The honest framing is retire-and-mop-up:
  preserve the ledgers, retire the two dedis, then point one cheap box (or a freed
  productive box once it drains its own bulk) at shards 1 & 2 to grind the remainder. The
  sizing insight is the sharp part: the tail's bottleneck is the bridge's 429 rate limit,
  not crawler capacity — crawl1 drained ~300 of 15k in *hours* — so two full dedis finish
  it no faster than one cheap box, just at multiples of the cost; weeks of dedi-time for
  ~30k repos is absurd money for a tail. Match the mop-up to the real constraint (a rate
  limit you wait on), not the nominal work (repos to fetch). The binding caveat: this is
  only zero-loss if the mop-up is actually committed and run — retire-and-forget would
  abandon those ~30k real repos as a permanently incomplete backfill, even though nothing
  was "lost."

### 2026-06-13 ~16:10 UTC — first box retired; the run in hard numbers

- **First box retired — and the corrected runbook earned its keep.** crawl1, the first
  of the two bridge-tail boxes, was retired cleanly at ~16:05, executing the exact safety
  sequence the 15:30 second-opinion added. *Archive first:* `ARCHIVE_SYNC_COMMAND` is
  `rclone move {file} → storagebox:emojistats-archive/shard1/`, so the durable full-text
  copy already lives off the dedi (44 GiB / 763 objects) and zero parquet remain locally
  *by design* — the thing that had to be checked-and-preserved was already safe, but only
  because someone checked. *Then the stop:* a single graceful SIGTERM that drains
  in-flight → `shutdown()` → `archiveSink.close()` finalizes the open parquet and runs the
  sync hook (`Result=success`, `ExecMainStatus=0` — not the force-quit-at-130 that would
  skip the flush). *Then the ledger:* WAL `checkpoint(TRUNCATE)` → consistent `.backup` →
  integrity-check → copied off-box, because a raw copy of a WAL-mode SQLite file can miss
  committed pages. Every one of those steps was a Codex correction from two entries up; in
  the live retire each was load-bearing, and skipping any would have risked the exact
  text-or-ledger the project exists to protect. The cleanest possible vindication of
  reviewing the *runbook*, not just the diff.
- **The run in hard numbers — and what "95% resolved" actually means.** Captured into
  `docs/backfill-stats-2026-06-13.md` (committed `fe96256`, and copied to
  `storagebox:_meta/` so the numbers travel with the archive for the mop-up): of **95.47M
  repos enumerated, 91.16M (95.48%) are resolved**, 4.31M (4.5%) still to crawl. But
  "resolved" and "data" are very different sizes, and that gap is the story, not a
  shortfall. Only **37.6% (35.89M) carry any captured data** — and of those, 24.08M are
  *empty* accounts (crawled, zero posts, entirely normal on Bluesky); the posts-bearing
  core is just **~11.8M DIDs → 2.09B posts** (384.7M with at least one emoji) and **~208.8
  GiB** of parquet text across 3,613 objects on the storagebox. The rest of the network is
  tails: **25.4% terminal/no-data** (21M of it 404 `RepoNotFound`, plus 2.8M taken-down),
  and **32.5% unreachable** — dominated by genuinely-dead hosts (`pds.trump.com` at ~4.7M
  per shard is a DNS NXDOMAIN; `plc.surge.sh` is a 451 legal block), not crawl failures.
  So the honest headline for the blogpost isn't "we got 95%": it's that a full-network
  backfill is mostly *accounting for absence* — empty repos, deleted accounts, dead PDSes
  — and the actual emoji corpus is ~12M active authors out of ~95M enumerated identities.
  The 4.31M remaining is the four productive shards still draining (s0 864k, s3 1.05M, s4
  1.23M, s5 1.14M) plus the two ~15k bridge tails now retiring into the deferred mop-up.

### 2026-06-13 ~16:25 UTC — the Bridgy 429 that was never a rate limit

- **The bridge tail wasn't throttling — `getRepo` is *disabled* on Bridgy Fed, dressed
  as a 429.** Confirmed two ways: Bridgy's creator told Alice directly, and a probe
  reproduces it. For a real bridged DID (`ap.brid.gy`, PDS `atproto.brid.gy`),
  `com.atproto.sync.getRepo` returns **HTTP 429** with the body *"temporarily disabled
  12 hrs after repo creation"* — Bridgy Fed serves the full-CAR export only for a repo's
  first ~12 hours, then disables it. Bridgy doesn't persist full MSTs for bridged
  accounts (it materializes records on demand), so there is no CAR to hand back — and it
  signals that *permanent* absence with **429, a rate-limit code**, not a 404 or
  `MethodNotImplemented`. Our crawler is getRepo-only, and a getRepo-only crawler reads
  429 as "alive, back off, retry" — so it retried a permanently-disabled endpoint under
  exponential backoff, forever, on every one of the ~30k bridged repos in the two shard
  tails. A permanent wall returning a transient status: that mismatch is the entire time
  sink. (Earlier entries called this tail "429 rate-limiting, real repos, just slow" —
  half right: the repos are real, but it was never going to clear with patience.)
- **And the data was reachable the whole time — just not via `getRepo`.** The same probe
  shows `com.atproto.repo.describeRepo` → **200** (the account exists, lists
  `app.bsky.feed.post` among its collections) and
  `com.atproto.repo.listRecords?collection=app.bsky.feed.post` → **200**. The bridged
  accounts aren't unreachable and their posts aren't lost; the one method we depend on is
  the one method Bridgy disables. The mop-up implication is concrete: shards 1 & 2's
  bridge tail is finishable by fetching those DIDs through `listRecords` (paginated per
  collection) instead of `getRepo` — a path that returns data rather than one that
  structurally never will. This retires the "weeks of dedi-time" estimate from the retire
  entry, which assumed getRepo throttling. (Honest caveat: the probe confirms the
  `listRecords` *path* works, not Bridgy's `listRecords` rate limits under real volume —
  that's untested.)
- **The lesson: a 429 is a status code, not ground truth — and atproto PDSes aren't
  uniform.** Two transferable rules. First, treat "429 with zero successes *ever*, over a
  long window" as categorically different from "429 with intermittent success": the
  former is a wall (unsupported, disabled, blocked) wearing throttle's clothing, and the
  back-off system should escalate it to terminal-for-this-method rather than retry it
  forever — what the cool-and-park work gropes toward, but keyed on *outcome history* (has
  this host/method ever succeeded?), not on the status code in isolation. Second, don't
  assume protocol uniformity: an atproto "PDS" can implement a subset of the sync API, and
  a five-minute capability probe (getRepo vs describeRepo vs listRecords) reveals it — we
  paid in crawl-days for a fact a single `curl`, or one question to the bridge's author,
  would have surfaced on day one. Capability-detect the host, carry a
  `getRepo`→`listRecords` fallback, and never let an HTTP status override what the host is
  observably doing.

### 2026-06-13 evening (17:00–21:00 UTC) — the handoff

- **The dead-host exit-ramp shipped, and Bridgy got blacklisted — closing two threads
  from this afternoon.** The 16:00 entry flagged that there was no path to un-dead a host,
  which is what made cooling-on-stall's hard-park unsafe to ship. `--revive-host`
  (`4c38d0f`) is that path: it drops a host from the ledger's `dead_hosts` registry
  (`removeDeadHost`) *and* resets only that host's parked `unreachable` rows to claimable
  (`resetUnreachableForHost`, shard-scoped, indexed), applied in `crawl.ts` *before*
  `createScheduler` re-seeds the exclusion set — so the verdict is gone before the
  re-seed, and it's selective by design (DNS/legal-dead hosts stay parked; only the named
  host is re-armed, never the blanket reset). With a clear path in hand the Bridgy
  decision became clean: `atproto.brid.gy` and `fed.brid.gy` went into every box's
  `dead_hosts`, parking the ~15–18k bridged repos per shard as `unreachable` instead of
  retrying a `getRepo` that the 16:25 probe proved will never answer. The preserved
  shard-1/2 bridge-DID lists live on the storagebox at `_meta/bridge-parked/`, with a
  one-line revive recorded for the day Bridgy ships `getRepo`. The keeper: blacklisting a
  host must ship with its inverse, or "park" silently means "abandon."
- **Incident: a verify *timer* overloaded ClickHouse and wedged crawl3 — after three
  wrong guesses.** At 18:20 a pre-existing systemd `emojistats-verify.timer` fired a full
  ledger↔ClickHouse reconcile against the *live* CH mid-crawl — hundreds of heavy `FINAL`
  digest group-bys that starved the crawler inserts into 30s socket-timeouts, leaked slots
  and wedged crawl3; CH load climbed 11→20 on the 8-core serving box. Diagnosis took three
  misses first — the post-deploy re-ramp (plausible, matched the midday load-16 transient,
  but no), cooling-on-stall (exonerated: zero stall events), a merge backlog (wrong:
  `system.merges` = 0) — before one look at `system.processes` named it instantly (348
  reconcile selects in two minutes). The fix was `systemctl stop` the timer + `pkill -9`
  the verify procs (deaf to SIGTERM, blocked in a 40s CH socket read), and CH load
  collapsed 20→0.4. Four lessons, most of them recurrences: inspect ground truth
  (`system.processes`) *before* theorizing — one query beat three hypotheses; an
  operational reality (a periodic timer) can outrank a careful new flag (`--no-reconcile`
  was built exactly to dodge this contention, and the timer ran the full reconcile
  anyway); a stopped service isn't a stopped process; and `verify` belongs strictly
  *post-drain*, never against the same single CH the crawl is hammering. The durable fix
  lives in the pix flake (the `systemctl stop` only holds until the next `nixos-rebuild`)
  and is the handoff's highest-priority infra item.
- **crawl4's two bugs — a hang and a crash in the same "box keeps restarting" costume.**
  Within one hour crawl4 destabilized twice from two unrelated causes, and telling them
  apart was the whole job. *Bug A (a hang):* a stale keepalive socket poisoned the
  loader's ClickHouse client pool; the retry reused the dead client, repos awaiting the
  flush held every concurrency slot, `fetching` pegged at 4096 while `loaded` froze — and
  the watchdog sailed past it because the box kept logging stats. Fixed (`b825b5c`) by
  classifying connection-level failures and rebuilding the client before the next retry.
  *Bug B (a crash):* `listClaimable` threw `RangeError: Maximum call stack size exceeded`
  — `rows.push(...retryRows)` spreads an array as function arguments, and in the end-game
  tail (most pending behind excluded/blacklisted hosts) the retry query returned tens of
  thousands of rows, overflowing the argument limit. Fixed (`6a06ccd`) by appending in a
  loop. Three keepers: "the box keeps restarting" is a *symptom*, not a diagnosis — a
  0-CPU hang, an exit-1 crash and an OOM are externally identical, so read the signature
  (`Result=`, `status=`, the fatal log line, `dmesg`) before reaching for a fix;
  `fn(...bigArray)` is a latent crash at scale that no small-input test catches (grep hot
  paths for spread-of-unbounded-arrays); and the watchdog's log-freshness signal — chosen
  to stop false-restarts on 404 churn — is blind to a chatty-but-stuck box, which is why
  the real health signal is *progress* (`loaded` advancing), not log liveness. That last
  one is the throughline of the whole run: the liveness detector went alert-only →
  auto-restart → CPU-gated → log-freshness → (still open) progress-gated, and nearly every
  crawler incident was a referendum on which signal tells the truth.
- **The limit of verification, named honestly — and the tooling to push it as far as it
  goes.** A clean result got pinned down (~18:00): you cannot prove set-subset from an
  O(1) digest. `verify` records a per-repo `(count, XOR-rkey-digest)` at crawl time and
  compares it to ClickHouse, but CH keeps growing from the live firehose, so "CH count ≥
  ledger" is *normal*, and once counts differ a single 64-bit XOR can't tell a dropped
  backfill row (masked by an offsetting live arrival) from a benign extra. (Filtering
  `src='backfill'` doesn't save it — `src` isn't in the `ReplacingMergeTree(did,rkey)`
  sort key, so a backfilled rkey later seen live collapses to the live row.) So "verify
  can't *prove* zero loss" is a property of digests, not evidence that loss happened — and
  the distinction matters: the write path decides loss, the checker only catches it. The
  two real closures are source-prevention (the `a31b514` loader fix plugs the only known
  drop mechanism) and re-fetch convergence, and the convergence tooling now exists
  (`1385aea`): `verify --emit-loose` writes the ambiguous LOOSE DID list, `crawl
  --did-file` re-fetches exactly those, repeat until LOOSE shrinks to the genuinely-live
  tail; an early random sample returned zero losses. The honesty caveat is carried into
  the handover too: even an exhaustive re-fetch only proves "every post *still* in the
  repo is in CH" — an upstream deletion since the crawl is invisible to both sides — so
  it's the strongest *practical* check, not a formal proof.
- **Handoff.** As of ~21:00 UTC the project passes to another agent, with `HANDOVER.md` as
  the operational baton (current state, open items in operator detail, tooling/gotchas
  reference — and an explicit "do not edit `retro.md`, it's owned separately"). State:
  crawl1 and crawl2 are retired and their boxes *deleted*; the four productive crawlers
  (crawl0/3/4/5) are in the slow end-game tail with ~2.8M repos left and an ETA measured in
  days, not hours; everything that shipped today was Codex-reviewed and deployed at
  `48de289`. The open items the next agent inherits, in priority order: remove/reschedule
  the `emojistats-verify.timer` in the pix flake (the highest-priority infra fix — a
  `systemctl stop` only holds to the next rebuild); run the post-drain verify→`--did-file`
  convergence pass for the zero-loss number; add the watchdog progress-signal (restart on
  `loaded`-flat + `fetching`-pegged, even while logging); fold the CH-client rebuild into
  the still-unfixed telemetry client; revive Bridgy if it ever ships `getRepo`; re-crawl
  the ~17% pre-widening `v:1` archive metadata; and the end-game sequence proper —
  `--final-sweep` → full verify → `v:1` re-crawl → public-site cutover. This retro stays a
  living document; the run isn't over, it has changed hands. What it has already earned, in
  one line: almost every hard hour of this backfill went not to the data but to telling a
  *stuck* system from a *slow* one — and the durable wins were the detectors and back-off
  rules that finally learned the difference.

### 2026-06-13 evening (21:14–22:25 UTC) — the takeover, and the 429 tail solved by reading the headers

The handoff's new agent (a Codex session on pix2) didn't drain the tail and call it a
night — in ~70 minutes it closed most of the open items the handoff listed, shipped eight
commits, and landed the one fix this whole run kept circling: pacing the mushroom hosts
*before* they 429 instead of backing off after. Alice steered it turn-by-turn (this was a
hands-on handoff, not an autonomous one), and three of the eight commits exist only because
she looked at the actual public page and didn't believe the numbers.

- **The headline: reactive 429 back-off → proactive rate-limit-header pacing
  (`49fe6d2`, `928c2e6`).** The end-game slowdown wasn't a dead worker — all four shards
  were fresh and active — it was the tail concentrating onto a handful of bsky "mushroom"
  PDS hosts (`jellybaby`, `morel`, `shiitake`, `oyster`…) all showing repeated 429 cooldowns
  with per-host queues pinned at depth 96. The existing back-off was AIMD: slam the host
  until it 429s, halve the cap, cool, repeat — so it only ever *learns* the limit by
  tripping it. Alice's call cut through it: "they literally give you headers about the rate
  limit." A fetch of a live mushroom response confirmed it —
  `ratelimit-limit: 3000`, `ratelimit-remaining: 2999`, `ratelimit-policy: 3000;w=300` —
  a budget the host had been advertising on *every* response and the crawler read on *none*
  of them (it parsed only `Retry-After`, i.e. only after a trip). `49fe6d2` carries the whole
  header family (`ratelimit-*`, `x-ratelimit-*` aliases, reset-as-epoch or as-delta, policy
  window) out of the parse worker on success, 429 *and* terminal responses — a 400/RepoNotFound
  still spends the same bucket — computes a per-host minimum start interval (`window / limit`),
  and has the scheduler `reserve()` a slot before each request rather than firing freely.
  The keeper is almost embarrassing in hindsight: the politest, fastest back-off was printed
  on every response from the start; reactive AIMD is what you build when you assume the server
  won't tell you its limit, and this one was telling us all along. Evidence: `host-pressure.ts`
  (`observeRateLimit`/`reserve`), `rate-limit.test.ts`, commits `49fe6d2`/`928c2e6`.
- **…and the pacing fix immediately needed its own fix (`928c2e6`).** The first deploy of
  header-pacing over-corrected: `fetching` fell to near-zero on all four shards, because jobs
  that learned a host's headers *late* were parked via `markThrottled` — pushed back to the
  ledger as throttled — instead of simply waiting for their next header-derived slot. The fix
  was to wait *inside* the per-host limiter without holding a global fetch slot or mutating
  ledger state. Post-fix the fleet recovered to ~27.9k rows/sec with ~1.82M active repos left,
  and the only in-window 429s left were a non-mushroom host. Same lesson the wedge saga taught
  in a different costume: a back-pressure mechanism that mutates durable state on every wait
  turns a transient pace into a recorded setback.
- **Exact recrawls made state-safe (`66777c5`) — the convergence path couldn't corrupt good
  rows.** The verify→`--did-file` convergence loop and the planned `v:1` metadata re-crawl both
  re-fetch repos that are *already* `loaded`/`verified`. The handoff didn't flag that a transient
  host failure during such a re-fetch would call `markRetry`/`markThrottled` and *downgrade* a
  good ledger row — turning a verified repo back into pending on a hiccup. `66777c5` threads a
  `preserveExisting` flag through the retry policy so a failed recrawl preserves the existing
  loaded/verified state (logged, not silently), and waits out host cooldowns rather than skipping;
  it also bounded `--did-file` to stream instead of scheduling the whole file at once. This is the
  prerequisite that makes the zero-loss convergence pass safe to actually run. Evidence:
  `retry.ts` (`preserveExisting` branch), commit `66777c5`.
- **The telemetry-client poison closed, and the worker pool hardened (`c81bb26`).** Handoff item
  §2.4 — the telemetry ClickHouse client that writes `backfill_progress` could keepalive-poison the
  same way the loader client did (`b825b5c` fixed the loader but not telemetry; that's why a shard's
  dashboard row could go stale on its own) — is now closed: the connection-error rebuild
  (`isConnectionError` → swap a fresh client) was ported into `CrawlTelemetry`, with the rebuilt
  clients owned and closed by the telemetry object so a rebuild can't leak past shutdown. Bundled
  in the same commit, a *proactive* parse-pool change: a worker reply-timeout used to just free the
  caller's slot and leave the (possibly half-dead) worker holding hidden jobs; now a timeout treats
  the worker as poisoned — reject its jobs, terminate, respawn. That one wasn't an observed incident,
  it was a code-review catch, but it's the same blind spot the watchdog story is about: a component
  that's stuck-but-not-dead is the dangerous state. Evidence: `telemetry.ts`, `parse-pool.ts`
  (`failWorker`), commit `c81bb26`.
- **The dashboard hot-`posts` scan fixed exactly as handed off (`d0d744c`).** The High finding I'd
  written up in `docs/issues/dashboard-live-stats-scan.md` — `getLiveStats` firing three full-table
  scans on raw `posts` (billions of rows) at ~2s cadence, competing with ingest for CH memory — was
  fixed along the recommended route: totals/rates/freshness now read the `posts_hourly` SummingMergeTree
  (current-hour sums for rates, `max(hour)` for freshness) and never touch raw `posts`; `verify`'s
  raw-`posts` orphan scan moved behind an opt-in `--orphans`. Satisfying confirmation that a handoff
  issue-doc written for "an agent to fix" was directly actionable. Evidence: `dashboard/src/server/stats.ts`,
  commit `d0d744c`.
- **Three display bugs Alice caught by not trusting the page — the dashboard was lying in the
  end-game (`7f1572b`, `4104389`, `13b468d`).** The data was sound throughout; the *presentation*
  had three independent bugs, each found by Alice reading the live site. (1) The public footer
  labeled emoji *occurrences* as "Emojis" but computed "Ratio" as posts-with-emojis ÷ posts-*without*-emojis
  — a number (~22.6%) that matched neither visible figure; `7f1572b` makes it total posts / emoji posts /
  emoji-post share = `postsWithEmojis / processedPosts` (18.41% on 2.29B posts), keeping the language tabs
  as raw occurrence counts on purpose. (2) The hero "Crawl progress" read **66%** when the run was actually
  at **98.5%** of enumerated repos — `resolved = total − pending − fetching − unreachable` put ~31M parked
  `unreachable` repos in the denominator but not the numerator; `4104389` counts parked-unreachable as done
  for the hero fraction (they're out of active budget) while still listing them separately for the sweep.
  (3) The fleet badge read "idle" while crawling, because activity/freshness used the *stalest* shard — and
  the stalest shards were `crawl1`/`crawl2`, whose telemetry is frozen because Alice **deleted those Hetzner
  boxes** (ledgers backed up for verify/recrawl); `13b468d` uses the *freshest* reporting shard for liveness
  while still warning on frozen ones. The throughline: in the end-game the dashboard's own accounting became
  a source of false alarms — a 66% that looked like a stall and an "idle" that looked like death were both
  artifacts of how parked/deleted shards were counted, not the crawl. The same "is it stuck or just
  slow / done?" question, now asked of the instrument instead of the system. `4104389` also fixed a real
  scheduler bug surfaced in the same review: `nextWake()` ignored rate-limit wakeups and a repo could wait
  on a delay created by *its own* reservation.
- **Status at session end (~22:25 UTC):** the crawl is still draining — ~1.82M active repos, ~27.9k
  rows/sec, **98.5%** of the 95.47M enumerated repos resolved; `crawl0/3/4/5` live, `crawl1`/`crawl2` boxes
  deleted with ledgers preserved on the storagebox. Still *not* run: the post-drain `--final-sweep`, the
  verify→`--did-file` convergence pass, the `v:1` metadata re-crawl, and the public-site cutover — the
  same end-game sequence the handoff named, now with its prerequisites (state-safe recrawl, paced hosts,
  honest dashboard) in place. The process note writes itself: this hour is the cleanest example in the whole
  run of the division of labor — the agent did the mechanism (header parsing, client rebuild, scheduler
  reservation) fast and correctly, and the human supplied the three things it kept missing on its own:
  the *insight* ("read the headers"), the *distrust* (looking at the real page and disbelieving 66% / "idle"),
  and the *ground truth* the agent couldn't see from the logs (shards 1 and 2 aren't slow, they're gone).

### 2026-06-13 late evening (22:25–23:02 UTC) — arming the end-game (and a final-sweep that would never exit)

The hour after the takeover was spent *arming* the end-game sequence rather than running it —
the backfill is still draining, so this is all preparation to fire the moment it does. Four
commits (`1b72628`, `bc20465`, `8abf22e`, `411ea08`), and the most important one is a bug the
agent found by reading the `--final-sweep` code instead of trusting the runbook that described it.

- **The recrawl "sleeper": launch the moment the backfill drains, not when a human notices
  (`8abf22e`, `bc20465`, `1b72628`).** The backfill ends when each ledger's finite enumeration
  has no `pending`/`fetching` rows left to claim — an event no one wants to babysit at 4am. The
  agent built `wait-for-backfill-drain.ts` (a JS waiter that reuses the crawler's *own* ClickHouse
  client and secret-env, explicitly to avoid "brittle shell SQL"), so a oneshot can block on drain
  and then start exactly one recrawl worker. `bc20465` adds **ledgerless recrawl shards** — the
  mechanism that lets the *deleted* crawl1/crawl2 be re-crawled from their backed-up DID lists with
  no live ledger to claim against — and `1b72628` adds `prepare-v1-recrawl.ts` to build the `v:1`
  pre-widening metadata re-crawl list. The keeper: the end-game of a multi-day batch job is a
  *scheduling* problem as much as a data one — the cheapest way to not waste the hours after drain
  is to make the next phase trigger on the completion event, not on someone watching a dashboard.
- **A load-bearing process correction from Alice: "we run nixos, all changes go through the pix
  repo."** The agent's first instinct was to arm the sleeper with ad-hoc `systemd-run` transient
  units directly on the boxes (NixOS keeps `/etc/systemd/system` read-only, so transient units are
  the obvious dodge). Alice stopped it cold — "we run nixos all changes go through the pix repo" —
  and the agent reversed: killed the ad-hoc units, removed the host-local scripts, moved the launcher
  into the pix flake. This is the *exact* recurrence the run keeps producing — the verify-timer that
  only a rebuild could truly remove, the runtime drop-ins that lived only in `/run` — now as a
  near-miss caught before it could rot: anything armed outside the flake is a landmine for the next
  `nixos-rebuild`, and the discipline is to refuse the convenient out-of-band fix even at 11pm.
- **`411ea08` — `--final-sweep` would have run forever, and the runbook didn't say so.** The handoff's
  end-game sequence *starts* with `--final-sweep`. Checking the actual implementation (not the runbook
  prose) the agent found it resets **all** `unreachable` rows back to claimable — including the
  dead-host registry rows (Bridgy, DNS-dead, legal). The scheduler still excludes those hosts, so the
  idle policy sees a permanent population of in-budget-but-unclaimable rows and the sweep *never
  reaches idle and never exits*. The fix is narrow: pass `dead_hosts` into the reset so registry rows
  stay parked out-of-budget and the sweep rearms only hosts the scheduler will actually claim;
  `--revive-host` stays the one explicit path to un-park a recovered dead host. A regression test pins
  the exact failure (one dead-host parked row + one normal parked row → only the normal one becomes
  claimable). Two keepers: a command's *termination* is part of its contract — "resets unreachable
  rows" hid an infinite loop in the interaction with the exclusion set; and the blacklist-needs-its-
  inverse rule from the afternoon (`--revive-host`) had a second edge no one had checked — the
  *sweep* could silently undo the registry too. Evidence: `ledger.ts`/`scheduler.ts` host-aware reset,
  `host-health.test.ts`, commit `411ea08`.
- **Two reconciliations with Alice, both ending in honesty.** She caught the dashboard ETA reading
  ~1.4h against the agent's "2–4h" — the agent confirmed the page is terminal-repo *drain* velocity
  (right for the active tail) and its own number was a padded ops estimate, not a contradiction. And
  she pinned the data-completeness question — backfill (historical repo snapshots) and Jetstream (live
  posts) only "meet" as *all data* if live ingest stayed up **and** a final PLC/listRepos catch-up
  picks up accounts discovered after enumeration began; the agreed order is drain → final-sweep →
  recrawl/catch-up. As of 23:02 UTC the session went quiet with the end-game armed but not yet fired:
  no `--final-sweep`, verify convergence, `v:1` recrawl or cutover has run.

### 2026-06-13 night (23:02–23:35 UTC) — the sweep and recrawl, deployed cold and waiting for drain

Still draining (active backlog fell from ~914k to ~840k repos across the four live crawlers in
this window; Alice's read: "done in 1-2-3 hours"), so this was the last of the prep — the
end-game units are now built, deployed, and sitting *inactive* on the boxes, armed to run the
moment the backfill finishes. One dashboard commit (`263bc7c`) and a Pix commit carry it.

- **The follow-through on "everything goes through the pix repo."** Last cycle Alice reversed an
  ad-hoc `systemd-run` launcher; this cycle the agent did it properly — two **manual** NixOS units
  in the flake (a final-sweep service on every crawl box, a `v:1`-recrawl service only on the four
  live crawlers with worker indexes 0–3), defined to *not* auto-start, committed to Pix and rolled
  out with a direct `nixos-rebuild switch` per host. Deploying them only *adds* dormant units; the
  running crawl service's rendered environment is unchanged, so arming the end-game can't perturb
  the drain it's waiting on. A duplicate `systemd.services` attribute caught in Nix eval before
  commit, and the per-host worker TSVs (~50 MB each) copied and checksum-verified ahead of time. This
  is the clean version of the lesson the run kept relearning: the next phase lives in the flake as a
  deliberate, inert artifact — not a host-local script that the next rebuild would silently erase.
- **A deploy gotcha worth its own line: pulling source isn't deploying.** The dashboard host-table/
  recrawl-status fix (`263bc7c`) didn't take after a source pull + restart — the service runs the
  built `dist/server/server.js`, so the change only landed after an explicit *build* on the serving
  box, then a restart. The same build-artifact-vs-source trap that bites every "I pulled, why is it
  still old" moment; the tell was the restarted service still serving the old table.
- **Verification scope, settled by reading the notes not vibing it.** Alice asked whether the edge-case
  bug means re-verifying "the whole shebang" or only part — the agent's answer (after checking the
  verification notes): a *full verification pass* is needed, but *not* a full re-fetch of everything.
  That's the shape the convergence tooling was built for — verify everything, re-fetch only the LOOSE
  tail it can't account for.
- **Pre-drain housekeeping: where the ledgers live, and what the disk is actually holding.** Mapped
  for the verify pass: live ledgers on the four crawlers, the *retired* shard1/2 ledgers preserved on
  the serving box (the only copy now that those boxes are deleted), and the `v:1` worker files staged.
  The agent deliberately stopped running heavy full-`sqlite` scans over the 34 GB ledgers during the
  active crawl (not worth the I/O contention) and used the ClickHouse progress table as the live
  source instead — sqlite reads deferred to post-drain. A disk check put numbers on the teardown
  hindsight: ClickHouse is ~57 GB (under the earlier ~75 GB guess), and the ~68 GB hogging
  `packages/backfill` on the serving box is the retired shard1/2 ledgers — kept untouched until the
  verify pass has consumed them, then reclaimable. As of 23:35 UTC nothing in the end-game sequence
  has *run*: final-sweep, verify convergence, `v:1` recrawl and cutover are all staged and cold,
  waiting on the drain.

### 2026-06-13/14 the drain tail (23:36–00:03 UTC) — and the O(n) claim-scan villain comes back for the last mile

Activation finished cleanly (all four hosts `nixos-rebuild switch`ed, current crawl stayed up, the
final-sweep and `v:1`-recrawl units installed *inactive* and each pointed at its own worker
TSV/ledger/archive dir), and the agent settled into a 5-minute drain watch. The tail is the slow
part, exactly as feared:

- **The drain, sampled honestly:** 23:36 ~826k pending / ~9,760 repos/min → 23:42 ~798k / 9,394 →
  23:47 ~770k / 8,482 → 23:52 ~749k / 8,064 → 23:57 ~730k / 7,490 → 00:02 ~713k / 6,914. A clean,
  honest taper — throughput sliding as the easy repos run out and the residue concentrates on
  cooling/shallow host queues; the live-host ETA drifted *up* from ~85 to ~103 minutes across the
  half hour as the rate fell, which is the correct direction for a tail and the opposite of the
  optimistic flat-rate projection. Retired shard1/2 stale rows are excluded from the count.
- **shard0 is the drag — and the reason is the run's oldest villain.** With lots of pending but
  almost no fetch slots in use, shard0 looked stuck; it wasn't. The logs showed `skipped` climbing
  *into the billions* with `topHosts` queue depth mostly 1–5 — the claim loop scanning enormous
  numbers of ineligible rows (parked behind cooling/dead hosts) to surface the few currently-claimable
  ones, so fetch slots bounce low not from deadness but from a claim path that's gone O(n) over a
  pending set that's now mostly un-claimable. This is the *same* family as the listClaimable cost
  and the bulk-park scans from earlier in the run — the claim scan is cheap while most rows are
  eligible and quadratic-feeling once the tail inverts that ratio. The agent's first move was the
  right one: check for an existing scan-depth/limit env knob before touching code, since the tail is
  the worst time to ship a risky change. (Open as of 00:03 UTC — the session went quiet mid-diagnosis;
  no fix committed, drain continuing toward an estimated ~01:40 UTC finish. The keeper, pending the
  resolution: an end-game is where every "fine while n is small / fine while the ratio holds" cost
  you let slide comes due at once.)

### 2026-06-14 the overnight tail-fight (00:03–08:52 UTC) — the last 1% took nine hours, and then the backfill drained to zero

This is the climax of the white whale: the enumeration is *done*, every active shard hit **zero
pending at ~08:51 UTC**. But the last ~700k repos — call it the final 1% — took nine hours and four
scheduler commits to clear, and the reason is the most instructive bug of the whole run: the
rate-pacing we built to survive the 429 storm was being *misread* by the claim loop as host
unavailability, so the scheduler starved itself exactly when the work got scarce. A fix in one
subsystem had planted a latent bug in another, and it only detonated in the tail.

- **The shard0 skip-scan, resolved (`c5d15cd`, 00:35Z) — necessary, not sufficient.** The billion-row
  skip-scans were a real defect: when a claim pass scheduled a tiny batch but *retained* 150k+
  busy/cooling rows, the next wake re-walked that retained array instead of forcing a fresh scan with
  current host exclusions. `c5d15cd` drops the retained backlog (`shouldDropRetainedBacklog`) when a
  pass schedules few but parks many, forcing a time-floored fresh scan. It cut wasted event-loop work
  but did *not* restore drain rate — throughput kept falling (2,130→887/min by 00:52), because the
  bottleneck had already moved downstream to pacing.
- **The claim loop was blocking on the wrong event (`c8a3f57`, 00:55Z).** Second defect: after a
  rate-limited host reserved a request, the loop did `await Promise.race(active)` — it waited for a
  *fetch to complete* before re-entering. With only a handful of slow downloads in flight, it kept
  missing the rate-limit windows of hosts that would happily allow 10 starts/sec. The fix wakes the
  loop at the next rate-limit start time even while long fetches run. It produced a dramatic burst
  (in-flight 28→939 in one wake) but not sustained throughput — the queues were genuinely shallow and
  the skip scans still high.
- **The discipline that mattered most at 2am: the agent refused to improvise the real fix.** It
  correctly diagnosed the remaining need — "a host-focused, rate-limited crawl mode that honors
  rate-limit reservations while bypassing generic claim scans" — and just as correctly declined to
  build it live overnight: "I'm not going to improvise an exact-mode sidecar tonight." So through the
  small hours the crawl ground at ~400–800/min (a real tail ETA of ~14–15h), and the only big drops
  came from `listrepos-diff` *classification* — bulk-marking PLC-only spam DIDs (jellybaby, morel,
  stropharia, ~850k rows at a time) as resolved without fetching CARs, a tail accelerator that deletes
  work rather than doing it. Honoring Alice's standing overnight order ("if you have nothing to do,
  sleep 15 minutes then check… if the backfill is complete, start the final sweep"), it never touched
  the risky path and never started the sweep on an incomplete drain.
- **Alice woke up and authorized the surgery (07:36Z) — and that's what ended it.** Her message —
  "we made progress but the backfill is somehow still not done… this last 1% will take forever, there
  must be a way to finish this" — is the load-bearing course-correction. With a human awake to own the
  risk, the two deferred fixes shipped within thirteen minutes:
  - **`75e4d21` (07:37Z) keep rate-paced hosts claimable.** The core bug, named at last: a host under
    short *header-pacing* (e.g. a 250ms inter-request floor) was being excluded from the SQL claim scan
    as if it were *down*. On shard4, down to ~3 live hosts, that meant the scanner kept concluding
    "nothing claimable," ran starved micro-bursts, and idled while a perfectly healthy host sat one
    pace-tick away. The fix splits the two questions cleanly: the claim-scan exclusion set uses only
    *true* backoff/dead/full hosts, while per-request pacing is applied later, right before the fetch.
    A regression test pins the exact case. shard4: ~72 → ~128/min on the first sample.
  - **`adc3d5a` (07:49Z) pace requests inside host queues — the decisive one.** The remaining throttle
    was claim-time *reservation* holding fleet-wide fetch slots while a paced row waited. Moving the
    header-pacing wait *inside* each per-host queue (before the global slot) let paced rows wait
    politely without occupying capacity the rest of the fleet could use. Result at 07:54: shard4
    ~1,549/min, fleet ~7,732/min — roughly a 10× recovery from the overnight grind.
- **Then it fell off a cliff, the good way.** Post-fix the tail collapsed on schedule: 08:05 ~146k →
  08:21 ~75k → 08:40 ~18k → 08:47 ~4k → **08:51 zero pending on all four shards** (crawl0 drained by
  08:31, crawl3 by 08:34, shard4/shard5 the last gate). A brief 08:14 slowdown to ~4,500/min was
  correctly read as *normal* tail capacity collapse — crawl0 down to one remaining host, others to
  two or three, bounded by per-host budgets — not a regression. As of the last line (08:52Z) the agent
  is "waiting one more minute for shard4's last in-flight rows" before stopping the crawl services and
  starting the final sweep.
- **Three keepers from the night.** (1) *The cross-subsystem latent bug:* the rate-pacing built to fix
  the 429 storm was semantically conflated with host-deadness in the claim scan — "this host needs a
  250ms pause" and "this host is unreachable" took the same exclusion path, invisible until the host
  pool shrank to where the difference was the whole game. The cure was making the distinction explicit
  (claimable vs. should-wait are different questions). (2) *Defer risky fixes to when a human owns the
  risk:* the agent grinding at a 14h ETA rather than improvising scheduler surgery at 2am was the right
  call, and Alice waking to authorize it was the right unblock — the ETA went from ~14h to ~75min once
  the fix shipped. (3) *Tail accelerators that delete work beat tail accelerators that do work faster:*
  `listrepos-diff` classification cleared more of the backlog overnight than crawling did.
- **A near-miss worth recording: broad-pattern `pkill` kept catching its own SSH wrapper.** At least
  three times a kill-by-pattern (`healthcheck`, a sqlite probe, the watch loop) matched the agent's own
  `ssh … pkill …` command or its parent, nearly killing the monitor instead of the target. Each time it
  was caught and verified before harm. The lesson is old and keeps being true: a `pkill -f <substring>`
  on a shared box is a foot-gun because your own tooling contains the substring; match on the unit or
  the full argv, not a fragment. And one reporting confusion Alice caught at 08:42 — the dashboard
  "repo breakdown" shows ~44k remaining because it sums the *frozen deleted* shard1/2 telemetry (~30k)
  on top of the live ~14.8k Bridgy/parked tail, while the active-drain query counts only live shards;
  not a bug, but a number that lies if you don't know which buckets it mixes.

The backfill is, after all of it, **drained** — the thing the November-2025 attempt never reached and
the thing this whole document was written around. What's *not* done: the final sweep (about to fire),
the verify→`--did-file` convergence pass, the `v:1` metadata recrawl, the Bridgy/`--revive-host` tail,
and the public-site cutover. The white whale is alongside; it isn't on the deck yet.

### 2026-06-14 the morning after the drain (08:52–13:29 UTC) — the final sweep ran clean, and then we found out we'd been losing the biggest repos all along

The end-game finally fired — and the first thing it surfaced was the worst bug of the entire run:
for the whole backfill we had been *silently dropping every repo bigger than 1 GiB*. Not corrupting
it, not partially loading it — dropping the entire repo, all its posts, and filing it under a status
that looked like a deliberate safety decision. The final sweep ran cleanly; the verify pass never
produced a number; and Alice halted the whole thing at 13:07 to stop and rebuild her mental model.
Eight commits, one genuine data-loss recovery, and zero progress toward cutover.

- **The final sweep ran and exited cleanly — after the dead-host/idle bug bit a third time.** At
  08:53 the sweep fired on all four live crawlers (drain confirmed: zero pending/fetching/in-flight),
  rearmed the retry rows, and drained to a hard tail. But it wouldn't *exit*: dead-host rows with
  `attempts < 5` were counted by the idle-wait check even though the claim scan excludes them — so the
  sweep saw "work remaining" forever. This is the **same dead-host-vs-idle interaction** that
  `411ea08` and the cooling-on-stall saga were supposed to have closed; it had one more edge. The fix
  (`5154411`, "ignore dead hosts when waiting idle", with a regression test) plus live ledger parking
  of the stragglers — ~26.8k loopback rows, a host 83 hours out on a `retry-after`, ~2,236
  rate-limit-bound `atp.referendumapp.com` rows — was hot-deployed *without restarting the running
  sweep*, and all four units deactivated cleanly by 09:30. The throughline holds: the single hardest
  thing in this system remained telling "parked and never coming back" from "busy and will retry," and
  the sweep's termination contract was the last place that distinction hadn't been nailed.
- **The data loss: 16 repos over 1 GiB, every post missing, for the entire run.** While narrating the
  sweep tail the agent mentioned a straggler near the `CAR_MAX_BYTES` cap. Alice, just awake, caught it
  cold: *"are you telling me the whole backfill we ignored car files over 1gb"* → *"please tell me we
  are not also missing posts."* We were. A repo whose CAR exceeds 1 GiB threw `QuarantineError`, landed
  as `status='quarantined'`, and because the pipeline parses the *whole* CAR before it writes anything
  to ClickHouse or the archive, an over-cap repo was **wholly** missing — not truncated, gone. Sixteen
  of them across all six shards (including three each on the *deleted* shard1/2). The status was
  "visible, not silent" in the ledger — but nobody was reading that row, so visible-in-principle was
  invisible-in-practice. The lesson is the sharpest in the document: **a cap you set for safety is a
  data-loss boundary unless something actively re-examines what it rejected.** "Quarantined" sounded
  like a decision; it was a hole.
- **Recovery, and the second cap hiding behind the first.** Raising `CAR_MAX_BYTES` to 8 GiB recovered
  some and immediately exposed the *next* limit — the 300 s absolute fetch timeout, which killed
  multi-GB downloads at 0.85–4.43 GB. Only with a 30-minute fetch timeout *and* a 32–34 GiB cap did
  they come back. The deleted shard1/2 ledgers were pulled from the storagebox (~73 GB), queried,
  recrawled with `LEDGER_DB_PATH` aimed at the copies, checkpointed and shipped back. Final tally:
  16 → 12 loaded, 2 empty, 2 still unrecoverable (genuinely malformed CARs) — **23,227,732 posts
  recovered from 23.65 GiB** of CAR data, including a single 4.06-million-post repo. `f309ce2` raises
  the default cap to 64 GiB (`0` disables it) and clears the stale timeout `error` text when a repo
  later resolves `empty`. The recursive shape is its own keeper: *raising one cap reveals the next* —
  the fetch timeout had been a no-op safety margin until the size cap moved and made it load-bearing.
- **Alice's standing instinct, vindicated again: audit for the pattern, not just the instance.** She
  ordered (*"after all this is done"*) a sweep for other baked-in limits and a full quarantine
  breakdown. The quarantine audit came back **16,948 rows, now entirely malformed/structural CAR
  errors** (decode-remainder, varint EOF, missing MST node…) with *zero* `CAR_MAX_BYTES` left — i.e.
  the only size-based loss is closed. The limits audit found the parser scan-buffer cap is fail-loud
  only (it disables a passive optimization, never drops posts) — no other silent data caps. This is
  the "fix the cause, not the symptom" rule paying off: the instance was 16 repos; the pattern-hunt
  proved there wasn't a second class of silent loss hiding behind a different constant.
- **Verify was attempted three times and never produced a number — the headline that didn't happen.**
  The zero-loss number this whole document keeps pointing at *still does not exist*. Three canary runs
  failed in a row: too slow at 1k-DID ClickHouse chunks; then the CH HTTP parameter-length limit at
  10k; then `max_query_size` with the DID list inlined into SQL. Each produced a real fix —
  `131d9e8` (a `backfill_verify_progress` table + dashboard card so a multi-hour verify is observable),
  `bb16852` (configurable chunk size), `82f6bec` (move the DID list into the POST *body* instead of
  HTTP params) — but no shard completed. At 13:07 Alice called it: *"lets stop i am too confused about
  things and we need to stop and clarify."* The honest status the agent gave: *"no useful verification
  result has been produced yet."* The right next step (a temp ClickHouse DID table joined per shard
  instead of shipping DID lists at all) is identified but unbuilt. So: drained, mostly de-lossed, and
  still entirely *unverified*.
- **The dashboard was lying about the finish line, in Alice's actual source of truth.** She pinned it:
  *"my source of truth is [the public dashboard]… it can only be one of these"* (the page is wrong or
  your numbers are). It was the page: the overview was scoped to the latest `run_id` — which was now
  the *over-cap recovery run* that only touched five shards — so shard5 silently dropped out of the
  hero totals. `820b3a2` aggregates the freshest snapshot *per logical shard* (six), `942f99d` redefines
  the hero so `loaded` includes `verified` (Alice's model: *"loaded is every repo we fetched that had
  post rows, verified is how much of that we verified, and at the very end loaded will equal verified"*),
  and `ee9e600` clarifies per-shard freshness. Browser-verified at 13:29, the corrected public numbers:
  **95,440,579 of 95,470,241 repos resolved across 6 shards, 30,361,263 parked unreachable,
  2,593,792,562 posts, 7.8 TiB**. The recurring end-game lesson, restated: when the instrument and the
  system disagree, the instrument's accounting (which `run_id`, which buckets, which shard speaks) is
  as likely to be the bug as the system — and in a hand-off-heavy end-game it usually is.
- **Status at 13:29 UTC — paused at Alice's request, mid-end-game, NOT done.** What's now true: the
  final sweep completed cleanly; the 1 GiB data-loss hole was found and 12/16 repos (~23.2M posts)
  recovered; the dashboard tells the truth again at ~2.59B posts / 7.8 TiB / 95.44M of 95.47M resolved.
  What's still ahead, all of it: a verify pass that actually *runs* (the scaling rework is unbuilt), the
  `v:1` metadata recrawl, the ledgerless `--revive-host` Bridgy recrawl of shard1/2, baking the
  verify-timer fix into the pix flake, and the public-site cutover. The drain was the milestone; this
  morning was the reminder that "drained" and "complete and verified" are separated by exactly the kind
  of bug — a silent cap, found by accident, on the largest and most data-rich repos in the network —
  that the rest of the run had taught us to expect and still didn't see coming.

### 2026-06-14 early afternoon (13:29–14:07 UTC) — making the dashboard tell the whole truth, and naming what the "unresolved" millions actually are

Verify still hasn't run — this window went to the question that has to be answered *before* verify
means anything: do the numbers on the page even mean what they say? Alice drove it
(*"the big one: are all these numbers correct? if yes, explain them"*), and the useful output isn't a
commit, it's an accounting of what the ~30M+ non-`loaded` repos actually are.

- **The dashboard was still showing only the tail.** The history charts were scoped to the latest
  `run_id` — which after the over-cap recovery run meant they displayed a sliver, not the project. The
  agent moved the timeline off `backfill_progress` (per-run) and onto `backfill_repo_events` (the
  append-only project history), so the chart now spans the real lifetime, `2026-06-12 00:09` →
  `2026-06-14 11:30`, with day-stamped x-labels (`b0768b2`); a verify-progress card got tie-broken
  timestamps and a "failed canary" label (`9a7120f`, `0c5f577`). Same recurring end-game lesson in a
  third costume: an instrument scoped to "the current run" lies about a multi-run project.
- **What the un-loaded buckets actually are — the number that reframes "98.5%".** Checking the status
  counts against the authoritative SQLite ledgers (active shard0/3/4/5 + the retired shard1/2 copies on
  the serving box, shard-filtered by `bucket % 6`) and grouping the error text into reason classes gave
  the first real anatomy of the ~30M `unreachable` + the `failed` and `quarantined` rows: `unreachable`
  is overwhelmingly **host-dead parking** (a PDS that stopped answering, not a dropped repo);
  `quarantined` is **almost entirely malformed-CAR decode errors** (16.9k, the structural-corruption
  tail from the data-loss entry above); and `failed` is overwhelmingly **listRepos returning DIDs that
  PLC knows about but the host does not actually serve** — i.e. the PLC directory and the PDS disagree
  about which repos exist. The reframing matters for the eventual honesty of the headline: a large part
  of "not captured" is not *lost data*, it's the network telling the truth about itself — dead hosts and
  directory/host disagreements that no amount of re-crawling can turn into posts. (Oldest post in the
  store cross-checked three ways — `posts_hourly` min-hour, raw `posts FINAL`, and part metadata — all
  agree: `2022-11-16`, the network's early days.)
- **The reason breakdown is being built data-backed, not hardcoded.** Alice's rule held — *"the numbers
  not hardcoded in the html but the data coming from wherever it should"* — so rather than bake the
  categorized counts into the page, the agent is adding a `backfill_status_reason_counts` ClickHouse
  rollup fed by a `backfill status-reasons` script that scans the ledgers, with a dashboard card reading
  the latest complete six-shard rollup. The authoritative source for *why* a repo didn't load is the
  per-repo ledger reason text, not the event stream (which only carries emitted terminal events, not the
  bulk parking states) — a distinction the agent flagged rather than papering over. As of 14:07 UTC the
  rollup table + script + pane are mid-build; verify, the `v:1` recrawl, the Bridgy revive, and cutover
  are all still ahead, exactly where the 13:07 halt left them.

### 2026-06-14 mid-afternoon (14:07–14:37 UTC) — the reason pane lands, the app turns 1.0.0, and the dashboard lies a fourth way

The status-reason pane shipped (`fca8ff1`: a `backfill_status_reason_counts` rollup + a
`status-reasons` ledger-scan script + the dashboard card), closing the data-backed breakdown
the previous entry left mid-build. The frontend was bumped to **1.0.0** (`8e0ebed`, deployed
and verified live) — a release marker, not the data cutover, which still gates on a verify that
hasn't run. And then Alice opened a page she'd *"barely looked at"* and it produced three more
correctness bugs — the same instrument-lies-confidently family, now its fourth distinct costume.

- **The public backfill page was wrong three ways at once, all from windowing/column mistakes.**
  Alice caught the tells herself: *"posts/s 1m and posts/s 15m is suspiciously the same number
  and the last 24 hours part shows more than 24."* Reproduced and root-caused (`caef707`): the two
  rate cards were hour-average math that algebraically *cancels to the same value*; the "last 24h"
  chart had **no upper time bound**, so future-dated post `created_at` timestamps leaked in and
  plotted well past 24 hours; freshness was computed off `created_at` buckets while the label said
  `ingested_at` (hence a *negative* freshness); and the root-page badges still showed `loaded`
  *excluding* `verified`, inconsistent with the detail page's "every fetched repo with post rows."
  Fixes: a live-only rate from raw `ingested_at` (live is ~45–50 posts/s; the backfill source has no
  recent rows), a bounded 24h window with day labels, and unified `loaded` semantics — backed by a
  new live-rate aggregate seeded historically so the page is correct immediately, not after a
  15-minute warmup.
- **The keeper, said plainly because it keeps recurring:** across this end-game the dashboard has now
  been wrong about progress (run_id-scoped hero dropping a shard), about liveness ("idle" off the
  stalest deleted shard), about its own history (run-scoped charts), and now about rates and time
  windows (averages that cancel, an unbounded window admitting future timestamps, the wrong
  timestamp column). Every one was the *instrument*, not the system. The transferable rule is
  getting sharp: a live dashboard needs **bounded windows, the right timestamp column named
  honestly, and one definition of each metric shared across every view** — without those it doesn't
  just lose precision, it states false things confidently, and in an end-game that reads as a crisis
  that isn't there. (A genuine data quirk fell out of it too: the network contains **future-dated
  post timestamps** — clients and bridges stamping `created_at` ahead of now — which any unbounded
  "recent" window will admit.)
- **Status unchanged where it matters:** verify still has not run, no `v:1` recrawl, no Bridgy
  `--revive-host` recrawl, no data cutover. What shipped this window is the public face — an honest,
  1.0.0-tagged dashboard — not the verified completion behind it.

### 2026-06-14 mid-afternoon (14:37–15:13 UTC) — the verify blocker breaks: a set-based verifier, reviewed by the other agent before it runs

After two days of verify being the wall the end-game couldn't get past — three canary attempts
killed by ClickHouse parameter and query-size limits on inlined DID lists — the verifier got the
rewrite it needed (`96ba6b1`, ~600 lines of `verify.ts` replaced). The shape is the one the
13:07 halt had already identified: instead of shipping DID lists *to* ClickHouse as `IN (...)`
strings, **stage the ledger's per-repo expectations into a ClickHouse table and classify the whole
shard with one joined aggregation**, materialized once so the follow-up counts and LOOSE emission
are cheap rather than re-running the expensive `posts FINAL` pass. The old chunked path survives
only for small sample/orphan lookups. A live end-to-end canary — a one-row temp ledger run through
the actual script on the serving box — passed clean: the repo promoted to `verified` with
`exact=1, loose=0, failed=0`. The machinery that produces the zero-loss number finally exists and
is deployed fleet-wide.

- **The keeper that the meta-retro just predicted, demonstrated in real time: the adversarial review
  is load-bearing — *especially on the checker itself*.** Alice's move was the right one: before
  trusting the new verifier, she had Codex ask Claude (Opus 4.8, xhigh effort) to review the diff.
  Claude found **three real issues** — a digest-padding bug, an over-broad promotion scope, and a
  query path that could still trigger full-table `FINAL` scans — all fixed before a single shard was
  verified. This matters more than an ordinary review: a *verifier* is the instrument that will
  certify the whole backfill as complete, so a bug in it doesn't corrupt data, it manufactures false
  confidence — the most expensive kind. Catching the checker's bugs *before* it runs is the
  difference between "verified" meaning something and "verified" being a second silent cap. The
  second-agent pattern earned its place in the playbook again, on the highest-stakes code in the run.
- **Honest status: the verifier is ready, the verdict is not.** Everything so far is canaries —
  one staged row, `exact=1`. The fleet-wide pass over ~95M repos, the real per-shard
  EXACT/LOOSE/FAIL counts, the `--did-file` convergence on whatever comes back LOOSE — none of that
  has run yet. Still also ahead: the `v:1` metadata recrawl, the ledgerless `--revive-host` Bridgy
  recrawl of the deleted shard1/2, baking the verify-timer fix into the flake, and the data cutover.
  But this is the first cycle in two days where the thing blocking all of it stopped being blocked.

### 2026-06-14 mid-afternoon (15:13–15:46 UTC) — the verdict lands: the zero-loss number, and it's clean

The number this whole document has been pointing at finally exists. With the scalable verifier
deployed, the fleet-wide pass actually ran — sequentially, one shard at a time — and the first two
shards came back overwhelmingly **exact**:

- **shard0:** `2,231,041 exact · 102,271 loose · 3 mismatches` — promoted 2,199,790 loaded repos to
  `verified`, left the 3 mismatches `loaded` for recrawl.
- **shard3:** `2,233,389 exact · 100,670 loose · 2 failures` — promoted 1,825,495 more to `verified`.

That is ~4.46M repos checked with **five genuine hard mismatches between them** — real count-short
rows, kept `loaded` for re-fetch, not waved through. The ~100k-per-shard `loose` band is the expected
shape, not loss: it's repos where ClickHouse holds *more* than the ledger receipt because the live
Jetstream firehose kept writing after the backfill snapshot — ambiguous by construction (the O(1)
digest can't distinguish a benign live arrival from a masked drop), which is exactly why the
convergence pass re-fetches them rather than trusting the count. After two days of "verify can't even
run," the first real verdict is: near-total exact match, a handful of genuine discrepancies, and a
known-ambiguous tail to converge — about as clean as this could land.

- **Running the verifier at scale surfaced the verifier's *own* workflow bugs — three, all fixed
  live (`3ec3edd`, `ff7cb17`).** (1) The promotion gate was too coarse: 3 bad repos blocked promoting
  the other ~2.33M — fixed to promote the good loaded repos and leave *only* the mismatches unverified,
  collecting the **full** mismatch DID set (not just the sample logged to stdout — a subtle edge the
  agent caught in self-review). (2) After classification the run hung burning CPU on a full SQLite
  terminal report before writing telemetry — made opt-in (`--terminal-report`). (3) A verifier-specific
  ClickHouse timeout + 8 GiB spill threshold so the scan doesn't inherit crawler defaults. The pattern
  holds: you only find the operational bugs in the thing that runs for minutes over billions of rows by
  *running* it — the canary proved the math, the real shard proved the workflow.
- **The bottleneck moved, instructively, from the verification to the bookkeeping.** The ClickHouse
  classification is fast — ~73s to scan ~2.31B rows at ~4–5 GiB, spill-free — but promoting ~2.2M rows
  in the per-shard SQLite ledger (updating status and maintaining several status indexes) became the
  long leg, ~4 minutes end-to-end per shard. So verify runs **sequentially** by necessity: each shard's
  CH scan is the shared bottleneck on the single serving box (five-wide would fight for CPU/IO and push
  toward the ~15 GiB memory ceiling), and the SQLite promotion is local per host. A reminder that at
  this scale the "expensive" step isn't always the one you designed for — here the digest math got cheap
  and the status-update bookkeeping got dear.
- **And the dashboard learned to show the *fleet*, not the last run.** Alice again read the page and
  flagged it (*"i want the backfill status page to show the combined results of all verifications not
  just the latest one"*; *"this is what i see… seems weird"*) — the "run shards 0 / 1" label was
  per-`run_id` and read as wrong while shard3 sat in `promoting` with `done=0`. The fix aggregates the
  latest meaningful row *per shard* across runs (excluding zero-progress failed canaries) into a
  fleet-wide "verified shards" rollup — the same instrument-scoping lesson, now applied to verification
  coverage. Still ahead: shards 4 and 5, the `--did-file` convergence on the LOOSE+mismatch tail, the
  `v:1` recrawl, the Bridgy revive, and cutover — but the verdict is no longer hypothetical.

### 2026-06-14 late afternoon (15:46–16:10 UTC) — all four live shards verified: eight discrepancies in nine million repos

The sequential pass finished the rest of the live fleet, and the full verdict is in:

| shard  | exact     | loose   | mismatches |
|--------|-----------|---------|------------|
| shard0 | 2,231,041 | 102,271 | 3 |
| shard3 | 2,233,389 | 100,670 | 2 |
| shard4 | 2,231,379 | 101,984 | 2 |
| shard5 | 2,232,947 | 101,196 | 1 |
| **total** | **8,928,756** | **406,121** | **8** |

**Eight** genuine count-short repos across **9,334,885** verified on the live fleet — a real-discrepancy
rate under one in a million. The ~406k `loose` (≈4.3%) is the known live-overlay band, not loss: ClickHouse
holds more than the backfill receipt because Jetstream kept writing, and the O(1) digest can't tell a benign
live arrival from a masked drop, so those go to the convergence pass rather than being trusted either way.
After everything — the wedges, the silent 1 GiB cap, the two days verify couldn't even run — the data itself
came back essentially intact. (This is the four *live* shards 0/3/4/5; the deleted shard1/2 still need their
own ledgerless verify from the preserved copies.)

- **The instrument needed one more honesty pass to report this correctly.** A stale shard5 row from an
  aborted *pre-rewrite* attempt (`done=1` but `repos_checked=0`) was polluting the rollup, so `done=1`
  alone wasn't a safe "completed" filter — the fix counts only rows *completed with checked repos, or
  fresh in-progress* (`a7400ed`, after a rebase/amend dance). And the badge that read **"last failed"**
  for a shard verified-with-mismatches got reworded to **"mismatch"** — "failed" is the wrong word for
  "8 of 9.3M didn't reconcile." Small, but exactly the genre of this end-game: the numbers were right
  and the *labels* were lying, one more time.
- **Throughput held its shape:** each shard ~2.33M repos, ClickHouse classification ~70–75s spill-free at
  ~4.7 GiB, then the SQLite promotion as the long disk-bound leg — ~4 min end-to-end, run strictly
  sequentially on the single serving box. No new data-loss or cap surprises this round; the verifier that
  was rewritten and adversarially reviewed an hour ago ran four shards clean.
- **The next question is now the real one, and Alice asked it plainly:** *"so what do we do with 1. loose
  2. fail"* — `fail` first (the 8 hard mismatches: re-fetch and reconcile, or confirm they're upstream
  deletions), `loose` second (the convergence `--did-file` pass that re-crawls the ambiguous band until it
  collapses to the genuinely-live tail). That work, plus the deleted shard1/2 verify, the `v:1` recrawl,
  the Bridgy revive, and cutover, is what stands between here and done — but the load-bearing fear of this
  whole project, *did we silently lose data*, now has a measured answer: eight repos, all visible, all
  recoverable.

### 2026-06-14 late afternoon (16:10–16:51 UTC) — "the verification round was for all of them": a four-of-six miss the dashboard hid, and a near-meltdown of the page

This window is the candid one. The clean four-shard verdict from the last entry was, it turned out,
only four of the **six** shards that were supposed to be verified — and the reason it read as complete
is that the instrument said so.

- **Operator miss: verify ran the live half of the fleet and called it done.** The agent treated
  "the live shards" (0/3/4/5) as the verification set after the shard1/2 boxes were deleted, and the
  dashboard's "verified shards 4/4" made that look like the whole fleet. Alice caught it flat —
  *"why did we not verify the other two?? the verification round was for all of them............"* — and
  she was right: the handover and this very document specified the pass as **active shard0/3/4/5 + the
  retired shard1/2 copies on the serving box**, the copies preserved for exactly this. The cause was a
  too-convenient mental substitution ("runnable hosts" for "shards to verify") backed by a label that
  confirmed it. The fix to the label came first — "verified shards" → "reporting shards," caption
  "latest reporting shard runs" (`2a03c1e`) — so 4/4 stops reading as 6/6; then the real fix: locate the
  ~36 GB retired ledgers on the serving box and run their verify from the preserved copies. (A first
  attempt failed instantly on auth — it used the crawler's secret source, not the serving box's — a
  small reminder that the retired-ledger path is a *different* environment than the crawl hosts.)
  Restarted correctly, shard1 and shard2 verifiers are now staging (~2.34M repos each), and the page
  honestly reads **4 / 6 reporting shards** — the state it should have shown all along. shard1/2 results
  are still pending; the six-shard verdict isn't in yet.
- **Then the cure nearly killed the patient: starting the retired-ledger verify almost melted the
  dashboard.** Bringing shard1/2 back into play meant the overview's *crawl* rollup suddenly saw both the
  old frozen shard1/2 crawler telemetry *and* the new retired-ledger snapshots, and double-counted —
  enumerated/status totals went wrong, and Alice (after two aborted turns) said *"okay. stop. this is
  getting out of hand."* Root cause, and it's the same root as every run-scoping bug this end-game:
  `fetchOverview()` takes the **latest row per shard across all `run_id`s**, which was fine when only one
  kind of run existed — and is actively dangerous now that backfill, `dev`, over-cap, verification, and
  recrawl telemetry all coexist. The specific poison was a single `dev/shard0` row from the targeted
  fail-round recrawl carrying `pending = 56,584,730`, picked as "newest shard0" and corrupting the whole
  crawl card. The fix pins the crawl overview to the **canonical backfill run IDs only**
  (`whale-2026`, `whale-2026-overcap`, `whale-2026-overcap-long`), excluding dev/verification/recrawl
  snapshots (`4dea6f5`) — then a 500 from a ClickHouse `argMax(run_id) AS run_id` alias colliding with
  the inner `WHERE`, fixed by renaming the alias (`06d1de1`). The keeper, now unavoidable: **the dashboard
  was built assuming a single run, and the end-game is many concurrent run types.** Every "the page is
  wrong" of the last two days is one instance of that unstated assumption breaking.
- **Meanwhile the fail-round recrawl is running, with the heap knob it needed.** The 8 mismatches (and the
  over-cap repos) are being re-fetched independently on the crawl hosts; `468528a` makes the *parser
  worker* heap configurable so the parent and worker caps can be sized per box (shard3's box has less RAM,
  so a smaller cap; shard0/4 get 24 GiB parent + worker) — large repos need the headroom. Still ahead and
  unchanged: the shard1/2 verdict, the `--did-file` convergence on the LOOSE band, the `v:1` recrawl, the
  Bridgy revive, and cutover. The honest reframe of this hour: the *data* answer is still excellent, but
  "verified" briefly meant "verified the part that was easy to reach," and it took the human reading the
  page — again — to turn 4/4 back into 4/6.

### 2026-06-14 evening (16:51–18:00 UTC) — all six shards reach the digest layer, but the verifier itself faked a pass on one

All six shards are now through digest/count verification — and the way the sixth got there is the
sharpest possible illustration of why "the digest matched" is *not* the same as "verified," the exact
distinction Alice held the line on. The six-shard picture at the count layer:

| shard  | exact      | loose   | fail |
|--------|------------|---------|------|
| shard0 | 2,231,041  | 102,271 | 3 |
| shard1 | 2,230,967  | 112,835 | 0 |
| shard2 | 2,219,025  | 112,243 | 0 |
| shard3 | 2,233,389  | 100,670 | 2 |
| shard4 | 2,231,379  | 101,984 | 2 |
| shard5 | 2,232,947  | 101,196 | 1 |
| **total** | **13,378,748** | **631,199** | **8** |

That is the digest/count layer only. It is **not done and not clean**: the 8 hard mismatches are
mid-recrawl (not yet reconciled to zero), the 631,199 `loose` are a *separate, not-yet-started*
convergence round, and "loose" by construction means ClickHouse has at least the ledger's post count
but the exact count+digest didn't match — usually live posts beyond the snapshot, but unproven until
re-fetched. Digest-verified is a measurement; the verdict needs the reconciliation.

- **The headline, and the vindication: shard2's first verify was a *false pass*.** Its run staged
  2.33M DIDs, classified **zero** ClickHouse rows (a bad/empty result), and then **promoted 1,994,670
  ledger rows to `verified` anyway** — the verifier certified ~2M repos it had not actually checked.
  This is precisely the nightmare the earlier entry named in the abstract ("a bug in the verifier
  doesn't corrupt data, it manufactures false confidence — the most expensive kind") happening for
  real, and it is exactly why a digest match isn't a verdict: the checker can pass on nothing. Caught,
  shard2 was marked back to not-verified and the guard added — `ce7f310`, *refuse partial/zero
  classification promotion* — so a verify that didn't actually classify can never promote again. The
  rerun came back honest: `2,219,025 exact, 112,243 loose, 0 fail`. Without the insistence that
  "verified" means the integrity pass actually ran, shard2 would be sitting in the table above as a
  clean ~2M-repo lie.
- **The 8 mismatches are being re-fetched — provisionally, not closed.** After a `5d559cb` fix to a
  *whale-batch dedup collision* in the loader (reviewed by Claude, verdict: safe and required before
  rerun), the 8 known fail DIDs were restarted host-sharded, one process per shard at
  `GLOBAL_CONCURRENCY=1` with high heap (whale repos need the headroom). By 18:00 it was "mostly done"
  — 7 of 8 fetched, shard5's DID reloaded `113,042` rows — but *mostly done* is not zero, and the
  reconciliation that turns these from "count-short" into "accounted for" hasn't been confirmed. The
  loose convergence is queued behind it as its own round.
- **A process-retro beat with a real price tag: don't kill a quiet Claude.** Trying to get the loader
  fix reviewed, the agent inferred Claude Code was *stuck* from a lack of stdout and killed the review
  — twice. Alice corrected it hard and with the economics attached: *"claude is not stuck, claude is
  almost never stuck, claude just takes time… killing a running claude code review costs a lot of
  money"*, *"every hour the crawler servers are running they cost me ~40 eurocents"*, *"time is
  literally money."* The right diagnostics, now recorded as a rule: smoke-test the CLI params with a
  tiny prompt (it returned `OK` in ~8s — params were fine), watch the review's **session JSONL** for
  live progress, and check CPU — silence on stdout is the *normal* shape of a long review, not
  evidence of a hang. The recovered review (from its session log) was the thing that cleared the
  loader fix to deploy. The lesson generalizes past Claude: "no output" is not "stuck," and tearing
  down a long-running job on that inference burns exactly the money and time you were trying to save.

So: six shards measured, one false pass caught and corrected, 8 mismatches re-fetching, 631k loose
still ahead — and per the standing correction, none of this gets called clean or done until the
re-fetches reconcile and the loose band converges.

### 2026-06-14 evening (18:00–18:38 UTC) — the convergence pass is memory-bound, and the false-pass guard catches an OOM

Getting from the digest verdict to the loose-convergence round turned out to be the expensive part —
not the math, the memory. To emit the loose DID set for re-fetch, the verifier re-runs the shard-wide
reconciliation (`INSERT … SELECT` over `posts FINAL`), and at full parallelism that scan **OOMed
ClickHouse** at the 12 GiB cap after ~684s. The fix throttles it (`ec6c2d5`): a `max_threads` cap, and
when that still wasn't enough, a strict single-lane profile — **1 ClickHouse thread, 128 MiB spill
thresholds, a 4 GiB query cap**. That holds memory at ~1.5–2.5 GiB and survives, but a single-thread
full-`FINAL` scan is **~20 minutes per shard** versus the ~73s the happy-path classification took. The
honest shape of verify cost, now clear: the digest classification is cheap; it's re-running the FINAL
scan *under memory pressure* — which the loose convergence forces — where the real time lives.

- **The guard added an hour ago for shard2's false pass just defended against a completely different
  failure.** When the reconciliation OOMed, ClickHouse killed the query and the `INSERT … SELECT` wrote
  **zero result rows** — which is *byte-for-byte identical* to a genuine empty classification. The
  `ce7f310` "refuse promotion on zero/partial classification" guard caught it and refused to promote,
  exactly as it would for a real empty result. So a guard built for one failure mode (a verifier that
  classifies nothing yet promotes anyway) turned out to also block a second the design never
  anticipated (an OOM that masquerades as an empty pass). That's defense-in-depth earned rather than
  planned — and it's the reason an OOM mid-convergence is a *slow retry* now instead of another
  ~2M-repo false "verified." The keeper: when "succeeded with nothing" and "failed loudly" produce the
  same observable (zero rows), a guard on the *invariant* ("you may not promote what you did not
  classify") covers failure modes you haven't thought of yet.
- **Status, held to the line:** the loose convergence has **not** produced numbers — the agent is still
  getting the throttled reconciliation to run survivably (strict single-lane shard0 in progress, ~20
  min), and only then does the loose DID list get emitted and re-fetched. The 8 hard mismatches are
  still mid-recrawl from the previous round, not confirmed reconciled. So nothing has converged and
  nothing is closed: six shards are digest-measured, the convergence machinery is fighting memory, and
  the 631k loose + 8 fail remain exactly as open as they were — just now with a verifier that won't lie
  about it even when ClickHouse falls over mid-scan.

### 2026-06-14 evening (18:38–19:10 UTC) — header overflow, a halted verifier and a full safety refactor

The throttled shard0 reconciliation survived the OOM cap but hit a third failure mode: after ~674s
and 87 GiB read, ClickHouse's HTTP progress headers overflowed and the client reset the connection —
`Parse Error: Header overflow`, no result rows written (pix2 session rollout-2026-06-13T21-12-24,
~line 14400). That makes three distinct ways the loose-convergence pass has failed to produce a
number: the original OOM, the zero-classification false pass, and now a protocol-level overflow from
long-running HTTP queries. Each one surfaced a different assumption about how ClickHouse communicates
failure.

Alice halted all production verifier runs: *"Have Claude review this script… I do not want to lose
more money on this."* A Claude code-review agent on pix2 audited `verify.ts` and returned **do not
start another production verifier run yet** — the first time a review agent has hard-blocked an ops
action in this project. The agent then implemented a substantial safety refactor:

- **`wait_end_of_query`** on all verifier ClickHouse commands — surfaces errors as query errors
  instead of the zero-row "succeeded with nothing" artifact that fooled promotion before
- **Removed `FINAL`** from reconciliation and orphan-example queries — `FINAL` forces an in-memory
  merge of the full `ReplacingMergeTree`, which is precisely what pushed the 12 GiB cap
- **`--loaded-only` mode** with a hard guard: can only run under a non-canonical shard label (e.g.
  `fail-shard0`), so cheap recrawl-verification cannot accidentally overwrite the dashboard's
  full-shard totals
- **Narrowed promotion policy:** exact-match loaded repos promote immediately; loose loaded repos
  promote *only* after an exhaustive sample passes — a failed loose sample no longer leaves the
  ledger marked verified
- **Missing `VERIFY_RUN_ID` warning:** default per-process run IDs are unsafe for fleet
  coordination; the verifier now logs it

Typecheck and 51 tests pass. The diff was sent to a second Claude review round; that review was still
running when the session ended (~19:08 UTC). **No commits have been pushed** — the entire refactor is
local on pix2, gated behind the second review.

- **Status, held to the line:** loose convergence still has not produced numbers. The 631k loose and
  8 hard mismatches remain exactly as open as the previous entry. The refactored verifier is closer
  to something that can survive its own queries, but it has not run yet.

### 2026-06-14 ~19:08 UTC — a second backfill run

Alice started a fresh Codex session (rollout-2026-06-14T19-08-16) with a "thermo-nuclear code
quality review" of the entire repo, especially the backfill path. The stated reason: *"now that I've
learned a lot from the first backfill I intend to run it the second time and see how much it
improves."* The retro's "If I did this again" section — the progress-gated watchdog, header-driven
pacing, no-silent-caps, write-time verify — is about to be tested against a real re-run.

The review agent (102 lines in at time of writing) is flagging structural risk in `verify.ts`
(1,439 lines), duplicated invariants in comments instead of types, and dashboard label
normalization. The first backfill is not done (verify still open, cutover still ahead), but the
planning for a second run is now concurrent with closing out the first.

### 2026-06-14 evening (19:10–19:44 UTC) — the safety refactor lands, the dashboard stops lying about "fail", and the review says don't run yet

Three things happened in quick succession across four parallel Codex sessions.

**The verifier safety refactor shipped.** The second Claude review passed the core logic with one
hardening note — tighten `--loaded-only` to an allowlist of `fail-shardN` labels rather than a
denylist of canonical ones, and block `--loaded-only --orphans`. The agent applied both and pushed
(`73cfb17` progress-header disable, `712a8e9` safe loaded-only reconciliation). No verifier was
deployed or run — the code is on origin, gated behind Alice's explicit go.

Alice then found a scalability regression the reviews had missed: `verify.ts:717` pulled every
successful DID through Node/SQLite for promotion. On a full shard that's millions of rows through a
single-threaded bottleneck. The agent patched promotion back to a small-list pattern — fetch only the
blocked DIDs from ClickHouse, stage those in SQLite, promote everything not blocked. The fix is local
on pix2, pending yet another review round.

**The dashboard's "fail 8" was a lie — a different kind.** Alice asked whether the 8 hard mismatches
had reconciled and found the dashboard still showing `fail 8` as if it were a current count. It
wasn't: the card was displaying the *historical digest-layer mismatch count* as though it were the
*current unresolved state*. The agent split the card into two concepts: `loaded open` (how many
mismatch repos are still unresolved after recrawl) and `digest diff` (the historical full-pass
classification). Committed as `b05173a`, hit a ClickHouse alias collision on deploy, fixed in
`1b573b4`, redeployed to the serving box and browser-verified. The ops dashboard now shows `loaded
open 8` and `digest diff 8` — honest about what each number means.

This is the fifth time a dashboard metric has told a story that turned out to be wrong (after the
run-id scoping, the unbounded window, the frozen-shard-as-active, and the four-of-six miss). The
recurring pattern: a dashboard that *looks* right at the moment it's built but silently becomes stale
as the underlying state moves. The dashboard never *lied*; it just never had enough context to know
its truth had expired.

**The thermo-nuclear review verdict: don't run the second backfill yet.** The code quality review
(session `rollout-2026-06-14T19-41-38`, still active) found three structural blockers for a clean
second run: the promotion path flips from allowlist to denylist (risky inversion); `verify.ts` is
1,458 lines with duplicated invariants in comments instead of types; and event telemetry has no
`run_id` or `shard` scope, so the dashboard cannot distinguish run 1 from run 2. That last one is
exactly the instrument-scoping problem the retro has documented since the beginning — the same class
of bug that made every progress metric unreliable until it was fixed, now blocking the second run's
ability to measure itself against the first.

- **Process note:** Alice had to abort two turns in the old session because the agent started running
  duplicate Claude review processes simultaneously (a capped and an uncapped review of the same
  diff). *"please… why are you running two claude reviews."* The context was exhausted; Alice asked
  for a handover to a fresh agent. Agent process management — knowing what's running and not
  duplicating expensive operations — is a failure mode distinct from the code quality the reviews are
  checking.
- **Status, held to the line:** the safety refactor is on origin, the dashboard is honest, but
  nothing has been *run*. No loose numbers, no reconciled mismatches, no convergence. The 631k loose
  and 8 hard mismatches remain exactly as open as before — just now with better tooling for when they
  do get closed.

### 2026-06-14 ~20:30 UTC — what the agent memories knew that the transcripts didn't

The retro has been mined from session transcripts and git commits. But the agents on pix2 also
maintain their own persistent memories — Claude's project-level memory files and the Codex agent's
`~/.codex/memories/` — and those contain decisions, incidents and findings that never surfaced in
the transcript text because the agents internalized them silently. Mining those memories revealed
several retro-worthy gaps.

**The night ops protocol.** When Alice sleeps and asks the ops agent to guard the fleet, the
standing orders are: stay in a `/loop`, never stop on errors; if idle, sleep ~10 min and re-check.
Code changes are allowed for new bottlenecks but must be conservative, and reviewed via the Codex
CLI (GPT 5.5, xhigh reasoning) with at most 3 review/fix rounds before deploy. This protocol —
conservative-change plus external-model review as a gate on autonomous deploys — is a significant
process decision about how multi-agent collaboration works when the human is asleep. The retro
mentioned "the watcher needs watching" (the 03:10 malformed command incident) but never documented
the protocol itself. In hindsight, the fact that the overnight guard loop *worked* — the drain
completed, the fleet stayed healthy, no bad deploys — is partly because the protocol constrained the
agent's autonomy to a narrow, reviewed band of action.

**Open correctness findings from the adversarial reviews.** The June 13 adversarial code review
(`docs/adversarial-code-review-2026-06-13.md`) and the June 14 thermo-nuclear review both found
High-severity issues that are still open. Two matter for run 2 planning because they affect whether
the write path can silently lose data:

- **GEN_RETENTION eviction** (`packages/backfill/src/loader.ts`): the adversarial review found that
  a repo spanning more than 128 flush generations could have earlier failed generations evicted from
  `#runByGen`, with `finish()` treating a missing entry as success — a durability violation where
  the write path tells the ledger it succeeded despite dropped rows. **This was fixed the same day**
  (`a31b514`, 2026-06-13): failed flushes are now retained forever in the map, and entries are only
  evicted on success. A missing entry now provably means the flush succeeded. The retro watcher
  initially reported this as still open (the adversarial review doc was never updated); the code
  tells a different story. [Corrected: the bug was found and fixed on the same day; it is not an
  open issue for run 2.]
- **Parse workers materialize whole repos in memory** (`packages/backfill/src/parse-worker.ts`):
  the worker collects every parsed post into a single `rows: ArchiveRow[]` array and posts the full
  array back to the main thread via structured cloning. With `CAR_MAX_BYTES` now at 64 GiB (raised
  from 1 GiB after the silent-cap data-loss discovery), multiple concurrent workers completing
  around the same time can cause memory spikes or OOMs through double-materialization. This is the
  specific mechanism behind the whale-repo memory pressure the retro has discussed; the fix is to
  stream row batches from worker to main instead of buffering the whole repo.

A third finding is lower severity but cheap to fix: the **live insert deduplication token**
(`packages/ingest/src/writer.ts`) is based only on `rows.length` plus the first and last
`(did, rkey)` pair. Two distinct batches with the same boundary tuples and length within
ClickHouse's dedup window would collide, silently dropping one. The token could hash all row keys
or the serialized payload instead.

**The stop-state discipline convergence.** The Codex agent's own memories independently arrived at
the same principle the retro calls "hold the line": *finish the named subgoal, document the stable
operating point or failed canaries, and do not over-claim broader completion.* The retro derived
this from watching the agent's behavior in transcripts; the agent derived it from Alice's
corrections. That both sides converged on the same rule — from different evidence, stored in
different systems — suggests it's load-bearing. The rule works because it keeps the agent honest
about what's done and what isn't, which is the same property that makes the retro itself useful:
you can't plan the second run from a narrative that overstates what the first one achieved.

### 2026-06-14 night (19:44–22:23 UTC) — the eight hard mismatches close, loose convergence begins, and the dashboard lies three more times

Three things converged in a long evening session (rollout-2026-06-14T19-31-25, 3548 lines).

**All eight hard digest mismatches are resolved.** An earlier fail-recrawl had silently cleared 4 of
the 8 to exact — but nobody knew, because the dashboard was using stale fail-run telemetry instead of
canonical ledger snapshots (showing `loaded open 8` when the ledgers said 4). Alice asked the
right question: *"are you super duper sure that the other 4 was recrawled and done correctly?"*
The agent checked and confirmed the first 4 from ledger evidence: verified exact, promoted, zero
mismatches. Then kicked off targeted recrawls for the remaining 4 DIDs — one 5M-post repo on
shard0 (mottlegill), one 4M-post and one 152k-post on shard3 (stropharia), one 55k-post on shard4
(magic). Each recrawled, loaded, loaded-only verified exact, promoted. **Loaded-open is now 0
across all shards.** The hard-mismatch thread that opened at 15:13 UTC is closed.

**The dashboard lied three more times** in the same session, for a running total of eight:

- `loaded open` sourced from stale `fail-rerun-fixed-20260614` telemetry snapshots, not the
  canonical crawl snapshots — showing 8 when the ledgers said 4 (`6b23981`)
- After the shard3/4 loaded-only promotions, the dashboard didn't learn about them because it reads
  ClickHouse telemetry, not live SQLite ledgers — still showing 4 when the truth was 1
  (`39ba5e6`)
- `digest diff 8` kept rendering as active failure debt after the hard mismatches were all
  resolved — the label needed a state transition from "active" to "historic" (`b166586`)

The pattern is now unmistakable and worth naming: **the dashboard's truth expires faster than the
dashboard updates.** Every fix in this session was the same shape: a metric that was correct when
written, displayed as current, but stale against the ledger's real state. The gap is structural — the
dashboard reads ClickHouse telemetry, but the ground truth lives in SQLite ledgers that only the
verifier promotes. Until a verifier run writes new telemetry, the dashboard shows the last thing it
was told, and the last thing it was told is now wrong.

**The thermo-nuclear code review drove real fixes.** Four commits from iterating on review rounds,
each reviewed by Claude before push:

- `1306327`: verification run accounting — promotion becomes an explicit passed-DID allowlist,
  telemetry carries `run_id`/`shard`, event schema migrated with `ADD COLUMN IF NOT EXISTS`,
  parse workers stream row batches with ack instead of buffering full repos
- `7a7960c`: run guardrails — explicit promotion staging objects, `BACKFILL_RUN_ID` guard (missing
  or `dev` → fail before touching ClickHouse), shard count documented in runbook, recrawl file
  backpressure
- `f246182`: recrawl host migration — preserved exact recrawls now PLC-refresh on `RepoNotFound`
  and retry with the corrected host; stalled workers get retired instead of leaking
- `97db9e2`: crawl guardrails from Claude's review — worker watchdog stays alive during
  backpressured streaming, telemetry schema preflight fails the crawl if event columns are missing

**Loose convergence has actually started.** After the hard mismatches closed, the agent began
emitting loose DID files. Shard0 completed: **112,118 loose DIDs**, 0 hard mismatches — the
reconciliation ran through 2.22B rows, 125 GiB, in ~10 minutes under the throttled single-lane
profile. Shard1 (retired ledger on the serving box) was still staging when the session was last
read. This is the first time the loose convergence has produced an actual number since the OOM
and header-overflow failures blocked it.

- **Process note:** Alice had to correct the agent's sequencing twice. The agent tried to kick off a
  "final canonical verify" when the next step was the loose round. *"please pay attention and follow
  the order time is money mistakes are very costly."* The order is hard → loose → final sweep →
  cutover. The agent conflated "verify the repairs" with "verify the whole fleet," and nearly ran
  the wrong expensive operation.
- **Status, held to the line:** the 8 hard mismatches are closed and verified exact. Loose
  convergence is running — shard0 emitted 112k DIDs, remaining shards in progress. The 631k loose
  band is about to shrink for the first time. Nothing has converged *to closure* yet, but the
  machinery is finally producing numbers instead of fighting infrastructure.

### 2026-06-14 late night (22:23–23:37 UTC) — ClickHouse OOMs the loose recrawl, and DID-file restarts replay everything

The loose convergence that finally started producing numbers hit the same wall — ClickHouse
`MEMORY_LIMIT_EXCEEDED` — but from a different angle. This time it wasn't a FINAL scan; it was
**four concurrent insert streams from the loose recrawlers** while materialized views tried to
aggregate them. `jemalloc.allocated` climbed above the 12 GiB server cap, and even status queries
started failing. The fix was operational: restart ClickHouse to clear allocator pressure, then
resume the loose recrawl at gentler settings — `GLOBAL_CONCURRENCY=128` (down from the default),
`PER_HOST=8`, `LOADER_BATCH_ROWS=500`. That held memory at ~3-4 GiB and the recrawl resumed at
~505 repos/min.

The dashboard lied again — the ninth time, by now so expected it barely rates a mention. The
loose recrawl progress card used the ledger's `loaded` count as progress, but for preserved
loaded/verified repos being re-fetched, that field stays flat while the crawler is actively
working. Fixed (`5bb8fce`) to use event-backed progress: `68,502 / 684,873` processed after the
fix deployed. The total loose DID count across all shards is **684,873** (up from the earlier
631k because the per-shard emission included repos the fleet-level rollup had deduplicated).

**The restart duplication bug.** Alice's question surfaced the real problem: exact `--did-file`
recrawl starts from line 1 on every restart. With loaded/verified rows intentionally reprocessable
(`preserveExisting`), every restart replays DIDs that already completed — burning rate-limit
budget on work already done. The first 70,908 DIDs had been processed, but every restart was
starting from DID #1 again. The agent first applied an ops workaround (extract processed DIDs
from ClickHouse, write remaining-DID files with `comm`), then Alice called it out and the agent
began a proper code fix: run-scoped `.done.<BACKFILL_RUN_ID>` checkpoint files that the scheduler
skips on restart. That fix is in progress but not yet pushed.

This is the same pattern as `listrepos-diff` clearing more backlog than crawling — when the
process can't remember what it's done, it wastes time rediscovering it. And it's another entry
for the run 2 preflight: any DID-targeted operation (`--did-file`, `--emit-loose`, exact recrawl)
must checkpoint progress durably per run, or restarts multiply the wall-clock by the restart
count. The ~27h ETA the dashboard was showing was partly real (the throttled concurrency) and
partly phantom (replayed DIDs inflating the denominator without advancing the numerator).

- **Status, held to the line:** loose recrawl is running at throttled settings across all 4
  shards. 70,908 of 684,873 loose DIDs processed (~10%). ClickHouse is stable at the lower
  insertion rate. The checkpoint fix is local, not yet pushed. Nothing has converged to closure.

### 2026-06-14/15 night (23:37–01:10 UTC) — the second-time playbook implemented mid-first-run: dynamic host caps from rate-limit headers

The retro's "If I did this again" section ranked "parse rate-limit headers and pace proactively —
from line one" as the #2 time-saver. Alice just implemented it — not for run 2, but mid-run-1's
loose convergence, because the ETA was >24 hours and she wanted to know why.

The diagnosis: the crawlers were using ~20 of 512 available fetch slots per host, because a
**static per-host concurrency cap** was the gatekeeper, not the PDS rate limits. The crawl was
rate-limited by its own code, not by the mushroom hosts. Alice asked the question that broke it
open: *"we literally parse the headers. why aren't we going as close as possible to those
limits?"* The answer was that the code parsed headers for pacing (spacing between requests) but
never used them to raise the cap (how many concurrent requests per host).

The fix (`8b6f626`): `host-pressure` now computes a dynamic cap from advertised rate-limit headers
— 60 seconds of advertised queue depth by default, capped at 1024 per host/process. The scheduler
updates `p-limit.concurrency` live as headers arrive. 429s and stalls still clamp via AIMD. The
`p-limit` library supports changing `.concurrency` at runtime, so the host queue expands after
headers arrive and contracts under pressure — the exact behavior the retro described.

Result on the canary (crawl0): active fetches climbed from ~20 to 256, zero retries, ClickHouse
memory stable at 4.1 GiB. Rolled to all 4 boxes. The ETA should drop substantially once the rate
window fills with the new throughput, but no number yet — the first waves are large CARs and the
10-minute rate sample needs time to settle.

Three other fixes landed in the same session:

- **DID-file checkpoint pushed** (`a434dbb`): the restart-duplication fix from the previous entry.
  72,613 DIDs checkpointed, remaining-file recrawlers restarted on fixed code.
- **Dashboard asset serving** (`adb1c1d`): the dashboard's JavaScript/CSS assets were 404ing from
  the bun server, so the page rendered as SSR HTML only — no hydration, no polling, no live
  refresh. The card that "looked stale" was actually a dead SPA. Fixed by adding an asset route.
- **Dashboard labels** (`738e81a`, `e3d4cdb`): raw event rows and unique DIDs were mixed in the
  same card without labels. The loose progress card now shows unique completed repos, unique loaded,
  unique issues, and raw event rows — each labeled for what it is.

- **Status, held to the line:** loose recrawl is running at full dynamic host caps across all 4
  shards. ~75k of 684,873 loose DIDs processed. The new throughput is still warming up — no
  reliable ETA yet, but the static bottleneck that was causing the >24h estimate is gone. Nothing
  has converged to closure.

### 2026-06-15 afternoon (~11:30–16:07 UTC) — the Rust rewrite is back on, and the retro becomes the spec

Alice reversed the "no Rust rewrite" decision from the previous evening. The catalyst wasn't
the performance argument — it was a structured adversarial design session using the
`grill-with-docs` skill against the retro and an initial v2 plan. The grilling surfaced gaps
the earlier conversation had missed, and the resulting design document
(`docs/backfill-v2-design.md`, `4e79790`, 361 lines) is a fundamentally different architecture
from the TypeScript preflight we drafted together.

What changed:

- **Parquet is the single source of truth.** ClickHouse is explicitly a derived, rebuildable
  projection. The crawler's only output is Parquet files on a Storage Box. This inverts v1's
  architecture where ClickHouse was the truth and Parquet was an archival sidecar — and it means
  verification is against the archive, not against a database that's being concurrently written by
  the firehose.
- **No live/backfill table overlap.** v1's "overlap collapses structurally" was elegant but
  created the "loose" verification category and every dashboard lie about it. v2 runs the backfill
  to completion, *then* starts Jetstream catch-up from a recorded timestamp. No concurrent writes
  to the same table from two sources, no "is this a backfill row or a firehose row" ambiguity.
- **MST root verification at crawl time.** Instead of post-hoc digest comparison (which couldn't
  prove set-subset once the firehose muddied the counts), v2 verifies the MST root against the
  signed commit at parse time. This is a **completeness proof**, not a statistical check — and it
  runs as part of the pipeline, not as a separate phase that OOMs ClickHouse.
- **HuggingFace publication** as an explicit product. The raw post corpus (observed, not
  cumulative-ever — a term the grill session pinned down) is packaged as a public dataset. This
  reframes the project from "emoji site with a backfill" to "a complete network snapshot that
  happens to power an emoji site."
- **CARs spooled to 512 GB local disk, then discarded.** The earlier "no CAR storage" decision
  stands for long-term, but CARs live on local NVMe during processing — eliminating the
  in-memory parse worker materialization problem entirely.
- **Stratified canary gate.** Not just "1 box first" but a structured canary: random repos, whale
  repos, old/future/invalid timestamps, emoji-heavy, third-party/Bridgy, malformed/partial cases,
  plus a short multi-box contention test. Hard-gated before fan-out.

The operational invariants section carries forward every retro lesson verbatim: scriptable
fleet ops, O(LIMIT) claim path, header-driven pacing, loud resource caps, deploy-via-git, WAL-safe
ledger backup, dead-host registry with its inverse. The tiebreaker is explicit: **correctness >
operability > performance > craft.** The design doc's provenance section names this retro as its
primary source.

The grill session also formalized domain language into a `CONTEXT.md` and an ADR
(`docs/adr/0001-raw-archive-and-public-corpus-boundary.md`): the distinction between the private
Raw Archive (operational truth) and the candidate Published Raw Observed Corpus (public, deferred
publication policy). A second grill round produced a review packet
(`docs/backfill-v2-final-review-packet.md`, `b99416d`) with 30 resolved decisions and a critique
checklist to apply back to the design record.

The retro's reason for existing was always "structured source for a future public blogpost." It
turned out to be something more: the spec for the rewrite. Every architectural decision in v2
traces to a failure mode the retro documented, and the design document says so explicitly: *"where
this document and the original plan disagree, this document wins — it incorporates corrections the
plan did not have."*

Meanwhile, the loose recrawl continues running on the v1 fleet. The parquet archive on the Storage
Box is 297.8 GiB across 4,958 files. Alice noted only two shards appear to still have active
recrawlers. The v1 end-game and the v2 design are running concurrently.

### 2026-06-15 evening (18:39–19:36 UTC) — v2 Rust code starts building, and the loose recrawl breaks the public counters

Two things in parallel:

**The v2 Rust rewrite started building.** Branch `v2-rust-backfill` on pix2 now has working code:
checkpoint A (scaffold/fixtures) was done before the session, and B (transport), C (parse), D
(receipts/manifests) were built in the same session — B and C delegated to parallel subagents,
integrated by the main agent. The `fetch-one` pipeline (A→B→C→D) is wired locally and compiling.
The Jacquard crate provides CAR/MST codec and API types as designed; the hand-rolled layers
(reqwest HttpClient, on-disk BlockStore, self-driven inactivity timeout, error classifier) are
in progress. First real-CAR test pending. The transition from design to implementation took ~2
hours from "can we start building" to a compiling pipeline.

**The loose recrawl inflated the public site's post count.** Alice noticed the public emoji site
showed an impossibly high post count. Root cause: the loose recrawl re-inserts posts that already
exist in ClickHouse (by design — `preserveExisting` re-fetches the repo and the
`ReplacingMergeTree` deduplicates on merge). But the **materialized views** (`posts_hourly`,
`emoji_hourly`, aggregate totals) increment on *insert*, not on *final merged state* — so every
re-inserted row inflated the aggregate without retracting the replaced row. ~898.6M backfill rows
inserted on the day, each one counted by the MV regardless of whether it was new or a duplicate.

This is not data loss — `posts FINAL` returns the correct deduplicated count, and the parquet
archive is unaffected. It's a **derived cache integrity failure**: the aggregates diverged from
the source table because they're append-only projections of an at-least-once write path. The fix
is an aggregate rebuild from `posts FINAL`, which the v2 design document already requires as a
post-backfill step (and one of the reasons v2 treats ClickHouse as a derived, rebuildable
projection rather than the truth).

The irony: this is the same class of bug the retro has documented ten times — a number displayed
as current that's actually stale against the underlying truth. The aggregate tables are one more
dashboard that lies by not knowing its own truth has expired. And it's one more argument for the
v2 architecture where Parquet is the truth and ClickHouse is explicitly disposable.

### 2026-06-16 — the rewrite explodes from pipeline to smoke test; the smoke test explodes the box

**The v2 Rust rewrite went from a compiling pipeline to 26,000 lines in ~24 hours.** Branch
`v2-rust-backfill` on pix2 accumulated 45 commits between the evening of June 15 and the
afternoon of June 16 — fetch, parse, archive, ledger, ClickHouse derive, scheduler, fleet
orchestration, emoji normalizer, scale smoke test, all wired and building. The workspace has two
crates (`emoji-normalizer` and `emojistats-backfill`) with Jacquard as a SHA-pinned fork-mirror
for AT Proto primitives. The main Codex agent orchestrated via parallel subagent sessions — at
least 6 concurrent sessions visible in the evening cluster alone, each owning a lane (ledger,
normalizer, fleet, transport, derive).

The Clippy configuration is the retro made into compiler policy. `unsafe_code = "forbid"`,
every lint group denied (`all`, `pedantic`, `nursery`, `cargo`), plus explicit denials on
`unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `indexing_slicing`, and
`arithmetic_side_effects`. The v1 lesson — "make the system reject bad work before it lands" —
is no longer a review-round finding. It's a build failure.

**The scale smoke test was OOM-killed.** `run-fleet` against 3 whale repos (the same ones that
exposed v1's 1 GiB CAR cap) at concurrency 4 hit 7 GB RSS and the kernel killed the Rust binary
at 22:11 UTC on June 15. The tmux pane vanished; the reliable recovery path was
`journalctl -k` → killed PID → tmux scope → Codex session JSONL. Response: `f257aef` added
fleet memory guardrails, then two streaming-architecture commits — `60263ae` (stream whale
archive path) and `fd94c08` (stream ClickHouse derive) — replaced the buffer-everything model.
This is the same pattern as v1's parse-worker memory finding (review High: "workers collect all
parsed posts into a single array") now caught at smoke-test time instead of in production under
load. The whale repos continue to be the project's best test fixture.

By the afternoon of June 16, Alice was already running thermo-nuclear code quality reviews
against the Rust code (sessions at 14:18 and 14:51 UTC) — the same discipline cycle that
surfaced v1's structural issues before the second backfill, now applied to v2 before a single
production byte is crawled.

**Meanwhile, the TS verify got smarter about loose samples.** Five commits to `verify.ts`
(`2ca7c55`..`54fbc85`, June 16 01:11–06:09 UTC) addressed the "post-load drift" problem:
repos classified as "loose" (ClickHouse has more posts than the ledger expected) because new
posts were created via the firehose *after* the backfill loaded the repo. The fix: track
`loaded_at` per repo, parse rkey TID timestamps, and filter out posts whose creation time
postdates the load. One approach — cutoff loose reconciliation at load time (`6ef8e37`) — was
reverted within five minutes (`1195e90`). The surviving approach adds bounded concurrency for
sample verification (`a9cfce2`), PLC directory resolution for stale PDS pointers (account
migrations and tombstones), retry with exponential backoff for transient failures, and a rule
that failed samples stay unpromoted rather than being incorrectly marked verified (`54fbc85`).
The five-minute revert is the verification story in miniature: every plausible shortcut through
the loose band reveals another edge case that makes the shortcut wrong.

### 2026-06-16 (continued) — the memory fix, the performance arc, and three reviews that agree

**The OOM had three root causes, not one.** The first scale smoke test died at 7 GB RSS, and a
second whale parse (`lb7`, 293 MB CAR, 2.5M posts) hit ~10 GB RSS before Alice stopped it
manually. She rejected the initial band-aid (an RSS cap) outright: "I don't want a band-aid. I
want a root cause fixed please." The three causes, each fixed independently:

- Jacquard's cached `MstCursor` kept the full MST in memory during traversal. Replaced with a
  streaming MST walker that holds only the current stack (`60263ae`).
- The parser accumulated all decoded posts into a `Vec<PostRecord>` before returning. Replaced
  with a visitor/streaming pattern that writes 1024-row batches to Parquet during traversal.
- The ClickHouse derive path read all archive rows into memory before inserting. Replaced with
  a streaming Parquet batch reader + bounded inserts (`fd94c08`).

The memory numbers before and after tell the story. Whale `lb7` (293 MB CAR): **10 GB RSS →
261 MB**. ClickHouse derive of the same repo: **7.3 GB → 35.6 MB**. Whale `o6g` (3.96 GB
CAR, 5.0M posts): **1.50 GB RSS**. The 24-DID final smoke test ran all whales at peak
**3.58 GB RSS** — down from a kernel OOM. 19 succeeded, 5 failed with expected terminal
classifications.

There was also an 18× parse speedup earlier in the session. CID verification was 90% of wall
time on a 41 MB test CAR (22s per repo in release mode). Replacing Jacquard's streaming CAR
reader with a one-pass verifier + indexer (`9749689`) dropped it to ~1.2s. Alice's reaction to
seeing 22s: "can we make it faster. that feels like a lot." Her reaction to 1.2s: "okay. yeah.
fine. commit. push." The `repo-stream` crate was also evaluated as a potential drop-in: it
benchmarked at 36s total on the o6g whale, but when properly compared against the streaming path
it was actually slower (87s vs 56s parse-only). Kept as a reference, not a dependency.

**A fourth root cause surfaced before the memory fixes: spool budget deadlock.** Four whale
fetches simultaneously filled the 512 MiB shared spool budget and all blocked mid-stream waiting
for budget to free. The fix: charge the budget after the CAR completes, not during active
download — the budget bounds concurrency, not in-flight bytes.

**Three independent thermo-nuclear code quality reviews converged on the same structural
blockers.** Alice ran the reviews on the afternoon of June 16 (sessions at 14:18, 14:51, and
15:11 UTC — potentially parallel agents). All three independently found:

- `main.rs` is a 2.6–3k line orchestration sink
- Six to seven Rust files over 1,000 lines
- The in-flight byte budget is charged *after* a full CAR finishes, so it doesn't actually bound
  what it claims to bound
- Two parallel committed-artifact systems (local and remote) that should be one engine with
  pluggable I/O
- Protocol duplication between `parse.rs` and `list_records.rs`

Verdict from all three: **"tests pass, but shape not approved."** Alice's response was to
package the Rust source into a zip and SCP it off the box for an independent external review.
This is the same pattern as v1's thermo-nuclear reviews — write the code fast, then submit it
to adversarial critique before it touches production. The difference is that v1's reviews came
after the backfill was already running; v2's come before a single production byte is crawled.

The session also revealed a working pattern for the parallel-subagent approach to the rewrite:
the main Codex agent orchestrated 6+ concurrent subagent sessions for individual lanes (ledger,
normalizer, fleet, transport, derive). The tradeoff: high velocity (26k lines in 24 hours) at
the cost of significant integration cleanup — subagents delivered code with compile issues,
missing helpers and stale tests that the orchestrating agent had to reconcile manually. The
concurrent sessions for the ledger lane alone produced atomic claims with CAS, worker leases,
stale recovery, and deterministic retry jitter — with 16/16 focused tests green.

### 2026-06-16 (final) — the second smoke passes all whales; the Codex agent's own memory tells the v1 story

**The second full smoke test ran all whales to completion.** After the index cap fix (512 MiB
default was too tight — f4z hit it at ~536 MiB, raised to 4 GiB; `998fee8`), a clean run
against all 24 fixture DIDs completed in **1,379 seconds (~23 minutes)**: 19 succeeded, 5
classified as expected terminal failures (account states, malformed CARs). Individual whale
results:

- **f4z** (1.86 GB CAR): fetched in 98.5s, parsed in 107.1s, peak 279 MB RSS at completion
- **4hm** (1.42 GB): fetched in 161.6s, parsed in 74.6s, peak 510 MB
- **lb7** (293 MB, 2.5M posts): parsed in 68.5s
- **ndj** (4.80 GB, just under the 5 GiB per-repo cap): fetched + parsed in 356s, ~1.5 GB RSS
- **o6g** (3.96 GB, 5.0M posts): transport decode error after 1.73 GB on first attempt, retried
  successfully; total ~616s with retry overhead, parsed in 367s, ~1.34 GB RSS at completion

The spool budget accounting validated under real pressure: when ndj owned a 4.69 GB CAR and
o6g was downloading at 857 MB, the total neared the 6 GiB in-flight cap and o6g correctly
paused until ndj's CAR was discarded. Fetch backpressure worked as designed — the budget bounds
concurrency, the per-repo cap bounds individual downloads, and the parse semaphore bounds
concurrent memory usage. Spool cleanup between repos kept the total smoke directory at ~1.6 GB
after each whale completed despite multi-GB CAR spools.

The 512 MiB → 4 GiB parser index cap fix is the same failure class as v1's 1 GiB CAR_MAX_BYTES:
a well-intentioned limit set too tight, silently rejecting legitimate data. The difference is
that v2 caught it during a smoke test, not after 16 repos had been silently dropped in production.
This is what "no silent caps" looks like when the caps are loud: the whale failed, the error was
obvious, and the fix was deployed before any production crawl.

A ClickHouse derive smoke also ran against the same data (464,595 emoji rows, 14 DID counters
inserted into a fresh smoke database). The derive needed schema pre-creation — the fresh smoke
database had no tables, exposing that the derive path assumes existing schema rather than
creating it. The session context exhausted before the derive completed.

**v2 structurally eliminates the loose band.** This is worth stating directly because the loose
convergence saga consumed more post-crawl time than the crawl itself. In v1, "loose" meant
ClickHouse had more posts than the ledger expected — usually because firehose inserts added
posts after the backfill loaded the repo. Reconciling this required re-fetching each loose
repo, comparing rkey sets, filtering post-load drift, handling PDS migrations and tombstones,
and all of it memory-bound by ClickHouse FINAL scans. The 631k loose DIDs from v1 needed
their own mini-infrastructure.

In v2, the MST root proof at parse time — reconstructing the MST and checking root CID against
the signed commit root — proves the rkey set is complete at the moment of fetch. If the proof
doesn't match, the repo fails loud and re-fetches immediately. There is no deferred "loose"
category. The separate `posts_backfill` table isolation means firehose inserts can't create
false positives. And the rkey digest is recomputed from Parquet (not from ClickHouse, which
only holds the emoji subset and structurally can't reproduce the full-repo digest). The
critique's #1 blocker — "emoji-only-in-CH kills v1's CH-side XOR-digest verification" — is
the reason v2 verifies from Parquet instead.

The design also includes a canary gate that goes beyond "does the pipeline pass": it requires
an **injected single-post drop** to be detected before the 8-box fan-out is approved. Verify
the verifier. v1's false-pass incident (shard2 promoted ~2M repos with zero CH classification
because the completeness guard didn't exist yet) is the reason this gate exists.

**What the Codex agent's own memory says about v1.** Mining the structured rollout summaries
and memory files on pix2 reveals what the ops agent learned through its own lens:

- **`backfill_repo_events` is lossy under ClickHouse pressure** — the agent learned this the
  hard way when ETA estimates diverged. The authoritative source is `backfill_progress`; events
  are best-effort and drop under load. This is why the dashboard lied about progress: it was
  reading the lossy signal.
- **Failed concurrency canaries are documented as "do not reuse"**: settings `5120/128/20` broke
  ClickHouse uploads entirely (frozen telemetry, socket hang up), `6144/96/16` didn't improve
  throughput. Both were tried, measured, reverted. The ETA target kept slipping — "under 4 days"
  was never durably verified before Alice called a stop.
- **Shard modulus mismatch** (code hardcoded 6 in `ledger.ts:81`, runbook documented 8 in
  `docs/backfill-runbook.md:149`) — caught by the thermo-nuclear review.
- **The Rust decision arc had three states**: assessed and deferred (Jun 14: "probably not worth
  it before the second backfill"), confirmed deferred by Alice ("i'll pass on the rust rewrite"),
  then reversed after the grill-with-docs session (Jun 15: "we are rewriting this whole fucking
  thing in rust"). The golden fixture strategy — freeze TS behavior into fixtures before porting
  — was proposed during the assessment phase and carried forward into the rewrite. The rewrite
  estimate from the assessment (5-7 focused days narrow, 8-12 broader) is tracking against the
  actual velocity (~24h from design to smoke test, with extensive parallel subagent work).
- **The agent's own documented failure modes**: launching two Claude review processes
  simultaneously (Alice: "please do not put price caps on claude ever"), planning artifacts
  treated as project truth before validation, critique mode that never switched back to
  implementation mode, and generic "large file" complaints filling review output instead of
  actionable structural findings. Alice's correction: "ignore too-long-file-that-is-responsible-
  for-5-different-things kind of issues, we are not doing big refactors like that *yet*."
- **The summary of the second-run risk**: "less about raw throughput and more about clean
  scoping, durable verification, and keeping the dashboard/telemetry truthful across multiple
  runs." This is the sentence the retro has been building toward since the first dashboard lie.

### 2026-06-16 (late) — the 58-finding review, autonomous multi-agent fix sprint, and two bugs the smoke caught

After the second smoke passed all whales, Alice asked the Codex agent for a comprehensive code
review of the Rust rewrite. It came back with 58 findings across P0/P1/P2 severity levels. Alice's
response: "how much work to fix p0/p1/p2? and also get files under 1k lines feels like we are
creating too much spaghetti code?" Then: "two things: 1. do things in parallel when possible 2.
i'm going to be afk for a few hours so be fully autonomous i'd like this done without any input
from me."

The agent dispatched four parallel subagents, each owning a slice of the fix list:

- **Subagent 1**: shared post decode module + listRecords parity + malformed core preservation
- **Subagent 2**: parser split (`parse.rs` 1637 lines → `parse/car.rs` + `parse/mst.rs` +
  `parse/record.rs`), RAII cleanup for CID verifier workers, duplicate CID counting
- **Subagent 3**: ledger split (`ledger.rs` ~2200 lines → `ledger/store.rs` + `ledger/codec.rs` +
  `ledger/tests.rs`), lease-based stale recovery, manifest sequence allocator
- **Subagent 4**: `storage_box` and `clickhouse` file splits under 1k lines, SSH helper boundary
  tightening

All four agents experienced compile-blocking interference from each other's concurrent edits — a
predictable consequence of four agents editing the same Rust crate with no build isolation. Targeted
tests passed for individual slices before cross-contamination hit. The agent merged the results
sequentially after all four returned.

Four commits landed on `v2-rust-backfill`:

- `cf84de2` — the hardening checkpoint. Stable content-addressed archive paths (replacing
  PID/timestamp names that defeated retry idempotency), idempotent receipt/sidecar promotion,
  derive validates before ClickHouse inserts, all files under 1k lines. 9,709 insertions /
  7,706 deletions across 37 files — the single largest commit in the rewrite.
- `9a3c6fa` — `tempfile` crate adoption. Custom temp-name/remove/persist code replaced with
  `NamedTempFile`/`TempPath`/`persist_noclobber`. Net LOC: 19,457 → 19,443.
- `4911795` — pipeline hardening. Incremental spool byte reservation (fixing the deadlock below),
  offset-first CAR indexing, `include!` seams replaced with real Rust modules, profile sidecar
  failures now fail loudly, Storage Box can commit from file or stream.
- `9138686` — review regression fixes. Derive memory bounded (no full payload buffering), dedupe
  token aligned with inserted row identity, visibility narrowed.

**The two bugs the smoke caught.** After the fixes landed, Alice demanded a full regression smoke.
It found two real bugs:

1. **Byte-budget deadlock.** Multiple large in-flight downloads filled the global byte budget and
   all blocked mid-stream — none could finish to release budget. The smoke stuck for ~7 minutes
   before being killed. Fix: switched from pre-reserve-full-cap to incremental per-chunk charging
   in `transport.rs`. Same failure class as the v1 spool budget — shared resource pools that
   block on full reservation instead of streaming.

2. **Derive receipt lookup drift.** Archive writes repo receipts as
   `<artifact-stem>.<receipt-hash>.receipt.json`, but derive looked for
   `<post-object-stem>.receipt.json`. The path-contract mismatch meant derive couldn't find any
   receipts. Regression test added at `manifest_derive/tests.rs`.

**Performance profiling proved the regression was environmental.** The post-fix smoke ran in 17:36
(1056s) vs the pre-fix 13:49 (829s). Rather than assume a code regression, the agent profiled
the o6g whale (3.96 GB retained CAR) in isolation:

| Configuration | Wall time | Notes |
|---------------|-----------|-------|
| Parse-only, threads=1 | 2:42 | index 36.1s, walk 125.9s |
| Parse-only, threads=8 | 1:57 | index 57.4s, walk 60.1s |
| Full archive, threads=1 | 2:34 | RSS 1.91 GB |
| Full archive, threads=8 | 2:59 | RSS 1.92 GB |

threads=1 beat threads=8 on the production archive path by 25 seconds. More threads help parse-only
(-45s) but hurt the full path due to context-switching overhead on the VPS. The 17:36 vs 13:49
difference was fleet IO/cache pressure, not a code change. ClickHouse derive remained stable at
~2:35, 499,535 emoji rows, max RSS 69 MB.

**Three more thermo-nuclear reviews.** Alice ran three independent reviews the same evening:

- **TS backfill review** found host-eligibility gaps: private-PDS blocking bypassed in exact
  recrawls, `refreshHost()` updating to unusable hosts without policy checks, IPv4-mapped IPv6
  loopback (`::ffff:127.0.0.1`) classified as public. Architecture recommendation: single
  host-admission boundary returning `PublicPdsHost | rejection`.
- **First Rust review** found two blockers: fleet budget (512 MiB) vs per-repo cap (2 GiB) means
  default settings reject default fetches; CAR parsing allocates full sections before CID handling.
  Also flagged `include!` as structural debt (already fixed in the hardening sprint).
- **Second Rust review** found derive payload buffering reintroducing whale-repo OOM risk, dedupe
  tokens omitting `run_id`/`shard`/`file_sequence` allowing replay miscounts, and
  `StorageBoxCommands::upload_reader` silently buffering instead of streaming.

A LoC reduction scan also ran: the biggest target is `packages/emoji-normalization/emoji.ts` at
68,994 generated lines — the real interface is tiny in `emojiNormalization.ts`. The TS backfill
and archive packages are deletion targets once Rust v2 reaches parity.

**The phase transition.** Alice's final question of the session: "so wahts next? are at the point
where 'spike works, we can build the backfill-scale one'?" The answer is yes. The Rust spike
proves the pipeline end-to-end — fetch, parse, MST proof, Parquet archive, receipt, ClickHouse
derive. What remains is the scale infrastructure around it: Storage Box wiring, census/seed
generation, shard workers, production observability. The rewrite estimate from the assessment
phase (5-7 focused days narrow, 8-12 broader) is tracking against actual velocity: ~30 hours from
design to validated spike, with extensive parallel subagent work.

The `include!` → real modules lesson is worth noting. The agent used `include!` to split large
files while keeping them in a shared private scope — technically reducing line counts while
preserving monolithic coupling. Alice's "feels like we are creating too much spaghetti code"
pushed the fix: real Rust modules with `pub(super)` and `pub(crate)` visibility boundaries.
The review called `include!` "an architecture smell that looks decomposed but behaves like giant
modules." The same pattern exists in v1's TS codebase — `verify.ts` at 1,458 lines owns too
many responsibilities but was never split because v1 was "runs twice and done."

### 2026-06-17 — three review rounds, an overnight autonomous sprint, and production plumbing

Between the end of the 58-finding review sprint and the start of Jun 17, three more full code
review rounds ran against the Rust rewrite — each time Alice said some variant of "fix all valid
ones" and the agent executed autonomously. The combined finding count across the three rounds was
approximately 100 distinct items spanning correctness, retry semantics, derive integrity, failure
classification, documentation drift and structural quality.

The overnight sprint happened because Alice said: "be fully autonomous i'm literally asleep do not
stop until the bugfixes are done, the big files are refactored into much smaller ones and split
properly and logically and the full smoke passes with no regressions. good luck, you got this."

The agent ran each round as: apply fixes → cargo test + clippy → full 24-DID whale smoke →
ClickHouse derive verification → record evidence in NOTES.md → next round. Six commits landed in
the Rust rewrite across this arc:

- `ee66323` — 30-finding review blocker fixes. listRecords artifacts isolated into their own
  dataset (`collection_paginated_posts`) to prevent fallback data contaminating root-proofed
  archives. Proof class renamed from `SnapshotComplete` to `ContentAddressedSnapshot` — honest
  about what the code actually verifies (CID traversal, not canonical MST root reconstruction).
  HTTP/2 made configurable (`--http-protocol http1|auto`), defaulting to HTTP/1 because each
  getRepo is one huge streaming body where multiplexing doesn't help. GitHub CI workflow added.
  Storage Box manifest append made atomic (contains + append under same lock). GIT_REV
  enforcement in release builds.
- `ba4606e` — 32-finding retry and derive hardening. New `OperatorDeferred` failure status for
  IO errors (ENOSPC, permission denied) that don't consume retry attempts and can be globally
  resumed after the machine is healthy. Byte-budget pressure separated from per-repo resource
  limits — fleet-wide budget occupancy becomes retryable backpressure, not terminal
  `ResourceLimited`. Normalizer identity promoted into ClickHouse serving rows (dedupe token,
  ORDER BY, query filters) so mixed-normalizer deployments are distinguishable. Archive encoding
  marker added to artifact stems preventing row-hash false conflicts. CID added to emoji
  projection for per-observation auditability.
- `24c9549` — structural split. All Rust source files brought under 1,000 LOC. Canary, derive
  manifest, list records and transport modules each split into implementation + test submodules.
  Transport rate-limit logic extracted to its own submodule.
- `16f9231` + `e09df37` — post-smoke evidence recorded in NOTES.md after each round.

**Performance across the review rounds:**

| Whale | Pre-review | Post ee66323 | Post 24c9549 split |
|-------|-----------|-------------|-------------------|
| o6g (3.96 GB) | 438s | 383s (-13%) | — |
| 4hm | 136s | 83s (-39%) | 87s |
| lb7 | 79s | 70s (-12%) | 93s (noise) |
| f4z | — | 183s | 188-193s |
| Fleet wall | — | 17:47 | 19:59 |
| Fleet max RSS | — | 2.74 GiB | 3.25 GiB |
| Derive | — | 2:39 / 72 MB | 2:53 / 71 MB |

The 4hm improvement (136s → 83s, -39%) was the CID verifier thread count moving from 1 to 4.
Fleet wall time regression (17:47 → 19:59) was network-dominated — longer queue times for
same-host jobs after earlier jobs claimed the rate-limit slots. Max RSS stayed well under the
64 GB crawler target.

**Two TS v1 commits also landed.** `fa06b11` added archive-only recrawl mode with a new
`pds-host-policy.ts` module (141 lines) — the host-eligibility boundary the TS thermo-nuclear
review recommended. `15ff29d` hardened aggregate ClickHouse rebuilds against OOM from unbounded
swap. v1 isn't dead yet while v2 matures.

**The review bundle incident.** Alice asked for a source zip for separate review. Commit `b235a9d`
shipped a 17.5 MB zip because `fixtures/*.car` binary test fixtures weren't excluded. Alice caught
it immediately ("this file is 17.5mb?"). Fixed in `83a8213` with a slim 243 KB replacement. Same
class of mistake as v1's accidental binary commits — generated artifacts in version control.

**Jun 17: named parallel subagents.** The parent session dispatched three named workers in
parallel, each on a bounded fix scope:

- **Helmholtz** — canary CLI command. New `canary_cmd.rs` (154 lines) + tests (157 lines). Reads
  JSON/JSONL evidence files, evaluates canary launch gates, prints JSON report, exits nonzero on
  failure. Committed as part of `497ec73`.
- **Bernoulli** — ClickHouse/derive serving identity. Propagated post CID through emoji projection
  into serving schema and ORDER BY. Added 8 MiB byte-size-aware payload chunking for streaming
  emoji inserts. Configured per-request timeouts. 128 tests passed.
- **Leibniz** — failure classification and documentation drift. Reclassified commit/Storage Box
  integrity conflicts as permanent (not retryable), transient commit IO as retryable, and
  oversized error bodies as `OperatorDeferred`. Rewrote the runbook's stale Bun/modulus-6 wording
  to match Rust/8-bucket reality.

All three hit compile interference from each other's concurrent uncommitted edits — the same
pattern as the prior 4-subagent sprint. Each correctly scoped verification to library-only tests
with dead-code suppression (`RUSTFLAGS='-A dead_code'`) and noted the limitation. The parent
session merged results and committed `497ec73` (Storage Box backend wiring + canary gate, 812
insertions across 16 files). Bernoulli and Leibniz's work remained uncommitted as of the last
session activity.

**The Storage Box decision.** The agent explicitly stated: "I'm not going to fake Storage Box
production readiness." Local archive path was hardened first; Storage Box documented as gated
until a canary/self-test path existed. Commit `497ec73` wired it as opt-in
`--archive-backend storage-box-ssh` with mirror commit semantics — local archive remains the
default, remote is additive. This matches the v2 design's "Parquet on Storage Box is the raw
archive, ClickHouse is derived" but doesn't pretend the remote path is battle-tested.

**Where things stand.** When Alice asked "how far are we?", the agent assessed ~40% complete. The
crawler core — fetch, parse, MST proof, Parquet archive, receipt, ClickHouse derive — is viable
and smoke-tested. What remains is distributed system architecture: census/seed generation from
PLC mirror, cross-box host pacing, shard workers, Jetstream catch-up spooler, production
observability (OTEL + Prometheus, which Alice asked about — "wait whats the difference between
otel and prometheus actually"), and the canary protocol before 8-box fan-out.

### 2026-06-17 (midday) — the review gauntlet, schema v2, and still not approved

Three more code review rounds ran against the Rust rewrite between the overnight sprint and
midday Jun 17. Each followed the same cycle: static review → autonomous fix sprint (sometimes
with parallel subagent pairs) → cargo test + clippy → full 24-DID whale smoke → ClickHouse
derive verification → zip evidence for Alice.

**Round 1 (30 findings).** Blockers: in-flight byte budget deadlock on concurrent unknown-size
downloads, Storage Box exposing local manifest before remote object commits, listRecords
pagination not paced through the shared per-host scheduler. All three fixed. Unknown-size
downloads now reserve the full per-repo cap up front. Storage Box manifest exposure deferred
to after remote commit. listRecords wired through shared host pacing. Parallel subagent workers
split the fix list: one handled derive/manifest, the other listRecords/scheduler. Both hit the
now-familiar cross-module compile barrier from concurrent edits but respected their ownership
boundaries.

**Round 2 (24 findings).** Focus shifted to proof-boundary drift and serving-table semantics. Key
fix: CID removed from ClickHouse serving ORDER BY — the review correctly identified it would
cause double-counting on re-fetch, contradicting the design doc. Proof metadata (dataset,
proof class) added to derived rows and counters so `collection_paginated_posts` (listRecords
fallback data) can't silently mix with root-proofed `raw_archive_posts`. Storage Box now uploads
the repo receipt alongside the archive, making remote derive self-sufficient.

**Round 3 (19 findings).** Alice asked: "how much of the previous rounds' findings were nit vs
real?" Agent estimated 70-80% real, 20-30% nit. Alice set the threshold: "once reviews get into
at least 50-60% nit territory we can continue with arch." This round was still above that bar.
Key fixes: streaming derive proof validation, archive schema bumped to v2, ClickHouse tables
rotated to `_r2`, DID added to counters/receipts/manifests, receipt-hash-scoped object receipts.
Alice on backwards compatibility: "given that this is a de-facto blank slate we do not give any
fucks about backwards compatibility."

**Smoke results held stable across all three rounds:** 24 claimed / 19 succeeded / 5 expected
failures. Derive consistently produced ~499,600 emoji rows from ~16.7M archived posts. Max RSS
peaked at 3.05 GiB — still well under the 64 GB crawler target. Derive wall time stable at
~2:56, RSS ~72 MB.

**Crate cleanup.** Alice asked about reinventing-the-wheel opportunities. Three crate adoptions
landed in `787eb49`: `fs4` for manifest file locking (replacing custom flock), `shell-words` for
SSH argument quoting (replacing hand-rolled escaping), `rusqlite_migration` for SQLite schema
versioning (replacing ad-hoc CREATE TABLE checks). The official `clickhouse` crate was evaluated
but deferred — viable for later but not a drop-in replacement yet.

**The latest thermo-nuclear review (Jun 17 13:29) still did not approve.** Two blockers remain:

1. `fetch_attempt.rs` at 1,096 lines is the new orchestration sink — runtime mode selection,
   host override persistence, retries, fallback, parse/archive spawning, telemetry, DTOs all
   crammed in. The earlier `main.rs` split helped but just moved the complexity.
2. Run identity/dedupe key conflict — ClickHouse stores run identity columns but excludes them
   from the table replacement key; derive omits run identity from dedupe tokens. Content
   idempotence and run-scoped publication are mixed in the same token boundary.

Three major findings on top: storage backend branching leaking through the archive writer,
two divergent derive paths (the streaming path added by subagent work alongside the original
full-load path, which may now be vestigial), and commit pipeline duplication in Storage Box.

**Rust codebase at 25,082 LOC** (18,789 non-test, 6,293 test) across 52 files. Two files
remain over 1,000 lines: `fetch_attempt.rs` (1,096) and `storage_box.rs` (1,084). Both
commits pushed to origin: `097020d` (proof and derive hardening, 2,324 insertions across
40 files) and `787eb49` (crate plumbing, 251 insertions across 9 files).

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
  rsync-cwd bugs (2×), `pkill -f` self-match (3× and counting).
- **Operational re-derivation:** the agent composes multi-parameter SSH launch commands
  from scratch every time instead of calling a script. A simple concurrency change
  (512→384) produced three cascading mistakes in five minutes: `pkill` self-match (again),
  wrong env file path (`emojistats-env` vs `emojistats-crawl-env`), and missing
  `CRAWL_SHARD_INDEX` / `CRAWL_SHARDS`. Each fix introduced the next bug. A human who'd
  done this once would have a launch script; the agent has no persistent operational
  memory between commands and re-derives (and re-breaks) the same invocation every time.
  Code reviews can't catch this because the bugs are in the *execution*, not the *code*.
  The fix is the same as write-time receipts: don't trust the operator to remember — make
  the system enforce it. Operations that are repeated more than once belong in a script,
  not in an agent's ad-hoc SSH composition.
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

## If I did this again — the second-time playbook

The honest thesis of the whole run: **the data was never the hard part.** Roughly a day
and a half of *healthy* throughput moved ~2.6B posts; the calendar cost was several days,
and the overwhelming majority of the hard hours went to two things — telling a *stuck*
system from a *slow* one, and discovering, reactively, the caps and detectors we should
have built up front. A second run isn't faster because the crawler is faster. It's faster
because you stop fighting your own instrumentation. Below, ranked by the time it would
actually save. (The verify/cutover items are still provisional — those phases were unfinished
when this was written.)

**The five that would save the most time, in order:**

1. **Build the progress-gated watchdog first.** This is the spine of the entire run — the
   liveness detector went alert-only → auto-restart → CPU-gated → log-freshness →
   progress-gated, and *nearly every crawler incident was a referendum on which signal tells
   the truth*. A 0-CPU hang, an exit-1 crash, and a chatty-but-wedged box look identical from
   the outside; CPU and log-freshness both lie (the wedge kept logging stats while `loaded`
   sat frozen). Define liveness as **work advancing** (`loaded`/`resolved` climbing) from day
   one and most of the firefight never happens.

2. **Parse rate-limit headers and pace proactively — from line one.** The single biggest tail
   bottleneck (and the thing that ate a whole overnight) was reactive AIMD back-off: slam a
   host until it 429s, halve, cool, repeat — so you only ever *learn* the limit by tripping it.
   The mushroom hosts advertise `ratelimit-limit/remaining/reset` on **every** response; the
   crawler read it on none. Read the headers, space to the advertised budget, and from the
   start keep **"is this host claimable" separate from "should this request wait"** — conflating
   pacing with host-deadness in the claim scan is exactly what starved the scheduler in the tail.

3. **No silent caps. Ever.** The worst bug of the run — every repo over a 1 GiB CAR cap was
   *wholly dropped*, posts and all, for the entire backfill, filed under a "quarantined" status
   nobody was reading — was a cap doing its job silently. Every limit that can reject data (CAR
   size, fetch timeout, attempt budget, claim-scan depth) must be **loud, generous by default,
   and paired with something that re-examines what it rejected.** And raising one cap reveals the
   next (the 1 GiB cap hid a 300 s fetch-timeout cap behind it) — so audit for the *pattern*, not
   the instance. A rejection status that no process reads again is data loss with a friendly label.

4. **Design verification into the write path, and make it scale, before you need it.** Verify
   was still failing to run at the very end (ClickHouse parameter-length limits on inline DID
   lists), and even when it runs, an O(1) `(count, XOR-digest)` receipt *cannot prove set-subset*
   once a live firehose keeps growing the table. Decide what "verified" means on day one; write
   the per-repo receipts at load time; build the convergence loop (re-fetch only the LOOSE tail)
   and the *scaling* verify (a temp DID table joined per shard, never DID lists shipped as query
   params) before the end-game, not during it.

5. **Classify the spam / PLC-only tail early and delete that work.** A large share of the
   "failed/unresolved" millions is not lost data — it's `listRepos` returning DIDs that PLC knows
   about but the host doesn't actually serve, plus bulk spam DIDs. `listrepos-diff` classification
   cleared *more* backlog in a night than crawling did. The tail is **politeness-bound, not
   capacity-bound**, so the lever is deleting work, not adding boxes — measure `GROUP BY pds_host`
   on the remaining pending before you provision anything.

**Build-in-from-day-one checklist** (the cheap insurance):

- progress-gated watchdog + per-shard freshness, not CPU/log-liveness
- header-driven host pacing, with the claimable-vs-should-wait split
- a dead-host registry **with its inverse** (`--revive-host`) shipped together — blacklisting
  without an un-blacklist means "park" silently becomes "abandon"
- generous, loud caps + a re-examination pass over anything rejected
- write-time verify receipts + a verify that scales; and put `src`/source **in** the
  `ReplacingMergeTree` sort key (or split backfill and live into separate tables) so
  verification can actually isolate backfilled rows — the `(did, rkey)`-with-`src`-outside
  sort key is precisely what made the digest unable to prove zero-loss
- everything load-bearing in the nix flake / IaC; no ad-hoc host scripts or `/run`-only drop-ins
  that the next rebuild silently erases
- **scriptable fleet operations**: a `scripts/fleet-crawl.sh` (or equivalent) that takes
  concurrency, shard index, run ID, and DID file as arguments, handles env file sourcing, shard
  labels, process management (kill-without-self-match), and health verification. The agent (or
  human) calls one command instead of composing 6-parameter SSH invocations from memory — the
  same principle as write-time receipts: don't trust the operator to remember, make the system
  enforce it. Every operational mistake in the late-night sessions (wrong env path, missing shard
  index, pkill self-match) would have been prevented by a script that encodes the right parameters
  once
- the dashboard scoped to **project lifetime, not the latest `run_id`**, and its numbers
  data-backed (aggregate tables), never hardcoded and never raw-`posts` scans at live cadence
- quote an ETA only from **sustained measured throughput on healthy software**, and report
  posts/min next to repos/min so "stopped wasting" and "got faster" stay distinguishable

**Keep doing (these earned their place):**

- the adversarial **second-agent review rounds** — they caught real pre-launch defects that would
  have been 3am outages (the seq-reuse parquet overwrite, a dead ExecStart)
- the core architecture — sharded crawlers → ClickHouse + per-repo SQLite ledger + zstd-parquet
  archive — largely held under 2.6B posts; the changes above are refinements, not a redesign
- naming bottlenecks out loud, the ETA honesty table, atomic commits, the runbook of
  settings-not-to-repeat
- the human's short skeptical questions as the highest-leverage tool in the kit — *"how were you
  off by almost an order of magnitude?"*, *"are we not also missing posts?"*, *"they literally
  give you headers"* each turned a stuck path

**What would still be slow (the irreducible floor):** you cannot crawl faster than the hosts
permit, dead hosts and PLC/host disagreement never become posts, and multi-GB repos take real
time even with the caps raised. So the honest second-time estimate is that the *data* moves in
roughly a day or two of wall-clock — the savings aren't in the crawl, they're in the days you'd
no longer lose to instrumentation you built right the first time.

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
