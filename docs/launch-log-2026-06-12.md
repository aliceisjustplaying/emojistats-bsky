# Backfill fleet launch log — night of 2026-06-11 → 12

Raw notes for the eventual writeup. Times are local (CEST). The goal: stand up
one serving box + six crawl boxes from bare Hetzner hardware to a running
full-network Bluesky backfill, overnight, with correctness guarantees built in.

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

## 02:15 — IT HUMS (first time, briefly)

- emoji: live ingest reconnected to Jetstream, dashboard public behind Caddy
  TLS at backfill.mosphere.at, parquet shipping to the Storage Box live/.
- ~20 minutes after first enumerate page: **522k backfill posts from 558
  repos in ClickHouse**, all six shards loading in parallel, enumeration
  still paging the PLC directory at 25ms/page (~45M DIDs to go).
- Zero-touch first boot proven on crawl0: pre-baked age key → sops decrypts →
  tailscale auto-enrolls with the right tag → emoji pin resolves. No hands.
