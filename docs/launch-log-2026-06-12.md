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
