# Nexus Rewrite Plan

Goal: replace our custom Bun-based backfill + live ingest pipeline with Bluesky’s Go-based `nexus` service for the historical ingest phase, then retire Nexus and consume Jetstream directly once the backfill is complete.

## Guiding Principles

- Nexus owns enumeration, CAR ingestion, verification, backfill ordering, and live buffering during the historical ingest phase.
- We keep the current Timescale/Parquet schema + validation pipeline because we still need time-series analytics, aggregate refresh jobs, and historical snapshots for Emojistats. Nexus delivers events; it does not replace our storage/analytics layer, so there’s no simplification to pursue right now beyond removing the Bun backfill.
- This is a one-way migration: once we validate the Nexus-driven backfill and cut over to Jetstream, we do **not** plan to fall back to the old Bun pipeline.
- End state: Jetstream supplies all live data; Nexus is decommissioned once the historical ingest is fully verified.

## Workstream 1: Nexus Deployment (Backfill Phase)

1. **Retire the Bun pipeline up front**
   - Freeze the existing Bun/Redis backfill code in git, remove its systemd units/scripts from hosts, and scrub the old env vars/Redis queues. Reference code remains in history; no dual-running.
2. **Build + package nexus**
   - Use the existing `cmd/nexus` codebase (`~/src/a/indigo`) to produce a binary or container for Hetzner.
   - Document build steps (Go version, env vars) so ops can reproduce.
3. **Storage**
   - Run Nexus with its default SQLite DB (`./nexus.db`) on reliable disk. Ensure filesystem snapshots/backups cover the DB file.
4. **Configuration**
   - Set `NEXUS_DATABASE_URL`, `NEXUS_RELAY_URL`, `NEXUS_COLLECTION_FILTERS` (e.g., `app.bsky.feed.post`), `NEXUS_BIND`, `NEXUS_FIREHOSE_PARALLELISM`, `NEXUS_RESYNC_PARALLELISM`, etc.
   - Start in dynamic mode: manually add DIDs via `/add-repos` while testing, then switch to full-network or collection-signal mode once confident.
5. **Ops hooks**
   - Ship logs/metrics (OTEL) into our monitoring stack.
   - Back up the Nexus SQLite DB file (daily snapshots) to protect repo state/outbox buffers.
6. **Service management**
   - Create systemd service for Nexus with restart policy, health check `/health`, and firewall rules restricting access to our internal network.

## Workstream 2: Unified Ingest Worker (Nexus ↔ Jetstream → Timescale/Parquet)

1. **Process layout**
   - Build a single TypeScript/Bun service that can pull from Nexus **or** Jetstream behind a common interface (transport adapters). This replaces both the old Bun backfill scripts and any separate Jetstream consumer—we maintain one codebase.
2. **Nexus adapter**
   - Connect to `ws://nexus:8080/channel` (WebSocket with acks). Flags for URL, ack timeout, reconnect policy. Resume automatically after restarts.
3. **Jetstream adapter**
   - Connect to Jetstream (default relay, all DIDs/collections) and locally filter to `app.bsky.feed.post` (and others as needed). Support cursors/resume semantics.
4. **Event pipeline**
   - Normalize events using existing emoji parsing logic, preserving the `live` flag from Nexus and tagging Jetstream events as live.
   - Continue writing both Timescale rows **and** Parquet snapshot files so validation + audits remain unchanged.
5. **Ack + retry semantics**
   - For Nexus: ack only after Timescale insert + Parquet flush. For Jetstream: commit cursor only after the same write path succeeds.
6. **Resilience hooks**
   - Reconnect/backoff with jitter, metrics for ingest lag/failure, and crash-safe batching so we can reprocess from the last successful cursor if a hard reset occurs.

## Workstream 3: Migration Timeline

1. **Pilot / validation**
   - Run Nexus + bridge locally with a small DID allowlist and compare Timescale row counts vs the Bun pipeline to prove parity.
2. **Hetzner deployment (backfill phase)**
   - Deploy Nexus + bridge on the Hetzner box.
   - Enumerate all DIDs (full-network or targeted collections) and let Nexus backfill while the bridge loads Timescale.
   - Continuously validate aggregated counts, Parquet snapshots, and Timescale totals to ensure data matches expectations.
3. **Cutover to Jetstream**
   - Once the Nexus-driven backfill is complete and validated, **stop Nexus**.
   - The ingest worker already understands both Nexus and Jetstream; flip it into Jetstream mode (see Workstream 4).
   - Run Jetstream + Nexus in parallel briefly (Jetstream for live, Nexus finishing stragglers) to confirm no gaps, then fully disable Nexus and delete its data once final validation passes.

## Workstream 4: Jetstream Consumer

1. **Client implementation**
   - Jetstream already emits _all_ DIDs/collections by default; we subscribe once, then locally filter to `app.bsky.feed.post` (or any other collection) while leaving DID scope unlimited.
   - Reuse the same ingest worker, swapping in Jetstream adapters for transport/cursor handling.
2. **Ordering/validation**
   - Jetstream doesn’t provide Nexus’s per-repo historical ordering. Because the backfill is already finished, we only consume live events, so this is acceptable.
   - Continue running Timescale validation/checks to detect any anomalies introduced after the cutover.
3. **Monitoring**
   - Track Jetstream lag, cursor positions, and reconnect counts.
   - Ensure existing dashboards reflect the new data source.

## Resilience Requirements

- Every service (Nexus, ingest worker, Timescale writers) runs under systemd with `Restart=always`, OOM notices wired to alerts, and watchdog timers.
- Persistent state (Nexus SQLite DB, Timescale, Parquet staging) sits on journaled storage; document exact recovery steps after a hard reboot.
- Implement crash-loop detection: if systemd restarts a unit >N times in M minutes, fire a PagerDuty alert and capture logs/core dumps automatically.
- Ensure the ingest worker can resume from last acknowledged event after power loss (Nexus WebSocket + Jetstream cursors already support this; verify and test).
- Run chaos tests: kill -9 services mid-backfill, power-cycle the Hetzner host, confirm end-to-end recovery with no data loss.

## Open Questions / Follow-ups

- **Backfill runtime:** budget 7–10 days for Nexus to enumerate + backfill the entire network (Futur’s 3-day run x ~3 to stay conservative). Size storage/monitoring for that window.
- **Bridge vs integrated ingest:** initial plan merges everything into one worker. If we later need separate processes (e.g., for scaling/permissions), outline criteria for splitting.
- **Jetstream validation:** after cutover, keep a short-lived Nexus shadow (read-only) to spot-check Jetstream data until we’re 100% confident, then delete it.

## Deliverables

1. Nexus deployment manifests + documentation (including resilience settings).
2. Unified ingest worker supporting both Nexus (backfill) and Jetstream (live) modes.
3. Validation report showing Nexus backfill parity and successful Jetstream cutover (with ~week-long runtime captured).
4. Updated AGENTS/runbooks describing the staged migration, resilience expectations, and final architecture.
5. Follow-up tickets for schema simplification, Jetstream failover drills, and any future ingest optimizations.
