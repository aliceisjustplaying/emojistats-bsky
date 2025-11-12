# Nexus Migration Plan (Atomic Steps)

## Status (2025-11-12)

**Implementation Complete**: Steps 3-8 and Step 13 are complete. The unified-ingest package is production-ready with:

- ✅ Nexus and Jetstream adapters
- ✅ Batched writes with flush promise system (maintains performance, ensures durability)
- ✅ Error resilience (flush promise always resolves, preventing pipeline hangs)
- ✅ Concurrent acks (prevents slow Redis from blocking Nexus)
- ✅ Non-emoji event acks (prevents stalling)
- ✅ Repo validation and progress tracking
- ✅ Comprehensive Prometheus metrics

**Next Steps**: Step 9 (multi-repo testing) → Step 10-11 (production deployment)

## Philosophy

Break this into tiny, testable steps. Each step should be completable in <1 hour and produce a verifiable result. Start with just running Nexus, then build incrementally.

## Step 1: Run Nexus Locally (30 minutes)

**Goal**: Get Nexus running on your laptop and verify it works.

1. Navigate to Nexus source: `cd ~/src/a/indigo/cmd/nexus`
2. Build Nexus: `go build -o nexus .`
3. Create a test directory: `mkdir -p /tmp/nexus-test && cd /tmp/nexus-test`
4. Run Nexus in manual mode:
   ```bash
   ./nexus \
     --database-url=sqlite://./nexus.db \
     --relay-url=https://relay1.us-east.bsky.network \
     --bind=:8080 \
     --disable-acks=true \
     --collection-filters=app.bsky.feed.post
   ```
5. In another terminal, test the health endpoint:
   ```bash
   curl http://localhost:8080/health
   ```
   Should return: `{"status":"ok"}`
6. Connect to WebSocket to see events (will be empty initially):
   ```bash
   websocat ws://localhost:8080/channel
   ```
   Leave this running.

**Success criteria**: Nexus starts without errors, health check works, WebSocket connects.

---

## Step 2: Add a Test Repo to Nexus (15 minutes)

**Goal**: Manually add a DID and watch Nexus backfill it.

1. With Nexus still running, add a test repo (use @bsky.app or your own DID):
   ```bash
   curl -X POST http://localhost:8080/add-repos \
     -H "Content-Type: application/json" \
     -d '{"dids": ["did:plc:z72i7hdynmk6r22z27h6tvur"]}'
   ```
2. Watch the WebSocket terminal - you should see JSON events streaming in.
3. Events will have `"live": false` during backfill, then `"live": true` for live events.
4. Check Nexus logs - should show backfill progress.

**Success criteria**: Events appear in WebSocket, backfill completes, live events start.

---

## Step 3: Parse Nexus Events in TypeScript (1 hour)

**Goal**: Write a tiny script that connects to Nexus WebSocket and logs events.

1. Create `packages/unified-ingest/` directory:

   ```bash
   mkdir -p packages/unified-ingest/src
   cd packages/unified-ingest
   ```

2. Initialize package:

   ```bash
   bun init
   ```

3. Install dependencies:

   ```bash
   bun add ws
   bun add -d @types/ws typescript
   ```

4. Create `src/test-nexus.ts`:

   ```typescript
   import WebSocket from "ws";

   const ws = new WebSocket("ws://localhost:8080/channel");

   ws.on("open", () => {
     console.log("Connected to Nexus");
   });

   ws.on("message", (data: Buffer) => {
     const event = JSON.parse(data.toString());
     if (
       event.type === "record" &&
       event.record?.collection === "app.bsky.feed.post"
     ) {
       console.log("Post event:", {
         id: event.id,
         did: event.record.did,
         rkey: event.record.rkey,
         live: event.record.live,
       });
     }
   });

   ws.on("error", (err) => {
     console.error("WebSocket error:", err);
   });
   ```

5. Run it: `bun run src/test-nexus.ts`

**Success criteria**: Script connects, receives events, logs post events correctly.

---

## Step 4: Send Acks Back to Nexus (30 minutes)

**Goal**: Acknowledge events so Nexus knows we processed them.

1. Update `src/test-nexus.ts` to send acks:

   ```typescript
   ws.on("message", (data: Buffer) => {
     const event = JSON.parse(data.toString());
     if (
       event.type === "record" &&
       event.record?.collection === "app.bsky.feed.post"
     ) {
       console.log("Post event:", event.id);

       // Send ack
       ws.send(JSON.stringify({ id: event.id }));
     }
   });
   ```

2. Restart Nexus with acks enabled (remove `--disable-acks`):

   ```bash
   ./nexus --database-url=sqlite://./nexus.db --relay-url=https://relay1.us-east.bsky.network --bind=:8080 --collection-filters=app.bsky.feed.post
   ```

3. Run the test script again - should still receive events and send acks.

**Success criteria**: Acks are sent, Nexus doesn't retry events, no errors.

---

## Step 5: Map Nexus Events to Our Format (1 hour)

**Goal**: Convert Nexus events to the same format our normalizer expects.

1. Look at `packages/live-ingest/src/normalizer.ts` - see what `NormalizedPost` looks like.
2. Create `src/adapters/nexus.ts`:

   ```typescript
   import WebSocket from "ws";
   import type { NormalizedPost } from "../types";

   interface NexusEvent {
     id: number;
     type: "record" | "user";
     record?: {
       did: string;
       collection: string;
       rkey: string;
       action: string;
       record: any;
       live: boolean;
     };
   }

   export function mapNexusEvent(event: NexusEvent): NormalizedPost | null {
     if (
       event.type !== "record" ||
       event.record?.collection !== "app.bsky.feed.post"
     ) {
       return null;
     }

     const rec = event.record.record;
     // Map fields to NormalizedPost format
     // (extract text, createdAt, etc.)
   }
   ```

3. Test mapping with a few sample events.

**Success criteria**: Nexus events correctly map to `NormalizedPost` format.

---

## Step 6: Write Events to Timescale (1 hour)

**Goal**: Use existing writer code to insert events into Timescale.

1. Copy `packages/backfill/backfill/writer.ts` to `packages/unified-ingest/src/writer.ts`
2. Copy `packages/backfill/backfill/db.ts` to `packages/unified-ingest/src/db.ts`
3. Copy `packages/backfill/backfill/dimensions.ts` to `packages/unified-ingest/src/dimensions.ts`
4. Update `src/test-nexus.ts` to:
   - Parse Nexus events
   - Map to `NormalizedPost`
   - Write to Timescale using `EmojiPostWriter`
   - Send ack after successful write

**Success criteria**: Events appear in `emoji_post` table, counts match events received.

---

## Step 7: Add Parquet Writing (30 minutes)

**Goal**: Also write events to Parquet files like the old pipeline.

1. Copy `packages/backfill/backfill/parquetSink.ts` to `packages/unified-ingest/src/parquetSink.ts`
2. Update writer to also write to Parquet
3. Verify Parquet file is created and has correct rows

**Success criteria**: Parquet file exists, row count matches Timescale inserts.

---

## Step 8: Handle Reconnection (30 minutes)

**Goal**: Make the Nexus adapter resilient to disconnects.

1. Add exponential backoff reconnection logic
2. Test by killing Nexus and restarting it
3. Verify events resume without duplicates

**Success criteria**: Reconnects automatically, no duplicate events, no crashes.

---

## Step 9: Test with Multiple Repos (1 hour)

**Goal**: Verify the pipeline works with a small set of repos.

1. Add 10-20 DIDs to Nexus via `/add-repos`
2. Let backfill complete
3. Compare Parquet + Timescale counts
4. Check validation logs

**Success criteria**: All repos processed, counts match, no validation errors.

---

## Step 10: Deploy Nexus on Hetzner (1 hour)

**Goal**: Get Nexus running on the production server.

1. Build Nexus binary (or copy from local)
2. Create systemd service file
3. Configure with `NEXUS_FULL_NETWORK_MODE=true`
4. Start service, verify health check

**Success criteria**: Nexus runs on Hetzner, health check works, logs look good.

---

## Step 11: Deploy Unified Worker on Hetzner (1 hour)

**Goal**: Run the unified ingest worker on Hetzner, connected to Nexus.

1. Copy `packages/unified-ingest` to Hetzner
2. Configure environment variables (DB URL, etc.)
3. Run as systemd service
4. Monitor logs and metrics

**Success criteria**: Worker connects to Nexus, processes events, writes to Timescale.

---

## Step 12: Monitor Full Backfill (Days/Weeks)

**Goal**: Let Nexus enumerate and backfill the entire network.

1. Monitor Nexus logs for enumeration progress
2. Monitor worker metrics (events/sec, lag, etc.)
3. Periodically validate counts
4. Wait for completion

**Success criteria**: All repos enumerated, backfill completes, validation passes.

---

## Step 13: Add Jetstream Adapter ✅ COMPLETE

**Goal**: Support consuming from Jetstream as an alternative source.

**Status**: Already implemented in `packages/unified-ingest/src/adapters/jetstream.ts`

- Jetstream adapter maps events to `UnifiedEvent` format
- Cursor persistence via Redis (`JETSTREAM_CURSOR_KEY`)
- Supports cursor override for testing
- Integrated into main worker via `INGEST_SOURCE=jetstream` config

**Success criteria**: ✅ Can consume from Jetstream, events write correctly.

---

## Step 14: Cutover to Jetstream (30 minutes)

**Goal**: Switch from Nexus to Jetstream for live events.

1. Stop Nexus service
2. Update unified worker config: `INGEST_SOURCE=jetstream`
3. Restart worker
4. Monitor for gaps or issues

**Success criteria**: Worker switches to Jetstream, no gaps in data, live events continue.

---

## Step 15: Cleanup (30 minutes)

**Goal**: Archive old code and update documentation.

1. Move `packages/backfill` to `packages/_archived/backfill`
2. Move `packages/live-ingest` to `packages/_archived/live-ingest`
3. Update `AGENTS.md` with new architecture
4. Document the migration

**Success criteria**: Old code archived, docs updated, everything still works.

---

## Notes

- Each step builds on the previous one
- You can stop and test at any step
- Steps 1-9 are all local development
- Steps 10-12 are production deployment
- Steps 13-15 are final migration

## Key Files to Create

- `packages/unified-ingest/src/main.ts` - Entry point
- `packages/unified-ingest/src/adapters/nexus.ts` - Nexus WebSocket client
- `packages/unified-ingest/src/adapters/jetstream.ts` - Jetstream adapter
- `packages/unified-ingest/src/adapters/types.ts` - UnifiedEvent interface
- `packages/unified-ingest/src/normalizer.ts` - Event normalization
- `packages/unified-ingest/src/writer.ts` - Timescale + Parquet writer (reused)
- `packages/unified-ingest/src/db.ts` - DB operations (reused)
- `packages/unified-ingest/src/dimensions.ts` - Dimension cache (reused)
- `packages/unified-ingest/src/parquetSink.ts` - Parquet writing (reused)

## Configuration

Environment variables for unified worker:

- `INGEST_SOURCE` - `nexus` | `jetstream`
- `NEXUS_URL` - WebSocket URL (default: `ws://localhost:8080/channel`)
- `JETSTREAM_ENDPOINT` - Jetstream URL
- `EMOJISTATS_DATABASE_URL` - Timescale connection string
- `EMOJI_BACKFILL_PARQUET_DIR` - Parquet output directory
- `EMOJI_MAX_PER_POST` - Max emojis per post (default: 250)
