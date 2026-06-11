# Implementation Plan: ClickHouse-Only Architecture with Tap Backfill

## Summary

Replace the current Postgres + Redis + BullMQ architecture with:
- **ClickHouse** as the only database (source of truth AND serving layer)
- **SQLite** for local dedupe (prevents double-counting)
- **In-process cache** in API server (1s TTL, replaces Redis)

Three processes total (only 2 after backfill):
- **Ingest Worker** (`packages/ingest`) — consumes Tap/Jetstream, writes ClickHouse
- **API Server** (`packages/backend`) — queries ClickHouse, emits via Socket.IO
- **Tap** (external Go binary) — only during backfill, then permanently stopped

---

## Architecture Overview

```
┌─────────────┐     ┌─────────────┐
│    Tap      │     │  Jetstream  │
│ (backfill)  │     │   (live)    │
└──────┬──────┘     └──────┬──────┘
       │                   │
       └─────────┬─────────┘
                 ▼
       ┌─────────────────────┐
       │   Ingest Worker     │
       │  (packages/ingest)  │
       │                     │
       │ - SQLite dedupe     │
       │ - In-memory agg     │
       │ - File cursor       │
       └────────┬────────────┘
                │
                ▼
       ┌─────────────────────┐
       │     ClickHouse      │
       │   (only database)   │
       └────────┬────────────┘
                │
                ▼
       ┌─────────────────────┐
       │    API Server       │
       │ - ClickHouse queries│
       │ - 1s in-process cache│
       │ - Socket.IO emit    │
       └─────────────────────┘
```

**No Redis.** ClickHouse serves queries directly with in-process caching.

---

## Phase 1: Create `packages/ingest` Package

```
packages/ingest/
├── package.json
├── tsconfig.json
├── src/
│   ├── index.ts              # Main entrypoint
│   ├── config.ts             # Environment config
│   ├── clickhouse/
│   │   ├── client.ts         # ClickHouse client wrapper
│   │   └── schema.sql        # DDL for tables
│   ├── adapters/
│   │   ├── types.ts          # PostCreate, EventAdapter interfaces
│   │   ├── jetstream.ts      # Jetstream adapter
│   │   └── tap.ts            # Tap adapter with ack handling
│   ├── aggregator.ts         # In-memory delta maps + flush
│   ├── dedupe.ts             # SQLite dedupe store
│   ├── cursor.ts             # File-based cursor persistence
│   └── normalizer.ts         # Emoji extraction (reuses emoji-normalization)
└── data/                     # Runtime (gitignored)
    ├── dedupe.sqlite
    └── cursor.txt
```

Dependencies:
- `@clickhouse/client`
- `@skyware/jetstream`
- `@atproto/tap`
- `better-sqlite3`
- `emoji-regex`
- `emoji-normalization` (workspace)
- `pino`

---

## Phase 2: ClickHouse Schema

**`src/clickhouse/schema.sql`** — 5 tables:

```sql
-- 1. Global emoji totals (for top emojis list)
CREATE TABLE IF NOT EXISTS emoji_total_global (
  emoji LowCardinality(String),
  cnt   UInt64
) ENGINE = SummingMergeTree ORDER BY emoji;

-- 2. Per-language emoji totals (for language tabs)
CREATE TABLE IF NOT EXISTS emoji_total_by_lang (
  lang  LowCardinality(String),
  emoji LowCardinality(String),
  cnt   UInt64
) ENGINE = SummingMergeTree ORDER BY (lang, emoji);

-- 3. Language totals (total emoji occurrences per language)
CREATE TABLE IF NOT EXISTS lang_total (
  lang LowCardinality(String),
  cnt  UInt64
) ENGINE = SummingMergeTree ORDER BY lang;

-- 4. Global counters (processedPosts, postsWithEmojis, etc.)
CREATE TABLE IF NOT EXISTS metrics_total (
  metric LowCardinality(String),
  value  UInt64
) ENGINE = SummingMergeTree ORDER BY metric;

-- 5. Hourly global history (for time-series charts)
CREATE TABLE IF NOT EXISTS emoji_hourly_global (
  hour  DateTime('UTC'),
  emoji LowCardinality(String),
  cnt   UInt64
) ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, emoji);
```

**Query pattern**: Always use `sum(cnt)` + `GROUP BY` (don't rely on merges having settled).

---

## Phase 3: SQLite Dedupe Store

**`src/dedupe.ts`**:

```typescript
class DedupeStore {
  constructor(dbPath: string)

  // Returns true if new (inserted), false if duplicate (ignored)
  tryMarkSeen(postId: string, createdAtMs: number): boolean

  // Cleanup rows older than retention period
  cleanup(retentionHours: number): void

  close(): void
}
```

Schema:
```sql
CREATE TABLE IF NOT EXISTS seen_posts (
  post_id TEXT PRIMARY KEY,      -- did/rkey
  created_at_ms INTEGER NOT NULL,
  seen_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_seen_at ON seen_posts(seen_at_ms);
```

- Key: `post_id = did + "/" + rkey`
- Retention: 72 hours (configurable)
- Cleanup: hourly interval

---

## Phase 4: File-Based Cursor

**`src/cursor.ts`**:

```typescript
class CursorStore {
  constructor(filePath: string, overridePath?: string)

  load(): number | undefined     // Returns cursor or undefined
  save(cursor: number): void     // Atomic write (temp + rename)
}
```

- Check `CURSOR_OVERRIDE.TXT` first (for manual reset)
- Fall back to cursor file
- On restart: subtract `RESTART_OVERLAP_SECONDS` (default 10) from loaded cursor

---

## Phase 5: Adapters (Tap + Jetstream)

**`src/adapters/types.ts`**:
```typescript
interface PostCreate {
  did: string
  rkey: string
  text: string
  langs?: string[]
  createdAt: string        // ISO timestamp
  source: 'tap' | 'jetstream'
  tapAck?: () => void      // Only for Tap events
}

interface EventAdapter {
  start(): Promise<void>
  stop(): Promise<void>
  onEvent(callback: (event: PostCreate) => void): void
}
```

**`src/adapters/tap.ts`**:
- WebSocket to Tap `/channel` endpoint
- Filter: `type === 'record' && action === 'create' && collection === 'app.bsky.feed.post'`
- Buffer events, call `tapAck()` only after flush succeeds
- Track `live: boolean` flag for backfill completion detection

**`src/adapters/jetstream.ts`**:
- Uses `@skyware/jetstream`
- `wantedCollections: ['app.bsky.feed.post']`
- Cursor persistence via `CursorStore`
- Auto-reconnect with cursor rewind

---

## Phase 6: In-Memory Aggregator

**`src/aggregator.ts`**:

```typescript
class Aggregator {
  // Delta maps (cleared on flush)
  private emojiGlobal: Map<string, number>           // emoji -> delta
  private emojiByLang: Map<string, number>           // `${lang}:${emoji}` -> delta
  private langTotal: Map<string, number>             // lang -> delta
  private metrics: Map<string, number>               // metric -> delta
  private emojiHourly: Map<string, number>           // `${hour}:${emoji}` -> delta

  // Pending events awaiting flush (for Tap ack)
  private pendingEvents: PostCreate[]

  accumulate(post: NormalizedPost): void
  accumulateNoEmoji(): void
  flush(clickhouse: ClickHouseClient): Promise<void>
  getPendingTapAcks(): (() => void)[]
}
```

**Accumulate logic per post**:
```typescript
metrics['processedPosts'] += 1

if (emojis.length === 0) {
  metrics['postsWithoutEmojis'] += 1
  return
}

metrics['postsWithEmojis'] += 1
metrics['processedEmojis'] += emojis.length

const hour = truncateToHourUTC(createdAt)

for (const emoji of emojis) {
  emojiGlobal[emoji] += 1
  emojiHourly[`${hour}:${emoji}`] += 1

  for (const lang of langsSet) {
    emojiByLang[`${lang}:${emoji}`] += 1
  }
}

for (const lang of langsSet) {
  langTotal[lang] += emojis.length
}
```

**Flush**: Insert batches into all 5 tables, then clear maps.

---

## Phase 7: Ingest Worker Entrypoint

**`src/index.ts`**:

```typescript
const config = loadConfig()
const clickhouse = createClickHouseClient(config)
const dedupe = new DedupeStore(config.dedupeDbPath)
const cursor = new CursorStore(config.cursorFilePath)
const aggregator = new Aggregator()

// Mode: 'tap' | 'jetstream' | 'both' (for cutover overlap)
const adapters: EventAdapter[] = []

if (config.mode === 'tap' || config.mode === 'both') {
  adapters.push(new TapAdapter(config.tapUrl))
}
if (config.mode === 'jetstream' || config.mode === 'both') {
  adapters.push(new JetstreamAdapter(config.jetstreamEndpoint, cursor))
}

// Shared event handler
const handleEvent = (event: PostCreate) => {
  const postId = `${event.did}/${event.rkey}`

  // Dedupe check
  if (!dedupe.tryMarkSeen(postId, Date.parse(event.createdAt))) {
    event.tapAck?.()  // Ack duplicates immediately
    return
  }

  // Normalize and accumulate
  const normalized = normalizePost(event)
  if (normalized) {
    aggregator.accumulate(normalized)
  } else {
    aggregator.accumulateNoEmoji()
  }
}

for (const adapter of adapters) {
  adapter.onEvent(handleEvent)
  await adapter.start()
}

// Flush loop
setInterval(async () => {
  await aggregator.flush(clickhouse)

  // Ack all Tap events that were in this flush
  for (const ack of aggregator.getPendingTapAcks()) {
    ack()
  }

  // Save Jetstream cursor
  if (jetstreamAdapter?.cursor) {
    cursor.save(jetstreamAdapter.cursor)
  }
}, config.flushIntervalMs)
```

**Config**:
- `INGEST_MODE`: `tap` | `jetstream` | `both`
- `TAP_URL`: WebSocket URL for Tap
- `JETSTREAM_ENDPOINT`: Jetstream URL
- `CLICKHOUSE_HOST`, `CLICKHOUSE_DATABASE`
- `DEDUPE_DB_PATH`, `CURSOR_FILE_PATH`
- `FLUSH_INTERVAL_MS` (default: 1000)

---

## Phase 8: API Server (ClickHouse-only)

**`packages/backend/src/api.ts`** — new entrypoint:

```typescript
const clickhouse = createClickHouseClient(config)
const cache = new StatsCache(1000)  // 1s TTL

// Socket.IO emit loop (unchanged interval)
setInterval(async () => {
  const stats = await cache.getOrFetch('global', () => getEmojiStats(clickhouse))
  const langs = await cache.getOrFetch('langs', () => getTopLanguages(clickhouse))

  io.emit('emojiStats', stats)
  io.emit('languageStats', langs)
}, EMIT_INTERVAL)

// Per-language handler
socket.on('getTopEmojisForLanguage', async (lang) => {
  const data = await cache.getOrFetch(`lang:${lang}`, () =>
    getTopEmojisForLanguage(clickhouse, lang)
  )
  socket.emit('topEmojisForLanguage', { language: lang, topEmojis: data })
})
```

**ClickHouse queries**:

```typescript
// getEmojiStats()
const metrics = await clickhouse.query(`
  SELECT metric, sum(value) as v FROM metrics_total GROUP BY metric
`)
const topEmojis = await clickhouse.query(`
  SELECT emoji, sum(cnt) as c FROM emoji_total_global
  GROUP BY emoji ORDER BY c DESC LIMIT 3790
`)

// getTopLanguages()
await clickhouse.query(`
  SELECT lang, sum(cnt) as c FROM lang_total
  GROUP BY lang ORDER BY c DESC LIMIT 30
`)

// getTopEmojisForLanguage(lang)
await clickhouse.query(`
  SELECT emoji, sum(cnt) as c FROM emoji_total_by_lang
  WHERE lang = {lang:String}
  GROUP BY emoji ORDER BY c DESC LIMIT 3790
`)
```

**In-process cache** (`StatsCache`):
```typescript
class StatsCache {
  private cache = new Map<string, { ts: number, data: any }>()

  constructor(private ttlMs: number) {}

  async getOrFetch<T>(key: string, fetch: () => Promise<T>): Promise<T> {
    const cached = this.cache.get(key)
    if (cached && Date.now() - cached.ts < this.ttlMs) {
      return cached.data
    }
    const data = await fetch()
    this.cache.set(key, { ts: Date.now(), data })
    return data
  }
}
```

---

## Phase 9: Remove Deprecated Code

**Delete from `packages/backend/`**:
- `src/index.ts` — replaced by `api.ts`
- `src/lib/postgres.ts`
- `src/lib/redis.ts`
- `src/lib/queue.ts`
- `src/lib/mqui.ts`
- `src/lib/schema.d.ts`
- `src/lib/jetstream.ts` — moves to `packages/ingest`
- `src/lib/lua/incrementEmojis.lua`
- `src/migrations/*`

**Modify**:
- `src/lib/emojiStats.ts` — replace Redis reads with ClickHouse queries
- `src/lib/metrics.ts` — remove Postgres/Redis metrics
- `package.json` — remove deps: `bullmq`, `@bull-board/*`, `fastify`, `pg`, `pg-native`, `kysely`, `redis`

**Optionally delete**:
- `packages/unified-ingest/` — functionality replaced by new `packages/ingest`

---

## File Changes Summary

### New: `packages/ingest/`
```
src/
├── index.ts
├── config.ts
├── clickhouse/client.ts
├── clickhouse/schema.sql
├── adapters/types.ts
├── adapters/jetstream.ts
├── adapters/tap.ts
├── aggregator.ts
├── dedupe.ts
├── cursor.ts
└── normalizer.ts
```

### New: `packages/backend/src/api.ts`

### Modified: `packages/backend/`
- `src/lib/emojiStats.ts` — ClickHouse queries instead of Redis
- `src/lib/metrics.ts` — remove DB metrics
- `package.json` — remove unused deps

### Deleted: `packages/backend/`
- `src/index.ts`, `src/lib/postgres.ts`, `src/lib/redis.ts`
- `src/lib/queue.ts`, `src/lib/mqui.ts`, `src/lib/jetstream.ts`
- `src/lib/lua/*`, `src/migrations/*`

---

## Environment Variables

**Ingest Worker**:
```
INGEST_MODE=jetstream           # tap | jetstream | both
TAP_URL=ws://localhost:8080/channel
JETSTREAM_ENDPOINT=wss://jetstream.atproto.tools/subscribe
CLICKHOUSE_HOST=http://localhost:8123
CLICKHOUSE_DATABASE=emoji_stats
DEDUPE_DB_PATH=./data/dedupe.sqlite
CURSOR_FILE_PATH=./data/cursor.txt
FLUSH_INTERVAL_MS=1000
DEDUPE_RETENTION_HOURS=72
```

**API Server**:
```
CLICKHOUSE_HOST=http://localhost:8123
CLICKHOUSE_DATABASE=emoji_stats
PORT=3100
METRICS_PORT=3101
ORIGINS=http://localhost:5173
CACHE_TTL_MS=1000
```

---

## Operational Runbook

### Initial Backfill
- Start ClickHouse
- Start Tap with `TAP_FULL_NETWORK=true`
- Start Ingest Worker with `INGEST_MODE=tap`
- Wait until Tap events show `live: true` consistently
- API Server can stay off (frontend not needed during backfill)

### Cutover (Tap → Jetstream)
- Start Ingest Worker with `INGEST_MODE=both`
- Both Tap and Jetstream run concurrently
- SQLite dedupe handles overlap duplicates
- After 30+ minutes of overlap, stop Tap permanently
- Switch to `INGEST_MODE=jetstream`

### Steady State
- Ingest Worker: `INGEST_MODE=jetstream` (Jetstream → ClickHouse)
- API Server: ClickHouse queries + cache → Socket.IO → Frontend

### Recovery
- Jetstream restart: cursor rewinds 10s, dedupe prevents double-count
- ClickHouse restart: ingest worker reconnects, continues
- No Redis rebuild needed (Redis doesn't exist)

---

## Verification

- Start ingest worker, verify ClickHouse tables populate:
  ```sql
  SELECT count() FROM emoji_total_global;
  SELECT metric, sum(value) FROM metrics_total GROUP BY metric;
  ```
- Start API server, verify Socket.IO emits match query results
- Restart ingest worker, verify no duplicate counts (check metrics before/after)
- Frontend should work unchanged (same Socket.IO contract)
