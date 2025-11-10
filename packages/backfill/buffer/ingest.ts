import { MemoryCache } from "@atproto/identity";
import { lexToJson } from "@atproto/lexicon";
import { WriteOpAction } from "@atproto/repo";
import { AtUri } from "@atproto/syntax";
import { type Event, jsonToLex, parseCid } from "@futur/bsky-indexer";
import { BackgroundQueue, Database } from "@zeppelin-social/bsky-backfill";
import console from "node:console";
import { createReadStream, readFileSync, writeFileSync } from "node:fs";
import process from "node:process";
import readline from "node:readline";
import { setTimeout as sleep } from "node:timers/promises";
import PQueue from "p-queue";
import { IdResolver, IndexingService } from "../backfill/indexingService.ts";
import { is } from "../backfill/util/lexicons.ts";
import type { ToInsertCommit } from "../backfill/workers/writeCollection.ts";

const BATCH_SIZE = 50_000;
const LOG_INTERVAL_MS = 30_000;

const useFileState = process.argv.includes("--file-state");

declare global {
  namespace NodeJS {
    interface ProcessEnv {
      BSKY_DB_POSTGRES_URL: string;
      BSKY_DB_POSTGRES_SCHEMA: string;
      BSKY_DID_PLC_URL: string;
    }
  }
}

for (const envVar of [
  "BSKY_DB_POSTGRES_URL",
  "BSKY_DB_POSTGRES_SCHEMA",
  "BSKY_DID_PLC_URL",
]) {
  if (!process.env[envVar])
    throw new Error(`Missing required env var ${envVar}`);
}

type CollectionMap = Map<string, ToInsertCommit[]>;

async function main() {
  const file = "relay-buffer.jsonl";

  const db = new Database({
    url: process.env.BSKY_DB_POSTGRES_URL,
    schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
    poolSize: 150,
  });
  const idResolver = new IdResolver({
    plcUrl: process.env.BSKY_DID_PLC_URL,
    didCache: new MemoryCache(),
  });
  const indexingSvc = new IndexingService(
    db,
    idResolver,
    new BackgroundQueue(db),
  );

  let startLine = 0;
  if (useFileState) {
    try {
      startLine = parseInt(readFileSync("relay-buffer.pos", "utf-8").trim());
    } catch {
      // Start at 0
    }
  }
  if (Number.isNaN(startLine)) startLine = 0;
  console.log(`Starting buffer ingest at line ${startLine}`);

  const rl = readline.createInterface({
    input: createReadStream(file, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let lineNo = 0;
  let sinceLastLog = Date.now();

  const collectionBuffer: CollectionMap = new Map();
  const getBufferSize = () =>
    collectionBuffer.values().reduce((acc, arr) => acc + arr.length, 0);

  const otherEvts = new PQueue({ concurrency: 20 });
  otherEvts.on("error", (e) => console.error("Error in otherEvts", e));

  const onExit = () => {
    console.log("Received SIGINT, trying to gracefully exit");
    Promise.race([flush(indexingSvc, lineNo, true), sleep(10_000)]).then(() =>
      process.exit(0),
    );
  };
  process.on("SIGINT", onExit);
  process.on("SIGTERM", onExit);
  process.on("SIGQUIT", onExit);

  for await (const line of rl) {
    lineNo++;

    if (lineNo <= startLine) continue;
    if (!line) continue;

    let evt;
    try {
      evt = JSON.parse(line) as Event;
    } catch {
      console.warn(`Failed to parse JSON at line ${lineNo}`);
      continue;
    }

    try {
      processEvent(evt);
    } catch (err) {
      console.warn(`Failed to process event at line ${lineNo}`, err);
    }

    const bufferSize = getBufferSize();
    if (bufferSize >= BATCH_SIZE) {
      await flush(indexingSvc, lineNo);
      collectionBuffer.clear();
    }

    if (Date.now() - sinceLastLog > LOG_INTERVAL_MS) {
      console.log(`Processed ${lineNo} lines (${bufferSize} records buffered)`);
      sinceLastLog = Date.now();
    }
  }

  await flush(indexingSvc, lineNo, true);
  console.log("Ingestion complete!");
  process.exit(0);

  async function flush(idxSvc: IndexingService, pos: number, final = false) {
    const bufferSize = getBufferSize();
    if (bufferSize === 0) return;

    const allRecords = [...collectionBuffer.values()].flat();

    try {
      const timeRecords = `Flushing ${bufferSize} records to record table`;
      const timeCollections = `Flushing ${bufferSize} records to collection tables`;
      console.time(timeRecords);
      console.time(timeCollections);
      console.time("Flushing other events");
      await Promise.all([
        Promise.all([
          idxSvc
            .bulkIndexToRecordTable(allRecords)
            .catch((e) =>
              console.error("Error bulk indexing to record table", e),
            )
            .finally(() => console.timeEnd(timeRecords)),
          idxSvc
            .bulkIndexToCollectionSpecificTables(collectionBuffer, {
              validate: false,
            })
            .catch((e) =>
              console.error(
                "Error bulk indexing to collection specific tables",
                e,
              ),
            )
            .finally(() => console.timeEnd(timeCollections)),
        ]),
        otherEvts
          .onSizeLessThan(100)
          .catch((e) => console.error("Error flushing other events", e))
          .finally(() => console.timeEnd("Flushing other events")), // Just want to prevent it from getting too big
      ]);
    } catch (err) {
      console.error("Error flushing batch – writing to disk", err);
      writeFileSync(
        `failed-batch-${Date.now()}.jsonl`,
        allRecords
          .map((r) => JSON.stringify({ uri: r.uri, obj: lexToJson(r.obj) }))
          .join("\n") + "\n",
      );
      if (final) throw err;
    } finally {
      writeFileSync("relay-buffer.pos", `${pos}`);
    }
  }

  function processEvent(evt: Event) {
    if (evt.$type === "com.atproto.sync.subscribeRepos#identity") {
      void otherEvts.add(() =>
        indexingSvc
          .indexHandle(evt.did, evt.time, true)
          .catch((e) => console.error("Error indexing handle", e)),
      );
      return;
    } else if (evt.$type === "com.atproto.sync.subscribeRepos#account") {
      if (evt.active === false && evt.status === "deleted") {
        void otherEvts.add(() =>
          indexingSvc
            .deleteActor(evt.did)
            .catch((e) => console.error("Error deleting actor", e)),
        );
      } else {
        void otherEvts.add(() =>
          indexingSvc
            .updateActorStatus(evt.did, evt.active, evt.status)
            .catch((e) => console.error("Error updating actor status", e)),
        );
      }
      return;
    } else if (evt.$type === "com.atproto.sync.subscribeRepos#sync") {
      // skip; we don't have blocks
      return;
    } else if (evt.$type !== "com.atproto.sync.subscribeRepos#commit") return;

    if (!evt.ops?.length) return;

    for (const op of evt.ops) {
      const uri = AtUri.make(evt.did, ...op.path.split("/"));
      if (op.action === "delete") {
        void otherEvts.add(() =>
          indexingSvc
            .deleteRecord(uri)
            .catch((err) => console.error("Error deleting record", err)),
        );
      } else {
        const record = jsonToLex(op.record);
        const cid = parseCid(op.cid);
        if (!is(uri.collection, record)) continue;
        if (op.action === "update") {
          // TODO: change this to a regular update once skipValidation is passed through to updateRecord
          void otherEvts.add(() =>
            indexingSvc
              .deleteRecord(uri)
              .then(() =>
                indexingSvc.indexRecord(
                  uri,
                  cid,
                  record,
                  WriteOpAction.Create,
                  evt.time,
                  { skipValidation: true },
                ),
              )
              .catch((e) => console.error("Error updating record", e)),
          );
        } else {
          if (!collectionBuffer.has(uri.collection)) {
            collectionBuffer.set(uri.collection, []);
          }
          collectionBuffer.get(uri.collection)!.push({
            uri,
            cid,
            timestamp: evt.time,
            obj: record,
          });
        }
      }
    }
  }
}

main().catch((err) => {
  console.error("Ingest error", err);
  process.exit(1);
});
