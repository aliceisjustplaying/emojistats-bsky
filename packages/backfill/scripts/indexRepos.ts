import { Client, ok, simpleFetchHandler } from "@atcute/client";
import { ValidationError } from "@atcute/lexicons";
import { parse as parseTID } from "@atcute/tid";
import { IdResolver, MemoryCache } from "@atproto/identity";
import type { RepoRecord } from "@atproto/lexicon";
import { getAndParseRecord, readCarWithRoot, verifyRepo } from "@atproto/repo";
import { AtUri } from "@atproto/syntax";
import { BackgroundQueue, Database } from "@zeppelin-social/bsky-backfill";
import { IndexingService } from "@zeppelin-social/bsky-backfill/dist/data-plane/server/indexing";
import { CID } from "multiformats/cid";

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
  if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

let spider = false;
let manual = false;
let noDelete = false;

async function main() {
  const args = process.argv.slice(2);
  {
    const spiderIndex = args.indexOf("--spider");
    if (spiderIndex !== -1) {
      args.splice(spiderIndex, 1);
      spider = true;
    }
  }
  {
    const manualIndex = args.indexOf("--manual");
    if (manualIndex !== -1) {
      args.splice(manualIndex, 1);
      manual = true;
    }
  }
  {
    const noDeleteIndex = args.indexOf("--no-delete");
    if (noDeleteIndex !== -1) {
      args.splice(noDeleteIndex, 1);
      noDelete = true;
    }
  }

  const db = new Database({
    url: process.env.BSKY_DB_POSTGRES_URL,
    schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
    poolSize: 100,
  });

  db.pool.on("connect", (client) => {
    client.query("SET statement_timeout=0;");
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

  await Promise.allSettled(
    args.map(async (did) => {
      if (!did.startsWith("did:")) {
        did = await idResolver.handle.resolve(did).then((r) => {
          if (!r) throw new Error(`Invalid DID/handle: ${did}`);
          return r;
        });
      }
      return indexRepo(did, indexingSvc, spider);
    }),
  ).then((p) =>
    p.forEach((r) => {
      if (r.status === "rejected") console.error(r.reason);
    }),
  );
}

async function indexRepo(
  did: string,
  indexingSvc: IndexingService,
  spider = false,
) {
  if (!noDelete) await indexingSvc.unindexActor(did);
  await indexingSvc.indexHandle(did, new Date().toISOString());

  const promises: Promise<unknown>[] = [];
  const follows: string[] = [];
  if (!manual) {
    promises.push(indexingSvc.indexRepo(did));
    if (spider) {
      let fs = await indexingSvc.db.db
        .selectFrom("follow")
        .select("subjectDid")
        .where("creator", "=", did)
        .execute();
      follows.push(...fs.map((f) => f.subjectDid));
    }
  } else {
    const commitData = await getRepoRecords(did, indexingSvc);
    if (Object.keys(commitData).length === 0) return;

    const records = Object.values(commitData).flat();
    const collections = new Map(Object.entries(commitData));

    promises.push(
      indexingSvc.bulkIndexToRecordTable(records),
      indexingSvc.bulkIndexToCollectionSpecificTables(collections, {
        validate: false,
      }), // validation occurs in getRepoRecords
    );

    if (spider) {
      follows.push(
        ...(collections
          .get("app.bsky.graph.follow")
          ?.map((f) => (f.obj as any).subject) ?? []),
      );
    }
  }

  if (follows.length) {
    console.log(`spidering ${follows.length} follows for ${did}`);
    promises.push(...follows.map((f) => indexRepo(f, indexingSvc, false)));
  }

  await Promise.allSettled(promises).then((p) =>
    p.forEach((r) => {
      if (r.status === "rejected")
        console.error(`failed to index repo for ${did}`, r.reason);
    }),
  );
}

async function getRepoRecords(did: string, indexingSvc: IndexingService) {
  try {
    const { idResolver } = indexingSvc;
    const { pds } = await idResolver.did.resolveAtprotoData(did);
    const agent = new Client({ handler: simpleFetchHandler({ service: pds }) });
    const repoBytes = await ok(
      agent.get(`com.atproto.sync.getRepo`, {
        params: { did: did as `did:${string}:${string}` },
        as: "bytes",
      }),
    );
    const { root, blocks } = await readCarWithRoot(repoBytes);
    const repo = await verifyRepo(blocks, root, did);

    const now = Date.now();

    const collections: Record<
      string,
      { uri: AtUri; cid: CID; timestamp: string; obj: RepoRecord }[]
    > = {};

    await Promise.all(
      repo.creates.map(async ({ cid, collection, rkey }) => {
        const uri = AtUri.make(did, collection, rkey);
        try {
          const { record } = await getAndParseRecord(blocks, cid);

          let indexedAtMs =
            (!!record &&
              typeof record.createdAt === "string" &&
              new Date(record.createdAt).getTime()) ||
            0;
          if (!indexedAtMs || isNaN(indexedAtMs)) {
            try {
              indexedAtMs = parseTID(rkey).timestamp;
            } catch {
              indexedAtMs = now;
            }
          }
          if (indexedAtMs > now) indexedAtMs = now;
          const indexedAt = new Date(indexedAtMs).toISOString();

          collections[collection] ??= [];
          collections[collection].push({
            uri,
            cid,
            timestamp: indexedAt,
            obj: record,
          });
        } catch (err) {
          if (err instanceof ValidationError) {
            console.warn(uri.toString(), "skipping indexing of invalid record");
          } else {
            console.error(
              uri.toString(),
              "skipping indexing due to error processing record",
            );
          }
        }
      }),
    );

    return collections;
  } catch (error) {
    console.warn(`failed to index repo for ${did}`, error);
    return {};
  }
}

void main();
