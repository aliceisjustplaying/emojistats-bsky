import { simpleFetchHandler, XRPC } from "@atcute/client";
import { MemoryCache } from "@atproto/identity";
import { AtUri } from "@atproto/syntax";
import { BackgroundQueue, Database } from "@zeppelin-social/bsky-backfill";
import { IndexingService } from "@zeppelin-social/bsky-backfill/dist/data-plane/server/indexing";
import { sql } from "kysely";
import { CID } from "multiformats/cid";
import PQueue from "p-queue";
import { IdResolver } from "../backfill/indexingService";
import { is } from "../backfill/util/lexicons";
import { jsonToLex } from "../backfill/workers/writeCollection";

declare global {
  namespace NodeJS {
    interface ProcessEnv {
      BSKY_DB_POSTGRES_URL: string;
      BSKY_DB_POSTGRES_SCHEMA: string;
      BSKY_DID_PLC_URL: string;
      FALLBACK_PLC_URL?: string;
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

for (const envVar of ["FALLBACK_PLC_URL"]) {
  if (!process.env[envVar]) console.warn(`Missing optional env var ${envVar}`);
}

type Queue = {
  add: <T>(fn: (client: XRPC) => Promise<T>) => Promise<T | void>;
};
const serviceToQueue: Record<string, Queue> = {};
function makeQueue(service: string): Queue {
  if (serviceToQueue[service]) return serviceToQueue[service];
  const pqueue = new PQueue({
    concurrency: 10,
    interval: 300 * 1000,
    intervalCap: 3000,
  });
  const xrpc = new XRPC({ handler: simpleFetchHandler({ service }) });
  return (serviceToQueue[service] = {
    add: async <T>(fn: (client: XRPC) => Promise<T>) => {
      return await pqueue.add(async () => {
        return await fn(xrpc);
      });
    },
  });
}

async function main() {
  const db = new Database({
    url: process.env.BSKY_DB_POSTGRES_URL,
    schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
    poolSize: 100,
  });

  const idResolver = new IdResolver({
    plcUrl: process.env.BSKY_DID_PLC_URL,
    fallbackPlc: process.env.FALLBACK_PLC_URL,
    didCache: new MemoryCache(),
  });

  const indexingSvc = new IndexingService(
    db,
    idResolver,
    new BackgroundQueue(db),
  );

  const queue = new PQueue({ concurrency: 5 });

  let offset = 0;
  while (true) {
    await queue.onSizeLessThan(5);

    const dids = await db.db
      .selectFrom("actor")
      .select(["did"])
      .whereNotExists((eb) =>
        eb
          .selectFrom("record")
          .where(
            "record.uri",
            "=",
            sql`concat('at://', ${eb.ref("actor.did")}, '/app.bsky.actor.profile/self')`,
          ),
      )
      .orderBy("did", "asc")
      .offset(offset)
      .limit(1000)
      .execute();

    if (dids.length === 0) break;
    offset += dids.length;

    const profiles: Array<{
      uri: AtUri;
      cid: CID;
      obj: unknown;
      timestamp: string;
      record: Record<string, unknown>;
    }> = [];

    void queue
      .add(() =>
        Promise.allSettled(
          dids.map(async ({ did }) => {
            const { pds } = await idResolver.did.resolveAtprotoData(did);
            if (!pds) return;

            const queue = makeQueue(pds);
            const profile = await queue.add(async (client) => {
              return await client.get("com.atproto.repo.getRecord", {
                params: {
                  repo: did,
                  collection: "app.bsky.actor.profile",
                  rkey: "self",
                },
              });
            });
            if (!profile) return;
            const { uri, cid, value } = profile.data;
            if (!cid || !is("app.bsky.actor.profile", value)) return;
            profiles.push({
              uri: new AtUri(uri),
              cid: CID.parse(cid),
              obj: jsonToLex(value),
              timestamp: value.createdAt ?? new Date().toISOString(),
              record: value,
            });
          }),
        ).then(() =>
          Promise.all([
            indexingSvc.bulkIndexToCollectionSpecificTables(
              new Map([["app.bsky.actor.profile", profiles]]),
              { validate: false },
            ),
            indexingSvc.bulkIndexToRecordTable(
              profiles.map(({ obj: _obj, ...p }) => ({ ...p, obj: p.record })),
            ),
          ]),
        ),
      )
      .then(() => console.log(`Indexed ${profiles.length} profiles`))
      .catch((err) => console.error(err));
  }
}

void main();
