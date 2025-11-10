import { AppBskyActorProfile } from "@atcute/bluesky";
import { simpleFetchHandler, XRPC } from "@atcute/client";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { AtUri } from "@atproto/syntax";
import { BackgroundQueue, Database } from "@zeppelin-social/bsky-backfill";
import { IndexingService } from "@zeppelin-social/bsky-backfill/dist/data-plane/server/indexing";
import { CID } from "multiformats/cid";
import { is } from "../backfill/util/lexicons";
import { jsonToLex } from "../backfill/workers/writeCollection";

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

async function main() {
  const args = process.argv.slice(2);

  const db = new Database({
    url: process.env.BSKY_DB_POSTGRES_URL,
    schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
    poolSize: 100,
  });
  const background = new BackgroundQueue(db);

  const idResolver = new IdResolver({
    plcUrl: process.env.BSKY_DID_PLC_URL,
    didCache: new MemoryCache(),
  });

  const indexingSvc = new IndexingService(db, idResolver, background);

  // I don't like using bsky.social but it's convenient
  const xrpc = new XRPC({
    handler: simpleFetchHandler({ service: "https://bsky.social" }),
  });

  const profiles: Array<{
    uri: AtUri;
    cid: CID;
    obj: unknown;
    timestamp: string;
    record: AppBskyActorProfile.Main;
  }> = [];

  await Promise.all(
    args.map(async (did) => {
      if (!did.startsWith("did:")) {
        did = await idResolver.handle.resolve(did).then((r) => {
          if (!r) throw new Error(`Invalid DID/handle: ${did}`);
          return r;
        });
      }

      const profile = await xrpc.get("com.atproto.repo.getRecord", {
        params: {
          repo: did,
          collection: "app.bsky.actor.profile",
          rkey: "self",
        },
      });
      if (!is("app.bsky.actor.profile", profile.data.value)) return;

      const uri = new AtUri(profile.data.uri);

      profiles.push({
        uri: new AtUri(profile.data.uri),
        cid: CID.parse(profile.data.cid!),
        obj: jsonToLex(profile.data.value),
        timestamp: profile.data.value.createdAt ?? new Date().toISOString(),
        record: profile.data.value,
      });

      await Promise.all([
        db.db
          .deleteFrom("record")
          .where("uri", "=", profile.data.uri)
          .execute(),
        db.db
          .deleteFrom("profile")
          .where("uri", "=", profile.data.uri)
          .execute(),
        indexingSvc.db.transaction((db) => {
          const idx = new IndexingService(db, idResolver, background);
          // @ts-expect-error — fake follow just to update counts
          idx.records.follow.aggregateOnCommit({
            creator: uri.host,
            subjectDid: uri.host,
          });
          // @ts-expect-error — similarly, fake post
          idx.records.post.aggregateOnCommit({ post: { creator: uri.host } });
          return idx.background.processAll();
        }),
      ]);
    }),
  );

  await Promise.all([
    indexingSvc.bulkIndexToCollectionSpecificTables(
      new Map([["app.bsky.actor.profile", profiles]]),
      { validate: false },
    ),
    indexingSvc.bulkIndexToRecordTable(
      profiles.map(({ obj: _obj, ...p }) => ({ ...p, obj: p.record })),
    ),
    indexingSvc.background.processAll(),
  ]);
}

void main();
