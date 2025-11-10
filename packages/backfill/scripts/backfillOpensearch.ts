import { AtUri } from "@atproto/syntax";
import { Client as OpenSearchClient } from "@opensearch-project/opensearch";
import { Database } from "@zeppelin-social/bsky-backfill";
import PQueue from "p-queue";
import { is } from "../backfill/util/lexicons";
import {
  POST_INDEX,
  type PostDoc,
  PROFILE_INDEX,
  type ProfileDoc,
  transformPost,
  transformProfile,
} from "../backfill/workers/opensearch";

declare global {
  namespace NodeJS {
    interface ProcessEnv {
      BSKY_DB_POSTGRES_URL: string;
      BSKY_DB_POSTGRES_SCHEMA: string;
      OPENSEARCH_URL: string;
      OPENSEARCH_USERNAME: string;
      OPENSEARCH_PASSWORD: string;
    }
  }
}

for (const envVar of [
  "BSKY_DB_POSTGRES_URL",
  "BSKY_DB_POSTGRES_SCHEMA",
  "OPENSEARCH_URL",
  "OPENSEARCH_USERNAME",
  "OPENSEARCH_PASSWORD",
]) {
  if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

const db = new Database({
  url: process.env.BSKY_DB_POSTGRES_URL,
  schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
  poolSize: 20,
});

const client = new OpenSearchClient({
  node: process.env.OPENSEARCH_URL,
  auth: {
    username: process.env.OPENSEARCH_USERNAME,
    password: process.env.OPENSEARCH_PASSWORD,
  },
});

async function backfillPosts() {
  const queue = new PQueue({ concurrency: 5 });

  let offset = 0;
  while (true) {
    await queue.onSizeLessThan(5);

    const posts = await db.db
      .selectFrom("record")
      .select(["uri", "cid", "json"])
      .where("uri", "in", (eb) =>
        eb
          .selectFrom("post")
          .select(["uri"])
          .orderBy("uri", "desc")
          .limit(10_000)
          .offset(offset),
      )
      .execute();

    if (posts.length === 0) break;
    offset += posts.length;

    void queue.add(async () => {
      const datasource = posts.reduce((acc, p) => {
        const { host: did, rkey } = new AtUri(p.uri);
        try {
          const post: unknown = JSON.parse(p.json);
          if (is("app.bsky.feed.post", post)) {
            acc.push(
              transformPost(post, did as `did:plc:${string}`, rkey, p.cid),
            );
          }
        } catch {}
        return acc;
      }, [] as PostDoc[]);

      const action = { index: { _index: POST_INDEX } };
      await client.helpers.bulk({ datasource, onDocument: () => action });
    });
  }
}

async function backfillProfiles() {
  const queue = new PQueue({ concurrency: 5 });

  let offset = 0;
  while (true) {
    await queue.onSizeLessThan(5);

    const profiles = await db.db
      .selectFrom("record")
      .select(["did", "cid", "json"])
      .where("uri", "in", (eb) =>
        eb
          .selectFrom("profile")
          .select(["uri"])
          .orderBy("uri", "desc")
          .limit(10_000)
          .offset(offset),
      )
      .execute();

    if (profiles.length === 0) break;
    offset += profiles.length;

    void queue.add(async () => {
      const datasource = profiles.reduce((acc, p) => {
        try {
          const profile: unknown = JSON.parse(p.json);
          if (is("app.bsky.actor.profile", profile)) {
            acc.push(
              transformProfile(profile, p.did as `did:plc:${string}`, p.cid),
            );
          }
        } catch {}
        return acc;
      }, [] as ProfileDoc[]);

      const action = { index: { _index: PROFILE_INDEX } };
      await client.helpers.bulk({ datasource, onDocument: () => action });
    });
  }
}

async function main() {
  await Promise.all([
    backfillPosts().catch((e) =>
      console.error(`Error backfilling posts: ${e}`),
    ),
    backfillProfiles().catch((e) =>
      console.error(`Error backfilling profiles: ${e}`),
    ),
  ]);
}

void main();
