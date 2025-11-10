import { isCanonicalResourceUri, isCid } from "@atcute/lexicons/syntax";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { jsonStringToLex, jsonToLex } from "@atproto/lexicon";
import { WriteOpAction } from "@atproto/repo";
import { AtUri } from "@atproto/syntax";
import { Client as OpenSearchClient } from "@opensearch-project/opensearch";
import { BackgroundQueue, Database } from "@zeppelin-social/bsky-backfill";
import type {
  DatabaseSchema,
  DatabaseSchemaType,
} from "@zeppelin-social/bsky-backfill/dist/data-plane/server/db/database-schema";
import {
  executeRaw,
  invalidReplyRoot,
  violatesThreadGate,
} from "@zeppelin-social/bsky-backfill/dist/data-plane/server/util";
import type {
  Record as PostRecord,
  ReplyRef,
} from "@zeppelin-social/bsky-backfill/dist/lexicon/types/app/bsky/feed/post";
import type { Record as PostgateRecord } from "@zeppelin-social/bsky-backfill/dist/lexicon/types/app/bsky/feed/postgate";
import type { Record as GateRecord } from "@zeppelin-social/bsky-backfill/dist/lexicon/types/app/bsky/feed/threadgate";
import {
  postUriToThreadgateUri,
  uriToDid,
} from "@zeppelin-social/bsky-backfill/dist/util/uris";
import { parsePostgate } from "@zeppelin-social/bsky-backfill/dist/views/util";
import { sql } from "kysely";
import { LRUCache } from "lru-cache";
import { CID } from "multiformats/cid";
import fs from "node:fs";
import { readFile } from "node:fs/promises";
import path from "node:path";
import readline from "node:readline/promises";
import { IndexingService } from "../backfill/indexingService";
import { is } from "../backfill/util/lexicons";
import {
  POST_INDEX,
  type PostDoc,
  PROFILE_INDEX,
  type ProfileDoc,
} from "../backfill/workers/opensearch";
import { writeWorkerAllocations } from "../backfill/workers/writeCollection";

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

const statePath = path.join(process.cwd(), "after-backfill-state.json");
let state: State = {
  post: { cursor: null, index: 0 },
  profile: { cursor: null, index: 0 },
  validation: { cursor: null, index: 0 },
};

const POOL_SIZE = 100;

const RECORDS_BATCH_SIZE = 50_000;

// const DB_SETTINGS = {
// 	max_parallel_workers: 24,
// 	max_parallel_workers_per_gather: 24,
// 	max_worker_processes: 32,
// 	maintenance_work_mem: "\"32GB\"",
// };

async function main() {
  const state = loadState();

  const db = new Database({
    url: process.env.BSKY_DB_POSTGRES_URL,
    schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
    poolSize: POOL_SIZE,
  });

  await alterDbSettings(db);
  addExitHandlers(db);

  console.log("beginning backfill...");

  await retryFailedWrites(db);

  await retryInParallel(
    ["post", backfillPostAggregates(db, state)],
    ["profile", backfillProfileAggregates(db, state)],
    // ["post validation", backfillPostValidation(db, state)]
  );
}

void main();

async function retryInParallel(
  ...fns: Array<[string, AsyncGenerator<string>]>
) {
  const retries = 5;
  return new Promise<void>((resolve) => {
    let completed = 0;
    const errored: Array<[string, string]> = [];

    for (const [fnName, fn] of fns) {
      (async () => {
        let lastErroredCursors: Array<string> = [];
        let done = false,
          cursor: string | undefined;
        while (!done) {
          try {
            ({ done = false, value: cursor } = await fn.next());
          } catch (err) {
            console.error(`error in ${fnName}:`, err);
            if (lastErroredCursors.length < retries) {
              lastErroredCursors.push(cursor ?? "");
              continue;
            } else if (cursor !== lastErroredCursors.at(-1)) {
              console.error(`retrying cursor ${cursor} for ${fnName}`);
              lastErroredCursors = [cursor ?? ""];
            }
            console.error(
              `max retries reached for ${fnName} at cursor ${cursor}`,
            );
            errored.push([fnName, cursor ?? ""]);
            done = true;
          }
        }
        completed++;
        if (completed === fns.length) {
          console.log("aggregate backfill completed successfully");
          if (errored.length > 0) {
            console.error("some functions errored:", errored);
          } else {
            console.log("no errors occurred");
          }
          resolve();
        }
      })();
    }
  });
}

async function* backfillPostAggregates(
  { db }: Database,
  state: State,
): AsyncGenerator<string> {
  const limit = 10_000;
  let rowCount = await fastRowCount(db, "post");
  console.log(`post row count: ${rowCount}`);

  let batches = Math.ceil(rowCount / limit);
  let i = state.post.index ?? 0,
    cursor = state.post.cursor ?? null;
  try {
    while (true) {
      if (i >= batches) {
        rowCount = await fastRowCount(db, "post");
        batches = Math.ceil(rowCount / limit);
      }

      saveState((s) => ({ ...s, post: { cursor, index: i } }));

      console.time(`backfilling posts ${i + 1}/${batches}`);

      const inserted = await sql`
   	  WITH inserted AS (
        WITH posts (uri, cid) AS (
            SELECT uri, cid
            FROM post
            WHERE uri IS NOT NULL
              AND cid IS NOT NULL
              AND (${cursor}::text IS NULL OR uri > ${cursor}::text)
            ORDER BY uri ASC
            LIMIT ${limit}::bigint
        )
        INSERT INTO post_agg ("uri", "replyCount", "likeCount", "repostCount", "quoteCount")
        SELECT
            p.uri,
            COALESCE(replies.count, 0) as "replyCount",
            COALESCE(likes.count, 0) as "likeCount",
            COALESCE(reposts.count, 0) as "repostCount",
            COALESCE(quotes.quoteCount, 0) as "quoteCount"
        FROM posts p
        LEFT JOIN (
            SELECT "replyParent" as uri, COUNT(*) as count
            FROM post
            WHERE "replyParent" IN (SELECT uri FROM posts)
                AND (post."violatesThreadGate" IS NULL OR post."violatesThreadGate" = false)
            GROUP BY "replyParent"
        ) replies ON replies.uri = p.uri
        LEFT JOIN (
            SELECT subject as uri, COUNT(*) as count
            FROM "like"
            WHERE subject IN (SELECT uri FROM posts)
            GROUP BY subject
        ) likes ON likes.uri = p.uri
        LEFT JOIN (
            SELECT subject as uri, COUNT(*) as count
            FROM repost
            WHERE subject IN (SELECT uri FROM posts)
            GROUP BY subject
        ) reposts ON reposts.uri = p.uri
        LEFT JOIN (
            SELECT subject as uri, COUNT(*) as quoteCount
            FROM quote
            WHERE subject IN (SELECT uri FROM posts)
            AND "subjectCid" IN (SELECT cid FROM posts WHERE quote.subject = posts.uri)
            GROUP BY subject
        ) quotes ON quotes.uri = p.uri
        ON CONFLICT (uri) DO UPDATE
            SET "replyCount" = excluded."replyCount",
                "likeCount" = excluded."likeCount",
                "repostCount" = excluded."repostCount",
                "quoteCount" = excluded."quoteCount"
        RETURNING uri
      ),
      batch_info AS (
          SELECT uri
          FROM post
          WHERE uri IS NOT NULL
            AND cid IS NOT NULL
            AND (${cursor}::text IS NULL OR uri > ${cursor}::text)
          ORDER BY uri ASC
          LIMIT ${limit}::bigint
      )
      SELECT
          COUNT(inserted.uri) as processed_count,
          MAX(batch_info.uri) as next_cursor,
          MIN(batch_info.uri) as batch_start,
          MAX(batch_info.uri) as batch_end
      FROM inserted
      FULL OUTER JOIN batch_info ON inserted.uri = batch_info.uri;
			`.execute(db);

      if (inserted.rows.length === 0) break;
      // @ts-expect-error — row is not typed
      if (inserted.rows[0].processed_count === 0) break;
      // @ts-expect-error — row is not typed
      yield* (cursor = inserted.rows[0].next_cursor);

      console.timeEnd(`backfilling posts ${i + 1}/${batches}`);
      i++;
    }
  } catch (err) {
    console.error(`backfilling posts ${i + 1}/${batches}`, err);
    if (err instanceof Error && err.stack) console.error(err.stack);
  }
}

async function* backfillProfileAggregates(
  { db }: Database,
  state: State,
): AsyncGenerator<string> {
  const limit = 1_000;
  let rowCount = await fastRowCount(db, "actor");
  console.log(`actor row count: ${rowCount}`);

  let batches = Math.ceil(rowCount / limit);
  let i = state.profile.index ?? 0,
    cursor = state.profile.cursor ?? null;
  try {
    while (true) {
      if (i >= batches) {
        rowCount = await fastRowCount(db, "actor");
        batches = Math.ceil(rowCount / limit);
      }

      saveState((s) => ({ ...s, profile: { cursor, index: i } }));

      console.time(`backfilling profiles ${i + 1}/${batches}`);

      const inserted = await sql`
      WITH batch_query AS (
        SELECT
          actor.did as creator,
          ROW_NUMBER() OVER (ORDER BY actor.did ASC) as rn
        FROM actor
        WHERE actor.did IS NOT NULL
          AND (${cursor}::text IS NULL OR ${cursor}::text = '' OR actor.did > ${cursor}::text)
        ORDER BY actor.did ASC
        LIMIT ${limit}::bigint
      ),
      batch_profiles AS (
        SELECT creator FROM batch_query
      ),
      followers_counts AS (
        SELECT f."subjectDid" as did, COUNT(*) as cnt
        FROM follow f
        WHERE f."subjectDid" IN (SELECT creator FROM batch_profiles)
        GROUP BY f."subjectDid"
      ),
      follows_counts AS (
        SELECT f.creator as did, COUNT(*) as cnt
        FROM follow f
        WHERE f.creator IN (SELECT creator FROM batch_profiles)
        GROUP BY f.creator
      ),
      posts_counts AS (
        SELECT p.creator as did, COUNT(*) as cnt
        FROM post p
        WHERE p.creator IN (SELECT creator FROM batch_profiles)
        GROUP BY p.creator
      ),
      insert_result AS (
        INSERT INTO profile_agg ("did", "followersCount", "followsCount", "postsCount")
        SELECT
          bp.creator,
          COALESCE(fl.cnt, 0),
          COALESCE(fo.cnt, 0),
          COALESCE(p.cnt, 0)
        FROM batch_profiles bp
        LEFT JOIN followers_counts fl ON fl.did = bp.creator
        LEFT JOIN follows_counts fo ON fo.did = bp.creator
        LEFT JOIN posts_counts p ON p.did = bp.creator
        ON CONFLICT (did) DO UPDATE
        SET "followersCount" = excluded."followersCount",
            "followsCount" = excluded."followsCount",
            "postsCount" = excluded."postsCount"
        RETURNING did
      )
      SELECT
        MAX(creator) as next_cursor,
        COUNT(*) as processed_count,
        MIN(creator) as batch_start,
        MAX(creator) as batch_end
      FROM batch_profiles;
			`.execute(db);

      console.timeEnd(`backfilling profiles ${i + 1}/${batches}`);

      if (inserted.rows.length === 0) break;
      // @ts-expect-error — row is not typed
      if (inserted.rows[0].processed_count === 0) break;
      // @ts-expect-error — row is not typed
      yield (cursor = inserted.rows[0].next_cursor);
      i++;
    }
  } catch (err) {
    console.error(`backfilling profiles ${i + 1}/${batches}`, err);
    if (err instanceof Error && err.stack) console.error(err.stack);
  }
}

async function* backfillPostValidation(
  { db }: Database,
  state: State,
): AsyncGenerator<string> {
  const limit = 10_000;

  let rowCount = await fastRowCount(db, "post");
  console.log(`post validation row count: ${rowCount}`);

  let batches = Math.ceil(rowCount / limit);
  let i = state.validation.index ?? 0,
    cursor = state.validation.cursor ?? null;
  try {
    while (true) {
      if (i >= batches) {
        rowCount = await fastRowCount(db, "post");
        batches = Math.ceil(rowCount / limit);
      }

      saveState((s) => ({ ...s, validation: { cursor, index: i } }));

      const invalidReplyUpdates: [
        uri: Array<string>,
        invalidReplyRoot: Array<boolean>,
        violatesThreadGate: Array<boolean>,
      ] = [[], [], []];

      console.time(`validating posts ${i + 1}/${batches}`);
      const posts = await db
        .selectFrom("post")
        .innerJoin("post_embed_record as embed", "embed.postUri", "uri")
        .select([
          "replyParent",
          "replyParentCid",
          "replyRoot",
          "replyRootCid",
          "creator",
          "uri",
          "embed.embedUri as embedUri",
        ])
        .where("replyParent", "is not", null)
        .where("replyRoot", "is not", null)
        .where("uri", ">", cursor ?? "")
        .orderBy("uri", "asc")
        .limit(limit)
        .execute();

      if (posts.length === 0) break;

      await Promise.all([validateReplyStatus(), validateEmbeddingRules()]);

      async function validateReplyStatus() {
        console.time(`validating reply status ${i + 1}/${batches}`);

        await Promise.all(
          posts.map(async (post) => {
            if (
              !post.replyParent ||
              !post.replyParentCid ||
              !post.replyRoot ||
              !post.replyRootCid
            )
              return;
            try {
              const { invalidReplyRoot, violatesThreadGate } =
                await validateReply(db, post.creator, {
                  parent: { uri: post.replyParent, cid: post.replyParentCid },
                  root: { uri: post.replyRoot, cid: post.replyRootCid },
                });
              if (invalidReplyRoot || violatesThreadGate) {
                invalidReplyUpdates[0].push(post.uri);
                invalidReplyUpdates[1].push(invalidReplyRoot);
                invalidReplyUpdates[2].push(violatesThreadGate);
              }
            } catch (err) {
              console.error(`validating post ${post.uri}`, err);
              if (err instanceof Error && err.stack) console.error(err.stack);
            }
          }),
        );

        await executeRaw(
          db,
          `
					UPDATE post SET "invalidReplyRoot" = v."invalidReplyRoot", "violatesThreadGate" = v."violatesThreadGate"
					FROM (
						SELECT * FROM unnest($1::text[], $2::boolean[], $3::boolean[]) AS t(uri, "invalidReplyRoot", "violatesThreadGate")
					) as v
					WHERE post.uri = v.uri
					`,
          invalidReplyUpdates,
        );

        console.timeEnd(`validating reply status ${i + 1}/${batches}`);
      }

      async function validateEmbeddingRules() {
        console.time(`validating embedding rules ${i + 1}/${batches}`);

        const embeds: Array<{ parentUri: string; embedUri: string }> = [];
        const violatesEmbeddingRulesUpdates: [
          uri: Array<string>,
          violatesEmbeddingRules: Array<boolean>,
        ] = [[], []];

        for (const post of posts) {
          if (post.embedUri) {
            embeds.push({ parentUri: post.uri, embedUri: post.embedUri });
          }
        }

        const embedsToUpdate = await validatePostEmbedsBulk(db, embeds);
        for (const embed of embedsToUpdate) {
          if (embed.violatesEmbeddingRules) {
            violatesEmbeddingRulesUpdates[0].push(embed.parentUri);
            violatesEmbeddingRulesUpdates[1].push(embed.violatesEmbeddingRules);
          }
        }

        if (violatesEmbeddingRulesUpdates.length) {
          await executeRaw(
            db,
            `
						UPDATE post SET "violatesEmbeddingRules" = v."violatesEmbeddingRules"
						FROM (
							SELECT * FROM unnest($1::text[], $2::boolean[]) AS t(uri, "violatesEmbeddingRules")
						) as v
						WHERE post.uri = v.uri
						`,
            violatesEmbeddingRulesUpdates,
          );
        }

        console.timeEnd(`validating embedding rules ${i + 1}/${batches}`);
      }

      console.timeEnd(`validating posts ${i + 1}/${batches}`);
      yield (cursor = posts[posts.length - 1].uri);
      i++;
    }
  } catch (err) {
    console.error(`validating posts ${i + 1}/${batches}`, err);
    if (err instanceof Error && err.stack) console.error(err.stack);
  }
}

async function validateReply(
  db: DatabaseSchema,
  creator: string,
  reply: ReplyRef,
) {
  const replyRefs = await getReplyRefs(db, reply);
  const invalidRoot =
    !replyRefs.parent || invalidReplyRoot(reply, replyRefs.parent);
  const violatesGate = await violatesThreadGate(
    db,
    creator,
    uriToDid(reply.root.uri),
    replyRefs.root?.record ?? null,
    replyRefs.gate?.record ?? null,
  );
  return { invalidReplyRoot: invalidRoot, violatesThreadGate: violatesGate };
}

async function retryFailedWrites(db: Database) {
  const osClient = new OpenSearchClient({
    node: process.env.OPENSEARCH_URL,
    auth: {
      username: process.env.OPENSEARCH_USERNAME,
      password: process.env.OPENSEARCH_PASSWORD,
    },
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

  const collections = writeWorkerAllocations.flat();

  const failedActorDids = (
    await readFile("./failed-actors.jsonl", "utf8").catch(() => "")
  )
    .split("\n")
    .flatMap((s) => s.split(","))
    .filter((d) => d.startsWith("did:plc:") || d.startsWith("did:web:"));
  const failedSearchDocs = (
    await readFile("./failed-search.jsonl", "utf8").catch(() => "")
  )
    .split("\n")
    .map((s) => {
      try {
        return JSON.parse(s) as PostDoc | ProfileDoc;
      } catch (e) {
        console.error(`Failed to parse failed-search.jsonl line ${s}`);
      }
    })
    .filter((s) => !!s);

  const onBeforeExit = () => {
    fs.writeFileSync("./failed-records.pos", `${recordsPosition}`);
    collections.forEach((c) =>
      fs.writeFileSync(`./failed-${c}.pos`, `${collectionPositions[c]}`),
    );
  };

  const exit = () => {
    onBeforeExit();
    process.exit(1);
  };

  const actorsPromise = (async () => {
    for (const chunk of chunkArray(failedActorDids, 100)) {
      await Promise.all(
        chunk.map(async (did) => {
          await indexingSvc.indexHandle(did, new Date().toISOString());
        }),
      );
    }
  })();

  const searchesPromise = (async () => {
    for (const chunk of chunkArray(failedSearchDocs, 100)) {
      await Promise.all(
        chunk.map(async (doc) => {
          const isPost = "record_rkey" in doc;
          try {
            await osClient.index({
              index: isPost ? POST_INDEX : PROFILE_INDEX,
              body: doc,
            });
          } catch (e) {
            console.warn(
              `Skipping search doc ${doc.did} ${
                isPost ? doc.record_rkey : "profile"
              }, ${e}`,
            );
          }
        }),
      );
    }
  })();

  const seenUris = new LRUCache<string, boolean>({ max: 5_000_000 });

  const recordsStartingPosition = fs.existsSync("./failed-records.pos")
    ? parseInt(await readFile("./failed-records.pos", "utf8"))
    : 0;
  let recordsPosition = 0;
  let collectionPositions: Record<string, number> = {};

  const recordsPromise = (async () => {
    if (!fs.existsSync("./failed-records.jsonl")) return;
    const fstream = fs.createReadStream("./failed-records.jsonl");
    const rl = readline.createInterface({
      input: fstream,
      crlfDelay: Infinity,
    });

    for await (const line of rl) {
      recordsPosition++;
      if (recordsPosition < recordsStartingPosition) continue;
      try {
        const msg = JSON.parse(line) as {
          uri: string;
          cid: string;
          timestamp: string;
          obj: Record<string, unknown>;
        };

        if (seenUris.has(msg.uri)) continue;

        if (
          !isCanonicalResourceUri(msg.uri) ||
          !isCid(msg.cid) ||
          isNaN(new Date(msg.timestamp).getTime())
        ) {
          console.log(`Skipping invalid record message ${JSON.stringify(msg)}`);
          continue;
        }

        const uri = new AtUri(msg.uri);
        fixCids(msg.obj);
        if (!is(uri.collection, msg.obj)) {
          console.log(`Skipping invalid record ${JSON.stringify(msg.obj)}`);
          continue;
        }

        await indexingSvc.indexRecord(
          new AtUri(msg.uri),
          CID.parse(msg.cid),
          jsonToLex(msg.obj),
          WriteOpAction.Create,
          msg.timestamp,
          { disableNotifs: true, skipValidation: true },
        );

        seenUris.set(msg.uri, true);
      } catch (e) {
        console.error(
          `Failed to parse failed-records.jsonl line ${recordsPosition}`,
        );
        if (`${e}`.includes("Out of memory")) exit();
      }
    }
  })();

  const collectionPromises = collections.map(async (collection) => {
    if (!fs.existsSync(`./failed-${collection}.jsonl`)) return;
    const fstream = fs.createReadStream(`./failed-${collection}.jsonl`);
    const rl = readline.createInterface({
      input: fstream,
      crlfDelay: Infinity,
    });

    const startingPosition = fs.existsSync(`./failed-${collection}.pos`)
      ? parseInt(await readFile(`./failed-${collection}.pos`, "utf8"))
      : 0;

    collectionPositions[collection] = 0;

    for await (const line of rl) {
      collectionPositions[collection]++;
      if (collectionPositions[collection] < startingPosition) continue;
      try {
        const msg = JSON.parse(line) as {
          uri: string;
          cid: string;
          timestamp: string;
          obj: Record<string, unknown>;
        };

        if (seenUris.has(msg.uri)) continue;

        if (
          !isCanonicalResourceUri(msg.uri) ||
          !isCid(msg.cid) ||
          isNaN(new Date(msg.timestamp).getTime())
        )
          continue;

        const uri = new AtUri(msg.uri);
        fixCids(msg.obj);
        if (!is(uri.collection, msg.obj)) {
          console.log(`Skipping invalid record ${JSON.stringify(msg.obj)}`);
          continue;
        }

        await indexingSvc.indexRecord(
          new AtUri(msg.uri),
          CID.parse(msg.cid),
          jsonToLex(msg.obj),
          WriteOpAction.Create,
          msg.timestamp,
          { disableNotifs: true, skipValidation: true },
        );

        seenUris.set(msg.uri, true);
      } catch (e) {
        console.error(
          `Failed to parse failed-${collection}.jsonl line ${
            collectionPositions[collection]
          }`,
        );
        if (`${e}`.includes("Out of memory")) exit();
      }
    }
  });

  process.on("beforeExit", onBeforeExit);
  process.on("exit", onBeforeExit);

  await Promise.all([
    actorsPromise,
    searchesPromise,
    ...collectionPromises,
    recordsPromise,
  ]);

  process.off("beforeExit", onBeforeExit);
  process.off("exit", onBeforeExit);

  console.log("Done reindexing failed records");
}

async function validatePostEmbedsBulk(
  db: DatabaseSchema,
  embeds: Array<{ parentUri: string; embedUri: string }>,
) {
  const uris = embeds.reduce(
    (acc, { parentUri, embedUri }) => {
      const postgateRecordUri = embedUri.replace(
        "app.bsky.feed.post",
        "app.bsky.feed.postgate",
      );
      acc[postgateRecordUri] = { parentUri, embedUri };
      return acc;
    },
    {} as Record<string, { parentUri: string; embedUri: string }>,
  );

  const { rows: postgateRecords } = await executeRaw<
    DatabaseSchemaType["record"]
  >(
    db,
    `
    SELECT * FROM record WHERE record.uri = ANY($1::text[])
    `,
    [Object.keys(uris)],
  );

  return postgateRecords.reduce(
    (acc, record) => {
      if (!record.json || !uris[record.uri]) return acc;
      const {
        embeddingRules: { canEmbed },
      } = parsePostgate({
        gate: jsonStringToLex(record.json) as PostgateRecord,
        viewerDid: uriToDid(uris[record.uri].parentUri),
        authorDid: uriToDid(uris[record.uri].embedUri),
      });
      acc.push({
        parentUri: uris[record.uri].parentUri,
        embedUri: uris[record.uri].embedUri,
        violatesEmbeddingRules: !canEmbed,
      });
      return acc;
    },
    [] as Array<{
      parentUri: string;
      embedUri: string;
      violatesEmbeddingRules: boolean;
    }>,
  );
}

async function getReplyRefs(db: DatabaseSchema, reply: ReplyRef) {
  const replyRoot = reply.root.uri;
  const replyParent = reply.parent.uri;
  const replyGate = postUriToThreadgateUri(replyRoot);
  const results = await db
    .selectFrom("record")
    .where("record.uri", "in", [replyRoot, replyGate, replyParent])
    .leftJoin("post", "post.uri", "record.uri")
    .selectAll("post")
    .select(["record.uri", "json"])
    .execute();
  const root = results.find((ref) => ref.uri === replyRoot);
  const parent = results.find((ref) => ref.uri === replyParent);
  const gate = results.find((ref) => ref.uri === replyGate);
  return {
    root: root && {
      uri: root.uri,
      invalidReplyRoot: root.invalidReplyRoot,
      record: jsonStringToLex(root.json) as PostRecord,
    },
    parent: parent && {
      uri: parent.uri,
      invalidReplyRoot: parent.invalidReplyRoot,
      record: jsonStringToLex(parent.json) as PostRecord,
    },
    gate: gate && {
      uri: gate.uri,
      record: jsonStringToLex(gate.json) as GateRecord,
    },
  };
}

async function fastRowCount(db: DatabaseSchema, table: string) {
  return sql<{ row_count: number }>`
		SELECT ((reltuples::bigint / relpages::bigint)::bigint * (pg_relation_size(oid) / current_setting('block_size')::int))::bigint
		AS row_count FROM pg_class
		WHERE oid = ${sql.literal(table)}::regclass
	`
    .execute(db)
    .then((res) => res.rows[0].row_count);
}

async function alterDbSettings(db: Database) {
  // return Promise.all(
  // 	Object.entries(DB_SETTINGS).map(([setting, value]) =>
  // 		db.pool.query(`ALTER SYSTEM SET ${setting} = ${value}`)
  // 	),
  // );
}

function addExitHandlers(db: Database) {
  let reset = false;
  process.on("beforeExit", async () => {
    console.log("Resetting DB settings");
    // await Promise.all(
    // 	Object.keys(DB_SETTINGS).map((setting) =>
    // 		db.pool.query(`ALTER SYSTEM RESET ${setting}`)
    // 	),
    // );

    console.log("Closing DB connection");
    await db.pool.end();
    reset = true;
  });
  process.on("exit", (code) => {
    if (reset) return;
    // console.log(
    // 	Object.keys(DB_SETTINGS).map((setting) => `ALTER SYSTEM RESET ${setting};`).join(" "),
    // );
    console.log(`Exiting with code ${code}`);
  });
}

interface State {
  post: { cursor: string | null; index: number };
  profile: { cursor: string | null; index: number };
  validation: { cursor: string | null; index: number };
}

function loadState(): State {
  if (!fs.existsSync(statePath)) {
    state = {
      post: { cursor: null, index: 0 },
      profile: { cursor: null, index: 0 },
      validation: { cursor: null, index: 0 },
    };
    saveState((s) => s);
    return state;
  }
  return (state = JSON.parse(fs.readFileSync(statePath, "utf-8")));
}

function saveState(updateState: (state: State) => State) {
  fs.writeFileSync(statePath, JSON.stringify(updateState(state), null, 2));
}

function chunkArray<T>(arr: T[], chunkSize: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < arr.length; i += chunkSize) {
    chunks.push(arr.slice(i, i + chunkSize));
  }
  return chunks;
}

function fixCids(obj: any) {
  if (typeof obj !== "object" || !obj) return;
  if (
    obj.uri &&
    obj.cid &&
    obj.cid.$link &&
    (Object.keys(obj).length === 2 ||
      obj.$type === "com.atproto.repo.strongRef")
  ) {
    obj.cid = obj.cid.$link;
  } else {
    for (const key in obj) {
      fixCids(obj[key]);
    }
  }
}
