import { setTimeout as sleep } from "node:timers/promises";
import { loadConfig } from "../backfill/config.js";
import {
  loadProducerQueueConfig,
  type ProducerQueueConfig,
} from "../backfill/queue/config.js";
import {
  appendRepoJob,
  createRedisStreamClient,
  ensureStream,
  getStreamLength,
  type RedisStreamClient,
} from "../backfill/queue/redisStream.js";
import {
  fetchPdses,
  getPdsCursorValue,
  listReposPage,
  markPdsDone,
  setCursorCachePath,
  setPdsCursorValue,
} from "../backfill/util/fetch.js";
import { loadAllowlist } from "../backfill/allowlist.js";
import { logger } from "../backfill/logger.js";

async function main() {
  const config = loadConfig();
  setCursorCachePath(config.cursorCachePath);

  const queueConfig = loadProducerQueueConfig();
  const allowlist = await loadAllowlist(config.allowlistPath);

  const redis = createRedisStreamClient({
    url: queueConfig.redisUrl,
    name: "backfill-producer",
  });
  await redis.connect();
  try {
    await ensureStream(redis, {
      stream: queueConfig.streamName,
      group: queueConfig.groupName,
    });
    await produceRepos({
      redis,
      queueConfig,
      allowlist,
    });
  } finally {
    await redis.quit();
  }
}

type ProduceContext = {
  redis: RedisStreamClient;
  queueConfig: ProducerQueueConfig;
  allowlist: Set<string> | null;
};

async function produceRepos({ redis, queueConfig, allowlist }: ProduceContext) {
  const pdses = await fetchPdses();
  let totalEnqueued = 0;
  for (const pds of pdses) {
    const cursor = getPdsCursorValue(pds);
    if (cursor === "DONE") {
      continue;
    }
    const enqueued = await produceForPds({
      redis,
      queueConfig,
      allowlist,
      pds,
      startingCursor: cursor ?? "",
    });
    totalEnqueued += enqueued;
  }
  logger.info({ totalEnqueued }, "Producer finished scanning PDS hosts");
}

type ProduceForPdsArgs = ProduceContext & {
  pds: string;
  startingCursor: string;
};

async function produceForPds({
  redis,
  queueConfig,
  allowlist,
  pds,
  startingCursor,
}: ProduceForPdsArgs) {
  let cursor = startingCursor;
  let enqueued = 0;
  while (true) {
    const page = await listReposPage(pds, cursor);
    if (!page) {
      logger.warn(
        { pds, cursor },
        "Stopping PDS enumeration due to fetch failure",
      );
      break;
    }
    const repos = page.repos ?? [];
    for (const repo of repos) {
      if (!repo.did) continue;
      if (allowlist && !allowlist.has(repo.did)) continue;
      await waitForStreamCapacity(redis, queueConfig);
      await appendRepoJob({
        client: redis,
        stream: queueConfig.streamName,
        job: {
          did: repo.did,
          pds,
        },
      });
      enqueued++;
    }
    if (!page.cursor || page.cursor === cursor) {
      markPdsDone(pds);
      logger.info({ pds, enqueued }, "PDS fully queued");
      break;
    }
    setPdsCursorValue(pds, page.cursor);
    cursor = page.cursor;
  }
  return enqueued;
}

async function waitForStreamCapacity(
  redis: RedisStreamClient,
  queueConfig: ProducerQueueConfig,
) {
  let delay = queueConfig.backpressurePollMs;
  while (true) {
    const length = await getStreamLength(redis, queueConfig.streamName);
    if (length < queueConfig.highWaterMark) {
      return;
    }
    logger.debug(
      { length, highWater: queueConfig.highWaterMark },
      "Producer waiting for stream capacity",
    );
    await sleep(delay);
    delay = Math.min(delay * 2, 30_000);
  }
}

await main().catch((err) => {
  logger.error({ err }, "Producer crashed");
  process.exitCode = 1;
});
