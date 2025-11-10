import { createClient, type RedisClientType } from "@redis/client";
import { logger } from "../logger.js";

const JOB_FIELD = "payload";
const BUSY_GROUP_ERR = "BUSYGROUP";

export type RedisStreamClient = RedisClientType<any, any, any>;

export type RedisClientOptions = {
  url: string;
  name?: string;
};

export type RepoJobPayload = {
  did: string;
  pds: string;
  rev?: string;
  batchId?: string;
};

export type RepoJobMessage = {
  id: string;
  payload: RepoJobPayload;
  raw: Record<string, string>;
};

export type AppendRepoJobParams = {
  client: RedisStreamClient;
  stream: string;
  job: RepoJobPayload;
  id?: string;
};

export type ReadJobsParams = {
  client: RedisStreamClient;
  stream: string;
  group: string;
  consumer: string;
  cursor?: string;
  count?: number;
  blockMs?: number;
};

export type AckJobsParams = {
  client: RedisStreamClient;
  stream: string;
  group: string;
  ids: string[];
};

export type ClaimStalledJobsParams = {
  client: RedisStreamClient;
  stream: string;
  group: string;
  consumer: string;
  minIdleMs: number;
  count?: number;
  cursor?: string;
};

export type ClaimStalledJobsResult = {
  nextCursor: string;
  jobs: RepoJobMessage[];
};

export function createRedisStreamClient(opts: RedisClientOptions) {
  const client = createClient({ url: opts.url });
  client.on("error", (err) => {
    logger.error(
      { err, redis_client_name: opts.name ?? "backfill" },
      "Redis client error",
    );
  });
  return client;
}

export async function ensureStream(
  client: RedisStreamClient,
  params: { stream: string; group: string; startId?: string },
) {
  try {
    await client.xGroupCreate(
      params.stream,
      params.group,
      params.startId ?? "0",
      {
        MKSTREAM: true,
      },
    );
  } catch (error) {
    if (!isBusyGroupError(error)) {
      throw error;
    }
  }
}

export async function appendRepoJob({
  client,
  stream,
  job,
  id = "*",
}: AppendRepoJobParams) {
  const payload = JSON.stringify(job);
  return await client.xAdd(stream, id, { [JOB_FIELD]: payload });
}

export async function readJobs({
  client,
  stream,
  group,
  consumer,
  cursor = ">",
  count,
  blockMs,
}: ReadJobsParams) {
  const options = buildReadOptions({ count, blockMs });
  const response = await client.xReadGroup(
    group,
    consumer,
    { key: stream, id: cursor },
    options,
  );
  if (!response) return [];

  const jobs: RepoJobMessage[] = [];
  for (const chunk of response) {
    if (!chunk) continue;
    for (const message of chunk.messages) {
      const parsed = toRepoJobMessage(message);
      if (parsed) {
        jobs.push(parsed);
      }
    }
  }
  return jobs;
}

export async function ackJobs({ client, stream, group, ids }: AckJobsParams) {
  if (ids.length === 0) return 0;
  return await client.xAck(stream, group, ids);
}

export async function claimStalledJobs({
  client,
  stream,
  group,
  consumer,
  minIdleMs,
  count,
  cursor = "0-0",
}: ClaimStalledJobsParams): Promise<ClaimStalledJobsResult> {
  const response = await client.xAutoClaim(
    stream,
    group,
    consumer,
    minIdleMs,
    cursor,
    count ? { COUNT: count } : undefined,
  );
  const jobs: RepoJobMessage[] = [];
  for (const message of response.messages ?? []) {
    if (!message) continue;
    const parsed = toRepoJobMessage(message);
    if (parsed) {
      jobs.push(parsed);
    }
  }
  return {
    nextCursor: response.nextId,
    jobs,
  };
}

export async function getStreamLength(
  client: RedisStreamClient,
  stream: string,
) {
  return await client.xLen(stream);
}

function buildReadOptions(opts: {
  count?: number;
  blockMs?: number;
}): { COUNT?: number; BLOCK?: number } | undefined {
  if (!opts.count && !opts.blockMs) {
    return undefined;
  }
  const options: { COUNT?: number; BLOCK?: number } = {};
  if (typeof opts.count === "number") {
    options.COUNT = opts.count;
  }
  if (typeof opts.blockMs === "number") {
    options.BLOCK = opts.blockMs;
  }
  return options;
}

type RedisCommandArgument = string | Buffer;

type RawStreamMessage = {
  id: string;
  message: Record<string, RedisCommandArgument>;
};

function toRepoJobMessage(message: RawStreamMessage): RepoJobMessage | null {
  const payloadField = message.message[JOB_FIELD];
  if (!payloadField) {
    logger.warn({ messageId: message.id }, "Missing repo job payload field");
    return null;
  }
  const rawPayload = normalizeValue(payloadField);
  try {
    const payload = JSON.parse(rawPayload) as RepoJobPayload;
    if (!isValidRepoJobPayload(payload)) {
      logger.warn({ messageId: message.id }, "Invalid repo job payload");
      return null;
    }
    return {
      id: message.id,
      payload,
      raw: normalizeMessageMap(message.message),
    };
  } catch (error) {
    logger.warn(
      { err: error, messageId: message.id },
      "Failed to parse repo job payload",
    );
    return null;
  }
}

function normalizeValue(value: RedisCommandArgument) {
  return typeof value === "string" ? value : value.toString("utf8");
}

function normalizeMessageMap(message: Record<string, RedisCommandArgument>) {
  const normalized: Record<string, string> = {};
  for (const [key, value] of Object.entries(message)) {
    normalized[key] = normalizeValue(value);
  }
  return normalized;
}

function isValidRepoJobPayload(payload: RepoJobPayload) {
  return (
    payload !== null &&
    typeof payload === "object" &&
    typeof payload.did === "string" &&
    payload.did.length > 0 &&
    typeof payload.pds === "string" &&
    payload.pds.length > 0
  );
}

function isBusyGroupError(error: unknown) {
  return (
    error instanceof Error &&
    (error.message.includes(BUSY_GROUP_ERR) ||
      error.message.includes("BUSYGROUP"))
  );
}
