type NumericEnv = number | undefined;

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var ${name}`);
  }
  return value;
}

function optionalNumber(name: string): NumericEnv {
  const raw = process.env[name];
  if (raw === undefined) return undefined;
  const parsed = Number(raw);
  if (Number.isNaN(parsed)) {
    throw new Error(`Env var ${name} must be numeric`);
  }
  return parsed;
}

export type RedisQueueConfig = {
  redisUrl: string;
  streamName: string;
  groupName: string;
};

export type ProducerQueueConfig = RedisQueueConfig & {
  highWaterMark: number;
  backpressurePollMs: number;
};

export type ConsumerQueueConfig = RedisQueueConfig & {
  consumerName: string;
  readCount: number;
  blockMs: number;
  stalledMinIdleMs: number;
  stalledClaimCount: number;
};

export function loadRedisQueueConfig(): RedisQueueConfig {
  return {
    redisUrl: requireEnv("BACKFILL_REDIS_URL"),
    streamName: process.env.BACKFILL_STREAM_NAME ?? "emoji:repo-jobs",
    groupName: process.env.BACKFILL_GROUP_NAME ?? "emoji-backfill",
  };
}

export function loadProducerQueueConfig(): ProducerQueueConfig {
  const base = loadRedisQueueConfig();
  return {
    ...base,
    highWaterMark: optionalNumber("BACKFILL_HIGH_WATER") ?? 50_000,
    backpressurePollMs: optionalNumber("BACKFILL_BACKPRESSURE_POLL_MS") ?? 1000,
  };
}

export function loadConsumerQueueConfig(): ConsumerQueueConfig {
  const base = loadRedisQueueConfig();
  const consumerName = process.env.BACKFILL_CONSUMER_NAME;
  if (!consumerName) {
    throw new Error("BACKFILL_CONSUMER_NAME is required for consumer workers");
  }
  return {
    ...base,
    consumerName,
    readCount: optionalNumber("BACKFILL_READ_COUNT") ?? 32,
    blockMs: optionalNumber("BACKFILL_BLOCK_MS") ?? 1000,
    stalledMinIdleMs:
      optionalNumber("BACKFILL_STALLED_MIN_IDLE_MS") ?? 5 * 60 * 1000,
    stalledClaimCount: optionalNumber("BACKFILL_STALLED_CLAIM_COUNT") ?? 128,
  };
}
