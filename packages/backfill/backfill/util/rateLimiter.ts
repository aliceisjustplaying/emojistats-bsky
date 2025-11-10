import { setTimeout as sleep } from "node:timers/promises";

export type RateLimiterContext = Record<string, string | number | undefined>;

export type RateLimiterOptions = {
  capacity: number;
  refillPerSec: number;
  sleepIntervalMs?: number;
  defaultContext?: RateLimiterContext;
  onWait?: (waitMs: number, context?: RateLimiterContext) => void;
};

export class RateLimiter {
  private tokens: number;
  private lastRefill: number;
  private readonly sleepIntervalMs: number;
  private readonly onWait?: (
    waitMs: number,
    context?: RateLimiterContext,
  ) => void;
  private readonly defaultContext?: RateLimiterContext;

  constructor(private readonly options: RateLimiterOptions) {
    this.tokens = options.capacity;
    this.lastRefill = Date.now();
    this.sleepIntervalMs = options.sleepIntervalMs ?? 50;
    this.onWait = options.onWait;
    this.defaultContext = options.defaultContext;
  }

  async take(context?: RateLimiterContext) {
    this.refill();
    let waitedMs = 0;
    while (this.tokens < 1) {
      await sleep(this.sleepIntervalMs);
      waitedMs += this.sleepIntervalMs;
      this.refill();
    }
    this.tokens -= 1;
    if (this.onWait) {
      const mergedContext = { ...this.defaultContext, ...context };
      this.onWait(waitedMs, mergedContext);
    }
  }

  private refill() {
    const now = Date.now();
    const elapsedSeconds = (now - this.lastRefill) / 1000;
    if (elapsedSeconds <= 0) return;
    this.tokens = Math.min(
      this.options.capacity,
      this.tokens + elapsedSeconds * this.options.refillPerSec,
    );
    this.lastRefill = now;
  }
}
