import { setTimeout as sleep } from 'node:timers/promises';

import logger from './logger.js';

const COOLDOWN_BASE_MS = 30_000;
const COOLDOWN_MAX_MS = 600_000;
/** A quiet host forgets its strikes after this long. */
const STRIKE_DECAY_MS = 600_000;
/** Wake-up jitter so a host's parked queue doesn't stampede back as one wave. */
const WAKE_JITTER_MS = 5_000;

interface HostState {
  strikes: number;
  until: number;
  lastStrike: number;
}

/**
 * 429-driven per-host backpressure. Static per-host caps are sized for the
 * spread-load era; once the crawl tail concentrates onto whichever mushroom
 * holds the deepest backlog, six boxes × cap against ONE host blows through
 * its rate limit (observed: 60k 429s from morel in 10 minutes — and every 429
 * burned a repo attempt, mass-parking repos that a politer pace would have
 * fetched fine). Each 429 arms an exponential cooldown for that host; the
 * scheduler sleeps that host's queue INSIDE its per-host limiter, which by
 * construction never pins global slots, so every other host keeps flowing.
 */
export interface HostPressure {
  /** Called by the retry policy when a fetch came back 429. */
  record429(host: string): void;
  /** Blocks while the host is cooling; returns immediately otherwise. */
  waitWhileCooling(host: string): Promise<void>;
}

export function createHostPressure(): HostPressure {
  const state = new Map<string, HostState>();

  return {
    record429(host: string): void {
      const now = Date.now();
      const prev = state.get(host);
      const strikes =
        prev !== undefined && now - prev.lastStrike < STRIKE_DECAY_MS
          ? prev.strikes + 1
          : 1;
      const cooldown = Math.min(
        COOLDOWN_BASE_MS * 2 ** (strikes - 1),
        COOLDOWN_MAX_MS,
      );
      const until = now + cooldown;
      // Never shorten an existing cooldown: concurrent 429s from one burst
      // all land here, and the longest window must win.
      state.set(host, {
        strikes,
        until: Math.max(until, prev?.until ?? 0),
        lastStrike: now,
      });
      if (strikes === 1 || strikes % 10 === 0) {
        logger.warn({ host, strikes, cooldownMs: cooldown }, 'host cooling');
      }
    },

    async waitWhileCooling(host: string): Promise<void> {
      // Loop: a fresh 429 from an in-flight request can re-arm the cooldown
      // while we sleep, and the re-check catches it.
      for (;;) {
        const cooling = state.get(host);
        if (cooling === undefined) return;
        const wait = cooling.until - Date.now();
        if (wait <= 0) return;
        await sleep(wait + Math.random() * WAKE_JITTER_MS);
      }
    },
  };
}
