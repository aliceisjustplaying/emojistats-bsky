import logger from './logger.js';

const COOLDOWN_BASE_MS = 30_000;
const COOLDOWN_MAX_MS = 600_000;
/** A quiet host forgets its strikes after this long. */
const STRIKE_DECAY_MS = 600_000;

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
 * fetched fine). Each 429 arms an exponential cooldown for that host.
 *
 * Consumers must never WAIT on a cooldown while holding resources: v1 of this
 * slept inside the per-host limiter, and within minutes the in-flight pool
 * was 3,072 parked repos with `fetching: 2` — the deepest host's claims
 * starved every healthy host. Instead, the scheduler skips cooling hosts at
 * claim time and requeues (markThrottled) any repo whose host started
 * cooling after its claim, so cooldown never occupies a slot.
 */
export interface HostPressure {
  /** Called by the retry policy when a fetch came back 429. */
  record429(host: string): void;
  /** Remaining cooldown for the host; 0 when it is fine to fetch. */
  coolingMs(host: string): number;
  /** True while a host-level cooldown is active. */
  isCooling(host: string): boolean;
  /** Earliest active host cooldown wake-up, or undefined when none are active. */
  nextWake(): number | undefined;
}

export function createHostPressure(): HostPressure {
  const state = new Map<string, HostState>();

  return {
    record429(host: string): void {
      const now = Date.now();
      const prev = state.get(host);
      if (prev !== undefined && now < prev.until) {
        const cooldown = Math.min(
          COOLDOWN_BASE_MS * 2 ** (prev.strikes - 1),
          COOLDOWN_MAX_MS,
        );
        state.set(host, {
          strikes: prev.strikes,
          until: Math.max(prev.until, now + cooldown),
          lastStrike: now,
        });
        return;
      }
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

    coolingMs(host: string): number {
      const cooling = state.get(host);
      if (cooling === undefined) return 0;
      return Math.max(0, cooling.until - Date.now());
    },

    isCooling(host: string): boolean {
      const cooling = state.get(host);
      return cooling !== undefined && cooling.until > Date.now();
    },

    nextWake(): number | undefined {
      const now = Date.now();
      let wake: number | undefined;
      for (const cooling of state.values()) {
        if (cooling.until <= now) continue;
        wake =
          wake === undefined ? cooling.until : Math.min(wake, cooling.until);
      }
      return wake;
    },
  };
}
