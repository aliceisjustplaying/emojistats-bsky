import { PER_HOST_CONCURRENCY, PER_HOST_CONCURRENCY_BSKY } from './config.js';
import logger from './logger.js';

const COOLDOWN_BASE_MS = 5_000;
const COOLDOWN_MAX_MS = 120_000;
/** A quiet host forgets its strikes after this long. */
const STRIKE_DECAY_MS = 120_000;
/** Successes between additive cap raises (the AI in AIMD). */
const CAP_RAISE_EVERY = 20;
/** A host this long without a 429 gets its full static cap back. */
const CAP_AMNESTY_MS = 600_000;

// bsky.social is the entryway fronting every mushroom: millions of early
// accounts' PLC tails still point there (the fetcher follows the DID doc on
// claim). With the third-party cap of 2 it became the whole fleet's
// bottleneck — two slots gating a 168-deep queue per box on launch night.
export const isBskyInfra = (host: string): boolean =>
  host.endsWith('.bsky.network') ||
  host === 'bsky.social' ||
  host.endsWith('//bsky.social');
export const hostCapFor = (host: string): number =>
  isBskyInfra(host) ? PER_HOST_CONCURRENCY_BSKY : PER_HOST_CONCURRENCY;

interface HostState {
  strikes: number;
  until: number;
  lastStrike: number;
  /** Dynamic concurrency cap; undefined until the first 429. */
  cap?: number;
  successStreak: number;
}

/**
 * 429-driven per-host backpressure. Static per-host caps are sized for the
 * spread-load era; once the crawl tail concentrates onto whichever mushroom
 * holds the deepest backlog, six boxes × cap against ONE host blows through
 * its rate limit (observed: 60k 429s from morel in 10 minutes — and every 429
 * burned a repo attempt, mass-parking repos that a politer pace would have
 * fetched fine).
 *
 * v2 ran pure exponential cooldowns (30s → 10 min). On a tail concentrated
 * onto one deep host that converges to ~1% duty cycle: the host serves a
 * burst at full cap for seconds, the first 429 re-arms 10 dark minutes,
 * repeat. v3 is AIMD instead: a 429 burst HALVES the host's concurrency cap
 * (multiplicative decrease) and arms only a short cooldown; sustained
 * successes raise the cap back one slot at a time (additive increase). The
 * cap converges to just under whatever the host actually tolerates and stays
 * there, instead of oscillating between "full blast" and "dark".
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
  /** Called when a host answered a request (any non-429 response). */
  recordSuccess(host: string): void;
  /** Remaining cooldown for the host; 0 when it is fine to fetch. */
  coolingMs(host: string): number;
  /** True while a host-level cooldown is active. */
  isCooling(host: string): boolean;
  /** Current AIMD concurrency cap for the host (≤ the static cap). */
  effectiveCap(host: string): number;
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
        // Concurrent 429s from one burst all land here; the burst already
        // halved the cap, so only extend the window — never shorten it.
        const cooldown = Math.min(
          COOLDOWN_BASE_MS * 2 ** (prev.strikes - 1),
          COOLDOWN_MAX_MS,
        );
        state.set(host, {
          ...prev,
          until: Math.max(prev.until, now + cooldown),
          lastStrike: now,
        });
        return;
      }
      const staticCap = hostCapFor(host);
      const strikes =
        prev !== undefined && now - prev.lastStrike < STRIKE_DECAY_MS
          ? prev.strikes + 1
          : 1;
      const cap = Math.max(1, Math.floor((prev?.cap ?? staticCap) / 2));
      const cooldown = Math.min(
        COOLDOWN_BASE_MS * 2 ** (strikes - 1),
        COOLDOWN_MAX_MS,
      );
      state.set(host, {
        strikes,
        // Never shorten an existing cooldown: the longest window must win.
        until: Math.max(now + cooldown, prev?.until ?? 0),
        lastStrike: now,
        cap,
        successStreak: 0,
      });
      if (strikes === 1 || strikes % 10 === 0) {
        logger.warn(
          { host, strikes, cap, cooldownMs: cooldown },
          'host cooling',
        );
      }
    },

    recordSuccess(host: string): void {
      const prev = state.get(host);
      if (prev?.cap === undefined) return;
      const staticCap = hostCapFor(host);
      const now = Date.now();
      if (now - prev.lastStrike >= CAP_AMNESTY_MS) {
        state.delete(host);
        return;
      }
      const successStreak = prev.successStreak + 1;
      const cap =
        successStreak % CAP_RAISE_EVERY === 0
          ? Math.min(staticCap, prev.cap + 1)
          : prev.cap;
      state.set(host, { ...prev, cap, successStreak });
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

    effectiveCap(host: string): number {
      const staticCap = hostCapFor(host);
      const cap = state.get(host)?.cap;
      return cap === undefined ? staticCap : Math.min(staticCap, cap);
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
