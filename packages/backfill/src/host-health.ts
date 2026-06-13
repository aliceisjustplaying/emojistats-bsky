import { isStallMessage } from './fetcher.js';
import logger from './logger.js';

/**
 * Consecutive hard failures before a host is declared dead for this run.
 * High on purpose: tripping parks the host's ENTIRE claimable backlog as
 * unreachable, so a flaky-but-alive host must never qualify. Only failure
 * modes that cannot be transient per-repo qualify at all (see classify).
 */
const TRIP_CONSECUTIVE = 30;
/** The failures must also span this long, so one burst can't trip alone. */
const TRIP_MIN_SPAN_MS = 30_000;

/**
 * Stall-specific trip thresholds (lower count, longer span than DNS/legal). A
 * host that goes silent — accepts the socket then never delivers (e.g. the
 * rate-limited atproto.brid.gy bridge that owns a whole drained shard's tail) —
 * is not "dead" but is unworkable now: every fetch eats the full stall budget
 * for nothing. Parking it (rows → unreachable, the deferred final-sweep list,
 * NOT lost) frees the box instead of burning dedi-hours trickling one host.
 * Conservative on purpose: any success / HTTP response resets the streak, so a
 * host making real progress can never trip; only sustained pure silence does.
 */
const TRIP_CONSECUTIVE_STALL = 6;
const TRIP_MIN_SPAN_STALL_MS = 120_000;

/**
 * Host-level deadness detection. The claim scan is host-blind: a PDS whose
 * domain no longer resolves (pds.trump.com: 1.9M pending rows per shard,
 * ENOTFOUND) or that legal-blocks everything (plc.surge.sh: http 451) feeds
 * the scheduler millions of rows that each fail individually, burn retry
 * waves, and crowd healthy hosts out of every 250k-row scan window. Once a
 * host proves dead — N consecutive non-transient failures over a minimum
 * span, zero successes in between — the scheduler bulk-parks its claimable
 * rows as out-of-budget 'unreachable' (the explicit final-sweep list; host
 * down ≠ data gone) and excludes the host from claim scans for the rest of
 * the run.
 */
export type DeadHostKind = 'dns' | 'legal' | 'stall';

export function classifyDeadness(message: string): DeadHostKind | null {
  // ENOTFOUND is DNS NXDOMAIN — a domain that stopped existing. EAI_AGAIN
  // (resolver trouble) deliberately does NOT count: that is our problem.
  if (message.includes('ENOTFOUND')) return 'dns';
  // 451: the host exists and refuses everything for legal reasons. Per-repo
  // retries cannot change the answer.
  if (/http 451\b/.test(message)) return 'legal';
  // Sustained stalls (half-open/silent socket) — parked under the higher
  // stall thresholds so a briefly-slow host recovers but a dead-silent one
  // (rate-limited bridge tail) stops eating the box.
  if (isStallMessage(message)) return 'stall';
  return null;
}

interface HostHealthState {
  consecutive: number;
  firstAt: number;
  kind: DeadHostKind;
  dead: boolean;
}

export interface HostHealth {
  /** Feed every classified repo failure; returns true when this call tripped the host. */
  recordFailure(host: string, message: string): void;
  /** Any successful response (including 429/404 — the host is alive). */
  recordSuccess(host: string): void;
  isDead(host: string): boolean;
  /** All hosts declared dead this run (for claim-scan exclusion). */
  deadHosts(): string[];
  /** Hosts that tripped since the last call — the scheduler parks these. */
  takeNewlyTripped(): string[];
  /** Seed a host as dead without tripping (ledger-persisted verdicts at startup). */
  markDead(host: string): void;
}

export function createHostHealth(): HostHealth {
  const state = new Map<string, HostHealthState>();
  let newlyTripped: string[] = [];

  return {
    recordFailure(host: string, message: string): void {
      const kind = classifyDeadness(message);
      const prev = state.get(host);
      if (kind === null) {
        // Any HTTP status — 5xx included — is proof of life: a server
        // resolved, accepted TCP+TLS and answered. It must clear an
        // ENOTFOUND/451 streak, or a host flapping between DNS failures
        // and real responses could accumulate 30 "consecutive" classified
        // failures across interleaved evidence of being alive. Pure
        // network errors (timeouts, resets) prove nothing either way and
        // leave the streak untouched.
        if (prev !== undefined && !prev.dead && /http \d{3}\b/.test(message))
          state.delete(host);
        return;
      }
      const now = Date.now();
      if (prev?.dead) return;
      // A kind change starts a fresh streak: stall and dns/legal have different
      // trip thresholds, so a few ENOTFOUNDs must not let a single later stall
      // (or vice-versa) inherit the count and trip under the wrong threshold.
      const next: HostHealthState =
        prev === undefined || prev.kind !== kind
          ? { consecutive: 1, firstAt: now, kind, dead: false }
          : { ...prev, consecutive: prev.consecutive + 1, kind };
      const tripCount =
        kind === 'stall' ? TRIP_CONSECUTIVE_STALL : TRIP_CONSECUTIVE;
      const tripSpan =
        kind === 'stall' ? TRIP_MIN_SPAN_STALL_MS : TRIP_MIN_SPAN_MS;
      if (next.consecutive >= tripCount && now - next.firstAt >= tripSpan) {
        next.dead = true;
        newlyTripped.push(host);
        logger.warn(
          { host, kind, consecutive: next.consecutive },
          'host declared dead for this run',
        );
      }
      state.set(host, next);
    },

    recordSuccess(host: string): void {
      // Deadness is sticky for the run: by the time a host trips, its rows
      // are being parked, and un-parking on one stray success would thrash.
      const prev = state.get(host);
      if (prev === undefined || prev.dead) return;
      state.delete(host);
    },

    isDead(host: string): boolean {
      return state.get(host)?.dead === true;
    },

    deadHosts(): string[] {
      return [...state.entries()]
        .filter(([, s]) => s.dead)
        .map(([host]) => host);
    },

    takeNewlyTripped(): string[] {
      const tripped = newlyTripped;
      newlyTripped = [];
      return tripped;
    },

    markDead(host: string): void {
      state.set(host, {
        consecutive: 0,
        firstAt: Date.now(),
        kind: 'dns',
        dead: true,
      });
    },
  };
}
