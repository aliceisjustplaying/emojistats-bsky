import {
  CAR_MAX_BYTES,
  REPO_FETCH_STALL_MS,
  REPO_FETCH_TIMEOUT_MS,
  USER_AGENT,
} from './config.js';

/**
 * Failure classification — drives the ledger transitions in crawl.ts:
 *
 *   400 RepoDeactivated             TerminalFetchError('deactivated')
 *   400 RepoTakendown               TerminalFetchError('takendown')
 *   400 RepoSuspended               TerminalFetchError('takendown')  — closest available status
 *   400 RepoNotFound                TerminalFetchError('failed')
 *   429 / 5xx                       RetryableError(transient) honoring Retry-After
 *   network / DNS / timeout         RetryableError(transient)
 *   other 4xx or unrecognized 400   RetryableError(non-transient) → 'failed' after MAX_ATTEMPTS
 *   body over CAR_MAX_BYTES         QuarantineError, request aborted (safety valve)
 */

export class RetryableError extends Error {
  readonly retryAfterMs: number | undefined;
  readonly rateLimit: RateLimitHint | undefined;
  /** Clearly transient (429/5xx/network/timeout) retries in waves forever; the rest gets MAX_ATTEMPTS. */
  readonly transient: boolean;

  constructor(
    message: string,
    opts: {
      transient: boolean;
      retryAfterMs?: number;
      rateLimit?: RateLimitHint;
      cause?: unknown;
    },
  ) {
    super(message, { cause: opts.cause });
    this.name = 'RetryableError';
    this.transient = opts.transient;
    this.retryAfterMs = opts.retryAfterMs;
    this.rateLimit = opts.rateLimit;
  }
}

export type TerminalFetchStatus = 'deactivated' | 'takendown' | 'failed';

export class TerminalFetchError extends Error {
  readonly status: TerminalFetchStatus;
  readonly rateLimit: RateLimitHint | undefined;

  constructor(
    status: TerminalFetchStatus,
    message: string,
    rateLimit?: RateLimitHint,
  ) {
    super(message);
    this.name = 'TerminalFetchError';
    this.status = status;
    this.rateLimit = rateLimit;
  }
}

export class QuarantineError extends Error {
  constructor(message: string, opts?: ErrorOptions) {
    super(message, opts);
    this.name = 'QuarantineError';
  }
}

/**
 * Marker embedded in every stall rejection so the retry policy and host-health
 * can recognise a half-open/silent-socket failure (distinct from a 429, a real
 * HTTP error, or a plain timeout) and drive stall-specific cooling/parking.
 */
export const STALL_REASON = 'stalled: no progress';

/** True when a failure message came from a withProgressTimeout stall. */
export function isStallMessage(message: string): boolean {
  return message.includes(STALL_REASON);
}

/**
 * Settles `promise`, OR rejects after `ms` of no settlement — whichever first.
 * The reject is driven by our OWN timer, so it fires even when the abort never
 * reaches a half-open socket and the wrapped read()/fetch() hangs forever; the
 * caller's job then completes and frees its concurrency slot. onTimeout fires
 * the AbortController as a best-effort socket kill, but correctness does not
 * depend on it landing. The late settlement of a hung promise is harmless: the
 * outer promise has already settled, so the then/catch below are no-ops.
 * Exported for tests.
 */
export function withProgressTimeout<T>(
  promise: Promise<T>,
  ms: number,
  phase: string,
  onTimeout: () => void,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      onTimeout();
      reject(new Error(`${STALL_REASON} for ${ms}ms during ${phase}`));
    }, ms);
    timer.unref();
  });
  // Promise.race attaches a handler to BOTH inputs, so a hung read()/fetch()
  // that rejects late (after the timeout already won) is delivered to race's
  // own settled-and-ignored handler — never an unhandled rejection.
  return Promise.race([promise, timeout]).finally(() => {
    clearTimeout(timer);
  });
}

/**
 * THE pds_host normalization — every writer of the ledger column (enumerate,
 * refreshHost) goes through here so the two can never drift. https endpoints
 * (the overwhelming majority) store the bare host, which keeps every existing
 * ledger row, host-grouping key and known-host equality check (bsky.social)
 * unchanged; the rare http endpoint stores the full 'http://host' string so
 * the scheme survives the round trip to fetchRepoCar below. url.host (not
 * hostname) keeps a nonstandard port — it disambiguates dev PDSes.
 */
export function pdsHostFromEndpoint(endpoint: string): string | undefined {
  try {
    const url = new URL(endpoint);
    const host = url.host.toLowerCase();
    if (host === '') return undefined;
    if (url.protocol === 'https:') return host;
    if (url.protocol === 'http:') return `http://${host}`;
    return undefined;
  } catch {
    return undefined;
  }
}

export interface FetchedCar {
  response: Response;
  rateLimit: RateLimitHint;
  /** CAR_MAX_BYTES is a high safety valve; stream errors are always RetryableError or QuarantineError. */
  body: ReadableStream<Uint8Array>;
  /** Bytes pulled through so far; the final value is the ledger's car_bytes. */
  bytesRead(): number;
}

export interface RateLimitHint {
  limit?: number;
  remaining?: number;
  resetAtMs?: number;
  windowMs?: number;
  retryAfterMs?: number;
}

const TERMINAL_BY_XRPC_ERROR: Record<string, TerminalFetchStatus> = {
  RepoDeactivated: 'deactivated',
  RepoTakendown: 'takendown',
  RepoSuspended: 'takendown',
  RepoNotFound: 'failed',
};

function parseRetryAfter(header: string | null): number | undefined {
  if (header === null || header === '') return undefined;
  const seconds = Number(header);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const dateMs = Date.parse(header);
  if (Number.isFinite(dateMs)) return Math.max(0, dateMs - Date.now());
  return undefined;
}

function parsePositiveNumber(value: string | null): number | undefined {
  if (value === null || value.trim() === '') return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : undefined;
}

function parseReset(value: string | null): number | undefined {
  if (value === null || value.trim() === '') return undefined;
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    // Bluesky PDSes currently send epoch seconds; RFC-style delta seconds are
    // also accepted for other servers.
    return numeric > 1_000_000_000
      ? numeric * 1000
      : Date.now() + numeric * 1000;
  }
  const dateMs = Date.parse(value);
  return Number.isFinite(dateMs) ? dateMs : undefined;
}

function parsePolicyWindow(policy: string | null): number | undefined {
  if (policy === null) return undefined;
  const window = /(?:^|[;,]\s*)w=(\d+(?:\.\d+)?)/i.exec(policy)?.[1];
  if (window === undefined) return undefined;
  const seconds = Number(window);
  return Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : undefined;
}

export function parseRateLimitHeaders(headers: Headers): RateLimitHint {
  const retryAfterMs = parseRetryAfter(headers.get('retry-after'));
  return {
    limit:
      parsePositiveNumber(headers.get('ratelimit-limit')) ??
      parsePositiveNumber(headers.get('x-ratelimit-limit')),
    remaining:
      parsePositiveNumber(headers.get('ratelimit-remaining')) ??
      parsePositiveNumber(headers.get('x-ratelimit-remaining')),
    resetAtMs:
      parseReset(headers.get('ratelimit-reset')) ??
      parseReset(headers.get('x-ratelimit-reset')),
    windowMs:
      parsePolicyWindow(headers.get('ratelimit-policy')) ??
      parsePolicyWindow(headers.get('x-ratelimit-policy')),
    retryAfterMs,
  };
}

function describe(err: unknown): string {
  if (err instanceof Error) {
    if (err.name === 'TimeoutError')
      return `timed out after ${REPO_FETCH_TIMEOUT_MS}ms`;
    const cause =
      err.cause instanceof Error && err.cause.message !== ''
        ? ` (${err.cause.message})`
        : '';
    return `${err.message}${cause}`;
  }
  return String(err);
}

async function classifyHttpError(
  response: Response,
  did: string,
  host: string,
  abort: AbortController,
): Promise<Error> {
  const retryAfterMs = parseRetryAfter(response.headers.get('retry-after'));
  const rateLimit = parseRateLimitHeaders(response.headers);

  let xrpcError: string | undefined;
  let detail = '';
  try {
    // Guard the error-body read too: a non-OK response whose body half-opens
    // would otherwise hang here (past the OK-path guard), re-leaking the slot.
    // On stall this rejects into the catch below and we classify on status
    // alone — correct for the 429/5xx and unknown-4xx branches that follow.
    detail = (
      await withProgressTimeout(
        response.text(),
        REPO_FETCH_STALL_MS,
        'error-body',
        () => abort.abort(),
      )
    ).slice(0, 500);
    const parsed: unknown = JSON.parse(detail);
    if (
      parsed !== null &&
      typeof parsed === 'object' &&
      typeof (parsed as { error?: unknown }).error === 'string'
    ) {
      xrpcError = (parsed as { error: string }).error;
    }
  } catch {
    // body unreadable or not JSON; whatever text we got is already in detail
  }

  const label = `getRepo ${did}@${host}: http ${response.status}${xrpcError === undefined ? '' : ` ${xrpcError}`}`;

  if (response.status === 400 && xrpcError !== undefined) {
    const terminal = TERMINAL_BY_XRPC_ERROR[xrpcError];
    if (terminal !== undefined)
      return new TerminalFetchError(terminal, label, rateLimit);
  }
  if (response.status === 429 || response.status >= 500) {
    return new RetryableError(label, {
      transient: true,
      retryAfterMs,
      rateLimit,
    });
  }
  return new RetryableError(`${label} ${detail}`.trim(), {
    transient: false,
    retryAfterMs,
    rateLimit,
  });
}

export async function fetchRepoCar(
  pdsHost: string,
  did: string,
): Promise<FetchedCar> {
  // pds_host carries a scheme only when it isn't https (see
  // pdsHostFromEndpoint); a bare host means https. The prefix/slash trims also
  // tolerate hand-edited ledger rows.
  const host = pdsHost.replace(/^https?:\/\//, '').replace(/\/+$/, '');
  const origin = pdsHost.startsWith('http://')
    ? `http://${host}`
    : `https://${host}`;
  const url = `${origin}/xrpc/com.atproto.sync.getRepo?did=${encodeURIComponent(did)}`;

  const abort = new AbortController();
  // One budget for connect + headers + full body download.
  const signal = AbortSignal.any([
    AbortSignal.timeout(REPO_FETCH_TIMEOUT_MS),
    abort.signal,
  ]);

  let response: Response;
  try {
    // Connect + TLS + headers must show progress within the stall budget; a
    // dead host that accepts the socket but never answers would otherwise hang
    // here without the AbortSignal.timeout reliably firing.
    response = await withProgressTimeout(
      fetch(url, {
        headers: {
          'user-agent': USER_AGENT,
          accept: 'application/vnd.ipld.car',
        },
        signal,
      }),
      REPO_FETCH_STALL_MS,
      'connect/headers',
      () => abort.abort(),
    );
  } catch (err) {
    throw new RetryableError(`getRepo ${did}@${host}: ${describe(err)}`, {
      transient: true,
      cause: err,
    });
  }

  if (!response.ok) throw await classifyHttpError(response, did, host, abort);
  if (response.body === null) {
    throw new RetryableError(`getRepo ${did}@${host}: response had no body`, {
      transient: true,
    });
  }

  const upstream = response.body.getReader();
  let total = 0;

  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      let result;
      try {
        // Inactivity budget per chunk: a half-open socket mid-stream makes
        // read() hang forever, and AbortSignal.timeout does not always break
        // it. The progress timer guarantees this settles, so the slot is freed
        // and the repo requeues (transient) instead of leaking into a wedge.
        result = await withProgressTimeout(
          upstream.read(),
          REPO_FETCH_STALL_MS,
          'body',
          () => abort.abort(),
        );
      } catch (err) {
        throw new RetryableError(
          `getRepo ${did}@${host}: body failed after ${total} bytes: ${describe(err)}`,
          {
            transient: true,
            cause: err,
          },
        );
      }
      if (result.done) {
        controller.close();
        return;
      }
      total += result.value.byteLength;
      if (CAR_MAX_BYTES > 0 && total > CAR_MAX_BYTES) {
        abort.abort();
        throw new QuarantineError(
          `getRepo ${did}@${host}: car exceeded CAR_MAX_BYTES (${CAR_MAX_BYTES}) at ${total}+ bytes`,
        );
      }
      controller.enqueue(result.value);
    },
    async cancel(reason) {
      // Consumer-initiated cancel (e.g. a parse-side error before EOF). Abort
      // the socket first so the cancel cannot block draining a half-open body,
      // and bound it anyway so cleanup never holds the worker slot past the
      // stall budget. All failures here are best-effort cleanup — swallowed.
      abort.abort();
      await withProgressTimeout(
        upstream.cancel(reason),
        REPO_FETCH_STALL_MS,
        'body-cancel',
        () => abort.abort(),
      ).catch(() => undefined);
    },
  });

  return {
    response,
    rateLimit: parseRateLimitHeaders(response.headers),
    body,
    bytesRead: () => total,
  };
}
