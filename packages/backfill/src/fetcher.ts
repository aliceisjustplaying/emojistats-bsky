import { CAR_MAX_BYTES, REPO_FETCH_TIMEOUT_MS, USER_AGENT } from './config.js';

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
 *   body over CAR_MAX_BYTES         QuarantineError, request aborted
 */

export class RetryableError extends Error {
  readonly retryAfterMs: number | undefined;
  /** Clearly transient (429/5xx/network/timeout) retries in waves forever; the rest gets MAX_ATTEMPTS. */
  readonly transient: boolean;

  constructor(
    message: string,
    opts: { transient: boolean; retryAfterMs?: number; cause?: unknown },
  ) {
    super(message, { cause: opts.cause });
    this.name = 'RetryableError';
    this.transient = opts.transient;
    this.retryAfterMs = opts.retryAfterMs;
  }
}

export type TerminalFetchStatus = 'deactivated' | 'takendown' | 'failed';

export class TerminalFetchError extends Error {
  readonly status: TerminalFetchStatus;

  constructor(status: TerminalFetchStatus, message: string) {
    super(message);
    this.name = 'TerminalFetchError';
    this.status = status;
  }
}

export class QuarantineError extends Error {
  constructor(message: string, opts?: ErrorOptions) {
    super(message, opts);
    this.name = 'QuarantineError';
  }
}

export interface FetchedCar {
  response: Response;
  /** CAR_MAX_BYTES is enforced here; stream errors are always RetryableError or QuarantineError. */
  body: ReadableStream<Uint8Array>;
  /** Bytes pulled through so far; the final value is the ledger's car_bytes. */
  bytesRead(): number;
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
): Promise<Error> {
  const retryAfterMs = parseRetryAfter(response.headers.get('retry-after'));

  let xrpcError: string | undefined;
  let detail = '';
  try {
    detail = (await response.text()).slice(0, 500);
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
    if (terminal !== undefined) return new TerminalFetchError(terminal, label);
  }
  if (response.status === 429 || response.status >= 500) {
    return new RetryableError(label, { transient: true, retryAfterMs });
  }
  return new RetryableError(`${label} ${detail}`.trim(), {
    transient: false,
    retryAfterMs,
  });
}

export async function fetchRepoCar(
  pdsHost: string,
  did: string,
): Promise<FetchedCar> {
  const host = pdsHost.replace(/^https?:\/\//, '').replace(/\/+$/, '');
  const url = `https://${host}/xrpc/com.atproto.sync.getRepo?did=${encodeURIComponent(did)}`;

  const abort = new AbortController();
  // One budget for connect + headers + full body download.
  const signal = AbortSignal.any([
    AbortSignal.timeout(REPO_FETCH_TIMEOUT_MS),
    abort.signal,
  ]);

  let response: Response;
  try {
    response = await fetch(url, {
      headers: { 'user-agent': USER_AGENT, accept: 'application/vnd.ipld.car' },
      signal,
    });
  } catch (err) {
    throw new RetryableError(`getRepo ${did}@${host}: ${describe(err)}`, {
      transient: true,
      cause: err,
    });
  }

  if (!response.ok) throw await classifyHttpError(response, did, host);
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
        result = await upstream.read();
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
      if (total > CAR_MAX_BYTES) {
        abort.abort();
        throw new QuarantineError(
          `getRepo ${did}@${host}: car exceeded CAR_MAX_BYTES (${CAR_MAX_BYTES}) at ${total}+ bytes`,
        );
      }
      controller.enqueue(result.value);
    },
    cancel(reason) {
      return upstream.cancel(reason).catch(() => undefined);
    },
  });

  return { response, body, bytesRead: () => total };
}
