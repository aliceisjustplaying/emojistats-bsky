import {
  Client,
  ok,
  simpleFetchHandler,
  type XRPCErrorPayload,
} from "@atcute/client";
import { setTimeout as sleep } from "node:timers/promises";
import { Agent, setGlobalDispatcher } from "undici";
import type {} from "@atcute/atproto";

type ExtractSuccessData<T> = T extends { ok: true; data: infer D } ? D : never;

type UnknownClientResponse = { status: number; headers: Headers } & (
  | { ok: true; data: unknown }
  | { ok: false; data: XRPCErrorPayload }
);

const agent = new Agent({ pipelining: 0 });
setGlobalDispatcher(agent);

const retryableStatusCodes = new Set([
  408, 409, 429, 500, 502, 503, 504, 520, 522, 523,
]);
const maxRetries = 5;

export class XRPCManager {
  clients = new Map<string, Client>();

  async query<T extends UnknownClientResponse>(
    service: string,
    fn: (client: Client) => Promise<T>,
    attempt = 0,
  ): Promise<ExtractSuccessData<T>> {
    try {
      return await this.queryNoRetry(service, fn);
    } catch (error) {
      this.maybeRetry(error, service, attempt++);
      throw error;
    }
  }

  async queryNoRetry<T extends UnknownClientResponse>(
    service: string,
    fn: (client: Client) => Promise<T>,
  ): Promise<ExtractSuccessData<T>> {
    const client = this.getOrCreateClient(service);
    return await ok(fn(client));
  }

  createClient(service: string) {
    const client = new Client({ handler: simpleFetchHandler({ service }) });
    this.clients.set(service, client);
    return client;
  }

  getOrCreateClient(service: string) {
    return this.clients.get(service) ?? this.createClient(service);
  }

  private maybeRetry(error: unknown, url: string, attempt = 0): never | false {
    if (!error || typeof error !== "object") return false;

    const errorStr = `${error}`.toLowerCase();
    if (
      errorStr.includes("tcp") ||
      errorStr.includes("network") ||
      errorStr.includes("dns")
    ) {
      throw new RetryError("Network error", 0, attempt);
    }

    if (error instanceof DOMException && error.name === "TimeoutError") {
      throw new RetryError("Timed out", 0, attempt);
    }
    if (error instanceof TypeError) return false;

    // Error must have headers and, if it does have a status, the status must be 429
    if (
      "headers" in error &&
      error.headers &&
      (!("status" in error) || error.status === 429)
    ) {
      let reset;
      if (
        error.headers instanceof Headers &&
        error.headers.has("ratelimit-reset")
      ) {
        reset = parseInt(error.headers.get("ratelimit-reset")!);
      } else if (
        typeof error.headers === "object" &&
        "ratelimit-reset" in error.headers
      ) {
        reset = parseInt(`${error.headers["ratelimit-reset"]}`);
      }
      if (reset) {
        const resetMs = reset * 1000;
        const delay = resetMs - Date.now();
        throw new RetryError(
          `Rate limited by ${url}, retrying in ${delay} seconds`,
          resetMs,
          attempt,
        );
      }
    }

    if (attempt >= maxRetries) return false;

    if (
      "status" in error &&
      typeof error.status === "number" &&
      retryableStatusCodes.has(error.status)
    ) {
      const delay = Math.pow(3, attempt + 1);
      const resetMs = Date.now() + delay * 1000;
      throw new RetryError(
        `Retrying ${error.status} in ${delay} seconds for ${url}`,
        resetMs,
        attempt,
      );
    }

    return false;
  }
}

export class RetryError extends Error {
  constructor(
    message: string,
    public readonly resetMs: number,
    public readonly attempt: number,
  ) {
    super(message);
  }

  async wait() {
    const delay = this.resetMs - Date.now();
    if (delay <= 0) return;
    await sleep(delay);
  }
}
