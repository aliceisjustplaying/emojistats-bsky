import { existsSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { errors, type Headers } from "undici";
import { logger } from "../logger.js";

let cursorCachePath = path.resolve(process.cwd(), "pds-cursor-cache.json");

export function setCursorCachePath(nextPath: string) {
  cursorCachePath = nextPath;
  pdsCursorCache = undefined as any;
}

export async function fetchPdses(): Promise<Array<string>> {
  const data = await fetch(
    "https://raw.githubusercontent.com/mary-ext/atproto-scraping/refs/heads/trunk/state.json",
  ).then((res) => (res.ok ? (res.json() as any) : null));

  if (!data.pdses) throw new Error("Failed to fetch PDSes");

  const pdses = Object.keys(data.pdses).filter((pds) =>
    pds.startsWith("https://"),
  );
  return pdses;
}

export type ListReposPage = {
  cursor?: string;
  repos: Array<{ did: string }>;
};

export async function listReposPage(
  pds: string,
  cursor: string,
): Promise<ListReposPage | null> {
  const url = new URL(`/xrpc/com.atproto.sync.listRepos`, pds);
  url.searchParams.set("limit", "1000");
  url.searchParams.set("cursor", cursor);
  while (true) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(10_000) });
      if (!res?.ok) {
        if (res?.status === 429) {
          await processRatelimitHeaders(res.headers, url.href);
          continue;
        }
        throw new Error(
          `Failed to fetch DIDs from ${pds}: ${res?.status ?? "unknown"} ${
            res?.statusText ?? ""
          }`,
        );
      }
      const { cursor: nextCursor, repos } = (await res.json()) as ListReposPage;
      return { cursor: nextCursor, repos };
    } catch (err: any) {
      const undiciError =
        err instanceof errors.UndiciError
          ? err
          : err instanceof Error && err.cause instanceof errors.UndiciError
            ? err.cause
            : null;
      const cursorLabel = cursor && cursor.length > 0 ? cursor : "<start>";
      if (
        [
          "ETIMEDOUT",
          "UND_ERR_CONNECT_TIMEOUT",
          "UND_ERR_HEADERS_TIMEOUT",
          "UND_ERR_SOCKET",
        ].includes(undiciError?.code ?? "")
      ) {
        logger.warn(
          { url: url.href, pds, code: undiciError?.code },
          "listRepos connect failure",
        );
        return null;
      }
      const reason = err?.message ?? `${err}`;
      if (pds.includes("bsky.network")) {
        logger.warn(
          { url: url.href, cursor: cursorLabel, reason },
          "listRepos transient failure",
        );
        await sleep(5000);
        continue;
      }
      logger.warn(
        { url: url.href, cursor: cursorLabel, reason },
        "listRepos giving up",
      );
      return null;
    }
  }
}

export async function* fetchAllDids() {
  const pdses = await fetchPdses();

  const cursors = getPdsCursorSnapshot();
  const pdsesToFetchFrom = pdses.filter((pds) => cursors[pds] !== "DONE");

  yield* roundRobinInterleaveIterators(pdsesToFetchFrom.map(fetchPdsDids));
}

async function* fetchPdsDids(pds: string) {
  let cursor = getPdsCursorValue(pds) ?? "";
  if (cursor === "DONE") {
    logger.warn({ pds }, "Skipping exhausted PDS");
    return;
  }
  let fetched = 0;
  while (true) {
    const page = await listReposPage(pds, cursor);
    if (!page) break;
    for (const repo of page.repos) {
      if (!repo.did) continue;
      yield [repo.did, pds] as const;
      fetched++;
    }
    const nextCursor = page.cursor;
    if (!nextCursor || nextCursor === cursor) break;
    setPdsCursorValue(pds, nextCursor);
    cursor = nextCursor;
  }
  const cursorLabel = cursor && cursor.length > 0 ? cursor : "<start>";
  logger.info({ pds, fetched, cursor: cursorLabel }, "PDS repos exhausted");
  markPdsDone(pds);
  return fetched;
}

// async function fetchPlcDids(map: Map<string, string> = new Map()): Promise<Map<string, string>> {
// 	let cursor = "";
// 	while (true) {
// 		console.log(`fetching plc dids, now ${map.size}`);
// 		const res = await fetch(`https://plc.directory/export?limit=1000%after=${cursor}`);
// 		if (!res.ok) {
// 			if (res.status === 429) {
// 				await sleep(10_000);
// 				continue;
// 			}
// 			throw new Error(`Failed to fetch PLC DIDs: ${res.status} ${res.statusText}`);
// 		}
//
// 		const lines = await res.text();
// 		const operations = lines.split("\n").map((line) => {
// 			try {
// 				return JSON.parse(line);
// 			} catch (e) {
// 				return null;
// 			}
// 		});
//
// 		for (const op of operations) {
// 			if (!op?.operation?.type) continue;
// 			if (op.operation.type === "create" && op.operation.service) {
// 				map.set(op.did, op.operation.service);
// 			} else if (op.operation.type === "plc_operation") {
// 				const pds = op.operation.services.atproto_pds.endpoint;
// 				if (pds) map.set(op.did, pds);
// 			} else if (op.operation.type === "plc_tombstone") map.delete(op.did);
// 		}
//
// 		cursor = operations.at(-1)?.createdAt;
// 		if (!cursor) break;
// 	}
//
// 	return map;
// }
//
// async function fetchWebDids(map: Map<string, string> = new Map()): Promise<Map<string, string>> {
// 	const data = await fetch(
// 		"https://raw.githubusercontent.com/mary-ext/atproto-scraping/refs/heads/trunk/state.json",
// 	).then((res) => res.ok ? res.json() as any : null);
// 	if (!data?.firehose?.didWebs) throw new Error("Failed to fetch web DIDs");
// 	for (const [did, { pds }] of Object.entries<{ pds: string }>(data.firehose.didWebs)) {
// 		map.set(did, pds);
// 	}
// 	return map;
// }

let pdsCursorCache: Record<string, string>;
const getPdsCursorCache = () => {
  if (!pdsCursorCache) {
    if (!existsSync(cursorCachePath)) {
      writeFileSync(cursorCachePath, "{}", "utf8");
    }
    pdsCursorCache = JSON.parse(
      readFileSync(cursorCachePath, "utf8"),
    ) as Record<string, string>;
  }
  return pdsCursorCache;
};
const savePdsCursorCache = () =>
  writeFileSync(cursorCachePath, JSON.stringify(pdsCursorCache));

export function getPdsCursorSnapshot() {
  return { ...getPdsCursorCache() };
}

export function getPdsCursorValue(pds: string) {
  return getPdsCursorCache()?.[pds];
}

export function setPdsCursorValue(pds: string, cursor: string) {
  getPdsCursorCache()[pds] = cursor;
  savePdsCursorCache();
}

export function markPdsDone(pds: string) {
  getPdsCursorCache()[pds] = "DONE";
  savePdsCursorCache();
}

export async function processRatelimitHeaders(headers: Headers, url: string) {
  const remainingHeader = headers.get("ratelimit-remaining"),
    resetHeader = headers.get("ratelimit-reset");
  if (!remainingHeader || !resetHeader) return;

  const ratelimitRemaining = parseInt(remainingHeader);
  if (isNaN(ratelimitRemaining) || ratelimitRemaining <= 1) {
    const ratelimitReset = parseInt(resetHeader) * 1000;
    if (isNaN(ratelimitReset)) {
      logger.error({ url }, "ratelimit-reset header is not numeric");
    } else {
      const now = Date.now();
      const waitTime = ratelimitReset - now + 1000; // add a second to be safe
      if (waitTime > 0) {
        await sleep(waitTime);
      }
    }
  }
}

export async function* roundRobinInterleaveIterators<T>(
  iterators: Array<AsyncIterator<T>>,
  concurrency = 25,
) {
  const getNext = (it: AsyncIterator<T>, idx: number) =>
    it
      .next()
      .then((res) => ({ idx, res, error: null }))
      .catch((error) => ({
        idx,
        res: null,
        error,
      }));

  // Queue of iterator indices waiting for their next turn
  const pending: number[] = iterators.map((_, i) => i);

  // Active promises by iterator index
  const activeMap = new Map<number, Promise<any>>();

  const launch = () => {
    while (activeMap.size < concurrency && pending.length) {
      const idx = pending.shift()!;
      const promise = getNext(iterators[idx], idx);
      activeMap.set(idx, promise);
    }
  };

  launch();

  const doneIterators = new Set<number>();

  while (doneIterators.size < iterators.length) {
    if (activeMap.size === 0) break;

    const result = await Promise.race(activeMap.values());
    activeMap.delete(result.idx);

    if (result.error) {
      logger.error(
        { iteratorIndex: result.idx, err: result.error },
        "Iterator worker failed",
      );
    }

    if (result.res.done) {
      doneIterators.add(result.idx);
    } else {
      // Emit value and put this iterator back in queue
      yield result.res.value;
      pending.push(result.idx);
    }

    // Top up the active promises pool to ensure we're operating at concurrency limit
    launch();
  }
}
