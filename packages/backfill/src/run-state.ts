/**
 * Mutable per-run state shared across the crawl's modules. Plain objects, not
 * channels: every module mutates the same instances the entrypoint created, so
 * the stats line and telemetry always read the live numbers.
 */

export interface CrawlStats {
  claimed: number;
  loaded: number;
  empty: number;
  retried: number;
  terminal: number;
  skipped: number;
  postRows: number;
  bytes: number;
}

export function createCrawlStats(): CrawlStats {
  return {
    claimed: 0,
    loaded: 0,
    empty: 0,
    retried: 0,
    terminal: 0,
    skipped: 0,
    postRows: 0,
    bytes: 0,
  };
}

/**
 * Cooperative shutdown flag. Set by the signal handlers, the archive trip and
 * the loader trip; polled by the claim/scheduling loop. Once true the run stops
 * claiming and drains in-flight repos.
 */
export interface CrawlControl {
  stopClaiming: boolean;
}
