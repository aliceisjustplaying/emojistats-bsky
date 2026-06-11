import { SqliteLedger } from './ledger.js';
import logger from './logger.js';

/** One-glance crawl health readout: `bun run status`. */

const LOADED_WINDOW_MINUTES = 5;

const ledger = new SqliteLedger();
try {
  const counts = ledger.statusCounts();
  const total = Object.values(counts).reduce((sum, n) => sum + n, 0);
  const loadedInWindow = ledger.loadedSince(
    Date.now() - LOADED_WINDOW_MINUTES * 60_000,
  );

  logger.info(
    {
      repos: total,
      counts,
      reposPerMin:
        Math.round((loadedInWindow / LOADED_WINDOW_MINUTES) * 100) / 100,
      totalPostsLoaded: ledger.totalPostsLoaded(),
      lastError: ledger.lastError(),
      plcCursor: ledger.getMeta('plc_cursor') ?? null,
    },
    'crawl status',
  );
} finally {
  ledger.close();
}
