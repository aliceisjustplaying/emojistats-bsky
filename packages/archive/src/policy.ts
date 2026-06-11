/**
 * The single owner of the storage-split policy (plan 0001 cost revision):
 * ClickHouse keeps text for emoji posts only, the Parquet archive keeps all
 * text. Every writer (live ingest, backfill loader) must route its decisions
 * through this module — never read TEXT_IN_CLICKHOUSE / ARCHIVE_ENABLED
 * directly for behavior.
 */

export interface StoragePolicy {
  readonly textInClickhouse: 'emoji' | 'all';
  readonly archiveEnabled: boolean;
}

/**
 * Validates the combination at startup. The dangerous configuration —
 * dropping non-emoji text from ClickHouse while the archive is disabled —
 * would silently discard data forever, so it refuses to start instead.
 */
export function resolveStoragePolicy(raw: {
  textInClickhouse: string;
  archiveEnabled: boolean;
}): StoragePolicy {
  if (raw.textInClickhouse !== 'emoji' && raw.textInClickhouse !== 'all') {
    throw new Error(
      `TEXT_IN_CLICKHOUSE must be 'emoji' or 'all', got '${raw.textInClickhouse}'`,
    );
  }
  if (raw.textInClickhouse === 'emoji' && !raw.archiveEnabled) {
    throw new Error(
      "TEXT_IN_CLICKHOUSE='emoji' with ARCHIVE_ENABLED=false would silently discard " +
        'non-emoji post text forever — the Parquet archive is its only durable home ' +
        "(plan 0001). Enable the archive or set TEXT_IN_CLICKHOUSE='all'.",
    );
  }
  return Object.freeze({
    textInClickhouse: raw.textInClickhouse,
    archiveEnabled: raw.archiveEnabled,
  });
}

/** The text value a ClickHouse row gets under this policy. */
export function clickhouseText(
  policy: StoragePolicy,
  text: string,
  hasEmojis: boolean,
): string {
  return policy.textInClickhouse === 'all' || hasEmojis ? text : '';
}
