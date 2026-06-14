import { once } from 'node:events';
import { createWriteStream, type WriteStream } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { parseArgs } from 'node:util';

import Database from 'better-sqlite3';

interface RecrawlRow {
  did: string;
  pdsHost: string;
  posts: number;
}

interface PartStats {
  file: string;
  hostFile: string;
  repos: number;
  posts: number;
}

const { values } = parseArgs({
  options: {
    ledger: { type: 'string' },
    shard: { type: 'string' },
    parts: { type: 'string', default: '4' },
    out: { type: 'string', default: 'data/v1-recrawl' },
  },
});

const ledgerPath = values.ledger;
if (ledgerPath === undefined || ledgerPath === '') {
  throw new Error('--ledger is required');
}

const shard =
  values.shard === undefined || values.shard === ''
    ? undefined
    : Number(values.shard);
if (shard !== undefined && (!Number.isInteger(shard) || shard < 0)) {
  throw new Error(
    `--shard must be a non-negative integer, got ${values.shard}`,
  );
}

const parts = Number(values.parts);
if (!Number.isInteger(parts) || parts <= 0) {
  throw new Error(`--parts must be a positive integer, got ${values.parts}`);
}

const outDir = values.out;
if (outDir === undefined || outDir === '') throw new Error('--out is required');

await mkdir(outDir, { recursive: true });

const db = new Database(ledgerPath, { readonly: true, fileMustExist: true });
const marker = db
  .prepare("SELECT value FROM meta WHERE key = 'archive_extras_since'")
  .get() as { value: string } | undefined;
if (marker === undefined) {
  throw new Error(`${ledgerPath} has no archive_extras_since meta key`);
}

const markerMs = Date.parse(marker.value);
if (!Number.isFinite(markerMs)) {
  throw new Error(
    `archive_extras_since is not an ISO timestamp: ${marker.value}`,
  );
}

const prefix = shard === undefined ? 'all' : `shard${shard}`;
const stats: PartStats[] = Array.from({ length: parts }, (_unused, i) => ({
  file: path.join(outDir, `v1-recrawl-${prefix}-part${i}.txt`),
  hostFile: path.join(outDir, `v1-recrawl-${prefix}-part${i}.tsv`),
  repos: 0,
  posts: 0,
}));
const streams = stats.map((part) => createWriteStream(part.file));
const hostStreams = stats.map((part) => createWriteStream(part.hostFile));

async function writeLine(stream: WriteStream, line: string): Promise<void> {
  if (stream.write(line)) return;
  await once(stream, 'drain');
}

function nextPart(): number {
  let best = 0;
  for (let i = 1; i < stats.length; i += 1) {
    if (stats[i].posts < stats[best].posts) best = i;
  }
  return best;
}

const shardFilter = shard === undefined ? '' : 'AND bucket = @shard';
const rows = db
  .prepare(
    `
    SELECT did, pds_host AS pdsHost, COALESCE(posts_total, 0) AS posts
    FROM repos
    WHERE status IN ('loaded', 'verified')
      AND loaded_at IS NOT NULL
      AND loaded_at < @markerMs
      ${shardFilter}
    ORDER BY COALESCE(posts_total, 0) DESC, did
  `,
  )
  .iterate({ markerMs, shard }) as IterableIterator<RecrawlRow>;

for (const row of rows) {
  const part = nextPart();
  await writeLine(streams[part], `${row.did}\n`);
  await writeLine(hostStreams[part], `${row.did}\t${row.pdsHost}\n`);
  stats[part].repos += 1;
  stats[part].posts += row.posts;
}

await Promise.all(
  [...streams, ...hostStreams].map(
    (stream) =>
      new Promise<void>((resolve, reject) => {
        stream.end((err?: Error | null) => {
          if (err) reject(err);
          else resolve();
        });
      }),
  ),
);
db.close();

const manifest = {
  ledger: path.resolve(ledgerPath),
  shard,
  marker: marker.value,
  markerMs,
  parts: stats,
  totals: {
    repos: stats.reduce((sum, part) => sum + part.repos, 0),
    posts: stats.reduce((sum, part) => sum + part.posts, 0),
  },
};
const manifestPath = path.join(outDir, `v1-recrawl-${prefix}-manifest.json`);
await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);

console.log(JSON.stringify(manifest, null, 2));
