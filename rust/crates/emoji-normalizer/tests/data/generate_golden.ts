import { existsSync, readFileSync, writeFileSync } from 'node:fs';

// Regenerates emoji_parity_golden.tsv: the frozen output of the legacy TypeScript
// emoji pipeline (the production path in packages/ingest/src/normalizer.ts:
// `text.match(emojiRegex)` then `batchNormalizeEmojis`) over the Unicode emoji-test.txt
// corpus. The Rust crate's tests/parity.rs asserts `extract_emoji_sequence` reproduces
// this byte-for-byte, locking in cross-language parity.
//
// Run from the repo root so the workspace resolves the legacy packages:
//   bun rust/crates/emoji-normalizer/tests/data/generate_golden.ts
import { batchNormalizeEmojis } from 'emoji-normalization';
import emojiRegexFunc from 'emoji-regex';

// The corpus is the Unicode emoji-test.txt for the version the `emojis` crate ships
// (currently 16.0). It is gitignored (*.txt); fetched on demand so regeneration is a
// single command. The committed emoji_parity_golden.tsv is what the Rust test reads.
const CORPUS_URL = 'https://www.unicode.org/Public/emoji/16.0/emoji-test.txt';
const emojiRegex: RegExp = emojiRegexFunc();
const dir = new URL('.', import.meta.url).pathname;

const corpusPath = `${dir}emoji-test.txt`;
if (!existsSync(corpusPath)) {
  console.log(`fetching ${CORPUS_URL}`);
  writeFileSync(corpusPath, await (await fetch(CORPUS_URL)).text());
}
const corpus = readFileSync(corpusPath, 'utf8');
const toHex = (s: string): string =>
  [...s].map((c) => c.codePointAt(0)!.toString(16).toUpperCase()).join(' ');

const seen = new Set<string>();
const rows: string[] = [];
for (const line of corpus.split('\n')) {
  const trimmed = line.trim();
  if (trimmed === '' || trimmed.startsWith('#')) continue;
  const cps = line.split(';')[0]?.trim();
  if (!cps) continue;
  const input = String.fromCodePoint(
    ...cps.split(/\s+/).map((h) => parseInt(h, 16)),
  );
  if (seen.has(input)) continue;
  seen.add(input);
  const matches = input.match(emojiRegex) ?? [];
  const out = batchNormalizeEmojis(matches);
  rows.push(`${toHex(input)}\t${out.map(toHex).join(',')}`);
}

writeFileSync(`${dir}emoji_parity_golden.tsv`, rows.join('\n') + '\n');
console.log(`wrote ${rows.length} golden rows`);
