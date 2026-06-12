import { isCarV1Header } from '@atcute/car';
import { CidLinkWrapper, decode as decodeCbor } from '@atcute/cbor';
import {
  fromStream as repoFromStream,
  isCommit,
  type Commit,
} from '@atcute/repo';

import { QuarantineError, RetryableError } from './fetcher.js';

/**
 * Non-verifying CAR walk on top of @atcute/repo's streaming reader, which owns
 * the MST traversal (commit → data root → nodes → records, any block order,
 * only unmatched blocks buffered). Two gaps are covered here because the lib
 * genuinely doesn't:
 *
 *  - rev: the reader validates the commit internally but never exposes it, so
 *    a passive CommitScanner taps the raw byte stream and decodes the root
 *    block itself. It never throws into the stream — on any confusion it goes
 *    dead and the reader (parsing the same bytes) stays authoritative.
 *  - missing-block semantics: the reader reports referenced-but-absent blocks
 *    instead of failing. Missing commit/mst-node or post record → quarantine;
 *    missing non-post record bodies are counted into recordsTotal and ignored;
 *    a missing post CID we already yielded is the byte-identical-record-under-
 *    several-rkeys pathology → counted as duplicatePostsSkipped, exactly as
 *    the hand-rolled walk did (the lib yields duplicates fine when both MST
 *    references precede the block; only the consumed-stray ordering is moot).
 */

const POST_COLLECTION = 'app.bsky.feed.post';

/** Caps the scanner's wait-for-more-bytes buffer; legit headers/commits are a few hundred bytes. */
const MAX_SCAN_BUFFER = 1_048_576;

export interface RepoPost {
  collection: typeof POST_COLLECTION;
  rkey: string;
  record: Record<string, unknown>;
}

export interface ParsedRepo {
  /**
   * Commit rev; populated while `posts` is consumed, final once it is
   * exhausted. Whenever the scanner succeeds, this is set BEFORE the first
   * post yields: the reader cannot resolve any MST entry without first
   * consuming the commit block, and the scanner taps every byte ahead of the
   * reader. The pipeline relies on that to start rev-tokened ClickHouse chunk
   * inserts mid-stream with (normally) zero rows buffered.
   */
  readonly rev: string | null;
  /** MST leaf entries across all collections; final once `posts` is exhausted. */
  readonly recordsTotal: number;
  /** Posts whose record CID was already yielded under another rkey (byte-identical records). */
  readonly duplicatePostsSkipped: number;
  readonly posts: AsyncGenerator<RepoPost, void, undefined>;
}

const EMPTY = new Uint8Array(0);

/** Returns null when the buffer ends mid-varint; throws on >9-byte varints. */
function tryVarint(
  buf: Uint8Array,
  offset: number,
): { value: number; nextOffset: number } | null {
  // inlined LEB128 accumulate: @atcute/car's readVarint is internal/unexported
  // and @atcute/varint is not a declared dependency of this package
  let value = 0;
  let shift = 0;
  for (let i = offset; i < buf.length; i += 1) {
    if (i - offset >= 9) throw new Error('varint too long');
    const byte = buf[i];
    value += shift < 28 ? (byte & 0x7f) << shift : (byte & 0x7f) * 2 ** shift;
    shift += 7;
    if ((byte & 0x80) === 0) return { value, nextOffset: i + 1 };
  }
  return null;
}

function bytesEqualAt(
  buf: Uint8Array,
  start: number,
  expected: Uint8Array,
): boolean {
  for (let i = 0; i < expected.length; i += 1) {
    if (buf[start + i] !== expected[i]) return false;
  }
  return true;
}

function concatBytes(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

/**
 * Incremental scan of the raw CAR byte stream for the root (commit) block.
 * Memory is bounded: non-matching block bodies are skipped arithmetically, the
 * buffer only holds an unconsumed frame tail capped at MAX_SCAN_BUFFER.
 */
class CommitScanner {
  commit: Commit | null = null;

  #phase: 0 | 1 | 2 = 0; // header | block frames | done-or-dead
  #buf: Uint8Array = EMPTY;
  #skip = 0; // remaining body bytes of a non-root block to discard
  #rootCid: Uint8Array | null = null;

  push(chunk: Uint8Array): void {
    if (this.#phase === 2) return;
    try {
      this.#scan(chunk);
    } catch {
      // passive by design: rev simply stays null and the repo reader's own
      // parse (or the final rev === null check) decides the repo's fate
      this.#phase = 2;
      this.#buf = EMPTY;
    }
  }

  #scan(chunk: Uint8Array): void {
    if (this.#skip > 0) {
      const n = Math.min(this.#skip, chunk.length);
      this.#skip -= n;
      if (this.#skip > 0) return;
      chunk = chunk.subarray(n);
    }
    const buf = this.#buf.length === 0 ? chunk : concatBytes(this.#buf, chunk);
    let offset = 0;

    while (true) {
      if (this.#phase === 0) {
        const head = tryVarint(buf, offset);
        if (head === null || head.nextOffset + head.value > buf.length) break;
        const header: unknown = decodeCbor(
          buf.subarray(head.nextOffset, head.nextOffset + head.value),
        );
        if (!isCarV1Header(header)) throw new Error('not a car v1 header');
        const root = header.roots[0];
        if (!(root instanceof CidLinkWrapper))
          throw new Error('car root is not a cid link');
        this.#rootCid = root.bytes;
        offset = head.nextOffset + head.value;
        this.#phase = 1;
        continue;
      }

      const frame = tryVarint(buf, offset);
      if (frame === null) break;
      if (frame.value < 36)
        throw new Error('block frame too short for a dasl cid');
      const cidStart = frame.nextOffset;
      if (cidStart + 36 > buf.length) break;
      const bodyStart = cidStart + 36;
      const bodyLen = frame.value - 36;

      if (bytesEqualAt(buf, cidStart, this.#rootCid!)) {
        if (bodyStart + bodyLen > buf.length) break; // wait for the full commit block
        const value: unknown = decodeCbor(
          buf.subarray(bodyStart, bodyStart + bodyLen),
        );
        if (!isCommit(value)) throw new Error('root block is not a commit');
        this.commit = value;
        this.#phase = 2;
        this.#buf = EMPTY;
        return;
      }

      const available = buf.length - bodyStart;
      if (available < bodyLen) {
        // skip the rest of this body without buffering it
        this.#skip = bodyLen - available;
        this.#buf = EMPTY;
        return;
      }
      offset = bodyStart + bodyLen;
    }

    this.#buf = offset < buf.length ? buf.subarray(offset) : EMPTY;
    if (this.#buf.length > MAX_SCAN_BUFFER)
      throw new Error('scan buffer cap exceeded');
  }
}

/** Identity pass-through that lets the scanner observe every byte the reader consumes. */
function tapStream(
  source: ReadableStream<Uint8Array>,
  scanner: CommitScanner,
): ReadableStream<Uint8Array> {
  const reader = source.getReader();
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      const result = await reader.read();
      if (result.done) {
        controller.close();
        return;
      }
      scanner.push(result.value);
      controller.enqueue(result.value);
    },
    cancel(reason) {
      return reader.cancel(reason).catch(() => undefined);
    },
  });
}

function asRecordMap(value: unknown, rkey: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new QuarantineError(`record ${rkey} is not a cbor map`);
  }
  return value as Record<string, unknown>;
}

export function parseRepoCar(stream: ReadableStream<Uint8Array>): ParsedRepo {
  const state = {
    rev: null as string | null,
    recordsTotal: 0,
    duplicatePostsSkipped: 0,
  };

  async function* walk(): AsyncGenerator<RepoPost, void, undefined> {
    const scanner = new CommitScanner();
    const repo = repoFromStream(tapStream(stream, scanner));
    // post record cids already yielded, to tell duplicates from truly missing blocks
    const consumedPosts = new Set<string>();

    try {
      for await (const entry of repo) {
        state.recordsTotal += 1;
        // Cooperative yield. This walk is synchronous CPU under the hood and
        // microtask awaits never release the event loop, so a like-heavy whale
        // repo otherwise starves every socket and timer in the process — on
        // launch night that meant ClickHouse client timeouts, frozen telemetry
        // and 128 stalled fetch slots. A macrotask break every 2000 records
        // bounds the blocking to tens of milliseconds.
        if (state.recordsTotal % 2000 === 0)
          await new Promise<void>((resolve) => setImmediate(resolve));
        if (state.rev === null && scanner.commit !== null)
          state.rev = scanner.commit.rev;
        if (entry.collection !== POST_COLLECTION) continue;
        consumedPosts.add(entry.cid.$link);
        yield {
          collection: POST_COLLECTION,
          rkey: entry.rkey,
          record: asRecordMap(entry.record, entry.rkey),
        };
      }
      if (state.rev === null && scanner.commit !== null)
        state.rev = scanner.commit.rev;

      // missingBlocks is only populated once the iteration above completed cleanly
      for (const missing of repo.missingBlocks) {
        if (missing.type !== 'record') {
          throw new QuarantineError(
            `car is missing a ${missing.type} block (${missing.cid})`,
          );
        }
        state.recordsTotal += 1; // the MST entry exists even though its block doesn't
        const slash = missing.key.indexOf('/');
        const collection =
          slash === -1 ? missing.key : missing.key.slice(0, slash);
        if (collection !== POST_COLLECTION) continue;
        if (consumedPosts.has(missing.cid)) {
          state.duplicatePostsSkipped += 1;
        } else {
          throw new QuarantineError(
            `post record ${missing.key} missing from the car (${missing.cid})`,
          );
        }
      }

      if (state.rev === null) {
        throw new QuarantineError('car ended without a commit block');
      }
    } catch (err) {
      // Fetch-layer errors keep their classification; anything else here is a
      // malformed CAR/CBOR and must quarantine the repo, never crash the worker.
      if (err instanceof QuarantineError || err instanceof RetryableError)
        throw err;
      throw new QuarantineError(
        `malformed car: ${err instanceof Error ? err.message : String(err)}`,
        { cause: err },
      );
    } finally {
      await repo.dispose().catch(() => undefined);
    }
  }

  return {
    get rev() {
      return state.rev;
    },
    get recordsTotal() {
      return state.recordsTotal;
    },
    get duplicatePostsSkipped() {
      return state.duplicatePostsSkipped;
    },
    posts: walk(),
  };
}
