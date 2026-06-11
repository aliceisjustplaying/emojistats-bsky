/**
 * The rkey digest, both sides of it (plan 0001): the JS fold the pipeline
 * writes to the ledger and the ClickHouse expression that recomputes it. They
 * must stay bit-identical, which is why they live in one file — retry.ts and
 * verify.ts compare across the pair and a drift would read as data loss.
 */

import { createHash } from 'node:crypto';

/**
 * One rkey's contribution to RepoCounts.rkeyDigest: sha256(rkey) bytes 0..7 as
 * a little-endian u64. Must stay bit-identical to CH_RKEY_DIGEST_EXPR below —
 * reinterpretAsUInt64 is little-endian, matching readBigUInt64LE (proven: both
 * map 'abc123' to 0x83c870ca523da16c). XOR of u64s stays a u64, so the
 * pipeline's fold needs no masking.
 */
export function rkeyHash64(rkey: string): bigint {
  return createHash('sha256').update(rkey).digest().readBigUInt64LE(0);
}

/**
 * ClickHouse recomputation of the same fold, as an aggregate over a per-DID
 * GROUP BY. Callers wrap it in hex() and pass the result through
 * normalizeDigestHex before comparing against the ledger.
 */
export const CH_RKEY_DIGEST_EXPR =
  'groupBitXor(reinterpretAsUInt64(substring(SHA256(rkey), 1, 8)))';

// hex() of a UInt64 prints big-endian digits of the VALUE and drops leading
// zero bytes ('01' for 1); the ledger stores fixed-width lowercase. Compare
// in one canonical form.
export function normalizeDigestHex(value: string): string {
  return value.toLowerCase().padStart(16, '0');
}
