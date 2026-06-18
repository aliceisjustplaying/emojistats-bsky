# emojistats-backfill

v2 backfill crawler for [emojistats](https://github.com/aliceisjustplaying/emojistats-bsky).
Fetches Bluesky repositories, emits `content_addressed_snapshot` proofs from `getRepo`
CAR/MST traversal, archives posts locally or to Storage Box over SSH, and derives the emoji serving projection. The
current proof is not canonical snapshot-complete root recomputation, `listRecords` outputs
are `collection_paginated_posts`, fleet runs are bounded batches, aggregate caches are
rebuilt with `clickhouse-rebuild-aggregates`, and the `StorageBox` backend needs canary
proof before production use.

See [`docs/backfill-v2-design.md`](../../../docs/backfill-v2-design.md) for the design, and
[`rust/check.sh`](../../check.sh) for the muster gate (fmt, clippy, test, nextest, deny,
audit, machete, coverage).

**Status:** Rust vertical slice with local and Storage Box archive paths; production fleet
operation is not ready until canary-proven.

## Canary trust boundary

`run-fleet` requires fresh, run-id-bound canary evidence signed with
`EMOJISTATS_CANARY_HMAC_KEY`. That signature prevents stale, mismatched, or tampered evidence
from being reused accidentally. It is not proof against a local operator who can read the
same signing key and choose to sign made-up evidence.

For the launch threat model this is a footgun guard, not a third-party security boundary.
`canary-sign` must be fed measured hard-gate records from the rehearsal/canary runner; bare
hard-gate `status: pass` records are intentionally rejected.
