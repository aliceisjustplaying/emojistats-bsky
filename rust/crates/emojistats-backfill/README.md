# emojistats-backfill

v2 backfill crawler for [emojistats](https://github.com/aliceisjustplaying/emojistats-bsky).
Fetches Bluesky repositories, emits `content_addressed_snapshot` proofs from `getRepo`
CAR/MST traversal, archives posts locally, and derives the emoji serving projection. The
current proof is not canonical snapshot-complete root recomputation, `listRecords` outputs
are `collection_paginated_posts`, fleet runs are bounded batches, and the `StorageBox`
backend needs canary proof before production use.

See [`docs/backfill-v2-design.md`](../../../docs/backfill-v2-design.md) for the design, and
[`rust/check.sh`](../../check.sh) for the muster gate (fmt, clippy, test, nextest, deny,
audit, machete, coverage).

**Status:** Rust vertical slice with local archive/derive proof; production fleet and
archive backend are not ready until canary-proven.
