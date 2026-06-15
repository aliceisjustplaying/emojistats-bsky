# emojistats-backfill

v2 backfill crawler for [emojistats](https://github.com/aliceisjustplaying/emojistats-bsky).
Fetches Bluesky repositories, proves snapshot completeness from the CAR/MST, archives posts
to the Raw Archive, and derives the emoji serving projection.

See [`docs/backfill-v2-design.md`](../../../docs/backfill-v2-design.md) for the design, and
[`rust/check.sh`](../../check.sh) for the muster gate (fmt, clippy, test, nextest, deny,
audit, machete, coverage).

**Status:** foundation scaffold — `fetch-one <did>` is the first vertical-slice milestone.
