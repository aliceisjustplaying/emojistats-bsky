# Emojistats

Emojistats measures emoji usage across Bluesky posts and publishes backfill-derived data products.

## Language

**Raw Archive**:
The private corpus of crawled post records retained as operational truth. It can include raw text, record extras, and provenance that are not necessarily safe for public redistribution.
_Avoid_: HuggingFace truth, public archive

**Published Raw Observed Corpus**:
The candidate public raw-text dataset produced from repository snapshots captured by the backfill. It includes raw post text and full record extras, with final filtering and release policy decided separately before publication.
_Avoid_: cumulative-ever snapshot, emoji-derived corpus

**Observed Corpus**:
A corpus made from repository records present at crawl time. It does not reconstruct records deleted before observation, and post delete events are not observed by this system.
_Avoid_: cumulative-ever corpus, full history

**DID Web Coverage**:
Best-effort coverage of `did:web` accounts discovered from known PDS listings, live observations, and manual seeds. It is not a guaranteed global census.
_Avoid_: complete did:web crawl

**Live Observed Post**:
A post record observed from the live stream after the live watermark. In the current product this means created posts only; it is used for the serving site, not for the Published Raw Observed Corpus.
_Avoid_: live mutation, delete event

**Jetstream Catch-Up**:
The serving-site catch-up phase that starts after the backfill and replays Jetstream from four hours before the backfill start time. It writes directly to the serving projection and is not part of the raw corpus.
_Avoid_: live/backfill overlap, dual-write

**Local Jetstream Fallback**:
A self-operated Jetstream server or spooler used when public Jetstream retention is not enough to guarantee catch-up. For a gap-free launch it must start before the public retention window can no longer cover the backfill rewind point.
_Avoid_: late-only fallback, mandatory live overlap

**Stratified Canary**:
The pre-fan-out test run that exercises representative normal and edge-case repository populations, storage publication, derive, ClickHouse, and failure injection. It must measure the launch-critical timings and sizes before the fleet run.
_Avoid_: monthly sample only, smoke test

**Record Extras**:
The non-core fields from a post record that are preserved alongside raw text, such as facets, reply references, embeds, self-labels, tags, and future lexicon fields.
_Avoid_: lossless JSON, blob

**Profile Sidecar**:
Profile metadata captured from `app.bsky.actor.profile/self` during the same repository fetch as posts. It does not imply handle verification, media fetching, or ClickHouse profile search.
_Avoid_: profile index, handle crawl

**Data-Model Lossless**:
Preservation of the post record's ATProto data-model fields after normalization into typed columns and canonical extras JSON. It does not promise byte-for-byte reconstruction of the original CBOR encoding.
_Avoid_: byte-lossless, CAR-lossless

**Normalizer Version**:
The version identity of the emoji normalization logic used to produce rows, including code revision and emoji data version. It travels with archive and serving outputs so mixed normalization can be detected.
_Avoid_: implicit normalizer, JS/Rust parity note

**Created-At Parse Status**:
The classification of a post record's author-supplied timestamp after parsing and normalization. It distinguishes valid, missing, invalid, and future timestamps so corpus partitions do not imply false time precision.
_Avoid_: created_at truth

**Snapshot Completeness**:
The claim that a fetched repository export contains a complete, self-consistent snapshot reachable from the exported commit data root. It is separate from signature verification and identity verification.
_Avoid_: authorship proof, identity proof

**Loud Resource Cap**:
A resource limit that rejects or pauses work only with an explicit status, metric, and recovery path. It prevents silent content loss while admitting that disk, time, parser, and upload limits are real.
_Avoid_: silent cap, no cap

**Collection-Paginated Record**:
A record fetched through paginated collection APIs when a full repository export is unavailable. It can support the serving projection but does not carry the Snapshot Completeness claim.
_Avoid_: root-proofed record, repo snapshot

**Observed Record Identity**:
The identity of a raw observed record in the archive and candidate public corpus, made from DID, record key, and CID. The serving emoji projection may use a smaller identity because it is not the raw corpus.
_Avoid_: rkey-only identity

**Receipt Row Hash**:
An ordered content hash over each archived post row's DID, record key, CID, normalized timestamp, text, languages, emoji extraction output, and canonical extras. It proves archived row content, not just key presence.
_Avoid_: rkey digest, count-only receipt

**Serving Emoji Projection**:
The ClickHouse-backed subset used by the public emoji stats site. It is derived from the Raw Archive and optimized for serving counts, not for preserving every raw record field.
_Avoid_: archive truth, raw corpus

**100% Website**:
The public site state after the backfill, archive derive, aggregate rebuild, and Jetstream Catch-Up have all completed. It excludes known launch gaps by definition.
_Avoid_: backfill-only site, partial launch
