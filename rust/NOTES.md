# v2 backfill (rust/) — implementation notes & continuity

Working notes for the `emojistats-backfill` Rust rewrite. Design source of truth:
`../docs/backfill-v2-design.md`. This file is the implementation-level companion (API map,
roadmap, conventions) so a fresh session can continue without re-deriving.

## Status (2026-06-15)

- Branch `v2-rust-backfill` (not pushed). Greenfield; no v1 reuse.
- **Checkpoint A done:** `fetch-one <did>` resolves DID→PDS over the live network
  (`did:plc:z72i7hdynmk6r22z27h6tvur` → `puffball.us-east.host.bsky.network`); invalid DIDs
  error cleanly. Full muster green.
- **Checkpoint B/C/D done locally:** `fetch-one <did>` now resolves DID→PDS, streams
  `getRepo` to a spooled `CAR`, parses from the `CAR` path with block `CID` verification
  and `MST` completeness, writes `Parquet` posts, writes receipt + local manifest JSON, and
  derives compact emoji JSONL rows.
- **Next-lane foundations started:** `ledger.rs` has retry/account-state transition types,
  `commit.rs` has a local Storage Box-shaped committed-artifact protocol, `derive.rs` has
  manifest-to-ClickHouse DTOs and dedupe tokening, and `canary.rs` encodes the stratified
  canary policy/gate model. These are library foundations, not a wired fleet runner yet.
- Real stress DID verified:
  `did:plc:vwzwgnygau7ed7b7wt5ux7y2` from `shiitake.us-east.host.bsky.network` spooled
  41,051,855 bytes, produced 6,407 post rows, 228 emoji rows, and carried 23,656 typed
  record decode failures as non-fatal parse diagnostics.
- Jacquard 0.12.0 via **fork-mirror git deps**: `github.com/aliceisjustplaying/jacquard`
  @ `39648622522fa62c4c0b12ac22b8a5f6893c845a` (== tag 0.12.0). reqwest pulls **rustls**
  (no openssl). Full 0.12.0 source also at `/tmp/jacquard` for reading (ephemeral).
- Build/gate: `./check.sh` (cc is on PATH now). All tools installed.

## fetch-one vertical slice

- **A — identity:** `src/main.rs` resolves DID→PDS using `PublicResolver`.
- **B — transport:** `src/transport.rs` streams `com.atproto.sync.getRepo` with Jacquard's
  `download()` path, captures standard and legacy rate-limit headers, writes the response
  body to a deterministic spool path, enforces idle timeout + byte cap, and classifies
  account-state, HTTP, timeout, cap, transport, and I/O errors.
- **C — parse:** `src/parse.rs` reads only a `CAR` path, indexes blocks by `CID`, verifies
  bytes hash back to the advertised `CID`, stores block offsets over the spooled file,
  parses the commit, proves `MST` root equality, walks records, and extracts typed
  `app.bsky.feed.post` plus optional profile data. Typed record decode failures are
  diagnostics; malformed records do not abort a complete snapshot.
- **D — archive/derive:** `src/archive.rs` converts parsed posts to archive rows, computes
  row-content receipt hashes and counts, writes `Parquet` with flat lossless columns plus
  `extras_json`, writes a local manifest entry, and derives local compact emoji JSONL rows.

## Next roadmap

- Persist crawler ledger state and wire retry/account-state transitions around `fetch-one`.
- Wire archive artifact writes through the committed-artifact protocol and add the remote
  Storage Box backend: temp upload, verify, final rename, receipt sidecar, manifest append
  only after the final object exists.
- Move emoji normalization into the shared WASM-able crate from the design before the
  browser/server serving path depends on it.
- Wire derive/ClickHouse ingest from committed manifest entries, then run the stratified
  canary and fleet scheduler work.

### Defaulted design choices (revisit if needed)

- **BlockStore** = index the spooled CAR file (`CID → (offset,len)`, seek to read) rather
  than a second on-disk copy; spill the index if a whale's is too large for RAM.
- **Parquet** = `arrow` + `parquet` crates.
- **Emoji** = currently minimal local Rust extraction in `archive.rs`; still promote it to
  the shared `emoji-normalizer` crate before this becomes a serving contract.

## Jacquard 0.12.0 API map (load-bearing; from recon — verify against `/tmp/jacquard`)

### Transport — `jacquard-common` (features: std, service-auth, crypto, reqwest-client, streaming)
- `jacquard_common::http_client::HttpClient`: `async fn send_http(&self, http::Request<Vec<u8>>) -> Result<http::Response<Vec<u8>>, Self::Error>`; `Error: std::error::Error + Display + Send + Sync + 'static`.
- `HttpClientExt` (feat `streaming`): `async fn send_http_streaming(&self, req) -> Result<http::Response<ByteStream>, Error>` + `send_http_bidirectional<S>` (upload only — return an Err, NOT `unimplemented!`, under our lint bar).
- reqwest impl template (`http_client.rs:118`): copy **all** headers for **any** status (no `error_for_status`), `resp.bytes_stream()` → `ByteStream::new(...)`. Our per-chunk inactivity timeout wraps each `stream.next()`; per-host pacing wraps `req.send()`.
- `XrpcExt::xrpc(base: Uri<&str>) -> XrpcCall` (blanket impl on every `HttpClient`). **Avoid** the stateful `XrpcClient`/`Agent`/`send()` (buffer body, drop headers, collapse errors).
- `XrpcCall::download(&req) -> Result<StreamingResponse, StreamError>` (feat `streaming`). Does **not** status-check. `StreamingResponse::{status(), headers(), into_parts()->(Parts, ByteStream)}`. Read `ratelimit-*` from `headers()` before consuming the body. `ByteStream::into_inner()` → `Pin<Box<dyn Stream<Item=Result<Bytes, StreamError>> + Send>>`.
- `GetRepo` @ `jacquard_api::com_atproto::sync::get_repo`: `{ did: Did<S>, since: Option<Tid> }`; NSID `com.atproto.sync.getRepo`; `Accept: application/vnd.ipld.car`. Pass `&GetRepo` to `download()`.
- **No** rate-limit header parsing exists in Jacquard — hand-roll `ratelimit-limit/remaining/reset`, `x-ratelimit-*`, `retry-after`.

### Parse — `jacquard-repo`
- `jacquard_repo::storage::BlockStore` (trait, `Clone + Send + Sync + 'static`): `get/put/has/put_many/get_many/apply_commit`. MST read path uses only `get/get_many/has` → implement those over disk; stub `put/put_many/apply_commit` by returning Err. Hold the disk handle in `Arc` (cheap `Clone`).
- All 3 built-in stores are in-RAM (incl. `FileBlockStore`, which slurps the whole CAR) → write our own.
- `jacquard_repo::car::stream_car(path) -> CarBlockStream`; `.next() -> Option<(Cid, Bytes)>` — streaming, whale-safe. (`read_car`/`parse_car_bytes` buffer everything — avoid for whales.)
- `jacquard_repo::commit::Commit<S>` `{ did, version, data: IpldCid (=MST root), rev, prev, sig }`; `Commit::from_cbor(&bytes)`; `commit.data()`. (Skip signature verify per design.)
- `jacquard_repo::mst::Mst::load(Arc<Store>, cid, layer: Option<usize>)` (lazy). `mst.get_pointer()` recomputes the root CID → **Snapshot Completeness = `get_pointer() == commit.data`**. `MstCursor`/`leaves_sequential()` for whales (`leaves()`/`collect_blocks()` collect into RAM). rkeys are the reconstructed MST leaf keys.
- `jacquard_repo::mst::util::compute_cid(&[u8]) -> IpldCid` (SHA-256, dag-cbor codec `0x71`). **No read path verifies bytes-hash-to-CID** — WE must `compute_cid` per block at ingest and reject mismatches (the other half of completeness). Guard non-dag-cbor codecs (raw `0x55`).
- Reference pattern: `jacquard_repo::commit::firehose::validate_v1_0` (load MST → `get_pointer()` == expected root).

### Types / errors — `jacquard-api`, `jacquard-common`, `jacquard-identity`
- All generated types are generic `<S: BosStr = SmolStr>`; use the `SmolStr` default.
- `GetRepoError` (get_repo.rs): `RepoNotFound/RepoTakendown/RepoSuspended/RepoDeactivated(Option<SmolStr>)` + `#[serde(untagged)] Other { error: SmolStr, message }` (preserves the raw code). Deserialize the body into this **regardless of HTTP status** (we own transport). Other endpoints' errors are `GenericError` (private `Data` newtype) — re-deserialize the body into our own `{error,message}` to recover the code.
- `listRepos` `Repo.status`: `RepoStatus` enum `Takendown/Suspended/Deleted/Deactivated/Desynchronized/Throttled/Other(S)`.
- `app_bsky::feed::post::Post<S>`: `text:S`, `created_at: Datetime`, optional `facets/reply/embed/langs(Vec<Language>)/labels/tags`, `extra_data: Option<BTreeMap<SmolStr,Data>>` (`#[serde(flatten)]` catch-all). `embed` is an open union with an injected `Unknown(Data)`. `Datetime` preserves the original string. → flat columns + `extras_json`.
- `app_bsky::actor::profile::Profile<S>`: all optional + `extra_data`.
- Identity: `PublicResolver = JacquardResolver<reqwest::Client>`; `PublicResolver::default()`; `IdentityResolver::pds_for_did(&Did) -> Uri<String>`. `Did::new_owned(&str) -> Result`. PLC bulk export = hand-roll (Jacquard is one-DID-per-GET: `plc.directory/{did}`).

## Conventions under the strict lint bar (see Cargo.toml `[workspace.lints]`)

- No `unwrap`/`expect`/`panic`/`todo`/`unimplemented` in non-test code → return Errs (thiserror), `?`/`map_err`. Stub unwanted trait methods by returning Err, never `unimplemented!`.
- No `indexing_slicing` → `.get()`. No `arithmetic_side_effects` → `checked_*`/`saturating_*` on byte/record counters.
- `doc_markdown` (pedantic): backtick code/type/format terms in doc comments (`getRepo`, `HttpClient`, `BlockStore`, `MST`, `CAR`, `Parquet`, …).
- `nextest` fails on zero tests → every crate needs ≥1 test.
- `deny.toml`: git deps need a `version =` (else flagged wildcard); the license allow-list and advisory `ignore` are tuned for the current tree (re-tune when deps change).
- `./check.sh` runs the full muster: fmt · clippy -D warnings · test · nextest · deny · audit · machete · llvm-cov.
