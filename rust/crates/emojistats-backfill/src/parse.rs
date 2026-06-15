//! Stage C `CAR` parser for the v2 backfill pipeline.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use cid::Cid as IpldCid;
use jacquard_api::app_bsky::{actor::profile::Profile, feed::post::Post};
use jacquard_repo::{
    DAG_CBOR_CID_CODEC,
    commit::Commit,
    error::RepoError,
    mst::{NodeData, util::compute_cid},
    storage::BlockStore,
};
use serde::Deserialize;
use smol_str::SmolStr;

/// Parsed one-repo output from Stage C.
#[derive(Debug, Clone)]
pub struct ParsedRepo {
    /// Commit metadata from the repo root block.
    pub commit: CommitMeta,
    /// Snapshot completeness proof details.
    pub completeness: CompletenessProof,
    /// Extracted `app.bsky.feed.post` records.
    pub posts: Vec<PostRecord>,
    /// Deterministic key summary for the traversed `MST`.
    pub rkey_digest: RkeyDigest,
    /// Extracted `app.bsky.actor.profile/self`, when present.
    pub profile: Option<ProfileRecord>,
    /// Non-fatal profile sidecar decode error, when the post snapshot can still be parsed.
    pub profile_decode_error: Option<String>,
    /// Number of typed record decode failures observed while walking reachable records.
    pub record_decode_error_count: u64,
    /// Number of typed post record decode failures observed while walking reachable records.
    pub post_decode_error_count: u64,
}

/// Parsed one-repo summary for streaming callers that do not retain post rows.
#[derive(Debug, Clone)]
pub struct ParsedRepoSummary {
    /// Commit metadata from the repo root block.
    pub commit: CommitMeta,
    /// Snapshot completeness proof details.
    pub completeness: CompletenessProof,
    /// Deterministic key summary for the traversed `MST`.
    pub rkey_digest: RkeyDigest,
    /// Extracted `app.bsky.actor.profile/self`, when present.
    pub profile: Option<ProfileRecord>,
    /// Non-fatal profile sidecar decode error, when the post snapshot can still be parsed.
    pub profile_decode_error: Option<String>,
    /// Number of typed record decode failures observed while walking reachable records.
    pub record_decode_error_count: u64,
    /// Number of typed post record decode failures observed while walking reachable records.
    pub post_decode_error_count: u64,
}

/// Resource caps for Stage C parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseConfig {
    /// Maximum number of `CAR` blocks accepted while verifying or indexing.
    pub max_car_blocks: u64,
    /// Maximum encoded `CAR` block section size accepted before allocation.
    pub max_block_bytes: u64,
    /// Maximum number of reachable repo records accepted while walking the `MST`.
    pub max_records: u64,
    /// Maximum `MST` cursor layer accepted while walking records.
    pub max_mst_depth: u64,
    /// Maximum number of non-fatal typed record decode errors accepted.
    pub max_decode_errors: u64,
    /// Maximum best-effort parser wall-clock time.
    pub max_parse_wall_clock: Duration,
}

impl Default for ParseConfig {
    fn default() -> Self {
        Self {
            max_car_blocks: 10_000_000,
            max_block_bytes: 67_108_864,
            max_records: 10_000_000,
            max_mst_depth: 256,
            max_decode_errors: 1_000_000,
            max_parse_wall_clock: Duration::from_mins(15),
        }
    }
}

/// Commit metadata needed by downstream archive and receipt code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitMeta {
    /// Commit block `CID`.
    pub cid: String,
    /// Repository `DID` claimed by the commit.
    pub did: String,
    /// Commit schema version.
    pub version: i64,
    /// Commit revision `TID`.
    pub rev: String,
    /// Commit `MST` root `CID`.
    pub data: String,
    /// Previous commit `CID`, if present.
    pub prev: Option<String>,
}

/// Completeness proof fields for a `getRepo` snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletenessProof {
    /// Proof class for the parsed input.
    pub class: CompletenessClass,
    /// Root `CID` entries declared by the `CAR` header.
    pub car_roots: Vec<String>,
    /// Number of `CAR` blocks with verified content-addressed `CID`s.
    pub verified_block_count: u64,
    /// Number of reachable `MST` leaves whose record block resolved by `CID`.
    pub reachable_record_count: u64,
    /// Whether the commit's `data` root matched the traversed `MST` root.
    pub mst_root_matches_commit: bool,
    /// Commit signature verification is deliberately out of scope for Stage C.
    pub repo_commit_signature_verified: bool,
    /// Identity verification is deliberately out of scope for Stage C.
    pub identity_verified: bool,
}

/// Completeness class assigned to the parsed repo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletenessClass {
    /// Complete `CAR` snapshot proven from commit root through `MST` leaves.
    SnapshotComplete,
}

/// Extracted post record plus repo key context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostRecord {
    /// Repo record key.
    pub rkey: String,
    /// Record block `CID`.
    pub cid: String,
    /// Typed Bluesky post record.
    pub record: Post<SmolStr>,
}

/// Extracted profile record plus repo key context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileRecord {
    /// Repo record key.
    pub rkey: String,
    /// Record block `CID`.
    pub cid: String,
    /// Typed Bluesky profile record.
    pub record: Profile<SmolStr>,
}

/// Deterministic key summary for archive receipt wiring.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RkeyDigest {
    /// Number of reachable repo records.
    pub all_records_count: u64,
    /// Number of reachable `app.bsky.feed.post` records.
    pub post_records_count: u64,
    /// First reachable repo key in `MST` order.
    pub first_key: Option<String>,
    /// Last reachable repo key in `MST` order.
    pub last_key: Option<String>,
}

/// Stage C parse failures.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// Filesystem operation failed.
    #[error("I/O while parsing {path}: {source}")]
    Io {
        /// Path being read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Jacquard repo primitive failed.
    #[error("Jacquard repo parse failed: {0}")]
    Repo(#[from] RepoError),
    /// `CAR` root/header shape is not usable as a repo snapshot.
    #[error("invalid CAR root set: {0}")]
    InvalidRoots(String),
    /// `CAR` block bytes do not match their advertised `CID`.
    #[error("CAR block CID mismatch: block={block_cid}, computed={computed_cid}")]
    CidMismatch {
        /// Advertised block `CID`.
        block_cid: String,
        /// Computed block `CID`.
        computed_cid: String,
    },
    /// Non-`dag-cbor` block found in the repo `CAR`.
    #[error("unsupported CAR block codec {codec:#x} for CID {cid}")]
    UnsupportedCodec {
        /// Block `CID`.
        cid: String,
        /// CID multicodec.
        codec: u64,
    },
    /// Commit block could not be found or decoded.
    #[error("commit block not found for CAR root {root}")]
    CommitNotFound {
        /// Root `CID` declared by the `CAR`.
        root: String,
    },
    /// The single `CAR` root did not decode as a repo commit.
    #[error("CAR root {root} did not decode as a repo commit: {message}")]
    RootCommitDecode {
        /// Root `CID` declared by the `CAR`.
        root: String,
        /// Decode error message.
        message: String,
    },
    /// The repo commit claimed a different `DID` than the caller requested.
    #[error("commit DID mismatch: requested={requested}, actual={actual}")]
    CommitDidMismatch {
        /// Requested repo `DID`.
        requested: String,
        /// Commit repo `DID`.
        actual: String,
    },
    /// A reachable block was missing from the `CAR`.
    #[error("reachable block missing from CAR: {cid}")]
    MissingBlock {
        /// Missing block `CID`.
        cid: String,
    },
    /// A typed record block failed to decode.
    #[error("failed to decode {collection} record {key} at {cid}: {source}")]
    RecordDecode {
        /// Full repo key.
        key: String,
        /// Collection being decoded.
        collection: &'static str,
        /// Record block `CID`.
        cid: String,
        /// Underlying DAG-CBOR decode error.
        #[source]
        source: Box<serde_ipld_dagcbor::DecodeError<std::convert::Infallible>>,
    },
    /// The `MST` root reached from the commit did not match `commit.data`.
    #[error("MST root mismatch: commit data={commit_data}, traversed root={traversed_root}")]
    MstRootMismatch {
        /// Commit `data` root.
        commit_data: String,
        /// Traversed `MST` root.
        traversed_root: String,
    },
    /// Integer overflow while counting parser resources.
    #[error("resource counter overflow: {field}")]
    ResourceCountOverflow {
        /// Counter name.
        field: &'static str,
    },
    /// Configured parser resource cap was exceeded.
    #[error("parser resource limit exceeded: {limit} observed={observed}; recovery={recovery}")]
    ResourceLimitExceeded {
        /// Limit name.
        limit: &'static str,
        /// Observed value.
        observed: u64,
        /// Operator recovery hint.
        recovery: &'static str,
    },
    /// Unsupported parse case with an explicit status.
    #[error("unsupported Stage C parse case: {feature}")]
    Unsupported {
        /// Unsupported feature.
        feature: &'static str,
    },
    /// Planned proof/extraction work that is intentionally not hidden.
    #[error("Stage C proof step not yet implemented: {feature}")]
    NotYetImplemented {
        /// Missing proof step.
        feature: &'static str,
    },
    /// Parser runtime could not be started.
    #[error("parser runtime failed: {0}")]
    Runtime(#[source] std::io::Error),
    /// Parser thread could not be started.
    #[error("parser thread failed: {0}")]
    ThreadSpawn(#[source] std::io::Error),
    /// Parser thread terminated unexpectedly.
    #[error("parser thread terminated unexpectedly")]
    RuntimeThreadTerminated,
    /// `CAR` varint was malformed.
    #[error("malformed CAR varint")]
    MalformedVarint,
    /// `CAR` length arithmetic overflowed.
    #[error("CAR length overflow while reading {field}")]
    CarLengthOverflow {
        /// Length field being processed.
        field: &'static str,
    },
    /// `CAR` section was malformed.
    #[error("malformed CAR section: {0}")]
    MalformedCar(String),
    /// `CID` bytes inside the `CAR` failed to decode.
    #[error("failed to decode CID from CAR block: {0}")]
    CidRead(#[source] Box<cid::Error>),
}

/// Error returned by streaming parse visitors.
#[derive(Debug, thiserror::Error)]
pub enum ParseVisitError<E> {
    /// Stage C parser failed.
    #[error(transparent)]
    Parse(#[from] ParseError),
    /// Caller-provided visitor failed.
    #[error("record visitor failed: {0}")]
    Visit(E),
}

/// Parse a spooled repo `CAR` from disk.
///
/// # Errors
///
/// Returns [`ParseError`] for malformed `CAR`s, `CID` mismatches, missing reachable blocks,
/// invalid commits, `MST` traversal failures, typed record decode failures, and local I/O errors.
pub fn parse_repo(car_path: &Path) -> Result<ParsedRepo, ParseError> {
    parse_repo_with_config(car_path, ParseConfig::default())
}

/// Parse a spooled repo `CAR` from disk with explicit resource caps.
///
/// # Errors
///
/// Returns [`ParseError`] for malformed `CAR`s, `CID` mismatches, missing reachable blocks,
/// invalid commits, `MST` traversal failures, typed record decode failures, configured resource
/// limits, and local I/O errors.
pub fn parse_repo_with_config(
    car_path: &Path,
    config: ParseConfig,
) -> Result<ParsedRepo, ParseError> {
    parse_repo_thread(car_path, None, config)
}

/// Parse a spooled repo `CAR` and assert that the commit `DID` matches the requested repo.
///
/// # Errors
///
/// Returns [`ParseError`] for the same cases as [`parse_repo`], plus a loud commit `DID`
/// mismatch when the root commit does not claim `requested_did`.
pub fn parse_repo_for_did(car_path: &Path, requested_did: &str) -> Result<ParsedRepo, ParseError> {
    parse_repo_for_did_with_config(car_path, requested_did, ParseConfig::default())
}

/// Parse a spooled repo `CAR` with explicit resource caps and a requested `DID` assertion.
///
/// # Errors
///
/// Returns [`ParseError`] for malformed input, configured resource limits, traversal failures,
/// local I/O errors, and commit `DID` mismatch.
pub fn parse_repo_for_did_with_config(
    car_path: &Path,
    requested_did: &str,
    config: ParseConfig,
) -> Result<ParsedRepo, ParseError> {
    parse_repo_thread(car_path, Some(requested_did.to_owned()), config)
}

/// Parse a spooled repo `CAR`, visiting each decoded post without retaining all posts.
///
/// The caller-owned `state` is moved into the parser thread and returned with the summary.
///
/// # Errors
///
/// Returns [`ParseVisitError::Parse`] for parser failures, or [`ParseVisitError::Visit`] when
/// `visit_post` fails.
pub fn parse_repo_for_did_with_state<S, E, F>(
    car_path: &Path,
    requested_did: &str,
    config: ParseConfig,
    state: S,
    visit_post: F,
) -> Result<(ParsedRepoSummary, S), ParseVisitError<E>>
where
    S: Send + 'static,
    E: Send + 'static,
    F: FnMut(&mut S, PostRecord) -> Result<(), E> + Send + 'static,
{
    parse_repo_visit_thread(
        car_path,
        Some(requested_did.to_owned()),
        config,
        state,
        visit_post,
    )
}

fn parse_repo_thread(
    car_path: &Path,
    requested_did: Option<String>,
    config: ParseConfig,
) -> Result<ParsedRepo, ParseError> {
    let car_path = car_path.to_path_buf();
    std::thread::Builder::new()
        .name("emojistats-stage-c-parse".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(ParseError::Runtime)?;
            runtime.block_on(parse_repo_async(
                &car_path,
                requested_did.as_deref(),
                config,
            ))
        })
        .map_err(ParseError::ThreadSpawn)?
        .join()
        .map_err(|_err| ParseError::RuntimeThreadTerminated)?
}

fn parse_repo_visit_thread<S, E, F>(
    car_path: &Path,
    requested_did: Option<String>,
    config: ParseConfig,
    state: S,
    mut visit_post: F,
) -> Result<(ParsedRepoSummary, S), ParseVisitError<E>>
where
    S: Send + 'static,
    E: Send + 'static,
    F: FnMut(&mut S, PostRecord) -> Result<(), E> + Send + 'static,
{
    let car_path = car_path.to_path_buf();
    std::thread::Builder::new()
        .name("emojistats-stage-c-parse".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(ParseError::Runtime)?;
            runtime.block_on(parse_repo_async_visit(
                &car_path,
                requested_did.as_deref(),
                config,
                state,
                &mut visit_post,
            ))
        })
        .map_err(ParseError::ThreadSpawn)?
        .join()
        .map_err(|_err| ParseError::RuntimeThreadTerminated)?
}

async fn parse_repo_async(
    car_path: &Path,
    requested_did: Option<&str>,
    config: ParseConfig,
) -> Result<ParsedRepo, ParseError> {
    let mut posts = Vec::new();
    let (summary, ()) = parse_repo_async_visit(
        car_path,
        requested_did,
        config,
        (),
        &mut |_state: &mut (), post| {
            posts.push(post);
            Ok::<(), std::convert::Infallible>(())
        },
    )
    .await
    .map_err(|error| match error {
        ParseVisitError::Parse(error) => error,
        ParseVisitError::Visit(error) => match error {},
    })?;

    Ok(ParsedRepo {
        commit: summary.commit,
        completeness: summary.completeness,
        posts,
        rkey_digest: summary.rkey_digest,
        profile: summary.profile,
        profile_decode_error: summary.profile_decode_error,
        record_decode_error_count: summary.record_decode_error_count,
        post_decode_error_count: summary.post_decode_error_count,
    })
}

async fn parse_repo_async_visit<S, E, F>(
    car_path: &Path,
    requested_did: Option<&str>,
    config: ParseConfig,
    mut state: S,
    visit_post: &mut F,
) -> Result<(ParsedRepoSummary, S), ParseVisitError<E>>
where
    F: FnMut(&mut S, PostRecord) -> Result<(), E>,
{
    let deadline = ParseDeadline::start(config.max_parse_wall_clock);
    let (stream_summary, store) = IndexedCarBlockStore::load(car_path, config, deadline)?;
    deadline.ensure_not_exceeded()?;
    let commit_root = single_car_root(&stream_summary.roots)?;
    let (commit_cid, commit) = load_commit(commit_root, &store).await?;
    deadline.ensure_not_exceeded()?;
    assert_requested_did(requested_did, commit.did().as_str())?;
    let (profile, profile_decode_error, decode_digest, rkey_digest) = walk_mst_records_visit(
        commit.data,
        &store,
        config,
        deadline,
        &mut state,
        visit_post,
    )
    .await?;

    let proof = CompletenessProof {
        class: CompletenessClass::SnapshotComplete,
        car_roots: stream_summary
            .roots
            .iter()
            .map(ToString::to_string)
            .collect(),
        verified_block_count: stream_summary.verified_block_count,
        reachable_record_count: rkey_digest.all_records_count,
        mst_root_matches_commit: true,
        repo_commit_signature_verified: false,
        identity_verified: false,
    };

    Ok((
        ParsedRepoSummary {
            commit: CommitMeta {
                cid: commit_cid.to_string(),
                did: commit.did().as_str().to_owned(),
                version: commit.version,
                rev: commit.rev().as_str().to_owned(),
                data: commit.data().to_string(),
                prev: commit.prev().map(ToString::to_string),
            },
            completeness: proof,
            rkey_digest,
            profile,
            profile_decode_error,
            record_decode_error_count: decode_digest.all_decode_errors_count,
            post_decode_error_count: decode_digest.post_decode_errors_count,
        },
        state,
    ))
}

fn single_car_root(roots: &[IpldCid]) -> Result<IpldCid, ParseError> {
    match roots {
        [] => Err(ParseError::InvalidRoots(
            "CAR header has no roots".to_owned(),
        )),
        [root] => Ok(*root),
        _many => Err(ParseError::Unsupported {
            feature: "multi-root repo CAR",
        }),
    }
}

async fn load_commit(
    root: IpldCid,
    store: &IndexedCarBlockStore,
) -> Result<(IpldCid, Commit<SmolStr>), ParseError> {
    let Some(bytes) = store.get(&root).await? else {
        return Err(ParseError::CommitNotFound {
            root: root.to_string(),
        });
    };

    let commit = Commit::<SmolStr>::from_cbor(bytes.as_ref()).map_err(|source| {
        ParseError::RootCommitDecode {
            root: root.to_string(),
            message: source.to_string(),
        }
    })?;
    Ok((root, commit))
}

fn assert_requested_did(requested_did: Option<&str>, actual_did: &str) -> Result<(), ParseError> {
    let Some(requested) = requested_did else {
        return Ok(());
    };
    if requested == actual_did {
        return Ok(());
    }

    Err(ParseError::CommitDidMismatch {
        requested: requested.to_owned(),
        actual: actual_did.to_owned(),
    })
}

async fn walk_mst_records_visit<S, E, F>(
    root: IpldCid,
    store: &IndexedCarBlockStore,
    config: ParseConfig,
    deadline: ParseDeadline,
    state: &mut S,
    visit_post: &mut F,
) -> Result<
    (
        Option<ProfileRecord>,
        Option<String>,
        DecodeDigest,
        RkeyDigest,
    ),
    ParseVisitError<E>,
>
where
    F: FnMut(&mut S, PostRecord) -> Result<(), E>,
{
    let mut cursor = StreamingMstCursor::new(root, store);
    let mut profile = None;
    let mut profile_decode_error = None;
    let mut decode_digest = DecodeDigest::default();
    let mut digest = RkeyDigest::default();

    while let Some(leaf) = cursor.next_leaf(config).await? {
        deadline.ensure_not_exceeded()?;
        let record_bytes = store
            .get(&leaf.cid)
            .await
            .map_err(ParseError::Repo)?
            .ok_or_else(|| ParseError::MissingBlock {
                cid: leaf.cid.to_string(),
            })?;
        update_digest(&mut digest, &leaf.key, config)?;
        let mut sinks = RecordSinks {
            state,
            visit_post,
            profile: &mut profile,
            profile_decode_error: &mut profile_decode_error,
            decode_digest: &mut decode_digest,
        };
        extract_known_record(
            &leaf.key,
            leaf.cid,
            record_bytes.as_ref(),
            &mut sinks,
            config,
        )?;
    }

    Ok((profile, profile_decode_error, decode_digest, digest))
}

struct StreamingMstCursor<'a> {
    root: Option<IpldCid>,
    store: &'a IndexedCarBlockStore,
    stack: Vec<StreamingMstFrame>,
}

impl<'a> StreamingMstCursor<'a> {
    const fn new(root: IpldCid, store: &'a IndexedCarBlockStore) -> Self {
        Self {
            root: Some(root),
            store,
            stack: Vec::new(),
        }
    }

    async fn next_leaf(
        &mut self,
        config: ParseConfig,
    ) -> Result<Option<StreamingMstLeaf>, ParseError> {
        loop {
            if let Some(root) = self.root.take() {
                self.push_node(root, config).await?;
                continue;
            }

            let Some(frame) = self.stack.last_mut() else {
                return Ok(None);
            };
            let Some(item) = frame.next() else {
                self.stack.pop();
                continue;
            };
            match item {
                StreamingMstItem::Tree(cid) => {
                    self.push_node(cid, config).await?;
                }
                StreamingMstItem::Leaf { key, cid } => {
                    return Ok(Some(StreamingMstLeaf { key, cid }));
                }
            }
        }
    }

    async fn push_node(&mut self, cid: IpldCid, config: ParseConfig) -> Result<(), ParseError> {
        let depth = checked_increment(
            u64::try_from(self.stack.len()).map_err(|_err| ParseError::CarLengthOverflow {
                field: "MST stack depth",
            })?,
            "mst_depth",
        )?;
        ensure_u64_at_most(
            depth,
            config.max_mst_depth,
            "max_mst_depth",
            "raise parser max_mst_depth only after inspecting the repo MST",
        )?;
        let bytes = self
            .store
            .get(&cid)
            .await?
            .ok_or_else(|| ParseError::MissingBlock {
                cid: cid.to_string(),
            })?;
        self.stack.push(StreamingMstFrame::decode(bytes.as_ref())?);
        Ok(())
    }
}

struct StreamingMstFrame {
    items: Vec<StreamingMstItem>,
    index: usize,
}

impl StreamingMstFrame {
    fn decode(bytes: &[u8]) -> Result<Self, ParseError> {
        let node: NodeData = serde_ipld_dagcbor::from_slice(bytes).map_err(|source| {
            ParseError::MalformedCar(format!("failed to decode MST node: {source}"))
        })?;
        let mut items = Vec::new();
        if let Some(left) = node.left {
            items.push(StreamingMstItem::Tree(left));
        }
        let mut last_key = String::new();
        for entry in node.entries {
            let prefix_len = usize::from(entry.prefix_len);
            if !last_key.is_char_boundary(prefix_len) || prefix_len > last_key.len() {
                return Err(ParseError::MalformedCar(
                    "MST entry prefix exceeds previous key".to_owned(),
                ));
            }
            let suffix = std::str::from_utf8(&entry.key_suffix).map_err(|source| {
                ParseError::MalformedCar(format!("invalid UTF-8 in MST key suffix: {source}"))
            })?;
            let key = format!("{}{}", &last_key[..prefix_len], suffix);
            items.push(StreamingMstItem::Leaf {
                key: key.clone(),
                cid: entry.value,
            });
            if let Some(tree) = entry.tree {
                items.push(StreamingMstItem::Tree(tree));
            }
            last_key = key;
        }
        Ok(Self { items, index: 0 })
    }

    fn next(&mut self) -> Option<StreamingMstItem> {
        let item = self.items.get(self.index)?.clone();
        self.index = self.index.checked_add(1)?;
        Some(item)
    }
}

#[derive(Clone)]
enum StreamingMstItem {
    Tree(IpldCid),
    Leaf { key: String, cid: IpldCid },
}

struct StreamingMstLeaf {
    key: String,
    cid: IpldCid,
}

fn extract_known_record<S, E, F>(
    key: &str,
    cid: IpldCid,
    record_bytes: &[u8],
    sinks: &mut RecordSinks<'_, S, F>,
    config: ParseConfig,
) -> Result<(), ParseVisitError<E>>
where
    F: FnMut(&mut S, PostRecord) -> Result<(), E>,
{
    let Some((collection, rkey)) = split_repo_key(key) else {
        return Ok(());
    };

    match collection {
        POST_COLLECTION => match serde_ipld_dagcbor::from_slice::<Post<SmolStr>>(record_bytes) {
            Ok(record) => (sinks.visit_post)(
                sinks.state,
                PostRecord {
                    rkey: rkey.to_owned(),
                    cid: cid.to_string(),
                    record,
                },
            )
            .map_err(ParseVisitError::Visit)?,
            Err(_error) => record_decode_failed(sinks.decode_digest, POST_COLLECTION, config)?,
        },
        PROFILE_COLLECTION if rkey == PROFILE_RKEY => {
            match serde_ipld_dagcbor::from_slice::<Profile<SmolStr>>(record_bytes) {
                Ok(record) => {
                    *sinks.profile = Some(ProfileRecord {
                        rkey: rkey.to_owned(),
                        cid: cid.to_string(),
                        record,
                    });
                }
                Err(error) => {
                    let message = error.to_string();
                    *sinks.profile_decode_error =
                        Some(format!("{PROFILE_COLLECTION}/{rkey} at {cid}: {message}"));
                    record_decode_failed(sinks.decode_digest, PROFILE_COLLECTION, config)?;
                }
            }
        }
        _other => {}
    }

    Ok(())
}

struct RecordSinks<'a, S, F> {
    state: &'a mut S,
    visit_post: &'a mut F,
    profile: &'a mut Option<ProfileRecord>,
    profile_decode_error: &'a mut Option<String>,
    decode_digest: &'a mut DecodeDigest,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DecodeDigest {
    all_decode_errors_count: u64,
    post_decode_errors_count: u64,
}

fn record_decode_failed(
    digest: &mut DecodeDigest,
    collection: &'static str,
    config: ParseConfig,
) -> Result<(), ParseError> {
    digest.all_decode_errors_count =
        checked_increment(digest.all_decode_errors_count, "all_decode_errors_count")?;
    if collection == POST_COLLECTION {
        digest.post_decode_errors_count =
            checked_increment(digest.post_decode_errors_count, "post_decode_errors_count")?;
    }
    enforce_decode_error_limit(digest.all_decode_errors_count, config.max_decode_errors)
}

fn update_digest(
    digest: &mut RkeyDigest,
    key: &str,
    config: ParseConfig,
) -> Result<(), ParseError> {
    digest.all_records_count = checked_increment(digest.all_records_count, "all_records_count")?;
    ensure_u64_at_most(
        digest.all_records_count,
        config.max_records,
        "max_records",
        "raise parser max_records only for a known-good repo",
    )?;
    if digest.first_key.is_none() {
        digest.first_key = Some(key.to_owned());
    }
    digest.last_key = Some(key.to_owned());

    if key.starts_with(POST_PREFIX) {
        digest.post_records_count =
            checked_increment(digest.post_records_count, "post_records_count")?;
    }

    Ok(())
}

fn split_repo_key(key: &str) -> Option<(&str, &str)> {
    key.split_once('/')
}

fn verify_block_cid(cid: IpldCid, data: &[u8]) -> Result<(), ParseError> {
    let codec = cid.codec();
    if codec != DAG_CBOR_CID_CODEC {
        return Err(ParseError::UnsupportedCodec {
            cid: cid.to_string(),
            codec,
        });
    }

    let computed_cid = compute_cid(data)?;
    if computed_cid != cid {
        return Err(ParseError::CidMismatch {
            block_cid: cid.to_string(),
            computed_cid: computed_cid.to_string(),
        });
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct IndexedCarBlockStore {
    path: Arc<PathBuf>,
    index: Arc<BTreeMap<IpldCid, BlockLocation>>,
}

impl IndexedCarBlockStore {
    fn load(
        path: &Path,
        config: ParseConfig,
        deadline: ParseDeadline,
    ) -> Result<(CarStreamSummary, Self), ParseError> {
        let indexed_car = index_car_blocks(path, config, deadline)?;
        let summary = CarStreamSummary {
            roots: indexed_car.roots,
            verified_block_count: indexed_car.verified_block_count,
        };
        let store = Self {
            path: Arc::new(path.to_path_buf()),
            index: Arc::new(indexed_car.index),
        };
        Ok((summary, store))
    }
}

#[allow(clippy::unused_async_trait_impl)]
impl BlockStore for IndexedCarBlockStore {
    async fn get(&self, cid: &IpldCid) -> jacquard_repo::Result<Option<Bytes>> {
        let Some(location) = self.index.get(cid) else {
            return Ok(None);
        };
        read_block_at(&self.path, location)
            .map(Bytes::from)
            .map(Some)
            .map_err(RepoError::io)
    }

    async fn put(&self, _data: &[u8]) -> jacquard_repo::Result<IpldCid> {
        Err(read_only_store_error())
    }

    async fn has(&self, cid: &IpldCid) -> jacquard_repo::Result<bool> {
        Ok(self.index.contains_key(cid))
    }

    async fn put_many(
        &self,
        _blocks: impl IntoIterator<Item = (IpldCid, Bytes)> + Send,
    ) -> jacquard_repo::Result<()> {
        Err(read_only_store_error())
    }

    async fn get_many(&self, cids: &[IpldCid]) -> jacquard_repo::Result<Vec<Option<Bytes>>> {
        let mut blocks = Vec::with_capacity(cids.len());
        for cid in cids {
            blocks.push(self.get(cid).await?);
        }
        Ok(blocks)
    }

    async fn apply_commit(&self, _commit: jacquard_repo::CommitData) -> jacquard_repo::Result<()> {
        Err(read_only_store_error())
    }
}

fn index_car_blocks(
    path: &Path,
    config: ParseConfig,
    deadline: ParseDeadline,
) -> Result<IndexedCar, ParseError> {
    let mut file = open_file(path)?;
    let Some(header_len) = read_varint(&mut file)? else {
        return Err(ParseError::InvalidRoots("CAR file is empty".to_owned()));
    };
    ensure_u64_at_most(
        header_len.value,
        config.max_block_bytes,
        "max_block_bytes",
        "raise parser max_block_bytes only for a known-good repo",
    )?;
    let header_len_usize =
        usize::try_from(header_len.value).map_err(|_err| ParseError::CarLengthOverflow {
            field: "header length",
        })?;
    let mut header_bytes = vec![0_u8; header_len_usize];
    file.read_exact(&mut header_bytes)
        .map_err(|source| ParseError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let header = parse_car_header(&header_bytes)?;
    let mut offset = checked_add_u64(header_len.bytes_read, header_len.value, "header")?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|source| ParseError::Io {
            path: path.to_path_buf(),
            source,
        })?;

    let mut index = BTreeMap::new();
    let mut indexed_block_count = 0_u64;
    while let Some(section_len) = read_varint(&mut file)? {
        offset = checked_add_u64(offset, section_len.bytes_read, "section varint")?;
        let section_start = offset;
        ensure_u64_at_most(
            section_len.value,
            config.max_block_bytes,
            "max_block_bytes",
            "raise parser max_block_bytes only for a known-good repo",
        )?;
        let section_len_usize =
            usize::try_from(section_len.value).map_err(|_err| ParseError::CarLengthOverflow {
                field: "section length",
            })?;
        let mut section = vec![0_u8; section_len_usize];
        file.read_exact(&mut section)
            .map_err(|source| ParseError::Io {
                path: path.to_path_buf(),
                source,
            })?;

        let mut cursor = Cursor::new(section.as_slice());
        let cid = IpldCid::read_bytes(&mut cursor)
            .map_err(|source| ParseError::CidRead(Box::new(source)))?;
        let cid_len = cursor.position();
        let data_len = section_len
            .value
            .checked_sub(cid_len)
            .ok_or(ParseError::MalformedCar(
                "block section shorter than CID".to_owned(),
            ))?;
        let data_start =
            usize::try_from(cid_len).map_err(|_err| ParseError::CarLengthOverflow {
                field: "CID length",
            })?;
        let data = section.get(data_start..).ok_or(ParseError::MalformedCar(
            "block data slice outside section".to_owned(),
        ))?;
        verify_block_cid(cid, data)?;

        match index.entry(cid) {
            Entry::Vacant(entry) => {
                entry.insert(BlockLocation {
                    offset: checked_add_u64(section_start, cid_len, "block data offset")?,
                    len: usize::try_from(data_len).map_err(|_err| {
                        ParseError::CarLengthOverflow {
                            field: "block data length",
                        }
                    })?,
                });
            }
            Entry::Occupied(_entry) => {}
        }

        indexed_block_count = checked_increment(indexed_block_count, "indexed_block_count")?;
        ensure_u64_at_most(
            indexed_block_count,
            config.max_car_blocks,
            "max_car_blocks",
            "raise parser max_car_blocks only for a known-good repo",
        )?;
        deadline.ensure_not_exceeded()?;
        offset = checked_add_u64(section_start, section_len.value, "section end")?;
    }

    Ok(IndexedCar {
        roots: header.roots,
        verified_block_count: indexed_block_count,
        index,
    })
}

fn parse_car_header(bytes: &[u8]) -> Result<CarHeader, ParseError> {
    let header = serde_ipld_dagcbor::from_slice::<CarHeader>(bytes).map_err(|source| {
        ParseError::MalformedCar(format!("failed to decode CAR header: {source}"))
    })?;
    if header.version != 1 {
        return Err(ParseError::Unsupported {
            feature: "non-v1 CAR",
        });
    }
    Ok(header)
}

fn read_block_at(path: &Path, location: &BlockLocation) -> std::io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(location.offset))?;
    let mut bytes = vec![0_u8; location.len];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn open_file(path: &Path) -> Result<File, ParseError> {
    File::open(path).map_err(|source| ParseError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn read_varint(reader: &mut impl Read) -> Result<Option<Varint>, ParseError> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    let mut bytes_read = 0_u64;

    loop {
        let mut one_byte = [0_u8; 1];
        let read = reader
            .read(&mut one_byte)
            .map_err(|source| ParseError::Io {
                path: PathBuf::from("<car varint>"),
                source,
            })?;
        if read == 0 {
            return if bytes_read == 0 {
                Ok(None)
            } else {
                Err(ParseError::MalformedVarint)
            };
        }

        let [byte] = one_byte;
        bytes_read = checked_increment(bytes_read, "varint bytes")?;
        let chunk =
            u64::from(byte & 0x7f)
                .checked_shl(shift)
                .ok_or(ParseError::CarLengthOverflow {
                    field: "varint shift",
                })?;
        value = checked_add_u64(value, chunk, "varint value")?;

        if byte & 0x80 == 0 {
            return Ok(Some(Varint { value, bytes_read }));
        }

        shift = shift.checked_add(7).ok_or(ParseError::CarLengthOverflow {
            field: "varint shift",
        })?;
        if shift >= 64 {
            return Err(ParseError::MalformedVarint);
        }
    }
}

fn checked_increment(value: u64, field: &'static str) -> Result<u64, ParseError> {
    value
        .checked_add(1)
        .ok_or(ParseError::ResourceCountOverflow { field })
}

fn checked_add_u64(lhs: u64, rhs: u64, field: &'static str) -> Result<u64, ParseError> {
    lhs.checked_add(rhs)
        .ok_or(ParseError::CarLengthOverflow { field })
}

const fn ensure_u64_at_most(
    observed: u64,
    limit: u64,
    limit_name: &'static str,
    recovery: &'static str,
) -> Result<(), ParseError> {
    if observed <= limit {
        return Ok(());
    }

    Err(ParseError::ResourceLimitExceeded {
        limit: limit_name,
        observed,
        recovery,
    })
}

const fn enforce_decode_error_limit(observed: u64, limit: u64) -> Result<(), ParseError> {
    ensure_u64_at_most(
        observed,
        limit,
        "max_decode_errors",
        "raise parser max_decode_errors only after inspecting malformed records",
    )
}

#[derive(Debug, Clone, Copy)]
struct ParseDeadline {
    started_at: Instant,
    max_wall_clock: Duration,
}

impl ParseDeadline {
    fn start(max_wall_clock: Duration) -> Self {
        Self {
            started_at: Instant::now(),
            max_wall_clock,
        }
    }

    fn ensure_not_exceeded(self) -> Result<(), ParseError> {
        let elapsed = self.started_at.elapsed();
        if elapsed <= self.max_wall_clock {
            return Ok(());
        }
        Err(ParseError::ResourceLimitExceeded {
            limit: "max_parse_wall_clock",
            observed: elapsed.as_secs(),
            recovery: "raise parser max_parse_wall_clock only for a known-good repo",
        })
    }
}

fn read_only_store_error() -> RepoError {
    RepoError::storage(std::io::Error::other(
        "indexed CAR block store is read-only",
    ))
}

#[derive(Debug, Clone)]
struct CarStreamSummary {
    roots: Vec<IpldCid>,
    verified_block_count: u64,
}

#[derive(Debug)]
struct IndexedCar {
    roots: Vec<IpldCid>,
    verified_block_count: u64,
    index: BTreeMap<IpldCid, BlockLocation>,
}

#[derive(Debug, Deserialize)]
struct CarHeader {
    roots: Vec<IpldCid>,
    version: u64,
}

#[derive(Debug, Clone, Copy)]
struct BlockLocation {
    offset: u64,
    len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Varint {
    value: u64,
    bytes_read: u64,
}

const POST_COLLECTION: &str = "app.bsky.feed.post";
const POST_PREFIX: &str = "app.bsky.feed.post/";
const PROFILE_COLLECTION: &str = "app.bsky.actor.profile";
const PROFILE_RKEY: &str = "self";

#[cfg(test)]
mod tests {
    use super::{
        ParseConfig, ParseError, RkeyDigest, Varint, assert_requested_did,
        enforce_decode_error_limit, read_varint, split_repo_key, update_digest,
    };

    #[test]
    fn splits_repo_key_into_collection_and_rkey() {
        assert_eq!(
            split_repo_key("app.bsky.feed.post/3kabc"),
            Some(("app.bsky.feed.post", "3kabc"))
        );
        assert_eq!(split_repo_key("app.bsky.feed.post"), None);
    }

    #[test]
    fn reads_multibyte_varint() {
        let mut bytes = [0xac, 0x02].as_slice();
        assert_eq!(
            read_varint(&mut bytes).unwrap(),
            Some(Varint {
                value: 300,
                bytes_read: 2
            })
        );
    }

    #[test]
    fn digest_tracks_first_last_and_post_counts() {
        let mut digest = RkeyDigest::default();
        let config = ParseConfig::default();

        update_digest(&mut digest, "app.bsky.actor.profile/self", config).unwrap();
        update_digest(&mut digest, "app.bsky.feed.post/3kabc", config).unwrap();

        assert_eq!(digest.all_records_count, 2);
        assert_eq!(digest.post_records_count, 1);
        assert_eq!(
            digest.first_key.as_deref(),
            Some("app.bsky.actor.profile/self")
        );
        assert_eq!(digest.last_key.as_deref(), Some("app.bsky.feed.post/3kabc"));
    }

    #[test]
    fn requested_did_mismatch_is_loud() {
        let error = assert_requested_did(Some("did:plc:requested"), "did:plc:actual")
            .expect_err("mismatch should fail");

        assert!(matches!(
            error,
            ParseError::CommitDidMismatch {
                requested,
                actual
            } if requested == "did:plc:requested" && actual == "did:plc:actual"
        ));
    }

    #[test]
    fn record_limit_is_loud() {
        let mut digest = RkeyDigest::default();
        let config = ParseConfig {
            max_records: 1,
            ..ParseConfig::default()
        };

        update_digest(&mut digest, "app.bsky.feed.post/3kabc", config).unwrap();
        let error = update_digest(&mut digest, "app.bsky.feed.post/3kdef", config)
            .expect_err("second record should exceed cap");

        assert!(matches!(
            error,
            ParseError::ResourceLimitExceeded {
                limit: "max_records",
                observed: 2,
                ..
            }
        ));
    }

    #[test]
    fn decode_error_limit_is_loud() {
        let error = enforce_decode_error_limit(2, 1).expect_err("decode cap should fail");

        assert!(matches!(
            error,
            ParseError::ResourceLimitExceeded {
                limit: "max_decode_errors",
                observed: 2,
                ..
            }
        ));
    }
}
