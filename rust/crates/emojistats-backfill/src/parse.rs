//! Stage C `CAR` parser for the v2 backfill pipeline.

use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use car::IndexedCarBlockStore;
use cid::Cid as IpldCid;
use jacquard_api::app_bsky::{actor::profile::Profile, feed::post::Post};
use jacquard_repo::{commit::Commit, error::RepoError};
use mst::walk_mst_records_visit;
use smol_str::SmolStr;

#[path = "raw_partial_post.rs"]
pub(crate) mod raw_partial_post;

mod car;
mod mst;
mod record;

const DEFAULT_MAX_INDEX_BYTES: u64 = 4_294_967_296;

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
    /// Coarse parser timings for crawler telemetry.
    pub timings: ParseTimings,
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
    /// Coarse parser timings for crawler telemetry.
    pub timings: ParseTimings,
}

/// Coarse parser stage timings in milliseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ParseTimings {
    /// Full parser wall time.
    pub total_ms: u64,
    /// CAR scan, block indexing, and CID verification.
    pub index_ms: u64,
    /// Root commit load and requested-DID validation.
    pub commit_ms: u64,
    /// MST traversal, record reads, and post/profile extraction.
    pub walk_ms: u64,
}

/// Resource caps for Stage C parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseConfig {
    /// Maximum number of `CAR` blocks accepted while verifying or indexing.
    pub max_car_blocks: u64,
    /// Maximum estimated memory used by the in-memory `CAR` block index.
    pub max_index_bytes: u64,
    /// Maximum encoded `CAR` block section size accepted before allocation.
    pub max_block_bytes: u64,
    /// Maximum number of reachable repo records accepted while walking the `MST`.
    pub max_records: u64,
    /// Maximum `MST` cursor layer accepted while walking records.
    pub max_mst_depth: u64,
    /// Maximum decoded entries accepted in one `MST` node.
    pub max_mst_node_entries: u64,
    /// Maximum total reconstructed key bytes accepted in one `MST` node.
    pub max_mst_node_key_bytes: u64,
    /// Maximum number of non-fatal typed record decode errors accepted.
    pub max_decode_errors: u64,
    /// Maximum best-effort parser wall-clock time.
    pub max_parse_wall_clock: Duration,
    /// Worker threads used for CAR block CID verification.
    ///
    /// Parallel verification sends block offsets to workers so queued work is bounded by job
    /// metadata, not by encoded block bytes.
    pub cid_verification_threads: usize,
}

impl Default for ParseConfig {
    fn default() -> Self {
        Self {
            max_car_blocks: 10_000_000,
            max_index_bytes: DEFAULT_MAX_INDEX_BYTES,
            max_block_bytes: 67_108_864,
            max_records: 10_000_000,
            max_mst_depth: 256,
            max_mst_node_entries: 65_536,
            max_mst_node_key_bytes: 8_388_608,
            max_decode_errors: 1_000_000,
            #[allow(clippy::duration_suboptimal_units)]
            max_parse_wall_clock: Duration::from_secs(15 * 60),
            cid_verification_threads: default_cid_verification_threads(),
        }
    }
}

/// Recommended worker count for CAR block CID verification.
#[must_use]
pub const fn default_cid_verification_threads() -> usize {
    4
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
    /// Number of verified `CAR` block entries whose `CID` had already appeared earlier.
    pub duplicate_block_cid_count: u64,
    /// Number of reachable `MST` leaves whose record block resolved by `CID`.
    pub reachable_record_count: u64,
    /// Whether the commit's `data` root block was present, traversed, and content-address verified.
    ///
    /// This does not recompute a new root from decoded `MST` nodes; it proves that the root block
    /// named by the commit exists in the `CAR`, its bytes match its `CID`, and traversal reached
    /// records through verified child links.
    pub mst_root_cid_verified: bool,
    /// Commit signature verification is deliberately out of scope for Stage C.
    pub repo_commit_signature_verified: bool,
    /// Identity verification is deliberately out of scope for Stage C.
    pub identity_verified: bool,
}

/// Completeness class assigned to the parsed repo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletenessClass {
    /// Complete `CAR` snapshot with content-address verified commit, `MST`, and record links.
    ContentAddressedSnapshot,
}

/// Extracted post record plus repo key context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostRecord {
    /// Repo record key.
    pub rkey: String,
    /// Record block `CID`.
    pub cid: String,
    /// Typed record body, or raw recovered fields when typed decode failed.
    pub body: PostRecordBody,
}

/// Parsed post body variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostRecordBody {
    /// Typed Bluesky post record.
    Typed(Box<Post<SmolStr>>),
    /// Raw fields recovered from a post record.
    RawPartial(RawPartialPostRecord),
}

/// Raw post fields preserved from `app.bsky.feed.post`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPartialPostRecord {
    /// Whether the raw post was missing fields needed by the typed lexicon model.
    pub typed_decode_failed: bool,
    /// Author-supplied `createdAt` bytes represented as JSON when present.
    pub created_at_raw: Option<String>,
    /// Author-supplied text when it was a string.
    pub text: Option<String>,
    /// Author-supplied langs when they were strings.
    pub langs: Vec<String>,
    /// Non-core record fields preserved as JSON.
    pub extras_json: serde_json::Value,
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
    parse_repo_sync(car_path, None, config)
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
    parse_repo_sync(car_path, Some(requested_did), config)
}

/// Parse a spooled repo `CAR` on the current thread, visiting each decoded post without retaining
/// all posts.
///
/// The caller-owned `state` is returned with the summary. Async callers should invoke this from
/// their blocking-task boundary.
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
    mut visit_post: F,
) -> Result<(ParsedRepoSummary, S), ParseVisitError<E>>
where
    F: FnMut(&mut S, PostRecord) -> Result<(), E>,
{
    parse_repo_visit(
        car_path,
        Some(requested_did),
        config,
        state,
        &mut visit_post,
    )
}

fn parse_repo_sync(
    car_path: &Path,
    requested_did: Option<&str>,
    config: ParseConfig,
) -> Result<ParsedRepo, ParseError> {
    let mut posts = Vec::new();
    let (summary, ()) = parse_repo_visit(
        car_path,
        requested_did,
        config,
        (),
        &mut |_state: &mut (), post| {
            posts.push(post);
            Ok::<(), std::convert::Infallible>(())
        },
    )
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
        timings: summary.timings,
    })
}

fn parse_repo_visit<S, E, F>(
    car_path: &Path,
    requested_did: Option<&str>,
    config: ParseConfig,
    mut state: S,
    visit_post: &mut F,
) -> Result<(ParsedRepoSummary, S), ParseVisitError<E>>
where
    F: FnMut(&mut S, PostRecord) -> Result<(), E>,
{
    let total_started = Instant::now();
    let deadline = ParseDeadline::start(config.max_parse_wall_clock);
    let index_started = Instant::now();
    let (stream_summary, store) = IndexedCarBlockStore::load(car_path, config, deadline)?;
    let index_ms = elapsed_millis(index_started);
    deadline.ensure_not_exceeded()?;
    let commit_started = Instant::now();
    let commit_root = single_car_root(&stream_summary.roots)?;
    let (commit_cid, commit) = load_commit(commit_root, &store)?;
    deadline.ensure_not_exceeded()?;
    assert_requested_did(requested_did, commit.did().as_str())?;
    let commit_ms = elapsed_millis(commit_started);
    let walk_started = Instant::now();
    let (profile, profile_decode_error, decode_digest, rkey_digest) = walk_mst_records_visit(
        commit.data,
        &store,
        config,
        deadline,
        &mut state,
        visit_post,
    )?;
    let walk_ms = elapsed_millis(walk_started);

    let proof = CompletenessProof {
        class: CompletenessClass::ContentAddressedSnapshot,
        car_roots: stream_summary
            .roots
            .iter()
            .map(ToString::to_string)
            .collect(),
        verified_block_count: stream_summary.verified_block_count,
        duplicate_block_cid_count: stream_summary.duplicate_block_cid_count,
        reachable_record_count: rkey_digest.all_records_count,
        mst_root_cid_verified: true,
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
            timings: ParseTimings {
                total_ms: elapsed_millis(total_started),
                index_ms,
                commit_ms,
                walk_ms,
            },
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

fn load_commit(
    root: IpldCid,
    store: &IndexedCarBlockStore,
) -> Result<(IpldCid, Commit<SmolStr>), ParseError> {
    let Some(bytes) = store.get_block_bytes(&root).map_err(ParseError::Repo)? else {
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

pub(super) fn checked_increment(value: u64, field: &'static str) -> Result<u64, ParseError> {
    value
        .checked_add(1)
        .ok_or(ParseError::ResourceCountOverflow { field })
}

pub(super) fn checked_add_u64(lhs: u64, rhs: u64, field: &'static str) -> Result<u64, ParseError> {
    lhs.checked_add(rhs)
        .ok_or(ParseError::CarLengthOverflow { field })
}

pub(super) const fn ensure_u64_at_most(
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

#[derive(Debug, Clone, Copy)]
pub(super) struct ParseDeadline {
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

    pub(super) fn ensure_not_exceeded(self) -> Result<(), ParseError> {
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

fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{
        ParseConfig, ParseError, RkeyDigest, assert_requested_did,
        car::{Varint, enforce_index_memory_limit, read_varint},
        default_cid_verification_threads,
        record::{enforce_decode_error_limit, split_repo_key, update_digest, validate_repo_key},
    };

    #[test]
    fn cid_verification_threads_default_to_bounded_parallelism() {
        assert_eq!(default_cid_verification_threads(), 4);
    }

    #[test]
    fn splits_repo_key_into_collection_and_rkey() {
        assert_eq!(
            split_repo_key("app.bsky.feed.post/3kabc"),
            Some(("app.bsky.feed.post", "3kabc"))
        );
        assert_eq!(split_repo_key("app.bsky.feed.post"), None);
        assert_eq!(split_repo_key("app.bsky.feed.post/"), None);
        assert_eq!(split_repo_key("app.bsky.feed.post/3kabc/extra"), None);
        assert_eq!(split_repo_key("not a collection/3kabc"), None);
    }

    #[test]
    fn validates_repo_key_shape() {
        validate_repo_key("app.bsky.feed.post/3kabc").unwrap();

        assert!(matches!(
            validate_repo_key("app.bsky.feed.post/"),
            Err(ParseError::MalformedCar(message))
                if message == "invalid repo key \"app.bsky.feed.post/\": rkey is empty"
        ));
        assert!(matches!(
            validate_repo_key("app.bsky.feed.post/3kabc/extra"),
            Err(ParseError::MalformedCar(message))
                if message
                    == "invalid repo key \"app.bsky.feed.post/3kabc/extra\": rkey contains an extra slash"
        ));
        assert!(matches!(
            validate_repo_key("app.bsky.feed.post"),
            Err(ParseError::MalformedCar(message))
                if message
                    == "invalid repo key \"app.bsky.feed.post\": missing collection/rkey separator"
        ));
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

    #[test]
    fn index_memory_limit_is_loud() {
        let error = enforce_index_memory_limit(2, 160).expect_err("index cap should fail");

        assert!(matches!(
            error,
            ParseError::ResourceLimitExceeded {
                limit: "max_index_bytes",
                observed: 320,
                ..
            }
        ));
    }
}
