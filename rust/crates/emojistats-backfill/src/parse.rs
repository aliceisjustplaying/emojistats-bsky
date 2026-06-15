//! Stage C `CAR` parser for the v2 backfill pipeline.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use cid::Cid as IpldCid;
use jacquard_api::app_bsky::{actor::profile::Profile, feed::post::Post};
use jacquard_repo::{
    DAG_CBOR_CID_CODEC, Mst, car,
    commit::Commit,
    error::RepoError,
    mst::{
        cursor::{CursorPosition, MstCursor},
        util::compute_cid,
    },
    storage::BlockStore,
};
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
    /// Typed record decode failures observed while walking reachable records.
    pub record_decode_errors: Vec<RecordDecodeFailure>,
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

/// Reachable record that failed typed decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordDecodeFailure {
    /// Full repo key.
    pub key: String,
    /// Collection being decoded.
    pub collection: &'static str,
    /// Record block `CID`.
    pub cid: String,
    /// Decode error message.
    pub message: String,
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

/// Parse a spooled repo `CAR` from disk.
///
/// # Errors
///
/// Returns [`ParseError`] for malformed `CAR`s, `CID` mismatches, missing reachable blocks,
/// invalid commits, `MST` traversal failures, typed record decode failures, and local I/O errors.
pub fn parse_repo(car_path: &Path) -> Result<ParsedRepo, ParseError> {
    let car_path = car_path.to_path_buf();
    std::thread::Builder::new()
        .name("emojistats-stage-c-parse".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(ParseError::Runtime)?;
            runtime.block_on(parse_repo_async(&car_path))
        })
        .map_err(ParseError::ThreadSpawn)?
        .join()
        .map_err(|_err| ParseError::RuntimeThreadTerminated)?
}

async fn parse_repo_async(car_path: &Path) -> Result<ParsedRepo, ParseError> {
    let stream_summary = verify_streamed_car(car_path).await?;
    let store = IndexedCarBlockStore::load(car_path)?;
    let commit_root = single_car_root(&stream_summary.roots)?;
    let (commit_cid, commit) = load_commit(commit_root, &store).await?;
    let (posts, profile, profile_decode_error, record_decode_errors, rkey_digest) =
        walk_mst_records(commit.data, &store).await?;

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

    Ok(ParsedRepo {
        commit: CommitMeta {
            cid: commit_cid.to_string(),
            did: commit.did().as_str().to_owned(),
            version: commit.version,
            rev: commit.rev().as_str().to_owned(),
            data: commit.data().to_string(),
            prev: commit.prev().map(ToString::to_string),
        },
        completeness: proof,
        posts,
        rkey_digest,
        profile,
        profile_decode_error,
        record_decode_errors,
    })
}

async fn verify_streamed_car(car_path: &Path) -> Result<CarStreamSummary, ParseError> {
    let mut stream = car::stream_car(car_path).await?;
    let roots = stream.roots().to_vec();
    let mut verified_block_count = 0_u64;

    while let Some((cid, bytes)) = stream.next().await? {
        verify_block_cid(cid, bytes.as_ref())?;
        verified_block_count = checked_increment(verified_block_count, "verified_block_count")?;
    }

    Ok(CarStreamSummary {
        roots,
        verified_block_count,
    })
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
    if let Some(bytes) = store.get(&root).await?
        && let Ok(commit) = Commit::<SmolStr>::from_cbor(bytes.as_ref())
    {
        return Ok((root, commit));
    }

    for cid in store.cids() {
        let Some(bytes) = store.get(&cid).await? else {
            continue;
        };
        if let Ok(commit) = Commit::<SmolStr>::from_cbor(bytes.as_ref()) {
            return Ok((cid, commit));
        }
    }

    Err(ParseError::CommitNotFound {
        root: root.to_string(),
    })
}

async fn walk_mst_records(
    root: IpldCid,
    store: &IndexedCarBlockStore,
) -> Result<
    (
        Vec<PostRecord>,
        Option<ProfileRecord>,
        Option<String>,
        Vec<RecordDecodeFailure>,
        RkeyDigest,
    ),
    ParseError,
> {
    let mst = Mst::load(Arc::new(store.clone()), root, None);
    let traversed_root = mst.root().await?;
    if traversed_root != root {
        return Err(ParseError::MstRootMismatch {
            commit_data: root.to_string(),
            traversed_root: traversed_root.to_string(),
        });
    }

    let mut cursor = MstCursor::new(mst);
    let mut posts = Vec::new();
    let mut profile = None;
    let mut profile_decode_error = None;
    let mut record_decode_errors = Vec::new();
    let mut digest = RkeyDigest::default();

    cursor.advance().await?;
    while !cursor.is_end() {
        match cursor.current().clone() {
            CursorPosition::Leaf { key, cid } => {
                let key_text = key.to_string();
                let record_bytes =
                    store
                        .get(&cid)
                        .await?
                        .ok_or_else(|| ParseError::MissingBlock {
                            cid: cid.to_string(),
                        })?;
                update_digest(&mut digest, &key_text)?;
                extract_known_record(
                    &key_text,
                    cid,
                    record_bytes.as_ref(),
                    &mut posts,
                    &mut profile,
                    &mut profile_decode_error,
                    &mut record_decode_errors,
                );
                cursor.advance().await?;
            }
            CursorPosition::Tree { .. } => {
                cursor.advance().await?;
            }
            CursorPosition::End => {}
        }
    }

    Ok((
        posts,
        profile,
        profile_decode_error,
        record_decode_errors,
        digest,
    ))
}

fn extract_known_record(
    key: &str,
    cid: IpldCid,
    record_bytes: &[u8],
    posts: &mut Vec<PostRecord>,
    profile: &mut Option<ProfileRecord>,
    profile_decode_error: &mut Option<String>,
    record_decode_errors: &mut Vec<RecordDecodeFailure>,
) {
    let Some((collection, rkey)) = split_repo_key(key) else {
        return;
    };

    match collection {
        POST_COLLECTION => match serde_ipld_dagcbor::from_slice::<Post<SmolStr>>(record_bytes) {
            Ok(record) => posts.push(PostRecord {
                rkey: rkey.to_owned(),
                cid: cid.to_string(),
                record,
            }),
            Err(error) => record_decode_errors.push(RecordDecodeFailure {
                key: key.to_owned(),
                collection: POST_COLLECTION,
                cid: cid.to_string(),
                message: error.to_string(),
            }),
        },
        PROFILE_COLLECTION if rkey == PROFILE_RKEY => {
            match serde_ipld_dagcbor::from_slice::<Profile<SmolStr>>(record_bytes) {
                Ok(record) => {
                    *profile = Some(ProfileRecord {
                        rkey: rkey.to_owned(),
                        cid: cid.to_string(),
                        record,
                    });
                }
                Err(error) => {
                    let message = error.to_string();
                    *profile_decode_error =
                        Some(format!("{PROFILE_COLLECTION}/{rkey} at {cid}: {message}"));
                    record_decode_errors.push(RecordDecodeFailure {
                        key: key.to_owned(),
                        collection: PROFILE_COLLECTION,
                        cid: cid.to_string(),
                        message,
                    });
                }
            }
        }
        _other => {}
    }
}

fn update_digest(digest: &mut RkeyDigest, key: &str) -> Result<(), ParseError> {
    digest.all_records_count = checked_increment(digest.all_records_count, "all_records_count")?;
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
    fn load(path: &Path) -> Result<Self, ParseError> {
        let index = index_car_blocks(path)?;
        Ok(Self {
            path: Arc::new(path.to_path_buf()),
            index: Arc::new(index),
        })
    }

    fn cids(&self) -> Vec<IpldCid> {
        self.index.keys().copied().collect()
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

fn index_car_blocks(path: &Path) -> Result<BTreeMap<IpldCid, BlockLocation>, ParseError> {
    let mut file = open_file(path)?;
    let Some(header_len) = read_varint(&mut file)? else {
        return Err(ParseError::InvalidRoots("CAR file is empty".to_owned()));
    };
    let mut offset = checked_add_u64(header_len.bytes_read, header_len.value, "header")?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|source| ParseError::Io {
            path: path.to_path_buf(),
            source,
        })?;

    let mut index = BTreeMap::new();
    while let Some(section_len) = read_varint(&mut file)? {
        offset = checked_add_u64(offset, section_len.bytes_read, "section varint")?;
        let section_start = offset;
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

        offset = checked_add_u64(section_start, section_len.value, "section end")?;
    }

    Ok(index)
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
    use super::{RkeyDigest, Varint, read_varint, split_repo_key, update_digest};

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

        update_digest(&mut digest, "app.bsky.actor.profile/self").unwrap();
        update_digest(&mut digest, "app.bsky.feed.post/3kabc").unwrap();

        assert_eq!(digest.all_records_count, 2);
        assert_eq!(digest.post_records_count, 1);
        assert_eq!(
            digest.first_key.as_deref(),
            Some("app.bsky.actor.profile/self")
        );
        assert_eq!(digest.last_key.as_deref(), Some("app.bsky.feed.post/3kabc"));
    }
}
