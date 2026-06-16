use std::{
    collections::{HashMap, hash_map::Entry},
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
    thread,
};

use bytes::Bytes;
use cid::Cid as IpldCid;
use jacquard_repo::{
    CommitData, DAG_CBOR_CID_CODEC, error::RepoError, mst::util::compute_cid, storage::BlockStore,
};
use serde::Deserialize;

use super::{
    ParseConfig, ParseDeadline, ParseError, checked_add_u64, checked_increment, ensure_u64_at_most,
};

const ESTIMATED_INDEX_BYTES_PER_BLOCK: u64 = 160;

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

#[derive(Clone, Copy)]
struct CidVerifyJob {
    cid: IpldCid,
    data: BlockLocation,
}

enum CidVerifier {
    Inline { file: Arc<File> },
    Parallel(ParallelCidVerifier),
}

struct ParallelCidVerifier {
    senders: Vec<mpsc::SyncSender<CidVerifyJob>>,
    workers: Vec<thread::JoinHandle<Result<(), ParseError>>>,
    next_sender: usize,
}

impl CidVerifier {
    fn start(file: &Arc<File>, worker_count: usize) -> Self {
        if worker_count <= 1 {
            return Self::Inline {
                file: Arc::clone(file),
            };
        }
        Self::Parallel(ParallelCidVerifier::start(file, worker_count))
    }

    fn verify(&mut self, job: CidVerifyJob) -> Result<(), ParseError> {
        match self {
            Self::Inline { file } => verify_block_cid_at(file, job),
            Self::Parallel(verifier) => verifier.verify(job),
        }
    }

    fn finish(self) -> Result<(), ParseError> {
        match self {
            Self::Inline { .. } => Ok(()),
            Self::Parallel(verifier) => verifier.finish(),
        }
    }
}

impl ParallelCidVerifier {
    fn start(file: &Arc<File>, worker_count: usize) -> Self {
        let worker_count = worker_count.max(1);
        let mut senders = Vec::with_capacity(worker_count);
        let mut workers = Vec::with_capacity(worker_count);
        for _worker in 0..worker_count {
            let (sender, receiver) = mpsc::sync_channel(2);
            senders.push(sender);
            let worker_file = Arc::clone(file);
            workers.push(thread::spawn(move || {
                verify_cid_jobs(&worker_file, receiver)
            }));
        }
        Self {
            senders,
            workers,
            next_sender: 0,
        }
    }

    fn verify(&mut self, job: CidVerifyJob) -> Result<(), ParseError> {
        let sender = self
            .senders
            .get(self.next_sender)
            .ok_or(ParseError::MalformedCar(
                "CID verifier has no workers".to_owned(),
            ))?;
        sender
            .send(job)
            .map_err(|_error| ParseError::MalformedCar("CID verifier stopped".to_owned()))?;
        let next_sender = self
            .next_sender
            .checked_add(1)
            .ok_or(ParseError::MalformedCar(
                "CID verifier sender index overflow".to_owned(),
            ))?;
        self.next_sender = if next_sender == self.senders.len() {
            0
        } else {
            next_sender
        };
        Ok(())
    }

    fn finish(mut self) -> Result<(), ParseError> {
        self.join_workers()
    }

    fn join_workers(&mut self) -> Result<(), ParseError> {
        self.senders.clear();
        for worker in self.workers.drain(..) {
            worker
                .join()
                .map_err(|_error| ParseError::MalformedCar("CID verifier panicked".to_owned()))??;
        }
        Ok(())
    }
}

impl Drop for ParallelCidVerifier {
    fn drop(&mut self) {
        let _ignored = self.join_workers();
    }
}

fn verify_cid_jobs(file: &File, receiver: mpsc::Receiver<CidVerifyJob>) -> Result<(), ParseError> {
    for job in receiver {
        verify_block_cid_at(file, job)?;
    }
    Ok(())
}

fn verify_block_cid_at(file: &File, job: CidVerifyJob) -> Result<(), ParseError> {
    let data = read_block_at(file, &job.data).map_err(|source| ParseError::Io {
        path: PathBuf::from("<car block verifier>"),
        source,
    })?;
    verify_block_cid(job.cid, &data)
}

#[derive(Debug, Clone)]
pub(super) struct IndexedCarBlockStore {
    file: Arc<File>,
    index: Arc<HashMap<IpldCid, BlockLocation>>,
}

impl IndexedCarBlockStore {
    pub(super) fn load(
        path: &Path,
        config: ParseConfig,
        deadline: ParseDeadline,
    ) -> Result<(CarStreamSummary, Self), ParseError> {
        let indexed_car = index_car_blocks(path, config, deadline)?;
        let summary = CarStreamSummary {
            roots: indexed_car.roots,
            verified_block_count: indexed_car.verified_block_count,
            duplicate_block_cid_count: indexed_car.duplicate_block_cid_count,
        };
        let store = Self {
            file: Arc::new(open_file(path)?),
            index: Arc::new(indexed_car.index),
        };
        Ok((summary, store))
    }

    pub(super) fn get_block_bytes(&self, cid: &IpldCid) -> jacquard_repo::Result<Option<Bytes>> {
        let Some(location) = self.index.get(cid) else {
            return Ok(None);
        };
        read_block_at(&self.file, location)
            .map(Bytes::from)
            .map(Some)
            .map_err(RepoError::io)
    }
}

#[allow(clippy::unused_async_trait_impl)]
impl BlockStore for IndexedCarBlockStore {
    async fn get(&self, cid: &IpldCid) -> jacquard_repo::Result<Option<Bytes>> {
        self.get_block_bytes(cid)
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

    async fn apply_commit(&self, _commit: CommitData) -> jacquard_repo::Result<()> {
        Err(read_only_store_error())
    }
}

fn index_car_blocks(
    path: &Path,
    config: ParseConfig,
    deadline: ParseDeadline,
) -> Result<IndexedCar, ParseError> {
    let mut file = open_file(path)?;
    let verify_file = Arc::new(file.try_clone().map_err(|source| ParseError::Io {
        path: path.to_path_buf(),
        source,
    })?);
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

    let mut index = HashMap::new();
    let mut verifier = CidVerifier::start(&verify_file, config.cid_verification_threads);
    let mut indexed_block_count = 0_u64;
    let mut duplicate_block_cid_count = 0_u64;
    while let Some(section_len) = read_varint(&mut file)? {
        offset = checked_add_u64(offset, section_len.bytes_read, "section varint")?;
        let section_start = offset;
        let block = read_car_block_section(&mut file, path, section_len, section_start, config)?;
        verifier.verify(CidVerifyJob {
            cid: block.cid,
            data: block.location,
        })?;

        match index.entry(block.cid) {
            Entry::Vacant(entry) => {
                entry.insert(block.location);
            }
            Entry::Occupied(_entry) => {
                duplicate_block_cid_count =
                    checked_increment(duplicate_block_cid_count, "duplicate_block_cid_count")?;
            }
        }

        indexed_block_count = checked_increment(indexed_block_count, "indexed_block_count")?;
        ensure_u64_at_most(
            indexed_block_count,
            config.max_car_blocks,
            "max_car_blocks",
            "raise parser max_car_blocks only for a known-good repo",
        )?;
        enforce_index_memory_limit(indexed_block_count, config.max_index_bytes)?;
        deadline.ensure_not_exceeded()?;
        offset = checked_add_u64(section_start, block.section_len, "section end")?;
    }
    verifier.finish()?;

    Ok(IndexedCar {
        roots: header.roots,
        verified_block_count: indexed_block_count,
        duplicate_block_cid_count,
        index,
    })
}

fn read_car_block_section(
    file: &mut File,
    path: &Path,
    section_len: Varint,
    section_start: u64,
    config: ParseConfig,
) -> Result<IndexedCarSection, ParseError> {
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
    let cid =
        IpldCid::read_bytes(&mut cursor).map_err(|source| ParseError::CidRead(Box::new(source)))?;
    let cid_len = cursor.position();
    let data_len = section_len
        .value
        .checked_sub(cid_len)
        .ok_or(ParseError::MalformedCar(
            "block section shorter than CID".to_owned(),
        ))?;
    let data_len_usize =
        usize::try_from(data_len).map_err(|_err| ParseError::CarLengthOverflow {
            field: "block data length",
        })?;

    Ok(IndexedCarSection {
        cid,
        location: BlockLocation {
            offset: checked_add_u64(section_start, cid_len, "block data offset")?,
            len: data_len_usize,
        },
        section_len: section_len.value,
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

fn read_block_at(file: &File, location: &BlockLocation) -> std::io::Result<Vec<u8>> {
    let mut bytes = vec![0_u8; location.len];
    file.read_exact_at(&mut bytes, location.offset)?;
    Ok(bytes)
}

fn open_file(path: &Path) -> Result<File, ParseError> {
    File::open(path).map_err(|source| ParseError::Io {
        path: path.to_path_buf(),
        source,
    })
}

pub(super) fn read_varint(reader: &mut impl Read) -> Result<Option<Varint>, ParseError> {
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

pub(super) const fn enforce_index_memory_limit(
    blocks: u64,
    max_index_bytes: u64,
) -> Result<(), ParseError> {
    let Some(observed) = blocks.checked_mul(ESTIMATED_INDEX_BYTES_PER_BLOCK) else {
        return Err(ParseError::ResourceLimitExceeded {
            limit: "max_index_bytes",
            observed: u64::MAX,
            recovery: "raise parser max_index_bytes only after confirming available parser RAM",
        });
    };
    ensure_u64_at_most(
        observed,
        max_index_bytes,
        "max_index_bytes",
        "raise parser max_index_bytes only after confirming available parser RAM",
    )
}

fn read_only_store_error() -> RepoError {
    RepoError::storage(std::io::Error::other(
        "indexed CAR block store is read-only",
    ))
}

#[derive(Debug, Clone)]
pub(super) struct CarStreamSummary {
    pub(super) roots: Vec<IpldCid>,
    pub(super) verified_block_count: u64,
    pub(super) duplicate_block_cid_count: u64,
}

#[derive(Debug)]
struct IndexedCar {
    pub(super) roots: Vec<IpldCid>,
    pub(super) verified_block_count: u64,
    duplicate_block_cid_count: u64,
    index: HashMap<IpldCid, BlockLocation>,
}

#[derive(Debug, Clone, Copy)]
struct IndexedCarSection {
    cid: IpldCid,
    location: BlockLocation,
    section_len: u64,
}

#[derive(Debug, Deserialize)]
struct CarHeader {
    pub(super) roots: Vec<IpldCid>,
    version: u64,
}

#[derive(Debug, Clone, Copy)]
struct BlockLocation {
    offset: u64,
    len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Varint {
    pub(super) value: u64,
    pub(super) bytes_read: u64,
}
