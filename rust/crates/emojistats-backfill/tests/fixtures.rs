#![allow(
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::must_use_candidate
)]

use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use cid::Cid as IpldCid;
use jacquard_common::types::string::{Did, Tid};
use jacquard_repo::{commit::Commit, mst::util::compute_cid};
use serde::Serialize;
use smol_str::SmolStr;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

pub struct TempCar {
    pub path: PathBuf,
    root: PathBuf,
}

impl TempCar {
    pub fn from_bytes(name: &str, bytes: &[u8]) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "emojistats-backfill-tests-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("test temp directory should be created");
        let path = root.join(name);
        fs::write(&path, bytes).expect("test CAR should be written");
        Self { path, root }
    }
}

impl Drop for TempCar {
    fn drop(&mut self) {
        let _ignored = fs::remove_dir_all(&self.root);
    }
}

#[derive(Serialize)]
struct CarHeader<'a> {
    roots: &'a [IpldCid],
    version: u64,
}

pub fn car_bytes(roots: &[IpldCid], blocks: &[(IpldCid, Vec<u8>)]) -> Vec<u8> {
    let header = serde_ipld_dagcbor::to_vec(&CarHeader { roots, version: 1 })
        .expect("CAR header should encode");
    let mut bytes = frame(&header);
    for (cid, data) in blocks {
        let mut section = cid.to_bytes();
        section.extend_from_slice(data);
        bytes.extend_from_slice(&frame(&section));
    }
    bytes
}

pub fn malformed_header_car_bytes() -> Vec<u8> {
    frame(&[0xff, 0xff, 0xff])
}

pub fn empty_roots_car_bytes() -> Vec<u8> {
    car_bytes(&[], &[])
}

pub fn root_without_block_car_bytes() -> Vec<u8> {
    let root = dag_cbor_cid(&serde_json::json!({ "root": "missing" }));
    car_bytes(&[root], &[])
}

pub fn non_commit_root_car_bytes() -> Vec<u8> {
    let block = dag_cbor_bytes(&serde_json::json!({ "not": "a commit" }));
    let root = compute_cid(&block).expect("fixture block CID should compute");
    car_bytes(&[root], &[(root, block)])
}

pub fn commit_only_car_bytes(actual_did: &str) -> Vec<u8> {
    let data = dag_cbor_cid(&serde_json::json!({ "entries": [] }));
    let commit = Commit {
        did: Did::<SmolStr>::new_owned(actual_did).expect("fixture DID should be valid"),
        version: 3,
        data,
        rev: Tid::new("3jui7kd54zh2y").expect("fixture TID should be valid"),
        prev: None,
        sig: Bytes::new(),
    };
    let block = commit.to_cbor().expect("fixture commit should encode");
    let root = compute_cid(&block).expect("fixture commit CID should compute");
    car_bytes(&[root], &[(root, block)])
}

fn dag_cbor_cid(value: &serde_json::Value) -> IpldCid {
    compute_cid(&dag_cbor_bytes(value)).expect("fixture CID should compute")
}

fn dag_cbor_bytes(value: &serde_json::Value) -> Vec<u8> {
    serde_ipld_dagcbor::to_vec(value).expect("fixture value should encode as DAG-CBOR")
}

fn frame(bytes: &[u8]) -> Vec<u8> {
    let mut framed = encode_varint(u64::try_from(bytes.len()).expect("fixture length fits u64"));
    framed.extend_from_slice(bytes);
    framed
}

fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let chunk = u8::try_from(value & 0x7f).expect("varint chunk fits u8");
        value >>= 7;
        if value == 0 {
            bytes.push(chunk);
            break;
        }
        bytes.push(chunk | 0x80);
    }
    bytes
}
