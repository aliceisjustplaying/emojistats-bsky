#![allow(clippy::expect_used)]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use emojistats_backfill::{
    archive::{
        ArchiveCommitContext, ArchivePostRow, CompletenessClass, CreatedAtParseStatus, FetchMethod,
        RepoReceipt, RepoReceiptInput, build_repo_receipt, current_normalizer, hash_profile_record,
        write_archive_artifacts,
    },
    commit::{ManifestEntry, Receipt},
    parse::ProfileRecord,
};
use jacquard_api::app_bsky::actor::profile::Profile;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use smol_str::SmolStr;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

#[test]
fn profile_sidecar_is_written_as_committed_artifact() {
    let temp = TempDir::new();
    let output_dir = temp.path.join("archive");
    let rows = vec![archive_row()];
    let profile = profile_record();
    let receipt = receipt_for(&rows, &profile);

    let artifacts = write_archive_artifacts(
        &output_dir,
        "did:plc:fixture123",
        &ArchiveCommitContext::new("run-test", "shard3", 9),
        &rows,
        Some(&profile),
        &receipt,
    )
    .expect("archive artifacts should write");

    let profile_path = artifacts
        .profile_sidecar_path
        .expect("profile sidecar should be committed");
    let profile_receipt_path = artifacts
        .profile_sidecar_receipt_path
        .expect("profile sidecar receipt should be written");
    let profile_manifest_path = artifacts
        .profile_sidecar_manifest_path
        .expect("profile sidecar manifest should be written");
    let receipt: Receipt = read_json(&profile_receipt_path);
    let manifest = fs::read_to_string(&profile_manifest_path)
        .expect("profile sidecar manifest should be readable");
    let mut lines = manifest.lines();
    let entry: ManifestEntry = serde_json::from_str(
        lines
            .next()
            .expect("profile sidecar manifest should contain one entry"),
    )
    .expect("profile sidecar manifest entry should decode");

    assert!(lines.next().is_none());
    assert_eq!(profile_path.parent(), Some(output_dir.as_path()));
    assert_path_name_shape(&profile_path, "did_plc_fixture123__", ".profile.json");
    assert_path_name_shape(
        &profile_receipt_path,
        "did_plc_fixture123__",
        ".profile.object-receipt.json",
    );
    assert_path_name_shape(
        &profile_manifest_path,
        "did_plc_fixture123__",
        ".profile.manifest.jsonl",
    );
    assert_eq!(receipt.dataset, "raw_profile_sidecar");
    assert_eq!(receipt.run_id, "run-test");
    assert_eq!(receipt.shard, "shard3");
    assert_eq!(receipt.file_sequence, 9);
    assert!(receipt.object_path.starts_with("did_plc_fixture123__"));
    assert!(receipt.object_path.ends_with(".profile.json"));
    assert_eq!(receipt.row_count, 1);
    assert_eq!(receipt.content_hash, sha256_file(&profile_path));
    assert_eq!(entry.dataset, receipt.dataset);
    assert_eq!(entry.run_id, receipt.run_id);
    assert_eq!(entry.shard, receipt.shard);
    assert_eq!(entry.file_sequence, receipt.file_sequence);
    assert_eq!(entry.object_path, receipt.object_path);
    assert_eq!(entry.row_count, receipt.row_count);
    assert_eq!(entry.bytes, receipt.bytes);
    assert_eq!(entry.content_hash, receipt.content_hash);
    assert_eq!(entry.receipt_hash, receipt.receipt_hash);

    let profile_json: serde_json::Value = read_json(&profile_path);
    assert_eq!(json_field(&profile_json, "rkey"), "self");
    assert_eq!(json_field(&profile_json, "cid"), "bafy-profile");
    assert_eq!(
        profile_json
            .get("record")
            .and_then(|record| record.get("displayName"))
            .and_then(serde_json::Value::as_str),
        Some("alice")
    );
}

fn assert_path_name_shape(path: &Path, prefix: &str, suffix: &str) {
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .expect("path should have UTF-8 file name");
    assert!(name.starts_with(prefix), "{name}");
    assert!(name.ends_with(suffix), "{name}");
}

fn receipt_for(rows: &[ArchivePostRow], profile: &ProfileRecord) -> RepoReceipt {
    build_repo_receipt(RepoReceiptInput {
        rows,
        observed_at: ArchiveCommitContext::fetch_one_local().observed_at,
        did: "did:plc:test",
        fetch_method: FetchMethod::GetRepo,
        completeness_class: CompletenessClass::ContentAddressedSnapshot,
        reachable_records_count: u64::try_from(rows.len()).expect("row count should fit u64"),
        reachable_post_records_count: u64::try_from(rows.len()).expect("row count should fit u64"),
        post_decode_error_count: 0,
        profile_row_hash: hash_profile_record(Some(profile)).expect("profile hash should build"),
        mst_root_cid: Some(
            "bafyreihyrpejdc3l3wqlbm7vuzx7hhvx6r5eg44vqyqjna6u6kwtpoyqte".to_owned(),
        ),
        commit_cid: Some("bafyreibqj2lhp4fpizc2zstcsl2mzo6fycjfnwc6kyz4xpr2lzyqlw6wxi".to_owned()),
        normalizer: current_normalizer(),
    })
    .expect("receipt should build")
}

fn archive_row() -> ArchivePostRow {
    ArchivePostRow {
        did: "did:plc:fixture123".to_owned(),
        rkey: "3jui7kd54zh2y".to_owned(),
        cid: "bafy-post".to_owned(),
        normalizer: current_normalizer(),
        account_status: None,
        record_status: None,
        public_content_label: None,
        created_at_raw: Some("2024-01-02T03:04:05Z".to_owned()),
        created_at_normalized: Some("2024-01-02T03:04:05Z".to_owned()),
        created_at_parse_status: CreatedAtParseStatus::Valid,
        text: "hello ✅".to_owned(),
        langs: vec!["en".to_owned()],
        emoji_sequence: vec!["✅".to_owned()],
        extras_json: serde_json::json!({ "fixture": "original" }),
    }
}

fn profile_record() -> ProfileRecord {
    ProfileRecord {
        rkey: "self".to_owned(),
        cid: "bafy-profile".to_owned(),
        record: Profile {
            avatar: None,
            banner: None,
            created_at: None,
            description: Some(SmolStr::new("profile fixture")),
            display_name: Some(SmolStr::new("alice")),
            joined_via_starter_pack: None,
            labels: None,
            pinned_post: None,
            pronouns: None,
            website: None,
            extra_data: None,
        },
    }
}

fn read_json<T>(path: &Path) -> T
where
    T: DeserializeOwned,
{
    let bytes = fs::read(path).expect("JSON file should be readable");
    serde_json::from_slice(&bytes).expect("JSON file should decode")
}

fn sha256_file(path: &Path) -> String {
    let bytes = fs::read(path).expect("hash input should be readable");
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn json_field<'a>(value: &'a serde_json::Value, key: &str) -> &'a str {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .expect("JSON string field should exist")
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "emojistats-profile-sidecar-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("test temp directory should be created");
        Self { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ignored = fs::remove_dir_all(&self.path);
    }
}
