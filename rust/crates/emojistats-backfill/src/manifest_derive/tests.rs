#![allow(clippy::indexing_slicing)]

use std::{
    fs,
    io::{BufReader, Cursor},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use super::{
    Error, FullLoadCaps, load_verified_clickhouse_batch, load_verified_clickhouse_batch_with_caps,
    read_committed_jsonl, verify_loader_input_for_streaming,
};
use crate::{
    archive::{
        ArchiveCommitContext, ArchivePostRow, CreatedAtParseStatus, NormalizerVersion, RepoReceipt,
        RepoReceiptInput, build_repo_receipt, current_normalizer, write_archive_artifacts,
    },
    commit::ManifestEntry,
};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

fn normalizer() -> NormalizerVersion {
    NormalizerVersion {
        name: "emoji-normalizer".to_owned(),
        semver: "0.1.0".to_owned(),
        git_rev: "test".to_owned(),
        unicode_version: "16.0".to_owned(),
        emoji_data_version: "16.0".to_owned(),
    }
}

fn entry(dataset: &str) -> ManifestEntry {
    ManifestEntry {
        run_id: "run-1".to_owned(),
        shard: "shard3".to_owned(),
        file_sequence: 42,
        dataset: dataset.to_owned(),
        object_path: format!("objects/{dataset}/part-000042.parquet"),
        row_count: 123,
        bytes: 456,
        content_hash: "content-hash".to_owned(),
        min_created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
        max_created_at_normalized: Some("2026-06-15T01:00:00Z".to_owned()),
        receipt_hash: "receipt-hash".to_owned(),
        normalizer: normalizer(),
        schema_version: 1,
    }
}

fn jsonl(entries: &[ManifestEntry]) -> String {
    let mut lines = String::new();
    for entry in entries {
        lines.push_str(&serde_json::to_string(entry).expect("serialize manifest entry"));
        lines.push('\n');
    }
    lines
}

fn archive_row(rkey: &str, text: &str, emojis: &[&str]) -> ArchivePostRow {
    ArchivePostRow {
        did: "did:plc:fixture123".to_owned(),
        rkey: rkey.to_owned(),
        cid: format!("bafy-{rkey}"),
        normalizer: current_normalizer(),
        account_status: None,
        record_status: None,
        public_content_label: None,
        created_at_raw: Some("2026-06-15T00:00:00Z".to_owned()),
        created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
        created_at_parse_status: CreatedAtParseStatus::Valid,
        text: text.to_owned(),
        langs: vec!["en".to_owned()],
        emoji_sequence: emojis.iter().map(|emoji| (*emoji).to_owned()).collect(),
        extras_json: serde_json::json!({}),
    }
}

fn repo_receipt(rows: &[ArchivePostRow]) -> RepoReceipt {
    build_repo_receipt(RepoReceiptInput {
        rows,
        reachable_records_count: u64::try_from(rows.len()).expect("row count should fit u64"),
        reachable_post_records_count: u64::try_from(rows.len()).expect("row count should fit u64"),
        post_decode_error_count: 0,
        profile_row_hash: None,
        mst_root_cid: Some("bafy-mst".to_owned()),
        commit_cid: Some("bafy-commit".to_owned()),
        normalizer: current_normalizer(),
    })
    .expect("receipt should build")
}

fn read_plan_from_path(path: &Path) -> super::Plan {
    let file = fs::File::open(path).expect("manifest should be readable");
    read_committed_jsonl(BufReader::new(file)).expect("manifest should parse")
}

#[test]
fn parses_jsonl_and_builds_loader_inputs_for_raw_archive_posts() {
    let raw_entry = entry("raw_archive_posts");
    let profile_entry = entry("raw_profile_sidecar");
    let plan = read_committed_jsonl(Cursor::new(jsonl(&[profile_entry, raw_entry.clone()])))
        .expect("read manifest jsonl");

    assert_eq!(plan.inputs.len(), 1);
    assert_eq!(plan.skipped_entries.len(), 1);
    let input = plan.inputs.first().expect("one loader input");
    assert_eq!(input.manifest.run_id, raw_entry.run_id);
    assert_eq!(
        input.manifest.local_path,
        std::path::PathBuf::from(raw_entry.object_path)
    );
    assert_eq!(plan.skipped_entries[0].dataset, "raw_profile_sidecar");
}

#[test]
fn skips_non_raw_dataset_and_rejects_bad_dataset_field() {
    let skipped = read_committed_jsonl(Cursor::new(jsonl(&[entry("raw_profile_sidecar")])))
        .expect("read skipped manifest jsonl");
    assert!(skipped.inputs.is_empty());
    assert_eq!(skipped.skipped_entries.len(), 1);

    let mut bad = entry("");
    bad.object_path = "objects/empty-dataset.parquet".to_owned();
    let error =
        read_committed_jsonl(Cursor::new(jsonl(&[bad]))).expect_err("empty dataset rejected");
    assert!(matches!(
        error,
        Error::EmptyField {
            line_number: 1,
            field: "dataset"
        }
    ));
}

#[test]
fn rejects_raw_archive_schema_mismatch() {
    let mut bad = entry("raw_archive_posts");
    bad.schema_version = 2;

    let error = read_committed_jsonl(Cursor::new(jsonl(&[bad]))).expect_err("bad schema rejected");

    assert!(matches!(
        error,
        Error::UnsupportedSchemaVersion {
            line_number: 1,
            actual: 2,
            expected: 1
        }
    ));
}

#[test]
fn stable_identity_fields_come_from_committed_manifest() {
    let mut raw_entry = entry("raw_archive_posts");
    raw_entry.object_path = "objects/raw_archive_posts/a.parquet".to_owned();
    raw_entry.bytes = 111;
    raw_entry.min_created_at_normalized = Some("2026-06-15T00:00:00Z".to_owned());
    let first = read_committed_jsonl(Cursor::new(jsonl(&[raw_entry.clone()])))
        .expect("read first manifest jsonl");

    raw_entry.object_path = "objects/raw_archive_posts/b.parquet".to_owned();
    raw_entry.bytes = 222;
    raw_entry.min_created_at_normalized = Some("2026-06-14T00:00:00Z".to_owned());
    let second =
        read_committed_jsonl(Cursor::new(jsonl(&[raw_entry]))).expect("read second manifest jsonl");

    let first_identity = &first.inputs.first().expect("first input").identity;
    let second_identity = &second.inputs.first().expect("second input").identity;
    assert_eq!(first_identity, second_identity);
    assert_eq!(first_identity.run_id, "run-1");
    assert_eq!(first_identity.shard, "shard3");
    assert_eq!(first_identity.file_sequence, 42);
    assert_eq!(first_identity.dataset, "raw_archive_posts");
    assert_eq!(first_identity.content_hash, "content-hash");
    assert_eq!(first_identity.receipt_hash, "receipt-hash");
    assert_eq!(first_identity.schema_version, 1);
}

#[test]
fn verified_manifest_entry_loads_clickhouse_batch() {
    let temp = TempDir::new("valid");
    let output_dir = temp.path.join("archive");
    let rows = vec![
        archive_row("a", "hello ✅", &["✅"]),
        archive_row("b", "fire 🔥🔥", &["🔥", "🔥"]),
    ];
    let receipt = repo_receipt(&rows);
    let artifacts = write_archive_artifacts(
        &output_dir,
        "did:plc:fixture123",
        &ArchiveCommitContext::fetch_one_local(),
        &rows,
        None,
        &receipt,
    )
    .expect("archive artifacts should write");
    let plan = read_plan_from_path(&artifacts.manifest_path);
    let input = plan.inputs.first().expect("loader input");

    let batch =
        load_verified_clickhouse_batch(&output_dir, input).expect("verified batch should load");

    assert_eq!(batch.manifest_identity, input.identity);
    assert_eq!(batch.emoji_rows.len(), 2);
    assert_eq!(batch.total_post_counter.posts_processed, 2);
    assert_eq!(batch.total_post_counter.emoji_occurrences, 3);
}

#[test]
fn streaming_verifier_finds_content_stem_repo_receipt() {
    let temp = TempDir::new("streaming-receipt");
    let output_dir = temp.path.join("archive");
    let rows = vec![archive_row("a", "hello ✅", &["✅"])];
    let receipt = repo_receipt(&rows);
    let artifacts = write_archive_artifacts(
        &output_dir,
        "did:plc:fixture123",
        &ArchiveCommitContext::fetch_one_local(),
        &rows,
        None,
        &receipt,
    )
    .expect("archive artifacts should write");
    let plan = read_plan_from_path(&artifacts.manifest_path);
    let input = plan.inputs.first().expect("loader input");

    let verified = verify_loader_input_for_streaming(&output_dir, input)
        .expect("streaming verifier should find repo receipt");

    assert_eq!(verified.repo_receipt.archived_post_rows_count, 1);
}

#[test]
fn verified_manifest_entry_rejects_missing_parquet() {
    let temp = TempDir::new("missing");
    let output_dir = temp.path.join("archive");
    let rows = vec![archive_row("a", "hello ✅", &["✅"])];
    let receipt = repo_receipt(&rows);
    let artifacts = write_archive_artifacts(
        &output_dir,
        "did:plc:fixture123",
        &ArchiveCommitContext::fetch_one_local(),
        &rows,
        None,
        &receipt,
    )
    .expect("archive artifacts should write");
    let plan = read_plan_from_path(&artifacts.manifest_path);
    fs::remove_file(&artifacts.parquet_path).expect("parquet should be removable");

    let error =
        load_verified_clickhouse_batch(&output_dir, plan.inputs.first().expect("loader input"))
            .expect_err("missing parquet should fail");

    assert!(matches!(error, Error::MissingArtifact { .. }));
}

#[test]
fn verified_manifest_entry_rejects_missing_repo_receipt() {
    let temp = TempDir::new("missing-receipt");
    let output_dir = temp.path.join("archive");
    let rows = vec![archive_row("a", "hello ✅", &["✅"])];
    let receipt = repo_receipt(&rows);
    let artifacts = write_archive_artifacts(
        &output_dir,
        "did:plc:fixture123",
        &ArchiveCommitContext::fetch_one_local(),
        &rows,
        None,
        &receipt,
    )
    .expect("archive artifacts should write");
    let plan = read_plan_from_path(&artifacts.manifest_path);
    fs::remove_file(&artifacts.receipt_path).expect("repo receipt should be removable");

    let error =
        load_verified_clickhouse_batch(&output_dir, plan.inputs.first().expect("loader input"))
            .expect_err("missing repo receipt should fail");

    assert!(matches!(error, Error::MissingRepoReceipt { .. }));
}

#[test]
fn verified_manifest_entry_rejects_parquet_hash_mismatch() {
    let temp = TempDir::new("hash-mismatch");
    let output_dir = temp.path.join("archive");
    let rows = vec![archive_row("a", "hello ✅", &["✅"])];
    let receipt = repo_receipt(&rows);
    let artifacts = write_archive_artifacts(
        &output_dir,
        "did:plc:fixture123",
        &ArchiveCommitContext::fetch_one_local(),
        &rows,
        None,
        &receipt,
    )
    .expect("archive artifacts should write");
    let plan = read_plan_from_path(&artifacts.manifest_path);
    fs::write(&artifacts.parquet_path, b"corrupt").expect("parquet should be mutable");

    let error =
        load_verified_clickhouse_batch(&output_dir, plan.inputs.first().expect("loader input"))
            .expect_err("hash mismatch should fail");

    assert!(matches!(
        error,
        Error::ByteMismatch { .. } | Error::ContentHashMismatch { .. }
    ));
}

#[test]
fn full_batch_load_rejects_manifest_above_explicit_caps_before_reading_rows() {
    let temp = TempDir::new("row-cap");
    let output_dir = temp.path.join("archive");
    let rows = vec![archive_row("a", "hello ✅", &["✅"])];
    let receipt = repo_receipt(&rows);
    let artifacts = write_archive_artifacts(
        &output_dir,
        "did:plc:fixture123",
        &ArchiveCommitContext::fetch_one_local(),
        &rows,
        None,
        &receipt,
    )
    .expect("archive artifacts should write");
    let plan = read_plan_from_path(&artifacts.manifest_path);
    let input = plan.inputs.first().expect("loader input");

    let error = load_verified_clickhouse_batch_with_caps(
        &output_dir,
        input,
        FullLoadCaps {
            max_rows: 0,
            max_bytes: u64::MAX,
        },
    )
    .expect_err("row cap should fail");

    assert!(matches!(error, Error::FullLoadRowCapExceeded { .. }));
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(name: &str) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "emojistats-manifest-derive-{name}-{}-{sequence}",
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
