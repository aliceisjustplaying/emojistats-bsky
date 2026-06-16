use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use super::{
    ArchiveCommitContext, ArchivePostRow, CompletenessClass, CreatedAtParseStatus, FetchMethod,
    NormalizerVersion, RepoReceiptInput, StreamingArchiveSink, StreamingReceiptInput,
    build_repo_receipt, classify_created_at, extract_emojis, hash_post_rows,
};

fn normalizer() -> NormalizerVersion {
    NormalizerVersion {
        name: "emoji-normalizer".to_owned(),
        semver: "0.1.0".to_owned(),
        git_rev: "test".to_owned(),
        unicode_version: "16.0".to_owned(),
        emoji_data_version: "16.0".to_owned(),
    }
}

fn row(text: &str, emojis: &[&str]) -> ArchivePostRow {
    ArchivePostRow {
        did: "did:plc:test".to_owned(),
        rkey: "abc".to_owned(),
        cid: "bafy-test".to_owned(),
        normalizer: normalizer(),
        account_status: None,
        record_status: None,
        public_content_label: None,
        created_at_raw: Some("2026-06-15T00:00:00Z".to_owned()),
        created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
        created_at_parse_status: CreatedAtParseStatus::Valid,
        text: text.to_owned(),
        langs: vec!["en".to_owned()],
        emoji_sequence: emojis.iter().map(|emoji| (*emoji).to_owned()).collect(),
        extras_json: serde_json::json!({"facets": []}),
    }
}

#[test]
fn row_hash_changes_when_content_changes() {
    let first = hash_post_rows(&[row("hello", &["✅"])]).expect("first row hash");
    let second = hash_post_rows(&[row("hello!", &["✅"])]).expect("second row hash");
    assert_ne!(first, second);
}

#[test]
fn receipt_counts_posts_and_emoji_occurrences() {
    let rows = [row("a", &["✅", "✅"]), row("b", &[])];
    let receipt = build_repo_receipt(RepoReceiptInput {
        rows: &rows,
        reachable_records_count: 3,
        reachable_post_records_count: 2,
        post_decode_error_count: 1,
        profile_row_hash: Some("profile-hash".to_owned()),
        mst_root_cid: Some("root".to_owned()),
        commit_cid: Some("commit".to_owned()),
        normalizer: normalizer(),
    });
    let receipt = receipt.expect("receipt should build");
    assert_eq!(receipt.reachable_records_count, 3);
    assert_eq!(receipt.reachable_post_records_count, 2);
    assert_eq!(receipt.archived_post_rows_count, 2);
    assert_eq!(receipt.post_decode_error_count, 1);
    assert_eq!(receipt.emoji_posts_count, 1);
    assert_eq!(receipt.emoji_occurrences_count, 2);
    assert_eq!(receipt.profile_row_hash, Some("profile-hash".to_owned()));
}

#[test]
fn extracts_grapheme_emoji_sequences() {
    assert_eq!(extract_emojis("hi ✅ 👩‍💻"), vec!["✅", "👩‍💻"]);
}

#[test]
fn classifies_created_at_statuses() {
    let missing = classify_created_at(None);
    assert_eq!(missing.status, CreatedAtParseStatus::Missing);
    assert_eq!(missing.normalized, None);

    let invalid = classify_created_at(Some("not-a-date"));
    assert_eq!(invalid.status, CreatedAtParseStatus::Invalid);
    assert_eq!(invalid.raw, Some("not-a-date".to_owned()));
    assert_eq!(invalid.normalized, None);

    let future = classify_created_at(Some("9999-12-31T23:59:59Z"));
    assert_eq!(future.status, CreatedAtParseStatus::Future);
    assert_eq!(future.normalized, None);

    let valid = classify_created_at(Some("2020-01-02T03:04:05Z"));
    assert_eq!(valid.status, CreatedAtParseStatus::Valid);
    assert_eq!(valid.normalized, Some("2020-01-02T03:04:05Z".to_owned()));
}

#[test]
fn unfinished_streaming_sink_removes_temp_files_on_drop() {
    let output_dir = unique_test_dir("streaming-sink-drop");
    fs::create_dir_all(&output_dir).expect("create test archive dir");

    let sink = StreamingArchiveSink::new(
        &output_dir,
        "did:plc:cleanup",
        ArchiveCommitContext::fetch_one_local(),
    )
    .expect("create sink");
    let parquet_temp = sink.parquet_temp_path.clone();
    let emoji_temp = sink.emoji_projection_temp_path.clone();
    assert!(parquet_temp.exists(), "{}", parquet_temp.display());
    assert!(emoji_temp.exists(), "{}", emoji_temp.display());
    drop(sink);

    assert!(!parquet_temp.exists());
    assert!(!emoji_temp.exists());
    fs::remove_dir_all(output_dir).expect("remove test archive dir");
}

#[test]
fn streaming_sink_writes_committed_manifest_entry() {
    let output_dir = unique_test_dir("streaming-sink-manifest");
    fs::create_dir_all(&output_dir).expect("create test archive dir");
    let mut sink = StreamingArchiveSink::new(
        &output_dir,
        "did:plc:manifest",
        ArchiveCommitContext::new("run-test", "shard7", 42),
    )
    .expect("create sink");
    sink.push_row(row("hello ✅", &["✅"])).expect("push row");
    let (_receipt, artifacts) = sink
        .finish(
            StreamingReceiptInput {
                fetch_method: FetchMethod::GetRepo,
                completeness_class: CompletenessClass::SnapshotComplete,
                reachable_records_count: 1,
                reachable_post_records_count: 1,
                post_decode_error_count: 0,
                profile_row_hash: None,
                mst_root_cid: Some("root".to_owned()),
                commit_cid: Some("commit".to_owned()),
            },
            None,
        )
        .expect("finish sink");

    let manifest_json = fs::read_to_string(&artifacts.manifest_path).expect("read manifest");
    let entry: crate::commit::ManifestEntry =
        serde_json::from_str(&manifest_json).expect("parse committed manifest");
    let object_receipt: crate::commit::Receipt =
        serde_json::from_slice(&fs::read(&artifacts.object_receipt_path).expect("read receipt"))
            .expect("parse committed receipt");

    assert!(entry.object_path.starts_with("did_plc_manifest__"));
    assert!(entry.object_path.ends_with(".posts.parquet"));
    assert_eq!(entry.run_id, "run-test");
    assert_eq!(entry.shard, "shard7");
    assert_eq!(entry.file_sequence, 42);
    assert_eq!(entry.dataset, "raw_archive_posts");
    assert_eq!(entry.row_count, 1);
    assert_eq!(object_receipt.object_path, entry.object_path);
    assert_eq!(object_receipt.content_hash, entry.content_hash);
    assert_eq!(object_receipt.receipt_hash, entry.receipt_hash);
    fs::remove_dir_all(output_dir).expect("remove test archive dir");
}

fn unique_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "emojistats-backfill-{name}-{}-{nanos}",
        std::process::id()
    ))
}
