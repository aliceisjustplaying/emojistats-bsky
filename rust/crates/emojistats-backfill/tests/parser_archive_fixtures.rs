#![allow(clippy::expect_used)]

mod fixtures;

use emojistats_backfill::{
    archive::{
        ArchivePostRow, CompletenessClass, CreatedAtParseStatus, FetchMethod, RepoReceiptInput,
        build_repo_receipt, classify_created_at, current_normalizer, hash_post_rows,
    },
    parse::{ParseError, parse_repo, parse_repo_for_did},
};
use serde_json::json;

use crate::fixtures::{
    TempCar, commit_only_car_bytes, empty_roots_car_bytes, malformed_header_car_bytes,
    non_commit_root_car_bytes, root_without_block_car_bytes,
};

#[test]
fn rejects_malformed_car_header() {
    let car = TempCar::from_bytes("malformed-header.car", &malformed_header_car_bytes());
    let error = parse_repo(&car.path).expect_err("malformed CAR header should fail");

    assert!(matches!(
        error,
        ParseError::MalformedCar(message)
            if message.contains("failed to decode CAR header")
    ));
}

#[test]
fn rejects_empty_roots_and_missing_or_invalid_root_blocks() {
    let empty_roots = TempCar::from_bytes("empty-roots.car", &empty_roots_car_bytes());
    let missing_root_block =
        TempCar::from_bytes("missing-root-block.car", &root_without_block_car_bytes());
    let invalid_root = TempCar::from_bytes("invalid-root.car", &non_commit_root_car_bytes());

    assert!(matches!(
        parse_repo(&empty_roots.path),
        Err(ParseError::InvalidRoots(message)) if message == "CAR header has no roots"
    ));
    assert!(matches!(
        parse_repo(&missing_root_block.path),
        Err(ParseError::CommitNotFound { .. })
    ));
    assert!(matches!(
        parse_repo(&invalid_root.path),
        Err(ParseError::RootCommitDecode { .. })
    ));
}

#[test]
fn rejects_requested_did_mismatch_before_mst_walk() {
    let car = TempCar::from_bytes(
        "commit-did-mismatch.car",
        &commit_only_car_bytes("did:plc:actual123"),
    );

    let error = parse_repo_for_did(&car.path, "did:plc:requested456")
        .expect_err("requested DID mismatch should fail");

    assert!(matches!(
        error,
        ParseError::CommitDidMismatch { requested, actual }
            if requested == "did:plc:requested456" && actual == "did:plc:actual123"
    ));
}

#[test]
fn classifies_created_at_values_for_archive_rows() {
    let missing = classify_created_at(None);
    let invalid = classify_created_at(Some("not-a-date"));
    let future = classify_created_at(Some("9999-01-01T00:00:00Z"));
    let valid = classify_created_at(Some("2024-01-02T03:04:05Z"));

    assert_eq!(missing.status, CreatedAtParseStatus::Missing);
    assert_eq!(missing.raw, None);
    assert_eq!(missing.normalized, None);
    assert_eq!(invalid.status, CreatedAtParseStatus::Invalid);
    assert_eq!(invalid.raw.as_deref(), Some("not-a-date"));
    assert_eq!(invalid.normalized, None);
    assert_eq!(future.status, CreatedAtParseStatus::Future);
    assert_eq!(future.raw.as_deref(), Some("9999-01-01T00:00:00Z"));
    assert_eq!(future.normalized, None);
    assert_eq!(valid.status, CreatedAtParseStatus::Valid);
    assert_eq!(valid.normalized.as_deref(), Some("2024-01-02T03:04:05Z"));
}

#[test]
fn archive_row_content_change_changes_receipt_hashes() {
    let row = archive_row("hello");
    let mut changed_row = row.clone();
    changed_row.text = "hello edited".to_owned();
    changed_row.extras_json = json!({ "fixture": "changed" });
    let rows = vec![row];
    let changed_rows = vec![changed_row];

    let receipt = receipt_for(&rows);
    let changed_receipt = receipt_for(&changed_rows);

    assert_eq!(
        hash_post_rows(&rows).expect("row hash should build"),
        receipt.post_rows_hash
    );
    assert_ne!(receipt.archive_rows_hash, changed_receipt.archive_rows_hash);
    assert_ne!(receipt.post_rows_hash, changed_receipt.post_rows_hash);
    assert_eq!(receipt.fetch_method, FetchMethod::GetRepo);
    assert_eq!(
        receipt.completeness_class,
        CompletenessClass::SnapshotComplete
    );
}

fn receipt_for(rows: &[ArchivePostRow]) -> emojistats_backfill::archive::RepoReceipt {
    build_repo_receipt(RepoReceiptInput {
        rows,
        reachable_records_count: u64::try_from(rows.len()).expect("row count should fit u64"),
        reachable_post_records_count: u64::try_from(rows.len()).expect("row count should fit u64"),
        post_decode_error_count: 0,
        profile_row_hash: None,
        mst_root_cid: Some(
            "bafyreihyrpejdc3l3wqlbm7vuzx7hhvx6r5eg44vqyqjna6u6kwtpoyqte".to_owned(),
        ),
        commit_cid: Some("bafyreibqj2lhp4fpizc2zstcsl2mzo6fycjfnwc6kyz4xpr2lzyqlw6wxi".to_owned()),
        normalizer: current_normalizer(),
    })
    .expect("receipt should build")
}

fn archive_row(text: &str) -> ArchivePostRow {
    ArchivePostRow {
        did: "did:plc:fixture123".to_owned(),
        rkey: "3jui7kd54zh2y".to_owned(),
        cid: "bafyreiay3v7pbmhrkpoc7j4x2vxsuv5n2rrr7q3kde4q2j67r2rsfjiyme".to_owned(),
        normalizer: current_normalizer(),
        account_status: None,
        record_status: None,
        public_content_label: None,
        created_at_raw: Some("2024-01-02T03:04:05Z".to_owned()),
        created_at_normalized: Some("2024-01-02T03:04:05Z".to_owned()),
        created_at_parse_status: CreatedAtParseStatus::Valid,
        text: text.to_owned(),
        langs: vec!["en".to_owned()],
        emoji_sequence: Vec::new(),
        extras_json: json!({ "fixture": "original" }),
    }
}
