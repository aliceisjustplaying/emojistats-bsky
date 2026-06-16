#![allow(clippy::expect_used)]

mod fixtures;

use bytes::Bytes;
use emojistats_backfill::{
    archive::{
        ArchivePostRow, CompletenessClass, CreatedAtParseStatus, FetchMethod, RepoReceiptInput,
        archive_rows_from_parsed_repo, build_repo_receipt, classify_created_at, current_normalizer,
        hash_post_rows,
    },
    parse::{
        CompletenessClass as ParseCompletenessClass, ParseConfig, ParseError, parse_repo,
        parse_repo_for_did, parse_repo_for_did_with_config,
    },
};
use jacquard_repo::mst::{NodeData, TreeEntry};
use serde_json::json;

use crate::fixtures::{
    TempCar, commit_only_car_bytes, empty_mst_block, empty_roots_car_bytes,
    malformed_header_car_bytes, non_commit_root_car_bytes, record_block,
    repo_car_with_root_node_bytes, root_without_block_car_bytes, single_post_car_bytes,
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
fn rejects_malformed_repo_keys_from_car_mst() {
    let did = "did:plc:fixture123";
    let car = TempCar::from_bytes(
        "empty-rkey.car",
        &single_post_car_bytes(did, "", &valid_post_json()),
    );

    let error = parse_repo_for_did(&car.path, did).expect_err("empty rkey should fail");

    assert!(matches!(
        error,
        ParseError::MalformedCar(message)
            if message == "invalid repo key \"app.bsky.feed.post/\": rkey is empty"
    ));
}

#[test]
fn reports_snapshot_complete_proof_for_verified_mst_root_block() {
    let did = "did:plc:fixture123";
    let car = TempCar::from_bytes(
        "snapshot-complete-proof.car",
        &single_post_car_bytes(did, "3kabc", &valid_post_json()),
    );

    let parsed = parse_repo_for_did(&car.path, did).expect("repo should parse");

    assert_eq!(
        parsed.completeness.class,
        ParseCompletenessClass::ContentAddressedSnapshot
    );
    assert_eq!(
        parsed.completeness.car_roots.as_slice(),
        std::slice::from_ref(&parsed.commit.cid)
    );
    assert_eq!(parsed.completeness.verified_block_count, 3);
    assert_eq!(parsed.completeness.duplicate_block_cid_count, 0);
    assert_eq!(parsed.completeness.reachable_record_count, 1);
    assert!(parsed.completeness.mst_root_cid_verified);
    assert!(!parsed.completeness.repo_commit_signature_verified);
    assert!(!parsed.completeness.identity_verified);
    assert!(!parsed.commit.data.is_empty());
}

#[test]
fn reports_duplicate_car_block_cids_in_snapshot_proof() {
    let did = "did:plc:fixture123";
    let (record_cid, record_bytes) = record_block(&valid_post_json());
    let node = NodeData {
        left: None,
        entries: vec![TreeEntry {
            key_suffix: Bytes::from_static(b"app.bsky.feed.post/3kabc"),
            prefix_len: 0,
            tree: None,
            value: record_cid,
        }],
    };
    let car = TempCar::from_bytes(
        "duplicate-block-cid.car",
        &repo_car_with_root_node_bytes(
            did,
            &node,
            &[
                (record_cid, record_bytes.clone()),
                (record_cid, record_bytes),
            ],
        ),
    );

    let parsed = parse_repo_for_did(&car.path, did).expect("repo with duplicate CID should parse");

    assert_eq!(parsed.completeness.verified_block_count, 4);
    assert_eq!(parsed.completeness.duplicate_block_cid_count, 1);
    assert_eq!(parsed.completeness.reachable_record_count, 1);
}

#[test]
fn parallel_cid_verifier_rejects_mismatched_block_bytes() {
    let did = "did:plc:fixture123";
    let (record_cid, mut record_bytes) = record_block(&valid_post_json());
    record_bytes.push(0);
    let node = NodeData {
        left: None,
        entries: vec![TreeEntry {
            key_suffix: Bytes::from_static(b"app.bsky.feed.post/3kabc"),
            prefix_len: 0,
            tree: None,
            value: record_cid,
        }],
    };
    let car = TempCar::from_bytes(
        "parallel-cid-mismatch.car",
        &repo_car_with_root_node_bytes(did, &node, &[(record_cid, record_bytes)]),
    );
    let config = ParseConfig {
        cid_verification_threads: 2,
        ..ParseConfig::default()
    };

    let error = parse_repo_for_did_with_config(&car.path, did, config)
        .expect_err("corrupt block bytes should fail CID verification");

    assert!(matches!(error, ParseError::CidMismatch { .. }));
}

#[test]
fn rejects_duplicate_mst_keys() {
    let did = "did:plc:fixture123";
    let key = "app.bsky.feed.post/3kabc";
    let (record_cid, record_bytes) = record_block(&valid_post_json());
    let node = NodeData {
        left: None,
        entries: vec![
            TreeEntry {
                key_suffix: Bytes::from(key.as_bytes().to_vec()),
                prefix_len: 0,
                tree: None,
                value: record_cid,
            },
            TreeEntry {
                key_suffix: Bytes::new(),
                prefix_len: u8::try_from(key.len()).expect("key length fits u8"),
                tree: None,
                value: record_cid,
            },
        ],
    };
    let car = TempCar::from_bytes(
        "duplicate-key.car",
        &repo_car_with_root_node_bytes(did, &node, &[(record_cid, record_bytes)]),
    );

    let error = parse_repo_for_did(&car.path, did).expect_err("duplicate key should fail");

    assert!(matches!(
        error,
        ParseError::MalformedCar(message) if message == format!("duplicate MST key: {key}")
    ));
}

#[test]
fn rejects_out_of_order_mst_keys() {
    let did = "did:plc:fixture123";
    let (record_cid, record_bytes) = record_block(&valid_post_json());
    let node = NodeData {
        left: None,
        entries: vec![
            TreeEntry {
                key_suffix: Bytes::from_static(b"app.bsky.feed.post/3kdef"),
                prefix_len: 0,
                tree: None,
                value: record_cid,
            },
            TreeEntry {
                key_suffix: Bytes::from_static(b"3kabc"),
                prefix_len: 19,
                tree: None,
                value: record_cid,
            },
        ],
    };
    let car = TempCar::from_bytes(
        "out-of-order-key.car",
        &repo_car_with_root_node_bytes(did, &node, &[(record_cid, record_bytes)]),
    );

    let error = parse_repo_for_did(&car.path, did).expect_err("out-of-order key should fail");

    assert!(matches!(
        error,
        ParseError::MalformedCar(message)
            if message
                == "MST keys out of order: previous=app.bsky.feed.post/3kdef, key=app.bsky.feed.post/3kabc"
    ));
}

#[test]
fn rejects_revisited_mst_node_cids() {
    let did = "did:plc:fixture123";
    let (record_cid, record_bytes) = record_block(&valid_post_json());
    let (child_cid, child_bytes) = empty_mst_block();
    let node = NodeData {
        left: Some(child_cid),
        entries: vec![TreeEntry {
            key_suffix: Bytes::from_static(b"app.bsky.feed.post/3kabc"),
            prefix_len: 0,
            tree: Some(child_cid),
            value: record_cid,
        }],
    };
    let car = TempCar::from_bytes(
        "revisited-node.car",
        &repo_car_with_root_node_bytes(
            did,
            &node,
            &[(child_cid, child_bytes), (record_cid, record_bytes)],
        ),
    );

    let error = parse_repo_for_did(&car.path, did).expect_err("revisited node CID should fail");

    assert!(matches!(
        error,
        ParseError::MalformedCar(message)
            if message.starts_with("MST node CID visited more than once:")
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
fn archives_raw_post_row_when_typed_decode_fails_for_missing_created_at() {
    let did = "did:plc:fixture123";
    let car = TempCar::from_bytes(
        "missing-created-at-post.car",
        &single_post_car_bytes(
            did,
            "3jui7kd54zh2y",
            &json!({
                "$type": "app.bsky.feed.post",
                "text": "hello ✅",
                "langs": ["en", "ja"],
                "custom": { "preserved": true }
            }),
        ),
    );

    let parsed = parse_repo_for_did(&car.path, did).expect("repo should parse with partial row");
    let rows = archive_rows_from_parsed_repo(&parsed).expect("archive rows should build");

    assert_eq!(parsed.rkey_digest.all_records_count, 1);
    assert_eq!(parsed.rkey_digest.post_records_count, 1);
    assert_eq!(parsed.post_decode_error_count, 1);
    assert_eq!(rows.len(), 1);
    let row = rows.first().expect("partial archive row");
    assert_eq!(row.did, did);
    assert_eq!(row.rkey, "3jui7kd54zh2y");
    assert!(!row.cid.is_empty());
    assert_eq!(row.record_status.as_deref(), Some("typed_decode_failed"));
    assert_eq!(row.created_at_parse_status, CreatedAtParseStatus::Missing);
    assert_eq!(row.created_at_raw, None);
    assert_eq!(row.created_at_normalized, None);
    assert_eq!(row.text, "hello ✅");
    assert_eq!(row.langs, ["en", "ja"]);
    assert_eq!(row.emoji_sequence, ["✅"]);
    assert_eq!(row.extras_json, json!({ "custom": { "preserved": true } }));
}

#[test]
fn archives_raw_post_row_when_typed_decode_fails_for_invalid_created_at() {
    let did = "did:plc:fixture123";
    let car = TempCar::from_bytes(
        "invalid-created-at-post.car",
        &single_post_car_bytes(
            did,
            "3jui7kd54zh2z",
            &json!({
                "$type": "app.bsky.feed.post",
                "createdAt": { "not": "a string" },
                "text": "raw text",
                "langs": ["en"]
            }),
        ),
    );

    let parsed = parse_repo_for_did(&car.path, did).expect("repo should parse with partial row");
    let rows = archive_rows_from_parsed_repo(&parsed).expect("archive rows should build");

    assert_eq!(parsed.rkey_digest.all_records_count, 1);
    assert_eq!(parsed.rkey_digest.post_records_count, 1);
    assert_eq!(parsed.post_decode_error_count, 1);
    assert_eq!(rows.len(), 1);
    let row = rows.first().expect("partial archive row");
    assert_eq!(row.record_status.as_deref(), Some("typed_decode_failed"));
    assert_eq!(row.created_at_parse_status, CreatedAtParseStatus::Invalid);
    assert_eq!(row.created_at_raw.as_deref(), Some(r#"{"not":"a string"}"#));
    assert_eq!(row.created_at_normalized, None);
    assert_eq!(row.text, "raw text");
    assert_eq!(row.langs, ["en"]);
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
        CompletenessClass::ContentAddressedSnapshot
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

fn valid_post_json() -> serde_json::Value {
    json!({
        "$type": "app.bsky.feed.post",
        "createdAt": "2024-01-02T03:04:05Z",
        "text": "hello"
    })
}
