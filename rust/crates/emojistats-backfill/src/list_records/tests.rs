use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    thread,
    time::{Duration, Instant, SystemTime},
};

use jacquard_common::deps::fluent_uri::Uri;
use serde_json::json;

use super::*;
use crate::archive::{CompletenessClass, FetchMethod, read_all_archive_post_rows};

const TEST_CID_A: &str = "bafyreihyrpejdc3l3wqlbm7vuzx7hhvx6r5eg44vqyqjna6u6kwtpoyqte";
const TEST_CID_B: &str = "bafyreibqj2lhp4fpizc2zstcsl2mzo6fycjfnwc6kyz4xpr2lzyqlw6wxi";

#[test]
fn paginated_pages_archive_collection_paginated_receipt() {
    let archive_dir = temp_dir("list-records-pages");
    let did = "did:plc:testrepo";
    let pages = vec![
        ListRecordsPage {
            records: vec![post_record(did, "3kabc", TEST_CID_A, "hello")],
            cursor: Some("next".to_owned()),
        },
        ListRecordsPage {
            records: vec![post_record(did, "3kabd", TEST_CID_B, "second")],
            cursor: None,
        },
    ];

    let output = archive_list_records_pages(did, &archive_dir, pages, ListRecordsConfig::default())
        .expect("archive listRecords pages");

    assert_eq!(output.records, 2);
    assert_eq!(output.archived_posts, 2);
    assert_eq!(output.decode_errors, 0);
    assert_eq!(output.receipt.fetch_method, FetchMethod::ListRecords);
    assert_eq!(
        output.receipt.completeness_class,
        CompletenessClass::CollectionPaginated
    );
    assert_eq!(output.receipt.mst_root_cid, None);
    assert_eq!(output.receipt.commit_cid, None);
    let rows = read_all_archive_post_rows(&output.artifacts.parquet_path).expect("read parquet");
    assert_eq!(rows.len(), 2);
    let first = rows.first().expect("first row");
    let second = rows.get(1).expect("second row");
    assert_eq!(first.rkey, "3kabc");
    assert_eq!(second.cid, TEST_CID_B);

    fs::remove_dir_all(archive_dir).expect("remove archive dir");
}

#[test]
fn invalid_record_is_counted_as_decode_error() {
    let archive_dir = temp_dir("list-records-decode-error");
    let did = "did:plc:testrepo";
    let pages = vec![ListRecordsPage {
        records: vec![ListRecordsRecord {
            uri: format!("at://{did}/{POST_COLLECTION}/3kabc"),
            cid: Some(TEST_CID_A.to_owned()),
            value: json!({"$type": POST_COLLECTION, "text": "missing createdAt"}),
        }],
        cursor: None,
    }];

    let output = archive_list_records_pages(did, &archive_dir, pages, ListRecordsConfig::default())
        .expect("archive listRecords pages");

    assert_eq!(output.records, 1);
    assert_eq!(output.archived_posts, 1);
    assert_eq!(output.decode_errors, 1);
    assert_eq!(output.receipt.post_decode_error_count, 1);
    let rows = read_all_archive_post_rows(&output.artifacts.parquet_path).expect("read parquet");
    assert_eq!(rows.len(), 1);
    let row = rows.first().expect("partial row");
    assert_eq!(row.record_status.as_deref(), Some("typed_decode_failed"));
    assert_eq!(
        row.created_at_parse_status,
        crate::archive::CreatedAtParseStatus::Missing
    );

    fs::remove_dir_all(archive_dir).expect("remove archive dir");
}

#[test]
fn duplicate_rkey_is_a_protocol_error() {
    let archive_dir = temp_dir("list-records-duplicate-rkey");
    let did = "did:plc:testrepo";
    let pages = vec![ListRecordsPage {
        records: vec![
            post_record(did, "3kabc", TEST_CID_A, "hello"),
            post_record(did, "3kabc", TEST_CID_B, "edited"),
        ],
        cursor: None,
    }];

    let error = archive_list_records_pages(did, &archive_dir, pages, ListRecordsConfig::default())
        .expect_err("duplicate rkey should fail");

    assert!(
        matches!(error, ListRecordsError::Protocol(message) if message.contains("duplicate listRecords rkey 3kabc"))
    );
    fs::remove_dir_all(archive_dir).expect("remove archive dir");
}

#[test]
fn duplicate_rkey_is_rejected_before_decode() {
    let archive_dir = temp_dir("list-records-duplicate-before-decode");
    let did = "did:plc:testrepo";
    let pages = vec![ListRecordsPage {
        records: vec![
            ListRecordsRecord {
                uri: format!("at://{did}/{POST_COLLECTION}/3kabc"),
                cid: None,
                value: json!({"$type": POST_COLLECTION, "text": "missing fields"}),
            },
            post_record(did, "3kabc", TEST_CID_A, "valid later"),
        ],
        cursor: None,
    }];

    let error = archive_list_records_pages(did, &archive_dir, pages, ListRecordsConfig::default())
        .expect_err("duplicate rkey should fail before decode");

    assert!(
        matches!(error, ListRecordsError::Protocol(message) if message.contains("duplicate listRecords rkey 3kabc"))
    );
    fs::remove_dir_all(archive_dir).expect("remove archive dir");
}

#[test]
fn invalid_record_cid_is_counted_as_decode_error() {
    let archive_dir = temp_dir("list-records-invalid-cid");
    let did = "did:plc:testrepo";
    let pages = vec![ListRecordsPage {
        records: vec![ListRecordsRecord {
            uri: format!("at://{did}/{POST_COLLECTION}/3kabc"),
            cid: Some("not-a-cid".to_owned()),
            value: json!({
                "$type": POST_COLLECTION,
                "createdAt": "2026-06-16T00:00:00Z",
                "text": "hello"
            }),
        }],
        cursor: None,
    }];

    let output = archive_list_records_pages(did, &archive_dir, pages, ListRecordsConfig::default())
        .expect("archive listRecords pages");

    assert_eq!(output.records, 1);
    assert_eq!(output.archived_posts, 0);
    assert_eq!(output.decode_errors, 1);
    fs::remove_dir_all(archive_dir).expect("remove archive dir");
}

#[test]
fn missing_record_cid_is_counted_as_decode_error() {
    let archive_dir = temp_dir("list-records-missing-cid");
    let did = "did:plc:testrepo";
    let pages = vec![ListRecordsPage {
        records: vec![ListRecordsRecord {
            uri: format!("at://{did}/{POST_COLLECTION}/3kabc"),
            cid: None,
            value: json!({
                "$type": POST_COLLECTION,
                "createdAt": "2026-06-16T00:00:00Z",
                "text": "hello"
            }),
        }],
        cursor: None,
    }];

    let output = archive_list_records_pages(did, &archive_dir, pages, ListRecordsConfig::default())
        .expect("archive listRecords pages");

    assert_eq!(output.records, 1);
    assert_eq!(output.archived_posts, 0);
    assert_eq!(output.decode_errors, 1);
    fs::remove_dir_all(archive_dir).expect("remove archive dir");
}

#[test]
fn decode_error_cap_is_loud() {
    let archive_dir = temp_dir("list-records-decode-cap");
    let did = "did:plc:testrepo";
    let pages = vec![ListRecordsPage {
        records: vec![
            ListRecordsRecord {
                uri: format!("at://{did}/{POST_COLLECTION}/3kabc"),
                cid: None,
                value: json!({"$type": POST_COLLECTION, "text": "first"}),
            },
            ListRecordsRecord {
                uri: format!("at://{did}/{POST_COLLECTION}/3kabd"),
                cid: None,
                value: json!({"$type": POST_COLLECTION, "text": "second"}),
            },
        ],
        cursor: None,
    }];

    let error = archive_list_records_pages(
        did,
        &archive_dir,
        pages,
        ListRecordsConfig {
            max_decode_errors: 1,
            ..ListRecordsConfig::default()
        },
    )
    .expect_err("decode cap should fail");

    match error {
        ListRecordsError::ResourceLimitExceeded {
            limit,
            observed,
            max,
        } => {
            assert_eq!(limit, "max_decode_errors");
            assert_eq!(observed, 2);
            assert_eq!(max, 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    fs::remove_dir_all(archive_dir).expect("remove archive dir");
}

#[tokio::test]
async fn fetch_rejects_cursor_cycle_beyond_immediate_repeat() {
    let archive_dir = temp_dir("list-records-cursor-cycle");
    let did_str = "did:plc:testrepo";
    let (base_url, handle) = spawn_list_records_server(vec![
        TestResponse::json_page(None, Some("cursor-a"), Some(10), true),
        TestResponse::json_page(None, Some("cursor-b"), Some(9), true),
        TestResponse::json_page(None, Some("cursor-a"), Some(8), true),
    ]);
    let http = Client::new();
    let pds = Uri::parse(base_url).expect("parse pds").clone();
    let did = Did::new_owned(did_str).expect("parse did");

    let error = fetch_and_archive_list_records(
        &http,
        &pds,
        &did,
        did_str,
        &archive_dir,
        ArchiveCommitContext::fetch_one_local(),
        ListRecordsConfig::default(),
    )
    .await
    .expect_err("cursor cycle should fail");

    match error {
        ListRecordsError::Protocol(message) => {
            assert_eq!(message, "PDS returned a repeated listRecords cursor");
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(handle.join().expect("server thread").len(), 3);
    fs::remove_dir_all(archive_dir).expect("remove archive dir");
}

#[tokio::test]
async fn fetch_enforces_page_byte_cap_without_content_length() {
    let did_str = "did:plc:testrepo";
    let body = json!({"records": [], "cursor": null}).to_string();
    let max_page_bytes = u64::try_from(body.len() - 1).expect("body length fits");
    let (base_url, handle) = spawn_list_records_server(vec![TestResponse::raw(body, None, false)]);
    let http = Client::new();
    let pds = Uri::parse(base_url).expect("parse pds").clone();
    let did = Did::new_owned(did_str).expect("parse did");

    let error = fetch_list_records_page(
        &http,
        &pds,
        &did,
        None,
        ListRecordsConfig {
            max_page_bytes,
            ..ListRecordsConfig::default()
        },
    )
    .await
    .expect_err("oversize page should fail");

    match error {
        ListRecordsError::ResourceLimitExceeded {
            limit,
            observed,
            max,
        } => {
            assert_eq!(limit, "max_page_bytes");
            assert!(observed > max);
            assert_eq!(max, max_page_bytes);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(handle.join().expect("server thread").len(), 1);
}

#[tokio::test]
async fn fetch_enforces_chunk_idle_timeout() {
    let did_str = "did:plc:testrepo";
    let body = json!({"records": [], "cursor": null}).to_string();
    let (base_url, handle) = spawn_list_records_server(vec![TestResponse::raw_delayed(
        body,
        None,
        true,
        Duration::from_millis(100),
    )]);
    let http = Client::new();
    let pds = Uri::parse(base_url).expect("parse pds").clone();
    let did = Did::new_owned(did_str).expect("parse did");

    let error = fetch_list_records_page(
        &http,
        &pds,
        &did,
        None,
        ListRecordsConfig {
            chunk_idle_timeout: Duration::from_millis(20),
            ..ListRecordsConfig::default()
        },
    )
    .await
    .expect_err("idle page body should fail");

    assert!(matches!(error, ListRecordsError::InactivityTimeout { .. }));
    assert_eq!(handle.join().expect("server thread").len(), 1);
}

#[tokio::test]
async fn rate_limit_observer_runs_after_each_fetched_page() {
    let archive_dir = temp_dir("list-records-rate-limit-observer");
    let did_str = "did:plc:testrepo";
    let first_record = post_record(did_str, "3kabc", TEST_CID_A, "hello");
    let second_record = post_record(did_str, "3kabd", TEST_CID_B, "second");
    let (base_url, handle) = spawn_list_records_server(vec![
        TestResponse::json_page(Some(&first_record), Some("next"), Some(4), true),
        TestResponse::json_page(Some(&second_record), None, Some(3), true),
    ]);
    let http = Client::new();
    let pds = Uri::parse(base_url).expect("parse pds").clone();
    let did = Did::new_owned(did_str).expect("parse did");
    let mut observed_remaining = Vec::new();

    let output = fetch_and_archive_list_records_with_rate_limit_observer(
        &http,
        &pds,
        &did,
        did_str,
        &archive_dir,
        ArchiveCommitContext::fetch_one_local(),
        ArchiveStorageConfig::Local,
        ListRecordsConfig::default(),
        |rate_limit| observed_remaining.push(rate_limit.remaining),
    )
    .await
    .expect("fetch and archive listRecords");

    assert_eq!(observed_remaining, vec![Some(4), Some(3)]);
    assert_eq!(
        output
            .rate_limits
            .iter()
            .map(|rate_limit| rate_limit.remaining)
            .collect::<Vec<_>>(),
        vec![Some(4), Some(3)]
    );
    assert_eq!(output.archived_posts, 2);
    assert_eq!(handle.join().expect("server thread").len(), 2);
    fs::remove_dir_all(archive_dir).expect("remove archive dir");
}

#[tokio::test]
async fn shared_host_pacer_reserves_each_page_request() {
    let archive_dir = temp_dir("list-records-host-pacing");
    let did_str = "did:plc:testrepo";
    let first_record = post_record(did_str, "3kabc", TEST_CID_A, "hello");
    let second_record = post_record(did_str, "3kabd", TEST_CID_B, "second");
    let (base_url, handle) = spawn_list_records_server(vec![
        TestResponse::json_page(Some(&first_record), Some("next"), None, true),
        TestResponse::json_page(Some(&second_record), None, None, true),
    ]);
    let http = Client::new();
    let pds = Uri::parse(base_url).expect("parse pds").clone();
    let did = Did::new_owned(did_str).expect("parse did");
    let pacer = HostPacer::shared();
    let started = Instant::now();

    let output = fetch_and_archive_list_records_with_precommit_check(
        &http,
        &pds,
        &did,
        did_str,
        &archive_dir,
        ArchiveCommitContext::fetch_one_local(),
        ArchiveStorageConfig::Local,
        ListRecordsConfig::default(),
        Some(ListRecordsHostPacing::new(
            &pacer,
            "pds.example",
            Some(Duration::from_millis(40)),
        )),
        |_rate_limit| {},
        || Ok(()),
    )
    .await
    .expect("fetch and archive listRecords");

    assert!(started.elapsed() >= Duration::from_millis(30));
    assert_eq!(output.archived_posts, 2);
    assert_eq!(handle.join().expect("server thread").len(), 2);
    fs::remove_dir_all(archive_dir).expect("remove archive dir");
}

#[tokio::test]
async fn archive_ms_tracks_finalization_not_fetch_wall() {
    let archive_dir = temp_dir("list-records-archive-ms");
    let did_str = "did:plc:testrepo";
    let body = json!({"records": [], "cursor": null}).to_string();
    let (base_url, handle) = spawn_list_records_server(vec![TestResponse::raw_delayed(
        body,
        None,
        true,
        Duration::from_millis(100),
    )]);
    let http = Client::new();
    let pds = Uri::parse(base_url).expect("parse pds").clone();
    let did = Did::new_owned(did_str).expect("parse did");
    let started = Instant::now();

    let output = fetch_and_archive_list_records_with_precommit_check(
        &http,
        &pds,
        &did,
        did_str,
        &archive_dir,
        ArchiveCommitContext::fetch_one_local(),
        ArchiveStorageConfig::Local,
        ListRecordsConfig::default(),
        None,
        |_rate_limit| {},
        || {
            thread::sleep(Duration::from_millis(20));
            Ok(())
        },
    )
    .await
    .expect("fetch and archive listRecords");
    let wall_ms = u64::try_from(started.elapsed().as_millis()).expect("elapsed fits");

    assert!(output.archive_ms >= 15);
    assert!(output.archive_ms.saturating_add(50) < wall_ms);
    assert_eq!(handle.join().expect("server thread").len(), 1);
    fs::remove_dir_all(archive_dir).expect("remove archive dir");
}

fn post_record(did: &str, rkey: &str, cid: &str, text: &str) -> ListRecordsRecord {
    ListRecordsRecord {
        uri: format!("at://{did}/{POST_COLLECTION}/{rkey}"),
        cid: Some(cid.to_owned()),
        value: json!({
            "$type": POST_COLLECTION,
            "createdAt": "2026-06-16T00:00:00Z",
            "text": text
        }),
    }
}

fn temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("{prefix}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

struct TestResponse {
    body: String,
    remaining: Option<u64>,
    content_length: bool,
    body_delay: Option<Duration>,
}

impl TestResponse {
    fn json_page(
        record: Option<&ListRecordsRecord>,
        cursor: Option<&str>,
        remaining: Option<u64>,
        content_length: bool,
    ) -> Self {
        Self::raw(
            json!({
                "records": record.into_iter().map(record_json).collect::<Vec<_>>(),
                "cursor": cursor
            })
            .to_string(),
            remaining,
            content_length,
        )
    }

    fn raw(body: String, remaining: Option<u64>, content_length: bool) -> Self {
        Self {
            body,
            remaining,
            content_length,
            body_delay: None,
        }
    }

    fn raw_delayed(
        body: String,
        remaining: Option<u64>,
        content_length: bool,
        body_delay: Duration,
    ) -> Self {
        Self {
            body,
            remaining,
            content_length,
            body_delay: Some(body_delay),
        }
    }
}

fn record_json(record: &ListRecordsRecord) -> serde_json::Value {
    json!({
        "uri": record.uri,
        "cid": record.cid,
        "value": record.value,
    })
}

fn spawn_list_records_server(
    responses: Vec<TestResponse>,
) -> (String, thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind test server");
    let addr = listener.local_addr().expect("server addr");
    let handle = thread::spawn(move || {
        responses
            .into_iter()
            .map(|response| {
                let (mut stream, _addr) = listener.accept().expect("accept request");
                let target = read_request_target(&mut stream);
                write_list_records_response(&mut stream, &response);
                target
            })
            .collect::<Vec<_>>()
    });

    (format!("http://{addr}"), handle)
}

fn read_request_target(stream: &mut TcpStream) -> String {
    let mut headers = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        let read = stream.read(&mut byte).expect("read request headers");
        if read == 0 {
            break;
        }
        headers.push(byte[0]);
        if headers.ends_with(b"\r\n\r\n") {
            break;
        }
    }

    let header_text = String::from_utf8(headers).expect("utf8 headers");
    header_text
        .lines()
        .next()
        .expect("request line")
        .split_whitespace()
        .nth(1)
        .expect("request target")
        .to_owned()
}

fn write_list_records_response(stream: &mut TcpStream, response: &TestResponse) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n"
    )
    .expect("write status");
    if let Some(remaining) = response.remaining {
        write!(stream, "ratelimit-remaining: {remaining}\r\n").expect("write rate limit");
    }
    if response.content_length {
        write!(stream, "Content-Length: {}\r\n", response.body.len())
            .expect("write content length");
    }
    write!(stream, "\r\n").expect("write header terminator");
    if let Some(delay) = response.body_delay {
        thread::sleep(delay);
    }
    let _ = write!(stream, "{}", response.body);
}
