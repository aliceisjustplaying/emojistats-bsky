#![allow(clippy::arithmetic_side_effects)]

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

use reqwest::{Client, StatusCode};
use serde_json::Value;

use super::{
    CLICKHOUSE_DATABASE_HEADER, CLICKHOUSE_KEY_HEADER, CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES,
    CLICKHOUSE_USER_HEADER, ClickHouseClientConfig, ClickHouseInsertContext, ClickHouseInsertError,
    ClickHouseSchemaError, ClickHouseSqlContext, ClickHouseSqlError, ClickHouseTable,
    JSON_EACH_ROW_FORMAT, aggregate_rebuild_sql, aggregate_rebuild_statements,
    classify_insert_status, classify_sql_status, create_schema_sql, derive_insert_payloads,
    execute_insert_payloads, execute_sql_statements,
};
use crate::{
    archive::current_normalizer,
    derive::{
        ClickHouseDeriveBatch, DeriveManifestIdentity, PostServingRow, TotalPostCounterInput,
    },
};

fn batch() -> ClickHouseDeriveBatch {
    ClickHouseDeriveBatch {
        manifest_identity: DeriveManifestIdentity {
            run_id: "run-1".to_owned(),
            shard: "shard0".to_owned(),
            file_sequence: 42,
            did: "did:plc:abc".to_owned(),
            dataset: "raw_archive_posts".to_owned(),
            fetch_method: "get_repo".to_owned(),
            completeness_class: "content_addressed_snapshot".to_owned(),
            content_hash: "content-hash".to_owned(),
            receipt_hash: "receipt-hash".to_owned(),
            observed_at: "2026-06-15T00:00:00Z".to_owned(),
            schema_version: 3,
            normalizer: current_normalizer(),
        },
        dedupe_token: "derive:test-token".to_owned(),
        post_rows: vec![
            PostServingRow {
                did: "did:plc:abc".to_owned(),
                rkey: "3kxyz".to_owned(),
                created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
                created_at_parse_status: crate::archive::CreatedAtParseStatus::Valid,
                langs: vec!["en".to_owned(), "ja".to_owned()],
                emojis: vec!["✅".to_owned(), "✅".to_owned(), "🔥".to_owned()],
            },
            PostServingRow {
                did: "did:plc:def".to_owned(),
                rkey: "3kxyy".to_owned(),
                created_at_normalized: None,
                created_at_parse_status: crate::archive::CreatedAtParseStatus::Missing,
                langs: Vec::new(),
                emojis: Vec::new(),
            },
        ],
        total_post_counter: TotalPostCounterInput {
            source: "backfill-v2-derive".to_owned(),
            run_id: "run-1".to_owned(),
            shard: "shard0".to_owned(),
            file_sequence: 42,
            did: "did:plc:abc".to_owned(),
            dataset: "raw_archive_posts".to_owned(),
            fetch_method: "get_repo".to_owned(),
            completeness_class: "content_addressed_snapshot".to_owned(),
            receipt_hash: "receipt-hash".to_owned(),
            normalizer: current_normalizer(),
            posts_processed: 3,
            posts_with_emojis: 2,
            emoji_occurrences: 3,
            min_created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
            max_created_at_normalized: None,
        },
    }
}

fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
    value.get(name).expect("field should exist")
}

#[derive(Debug, PartialEq, Eq)]
struct RecordedRequest {
    target: String,
    body: String,
}

fn spawn_http_server(
    responses: Vec<(u16, String)>,
) -> (String, thread::JoinHandle<Vec<RecordedRequest>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind test server");
    let addr = listener.local_addr().expect("server addr");
    let handle = thread::spawn(move || {
        responses
            .into_iter()
            .map(|(status, body)| {
                let (mut stream, _addr) = listener.accept().expect("accept request");
                let request = read_http_request(&mut stream);
                write_http_response(&mut stream, status, &body);
                request
            })
            .collect::<Vec<_>>()
    });

    (format!("http://{addr}"), handle)
}

fn read_http_request(stream: &mut TcpStream) -> RecordedRequest {
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
    let content_length = header_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().expect("content length"))
        })
        .unwrap_or(0);
    let mut body = vec![0_u8; content_length];
    stream.read_exact(&mut body).expect("read request body");
    let request_line = header_text.lines().next().expect("request line").to_owned();
    let target = request_line
        .split_whitespace()
        .nth(1)
        .expect("request target")
        .to_owned();

    RecordedRequest {
        target,
        body: String::from_utf8(body).expect("utf8 body"),
    }
}

fn write_http_response(stream: &mut TcpStream, status: u16, body: &str) {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        503 => "Service Unavailable",
        _ => "Status",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("write response");
}

#[test]
fn schema_sql_contains_typed_table_names_and_engines() {
    let sql = create_schema_sql("emojistats").expect("schema sql");

    assert!(sql.contains("CREATE TABLE IF NOT EXISTS emojistats.v2_post_serving_r3"));
    assert!(sql.contains("CREATE TABLE IF NOT EXISTS emojistats.v2_total_post_counters_r3"));
    assert!(sql.contains("CREATE TABLE IF NOT EXISTS emojistats.v2_emoji_total_r3"));
    assert!(sql.contains("CREATE TABLE IF NOT EXISTS emojistats.v2_posts_hourly_r3"));
    assert!(sql.contains("ENGINE = ReplacingMergeTree(observed_at)"));
    assert!(sql.contains("non_replicated_deduplication_window = 10000"));
    assert!(sql.contains("LowCardinality(String)"));
    assert!(sql.contains("normalizer_git_rev LowCardinality(String)"));
    assert!(!sql.contains("cid String CODEC(ZSTD(1))"));
    assert!(!sql.contains("text String"));
    assert!(sql.contains("dataset LowCardinality(String)"));
    assert!(sql.contains("fetch_method LowCardinality(String)"));
    assert!(sql.contains("completeness_class LowCardinality(String)"));
    assert!(sql.contains("emojis Array(LowCardinality(String))"));
    assert!(sql.contains(
        "ORDER BY (src, normalizer_git_rev, dataset, fetch_method, completeness_class, did, rkey)"
    ));
    assert!(sql.contains(
        "ORDER BY (src, normalizer_git_rev, dataset, fetch_method, completeness_class, run_id, shard, file_sequence, receipt_hash, did)"
    ));
}

#[test]
fn aggregate_rebuild_sql_uses_compact_post_rows_and_array_join() {
    let sql = aggregate_rebuild_sql("emojistats").expect("aggregate rebuild sql");

    assert!(!sql.contains("TRUNCATE TABLE"));
    assert!(sql.contains("DROP TABLE IF EXISTS emojistats.v2_emoji_total_r3__rebuild_shadow SYNC"));
    assert!(sql.contains("CREATE TABLE emojistats.v2_emoji_total_r3__rebuild_shadow"));
    assert!(sql.contains("INSERT INTO emojistats.v2_emoji_total_r3__rebuild_shadow"));
    assert!(sql.contains(
        "EXCHANGE TABLES emojistats.v2_emoji_total_r3 AND emojistats.v2_emoji_total_r3__rebuild_shadow"
    ));
    assert!(sql.contains("FROM emojistats.v2_post_serving_r3 FINAL"));
    assert!(sql.contains("ARRAY JOIN emojis AS emoji"));
    assert!(sql.contains("arrayJoin(langs) AS lang"));
    assert!(sql.contains("arrayJoin(emojis) AS emoji"));
    assert!(sql.contains("INSERT INTO emojistats.v2_posts_hourly_r3__rebuild_shadow"));
    assert!(sql.contains("sum(emoji_occurrences) AS total_emoji_occurrences"));
    assert_eq!(sql.matches("max_memory_usage = 8589934592").count(), 4);
    assert_eq!(
        sql.matches("max_bytes_before_external_group_by = 1073741824")
            .count(),
        4
    );
}

#[test]
fn aggregate_rebuild_statements_are_ordered_shadow_insert_exchange_drop() {
    let statements = aggregate_rebuild_statements("emojistats").expect("aggregate statements");

    let tables = [
        ClickHouseTable::EmojiTotal,
        ClickHouseTable::EmojiTotalByLang,
        ClickHouseTable::LangTotal,
        ClickHouseTable::PostsHourly,
    ];

    assert_eq!(statements.len(), tables.len() * 5);
    assert!(
        statements
            .iter()
            .all(|statement| !statement.starts_with("TRUNCATE TABLE"))
    );

    for (table_index, table) in tables.iter().enumerate() {
        let offset = table_index * 5;
        let table_name = table.name();
        let shadow_table = format!("{table_name}__rebuild_shadow");
        let create_prefix = format!("CREATE TABLE emojistats.{shadow_table} (");
        let insert_prefix = format!("INSERT INTO emojistats.{shadow_table}");

        assert_eq!(
            statements.get(offset).expect("drop shadow"),
            &format!("DROP TABLE IF EXISTS emojistats.{shadow_table} SYNC;")
        );
        assert!(
            statements
                .get(offset + 1)
                .expect("create shadow")
                .starts_with(create_prefix.as_str())
        );
        assert!(
            statements
                .get(offset + 2)
                .expect("insert shadow")
                .starts_with(insert_prefix.as_str())
        );
        assert!(
            statements
                .get(offset + 2)
                .expect("insert shadow")
                .contains("max_memory_usage = 8589934592")
        );
        assert!(
            statements
                .get(offset + 2)
                .expect("insert shadow")
                .contains("max_bytes_before_external_group_by = 1073741824")
        );
        assert_eq!(
            statements.get(offset + 3).expect("exchange"),
            &format!("EXCHANGE TABLES emojistats.{table_name} AND emojistats.{shadow_table};")
        );
        assert_eq!(
            statements.get(offset + 4).expect("drop old table"),
            &format!("DROP TABLE IF EXISTS emojistats.{shadow_table} SYNC;")
        );
    }
}

#[test]
fn schema_sql_rejects_untrusted_database_identifiers() {
    let error = create_schema_sql("emojistats; DROP TABLE posts").expect_err("bad database");

    assert!(matches!(
        error,
        ClickHouseSchemaError::InvalidIdentifier { .. }
    ));
}

#[test]
fn json_each_row_payloads_include_derive_rows_and_total_counter() {
    let payloads = derive_insert_payloads(&batch()).expect("payloads");

    assert_eq!(payloads.len(), 2);
    let post_payload = payloads.first().expect("post payload should exist");
    assert_eq!(post_payload.table, ClickHouseTable::PostServing);
    assert_eq!(post_payload.format, JSON_EACH_ROW_FORMAT);
    assert_eq!(post_payload.row_count, 2);
    assert!(post_payload.body.ends_with('\n'));
    let post_lines = post_payload.body.lines().collect::<Vec<_>>();
    assert_eq!(post_lines.len(), 2);
    let first_line = post_lines.first().expect("first post line should exist");
    let first: Value = serde_json::from_str(first_line).expect("first post row json");
    assert_eq!(field(&first, "src"), "backfill-v2-derive");
    assert_eq!(field(&first, "dataset"), "raw_archive_posts");
    assert_eq!(field(&first, "fetch_method"), "get_repo");
    assert_eq!(
        field(&first, "completeness_class"),
        "content_addressed_snapshot"
    );
    assert_eq!(
        field(&first, "normalizer_name"),
        &Value::String(batch().manifest_identity.normalizer.name)
    );
    assert_eq!(field(&first, "did"), "did:plc:abc");
    assert!(first.get("cid").is_none());
    assert!(first.get("text").is_none());
    assert_eq!(field(&first, "emoji_occurrences"), 3);
    assert_eq!(field(&first, "observed_at"), "2026-06-15T00:00:00Z");
    let emojis = field(&first, "emojis")
        .as_array()
        .expect("emojis should be an array");
    assert_eq!(emojis.get(1), Some(&Value::String("✅".to_owned())));
    let langs = field(&first, "langs")
        .as_array()
        .expect("langs should be an array");
    assert_eq!(langs.get(1), Some(&Value::String("ja".to_owned())));

    let counter_payload = payloads.get(1).expect("counter payload should exist");
    assert_eq!(counter_payload.table, ClickHouseTable::TotalPostCounter);
    assert_eq!(counter_payload.row_count, 1);
    let counter_line = counter_payload
        .body
        .lines()
        .next()
        .expect("counter row should exist");
    let counter: Value = serde_json::from_str(counter_line).expect("counter row json");
    assert_eq!(
        field(&counter, "normalizer_git_rev"),
        &Value::String(batch().total_post_counter.normalizer.git_rev)
    );
    assert_eq!(field(&counter, "posts_processed"), 3);
    assert_eq!(field(&counter, "posts_with_emojis"), 2);
    assert_eq!(field(&counter, "max_created_at"), &Value::Null);
}

#[test]
fn request_builder_includes_auth_headers_and_dedupe_settings() {
    let payload = derive_insert_payloads(&batch())
        .expect("payloads")
        .remove(0);
    let config = ClickHouseClientConfig::new(
        "http://localhost:8123",
        "emojistats",
        "alice",
        "secret",
        "emojistats-backfill-test",
    )
    .expect("client config");

    let request = config.insert_request(&payload).expect("request");
    let url = request.url().as_str();

    assert_eq!(request.method(), "POST");
    assert_eq!(request.timeout(), Some(&Duration::from_secs(30)));
    assert!(url.contains("database=emojistats"));
    assert!(url.contains("query=INSERT"));
    assert!(url.contains("insert_deduplicate=1"));
    assert!(url.contains("insert_deduplication_token=derive%3Atest-token"));
    assert!(url.contains("date_time_input_format=best_effort"));
    assert_eq!(
        request.headers().get(&CLICKHOUSE_USER_HEADER),
        Some(&"alice".parse().expect("header value"))
    );
    assert_eq!(
        request.headers().get(&CLICKHOUSE_KEY_HEADER),
        Some(&"secret".parse().expect("header value"))
    );
    assert_eq!(
        request.headers().get(&CLICKHOUSE_DATABASE_HEADER),
        Some(&"emojistats".parse().expect("header value"))
    );
}

#[tokio::test]
async fn execute_insert_payloads_sends_payloads_in_order() {
    let payloads = derive_insert_payloads(&batch()).expect("payloads");
    let (url, handle) = spawn_http_server(vec![
        (200, "emoji-ok".to_owned()),
        (200, "counter-ok".to_owned()),
    ]);
    let config = ClickHouseClientConfig::new(
        &url,
        "emojistats",
        "alice",
        "secret",
        "emojistats-backfill-test",
    )
    .expect("client config")
    .with_timeout_and_retry_policy(
        Duration::from_secs(30),
        Duration::from_secs(10),
        Duration::ZERO,
        Duration::ZERO,
        1,
    );
    let client = Client::new();

    let receipts = execute_insert_payloads(&client, &config, &payloads)
        .await
        .expect("insert payloads");
    let requests = handle.join().expect("server thread");

    assert_eq!(receipts.len(), 2);
    assert_eq!(
        receipts.first().expect("first receipt").context.table,
        ClickHouseTable::PostServing
    );
    assert_eq!(
        receipts.get(1).expect("second receipt").context.table,
        ClickHouseTable::TotalPostCounter
    );
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .first()
            .expect("first request")
            .target
            .contains("v2_post_serving_r3")
    );
    assert!(
        requests
            .get(1)
            .expect("second request")
            .target
            .contains("v2_total_post_counters_r3")
    );
    assert!(
        requests
            .first()
            .expect("first request")
            .target
            .contains("insert_deduplication_token=derive%3Atest-token")
    );
    assert_eq!(
        requests.first().expect("first request").body,
        payloads.first().expect("first payload").body
    );
    assert_eq!(
        requests.get(1).expect("second request").body,
        payloads.get(1).expect("second payload").body
    );
}

#[tokio::test]
async fn execute_insert_payloads_classifies_retryable_status_with_snippet() {
    let payload = derive_insert_payloads(&batch())
        .expect("payloads")
        .remove(0);
    let response_body = format!(
        "too many parts {}",
        "x".repeat(CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES + 20)
    );
    let (url, handle) = spawn_http_server(vec![(503, response_body)]);
    let config = ClickHouseClientConfig::new(
        &url,
        "emojistats",
        "alice",
        "secret",
        "emojistats-backfill-test",
    )
    .expect("client config")
    .with_timeout_and_retry_policy(
        Duration::from_secs(30),
        Duration::from_secs(10),
        Duration::ZERO,
        Duration::ZERO,
        1,
    );
    let client = Client::new();

    let error = execute_insert_payloads(&client, &config, &[payload])
        .await
        .expect_err("retryable status");
    let requests = handle.join().expect("server thread");

    assert_eq!(requests.len(), 1);
    match error {
        ClickHouseInsertError::RetryableStatus {
            context,
            status,
            response_snippet,
        } => {
            assert_eq!(status, 503);
            assert_eq!(context.table, ClickHouseTable::PostServing);
            assert_eq!(context.dedupe_token, "derive:test-token");
            assert!(context.insert_deduplicate);
            assert_eq!(
                response_snippet.expect("response snippet").len(),
                CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn execute_insert_payloads_retries_bounded_retryable_statuses() {
    let payload = derive_insert_payloads(&batch())
        .expect("payloads")
        .remove(0);
    let (url, handle) =
        spawn_http_server(vec![(503, "busy".to_owned()), (200, "emoji-ok".to_owned())]);
    let config = ClickHouseClientConfig::new(
        &url,
        "emojistats",
        "alice",
        "secret",
        "emojistats-backfill-test",
    )
    .expect("client config")
    .with_timeout_and_retry_policy(
        Duration::from_secs(30),
        Duration::from_secs(10),
        Duration::ZERO,
        Duration::ZERO,
        2,
    );
    let client = Client::new();

    let receipts = execute_insert_payloads(&client, &config, &[payload])
        .await
        .expect("retry should succeed");
    let requests = handle.join().expect("server thread");

    assert_eq!(receipts.len(), 1);
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn execute_sql_statements_sends_statements_in_order() {
    let statements = vec![
        "TRUNCATE TABLE IF EXISTS emojistats.v2_emoji_total_r3;".to_owned(),
        "INSERT INTO emojistats.v2_emoji_total_r3 SELECT 1;".to_owned(),
    ];
    let (url, handle) = spawn_http_server(vec![
        (200, "truncate-ok".to_owned()),
        (200, "insert-ok".to_owned()),
    ]);
    let config = ClickHouseClientConfig::new(
        &url,
        "emojistats",
        "alice",
        "secret",
        "emojistats-backfill-test",
    )
    .expect("client config")
    .with_timeout_and_retry_policy(
        Duration::from_secs(30),
        Duration::from_secs(10),
        Duration::ZERO,
        Duration::ZERO,
        1,
    );
    let client = Client::new();

    let receipts = execute_sql_statements(&client, &config, &statements)
        .await
        .expect("sql statements");
    let requests = handle.join().expect("server thread");

    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts.first().expect("first").context.statement_index, 0);
    assert_eq!(receipts.get(1).expect("second").context.statement_index, 1);
    assert_eq!(requests.len(), 2);
    assert!(requests.first().expect("first").target.contains("TRUNCATE"));
    assert!(requests.get(1).expect("second").target.contains("INSERT"));
    assert!(requests.first().expect("first").body.is_empty());
    assert!(requests.get(1).expect("second").body.is_empty());
}

#[tokio::test]
async fn aggregate_rebuild_insert_is_not_retried() {
    let statements =
        vec!["INSERT INTO emojistats.v2_emoji_total_r3__rebuild_shadow SELECT 1;".to_owned()];
    let (url, handle) = spawn_http_server(vec![(503, "busy".to_owned())]);
    let config = ClickHouseClientConfig::new(
        &url,
        "emojistats",
        "alice",
        "secret",
        "emojistats-backfill-test",
    )
    .expect("client config")
    .with_timeout_and_retry_policy(
        Duration::from_secs(30),
        Duration::from_secs(10),
        Duration::ZERO,
        Duration::ZERO,
        2,
    );
    let client = Client::new();

    let error = execute_sql_statements(&client, &config, &statements)
        .await
        .expect_err("ambiguous shadow insert failure should not retry");
    let requests = handle.join().expect("server thread");

    assert_eq!(requests.len(), 1);
    match error {
        ClickHouseSqlError::RetryableStatus {
            context, status, ..
        } => {
            assert_eq!(context.statement_index, 0);
            assert_eq!(status, 503);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn classify_sql_status_marks_server_errors_retryable() {
    let context = ClickHouseSqlContext {
        statement_index: 0,
        statement: "TRUNCATE TABLE t".to_owned(),
    };

    let error = classify_sql_status(
        context,
        StatusCode::SERVICE_UNAVAILABLE,
        Some("busy".to_owned()),
    );

    match error {
        ClickHouseSqlError::RetryableStatus {
            context,
            status,
            response_snippet,
        } => {
            assert_eq!(context.statement_index, 0);
            assert_eq!(status, 503);
            assert_eq!(response_snippet.as_deref(), Some("busy"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn classify_insert_status_marks_client_errors_permanent() {
    let context = ClickHouseInsertContext {
        table: ClickHouseTable::TotalPostCounter,
        row_count: 1,
        dedupe_token: "derive:test-token".to_owned(),
        insert_deduplicate: true,
    };

    let error = classify_insert_status(context, StatusCode::BAD_REQUEST, Some("syntax".to_owned()));

    match error {
        ClickHouseInsertError::PermanentStatus {
            context,
            status,
            response_snippet,
        } => {
            assert_eq!(status, 400);
            assert_eq!(context.table, ClickHouseTable::TotalPostCounter);
            assert_eq!(response_snippet.as_deref(), Some("syntax"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn empty_post_batches_only_emit_total_counter_payload() {
    let mut batch = batch();
    batch.post_rows.clear();

    let payloads = derive_insert_payloads(&batch).expect("payloads");

    assert_eq!(payloads.len(), 1);
    let payload = payloads.first().expect("counter payload should exist");
    assert_eq!(payload.table, ClickHouseTable::TotalPostCounter);
}
