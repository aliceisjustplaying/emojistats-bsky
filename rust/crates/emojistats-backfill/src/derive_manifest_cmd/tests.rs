use std::{
    fs,
    io::{BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    thread,
};

use emojistats_backfill::{
    archive::{
        ArchiveCommitContext, ArchivePostRow, CompletenessClass, CreatedAtParseStatus, FetchMethod,
        LocalManifestEntry, NormalizerVersion, RepoReceipt, RepoReceiptInput, build_repo_receipt,
        current_normalizer, write_archive_artifacts,
    },
    clickhouse::{ClickHouseInsertPayload, ClickHouseTable, JSON_EACH_ROW_FORMAT},
    derive::BACKFILL_DERIVE_SOURCE,
    manifest_derive::read_committed_jsonl,
    metrics::noop_metrics_recorder,
};

use super::*;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

fn test_normalizer() -> NormalizerVersion {
    NormalizerVersion {
        name: "emoji-normalizer".to_owned(),
        semver: "0.1.0".to_owned(),
        git_rev: "test-git-rev".to_owned(),
        unicode_version: "16.0.0".to_owned(),
        emoji_data_version: "16.0".to_owned(),
    }
}

fn identity() -> DeriveManifestIdentity {
    DeriveManifestIdentity {
        run_id: "run-1".to_owned(),
        shard: "shard0".to_owned(),
        file_sequence: 7,
        did: "did:plc:test".to_owned(),
        dataset: "raw_archive_posts".to_owned(),
        fetch_method: "get_repo".to_owned(),
        completeness_class: "content_addressed_snapshot".to_owned(),
        content_hash: "content-hash".to_owned(),
        receipt_hash: "receipt-hash".to_owned(),
        schema_version: 2,
        normalizer: test_normalizer(),
    }
}

fn emoji_rows() -> Vec<EmojiProjectionRow> {
    vec![
        EmojiProjectionRow {
            did: "did:plc:test".to_owned(),
            rkey: "a".to_owned(),
            cid: "bafy-a".to_owned(),
            created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
            created_at_parse_status: CreatedAtParseStatus::Valid,
            emoji: ":test:".to_owned(),
            occurrences: 2,
            langs: vec!["en".to_owned(), "ja".to_owned()],
        },
        EmojiProjectionRow {
            did: "did:plc:test".to_owned(),
            rkey: "b".to_owned(),
            cid: "bafy-b".to_owned(),
            created_at_normalized: None,
            created_at_parse_status: CreatedAtParseStatus::Missing,
            emoji: ":other:".to_owned(),
            occurrences: 1,
            langs: Vec::new(),
        },
    ]
}

fn counter() -> TotalPostCounterInput {
    TotalPostCounterInput {
        source: BACKFILL_DERIVE_SOURCE.to_owned(),
        run_id: "run-1".to_owned(),
        shard: "shard0".to_owned(),
        file_sequence: 7,
        did: "did:plc:test".to_owned(),
        dataset: "raw_archive_posts".to_owned(),
        fetch_method: "get_repo".to_owned(),
        completeness_class: "content_addressed_snapshot".to_owned(),
        receipt_hash: "receipt-hash".to_owned(),
        normalizer: test_normalizer(),
        posts_processed: 3,
        posts_with_emojis: 2,
        emoji_occurrences: 4,
        min_created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
        max_created_at_normalized: Some("2026-06-15T01:00:00Z".to_owned()),
    }
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
        observed_at: ArchiveCommitContext::fetch_one_local().observed_at,
        did: "did:plc:test",
        fetch_method: FetchMethod::GetRepo,
        completeness_class: CompletenessClass::ContentAddressedSnapshot,
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

fn clickhouse_config() -> ClickHouseClientConfig {
    ClickHouseClientConfig::new(
        "http://localhost:8123",
        "emojistats",
        "alice",
        "secret",
        "emojistats-backfill-test",
    )
    .expect("clickhouse config should build")
}

fn derive_run_context<'a>(
    http: &'a reqwest::Client,
    clickhouse: &'a ClickHouseClientConfig,
    dry_run: bool,
    derive_ledger: &'a mut DeriveLedger,
    summary: &'a mut DeriveManifestSummary,
    metrics: &'a SharedMetricsRecorder,
) -> DeriveRunContext<'a> {
    DeriveRunContext {
        http,
        clickhouse,
        dry_run,
        derive_ledger,
        summary,
        metrics,
    }
}

fn read_first_input(manifest_path: &Path) -> LoaderInput {
    let file = fs::File::open(manifest_path).expect("manifest should be readable");
    let plan = read_committed_jsonl(BufReader::new(file)).expect("manifest should parse");
    plan.inputs
        .first()
        .expect("manifest should contain raw archive input")
        .clone()
}

#[derive(Debug, PartialEq, Eq)]
struct RecordedRequest {
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

    RecordedRequest {
        body: String::from_utf8(body).expect("utf8 body"),
    }
}

fn write_http_response(stream: &mut TcpStream, status: u16, body: &str) {
    let reason = match status {
        200 => "OK",
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

fn payload(table: ClickHouseTable, token: &str, body: &str) -> ClickHouseInsertPayload {
    ClickHouseInsertPayload {
        table,
        format: JSON_EACH_ROW_FORMAT,
        body: body.to_owned(),
        row_count: body.lines().count(),
        dedupe_token: token.to_owned(),
    }
}

fn verified_input() -> VerifiedLoaderInput {
    let identity = identity();
    VerifiedLoaderInput {
        manifest: LocalManifestEntry {
            run_id: identity.run_id.clone(),
            shard: identity.shard.clone(),
            file_sequence: identity.file_sequence,
            did: identity.did.clone(),
            dataset: identity.dataset.clone(),
            local_path: PathBuf::from("objects/raw_archive_posts/archive.parquet"),
            row_count: 0,
            bytes: 0,
            content_hash: identity.content_hash.clone(),
            min_created_at_normalized: None,
            max_created_at_normalized: None,
            receipt_hash: identity.receipt_hash.clone(),
            repo_receipt_path: None,
            schema_version: identity.schema_version,
            normalizer: current_normalizer(),
        },
        identity,
        object_path: PathBuf::from("/tmp/archive.parquet"),
        repo_receipt: repo_receipt(&[]),
    }
}

#[test]
fn canonical_streaming_emoji_dedupe_token_is_stable_and_framed() {
    let token = canonical_streaming_emoji_dedupe_token(&identity(), 0, &emoji_rows()).unwrap();

    assert_eq!(
        token,
        "derive:emoji:c140796160d4d7cb339d514053a68b76991bd67156693b53a88b0f07dbb8d629"
    );
}

#[test]
fn canonical_streaming_counter_dedupe_token_is_stable_and_framed() {
    let token = canonical_streaming_counter_dedupe_token(&identity(), &counter()).unwrap();

    assert_eq!(
        token,
        "derive:counter:6066bca798caeabbb48dad11cdd9fffa898176fccd5681c3de78130c471b25b4"
    );
}

#[test]
fn canonical_streaming_dedupe_tokens_include_lane_and_chunk() {
    let rows = emoji_rows();
    let first = canonical_streaming_emoji_dedupe_token(&identity(), 0, &rows).unwrap();
    let second = canonical_streaming_emoji_dedupe_token(&identity(), 1, &rows).unwrap();
    let counter = canonical_streaming_counter_dedupe_token(&identity(), &counter()).unwrap();

    assert_ne!(first, second);
    assert!(first.starts_with("derive:emoji:"));
    assert!(counter.starts_with("derive:counter:"));
}

#[test]
fn canonical_streaming_dedupe_tokens_are_stable_across_replay_manifest_sequence() {
    let mut replay_identity = identity();
    replay_identity.run_id = "run-2".to_owned();
    replay_identity.shard = "shard9".to_owned();
    replay_identity.file_sequence = 99;
    let mut replay_counter = counter();
    replay_counter.run_id = "run-2".to_owned();
    replay_counter.shard = "shard9".to_owned();
    replay_counter.file_sequence = 99;

    assert_eq!(
        canonical_streaming_emoji_dedupe_token(&identity(), 0, &emoji_rows()).unwrap(),
        canonical_streaming_emoji_dedupe_token(&replay_identity, 0, &emoji_rows()).unwrap()
    );
    assert_eq!(
        canonical_streaming_counter_dedupe_token(&identity(), &counter()).unwrap(),
        canonical_streaming_counter_dedupe_token(&replay_identity, &replay_counter).unwrap()
    );
}

#[test]
fn canonical_streaming_payload_rejects_single_emoji_row_over_payload_cap() {
    let verified = verified_input();
    let mut row = archive_row("oversized", "hello ✅", &["✅"]);
    row.langs = vec!["x".repeat(DEFAULT_EMOJI_SERVING_PAYLOAD_MAX_BYTES)];
    let mut state = CanonicalStreamingPayloadState::new(&verified);

    let error = state
        .consume_rows(&[row])
        .expect_err("oversized single row should fail before chunking");
    let error_text = error.to_string();

    assert!(error_text.contains("emoji serving row exceeds payload byte cap"));
    assert!(error_text.contains("did=did:plc:fixture123"));
    assert!(error_text.contains("rkey=oversized"));
    assert!(error_text.contains("cid=bafy-oversized"));
    assert!(error_text.contains("emoji=✅"));
}

#[tokio::test]
async fn dry_run_missing_repo_receipt_attempts_zero_payloads() {
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
    let input = read_first_input(&artifacts.manifest_path);
    fs::remove_file(&artifacts.receipt_path).expect("repo receipt should be removable");
    let clickhouse = clickhouse_config();
    let http = clickhouse.http_client().expect("http client");
    let mut derive_ledger = DeriveLedger::new(None).expect("derive ledger");
    let mut summary = DeriveManifestSummary::default();
    let metrics = noop_metrics_recorder();
    let mut context = derive_run_context(
        &http,
        &clickhouse,
        true,
        &mut derive_ledger,
        &mut summary,
        &metrics,
    );

    let error = derive_loader_input_canonical_streaming(&output_dir, &input, &mut context)
        .await
        .expect_err("missing repo receipt should fail");

    assert!(error.to_string().contains("repo receipt is missing"));
    assert_eq!(summary.attempted_insert_payloads, 0);
    assert_eq!(summary.attempted_insert_rows, 0);
    assert_eq!(summary.inserted_payloads, 0);
    assert_eq!(summary.inserted_rows, 0);
    assert_eq!(summary.archive_files, 0);
}

#[tokio::test]
async fn dry_run_corrupt_repo_receipt_attempts_zero_payloads() {
    let temp = TempDir::new("corrupt-receipt");
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
    let input = read_first_input(&artifacts.manifest_path);
    let mut corrupt_receipt = receipt;
    corrupt_receipt.post_rows_hash = "not-the-archive-row-hash".to_owned();
    fs::write(
        &artifacts.receipt_path,
        serde_json::to_vec_pretty(&corrupt_receipt).expect("receipt should serialize"),
    )
    .expect("corrupt receipt should write");
    let clickhouse = clickhouse_config();
    let http = clickhouse.http_client().expect("http client");
    let mut derive_ledger = DeriveLedger::new(None).expect("derive ledger");
    let mut summary = DeriveManifestSummary::default();
    let metrics = noop_metrics_recorder();
    let mut context = derive_run_context(
        &http,
        &clickhouse,
        true,
        &mut derive_ledger,
        &mut summary,
        &metrics,
    );

    let error = derive_loader_input_canonical_streaming(&output_dir, &input, &mut context)
        .await
        .expect_err("corrupt repo receipt should fail");

    let error_text = error.to_string();
    assert!(
        error_text.contains("receipt"),
        "unexpected corrupt receipt error: {error_text}"
    );
    assert_eq!(summary.attempted_insert_payloads, 0);
    assert_eq!(summary.attempted_insert_rows, 0);
    assert_eq!(summary.inserted_payloads, 0);
    assert_eq!(summary.inserted_rows, 0);
    assert_eq!(summary.archive_files, 0);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn insert_payloads_records_partial_success_and_resumes_from_ledger() {
    let temp = TempDir::new("partial-ledger");
    let ledger_path = temp.path.join("derive-ledger.jsonl");
    let verified = verified_input();
    let payloads = vec![
        payload(
            ClickHouseTable::EmojiServing,
            "derive:emoji:first",
            "{\"a\":1}\n",
        ),
        payload(
            ClickHouseTable::TotalPostCounter,
            "derive:counter:second",
            "{\"b\":2}\n",
        ),
    ];
    let (url, handle) =
        spawn_http_server(vec![(200, "emoji-ok".to_owned()), (503, "busy".to_owned())]);
    let clickhouse = ClickHouseClientConfig::new(
        &url,
        "emojistats",
        "alice",
        "secret",
        "emojistats-backfill-test",
    )
    .expect("clickhouse config")
    .with_timeout_and_retry_policy(
        std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(10),
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
        1,
    );
    let http = clickhouse.http_client().expect("http client");
    let mut derive_ledger = DeriveLedger::new(Some(&ledger_path)).expect("derive ledger");
    let mut summary = DeriveManifestSummary::default();
    let metrics = noop_metrics_recorder();
    let mut context = derive_run_context(
        &http,
        &clickhouse,
        false,
        &mut derive_ledger,
        &mut summary,
        &metrics,
    );

    let error = apply_derive_payloads(&mut context, &verified, &payloads)
        .await
        .expect_err("second payload should fail");
    let requests = handle.join().expect("server thread");

    assert!(error.to_string().contains("ClickHouse"));
    assert_eq!(requests.len(), 2);
    assert_eq!(summary.attempted_insert_payloads, 2);
    assert_eq!(summary.attempted_insert_rows, 2);
    assert_eq!(summary.inserted_payloads, 1);
    assert_eq!(summary.inserted_rows, 1);
    let ledger_lines = fs::read_to_string(&ledger_path).expect("ledger should exist");
    assert_eq!(ledger_lines.lines().count(), 1);

    let (url, handle) = spawn_http_server(vec![(200, "counter-ok".to_owned())]);
    let clickhouse = ClickHouseClientConfig::new(
        &url,
        "emojistats",
        "alice",
        "secret",
        "emojistats-backfill-test",
    )
    .expect("clickhouse config")
    .with_timeout_and_retry_policy(
        std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(10),
        std::time::Duration::ZERO,
        std::time::Duration::ZERO,
        1,
    );
    let http = clickhouse.http_client().expect("http client");
    let mut derive_ledger = DeriveLedger::new(Some(&ledger_path)).expect("derive ledger");
    let mut summary = DeriveManifestSummary::default();
    let metrics = noop_metrics_recorder();
    let mut context = derive_run_context(
        &http,
        &clickhouse,
        false,
        &mut derive_ledger,
        &mut summary,
        &metrics,
    );

    apply_derive_payloads(&mut context, &verified, &payloads)
        .await
        .expect("resume should insert only missing payload");
    let requests = handle.join().expect("server thread");

    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests.first().expect("request").body,
        payloads.get(1).expect("second payload").body
    );
    assert_eq!(summary.skipped_insert_payloads, 1);
    assert_eq!(summary.skipped_insert_rows, 1);
    assert_eq!(summary.attempted_insert_payloads, 1);
    assert_eq!(summary.inserted_payloads, 1);
    let ledger_lines = fs::read_to_string(&ledger_path).expect("ledger should exist");
    assert_eq!(ledger_lines.lines().count(), 2);
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(name: &str) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "emojistats-derive-manifest-cmd-{name}-{}-{sequence}",
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
