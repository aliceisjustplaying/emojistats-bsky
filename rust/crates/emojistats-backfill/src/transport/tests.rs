#![allow(clippy::indexing_slicing)]

use std::{
    path::PathBuf,
    time::{Duration, UNIX_EPOCH},
};

use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use jacquard_common::{stream::ByteStream, types::did::Did};

use super::{
    AccountState, FetchByteBudget, FetchConfig, FetchError, RateLimitSnapshot, StreamLimits,
    classify_http_error, parse_http_date_retry_after, spool_path, stream_to_file,
};

#[test]
fn parses_standard_rate_limit_headers() {
    let mut headers = HeaderMap::new();
    headers.insert("ratelimit-limit", "3000".parse().unwrap());
    headers.insert("ratelimit-remaining", "2999".parse().unwrap());
    headers.insert("ratelimit-reset", "42".parse().unwrap());
    headers.insert("ratelimit-policy", "3000;w=300".parse().unwrap());
    headers.insert("retry-after", "5".parse().unwrap());

    let snapshot = RateLimitSnapshot::from_headers(&headers);

    assert_eq!(snapshot.limit, Some(3000));
    assert_eq!(snapshot.remaining, Some(2999));
    assert_eq!(snapshot.reset, Some(42));
    assert_eq!(snapshot.retry_after, Some(Duration::from_secs(5)));
    assert_eq!(snapshot.policy.as_deref(), Some("3000;w=300"));
}

#[test]
#[allow(clippy::duration_suboptimal_units)]
fn parses_http_date_retry_after() {
    let delay = parse_http_date_retry_after(
        "Tue, 16 Jun 2026 00:00:10 GMT",
        UNIX_EPOCH + Duration::from_secs(1_781_568_000),
    );

    assert_eq!(delay, Some(Duration::from_secs(10)));
}

#[test]
#[allow(clippy::duration_suboptimal_units)]
fn cooldown_delay_uses_empty_remaining_reset() {
    let snapshot = RateLimitSnapshot {
        remaining: Some(0),
        reset: Some(1_781_568_030),
        ..RateLimitSnapshot::default()
    };

    assert_eq!(
        snapshot.cooldown_delay(UNIX_EPOCH + Duration::from_secs(1_781_568_000)),
        Some(Duration::from_secs(30))
    );
}

#[test]
fn falls_back_to_x_rate_limit_headers() {
    let mut headers = HeaderMap::new();
    headers.insert("x-ratelimit-limit", "100".parse().unwrap());
    headers.insert("x-ratelimit-remaining", "7".parse().unwrap());
    headers.insert("x-ratelimit-reset", "99".parse().unwrap());

    let snapshot = RateLimitSnapshot::from_headers(&headers);

    assert_eq!(snapshot.limit, Some(100));
    assert_eq!(snapshot.remaining, Some(7));
    assert_eq!(snapshot.reset, Some(99));
}

#[test]
fn classifies_repo_account_states() {
    let body = br#"{"error":"RepoSuspended","message":"nope"}"#;

    let err = classify_http_error(StatusCode::FORBIDDEN, RateLimitSnapshot::default(), body);

    match err {
        FetchError::AccountState {
            state,
            status,
            message,
            rate_limit: _,
        } => {
            assert_eq!(state, AccountState::RepoSuspended);
            assert_eq!(status, 403);
            assert_eq!(message.as_deref(), Some("nope"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn preserves_unknown_xrpc_error_code() {
    let body = br#"{"error":"HostThrottled","message":"slow down"}"#;

    let err = classify_http_error(
        StatusCode::TOO_MANY_REQUESTS,
        RateLimitSnapshot::default(),
        body,
    );

    match err {
        FetchError::HttpStatus {
            status,
            error_code,
            message,
            rate_limit: _,
        } => {
            assert_eq!(status, 429);
            assert_eq!(error_code.as_deref(), Some("HostThrottled"));
            assert_eq!(message.as_deref(), Some("slow down"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn default_config_sets_spool_dir_and_limits() {
    let config = FetchConfig::new(PathBuf::from("/tmp/spool"));

    assert_eq!(config.spool_dir, PathBuf::from("/tmp/spool"));
    assert_eq!(config.chunk_idle_timeout, Duration::from_secs(30));
    assert_eq!(config.max_bytes, 2_147_483_648);
    assert!(config.byte_budget.is_none());
}

#[tokio::test]
async fn byte_budget_blocks_until_prior_reservation_is_dropped() {
    let budget = FetchByteBudget::new(10);
    let mut first = budget.reservation();
    first.reserve_capacity(10).await.unwrap();
    let mut second = budget.reservation();

    let blocked = tokio::time::timeout(Duration::from_millis(10), second.reserve_capacity(1)).await;

    assert!(blocked.is_err());
    drop(first);
    tokio::time::timeout(Duration::from_secs(1), second.reserve_capacity(1))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn byte_budget_rejects_single_repo_larger_than_budget() {
    let budget = FetchByteBudget::new(10);
    let mut reservation = budget.reservation();

    let error = reservation.reserve_capacity(11).await.unwrap_err();

    match error {
        FetchError::InFlightBytesExceeded {
            max_bytes,
            observed_bytes,
        } => {
            assert_eq!(max_bytes, 10);
            assert_eq!(observed_bytes, 11);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(reservation.charged_bytes(), 0);
}

#[tokio::test]
async fn byte_budget_blocks_before_streaming_file() {
    let budget = FetchByteBudget::new(10);
    let mut first = budget.reservation();
    first.reserve_capacity(8).await.unwrap();
    let path = std::env::temp_dir().join(format!(
        "emojistats-byte-budget-stream-{}-{}.car",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let body = ByteStream::new(futures_util::stream::iter([Ok(Bytes::from_static(b"abc"))]));
    let task_path = path.clone();
    let handle = tokio::spawn(async move {
        stream_to_file(
            body,
            &task_path,
            StreamLimits {
                chunk_idle_timeout: Duration::from_secs(1),
                download_timeout: Duration::from_secs(10),
                min_progress_bytes: 0,
                min_progress_interval: Duration::from_secs(1),
                max_bytes: 3,
            },
            3,
            Some(&budget),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!handle.is_finished());
    assert!(!path.exists() || std::fs::metadata(&path).unwrap().len() == 0);

    drop(first);
    let (result, reservation) = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result, 3);
    assert_eq!(reservation.unwrap().charged_bytes(), 3);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn spool_path_sanitizes_did() {
    let did = Did::new_owned("did:plc:abc123").unwrap();

    let path = spool_path(PathBuf::from("/tmp/spool").as_path(), &did);

    let file_name = path.file_name().and_then(std::ffi::OsStr::to_str).unwrap();
    assert!(file_name.starts_with("repo-did_plc_abc123."));
    assert!(
        std::path::Path::new(file_name)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("car"))
    );
}
