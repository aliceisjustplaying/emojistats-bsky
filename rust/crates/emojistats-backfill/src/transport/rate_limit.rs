use std::{
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use http::HeaderMap;
use serde::{Deserialize, Serialize};

/// Parsed `ratelimit-*`, `x-ratelimit-*`, and `retry-after` headers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RateLimitSnapshot {
    /// Advertised request limit.
    pub limit: Option<u64>,
    /// Remaining requests in the current window.
    pub remaining: Option<u64>,
    /// Reset value as advertised by the host.
    pub reset: Option<u64>,
    /// Retry delay when the host provides a seconds-based `retry-after`.
    pub retry_after: Option<Duration>,
    /// Raw `ratelimit-policy` header.
    pub policy: Option<String>,
}

impl RateLimitSnapshot {
    /// Parse rate-limit headers from a response.
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Self {
        Self {
            limit: parse_u64_header(headers, "ratelimit-limit")
                .or_else(|| parse_u64_header(headers, "x-ratelimit-limit")),
            remaining: parse_u64_header(headers, "ratelimit-remaining")
                .or_else(|| parse_u64_header(headers, "x-ratelimit-remaining")),
            reset: parse_u64_header(headers, "ratelimit-reset")
                .or_else(|| parse_u64_header(headers, "x-ratelimit-reset")),
            retry_after: parse_retry_after_header(headers),
            policy: parse_string_header(headers, "ratelimit-policy"),
        }
    }

    /// Return the host cooldown implied by these headers, if any.
    #[must_use]
    pub fn cooldown_delay(&self, now: SystemTime) -> Option<Duration> {
        if let Some(retry_after) = self.retry_after {
            return Some(retry_after);
        }
        if self.remaining != Some(0) {
            return None;
        }
        self.reset.and_then(|reset| reset_delay(reset, now))
    }
}

/// Terminal account states returned by `com.atproto.sync.getRepo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum AccountState {
    /// The repo does not exist on this host.
    RepoNotFound,
    /// The repo is taken down.
    RepoTakendown,
    /// The repo is suspended.
    RepoSuspended,
    /// The repo is deactivated.
    RepoDeactivated,
}

impl fmt::Display for AccountState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::RepoNotFound => "RepoNotFound",
            Self::RepoTakendown => "RepoTakendown",
            Self::RepoSuspended => "RepoSuspended",
            Self::RepoDeactivated => "RepoDeactivated",
        };
        f.write_str(name)
    }
}

fn parse_u64_header(headers: &HeaderMap, name: &'static str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn parse_retry_after_header(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get("retry-after")?.to_str().ok()?.trim();
    value
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| parse_http_date_retry_after(value, SystemTime::now()))
}

pub(super) fn parse_http_date_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    httpdate::parse_http_date(value)
        .ok()?
        .duration_since(now)
        .ok()
}

fn reset_delay(reset: u64, now: SystemTime) -> Option<Duration> {
    let now = now.duration_since(UNIX_EPOCH).ok()?.as_secs();
    if reset > now {
        return reset.checked_sub(now).map(Duration::from_secs);
    }
    if (1..=86_400).contains(&reset) {
        return Some(Duration::from_secs(reset));
    }
    None
}

fn parse_string_header(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}
