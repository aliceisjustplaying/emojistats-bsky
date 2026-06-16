use cid::Cid as IpldCid;
use jacquard_api::app_bsky::actor::profile::Profile;
use jacquard_common::types::{nsid::validate_nsid, recordkey::Rkey};
use smol_str::SmolStr;

use super::{
    ParseConfig, ParseError, ParseVisitError, PostRecord, ProfileRecord, RkeyDigest,
    checked_increment, ensure_u64_at_most,
};
use crate::post_decode;

pub(super) fn extract_known_record<S, E, F>(
    key: &str,
    cid: IpldCid,
    record_bytes: &[u8],
    sinks: &mut RecordSinks<'_, S, F>,
    config: ParseConfig,
) -> Result<(), ParseVisitError<E>>
where
    F: FnMut(&mut S, PostRecord) -> Result<(), E>,
{
    let Some((collection, rkey)) = split_repo_key(key) else {
        return Ok(());
    };

    match collection {
        POST_COLLECTION => {
            if let Some(decoded) = post_decode::from_cbor(record_bytes) {
                if decoded.typed_decode_failed {
                    record_decode_failed(sinks.decode_digest, POST_COLLECTION, config)?;
                }
                (sinks.visit_post)(
                    sinks.state,
                    PostRecord {
                        rkey: rkey.to_owned(),
                        cid: cid.to_string(),
                        body: decoded.body,
                    },
                )
                .map_err(ParseVisitError::Visit)?;
            } else {
                record_decode_failed(sinks.decode_digest, POST_COLLECTION, config)?;
            }
        }
        PROFILE_COLLECTION if rkey == PROFILE_RKEY => {
            match serde_ipld_dagcbor::from_slice::<Profile<SmolStr>>(record_bytes) {
                Ok(record) => {
                    *sinks.profile = Some(ProfileRecord {
                        rkey: rkey.to_owned(),
                        cid: cid.to_string(),
                        record,
                    });
                }
                Err(error) => {
                    let message = error.to_string();
                    *sinks.profile_decode_error =
                        Some(format!("{PROFILE_COLLECTION}/{rkey} at {cid}: {message}"));
                    record_decode_failed(sinks.decode_digest, PROFILE_COLLECTION, config)?;
                }
            }
        }
        _other => {}
    }

    Ok(())
}

pub(super) struct RecordSinks<'a, S, F> {
    pub(super) state: &'a mut S,
    pub(super) visit_post: &'a mut F,
    pub(super) profile: &'a mut Option<ProfileRecord>,
    pub(super) profile_decode_error: &'a mut Option<String>,
    pub(super) decode_digest: &'a mut DecodeDigest,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct DecodeDigest {
    pub(super) all_decode_errors_count: u64,
    pub(super) post_decode_errors_count: u64,
}

pub(super) const fn enforce_decode_error_limit(
    observed: u64,
    limit: u64,
) -> Result<(), ParseError> {
    ensure_u64_at_most(
        observed,
        limit,
        "max_decode_errors",
        "raise parser max_decode_errors only after inspecting malformed records",
    )
}

pub(super) fn record_decode_failed(
    digest: &mut DecodeDigest,
    collection: &'static str,
    config: ParseConfig,
) -> Result<(), ParseError> {
    digest.all_decode_errors_count =
        checked_increment(digest.all_decode_errors_count, "all_decode_errors_count")?;
    if collection == POST_COLLECTION {
        digest.post_decode_errors_count =
            checked_increment(digest.post_decode_errors_count, "post_decode_errors_count")?;
    }
    enforce_decode_error_limit(digest.all_decode_errors_count, config.max_decode_errors)
}

pub(super) fn update_digest(
    digest: &mut RkeyDigest,
    key: &str,
    config: ParseConfig,
) -> Result<(), ParseError> {
    digest.all_records_count = checked_increment(digest.all_records_count, "all_records_count")?;
    ensure_u64_at_most(
        digest.all_records_count,
        config.max_records,
        "max_records",
        "raise parser max_records only for a known-good repo",
    )?;
    if digest.first_key.is_none() {
        digest.first_key = Some(key.to_owned());
    }
    digest.last_key = Some(key.to_owned());

    if key.starts_with(POST_PREFIX) {
        digest.post_records_count =
            checked_increment(digest.post_records_count, "post_records_count")?;
    }

    Ok(())
}

pub(super) fn split_repo_key(key: &str) -> Option<(&str, &str)> {
    let (collection, rkey) = key.split_once('/')?;
    validate_repo_key_parts(collection, rkey).ok()?;
    Some((collection, rkey))
}

pub(super) fn validate_repo_key(key: &str) -> Result<(), ParseError> {
    let Some((collection, rkey)) = key.split_once('/') else {
        return Err(invalid_repo_key(key, "missing collection/rkey separator"));
    };
    validate_repo_key_parts(collection, rkey).map_err(|message| invalid_repo_key(key, message))
}

pub(super) fn validate_repo_key_parts(collection: &str, rkey: &str) -> Result<(), &'static str> {
    if validate_nsid(collection).is_err() {
        return Err("collection is not a valid NSID");
    }
    if rkey.is_empty() {
        return Err("rkey is empty");
    }
    if rkey.contains('/') {
        return Err("rkey contains an extra slash");
    }
    if Rkey::<&str>::new(rkey).is_err() {
        return Err("rkey is not a valid record key");
    }
    Ok(())
}

fn invalid_repo_key(key: &str, message: impl Into<String>) -> ParseError {
    ParseError::MalformedCar(format!("invalid repo key {key:?}: {}", message.into()))
}

const POST_COLLECTION: &str = "app.bsky.feed.post";
const POST_PREFIX: &str = "app.bsky.feed.post/";
const PROFILE_COLLECTION: &str = "app.bsky.actor.profile";
const PROFILE_RKEY: &str = "self";
