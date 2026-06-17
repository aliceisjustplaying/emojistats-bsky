use super::{
    ArchiveError, ArchivePostRow, DateTime, NormalizerVersion, PARTIAL_RECORD_STATUS, ParsedRepo,
    PostRecord, PostRecordBody, RawPartialPostRecord, Utc,
    archive_io::{extract_emojis, record_extras_json},
    classify_created_at_observed_at,
};

/// Convert parsed post records into the first archive-row shape.
///
/// # Errors
///
/// Returns [`ArchiveError`] if record extras cannot be serialized without loss.
pub fn archive_rows_from_parsed_repo(
    parsed: &ParsedRepo,
) -> Result<Vec<ArchivePostRow>, ArchiveError> {
    let normalizer = current_normalizer();
    parsed
        .posts
        .iter()
        .map(|post| archive_row_from_post(&parsed.commit.did, post, &normalizer))
        .collect()
}

/// Convert one parsed post into an archive row without retaining the whole repo.
///
/// # Errors
///
/// Returns [`ArchiveError`] if record extras cannot be serialized without loss.
pub fn archive_row_from_post(
    did: &str,
    post: &PostRecord,
    normalizer: &NormalizerVersion,
) -> Result<ArchivePostRow, ArchiveError> {
    archive_row_from_post_observed_at(did, post, normalizer, Utc::now())
}

/// Convert one parsed post into an archive row relative to a fixed observation time.
///
/// # Errors
///
/// Returns [`ArchiveError`] if record extras cannot be serialized without loss.
pub fn archive_row_from_post_observed_at(
    did: &str,
    post: &PostRecord,
    normalizer: &NormalizerVersion,
    observed_at: DateTime<Utc>,
) -> Result<ArchivePostRow, ArchiveError> {
    match &post.body {
        PostRecordBody::Typed(record) => {
            archive_row_from_typed_post(did, &post.rkey, &post.cid, record, normalizer, observed_at)
        }
        PostRecordBody::RawPartial(record) => Ok(archive_row_from_raw_partial_post(
            did,
            post,
            record,
            normalizer,
            observed_at,
        )),
    }
}

/// Convert an owned parsed post into an archive row.
///
/// # Errors
///
/// Returns [`ArchiveError`] if record extras cannot be serialized without loss.
pub fn archive_row_from_owned_post(
    did: &str,
    post: PostRecord,
    normalizer: &NormalizerVersion,
) -> Result<ArchivePostRow, ArchiveError> {
    archive_row_from_owned_post_observed_at(did, post, normalizer, Utc::now())
}

/// Convert an owned parsed post into an archive row relative to a fixed observation time.
///
/// # Errors
///
/// Returns [`ArchiveError`] if record extras cannot be serialized without loss.
pub fn archive_row_from_owned_post_observed_at(
    did: &str,
    post: PostRecord,
    normalizer: &NormalizerVersion,
    observed_at: DateTime<Utc>,
) -> Result<ArchivePostRow, ArchiveError> {
    let PostRecord { rkey, cid, body } = post;
    match body {
        PostRecordBody::Typed(record) => {
            archive_row_from_typed_post(did, &rkey, &cid, &record, normalizer, observed_at)
        }
        PostRecordBody::RawPartial(record) => Ok(archive_row_from_owned_raw_partial_post(
            did,
            rkey,
            cid,
            record,
            normalizer,
            observed_at,
        )),
    }
}

fn archive_row_from_typed_post(
    did: &str,
    rkey: &str,
    cid: &str,
    record: &jacquard_api::app_bsky::feed::post::Post<smol_str::SmolStr>,
    normalizer: &NormalizerVersion,
    observed_at: DateTime<Utc>,
) -> Result<ArchivePostRow, ArchiveError> {
    let created_at = record.created_at.as_str();
    let classified = classify_created_at_observed_at(Some(created_at), observed_at);
    Ok(ArchivePostRow {
        did: did.to_owned(),
        rkey: rkey.to_owned(),
        cid: cid.to_owned(),
        normalizer: normalizer.clone(),
        account_status: None,
        record_status: None,
        public_content_label: None,
        created_at_raw: classified.raw,
        created_at_normalized: classified.normalized,
        created_at_parse_status: classified.status,
        text: record.text.to_string(),
        langs: record.langs.as_ref().map_or_else(Vec::new, |langs| {
            langs.iter().map(ToString::to_string).collect()
        }),
        emoji_sequence: extract_emojis(record.text.as_str()),
        extras_json: record_extras_json(record)?,
    })
}

fn archive_row_from_raw_partial_post(
    did: &str,
    post: &PostRecord,
    partial: &RawPartialPostRecord,
    normalizer: &NormalizerVersion,
    observed_at: DateTime<Utc>,
) -> ArchivePostRow {
    let classified =
        classify_created_at_observed_at(partial.created_at_raw.as_deref(), observed_at);
    let text = partial.text.clone().unwrap_or_default();
    ArchivePostRow {
        did: did.to_owned(),
        rkey: post.rkey.clone(),
        cid: post.cid.clone(),
        normalizer: normalizer.clone(),
        account_status: None,
        record_status: partial
            .typed_decode_failed
            .then(|| PARTIAL_RECORD_STATUS.to_owned()),
        public_content_label: None,
        created_at_raw: classified.raw,
        created_at_normalized: classified.normalized,
        created_at_parse_status: classified.status,
        emoji_sequence: extract_emojis(&text),
        text,
        langs: partial.langs.clone(),
        extras_json: partial.extras_json.clone(),
    }
}

fn archive_row_from_owned_raw_partial_post(
    did: &str,
    rkey: String,
    cid: String,
    partial: RawPartialPostRecord,
    normalizer: &NormalizerVersion,
    observed_at: DateTime<Utc>,
) -> ArchivePostRow {
    let classified =
        classify_created_at_observed_at(partial.created_at_raw.as_deref(), observed_at);
    let text = partial.text.unwrap_or_default();
    ArchivePostRow {
        did: did.to_owned(),
        rkey,
        cid,
        normalizer: normalizer.clone(),
        account_status: None,
        record_status: partial
            .typed_decode_failed
            .then(|| PARTIAL_RECORD_STATUS.to_owned()),
        public_content_label: None,
        created_at_raw: classified.raw,
        created_at_normalized: classified.normalized,
        created_at_parse_status: classified.status,
        emoji_sequence: extract_emojis(&text),
        text,
        langs: partial.langs,
        extras_json: partial.extras_json,
    }
}

/// Current vertical-slice normalizer identity.
#[must_use]
pub fn current_normalizer() -> NormalizerVersion {
    emoji_normalizer::version()
}
