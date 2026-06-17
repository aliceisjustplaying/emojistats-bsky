use sha2::{Digest, Sha256};

use super::{DeriveError, DeriveManifestIdentity, PostServingRow, TotalPostCounterInput};
use crate::archive::NormalizerVersion;

// Canonical streaming derive tokens are lane/chunk-framed and use the same stable manifest
// identity fields as full-batch derive tokens: dataset, DID, proof hashes, schema, and normalizer.
const STREAMING_DEDUPE_TOKEN_DOMAIN: &str = "emojistats-backfill-streaming-derive-token-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamingDedupeLane {
    Post,
    Counter,
}

impl StreamingDedupeLane {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Post => "post",
            Self::Counter => "counter",
        }
    }
}

/// Build the canonical `ClickHouse` dedupe token for one streaming post chunk.
///
/// # Errors
///
/// Returns an error if any framed field length or row count cannot be represented.
pub fn canonical_streaming_post_dedupe_token(
    identity: &DeriveManifestIdentity,
    chunk_index: u64,
    rows: &[PostServingRow],
) -> anyhow::Result<String> {
    let mut hasher =
        canonical_streaming_dedupe_hasher(identity, StreamingDedupeLane::Post, Some(chunk_index))?;
    hash_str_frame(&mut hasher, "payload.kind", "post_rows")?;
    hash_u64_frame(
        &mut hasher,
        "post_rows.len",
        count_len(rows.len(), "streaming dedupe post row count")?,
    )?;
    for (index, row) in rows.iter().enumerate() {
        hash_u64_frame(
            &mut hasher,
            "post_row.index",
            count_len(index, "streaming dedupe post row index")?,
        )?;
        hash_post_row_frames(&mut hasher, row)?;
    }
    Ok(streaming_dedupe_token(StreamingDedupeLane::Post, hasher))
}

/// Build the canonical `ClickHouse` dedupe token for one streaming total counter payload.
///
/// # Errors
///
/// Returns an error if any framed field length or counter value cannot be represented.
pub fn canonical_streaming_counter_dedupe_token(
    identity: &DeriveManifestIdentity,
    counter: &TotalPostCounterInput,
) -> anyhow::Result<String> {
    let mut hasher =
        canonical_streaming_dedupe_hasher(identity, StreamingDedupeLane::Counter, None)?;
    hash_str_frame(&mut hasher, "payload.kind", "total_post_counter")?;
    hash_counter_frames(&mut hasher, counter)?;
    Ok(streaming_dedupe_token(StreamingDedupeLane::Counter, hasher))
}

fn canonical_streaming_dedupe_hasher(
    identity: &DeriveManifestIdentity,
    lane: StreamingDedupeLane,
    chunk_index: Option<u64>,
) -> anyhow::Result<Sha256> {
    let mut hasher = Sha256::new();
    hash_str_frame(&mut hasher, "domain", STREAMING_DEDUPE_TOKEN_DOMAIN)?;
    hash_str_frame(&mut hasher, "lane", lane.as_str())?;
    hash_optional_u64_frame(&mut hasher, "chunk_index", chunk_index)?;
    hash_identity_frames(&mut hasher, identity)?;
    Ok(hasher)
}

fn streaming_dedupe_token(lane: StreamingDedupeLane, hasher: Sha256) -> String {
    format!(
        "derive:{}:{}",
        lane.as_str(),
        hex::encode(hasher.finalize())
    )
}

fn hash_identity_frames(
    hasher: &mut Sha256,
    identity: &DeriveManifestIdentity,
) -> anyhow::Result<()> {
    hash_str_frame(hasher, "identity.dataset", &identity.dataset)?;
    hash_str_frame(hasher, "identity.did", &identity.did)?;
    hash_str_frame(hasher, "identity.fetch_method", &identity.fetch_method)?;
    hash_str_frame(
        hasher,
        "identity.completeness_class",
        &identity.completeness_class,
    )?;
    hash_str_frame(hasher, "identity.content_hash", &identity.content_hash)?;
    hash_str_frame(hasher, "identity.receipt_hash", &identity.receipt_hash)?;
    hash_str_frame(hasher, "identity.observed_at", &identity.observed_at)?;
    hash_u16_frame(hasher, "identity.schema_version", identity.schema_version)?;
    hash_normalizer_frames(hasher, "identity.normalizer", &identity.normalizer)
}

fn hash_post_row_frames(hasher: &mut Sha256, row: &PostServingRow) -> anyhow::Result<()> {
    hash_str_frame(hasher, "post_row.did", &row.did)?;
    hash_str_frame(hasher, "post_row.rkey", &row.rkey)?;
    hash_optional_str_frame(
        hasher,
        "post_row.created_at_normalized",
        row.created_at_normalized.as_deref(),
    )?;
    hash_str_frame(
        hasher,
        "post_row.created_at_parse_status",
        row.created_at_parse_status.as_str(),
    )?;
    hash_u64_frame(
        hasher,
        "post_row.langs.len",
        count_len(row.langs.len(), "streaming dedupe language count")?,
    )?;
    for (index, lang) in row.langs.iter().enumerate() {
        hash_u64_frame(
            hasher,
            "post_row.lang.index",
            count_len(index, "streaming dedupe language index")?,
        )?;
        hash_str_frame(hasher, "post_row.lang", lang)?;
    }
    hash_u64_frame(
        hasher,
        "post_row.emojis.len",
        count_len(row.emojis.len(), "streaming dedupe emoji count")?,
    )?;
    for (index, emoji) in row.emojis.iter().enumerate() {
        hash_u64_frame(
            hasher,
            "post_row.emoji.index",
            count_len(index, "streaming dedupe emoji index")?,
        )?;
        hash_str_frame(hasher, "post_row.emoji", emoji)?;
    }
    Ok(())
}

fn hash_counter_frames(hasher: &mut Sha256, counter: &TotalPostCounterInput) -> anyhow::Result<()> {
    hash_str_frame(hasher, "counter.source", &counter.source)?;
    hash_str_frame(hasher, "counter.did", &counter.did)?;
    hash_str_frame(hasher, "counter.dataset", &counter.dataset)?;
    hash_str_frame(hasher, "counter.fetch_method", &counter.fetch_method)?;
    hash_str_frame(
        hasher,
        "counter.completeness_class",
        &counter.completeness_class,
    )?;
    hash_str_frame(hasher, "counter.receipt_hash", &counter.receipt_hash)?;
    hash_normalizer_frames(hasher, "counter.normalizer", &counter.normalizer)?;
    hash_u64_frame(hasher, "counter.posts_processed", counter.posts_processed)?;
    hash_u64_frame(
        hasher,
        "counter.posts_with_emojis",
        counter.posts_with_emojis,
    )?;
    hash_u64_frame(
        hasher,
        "counter.emoji_occurrences",
        counter.emoji_occurrences,
    )?;
    hash_optional_str_frame(
        hasher,
        "counter.min_created_at_normalized",
        counter.min_created_at_normalized.as_deref(),
    )?;
    hash_optional_str_frame(
        hasher,
        "counter.max_created_at_normalized",
        counter.max_created_at_normalized.as_deref(),
    )
}

fn hash_normalizer_frames(
    hasher: &mut Sha256,
    label: &'static str,
    normalizer: &NormalizerVersion,
) -> anyhow::Result<()> {
    hash_str_frame(hasher, label, &normalizer.name)?;
    hash_str_frame(hasher, label, &normalizer.semver)?;
    hash_str_frame(hasher, label, &normalizer.git_rev)?;
    hash_str_frame(hasher, label, &normalizer.unicode_version)?;
    hash_str_frame(hasher, label, &normalizer.emoji_data_version)
}

fn hash_optional_str_frame(
    hasher: &mut Sha256,
    label: &'static str,
    value: Option<&str>,
) -> anyhow::Result<()> {
    match value {
        Some(value) => {
            hash_str_frame(hasher, label, "some")?;
            hash_str_frame(hasher, label, value)
        }
        None => hash_str_frame(hasher, label, "none"),
    }
}

fn hash_optional_u64_frame(
    hasher: &mut Sha256,
    label: &'static str,
    value: Option<u64>,
) -> anyhow::Result<()> {
    match value {
        Some(value) => {
            hash_str_frame(hasher, label, "some")?;
            hash_u64_frame(hasher, label, value)
        }
        None => hash_str_frame(hasher, label, "none"),
    }
}

fn hash_str_frame(hasher: &mut Sha256, label: &'static str, value: &str) -> anyhow::Result<()> {
    hash_frame(hasher, label, value.as_bytes())
}

fn hash_u64_frame(hasher: &mut Sha256, label: &'static str, value: u64) -> anyhow::Result<()> {
    hash_frame(hasher, label, &value.to_be_bytes())
}

fn hash_u16_frame(hasher: &mut Sha256, label: &'static str, value: u16) -> anyhow::Result<()> {
    hash_frame(hasher, label, &value.to_be_bytes())
}

fn hash_frame(hasher: &mut Sha256, label: &'static str, value: &[u8]) -> anyhow::Result<()> {
    hasher.update(count_len(label.len(), "streaming dedupe frame label length")?.to_be_bytes());
    hasher.update(label.as_bytes());
    hasher.update(count_len(value.len(), "streaming dedupe frame value length")?.to_be_bytes());
    hasher.update(value);
    Ok(())
}

fn count_len(value: usize, field: &'static str) -> Result<u64, DeriveError> {
    u64::try_from(value).map_err(|_err| DeriveError::CountOverflow { field })
}
