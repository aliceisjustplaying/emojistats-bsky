use sha2::{Digest, Sha256};

use super::{ArchiveError, ArchivePostRow, NormalizerVersion, POST_COLLECTION};

/// Incremental hasher for canonical archive post row content.
#[derive(Debug, Default)]
pub struct ArchivePostRowsHasher {
    hasher: Sha256,
}

impl ArchivePostRowsHasher {
    #[must_use]
    pub fn new() -> Self {
        Self {
            hasher: Sha256::new(),
        }
    }

    /// Add one archive row to the canonical post-row hash.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveError`] if row content cannot be framed for hashing.
    pub fn push_row(&mut self, row: &ArchivePostRow) -> Result<(), ArchiveError> {
        hash_post_row_into(&mut self.hasher, row)
    }

    #[must_use]
    pub fn finish(self) -> String {
        hex::encode(self.hasher.finalize())
    }
}

/// Hash the canonical row content named in `docs/backfill-v2-design.md`.
///
/// # Errors
///
/// Returns [`ArchiveError`] if any hashed string length cannot fit the stable hash framing.
pub fn hash_post_rows(rows: &[ArchivePostRow]) -> Result<String, ArchiveError> {
    let mut hasher = Sha256::new();
    for row in rows {
        hash_post_row_into(&mut hasher, row)?;
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(super) fn hash_post_row_into(
    hasher: &mut Sha256,
    row: &ArchivePostRow,
) -> Result<(), ArchiveError> {
    hash_field(hasher, POST_COLLECTION)?;
    hash_field(hasher, &row.did)?;
    hash_field(hasher, &row.rkey)?;
    hash_field(hasher, &row.cid)?;
    hash_normalizer(hasher, &row.normalizer)?;
    hash_optional_field(hasher, row.account_status.as_deref())?;
    hash_optional_field(hasher, row.record_status.as_deref())?;
    hash_optional_field(hasher, row.public_content_label.as_deref())?;
    hash_optional_field(hasher, row.created_at_raw.as_deref())?;
    hash_optional_field(hasher, row.created_at_normalized.as_deref())?;
    hash_field(hasher, row.created_at_parse_status.as_str())?;
    hash_field(hasher, &row.text)?;
    hash_string_slice(hasher, &row.langs)?;
    hash_string_slice(hasher, &row.emoji_sequence)?;
    hash_extras_json(hasher, &row.extras_json)
}

pub(super) fn hash_string_slice(
    hasher: &mut Sha256,
    values: &[String],
) -> Result<(), ArchiveError> {
    for value in values {
        hash_field(hasher, value)?;
    }
    hash_field(hasher, "")
}

pub(super) fn hash_optional_field(
    hasher: &mut Sha256,
    value: Option<&str>,
) -> Result<(), ArchiveError> {
    match value {
        Some(value) => {
            hash_field(hasher, "some")?;
            hash_field(hasher, value)
        }
        None => hash_field(hasher, "none"),
    }
}

fn hash_normalizer(
    hasher: &mut Sha256,
    normalizer: &NormalizerVersion,
) -> Result<(), ArchiveError> {
    hash_field(hasher, &normalizer.name)?;
    hash_field(hasher, &normalizer.semver)?;
    hash_field(hasher, &normalizer.git_rev)?;
    hash_field(hasher, &normalizer.unicode_version)?;
    hash_field(hasher, &normalizer.emoji_data_version)
}

pub(super) fn append_normalizer_frames(
    target: &mut Vec<u8>,
    normalizer: &NormalizerVersion,
) -> Result<(), ArchiveError> {
    append_hash_field_frame(target, &normalizer.name)?;
    append_hash_field_frame(target, &normalizer.semver)?;
    append_hash_field_frame(target, &normalizer.git_rev)?;
    append_hash_field_frame(target, &normalizer.unicode_version)?;
    append_hash_field_frame(target, &normalizer.emoji_data_version)
}

pub(super) fn framed_fields<const N: usize>(values: [&str; N]) -> Result<Vec<u8>, ArchiveError> {
    let mut framed = Vec::new();
    for value in values {
        append_hash_field_frame(&mut framed, value)?;
    }
    Ok(framed)
}

pub(super) fn append_hash_field_frame(
    target: &mut Vec<u8>,
    value: &str,
) -> Result<(), ArchiveError> {
    let len = u64::try_from(value.len()).map_err(|_error| ArchiveError::CountOverflow {
        field: "hash_field_length",
    })?;
    target.extend_from_slice(&len.to_be_bytes());
    target.extend_from_slice(value.as_bytes());
    Ok(())
}

pub(super) fn hash_field(hasher: &mut Sha256, value: &str) -> Result<(), ArchiveError> {
    hash_field_bytes(hasher, value.as_bytes())
}

pub(super) fn hash_field_bytes(hasher: &mut Sha256, value: &[u8]) -> Result<(), ArchiveError> {
    let len = u64::try_from(value.len()).map_err(|_error| ArchiveError::CountOverflow {
        field: "hash_field_length",
    })?;
    hasher.update(len.to_be_bytes());
    hasher.update(value);
    Ok(())
}

pub(super) fn hash_extras_json(
    hasher: &mut Sha256,
    value: &serde_json::Value,
) -> Result<(), ArchiveError> {
    if matches!(value, serde_json::Value::Object(fields) if fields.is_empty()) {
        return hash_field(hasher, "{}");
    }
    hash_field_bytes(
        hasher,
        &super::archive_io::json_bytes(&canonical_json_value(value))?,
    )
}

fn canonical_json_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_json_value).collect::<Vec<_>>())
        }
        serde_json::Value::Object(fields) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = fields.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                if let Some(value) = fields.get(key) {
                    sorted.insert(key.clone(), canonical_json_value(value));
                }
            }
            serde_json::Value::Object(sorted)
        }
        other => other.clone(),
    }
}
