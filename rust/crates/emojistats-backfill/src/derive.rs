//! Derive-lane DTOs for turning committed archive manifests into `ClickHouse` loads.

use sha2::{Digest, Sha256};

use crate::archive::{ArchivePostRow, EmojiProjectionRow, LocalManifestEntry};

/// Source marker for rows produced by the v2 backfill derive lane.
pub const BACKFILL_DERIVE_SOURCE: &str = "backfill-v2-derive";

/// Minimal identity for a committed archive manifest entry.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeriveManifestIdentity {
    pub run_id: String,
    pub shard: String,
    pub file_sequence: u64,
    pub dataset: String,
    pub content_hash: String,
    pub receipt_hash: String,
    pub schema_version: u16,
}

/// Counter input for the total-post path that cannot be reconstructed from emoji rows.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TotalPostCounterInput {
    pub source: String,
    pub run_id: String,
    pub shard: String,
    pub file_sequence: u64,
    pub receipt_hash: String,
    pub posts_processed: u64,
    pub posts_with_emojis: u64,
    pub emoji_occurrences: u64,
    pub min_created_at_normalized: Option<String>,
    pub max_created_at_normalized: Option<String>,
}

/// One committed archive manifest plus the verified archive rows it names.
#[derive(Debug, Clone, Copy)]
pub struct DeriveBatchInput<'a> {
    pub manifest: &'a LocalManifestEntry,
    pub archive_rows: &'a [ArchivePostRow],
}

/// `ClickHouse`-ready batch payload. Network insert/retry policy stays outside this module.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClickHouseDeriveBatch {
    pub manifest_identity: DeriveManifestIdentity,
    pub dedupe_token: String,
    pub emoji_rows: Vec<EmojiProjectionRow>,
    pub total_post_counter: TotalPostCounterInput,
}

/// Derive-lane failures before any `ClickHouse` network write is attempted.
#[derive(Debug, thiserror::Error)]
pub enum DeriveError {
    #[error(
        "manifest row_count {manifest_rows} did not match verified archive row count {archive_rows}"
    )]
    RowCountMismatch {
        manifest_rows: u64,
        archive_rows: u64,
    },
    #[error("resource counter overflow: {field}")]
    CountOverflow { field: &'static str },
}

/// Build the DTOs a `ClickHouse` insert lane needs from one committed manifest entry.
///
/// # Errors
///
/// Returns [`DeriveError`] if manifest row counts do not match verified rows or a counter
/// exceeds `u64`.
pub fn derive_clickhouse_batch(
    input: DeriveBatchInput<'_>,
) -> Result<ClickHouseDeriveBatch, DeriveError> {
    validate_manifest_row_count(input.manifest, input.archive_rows)?;
    let manifest_identity = manifest_identity(input.manifest);
    let emoji_rows = derive_emoji_projection_rows(input.archive_rows)?;
    let total_post_counter = total_post_counter_input(input.manifest, input.archive_rows)?;
    let dedupe_token = derive_dedupe_token(&manifest_identity, &emoji_rows, &total_post_counter)?;

    Ok(ClickHouseDeriveBatch {
        manifest_identity,
        dedupe_token,
        emoji_rows,
        total_post_counter,
    })
}

/// Extract the stable manifest identity used by derive ledgers.
#[must_use]
pub fn manifest_identity(manifest: &LocalManifestEntry) -> DeriveManifestIdentity {
    DeriveManifestIdentity {
        run_id: manifest.run_id.clone(),
        shard: manifest.shard.clone(),
        file_sequence: manifest.file_sequence,
        dataset: manifest.dataset.clone(),
        content_hash: manifest.content_hash.clone(),
        receipt_hash: manifest.receipt_hash.clone(),
        schema_version: manifest.schema_version,
    }
}

/// Compute the idempotent `ClickHouse` insert dedupe token for a derived batch.
///
/// # Errors
///
/// Returns [`DeriveError`] if any framed hash field length exceeds `u64`.
pub fn derive_dedupe_token(
    identity: &DeriveManifestIdentity,
    emoji_rows: &[EmojiProjectionRow],
    total_post_counter: &TotalPostCounterInput,
) -> Result<String, DeriveError> {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, "emojistats-backfill-derive-v1")?;
    hash_manifest_identity(&mut hasher, identity)?;
    for row in emoji_rows {
        hash_emoji_row(&mut hasher, row)?;
    }
    hash_total_post_counter(&mut hasher, total_post_counter)?;
    Ok(format!("derive:{}", hex::encode(hasher.finalize())))
}

/// Build the non-emoji total-post counter input for one manifest batch.
///
/// # Errors
///
/// Returns [`DeriveError`] if an archive-row count exceeds `u64`.
pub fn total_post_counter_input(
    manifest: &LocalManifestEntry,
    rows: &[ArchivePostRow],
) -> Result<TotalPostCounterInput, DeriveError> {
    Ok(TotalPostCounterInput {
        source: BACKFILL_DERIVE_SOURCE.to_owned(),
        run_id: manifest.run_id.clone(),
        shard: manifest.shard.clone(),
        file_sequence: manifest.file_sequence,
        receipt_hash: manifest.receipt_hash.clone(),
        posts_processed: count_rows(rows)?,
        posts_with_emojis: count_posts_with_emojis(rows)?,
        emoji_occurrences: count_emoji_occurrences(rows)?,
        min_created_at_normalized: manifest.min_created_at_normalized.clone(),
        max_created_at_normalized: manifest.max_created_at_normalized.clone(),
    })
}

fn validate_manifest_row_count(
    manifest: &LocalManifestEntry,
    rows: &[ArchivePostRow],
) -> Result<(), DeriveError> {
    let archive_rows = count_rows(rows)?;
    if manifest.row_count == archive_rows {
        Ok(())
    } else {
        Err(DeriveError::RowCountMismatch {
            manifest_rows: manifest.row_count,
            archive_rows,
        })
    }
}

fn derive_emoji_projection_rows(
    rows: &[ArchivePostRow],
) -> Result<Vec<EmojiProjectionRow>, DeriveError> {
    let mut projected = Vec::new();
    for row in rows {
        projected.extend(emoji_projection_rows(row)?);
    }
    Ok(projected)
}

fn emoji_projection_rows(row: &ArchivePostRow) -> Result<Vec<EmojiProjectionRow>, DeriveError> {
    emoji_projection_rows_for_post(row)
}

/// Derive compact emoji projection rows for one archive post row.
///
/// # Errors
///
/// Returns [`DeriveError`] if occurrence counters overflow.
pub fn emoji_projection_rows_for_post(
    row: &ArchivePostRow,
) -> Result<Vec<EmojiProjectionRow>, DeriveError> {
    let mut rows = Vec::new();
    for emoji in &row.emoji_sequence {
        if let Some(existing) = rows
            .iter_mut()
            .find(|candidate: &&mut EmojiProjectionRow| candidate.emoji == *emoji)
        {
            existing.occurrences =
                existing
                    .occurrences
                    .checked_add(1)
                    .ok_or(DeriveError::CountOverflow {
                        field: "emoji_occurrences",
                    })?;
        } else {
            rows.push(EmojiProjectionRow {
                did: row.did.clone(),
                rkey: row.rkey.clone(),
                created_at_normalized: row.created_at_normalized.clone(),
                emoji: emoji.clone(),
                occurrences: 1,
                langs: row.langs.clone(),
            });
        }
    }

    Ok(rows)
}

fn count_rows(rows: &[ArchivePostRow]) -> Result<u64, DeriveError> {
    u64::try_from(rows.len()).map_err(|_error| DeriveError::CountOverflow { field: "rows" })
}

fn count_posts_with_emojis(rows: &[ArchivePostRow]) -> Result<u64, DeriveError> {
    u64::try_from(
        rows.iter()
            .filter(|row| !row.emoji_sequence.is_empty())
            .count(),
    )
    .map_err(|_error| DeriveError::CountOverflow {
        field: "posts_with_emojis",
    })
}

fn count_emoji_occurrences(rows: &[ArchivePostRow]) -> Result<u64, DeriveError> {
    rows.iter().try_fold(0_u64, |accumulator, row| {
        let row_count = u64::try_from(row.emoji_sequence.len()).map_err(|_error| {
            DeriveError::CountOverflow {
                field: "emoji_occurrences",
            }
        })?;
        accumulator
            .checked_add(row_count)
            .ok_or(DeriveError::CountOverflow {
                field: "emoji_occurrences",
            })
    })
}

fn hash_manifest_identity(
    hasher: &mut Sha256,
    identity: &DeriveManifestIdentity,
) -> Result<(), DeriveError> {
    hash_field(hasher, &identity.run_id)?;
    hash_field(hasher, &identity.shard)?;
    hash_u64(hasher, identity.file_sequence);
    hash_field(hasher, &identity.dataset)?;
    hash_field(hasher, &identity.content_hash)?;
    hash_field(hasher, &identity.receipt_hash)?;
    hash_u16(hasher, identity.schema_version);
    Ok(())
}

fn hash_emoji_row(hasher: &mut Sha256, row: &EmojiProjectionRow) -> Result<(), DeriveError> {
    hash_field(hasher, &row.did)?;
    hash_field(hasher, &row.rkey)?;
    hash_optional_field(hasher, row.created_at_normalized.as_deref())?;
    hash_field(hasher, &row.emoji)?;
    hash_u64(hasher, row.occurrences);
    for lang in &row.langs {
        hash_field(hasher, lang)?;
    }
    hash_field(hasher, "")
}

fn hash_total_post_counter(
    hasher: &mut Sha256,
    counter: &TotalPostCounterInput,
) -> Result<(), DeriveError> {
    hash_field(hasher, &counter.source)?;
    hash_field(hasher, &counter.run_id)?;
    hash_field(hasher, &counter.shard)?;
    hash_u64(hasher, counter.file_sequence);
    hash_field(hasher, &counter.receipt_hash)?;
    hash_u64(hasher, counter.posts_processed);
    hash_u64(hasher, counter.posts_with_emojis);
    hash_u64(hasher, counter.emoji_occurrences);
    hash_optional_field(hasher, counter.min_created_at_normalized.as_deref())?;
    hash_optional_field(hasher, counter.max_created_at_normalized.as_deref())
}

fn hash_optional_field(hasher: &mut Sha256, value: Option<&str>) -> Result<(), DeriveError> {
    match value {
        Some(value) => {
            hash_field(hasher, "some")?;
            hash_field(hasher, value)
        }
        None => hash_field(hasher, "none"),
    }
}

fn hash_field(hasher: &mut Sha256, value: &str) -> Result<(), DeriveError> {
    let len = u64::try_from(value.len()).map_err(|_error| DeriveError::CountOverflow {
        field: "hash_field_length",
    })?;
    hash_u64(hasher, len);
    hasher.update(value.as_bytes());
    Ok(())
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_be_bytes());
}

fn hash_u16(hasher: &mut Sha256, value: u16) {
    hasher.update(value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        DeriveBatchInput, DeriveError, derive_clickhouse_batch, derive_dedupe_token,
        manifest_identity,
    };
    use crate::archive::{
        ArchivePostRow, CreatedAtParseStatus, LocalManifestEntry, NormalizerVersion,
    };

    fn normalizer() -> NormalizerVersion {
        NormalizerVersion {
            name: "emoji-normalizer".to_owned(),
            semver: "0.1.0".to_owned(),
            git_rev: "test".to_owned(),
            unicode_version: "16.0".to_owned(),
            emoji_data_version: "16.0".to_owned(),
        }
    }

    fn row(rkey: &str, emojis: &[&str]) -> ArchivePostRow {
        ArchivePostRow {
            did: "did:plc:test".to_owned(),
            rkey: rkey.to_owned(),
            cid: format!("bafy-{rkey}"),
            normalizer: normalizer(),
            account_status: None,
            record_status: None,
            public_content_label: None,
            created_at_raw: Some("2026-06-15T00:00:00Z".to_owned()),
            created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
            created_at_parse_status: CreatedAtParseStatus::Valid,
            text: "hello".to_owned(),
            langs: vec!["en".to_owned(), "ja".to_owned()],
            emoji_sequence: emojis.iter().map(|emoji| (*emoji).to_owned()).collect(),
            extras_json: serde_json::json!({}),
        }
    }

    fn manifest(row_count: u64) -> LocalManifestEntry {
        LocalManifestEntry {
            run_id: "run-1".to_owned(),
            shard: "shard0".to_owned(),
            file_sequence: 7,
            dataset: "raw_archive_posts".to_owned(),
            local_path: PathBuf::from("/tmp/archive.parquet"),
            row_count,
            bytes: 123,
            content_hash: "content-hash".to_owned(),
            min_created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
            max_created_at_normalized: Some("2026-06-15T01:00:00Z".to_owned()),
            receipt_hash: "receipt-hash".to_owned(),
            schema_version: 1,
            normalizer: normalizer(),
        }
    }

    #[test]
    fn derives_emoji_rows_and_total_post_counter() {
        let rows = [row("a", &["✅", "✅", "🔥"]), row("b", &[])];
        let batch = derive_clickhouse_batch(DeriveBatchInput {
            manifest: &manifest(2),
            archive_rows: &rows,
        })
        .expect("derive batch");

        assert_eq!(batch.emoji_rows.len(), 2);
        let first = batch
            .emoji_rows
            .first()
            .expect("first emoji row should exist");
        let second = batch
            .emoji_rows
            .get(1)
            .expect("second emoji row should exist");
        assert_eq!(first.emoji, "✅");
        assert_eq!(first.occurrences, 2);
        assert_eq!(second.emoji, "🔥");
        assert_eq!(batch.total_post_counter.posts_processed, 2);
        assert_eq!(batch.total_post_counter.posts_with_emojis, 1);
        assert_eq!(batch.total_post_counter.emoji_occurrences, 3);
    }

    #[test]
    fn dedupe_token_is_stable_and_payload_sensitive() {
        let rows = [row("a", &["✅"])];
        let batch = derive_clickhouse_batch(DeriveBatchInput {
            manifest: &manifest(1),
            archive_rows: &rows,
        })
        .expect("derive batch");
        let same_token = derive_dedupe_token(
            &batch.manifest_identity,
            &batch.emoji_rows,
            &batch.total_post_counter,
        )
        .expect("dedupe token");

        let changed_rows = [row("b", &["✅"])];
        let changed = derive_clickhouse_batch(DeriveBatchInput {
            manifest: &manifest(1),
            archive_rows: &changed_rows,
        })
        .expect("changed derive batch");

        assert_eq!(batch.dedupe_token, same_token);
        assert_ne!(batch.dedupe_token, changed.dedupe_token);
    }

    #[test]
    fn manifest_row_count_must_match_verified_rows() {
        let rows = [row("a", &["✅"])];
        let error = derive_clickhouse_batch(DeriveBatchInput {
            manifest: &manifest(2),
            archive_rows: &rows,
        })
        .expect_err("row count mismatch");

        assert!(matches!(
            error,
            DeriveError::RowCountMismatch {
                manifest_rows: 2,
                archive_rows: 1
            }
        ));
    }

    #[test]
    fn manifest_identity_omits_local_path() {
        let first = manifest_identity(&manifest(1));
        let mut second_manifest = manifest(1);
        second_manifest.local_path = PathBuf::from("/different/local/path.parquet");
        let second = manifest_identity(&second_manifest);

        assert_eq!(first, second);
    }
}
