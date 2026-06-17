use sha2::{Digest as _, Sha256};

use super::{
    ARCHIVE_SCHEMA_VERSION, ArchiveCommitContext, ArchiveError, ArchivePostRow, CompletenessClass,
    EmojiProjectionRow, FetchMethod, File, LocalManifestEntry, LocalStore, ManifestMode, Metadata,
    NONCANONICAL_POSTS_DATASET, NamedTempFile, Path, PathBuf, PostDataset, ProfileRecord,
    ProfileSidecarRow, RepoReceipt, RepoReceiptInput, Request, Serialize, TempPath, Write,
    derive_emoji_projection_rows, format_observed_at, hash::hash_field_bytes, hash_post_rows,
    hash_serialized_json,
};

/// Build a content receipt from already-normalized post rows.
///
/// # Errors
///
/// Returns [`ArchiveError`] if any counter or hash length overflows the receipt schema.
pub fn build_repo_receipt(input: RepoReceiptInput<'_>) -> Result<RepoReceipt, ArchiveError> {
    let rows = input.rows;
    let post_rows_hash = hash_post_rows(rows)?;
    let emoji_projection_rows = derive_emoji_projection_rows(rows)?;
    let emoji_projection_hash = hash_emoji_projection_rows(&emoji_projection_rows)?;
    Ok(RepoReceipt {
        observed_at: format_observed_at(input.observed_at),
        did: input.did.to_owned(),
        fetch_method: input.fetch_method,
        completeness_class: input.completeness_class,
        reachable_records_count: input.reachable_records_count,
        reachable_post_records_count: input.reachable_post_records_count,
        archived_post_rows_count: u64::try_from(rows.len()).map_err(|_error| {
            ArchiveError::CountOverflow {
                field: "archived_post_rows_count",
            }
        })?,
        post_decode_error_count: input.post_decode_error_count,
        emoji_posts_count: count_emoji_posts(rows)?,
        emoji_occurrences_count: count_emoji_occurrences(rows)?,
        mst_root_cid: input.mst_root_cid,
        commit_cid: input.commit_cid,
        archive_rows_hash: post_rows_hash.clone(),
        post_rows_hash,
        emoji_projection_hash,
        profile_row_hash: input.profile_row_hash,
        normalizer: input.normalizer,
        repo_commit_signature_verified: false,
        identity_verified: false,
    })
}

/// Hash a profile sidecar row when Stage C extracted one.
///
/// # Errors
///
/// Returns [`ArchiveError`] if the profile row cannot be serialized without loss.
pub fn hash_profile_record(
    profile: Option<&ProfileRecord>,
) -> Result<Option<String>, ArchiveError> {
    profile.map(hash_one_profile_record).transpose()
}

fn hash_one_profile_record(profile: &ProfileRecord) -> Result<String, ArchiveError> {
    let mut hasher = Sha256::new();
    hash_field_bytes(&mut hasher, &json_bytes(&profile_sidecar_row(profile))?)?;
    Ok(hex::encode(hasher.finalize()))
}

fn hash_emoji_projection_rows(rows: &[EmojiProjectionRow]) -> Result<String, ArchiveError> {
    let mut hasher = Sha256::new();
    for row in rows {
        hash_field_bytes(&mut hasher, &json_bytes(row)?)?;
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(super) fn build_commit_metadata(
    rows: &[ArchivePostRow],
    receipt: &RepoReceipt,
    commit_context: &ArchiveCommitContext,
) -> Result<Metadata, ArchiveError> {
    Ok(Metadata {
        run_id: commit_context.run_id.clone(),
        shard: commit_context.shard.clone(),
        file_sequence: commit_context.file_sequence,
        did: receipt.did.clone(),
        dataset: receipt_dataset(receipt).to_owned(),
        row_count: u64::try_from(rows.len())
            .map_err(|_error| ArchiveError::CountOverflow { field: "row_count" })?,
        min_created_at_normalized: min_created_at(rows),
        max_created_at_normalized: max_created_at(rows),
        receipt_hash: hash_serialized_json(receipt)?,
        repo_receipt_path: None,
        normalizer: receipt.normalizer.clone(),
        schema_version: ARCHIVE_SCHEMA_VERSION,
    })
}

pub(super) const fn receipt_dataset(receipt: &RepoReceipt) -> &'static str {
    match (receipt.fetch_method, receipt.completeness_class) {
        (FetchMethod::GetRepo, CompletenessClass::ContentAddressedSnapshot) => {
            PostDataset::RawArchivePosts.as_str()
        }
        (FetchMethod::ListRecords, CompletenessClass::CollectionPaginated) => {
            PostDataset::CollectionPaginatedPosts.as_str()
        }
        (FetchMethod::GetRepo, CompletenessClass::CollectionPaginated)
        | (FetchMethod::ListRecords, CompletenessClass::ContentAddressedSnapshot) => {
            NONCANONICAL_POSTS_DATASET
        }
    }
}

pub(super) struct ProfileSidecarCommitPaths {
    pub object_path: PathBuf,
    pub receipt_path: PathBuf,
    pub manifest_path: PathBuf,
    pub manifest_mode: ManifestMode,
}

fn build_profile_sidecar_metadata(
    did: &str,
    receipt: &RepoReceipt,
    commit_context: &ArchiveCommitContext,
) -> Result<Metadata, ArchiveError> {
    Ok(Metadata {
        run_id: commit_context.run_id.clone(),
        shard: commit_context.shard.clone(),
        file_sequence: commit_context.file_sequence,
        did: did.to_owned(),
        dataset: "raw_profile_sidecar".to_owned(),
        row_count: 1,
        min_created_at_normalized: None,
        max_created_at_normalized: None,
        receipt_hash: hash_serialized_json(receipt)?,
        repo_receipt_path: None,
        normalizer: receipt.normalizer.clone(),
        schema_version: ARCHIVE_SCHEMA_VERSION,
    })
}

pub(super) fn commit_profile_sidecar(
    store: &LocalStore,
    paths: ProfileSidecarCommitPaths,
    profile: &ProfileRecord,
    receipt: &RepoReceipt,
    commit_context: &ArchiveCommitContext,
) -> Result<crate::commit::Artifact, ArchiveError> {
    let mut request = profile_sidecar_request(
        paths.object_path,
        paths.receipt_path,
        paths.manifest_path,
        receipt,
        commit_context,
    )?;
    request.manifest_mode = paths.manifest_mode;
    Ok(store.commit(&request, |file| {
        write_profile_sidecar_json_to_writer(file, profile).map_err(|error| {
            crate::commit::Error::writer(format!("write profile sidecar JSON: {error}"))
        })
    })?)
}

pub(super) fn write_profile_sidecar_temp(
    output_dir: &Path,
    profile: &ProfileRecord,
) -> Result<TempPath, ArchiveError> {
    let mut temp = NamedTempFile::new_in(output_dir)?;
    write_profile_sidecar_json_to_writer(temp.as_file_mut(), profile)?;
    temp.as_file().sync_all()?;
    Ok(temp.into_temp_path())
}

pub(super) fn profile_sidecar_request(
    object_path: PathBuf,
    receipt_path: PathBuf,
    manifest_path: PathBuf,
    receipt: &RepoReceipt,
    commit_context: &ArchiveCommitContext,
) -> Result<Request, ArchiveError> {
    Ok(Request {
        object_path,
        receipt_path,
        manifest_path,
        manifest_mode: ManifestMode::AppendJsonl,
        metadata: build_profile_sidecar_metadata(&receipt.did, receipt, commit_context)?,
    })
}

pub(super) fn local_manifest_from_committed(
    committed: &crate::commit::Artifact,
    receipt: &RepoReceipt,
) -> LocalManifestEntry {
    LocalManifestEntry {
        manifest_format_version: committed.entry.manifest_format_version,
        run_id: committed.entry.run_id.clone(),
        shard: committed.entry.shard.clone(),
        file_sequence: committed.entry.file_sequence,
        did: committed.entry.did.clone(),
        dataset: committed.entry.dataset.clone(),
        local_path: committed.object_path.clone(),
        row_count: committed.entry.row_count,
        bytes: committed.entry.bytes,
        content_hash: committed.entry.content_hash.clone(),
        min_created_at_normalized: committed.entry.min_created_at_normalized.clone(),
        max_created_at_normalized: committed.entry.max_created_at_normalized.clone(),
        receipt_hash: committed.entry.receipt_hash.clone(),
        repo_receipt_path: committed
            .entry
            .repo_receipt_path
            .as_ref()
            .map(PathBuf::from),
        schema_version: committed.entry.schema_version,
        normalizer: receipt.normalizer.clone(),
    }
}

pub(super) fn write_emoji_projection_jsonl(
    path: &Path,
    rows: &[EmojiProjectionRow],
) -> Result<(), ArchiveError> {
    let mut file = File::create(path)?;
    for row in rows {
        serde_json::to_writer(&mut file, row)?;
        file.write_all(b"\n")?;
    }
    file.sync_all()?;
    Ok(())
}

fn write_profile_sidecar_json_to_writer<W>(
    mut writer: W,
    profile: &ProfileRecord,
) -> Result<(), ArchiveError>
where
    W: Write,
{
    serde_json::to_writer_pretty(&mut writer, &profile_sidecar_row(profile))?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn profile_sidecar_row(profile: &ProfileRecord) -> ProfileSidecarRow<'_> {
    ProfileSidecarRow {
        rkey: &profile.rkey,
        cid: &profile.cid,
        record: &profile.record,
    }
}

pub(super) fn extract_emojis(text: &str) -> Vec<String> {
    emoji_normalizer::extract_emoji_sequence(text)
}

fn count_emoji_posts(rows: &[ArchivePostRow]) -> Result<u64, ArchiveError> {
    u64::try_from(
        rows.iter()
            .filter(|row| !row.emoji_sequence.is_empty())
            .count(),
    )
    .map_err(|_error| ArchiveError::CountOverflow {
        field: "emoji_posts_count",
    })
}

fn count_emoji_occurrences(rows: &[ArchivePostRow]) -> Result<u64, ArchiveError> {
    rows.iter().try_fold(0_u64, |accumulator, row| {
        let row_count = u64::try_from(row.emoji_sequence.len()).map_err(|_error| {
            ArchiveError::CountOverflow {
                field: "emoji_occurrences_count",
            }
        })?;
        accumulator
            .checked_add(row_count)
            .ok_or(ArchiveError::CountOverflow {
                field: "emoji_occurrences_count",
            })
    })
}

pub(super) fn json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ArchiveError> {
    super::json::json_bytes(value)
}

pub(super) fn write_json_pretty<T: Serialize>(path: &Path, value: &T) -> Result<(), ArchiveError> {
    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ArchiveFileDigest {
    pub(super) bytes: u64,
    pub(super) sha256: String,
}

pub(super) fn hash_file_for_archive(path: &Path) -> Result<ArchiveFileDigest, ArchiveError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; 65_536].into_boxed_slice();
    loop {
        let read = std::io::Read::read(&mut file, &mut buffer)?;
        if read == 0 {
            break;
        }
        let chunk = buffer.get(..read).ok_or(ArchiveError::CountOverflow {
            field: "archive_file_hash_chunk",
        })?;
        hasher.update(chunk);
        let read_u64 = u64::try_from(read).map_err(|_error| ArchiveError::CountOverflow {
            field: "archive_file_hash_bytes",
        })?;
        bytes = bytes
            .checked_add(read_u64)
            .ok_or(ArchiveError::CountOverflow {
                field: "archive_file_hash_bytes",
            })?;
    }
    Ok(ArchiveFileDigest {
        bytes,
        sha256: hex::encode(hasher.finalize()),
    })
}

fn min_created_at(rows: &[ArchivePostRow]) -> Option<String> {
    rows.iter()
        .filter_map(|row| row.created_at_normalized.as_deref())
        .min()
        .map(ToOwned::to_owned)
}

pub(super) fn update_min_max_created_at(
    min_value: &mut Option<String>,
    max_value: &mut Option<String>,
    value: Option<&str>,
) {
    let Some(value) = value else {
        return;
    };
    if min_value.as_deref().is_none_or(|current| value < current) {
        *min_value = Some(value.to_owned());
    }
    if max_value.as_deref().is_none_or(|current| value > current) {
        *max_value = Some(value.to_owned());
    }
}

fn max_created_at(rows: &[ArchivePostRow]) -> Option<String> {
    rows.iter()
        .filter_map(|row| row.created_at_normalized.as_deref())
        .max()
        .map(ToOwned::to_owned)
}

pub(super) fn record_extras_json(
    record: &jacquard_api::app_bsky::feed::post::Post<smol_str::SmolStr>,
) -> Result<serde_json::Value, ArchiveError> {
    let mut extras = serde_json::Map::new();
    insert_optional_json(&mut extras, "embed", record.embed.as_ref())?;
    insert_optional_json(&mut extras, "facets", record.facets.as_ref())?;
    insert_optional_json(&mut extras, "labels", record.labels.as_ref())?;
    insert_optional_json(&mut extras, "reply", record.reply.as_ref())?;
    insert_optional_json(&mut extras, "tags", record.tags.as_ref())?;
    insert_extra_data_json(&mut extras, record.extra_data.as_ref())?;
    Ok(serde_json::Value::Object(extras))
}

fn insert_optional_json<T: Serialize>(
    target: &mut serde_json::Map<String, serde_json::Value>,
    key: &'static str,
    value: Option<&T>,
) -> Result<(), ArchiveError> {
    if let Some(value) = value {
        target.insert(key.to_owned(), serde_json::to_value(value)?);
    }
    Ok(())
}

fn insert_extra_data_json<T: Serialize>(
    target: &mut serde_json::Map<String, serde_json::Value>,
    value: Option<&std::collections::BTreeMap<smol_str::SmolStr, T>>,
) -> Result<(), ArchiveError> {
    let Some(value) = value else {
        return Ok(());
    };
    for (key, extra_value) in value {
        let key = key.to_string();
        if !target.contains_key(&key) {
            target.insert(key, serde_json::to_value(extra_value)?);
        }
    }
    Ok(())
}
