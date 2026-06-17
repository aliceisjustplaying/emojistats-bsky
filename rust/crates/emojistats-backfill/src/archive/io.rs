use sha2::Digest as _;

use super::{
    ARCHIVE_SCHEMA_VERSION, Arc, ArchiveCommitContext, ArchiveError, ArchivePostRow, Array,
    ArrayRef, ArrowWriter, CompletenessClass, Compression, Cow, CreatedAtParseStatus, DataType,
    DeriveError, EmojiProjectionRow, FetchMethod, Field, File, LocalManifestEntry, LocalStore,
    ManifestMode, Metadata, NormalizerVersion, PARQUET_BATCH_ROWS, POST_COLLECTION,
    ParquetRecordBatchReaderBuilder, Path, PathBuf, ProfileRecord, ProfileSidecarRow, RecordBatch,
    RepoReceipt, RepoReceiptInput, Request, Schema, Serialize, Sha256, StringArray, StringBuilder,
    Utc, Write, WriterProperties, ZstdLevel, derive_emoji_projection_rows, format_observed_at,
    hash_serialized_json,
};

/// Read raw archive post rows from the Stage D Parquet shape.
///
/// # Errors
///
/// Returns [`ArchiveError`] when the file cannot be read as the expected archive schema,
/// or when JSON-encoded row fields fail to decode.
/// Read every archive post row into memory.
///
/// This is intended for tests and explicitly capped full-load verification paths. Whale-scale
/// derive code should use `ParquetRecordBatchReaderBuilder` directly and stream batches.
pub fn read_all_archive_post_rows(path: &Path) -> Result<Vec<ArchivePostRow>, ArchiveError> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut rows = Vec::new();
    for batch in reader {
        append_archive_rows_from_batch(&mut rows, &batch?)?;
    }
    Ok(rows)
}

/// Decode one `Parquet` record batch into archive rows.
///
/// # Errors
///
/// Returns [`ArchiveError`] when the batch does not match the archive schema or JSON fields
/// cannot be decoded.
pub fn archive_post_rows_from_record_batch(
    batch: &RecordBatch,
) -> Result<Vec<ArchivePostRow>, ArchiveError> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    append_archive_rows_from_batch(&mut rows, batch)?;
    Ok(rows)
}

/// Build a content receipt from already-normalized post rows.
///
/// # Errors
///
/// Returns [`ArchiveError`] if any counter or hash length overflows the receipt schema.
pub fn build_repo_receipt(input: RepoReceiptInput<'_>) -> Result<RepoReceipt, ArchiveError> {
    let rows = input.rows;
    let post_rows_hash = hash_post_rows(rows)?;
    let emoji_projection_rows =
        derive_emoji_projection_rows(rows).map_err(archive_error_from_derive)?;
    let emoji_projection_hash = hash_emoji_projection_rows(&emoji_projection_rows)?;
    Ok(RepoReceipt {
        observed_at: format_observed_at(Utc::now()),
        fetch_method: FetchMethod::GetRepo,
        completeness_class: CompletenessClass::ContentAddressedSnapshot,
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

pub(super) fn write_posts_parquet_to_writer<W>(
    writer: W,
    rows: &[ArchivePostRow],
) -> Result<(), ArchiveError>
where
    W: Write + Send,
{
    let schema = archive_schema();
    let mut writer = ArrowWriter::try_new(
        writer,
        Arc::clone(&schema),
        Some(parquet_writer_properties()?),
    )?;
    for chunk in rows.chunks(PARQUET_BATCH_ROWS) {
        let batch = post_record_batch(&schema, chunk)?;
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

pub(super) fn archive_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("did", DataType::Utf8, false),
        Field::new("rkey", DataType::Utf8, false),
        Field::new("cid", DataType::Utf8, false),
        Field::new("normalizer_name", DataType::Utf8, false),
        Field::new("normalizer_semver", DataType::Utf8, false),
        Field::new("normalizer_git_rev", DataType::Utf8, false),
        Field::new("normalizer_unicode_version", DataType::Utf8, false),
        Field::new("normalizer_emoji_data_version", DataType::Utf8, false),
        Field::new("account_status", DataType::Utf8, true),
        Field::new("record_status", DataType::Utf8, true),
        Field::new("public_content_label", DataType::Utf8, true),
        Field::new("created_at_raw", DataType::Utf8, true),
        Field::new("created_at_normalized", DataType::Utf8, true),
        Field::new("created_at_parse_status", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("langs_json", DataType::Utf8, false),
        Field::new("emoji_sequence_json", DataType::Utf8, false),
        Field::new("extras_json", DataType::Utf8, false),
    ]))
}

pub(super) fn parquet_writer_properties() -> Result<WriterProperties, ArchiveError> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(1)
                .map_err(|error| ArchiveError::InvalidCompression(error.to_string()))?,
        ))
        .build())
}

pub(super) fn post_record_batch(
    schema: &Arc<Schema>,
    rows: &[ArchivePostRow],
) -> Result<RecordBatch, ArchiveError> {
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.did.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.rkey.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.cid.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.normalizer.name.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.normalizer.semver.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.normalizer.git_rev.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|row| row.normalizer.unicode_version.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|row| row.normalizer.emoji_data_version.as_str()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.account_status.as_deref()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.record_status.as_deref()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.public_content_label.as_deref()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.created_at_raw.as_deref()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.created_at_normalized.as_deref()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.created_at_parse_status.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.text.as_str()),
            )),
            json_string_array(rows.iter().map(|row| json_string_slice(&row.langs)))?,
            json_string_array(
                rows.iter()
                    .map(|row| json_string_slice(&row.emoji_sequence)),
            )?,
            json_string_array(rows.iter().map(|row| extras_json_string(&row.extras_json)))?,
        ],
    )?)
}

fn append_archive_rows_from_batch(
    rows: &mut Vec<ArchivePostRow>,
    batch: &RecordBatch,
) -> Result<(), ArchiveError> {
    let did = string_column(batch, "did")?;
    let rkey = string_column(batch, "rkey")?;
    let cid = string_column(batch, "cid")?;
    let normalizer_name = string_column(batch, "normalizer_name")?;
    let normalizer_semver = string_column(batch, "normalizer_semver")?;
    let normalizer_git_rev = string_column(batch, "normalizer_git_rev")?;
    let normalizer_unicode_version = string_column(batch, "normalizer_unicode_version")?;
    let normalizer_emoji_data_version = string_column(batch, "normalizer_emoji_data_version")?;
    let account_status = string_column(batch, "account_status")?;
    let record_status = string_column(batch, "record_status")?;
    let public_content_label = string_column(batch, "public_content_label")?;
    let created_at_raw = string_column(batch, "created_at_raw")?;
    let created_at_normalized = string_column(batch, "created_at_normalized")?;
    let created_at_parse_status = string_column(batch, "created_at_parse_status")?;
    let text = string_column(batch, "text")?;
    let langs_json = string_column(batch, "langs_json")?;
    let emoji_sequence_json = string_column(batch, "emoji_sequence_json")?;
    let extras_json = string_column(batch, "extras_json")?;

    for row_index in 0..batch.num_rows() {
        rows.push(ArchivePostRow {
            did: required_string(did, row_index, "did")?.to_owned(),
            rkey: required_string(rkey, row_index, "rkey")?.to_owned(),
            cid: required_string(cid, row_index, "cid")?.to_owned(),
            normalizer: NormalizerVersion {
                name: required_string(normalizer_name, row_index, "normalizer_name")?.to_owned(),
                semver: required_string(normalizer_semver, row_index, "normalizer_semver")?
                    .to_owned(),
                git_rev: required_string(normalizer_git_rev, row_index, "normalizer_git_rev")?
                    .to_owned(),
                unicode_version: required_string(
                    normalizer_unicode_version,
                    row_index,
                    "normalizer_unicode_version",
                )?
                .to_owned(),
                emoji_data_version: required_string(
                    normalizer_emoji_data_version,
                    row_index,
                    "normalizer_emoji_data_version",
                )?
                .to_owned(),
            },
            account_status: optional_string(account_status, row_index),
            record_status: optional_string(record_status, row_index),
            public_content_label: optional_string(public_content_label, row_index),
            created_at_raw: optional_string(created_at_raw, row_index),
            created_at_normalized: optional_string(created_at_normalized, row_index),
            created_at_parse_status: parse_created_at_parse_status(required_string(
                created_at_parse_status,
                row_index,
                "created_at_parse_status",
            )?)?,
            text: required_string(text, row_index, "text")?.to_owned(),
            langs: serde_json::from_str(required_string(langs_json, row_index, "langs_json")?)?,
            emoji_sequence: serde_json::from_str(required_string(
                emoji_sequence_json,
                row_index,
                "emoji_sequence_json",
            )?)?,
            extras_json: serde_json::from_str(required_string(
                extras_json,
                row_index,
                "extras_json",
            )?)?,
        });
    }

    Ok(())
}

fn string_column<'a>(
    batch: &'a RecordBatch,
    column: &'static str,
) -> Result<&'a StringArray, ArchiveError> {
    let index = batch
        .schema()
        .index_of(column)
        .map_err(|_error| ArchiveError::InvalidParquetColumn { column })?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(ArchiveError::InvalidParquetColumn { column })
}

fn required_string<'a>(
    array: &'a StringArray,
    row_index: usize,
    column: &'static str,
) -> Result<&'a str, ArchiveError> {
    if array.is_null(row_index) {
        Err(ArchiveError::UnexpectedParquetNull { column })
    } else {
        Ok(array.value(row_index))
    }
}

fn optional_string(array: &StringArray, row_index: usize) -> Option<String> {
    if array.is_null(row_index) {
        None
    } else {
        Some(array.value(row_index).to_owned())
    }
}

fn parse_created_at_parse_status(value: &str) -> Result<CreatedAtParseStatus, ArchiveError> {
    match value {
        "valid" => Ok(CreatedAtParseStatus::Valid),
        "missing" => Ok(CreatedAtParseStatus::Missing),
        "invalid" => Ok(CreatedAtParseStatus::Invalid),
        "future" => Ok(CreatedAtParseStatus::Future),
        _ => Err(ArchiveError::InvalidParquetValue {
            column: "created_at_parse_status",
            value: value.to_owned(),
        }),
    }
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
        dataset: "raw_archive_posts".to_owned(),
        row_count: u64::try_from(rows.len())
            .map_err(|_error| ArchiveError::CountOverflow { field: "row_count" })?,
        min_created_at_normalized: min_created_at(rows),
        max_created_at_normalized: max_created_at(rows),
        receipt_hash: hash_serialized_json(receipt)?,
        normalizer: receipt.normalizer.clone(),
        schema_version: ARCHIVE_SCHEMA_VERSION,
    })
}

fn build_profile_sidecar_metadata(
    receipt: &RepoReceipt,
    commit_context: &ArchiveCommitContext,
) -> Result<Metadata, ArchiveError> {
    Ok(Metadata {
        run_id: commit_context.run_id.clone(),
        shard: commit_context.shard.clone(),
        file_sequence: commit_context.file_sequence,
        dataset: "raw_profile_sidecar".to_owned(),
        row_count: 1,
        min_created_at_normalized: None,
        max_created_at_normalized: None,
        receipt_hash: hash_serialized_json(receipt)?,
        normalizer: receipt.normalizer.clone(),
        schema_version: ARCHIVE_SCHEMA_VERSION,
    })
}

pub(super) fn commit_profile_sidecar(
    store: &LocalStore,
    object_path: PathBuf,
    receipt_path: PathBuf,
    manifest_path: PathBuf,
    profile: &ProfileRecord,
    receipt: &RepoReceipt,
    commit_context: &ArchiveCommitContext,
) -> Result<crate::commit::Artifact, ArchiveError> {
    let request = profile_sidecar_request(
        object_path,
        receipt_path,
        manifest_path,
        receipt,
        commit_context,
    )?;
    Ok(store.commit(&request, |file| {
        write_profile_sidecar_json_to_writer(file, profile).map_err(|error| {
            crate::commit::Error::writer(format!("write profile sidecar JSON: {error}"))
        })
    })?)
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
        metadata: build_profile_sidecar_metadata(receipt, commit_context)?,
    })
}

pub(super) fn local_manifest_from_committed(
    committed: &crate::commit::Artifact,
    receipt: &RepoReceipt,
) -> LocalManifestEntry {
    LocalManifestEntry {
        run_id: committed.entry.run_id.clone(),
        shard: committed.entry.shard.clone(),
        file_sequence: committed.entry.file_sequence,
        dataset: committed.entry.dataset.clone(),
        local_path: committed.object_path.clone(),
        row_count: committed.entry.row_count,
        bytes: committed.entry.bytes,
        content_hash: committed.entry.content_hash.clone(),
        min_created_at_normalized: committed.entry.min_created_at_normalized.clone(),
        max_created_at_normalized: committed.entry.max_created_at_normalized.clone(),
        receipt_hash: committed.entry.receipt_hash.clone(),
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

pub(super) const fn archive_error_from_derive(error: DeriveError) -> ArchiveError {
    match error {
        DeriveError::CountOverflow { field } => ArchiveError::CountOverflow { field },
        DeriveError::RowCountMismatch { .. } => ArchiveError::CountOverflow {
            field: "derive_row_count_mismatch",
        },
    }
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

fn json_string_array(
    values: impl Iterator<Item = Result<Cow<'static, str>, ArchiveError>>,
) -> Result<ArrayRef, ArchiveError> {
    let mut builder = StringBuilder::new();
    for value in values {
        builder.append_value(value?.as_ref());
    }
    Ok(Arc::new(builder.finish()))
}

pub(super) fn json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ArchiveError> {
    Ok(serde_json::to_vec(value)?)
}

fn json_string<T: Serialize>(value: &T) -> Result<String, ArchiveError> {
    Ok(serde_json::to_string(value)?)
}

fn json_string_slice(value: &[String]) -> Result<Cow<'static, str>, ArchiveError> {
    if value.is_empty() {
        return Ok(Cow::Borrowed("[]"));
    }
    Ok(Cow::Owned(json_string(&value)?))
}

fn extras_json_string(value: &serde_json::Value) -> Result<Cow<'static, str>, ArchiveError> {
    if matches!(value, serde_json::Value::Object(fields) if fields.is_empty()) {
        return Ok(Cow::Borrowed("{}"));
    }
    Ok(Cow::Owned(stable_rust_json(value)?))
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

const ARCHIVE_OBJECT_ENCODING_ID: &str = "archive_object_v2";

pub(super) fn stable_artifact_stem(did: &str, dataset: &str, content_hash: &str) -> String {
    format!(
        "{}.{}.{}.{}",
        safe_file_component(did),
        safe_file_component(dataset),
        ARCHIVE_OBJECT_ENCODING_ID,
        content_hash
    )
}

pub(super) fn stable_repo_receipt_name(did: &str, receipt_hash: &str) -> String {
    format!("{}.{}.receipt.json", safe_file_component(did), receipt_hash)
}

fn safe_file_component(value: &str) -> String {
    let mut safe = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            safe.push(ch);
        } else {
            safe.push('_');
        }
    }
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    let digest = hex::encode(hasher.finalize());
    safe.push_str("__");
    safe.extend(digest.chars().take(16));
    safe
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
    hash_field_bytes(hasher, &json_bytes(&canonical_json_value(value))?)
}

fn stable_rust_json(value: &serde_json::Value) -> Result<String, ArchiveError> {
    Ok(serde_json::to_string(&canonical_json_value(value))?)
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
