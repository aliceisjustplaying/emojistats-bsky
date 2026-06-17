use super::{
    Arc, ArchiveError, ArchivePostRow, Array, ArrayRef, ArrowWriter, Compression, Cow,
    CreatedAtParseStatus, DataType, Field, File, NormalizerVersion, PARQUET_BATCH_ROWS,
    ParquetRecordBatchReaderBuilder, Path, RecordBatch, Schema, Serialize, StringArray,
    StringBuilder, Write, WriterProperties, ZstdLevel,
};

/// Read every archive post row into memory.
///
/// This is intended for tests and explicitly capped full-load verification paths. Whale-scale
/// derive code should use `ParquetRecordBatchReaderBuilder` directly and stream batches.
///
/// # Errors
///
/// Returns [`ArchiveError`] when the file cannot be read as the expected archive schema, or when
/// JSON-encoded row fields fail to decode.
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
/// Returns [`ArchiveError`] when the batch does not match the archive schema or JSON fields cannot
/// be decoded.
pub fn archive_post_rows_from_record_batch(
    batch: &RecordBatch,
) -> Result<Vec<ArchivePostRow>, ArchiveError> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    append_archive_rows_from_batch(&mut rows, batch)?;
    Ok(rows)
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

fn json_string_array(
    values: impl Iterator<Item = Result<Cow<'static, str>, ArchiveError>>,
) -> Result<ArrayRef, ArchiveError> {
    let mut builder = StringBuilder::new();
    for value in values {
        builder.append_value(value?.as_ref());
    }
    Ok(Arc::new(builder.finish()))
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
