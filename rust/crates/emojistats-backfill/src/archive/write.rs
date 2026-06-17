use super::{
    ARCHIVE_SCHEMA_VERSION, Arc, ArchiveArtifacts, ArchiveCommitContext, ArchiveError,
    ArchivePostRow, ArchiveStorageConfig, ArrowWriter, CompletenessClass, DateTime, Digest,
    FetchMethod, File, LocalManifestEntry, LocalStore, ManifestMode, Metadata, NamedTempFile,
    NormalizerVersion, PARQUET_BATCH_ROWS, PARTIAL_RECORD_STATUS, POST_COLLECTION, ParsedRepo,
    Path, PathBuf, PostRecord, PostRecordBody, ProfileRecord, RawPartialPostRecord, RepoReceipt,
    Request, Schema, Sha256, TempPath, Utc, Write,
    archive_io::{
        ProfileSidecarCommitPaths, append_hash_field_frame, append_normalizer_frames,
        archive_error_from_derive, archive_schema, build_commit_metadata, commit_profile_sidecar,
        extract_emojis, framed_fields, hash_extras_json, hash_field, hash_field_bytes,
        hash_optional_field, hash_post_row_into, hash_string_slice, json_bytes,
        local_manifest_from_committed, parquet_writer_properties, post_record_batch,
        receipt_dataset, record_extras_json, stable_artifact_stem, stable_object_receipt_path,
        stable_repo_receipt_name, update_min_max_created_at, write_emoji_projection_jsonl,
        write_json_pretty, write_posts_parquet_to_writer,
    },
    borrowed_emoji_projection_rows_for_post, classify_created_at_observed_at,
    commit_backend::ArchiveCommitBackend,
    derive_emoji_projection_rows, format_observed_at, fs, hash_serialized_json,
    promote_temp_idempotent, write_temp_idempotent,
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

/// Write local archive artifacts for one parsed repo.
///
/// # Errors
///
/// Returns [`ArchiveError`] if local filesystem, `Parquet`, `Arrow`, serialization, or
/// resource-count work fails.
pub fn write_archive_artifacts(
    output_dir: &Path,
    did: &str,
    commit_context: &ArchiveCommitContext,
    rows: &[ArchivePostRow],
    profile: Option<&ProfileRecord>,
    receipt: &RepoReceipt,
) -> Result<ArchiveArtifacts, ArchiveError> {
    fs::create_dir_all(output_dir)?;
    let receipt_hash = hash_serialized_json(receipt)?;
    let artifact_stem =
        stable_artifact_stem(did, receipt_dataset(receipt), &receipt.post_rows_hash);
    let parquet_object_path = PathBuf::from(format!("{artifact_stem}.posts.parquet"));
    let receipt_path = output_dir.join(stable_repo_receipt_name(did, &receipt_hash));
    let object_receipt_object_path =
        stable_object_receipt_path(&artifact_stem, &receipt_hash, "posts");
    let manifest_object_path = PathBuf::from(format!("{artifact_stem}.manifest.jsonl"));
    let emoji_projection_stem =
        stable_artifact_stem(did, "emoji_projection", &receipt.emoji_projection_hash);
    let emoji_projection_path = output_dir.join(format!("{emoji_projection_stem}.emoji.jsonl"));
    let profile_stem = stable_artifact_stem(
        did,
        "raw_profile_sidecar",
        receipt.profile_row_hash.as_deref().unwrap_or(&receipt_hash),
    );
    let profile_sidecar_object_path = PathBuf::from(format!("{profile_stem}.profile.json"));
    let profile_sidecar_receipt_object_path =
        stable_object_receipt_path(&profile_stem, &receipt_hash, "profile");
    let profile_sidecar_manifest_object_path =
        PathBuf::from(format!("{profile_stem}.profile.manifest.jsonl"));

    write_temp_idempotent(&receipt_path, |path| write_json_pretty(path, receipt))?;
    let store = LocalStore::new(output_dir);
    let mut commit_metadata = build_commit_metadata(rows, receipt, commit_context)?;
    commit_metadata.repo_receipt_path = Some(
        receipt_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(ArchiveError::CountOverflow {
                field: "repo_receipt_file_name",
            })?
            .to_owned(),
    );
    let commit_request = Request {
        object_path: parquet_object_path,
        receipt_path: object_receipt_object_path,
        manifest_path: manifest_object_path,
        manifest_mode: ManifestMode::AppendJsonl,
        metadata: commit_metadata,
    };
    let committed = store.commit(&commit_request, |file| {
        write_posts_parquet_to_writer(file, rows)
            .map_err(|error| crate::commit::Error::writer(format!("write posts parquet: {error}")))
    })?;
    let emoji_projection_rows =
        derive_emoji_projection_rows(rows).map_err(archive_error_from_derive)?;
    let emoji_rows = u64::try_from(emoji_projection_rows.len()).map_err(|_error| {
        ArchiveError::CountOverflow {
            field: "emoji_rows",
        }
    })?;
    write_temp_idempotent(&emoji_projection_path, |path| {
        write_emoji_projection_jsonl(path, &emoji_projection_rows)
    })?;
    let committed_profile = profile
        .map(|profile| {
            commit_profile_sidecar(
                &store,
                ProfileSidecarCommitPaths {
                    object_path: profile_sidecar_object_path,
                    receipt_path: profile_sidecar_receipt_object_path,
                    manifest_path: profile_sidecar_manifest_object_path,
                    manifest_mode: ManifestMode::AppendJsonl,
                },
                profile,
                receipt,
                commit_context,
            )
        })
        .transpose()?;

    let manifest = local_manifest_from_committed(&committed, receipt);

    Ok(ArchiveArtifacts {
        parquet_path: committed.object_path,
        receipt_path,
        object_receipt_path: committed.receipt_path,
        manifest_path: committed.manifest_path,
        emoji_projection_path,
        profile_sidecar_path: committed_profile
            .as_ref()
            .map(|artifact| artifact.object_path.clone()),
        profile_sidecar_receipt_path: committed_profile
            .as_ref()
            .map(|artifact| artifact.receipt_path.clone()),
        profile_sidecar_manifest_path: committed_profile.map(|artifact| artifact.manifest_path),
        manifest,
        emoji_rows,
    })
}

/// Streaming writer for one repo's archive artifacts.
pub struct StreamingArchiveSink {
    output_dir: PathBuf,
    parquet_temp_path: TempPath,
    emoji_projection_temp_path: TempPath,
    writer: Option<ArrowWriter<File>>,
    schema: Arc<Schema>,
    batch: Vec<ArchivePostRow>,
    rows_hash: Sha256,
    emoji_projection_hash: Sha256,
    archived_post_rows_count: u64,
    emoji_posts_count: u64,
    emoji_occurrences_count: u64,
    emoji_rows: u64,
    min_created_at_normalized: Option<String>,
    max_created_at_normalized: Option<String>,
    normalizer: NormalizerVersion,
    commit_context: ArchiveCommitContext,
    storage_config: ArchiveStorageConfig,
    observed_at: DateTime<Utc>,
    did: String,
    hash_prefix: Vec<u8>,
    hash_after_cid: Vec<u8>,
    hash_public_none: Vec<u8>,
    emoji_file: File,
}

/// Summary fields needed to finish a streaming repo receipt.
#[derive(Debug, Clone)]
pub struct StreamingReceiptInput {
    pub fetch_method: FetchMethod,
    pub completeness_class: CompletenessClass,
    pub reachable_records_count: u64,
    pub reachable_post_records_count: u64,
    pub post_decode_error_count: u64,
    pub profile_row_hash: Option<String>,
    pub mst_root_cid: Option<String>,
    pub commit_cid: Option<String>,
}

impl StreamingArchiveSink {
    /// Create a streaming sink for one repo.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveError`] if local files or the `Parquet` writer cannot be opened.
    pub fn new(
        output_dir: &Path,
        did: &str,
        commit_context: ArchiveCommitContext,
    ) -> Result<Self, ArchiveError> {
        Self::new_with_storage(output_dir, did, commit_context, ArchiveStorageConfig::Local)
    }

    /// Create a streaming sink with an explicit archive storage backend.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveError`] if local files or the `Parquet` writer cannot be opened.
    pub fn new_with_storage(
        output_dir: &Path,
        did: &str,
        commit_context: ArchiveCommitContext,
        storage_config: ArchiveStorageConfig,
    ) -> Result<Self, ArchiveError> {
        fs::create_dir_all(output_dir)?;
        let parquet_temp = NamedTempFile::new_in(output_dir)?;
        let emoji_projection_temp = NamedTempFile::new_in(output_dir)?;
        let parquet_file = parquet_temp.reopen()?;
        let emoji_file = emoji_projection_temp.reopen()?;
        let parquet_temp_path = parquet_temp.into_temp_path();
        let emoji_projection_temp_path = emoji_projection_temp.into_temp_path();
        let schema = archive_schema();
        let normalizer = current_normalizer();
        let writer = ArrowWriter::try_new(
            parquet_file,
            Arc::clone(&schema),
            Some(parquet_writer_properties()?),
        )?;
        let hash_prefix = framed_fields([POST_COLLECTION, did])?;
        let mut hash_after_cid = Vec::new();
        append_normalizer_frames(&mut hash_after_cid, &normalizer)?;
        append_hash_field_frame(&mut hash_after_cid, "none")?;
        let hash_public_none = framed_fields(["none"])?;
        Ok(Self {
            output_dir: output_dir.to_path_buf(),
            parquet_temp_path,
            emoji_projection_temp_path,
            writer: Some(writer),
            schema,
            batch: Vec::with_capacity(PARQUET_BATCH_ROWS),
            rows_hash: Sha256::new(),
            emoji_projection_hash: Sha256::new(),
            archived_post_rows_count: 0,
            emoji_posts_count: 0,
            emoji_occurrences_count: 0,
            emoji_rows: 0,
            min_created_at_normalized: None,
            max_created_at_normalized: None,
            normalizer,
            observed_at: commit_context.observed_at,
            commit_context,
            storage_config,
            did: did.to_owned(),
            hash_prefix,
            hash_after_cid,
            hash_public_none,
            emoji_file,
        })
    }

    #[cfg(test)]
    pub(crate) fn parquet_temp_path(&self) -> &Path {
        &self.parquet_temp_path
    }

    #[cfg(test)]
    pub(crate) fn emoji_projection_temp_path(&self) -> &Path {
        &self.emoji_projection_temp_path
    }

    /// Normalizer version used by this sink.
    #[must_use]
    pub const fn normalizer(&self) -> &NormalizerVersion {
        &self.normalizer
    }

    /// Fixed observation time used for deterministic timestamp classification.
    #[must_use]
    pub const fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }

    /// Write one archive row into the streaming artifacts.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveError`] if hashing, JSONL writing, or `Parquet` batch writing fails.
    pub fn push_row(&mut self, row: ArchivePostRow) -> Result<(), ArchiveError> {
        self.hash_streaming_row(&row)?;
        self.archived_post_rows_count =
            self.archived_post_rows_count
                .checked_add(1)
                .ok_or(ArchiveError::CountOverflow {
                    field: "archived_post_rows_count",
                })?;
        if !row.emoji_sequence.is_empty() {
            self.emoji_posts_count =
                self.emoji_posts_count
                    .checked_add(1)
                    .ok_or(ArchiveError::CountOverflow {
                        field: "emoji_posts_count",
                    })?;
        }
        let row_occurrences = u64::try_from(row.emoji_sequence.len()).map_err(|_error| {
            ArchiveError::CountOverflow {
                field: "emoji_occurrences_count",
            }
        })?;
        self.emoji_occurrences_count = self
            .emoji_occurrences_count
            .checked_add(row_occurrences)
            .ok_or(ArchiveError::CountOverflow {
                field: "emoji_occurrences_count",
            })?;
        update_min_max_created_at(
            &mut self.min_created_at_normalized,
            &mut self.max_created_at_normalized,
            row.created_at_normalized.as_deref(),
        );
        if !row.emoji_sequence.is_empty() {
            self.write_emoji_projection_rows(&row)?;
        }
        self.batch.push(row);
        if self.batch.len() >= PARQUET_BATCH_ROWS {
            self.flush_batch()?;
        }
        Ok(())
    }

    fn hash_streaming_row(&mut self, row: &ArchivePostRow) -> Result<(), ArchiveError> {
        if row.did != self.did
            || row.normalizer != self.normalizer
            || row.account_status.is_some()
            || row.public_content_label.is_some()
        {
            return hash_post_row_into(&mut self.rows_hash, row);
        }
        self.rows_hash.update(&self.hash_prefix);
        hash_field(&mut self.rows_hash, &row.rkey)?;
        hash_field(&mut self.rows_hash, &row.cid)?;
        self.rows_hash.update(&self.hash_after_cid);
        hash_optional_field(&mut self.rows_hash, row.record_status.as_deref())?;
        self.rows_hash.update(&self.hash_public_none);
        hash_optional_field(&mut self.rows_hash, row.created_at_raw.as_deref())?;
        hash_optional_field(&mut self.rows_hash, row.created_at_normalized.as_deref())?;
        hash_field(&mut self.rows_hash, row.created_at_parse_status.as_str())?;
        hash_field(&mut self.rows_hash, &row.text)?;
        hash_string_slice(&mut self.rows_hash, &row.langs)?;
        hash_string_slice(&mut self.rows_hash, &row.emoji_sequence)?;
        hash_extras_json(&mut self.rows_hash, &row.extras_json)
    }

    fn write_emoji_projection_rows(&mut self, row: &ArchivePostRow) -> Result<(), ArchiveError> {
        for projection_row in
            borrowed_emoji_projection_rows_for_post(row).map_err(archive_error_from_derive)?
        {
            let json = json_bytes(&projection_row)?;
            hash_field_bytes(&mut self.emoji_projection_hash, &json)?;
            self.emoji_file.write_all(&json)?;
            self.emoji_file.write_all(b"\n")?;
            self.emoji_rows =
                self.emoji_rows
                    .checked_add(1)
                    .ok_or(ArchiveError::CountOverflow {
                        field: "emoji_rows",
                    })?;
        }

        Ok(())
    }

    /// Finish all artifacts and return the receipt plus artifact paths.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveError`] for filesystem, hash, JSON, or `Parquet` failures.
    pub fn finish(
        mut self,
        input: StreamingReceiptInput,
        profile: Option<&ProfileRecord>,
    ) -> Result<(RepoReceipt, ArchiveArtifacts), ArchiveError> {
        self.finish_stream_files()?;
        let receipt = self.build_streaming_receipt(input);
        let receipt_hash = hash_serialized_json(&receipt)?;
        let dataset = receipt_dataset(&receipt);
        let artifact_stem = stable_artifact_stem(&self.did, dataset, &receipt.post_rows_hash);
        let receipt_path = self
            .output_dir
            .join(stable_repo_receipt_name(&self.did, &receipt_hash));
        write_temp_idempotent(&receipt_path, |path| write_json_pretty(path, &receipt))?;
        let committed_posts =
            self.commit_streaming_posts(&receipt_hash, &artifact_stem, dataset, &receipt_path)?;
        let emoji_stem = stable_artifact_stem(
            &self.did,
            "emoji_projection",
            &receipt.emoji_projection_hash,
        );
        let emoji_projection_path = self.output_dir.join(format!("{emoji_stem}.emoji.jsonl"));
        promote_temp_idempotent(
            self.emoji_projection_temp_path.as_ref(),
            &emoji_projection_path,
        )?;
        let manifest = local_manifest_from_committed(&committed_posts, &receipt);
        let committed_profile = self.commit_backend().commit_profile(
            &self.did,
            profile,
            &receipt,
            &receipt_hash,
            &self.commit_context,
        )?;
        let artifacts = self.into_artifacts(
            manifest,
            committed_posts,
            committed_profile,
            receipt_path,
            emoji_projection_path,
        );
        Ok((receipt, artifacts))
    }

    fn finish_stream_files(&mut self) -> Result<(), ArchiveError> {
        self.flush_batch()?;
        self.writer
            .take()
            .ok_or(ArchiveError::CountOverflow {
                field: "streaming_parquet_writer_missing",
            })?
            .close()?;
        self.emoji_file.sync_all()?;
        Ok(())
    }

    fn build_streaming_receipt(&self, input: StreamingReceiptInput) -> RepoReceipt {
        let post_rows_hash = hex::encode(self.rows_hash.clone().finalize());
        RepoReceipt {
            observed_at: format_observed_at(self.observed_at),
            did: self.did.clone(),
            fetch_method: input.fetch_method,
            completeness_class: input.completeness_class,
            reachable_records_count: input.reachable_records_count,
            reachable_post_records_count: input.reachable_post_records_count,
            archived_post_rows_count: self.archived_post_rows_count,
            post_decode_error_count: input.post_decode_error_count,
            emoji_posts_count: self.emoji_posts_count,
            emoji_occurrences_count: self.emoji_occurrences_count,
            mst_root_cid: input.mst_root_cid,
            commit_cid: input.commit_cid,
            archive_rows_hash: post_rows_hash.clone(),
            post_rows_hash,
            emoji_projection_hash: hex::encode(self.emoji_projection_hash.clone().finalize()),
            profile_row_hash: input.profile_row_hash,
            normalizer: self.normalizer.clone(),
            repo_commit_signature_verified: false,
            identity_verified: false,
        }
    }

    fn commit_streaming_posts(
        &self,
        receipt_hash: &str,
        artifact_stem: &str,
        dataset: &str,
        repo_receipt_path: &Path,
    ) -> Result<crate::commit::Artifact, ArchiveError> {
        let request = Request {
            object_path: PathBuf::from(format!("{artifact_stem}.posts.parquet")),
            receipt_path: stable_object_receipt_path(artifact_stem, receipt_hash, "posts"),
            manifest_path: PathBuf::from(format!("{artifact_stem}.manifest.jsonl")),
            manifest_mode: ManifestMode::AppendJsonl,
            metadata: self.streaming_posts_metadata(receipt_hash, dataset, repo_receipt_path)?,
        };
        self.commit_backend().commit_prepared_posts(
            &request,
            self.parquet_temp_path.as_ref(),
            repo_receipt_path,
        )
    }

    fn streaming_posts_metadata(
        &self,
        receipt_hash: &str,
        dataset: &str,
        repo_receipt_path: &Path,
    ) -> Result<Metadata, ArchiveError> {
        Ok(Metadata {
            run_id: self.commit_context.run_id.clone(),
            shard: self.commit_context.shard.clone(),
            file_sequence: self.commit_context.file_sequence,
            did: self.did.clone(),
            dataset: dataset.to_owned(),
            row_count: self.archived_post_rows_count,
            min_created_at_normalized: self.min_created_at_normalized.clone(),
            max_created_at_normalized: self.max_created_at_normalized.clone(),
            receipt_hash: receipt_hash.to_owned(),
            repo_receipt_path: Some(
                repo_receipt_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or(ArchiveError::CountOverflow {
                        field: "repo_receipt_file_name",
                    })?
                    .to_owned(),
            ),
            normalizer: self.normalizer.clone(),
            schema_version: ARCHIVE_SCHEMA_VERSION,
        })
    }

    fn commit_backend(&self) -> ArchiveCommitBackend<'_> {
        ArchiveCommitBackend::new(&self.output_dir, &self.storage_config)
    }

    fn into_artifacts(
        self,
        manifest: LocalManifestEntry,
        committed_posts: crate::commit::Artifact,
        committed_profile: Option<crate::commit::Artifact>,
        receipt_path: PathBuf,
        emoji_projection_path: PathBuf,
    ) -> ArchiveArtifacts {
        ArchiveArtifacts {
            parquet_path: committed_posts.object_path,
            receipt_path,
            object_receipt_path: committed_posts.receipt_path,
            manifest_path: committed_posts.manifest_path,
            emoji_projection_path,
            profile_sidecar_path: committed_profile
                .as_ref()
                .map(|artifact| artifact.object_path.clone()),
            profile_sidecar_receipt_path: committed_profile
                .as_ref()
                .map(|artifact| artifact.receipt_path.clone()),
            profile_sidecar_manifest_path: committed_profile.map(|artifact| artifact.manifest_path),
            manifest,
            emoji_rows: self.emoji_rows,
        }
    }

    fn flush_batch(&mut self) -> Result<(), ArchiveError> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let batch = post_record_batch(&self.schema, &self.batch)?;
        self.writer
            .as_mut()
            .ok_or(ArchiveError::CountOverflow {
                field: "streaming_parquet_writer_missing",
            })?
            .write(&batch)?;
        self.batch.clear();
        Ok(())
    }
}

impl Drop for StreamingArchiveSink {
    fn drop(&mut self) {
        self.writer.take();
    }
}
