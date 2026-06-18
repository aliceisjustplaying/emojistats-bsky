use sha2::{Digest as _, Sha256};

use super::{
    ARCHIVE_SCHEMA_VERSION, Arc, ArchiveArtifacts, ArchiveCommitContext, ArchiveError,
    ArchivePostRow, ArchiveStorageConfig, ArrowWriter, CompletenessClass, DateTime, FetchMethod,
    File, LocalManifestEntry, ManifestMode, Metadata, NamedTempFile, NormalizerVersion,
    PARQUET_BATCH_ROWS, Path, PathBuf, ProfileRecord, RepoReceipt, Request, Schema, TempPath, Utc,
    archive_io::{
        local_manifest_from_committed, receipt_dataset, update_min_max_created_at,
        write_json_pretty,
    },
    commit_backend::ArchiveCommitBackend,
    format_observed_at, fs,
    hash::hash_post_row_into,
    hash_serialized_json,
    naming::{
        stable_artifact_stem, stable_manifest_path, stable_object_receipt_path,
        stable_repo_receipt_name,
    },
    parquet::{archive_schema, parquet_writer_properties, post_record_batch},
    projection_writer::StreamingProjectionWriter,
    promote_temp_idempotent,
    row::current_normalizer,
    write_temp_idempotent,
};

/// Streaming writer for one repo's archive artifacts.
pub struct StreamingArchiveSink {
    output_dir: PathBuf,
    parquet_temp_path: TempPath,
    emoji_projection_temp_path: TempPath,
    writer: Option<ArrowWriter<File>>,
    schema: Arc<Schema>,
    batch: Vec<ArchivePostRow>,
    rows_hash: Sha256,
    projection_writer: StreamingProjectionWriter,
    archived_post_rows_count: u64,
    emoji_posts_count: u64,
    emoji_occurrences_count: u64,
    min_created_at_normalized: Option<String>,
    max_created_at_normalized: Option<String>,
    normalizer: NormalizerVersion,
    commit_context: ArchiveCommitContext,
    storage_config: ArchiveStorageConfig,
    observed_at: DateTime<Utc>,
    did: String,
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
        Ok(Self {
            output_dir: output_dir.to_path_buf(),
            parquet_temp_path,
            emoji_projection_temp_path,
            writer: Some(writer),
            schema,
            batch: Vec::with_capacity(PARQUET_BATCH_ROWS),
            rows_hash: Sha256::new(),
            projection_writer: StreamingProjectionWriter::new(emoji_file),
            archived_post_rows_count: 0,
            emoji_posts_count: 0,
            emoji_occurrences_count: 0,
            min_created_at_normalized: None,
            max_created_at_normalized: None,
            normalizer,
            observed_at: commit_context.observed_at,
            commit_context,
            storage_config,
            did: did.to_owned(),
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
            self.projection_writer.write_row(&row)?;
        }
        self.batch.push(row);
        if self.batch.len() >= PARQUET_BATCH_ROWS {
            self.flush_batch()?;
        }
        Ok(())
    }

    fn hash_streaming_row(&mut self, row: &ArchivePostRow) -> Result<(), ArchiveError> {
        hash_post_row_into(&mut self.rows_hash, row)
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
        self.projection_writer.sync()?;
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
            emoji_projection_hash: self.projection_writer.hash(),
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
            manifest_path: stable_manifest_path(
                &self.commit_context.run_id,
                &self.commit_context.shard,
            ),
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
            emoji_rows: self.projection_writer.rows(),
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
