use super::{
    ArchiveArtifacts, ArchiveCommitContext, ArchiveError, ArchivePostRow, LocalManifestEntry,
    LocalStore, ManifestMode, Path, PathBuf, ProfileRecord, RepoReceipt, Request,
    archive_io::{
        ProfileSidecarCommitPaths, build_commit_metadata, commit_profile_sidecar,
        local_manifest_from_committed, receipt_dataset, write_emoji_projection_jsonl,
        write_json_pretty,
    },
    derive_emoji_projection_rows, fs, hash_serialized_json,
    naming::{
        stable_artifact_stem, stable_manifest_path, stable_object_receipt_path,
        stable_repo_receipt_name,
    },
    parquet::write_posts_parquet_to_writer,
    write_temp_idempotent,
};

/// Write local fixture archive artifacts for one parsed repo.
///
/// # Errors
///
/// Returns [`ArchiveError`] if local filesystem, `Parquet`, `Arrow`, serialization, or
/// resource-count work fails.
pub fn write_local_archive_artifacts(
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
    let manifest_object_path = stable_manifest_path(&commit_context.run_id, &commit_context.shard);
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
        stable_manifest_path(&commit_context.run_id, &commit_context.shard);

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
    let emoji_projection_rows = derive_emoji_projection_rows(rows)?;
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

    let manifest: LocalManifestEntry = local_manifest_from_committed(&committed, receipt);

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
