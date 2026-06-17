use super::{
    ArchiveCommitContext, ArchiveError, ArchiveStorageConfig, LocalStore, ManifestMode, Path,
    PathBuf, ProfileRecord, RepoReceipt, Request, StorageBoxArchiveConfig,
    StorageBoxRcloneArchiveConfig,
    archive_io::{ProfileSidecarCommitPaths, commit_profile_sidecar, profile_sidecar_request},
    naming::{stable_artifact_stem, stable_object_receipt_path},
};

/// Selected archive commit backend for local staging, final exposure, and remote mirroring.
pub(super) struct ArchiveCommitBackend<'a> {
    output_dir: &'a Path,
    storage_config: &'a ArchiveStorageConfig,
}

impl<'a> ArchiveCommitBackend<'a> {
    pub(super) const fn new(
        output_dir: &'a Path,
        storage_config: &'a ArchiveStorageConfig,
    ) -> Self {
        Self {
            output_dir,
            storage_config,
        }
    }

    pub(super) fn commit_prepared_posts(
        &self,
        request: &Request,
        temp_path: &Path,
        repo_receipt_path: &Path,
    ) -> Result<crate::commit::Artifact, ArchiveError> {
        match self.storage_config {
            ArchiveStorageConfig::Local => {
                let store = LocalStore::new(self.output_dir);
                store
                    .commit_prepared_temp(request, temp_path)
                    .map_err(Into::into)
            }
            ArchiveStorageConfig::StorageBoxSsh(config) => {
                let mut remote_request = request.clone();
                remote_request.manifest_mode = ManifestMode::AppendJsonl;
                commit_file_to_storage_box(
                    config,
                    &remote_request,
                    temp_path,
                    Some(repo_receipt_path),
                )?;
                let store = LocalStore::new(self.output_dir);
                let mut local_request = request.clone();
                local_request.manifest_mode = ManifestMode::Skip;
                store
                    .commit_prepared_temp(&local_request, temp_path)
                    .map_err(Into::into)
            }
            ArchiveStorageConfig::StorageBoxRclone(config) => {
                let mut remote_request = request.clone();
                remote_request.manifest_mode = ManifestMode::AppendJsonl;
                commit_file_to_storage_box_rclone(
                    config,
                    &remote_request,
                    temp_path,
                    Some(repo_receipt_path),
                )?;
                let store = LocalStore::new(self.output_dir);
                let mut local_request = request.clone();
                local_request.manifest_mode = ManifestMode::Skip;
                store
                    .commit_prepared_temp(&local_request, temp_path)
                    .map_err(Into::into)
            }
        }
    }

    pub(super) fn commit_profile(
        &self,
        did: &str,
        profile: Option<&ProfileRecord>,
        receipt: &RepoReceipt,
        receipt_hash: &str,
        commit_context: &ArchiveCommitContext,
    ) -> Result<Option<crate::commit::Artifact>, ArchiveError> {
        let Some(profile) = profile else {
            return Ok(None);
        };
        let store = LocalStore::new(self.output_dir);
        let profile_stem = stable_artifact_stem(
            did,
            "raw_profile_sidecar",
            receipt.profile_row_hash.as_deref().unwrap_or(receipt_hash),
        );
        let object_path = PathBuf::from(format!("{profile_stem}.profile.json"));
        let receipt_path = stable_object_receipt_path(&profile_stem, receipt_hash, "profile");
        let manifest_path = PathBuf::from(format!("{profile_stem}.profile.manifest.jsonl"));
        let request = profile_sidecar_request(
            object_path.clone(),
            receipt_path.clone(),
            manifest_path.clone(),
            receipt,
            commit_context,
        )?;
        let committed = match self.storage_config {
            ArchiveStorageConfig::Local => commit_profile_sidecar(
                &store,
                ProfileSidecarCommitPaths {
                    object_path,
                    receipt_path,
                    manifest_path,
                    manifest_mode: ManifestMode::AppendJsonl,
                },
                profile,
                receipt,
                commit_context,
            )?,
            ArchiveStorageConfig::StorageBoxSsh(config) => {
                let mut remote_request = request;
                remote_request.manifest_mode = ManifestMode::AppendJsonl;
                let temp_profile =
                    super::archive_io::write_profile_sidecar_temp(self.output_dir, profile)?;
                commit_file_to_storage_box(config, &remote_request, temp_profile.as_ref(), None)?;
                commit_profile_sidecar(
                    &store,
                    ProfileSidecarCommitPaths {
                        object_path,
                        receipt_path,
                        manifest_path,
                        manifest_mode: ManifestMode::Skip,
                    },
                    profile,
                    receipt,
                    commit_context,
                )?
            }
            ArchiveStorageConfig::StorageBoxRclone(config) => {
                let mut remote_request = request;
                remote_request.manifest_mode = ManifestMode::AppendJsonl;
                let temp_profile =
                    super::archive_io::write_profile_sidecar_temp(self.output_dir, profile)?;
                commit_file_to_storage_box_rclone(
                    config,
                    &remote_request,
                    temp_profile.as_ref(),
                    None,
                )?;
                commit_profile_sidecar(
                    &store,
                    ProfileSidecarCommitPaths {
                        object_path,
                        receipt_path,
                        manifest_path,
                        manifest_mode: ManifestMode::Skip,
                    },
                    profile,
                    receipt,
                    commit_context,
                )?
            }
        };
        Ok(Some(committed))
    }
}

fn commit_file_to_storage_box(
    config: &StorageBoxArchiveConfig,
    request: &Request,
    object_path: &Path,
    repo_receipt_path: Option<&Path>,
) -> Result<(), ArchiveError> {
    let storage_config = crate::storage_box::StorageBoxConfig::new(config.remote_root.clone());
    let mut ssh_config = crate::storage_box::StorageBoxSshConfig::new(config.ssh_remote.clone())
        .with_ssh_program(config.ssh_program.clone())
        .with_command_timeout(config.command_timeout);
    for arg in &config.ssh_args {
        ssh_config = ssh_config.with_ssh_arg(arg.clone());
    }
    let commands = crate::storage_box::SshStorageBoxCommands::new(ssh_config);
    let mut backend = crate::storage_box::StorageBoxBackend::new(storage_config, commands);
    if let Some(repo_receipt_path) = repo_receipt_path {
        let relative_repo_receipt_path = PathBuf::from(repo_receipt_path.file_name().ok_or(
            ArchiveError::CountOverflow {
                field: "repo_receipt_file_name",
            },
        )?);
        backend.commit_auxiliary_file(
            request,
            &relative_repo_receipt_path,
            repo_receipt_path,
            "repo receipt",
        )?;
    }
    backend.commit_file(request, object_path)?;
    Ok(())
}

fn commit_file_to_storage_box_rclone(
    config: &StorageBoxRcloneArchiveConfig,
    request: &Request,
    object_path: &Path,
    repo_receipt_path: Option<&Path>,
) -> Result<(), ArchiveError> {
    let storage_config = crate::storage_box::StorageBoxConfig::new(config.remote_root.clone());
    let rclone_config = crate::storage_box::StorageBoxRcloneConfig::new(
        config.config_path.clone(),
        config.remote_name.clone(),
    )
    .with_rclone_program(config.rclone_program.clone())
    .with_command_timeout(config.command_timeout);
    let commands = crate::storage_box::RcloneStorageBoxCommands::new(rclone_config);
    let mut backend = crate::storage_box::StorageBoxBackend::new(storage_config, commands);
    if let Some(repo_receipt_path) = repo_receipt_path {
        let relative_repo_receipt_path = PathBuf::from(repo_receipt_path.file_name().ok_or(
            ArchiveError::CountOverflow {
                field: "repo_receipt_file_name",
            },
        )?);
        backend.commit_auxiliary_file(
            request,
            &relative_repo_receipt_path,
            repo_receipt_path,
            "repo receipt",
        )?;
    }
    backend.commit_file(request, object_path)?;
    Ok(())
}
