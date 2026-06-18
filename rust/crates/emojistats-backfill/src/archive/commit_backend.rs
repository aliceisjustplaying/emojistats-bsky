use super::{
    ArchiveCommitContext, ArchiveError, ArchiveStorageConfig, LocalStore, ManifestMode, Path,
    PathBuf, ProfileRecord, RepoReceipt, Request, StorageBoxArchiveConfig,
    StorageBoxRcloneArchiveConfig,
    archive_io::{ProfileSidecarCommitPaths, commit_profile_sidecar, profile_sidecar_request},
    naming::{stable_artifact_stem, stable_manifest_path, stable_object_receipt_path},
};

trait ArchiveCommitStore {
    fn commit_prepared_posts(
        &self,
        request: &Request,
        temp_path: &Path,
        repo_receipt_path: &Path,
    ) -> Result<crate::commit::Artifact, ArchiveError>;

    fn commit_prepared_profile(
        &self,
        paths: ProfileSidecarCommitPaths,
        request: Request,
        profile: &ProfileRecord,
        receipt: &RepoReceipt,
        commit_context: &ArchiveCommitContext,
    ) -> Result<crate::commit::Artifact, ArchiveError>;
}

struct LocalArchiveCommitStore<'a> {
    output_dir: &'a Path,
}

struct StorageBoxSshArchiveCommitStore<'a> {
    output_dir: &'a Path,
    config: &'a StorageBoxArchiveConfig,
}

struct StorageBoxRcloneArchiveCommitStore<'a> {
    output_dir: &'a Path,
    config: &'a StorageBoxRcloneArchiveConfig,
}

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
        self.store()
            .commit_prepared_posts(request, temp_path, repo_receipt_path)
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
        let profile_stem = stable_artifact_stem(
            did,
            "raw_profile_sidecar",
            receipt.profile_row_hash.as_deref().unwrap_or(receipt_hash),
        );
        let object_path = PathBuf::from(format!("{profile_stem}.profile.json"));
        let receipt_path = stable_object_receipt_path(&profile_stem, receipt_hash, "profile");
        let manifest_path = stable_manifest_path(&commit_context.run_id, &commit_context.shard);
        let request = profile_sidecar_request(
            object_path.clone(),
            receipt_path.clone(),
            manifest_path.clone(),
            receipt,
            commit_context,
        )?;
        let committed = self.store().commit_prepared_profile(
            ProfileSidecarCommitPaths {
                object_path,
                receipt_path,
                manifest_path,
                manifest_mode: ManifestMode::AppendJsonl,
            },
            request,
            profile,
            receipt,
            commit_context,
        )?;
        Ok(Some(committed))
    }

    fn store(&self) -> Box<dyn ArchiveCommitStore + '_> {
        match self.storage_config {
            ArchiveStorageConfig::Local => Box::new(LocalArchiveCommitStore {
                output_dir: self.output_dir,
            }),
            ArchiveStorageConfig::StorageBoxSsh(config) => {
                Box::new(StorageBoxSshArchiveCommitStore {
                    output_dir: self.output_dir,
                    config,
                })
            }
            ArchiveStorageConfig::StorageBoxRclone(config) => {
                Box::new(StorageBoxRcloneArchiveCommitStore {
                    output_dir: self.output_dir,
                    config,
                })
            }
        }
    }
}

impl ArchiveCommitStore for LocalArchiveCommitStore<'_> {
    fn commit_prepared_posts(
        &self,
        request: &Request,
        temp_path: &Path,
        _repo_receipt_path: &Path,
    ) -> Result<crate::commit::Artifact, ArchiveError> {
        let store = LocalStore::new(self.output_dir);
        store
            .commit_prepared_temp(request, temp_path)
            .map_err(Into::into)
    }

    fn commit_prepared_profile(
        &self,
        mut paths: ProfileSidecarCommitPaths,
        _request: Request,
        profile: &ProfileRecord,
        receipt: &RepoReceipt,
        commit_context: &ArchiveCommitContext,
    ) -> Result<crate::commit::Artifact, ArchiveError> {
        paths.manifest_mode = ManifestMode::AppendJsonl;
        commit_profile_sidecar(
            &LocalStore::new(self.output_dir),
            paths,
            profile,
            receipt,
            commit_context,
        )
    }
}

impl ArchiveCommitStore for StorageBoxSshArchiveCommitStore<'_> {
    fn commit_prepared_posts(
        &self,
        request: &Request,
        temp_path: &Path,
        repo_receipt_path: &Path,
    ) -> Result<crate::commit::Artifact, ArchiveError> {
        let mut remote_request = request.clone();
        remote_request.manifest_mode = ManifestMode::AppendJsonl;
        let mut backend = self.storage_box_backend();
        commit_auxiliary_repo_receipt(&mut backend, &remote_request, repo_receipt_path)?;
        backend.commit_file(&remote_request, temp_path)?;
        commit_local_without_manifest(self.output_dir, request, temp_path)
    }

    fn commit_prepared_profile(
        &self,
        mut paths: ProfileSidecarCommitPaths,
        mut request: Request,
        profile: &ProfileRecord,
        receipt: &RepoReceipt,
        commit_context: &ArchiveCommitContext,
    ) -> Result<crate::commit::Artifact, ArchiveError> {
        request.manifest_mode = ManifestMode::AppendJsonl;
        let temp_profile = super::archive_io::write_profile_sidecar_temp(self.output_dir, profile)?;
        self.storage_box_backend()
            .commit_file(&request, temp_profile.as_ref())?;
        paths.manifest_mode = ManifestMode::Skip;
        commit_profile_sidecar(
            &LocalStore::new(self.output_dir),
            paths,
            profile,
            receipt,
            commit_context,
        )
    }
}

impl StorageBoxSshArchiveCommitStore<'_> {
    fn storage_box_backend(
        &self,
    ) -> crate::storage_box::StorageBoxBackend<crate::storage_box::SshStorageBoxCommands> {
        let storage_config =
            crate::storage_box::StorageBoxConfig::new(self.config.remote_root.clone());
        let mut ssh_config =
            crate::storage_box::StorageBoxSshConfig::new(self.config.ssh_remote.clone())
                .with_ssh_program(self.config.ssh_program.clone())
                .with_command_timeout(self.config.command_timeout);
        for arg in &self.config.ssh_args {
            ssh_config = ssh_config.with_ssh_arg(arg.clone());
        }
        let commands = crate::storage_box::SshStorageBoxCommands::new(ssh_config);
        crate::storage_box::StorageBoxBackend::new(storage_config, commands)
    }
}

impl ArchiveCommitStore for StorageBoxRcloneArchiveCommitStore<'_> {
    fn commit_prepared_posts(
        &self,
        request: &Request,
        temp_path: &Path,
        repo_receipt_path: &Path,
    ) -> Result<crate::commit::Artifact, ArchiveError> {
        let mut remote_request = request.clone();
        remote_request.manifest_mode = ManifestMode::Skip;
        let mut backend = self.storage_box_backend();
        commit_auxiliary_repo_receipt(&mut backend, &remote_request, repo_receipt_path)?;
        backend.commit_file_without_manifest(&remote_request, temp_path)?;
        commit_local_with_manifest(self.output_dir, request, temp_path)
    }

    fn commit_prepared_profile(
        &self,
        mut paths: ProfileSidecarCommitPaths,
        mut request: Request,
        profile: &ProfileRecord,
        receipt: &RepoReceipt,
        commit_context: &ArchiveCommitContext,
    ) -> Result<crate::commit::Artifact, ArchiveError> {
        request.manifest_mode = ManifestMode::Skip;
        let temp_profile = super::archive_io::write_profile_sidecar_temp(self.output_dir, profile)?;
        self.storage_box_backend()
            .commit_file_without_manifest(&request, temp_profile.as_ref())?;
        paths.manifest_mode = ManifestMode::AppendJsonl;
        commit_profile_sidecar(
            &LocalStore::new(self.output_dir),
            paths,
            profile,
            receipt,
            commit_context,
        )
    }
}

impl StorageBoxRcloneArchiveCommitStore<'_> {
    fn storage_box_backend(
        &self,
    ) -> crate::storage_box::StorageBoxBackend<crate::storage_box::RcloneStorageBoxCommands> {
        let storage_config =
            crate::storage_box::StorageBoxConfig::new(self.config.remote_root.clone());
        let rclone_config = crate::storage_box::StorageBoxRcloneConfig::new(
            self.config.config_path.clone(),
            self.config.remote_name.clone(),
        )
        .with_rclone_program(self.config.rclone_program.clone())
        .with_command_timeout(self.config.command_timeout);
        let commands = crate::storage_box::RcloneStorageBoxCommands::new(rclone_config);
        crate::storage_box::StorageBoxBackend::new(storage_config, commands)
    }
}

fn commit_auxiliary_repo_receipt<C>(
    backend: &mut crate::storage_box::StorageBoxBackend<C>,
    request: &Request,
    repo_receipt_path: &Path,
) -> Result<(), ArchiveError>
where
    C: crate::storage_box::StorageBoxCommands,
{
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
    Ok(())
}

fn commit_local_without_manifest(
    output_dir: &Path,
    request: &Request,
    temp_path: &Path,
) -> Result<crate::commit::Artifact, ArchiveError> {
    let mut local_request = request.clone();
    local_request.manifest_mode = ManifestMode::Skip;
    LocalStore::new(output_dir)
        .commit_prepared_temp(&local_request, temp_path)
        .map_err(Into::into)
}

fn commit_local_with_manifest(
    output_dir: &Path,
    request: &Request,
    temp_path: &Path,
) -> Result<crate::commit::Artifact, ArchiveError> {
    let mut local_request = request.clone();
    local_request.manifest_mode = ManifestMode::AppendJsonl;
    LocalStore::new(output_dir)
        .commit_prepared_temp(&local_request, temp_path)
        .map_err(Into::into)
}
