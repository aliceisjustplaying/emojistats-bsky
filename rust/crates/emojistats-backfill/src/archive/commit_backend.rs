use super::{
    ArchiveCommitContext, ArchiveError, ArchiveStorageConfig, LocalStore, ManifestMode, Path,
    PathBuf, ProfileRecord, RepoReceipt, Request, StorageBoxArchiveConfig,
    archive_io::{
        ProfileSidecarCommitPaths, commit_profile_sidecar, profile_sidecar_request,
        stable_object_receipt_path,
    },
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
        let store = LocalStore::new(self.output_dir);
        let mut local_request = request.clone();
        local_request.manifest_mode = self.local_manifest_mode();
        let committed = store.commit_prepared_temp(&local_request, temp_path)?;
        self.mirror_storage_box(request, &committed, Some(repo_receipt_path))?;
        Ok(committed)
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
        let profile_stem = super::archive_io::stable_artifact_stem(
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
        let committed = commit_profile_sidecar(
            &store,
            ProfileSidecarCommitPaths {
                object_path,
                receipt_path,
                manifest_path,
                manifest_mode: self.local_manifest_mode(),
            },
            profile,
            receipt,
            commit_context,
        )?;
        self.mirror_storage_box(&request, &committed, None)?;
        Ok(Some(committed))
    }

    const fn local_manifest_mode(&self) -> ManifestMode {
        match self.storage_config {
            ArchiveStorageConfig::Local => ManifestMode::AppendJsonl,
            ArchiveStorageConfig::StorageBoxSsh(_) => ManifestMode::Skip,
        }
    }

    fn mirror_storage_box(
        &self,
        request: &Request,
        committed: &crate::commit::Artifact,
        repo_receipt_path: Option<&Path>,
    ) -> Result<(), ArchiveError> {
        let ArchiveStorageConfig::StorageBoxSsh(config) = self.storage_config else {
            return Ok(());
        };
        let mut remote_request = request.clone();
        remote_request.manifest_mode = ManifestMode::AppendJsonl;
        commit_file_to_storage_box(
            config,
            &remote_request,
            &committed.object_path,
            repo_receipt_path,
        )
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
