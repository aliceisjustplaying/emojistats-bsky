use std::time::Duration;

use emojistats_backfill::archive::{
    ArchiveStorageConfig, StorageBoxArchiveConfig, StorageBoxRcloneArchiveConfig,
};

use super::cli::{ArchiveBackend, ArchiveStorageArgs};

pub(super) fn archive_storage_config(
    args: ArchiveStorageArgs,
) -> anyhow::Result<ArchiveStorageConfig> {
    let ArchiveStorageArgs {
        archive_backend,
        storage_box_remote,
        storage_box_rclone_remote,
        storage_box_rclone_config,
        storage_box_rclone_program,
        storage_box_root,
        storage_box_ssh_program,
        storage_box_ssh_arg,
        storage_box_command_timeout_secs,
    } = args;
    match archive_backend {
        ArchiveBackend::Local => Ok(ArchiveStorageConfig::Local),
        ArchiveBackend::StorageBoxSsh => {
            let remote = storage_box_remote
                .ok_or_else(|| anyhow::anyhow!("--storage-box-remote is required"))?;
            let root = storage_box_root
                .ok_or_else(|| anyhow::anyhow!("--storage-box-root is required"))?;
            let mut config = StorageBoxArchiveConfig::new(root, remote);
            config.ssh_program = storage_box_ssh_program;
            config.ssh_args = storage_box_ssh_arg;
            config.command_timeout = Duration::from_secs(storage_box_command_timeout_secs);
            Ok(ArchiveStorageConfig::StorageBoxSsh(config))
        }
        ArchiveBackend::StorageBoxRclone => {
            let root = storage_box_root
                .ok_or_else(|| anyhow::anyhow!("--storage-box-root is required"))?;
            let config_path = storage_box_rclone_config
                .ok_or_else(|| anyhow::anyhow!("--storage-box-rclone-config is required"))?;
            let mut config =
                StorageBoxRcloneArchiveConfig::new(root, storage_box_rclone_remote, config_path);
            config.rclone_program = storage_box_rclone_program;
            config.command_timeout = Duration::from_secs(storage_box_command_timeout_secs);
            Ok(ArchiveStorageConfig::StorageBoxRclone(config))
        }
    }
}
