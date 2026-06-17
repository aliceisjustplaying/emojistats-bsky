use std::{
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use super::{Error, StorageBoxConfig};
use crate::commit::Request;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RemotePaths {
    pub(super) object: String,
    pub(super) temp_object: String,
    pub(super) receipt: String,
    pub(super) temp_receipt: String,
    pub(super) manifest: String,
    pub(super) object_manifest_path: String,
}

impl RemotePaths {
    pub(super) fn for_request(config: &StorageBoxConfig, request: &Request) -> Result<Self, Error> {
        let root = normalize_root(&config.remote_root)?;
        let temp_directory = normalize_temp_directory(&config.temp_directory)?;
        let object_manifest_path = manifest_path_string("object", &request.object_path)?;
        let receipt_manifest_path = manifest_path_string("receipt", &request.receipt_path)?;
        let manifest_path = manifest_path_string("manifest", &request.manifest_path)?;
        let object = join_remote(&root, &object_manifest_path);
        let receipt = join_remote(&root, &receipt_manifest_path);
        let manifest = join_remote(&root, &manifest_path);
        let temp_base = join_remote(&root, &temp_directory);
        let temp_run = join_remote(
            &temp_base,
            &safe_component("run id", &request.metadata.run_id)?,
        );
        let temp_shard = join_remote(
            &temp_run,
            &safe_component("shard", &request.metadata.shard)?,
        );
        let temp_object = join_remote(
            &temp_shard,
            &temp_name_for(
                "object",
                &request.object_path,
                request.metadata.file_sequence,
            )?,
        );
        let temp_receipt = join_remote(
            &temp_shard,
            &temp_name_for(
                "receipt",
                &request.receipt_path,
                request.metadata.file_sequence,
            )?,
        );

        Ok(Self {
            object,
            temp_object,
            receipt,
            temp_receipt,
            manifest,
            object_manifest_path,
        })
    }
}

pub(super) fn normalize_root(root: &str) -> Result<String, Error> {
    let trimmed = root.trim_end_matches('/');
    if trimmed.is_empty() || !trimmed.starts_with('/') {
        return Err(Error::InvalidRemoteRoot(root.to_owned()));
    }
    if trimmed.chars().any(char::is_control)
        || trimmed
            .split('/')
            .any(|component| component == "." || component == "..")
    {
        return Err(Error::InvalidRemoteRoot(root.to_owned()));
    }
    Ok(trimmed.to_owned())
}

pub(super) fn normalize_temp_directory(path: &Path) -> Result<String, Error> {
    let normalized = relative_path_string("temp directory", path).map_err(|error| match error {
        Error::PathEscapesRoot { path, .. } => Error::TempDirectoryEscapesRoot { path },
        other => other,
    })?;
    Ok(normalized)
}

pub(super) fn manifest_path_string(kind: &'static str, path: &Path) -> Result<String, Error> {
    relative_path_string(kind, path)
}

fn relative_path_string(kind: &'static str, path: &Path) -> Result<String, Error> {
    let raw = path.to_str().ok_or_else(|| Error::NonUtf8Path {
        kind,
        path: path.to_path_buf(),
    })?;
    if raw
        .split('/')
        .any(|component| component == "." || component == "..")
    {
        return Err(Error::PathEscapesRoot {
            kind,
            path: path.to_path_buf(),
        });
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| Error::NonUtf8Path {
                    kind,
                    path: path.to_path_buf(),
                })?;
                parts.push(part);
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(Error::PathEscapesRoot {
                    kind,
                    path: path.to_path_buf(),
                });
            }
        }
    }
    if parts.is_empty() {
        return Err(Error::MissingFileName {
            kind,
            path: path.to_path_buf(),
        });
    }
    Ok(parts.join("/"))
}

pub(super) fn safe_component(kind: &'static str, value: &str) -> Result<String, Error> {
    let path = PathBuf::from(value);
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(component)), None) => component
            .to_str()
            .map(ToOwned::to_owned)
            .ok_or(Error::NonUtf8Path { kind, path }),
        _ => Err(Error::PathEscapesRoot { kind, path }),
    }
}

pub(super) fn temp_name_for(
    kind: &'static str,
    path: &Path,
    file_sequence: u64,
) -> Result<String, Error> {
    let file_name = path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .ok_or_else(|| Error::MissingFileName {
            kind,
            path: path.to_path_buf(),
        })?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    Ok(format!(
        "{file_name}.tmp.{file_sequence}.{}.{}",
        std::process::id(),
        timestamp
    ))
}

pub(super) fn join_remote(root: &str, relative: &str) -> String {
    format!("{root}/{relative}")
}
