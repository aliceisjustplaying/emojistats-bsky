use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use fs4::{FileExt, TryLockError};

use super::{Error, ManifestEntry, ManifestMode, sync_parent_dir, write_json_temp_promote};

const MANIFEST_LOCK_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MANIFEST_LOCK_MAX_WAIT: Duration = Duration::from_secs(60);

pub(super) fn write_manifest(
    path: &Path,
    mode: ManifestMode,
    entry: &ManifestEntry,
) -> Result<(), Error> {
    match mode {
        ManifestMode::Skip => Ok(()),
        ManifestMode::AppendJsonl => append_manifest_jsonl(path, entry),
        ManifestMode::ReplaceJsonArray => write_json_temp_promote(path, "manifest", &[entry]),
    }
}

pub(super) fn write_manifest_if_missing(
    path: &Path,
    mode: ManifestMode,
    entry: &ManifestEntry,
) -> Result<(), Error> {
    if mode == ManifestMode::Skip {
        return Ok(());
    }
    if mode == ManifestMode::AppendJsonl {
        return append_manifest_jsonl_if_missing(path, entry);
    }
    if manifest_contains_entry(path, mode, entry)? {
        Ok(())
    } else {
        write_manifest(path, mode, entry)
    }
}

fn manifest_contains_entry(
    path: &Path,
    mode: ManifestMode,
    entry: &ManifestEntry,
) -> Result<bool, Error> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(Error::Io {
                operation: "read manifest",
                path: path.to_path_buf(),
                source,
            });
        }
    };

    match mode {
        ManifestMode::Skip => Ok(false),
        ManifestMode::AppendJsonl => {
            for line in contents.lines().filter(|line| !line.trim().is_empty()) {
                let candidate: ManifestEntry =
                    serde_json::from_str(line).map_err(|source| Error::JsonRead {
                        path: path.to_path_buf(),
                        source,
                    })?;
                if candidate == *entry {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        ManifestMode::ReplaceJsonArray => {
            let entries: Vec<ManifestEntry> =
                serde_json::from_str(&contents).map_err(|source| Error::JsonRead {
                    path: path.to_path_buf(),
                    source,
                })?;
            Ok(entries.iter().any(|candidate| candidate == entry))
        }
    }
}

fn append_manifest_jsonl(path: &Path, entry: &ManifestEntry) -> Result<(), Error> {
    let _lock = ManifestAppendLock::acquire(path)?;
    append_manifest_jsonl_unlocked(path, entry)
}

fn append_manifest_jsonl_if_missing(path: &Path, entry: &ManifestEntry) -> Result<(), Error> {
    let _lock = ManifestAppendLock::acquire(path)?;
    if manifest_contains_entry(path, ManifestMode::AppendJsonl, entry)? {
        return Ok(());
    }
    append_manifest_jsonl_unlocked(path, entry)
}

fn append_manifest_jsonl_unlocked(path: &Path, entry: &ManifestEntry) -> Result<(), Error> {
    let mut line = serde_json::to_vec(entry).map_err(|source| Error::Json {
        path: path.to_path_buf(),
        source,
    })?;
    line.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| Error::Io {
            operation: "open manifest for append",
            path: path.to_path_buf(),
            source,
        })?;
    file.write_all(&line).map_err(|source| Error::Io {
        operation: "write manifest record",
        path: path.to_path_buf(),
        source,
    })?;
    file.sync_all().map_err(|source| Error::Io {
        operation: "fsync manifest",
        path: path.to_path_buf(),
        source,
    })?;
    drop(file);
    sync_parent_dir(path, "manifest")
}

struct ManifestAppendLock {
    file: File,
}

impl ManifestAppendLock {
    fn acquire(path: &Path) -> Result<Self, Error> {
        let lock_path = manifest_lock_path(path)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|source| Error::Io {
                operation: "open manifest append lock",
                path: lock_path.clone(),
                source,
            })?;
        let started = Instant::now();
        loop {
            match FileExt::try_lock(&file) {
                Ok(()) => {
                    file.set_len(0).map_err(|source| Error::Io {
                        operation: "truncate manifest append lock",
                        path: lock_path.clone(),
                        source,
                    })?;
                    writeln!(&file, "{}", std::process::id()).map_err(|source| Error::Io {
                        operation: "write manifest append lock",
                        path: lock_path.clone(),
                        source,
                    })?;
                    file.sync_all().map_err(|source| Error::Io {
                        operation: "fsync manifest append lock",
                        path: lock_path.clone(),
                        source,
                    })?;
                    sync_parent_dir(&lock_path, "manifest append lock")?;
                    return Ok(Self { file });
                }
                Err(TryLockError::WouldBlock) => {
                    if started.elapsed() >= MANIFEST_LOCK_MAX_WAIT {
                        return Err(Error::Io {
                            operation: "acquire manifest append lock",
                            path: lock_path,
                            source: io::Error::new(
                                io::ErrorKind::WouldBlock,
                                "manifest append lock timed out",
                            ),
                        });
                    }
                    thread::sleep(MANIFEST_LOCK_POLL_INTERVAL);
                }
                Err(TryLockError::Error(source)) => {
                    return Err(Error::Io {
                        operation: "acquire manifest append lock",
                        path: lock_path,
                        source,
                    });
                }
            }
        }
    }
}

impl Drop for ManifestAppendLock {
    fn drop(&mut self) {
        let _ignored = FileExt::unlock(&self.file);
    }
}

fn manifest_lock_path(path: &Path) -> Result<PathBuf, Error> {
    let file_name =
        path.file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| Error::MissingFileName {
                kind: "manifest lock",
                path: path.to_path_buf(),
            })?;
    Ok(path.with_file_name(format!(".{file_name}.lock")))
}
