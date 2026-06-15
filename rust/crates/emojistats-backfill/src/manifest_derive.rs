//! Foundations for deriving load inputs from committed archive manifests.

use std::{
    io::{self, BufRead},
    path::{Component, Path, PathBuf},
};

use crate::{
    archive::LocalManifestEntry,
    commit::ManifestEntry,
    derive::{DeriveManifestIdentity, manifest_identity},
};

const RAW_ARCHIVE_POSTS_DATASET: &str = "raw_archive_posts";
const RAW_ARCHIVE_POSTS_SCHEMA_VERSION: u16 = 1;

/// A committed raw-archive manifest prepared for the derive loader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoaderInput {
    pub manifest: LocalManifestEntry,
    pub identity: DeriveManifestIdentity,
}

/// Result of reading a mixed committed manifest stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub inputs: Vec<LoaderInput>,
    pub skipped_entries: Vec<SkippedEntry>,
}

/// A well-formed committed manifest row that is not a raw archive post object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedEntry {
    pub line_number: usize,
    pub dataset: String,
    pub object_path: String,
}

/// Failure while reading derive inputs from a committed manifest.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("read committed manifest line {line_number}: {source}")]
    Io {
        line_number: usize,
        #[source]
        source: io::Error,
    },
    #[error("parse committed manifest line {line_number}: {source}")]
    Json {
        line_number: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("line number overflow while reading committed manifest")]
    LineNumberOverflow,
    #[error("committed manifest line {line_number} has an empty {field}")]
    EmptyField {
        line_number: usize,
        field: &'static str,
    },
    #[error(
        "committed raw archive manifest line {line_number} has schema_version {actual}, expected {expected}"
    )]
    UnsupportedSchemaVersion {
        line_number: usize,
        actual: u16,
        expected: u16,
    },
    #[error(
        "committed manifest line {line_number} object_path escapes archive root: {object_path}"
    )]
    ObjectPathEscapesRoot {
        line_number: usize,
        object_path: String,
    },
}

/// Read a committed JSONL manifest and prepare raw archive entries for derive loading.
///
/// Non-empty lines must deserialize as [`ManifestEntry`]. Entries for datasets other than
/// `raw_archive_posts` are reported as skips; raw archive entries are validated and mapped
/// into [`LocalManifestEntry`] plus the stable derive identity.
///
/// # Errors
///
/// Returns [`Error`] when a line cannot be read or parsed, or when a target raw archive
/// manifest entry has invalid schema or required fields.
pub fn read_committed_jsonl<R>(reader: R) -> Result<Plan, Error>
where
    R: BufRead,
{
    let mut inputs = Vec::new();
    let mut skipped_entries = Vec::new();

    for (line_index, line) in reader.lines().enumerate() {
        let line_number = line_index.checked_add(1).ok_or(Error::LineNumberOverflow)?;
        let line = line.map_err(|source| Error::Io {
            line_number,
            source,
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let entry: ManifestEntry = serde_json::from_str(&line).map_err(|source| Error::Json {
            line_number,
            source,
        })?;
        match loader_input_from_entry(entry, line_number)? {
            EntryDisposition::Load(input) => inputs.push(*input),
            EntryDisposition::Skip(skip) => skipped_entries.push(skip),
        }
    }

    Ok(Plan {
        inputs,
        skipped_entries,
    })
}

enum EntryDisposition {
    Load(Box<LoaderInput>),
    Skip(SkippedEntry),
}

fn loader_input_from_entry(
    entry: ManifestEntry,
    line_number: usize,
) -> Result<EntryDisposition, Error> {
    validate_required_fields(&entry, line_number)?;
    if entry.dataset != RAW_ARCHIVE_POSTS_DATASET {
        return Ok(EntryDisposition::Skip(SkippedEntry {
            line_number,
            dataset: entry.dataset,
            object_path: entry.object_path,
        }));
    }

    validate_raw_archive_entry(&entry, line_number)?;
    let manifest = local_manifest_from_entry(entry);
    let identity = manifest_identity(&manifest);

    Ok(EntryDisposition::Load(Box::new(LoaderInput {
        manifest,
        identity,
    })))
}

fn validate_required_fields(entry: &ManifestEntry, line_number: usize) -> Result<(), Error> {
    validate_non_empty(&entry.dataset, "dataset", line_number)?;
    validate_non_empty(&entry.run_id, "run_id", line_number)?;
    validate_non_empty(&entry.shard, "shard", line_number)?;
    validate_non_empty(&entry.object_path, "object_path", line_number)?;
    validate_non_empty(&entry.content_hash, "content_hash", line_number)?;
    validate_non_empty(&entry.receipt_hash, "receipt_hash", line_number)?;
    validate_scoped_object_path(&entry.object_path, line_number)
}

const fn validate_raw_archive_entry(
    entry: &ManifestEntry,
    line_number: usize,
) -> Result<(), Error> {
    if entry.schema_version == RAW_ARCHIVE_POSTS_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(Error::UnsupportedSchemaVersion {
            line_number,
            actual: entry.schema_version,
            expected: RAW_ARCHIVE_POSTS_SCHEMA_VERSION,
        })
    }
}

const fn validate_non_empty(
    value: &str,
    field: &'static str,
    line_number: usize,
) -> Result<(), Error> {
    if value.is_empty() {
        Err(Error::EmptyField { line_number, field })
    } else {
        Ok(())
    }
}

fn validate_scoped_object_path(object_path: &str, line_number: usize) -> Result<(), Error> {
    let path = Path::new(object_path);
    for component in path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(Error::ObjectPathEscapesRoot {
                    line_number,
                    object_path: object_path.to_owned(),
                });
            }
        }
    }
    Ok(())
}

fn local_manifest_from_entry(entry: ManifestEntry) -> LocalManifestEntry {
    LocalManifestEntry {
        run_id: entry.run_id,
        shard: entry.shard,
        file_sequence: entry.file_sequence,
        dataset: entry.dataset,
        local_path: PathBuf::from(entry.object_path),
        row_count: entry.row_count,
        bytes: entry.bytes,
        content_hash: entry.content_hash,
        min_created_at_normalized: entry.min_created_at_normalized,
        max_created_at_normalized: entry.max_created_at_normalized,
        receipt_hash: entry.receipt_hash,
        schema_version: entry.schema_version,
        normalizer: entry.normalizer,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]

    use std::io::Cursor;

    use super::{Error, read_committed_jsonl};
    use crate::{archive::NormalizerVersion, commit::ManifestEntry};

    fn normalizer() -> NormalizerVersion {
        NormalizerVersion {
            name: "emoji-normalizer".to_owned(),
            semver: "0.1.0".to_owned(),
            git_rev: "test".to_owned(),
            unicode_version: "16.0".to_owned(),
            emoji_data_version: "16.0".to_owned(),
        }
    }

    fn entry(dataset: &str) -> ManifestEntry {
        ManifestEntry {
            run_id: "run-1".to_owned(),
            shard: "shard3".to_owned(),
            file_sequence: 42,
            dataset: dataset.to_owned(),
            object_path: format!("objects/{dataset}/part-000042.parquet"),
            row_count: 123,
            bytes: 456,
            content_hash: "content-hash".to_owned(),
            min_created_at_normalized: Some("2026-06-15T00:00:00Z".to_owned()),
            max_created_at_normalized: Some("2026-06-15T01:00:00Z".to_owned()),
            receipt_hash: "receipt-hash".to_owned(),
            normalizer: normalizer(),
            schema_version: 1,
        }
    }

    fn jsonl(entries: &[ManifestEntry]) -> String {
        let mut lines = String::new();
        for entry in entries {
            lines.push_str(&serde_json::to_string(entry).expect("serialize manifest entry"));
            lines.push('\n');
        }
        lines
    }

    #[test]
    fn parses_jsonl_and_builds_loader_inputs_for_raw_archive_posts() {
        let raw_entry = entry("raw_archive_posts");
        let profile_entry = entry("raw_profile_sidecar");
        let plan = read_committed_jsonl(Cursor::new(jsonl(&[profile_entry, raw_entry.clone()])))
            .expect("read manifest jsonl");

        assert_eq!(plan.inputs.len(), 1);
        assert_eq!(plan.skipped_entries.len(), 1);
        let input = plan.inputs.first().expect("one loader input");
        assert_eq!(input.manifest.run_id, raw_entry.run_id);
        assert_eq!(
            input.manifest.local_path,
            std::path::PathBuf::from(raw_entry.object_path)
        );
        assert_eq!(plan.skipped_entries[0].dataset, "raw_profile_sidecar");
    }

    #[test]
    fn skips_non_raw_dataset_and_rejects_bad_dataset_field() {
        let skipped = read_committed_jsonl(Cursor::new(jsonl(&[entry("raw_profile_sidecar")])))
            .expect("read skipped manifest jsonl");
        assert!(skipped.inputs.is_empty());
        assert_eq!(skipped.skipped_entries.len(), 1);

        let mut bad = entry("");
        bad.object_path = "objects/empty-dataset.parquet".to_owned();
        let error =
            read_committed_jsonl(Cursor::new(jsonl(&[bad]))).expect_err("empty dataset rejected");
        assert!(matches!(
            error,
            Error::EmptyField {
                line_number: 1,
                field: "dataset"
            }
        ));
    }

    #[test]
    fn rejects_raw_archive_schema_mismatch() {
        let mut bad = entry("raw_archive_posts");
        bad.schema_version = 2;

        let error =
            read_committed_jsonl(Cursor::new(jsonl(&[bad]))).expect_err("bad schema rejected");

        assert!(matches!(
            error,
            Error::UnsupportedSchemaVersion {
                line_number: 1,
                actual: 2,
                expected: 1
            }
        ));
    }

    #[test]
    fn stable_identity_fields_come_from_committed_manifest() {
        let mut raw_entry = entry("raw_archive_posts");
        raw_entry.object_path = "objects/raw_archive_posts/a.parquet".to_owned();
        raw_entry.bytes = 111;
        raw_entry.min_created_at_normalized = Some("2026-06-15T00:00:00Z".to_owned());
        let first = read_committed_jsonl(Cursor::new(jsonl(&[raw_entry.clone()])))
            .expect("read first manifest jsonl");

        raw_entry.object_path = "objects/raw_archive_posts/b.parquet".to_owned();
        raw_entry.bytes = 222;
        raw_entry.min_created_at_normalized = Some("2026-06-14T00:00:00Z".to_owned());
        let second = read_committed_jsonl(Cursor::new(jsonl(&[raw_entry])))
            .expect("read second manifest jsonl");

        let first_identity = &first.inputs.first().expect("first input").identity;
        let second_identity = &second.inputs.first().expect("second input").identity;
        assert_eq!(first_identity, second_identity);
        assert_eq!(first_identity.run_id, "run-1");
        assert_eq!(first_identity.shard, "shard3");
        assert_eq!(first_identity.file_sequence, 42);
        assert_eq!(first_identity.dataset, "raw_archive_posts");
        assert_eq!(first_identity.content_hash, "content-hash");
        assert_eq!(first_identity.receipt_hash, "receipt-hash");
        assert_eq!(first_identity.schema_version, 1);
    }
}
