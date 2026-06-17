use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};

use super::{Error, LocalStore, ManifestEntry, ManifestMode, Metadata, Receipt, Request};
use crate::archive::NormalizerVersion;

fn normalizer() -> NormalizerVersion {
    NormalizerVersion {
        name: "emoji-normalizer".to_owned(),
        semver: "0.1.0".to_owned(),
        git_rev: "test".to_owned(),
        unicode_version: "16.0".to_owned(),
        emoji_data_version: "16.0".to_owned(),
    }
}

fn metadata(file_sequence: u64) -> Metadata {
    Metadata {
        run_id: "run-1".to_owned(),
        shard: "shard0".to_owned(),
        file_sequence,
        did: "did:plc:test".to_owned(),
        dataset: "raw_archive_posts".to_owned(),
        row_count: 2,
        min_created_at_normalized: Some("2026-06-01T00:00:00Z".to_owned()),
        max_created_at_normalized: Some("2026-06-02T00:00:00Z".to_owned()),
        receipt_hash: "repo-receipt-hash".to_owned(),
        repo_receipt_path: None,
        normalizer: normalizer(),
        schema_version: 3,
    }
}

fn request(file_sequence: u64, mode: ManifestMode) -> Request {
    Request {
        object_path: PathBuf::from(format!("objects/run-1/shard0/{file_sequence}.parquet")),
        receipt_path: PathBuf::from(format!("objects/run-1/shard0/{file_sequence}.receipt.json")),
        manifest_path: PathBuf::from("manifests/raw.jsonl"),
        manifest_mode: mode,
        metadata: metadata(file_sequence),
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock should be after epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "emojistats-commit-{name}-{}-{stamp}",
        std::process::id()
    ));
    fs::create_dir_all(&path).expect("test temp dir should be created");
    path
}

fn read_json<T>(path: &Path) -> T
where
    T: serde::de::DeserializeOwned,
{
    let bytes = fs::read(path).expect("test JSON file should be readable");
    serde_json::from_slice(&bytes).expect("test JSON should decode")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[test]
fn commits_object_receipt_and_jsonl_manifest() {
    let root = temp_dir("jsonl");
    let store = LocalStore::new(&root);
    let artifact = store
        .commit(&request(1, ManifestMode::AppendJsonl), |file| {
            file.write_all(b"abc").map_err(|source| Error::Io {
                operation: "test write",
                path: PathBuf::from("test"),
                source,
            })
        })
        .expect("commit should succeed");

    assert_eq!(
        fs::read(&artifact.object_path).expect("object should be readable"),
        b"abc"
    );
    assert_eq!(artifact.entry.bytes, 3);
    assert_eq!(artifact.entry.content_hash, sha256_hex(b"abc"));
    assert_eq!(artifact.entry.object_path, "objects/run-1/shard0/1.parquet");

    let receipt: Receipt = read_json(&artifact.receipt_path);
    assert_eq!(receipt, artifact.receipt);
    assert_eq!(receipt.protocol_version, 1);

    let manifest =
        fs::read_to_string(&artifact.manifest_path).expect("manifest should be readable");
    let mut lines = manifest.lines();
    let first = lines.next().expect("manifest should contain one entry");
    assert!(lines.next().is_none());
    let manifest_entry: ManifestEntry =
        serde_json::from_str(first).expect("manifest entry should decode");
    assert_eq!(manifest_entry, artifact.entry);

    fs::remove_dir_all(root).expect("test temp dir should be removed");
}

#[test]
fn commits_prepared_temp_object_receipt_and_manifest() {
    let root = temp_dir("prepared");
    let store = LocalStore::new(&root);
    let prepared_path = root.join("prepared-object.tmp");
    fs::write(&prepared_path, b"prepared").expect("prepared object should be written");

    let artifact = store
        .commit_prepared_temp(&request(3, ManifestMode::AppendJsonl), &prepared_path)
        .expect("prepared commit should succeed");

    assert!(!prepared_path.exists());
    assert_eq!(
        fs::read(&artifact.object_path).expect("object should be readable"),
        b"prepared"
    );
    assert_eq!(artifact.entry.bytes, 8);
    assert_eq!(artifact.entry.content_hash, sha256_hex(b"prepared"));

    let receipt: Receipt = read_json(&artifact.receipt_path);
    assert_eq!(receipt, artifact.receipt);
    let manifest =
        fs::read_to_string(&artifact.manifest_path).expect("manifest should be readable");
    let entry: ManifestEntry =
        serde_json::from_str(manifest.trim()).expect("manifest entry should decode");
    assert_eq!(entry, artifact.entry);

    fs::remove_dir_all(root).expect("test temp dir should be removed");
}

#[test]
fn skip_manifest_mode_commits_object_and_receipt_without_manifest_exposure() {
    let root = temp_dir("skip-manifest");
    let store = LocalStore::new(&root);
    let artifact = store
        .commit(&request(31, ManifestMode::Skip), |file| {
            file.write_all(b"remote-first").map_err(|source| Error::Io {
                operation: "test write",
                path: PathBuf::from("test"),
                source,
            })
        })
        .expect("commit should succeed");

    assert_eq!(
        fs::read(&artifact.object_path).expect("object should be readable"),
        b"remote-first"
    );
    let receipt: Receipt = read_json(&artifact.receipt_path);
    assert_eq!(receipt, artifact.receipt);
    assert!(
        !artifact.manifest_path.exists(),
        "local manifest should not be exposed in skip mode"
    );

    fs::remove_dir_all(root).expect("test temp dir should be removed");
}

#[test]
fn retry_repairs_missing_jsonl_manifest_when_object_and_receipt_match() {
    let root = temp_dir("repair-missing-manifest");
    let store = LocalStore::new(&root);
    let request = request(4, ManifestMode::AppendJsonl);
    let artifact = store
        .commit(&request, |file| {
            file.write_all(b"retryable").map_err(|source| Error::Io {
                operation: "test write",
                path: PathBuf::from("test"),
                source,
            })
        })
        .expect("initial commit should succeed");
    fs::remove_file(&artifact.manifest_path).expect("manifest should be removable");

    let repaired = store
        .commit(&request, |file| {
            file.write_all(b"retryable").map_err(|source| Error::Io {
                operation: "test write",
                path: PathBuf::from("test"),
                source,
            })
        })
        .expect("retry should repair missing manifest");

    assert_eq!(repaired.entry, artifact.entry);
    let manifest =
        fs::read_to_string(&artifact.manifest_path).expect("manifest should be repaired");
    let entries = manifest.lines().collect::<Vec<_>>();
    assert_eq!(entries.len(), 1);
    let first_entry = entries.first().expect("manifest should contain one entry");
    let manifest_entry: ManifestEntry =
        serde_json::from_str(first_entry).expect("manifest entry should decode");
    assert_eq!(manifest_entry, artifact.entry);

    fs::remove_dir_all(root).expect("test temp dir should be removed");
}

#[test]
fn retry_repairs_missing_receipt_when_object_matches() {
    let root = temp_dir("repair-missing-receipt");
    let store = LocalStore::new(&root);
    let request = request(6, ManifestMode::AppendJsonl);
    let artifact = store
        .commit(&request, |file| {
            file.write_all(b"retryable").map_err(|source| Error::Io {
                operation: "test write",
                path: PathBuf::from("test"),
                source,
            })
        })
        .expect("initial commit should succeed");
    fs::remove_file(&artifact.receipt_path).expect("receipt should be removable");

    let repaired = store
        .commit(&request, |file| {
            file.write_all(b"retryable").map_err(|source| Error::Io {
                operation: "test write",
                path: PathBuf::from("test"),
                source,
            })
        })
        .expect("retry should repair missing receipt");

    assert_eq!(repaired.entry, artifact.entry);
    let receipt: Receipt = read_json(&artifact.receipt_path);
    assert_eq!(receipt, artifact.receipt);
    let manifest =
        fs::read_to_string(&artifact.manifest_path).expect("manifest should be readable");
    assert_eq!(manifest.lines().count(), 1);

    fs::remove_dir_all(root).expect("test temp dir should be removed");
}

#[test]
fn retry_with_existing_jsonl_manifest_does_not_duplicate_entry() {
    let root = temp_dir("repair-existing-manifest");
    let store = LocalStore::new(&root);
    let request = request(5, ManifestMode::AppendJsonl);
    let artifact = store
        .commit(&request, |file| {
            file.write_all(b"retryable").map_err(|source| Error::Io {
                operation: "test write",
                path: PathBuf::from("test"),
                source,
            })
        })
        .expect("initial commit should succeed");

    let repaired = store
        .commit(&request, |file| {
            file.write_all(b"retryable").map_err(|source| Error::Io {
                operation: "test write",
                path: PathBuf::from("test"),
                source,
            })
        })
        .expect("retry should be idempotent");

    assert_eq!(repaired.entry, artifact.entry);
    let manifest =
        fs::read_to_string(&artifact.manifest_path).expect("manifest should be readable");
    assert_eq!(manifest.lines().count(), 1);

    fs::remove_dir_all(root).expect("test temp dir should be removed");
}

#[test]
fn retry_repairs_truncated_jsonl_manifest_tail() {
    let root = temp_dir("repair-truncated-manifest-tail");
    let store = LocalStore::new(&root);
    let request = request(8, ManifestMode::AppendJsonl);
    let artifact = store
        .commit(&request, |file| {
            file.write_all(b"retryable").map_err(|source| Error::Io {
                operation: "test write",
                path: PathBuf::from("test"),
                source,
            })
        })
        .expect("initial commit should succeed");
    {
        let mut manifest = fs::OpenOptions::new()
            .append(true)
            .open(&artifact.manifest_path)
            .expect("manifest should open");
        manifest
            .write_all(b"{\"truncated\"")
            .expect("truncated tail should write");
    }

    let repaired = store
        .commit(&request, |file| {
            file.write_all(b"retryable").map_err(|source| Error::Io {
                operation: "test write",
                path: PathBuf::from("test"),
                source,
            })
        })
        .expect("retry should repair truncated manifest tail");

    assert_eq!(repaired.entry, artifact.entry);
    let manifest =
        fs::read_to_string(&artifact.manifest_path).expect("manifest should be readable");
    let entries = manifest.lines().collect::<Vec<_>>();
    assert_eq!(entries.len(), 1);
    let manifest_entry: ManifestEntry =
        serde_json::from_str(entries.first().expect("manifest should contain entry"))
            .expect("manifest entry should decode");
    assert_eq!(manifest_entry, artifact.entry);

    fs::remove_dir_all(root).expect("test temp dir should be removed");
}

#[test]
fn replace_manifest_writes_json_array() {
    let root = temp_dir("array");
    let store = LocalStore::new(&root);
    let artifact = store
        .commit(&request(7, ManifestMode::ReplaceJsonArray), |file| {
            file.write_all(b"payload").map_err(|source| Error::Io {
                operation: "test write",
                path: PathBuf::from("test"),
                source,
            })
        })
        .expect("commit should succeed");

    let entries: Vec<ManifestEntry> = read_json(&artifact.manifest_path);
    assert_eq!(entries, vec![artifact.entry]);
    fs::remove_dir_all(root).expect("test temp dir should be removed");
}

#[test]
fn failed_object_write_leaves_no_committed_outputs() {
    let root = temp_dir("failure");
    let store = LocalStore::new(&root);
    let result = store.commit(&request(1, ManifestMode::AppendJsonl), |_file| {
        Err(Error::writer("boom"))
    });

    assert!(matches!(result, Err(Error::Writer(message)) if message == "boom"));
    assert!(!root.join("objects/run-1/shard0/1.parquet").exists());
    assert!(!root.join("objects/run-1/shard0/1.receipt.json").exists());
    assert!(!root.join("manifests/raw.jsonl").exists());
    let object_dir = root.join("objects/run-1/shard0");
    let temp_count = fs::read_dir(object_dir)
        .expect("object dir should exist")
        .count();
    assert_eq!(temp_count, 0);
    fs::remove_dir_all(root).expect("test temp dir should be removed");
}

#[test]
fn rejects_paths_that_escape_store_root() {
    let root = temp_dir("escape");
    let store = LocalStore::new(&root);
    let mut escaping = request(1, ManifestMode::AppendJsonl);
    escaping.object_path = PathBuf::from("../escape.parquet");

    let result = store.commit(&escaping, |file| {
        file.write_all(b"abc").map_err(|source| Error::Io {
            operation: "test write",
            path: PathBuf::from("test"),
            source,
        })
    });

    assert!(matches!(
        result,
        Err(Error::PathEscapesRoot { kind: "object", .. })
    ));
    assert!(!root.join("../escape.parquet").exists());
    fs::remove_dir_all(root).expect("test temp dir should be removed");
}
