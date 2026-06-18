use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use sha2::{Digest, Sha256};

use super::{
    CommandError, Error, RcloneStorageBoxCommands, SshStorageBoxCommands, StorageBoxBackend,
    StorageBoxCommands, StorageBoxConfig, StorageBoxRcloneConfig, StorageBoxSshConfig,
};
use crate::{
    archive::{
        ArchiveCommitContext, ArchivePostRow, CompletenessClass, CreatedAtParseStatus, FetchMethod,
        NormalizerVersion, RepoReceipt, RepoReceiptInput, build_repo_receipt, current_normalizer,
        write_local_archive_artifacts,
    },
    commit::{ManifestEntry, ManifestMode, Metadata, Request},
    manifest_derive::{
        ManifestReadItem, debug_materialize_clickhouse_batch, stream_committed_jsonl,
        verify_loader_input_for_streaming,
    },
};

const CANARY_DID: &str = "did:plc:storageboxcanary";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Operation {
    name: &'static str,
    path: String,
    target: Option<String>,
}

#[derive(Debug, Default)]
struct FakeCommands {
    files: BTreeMap<String, Vec<u8>>,
    operations: Vec<Operation>,
    upload_limit: Option<usize>,
}

#[derive(Debug, Default)]
struct LocalRemoteCommands;

impl StorageBoxCommands for FakeCommands {
    fn upload(&mut self, remote_path: &str, bytes: &[u8]) -> Result<(), CommandError> {
        self.operations.push(Operation {
            name: "upload",
            path: remote_path.to_owned(),
            target: None,
        });
        let stored = self.upload_limit.map_or_else(
            || bytes.to_vec(),
            |limit| bytes.iter().copied().take(limit).collect(),
        );
        self.files.insert(remote_path.to_owned(), stored);
        Ok(())
    }

    fn upload_reader(
        &mut self,
        remote_path: &str,
        reader: &mut (dyn Read + Send),
    ) -> Result<(), CommandError> {
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(|error| CommandError::new(format!("upload source read failed: {error}")))?;
        self.upload(remote_path, &bytes)
    }

    fn stat_len(&mut self, remote_path: &str) -> Result<Option<u64>, CommandError> {
        self.operations.push(Operation {
            name: "stat",
            path: remote_path.to_owned(),
            target: None,
        });
        self.files
            .get(remote_path)
            .map(|bytes| u64::try_from(bytes.len()))
            .transpose()
            .map_err(|_error| CommandError::new("test file too large"))
    }

    fn sha256(&mut self, remote_path: &str) -> Result<Option<String>, CommandError> {
        self.operations.push(Operation {
            name: "sha256",
            path: remote_path.to_owned(),
            target: None,
        });
        Ok(self.files.get(remote_path).map(|bytes| {
            let mut hasher = Sha256::new();
            hasher.update(bytes);
            hex::encode(hasher.finalize())
        }))
    }

    fn read_prefix(
        &mut self,
        remote_path: &str,
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>, CommandError> {
        self.operations.push(Operation {
            name: "read_prefix",
            path: remote_path.to_owned(),
            target: None,
        });
        Ok(self
            .files
            .get(remote_path)
            .map(|bytes| bytes.iter().copied().take(max_bytes).collect::<Vec<u8>>()))
    }

    fn remove(&mut self, remote_path: &str) -> Result<(), CommandError> {
        self.operations.push(Operation {
            name: "remove",
            path: remote_path.to_owned(),
            target: None,
        });
        self.files.remove(remote_path);
        Ok(())
    }

    fn rename(&mut self, from: &str, to: &str) -> Result<(), CommandError> {
        self.operations.push(Operation {
            name: "rename",
            path: from.to_owned(),
            target: Some(to.to_owned()),
        });
        if self.files.contains_key(to) {
            return Err(CommandError::new("final path already exists"));
        }
        let bytes = self
            .files
            .remove(from)
            .ok_or_else(|| CommandError::new("missing rename source"))?;
        self.files.insert(to.to_owned(), bytes);
        Ok(())
    }

    fn append_manifest_record_if_missing(
        &mut self,
        remote_path: &str,
        record_without_newline: &[u8],
    ) -> Result<(), CommandError> {
        self.operations.push(Operation {
            name: "append_manifest_record_if_missing",
            path: remote_path.to_owned(),
            target: None,
        });
        let present = self.files.get(remote_path).is_some_and(|bytes| {
            bytes
                .split(|byte| *byte == b'\n')
                .any(|line| line == record_without_newline)
        });
        if !present {
            let file = self.files.entry(remote_path.to_owned()).or_default();
            file.extend_from_slice(record_without_newline);
            file.push(b'\n');
        }
        Ok(())
    }

    fn contains_manifest_record(
        &mut self,
        remote_path: &str,
        record_without_newline: &[u8],
    ) -> Result<bool, CommandError> {
        self.operations.push(Operation {
            name: "contains_manifest_record",
            path: remote_path.to_owned(),
            target: None,
        });
        Ok(self.files.get(remote_path).is_some_and(|bytes| {
            bytes
                .split(|byte| *byte == b'\n')
                .any(|line| line == record_without_newline)
        }))
    }
}

impl StorageBoxCommands for LocalRemoteCommands {
    fn upload(&mut self, remote_path: &str, bytes: &[u8]) -> Result<(), CommandError> {
        write_remote(remote_path, bytes)
    }

    fn upload_reader(
        &mut self,
        remote_path: &str,
        reader: &mut (dyn Read + Send),
    ) -> Result<(), CommandError> {
        let path = Path::new(remote_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(command_error)?;
        }
        let mut file = File::create(path).map_err(command_error)?;
        std::io::copy(reader, &mut file).map_err(command_error)?;
        file.sync_all().map_err(command_error)?;
        Ok(())
    }

    fn stat_len(&mut self, remote_path: &str) -> Result<Option<u64>, CommandError> {
        match fs::metadata(remote_path) {
            Ok(metadata) => Ok(Some(metadata.len())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(command_error(error)),
        }
    }

    fn sha256(&mut self, remote_path: &str) -> Result<Option<String>, CommandError> {
        let mut file = match File::open(remote_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(command_error(error)),
        };
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 65_536].into_boxed_slice();
        loop {
            let read = file.read(&mut buffer).map_err(command_error)?;
            if read == 0 {
                break;
            }
            let chunk = buffer
                .get(..read)
                .expect("read byte count should fit buffer");
            hasher.update(chunk);
        }
        Ok(Some(hex::encode(hasher.finalize())))
    }

    fn read_prefix(
        &mut self,
        remote_path: &str,
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>, CommandError> {
        let mut file = match File::open(remote_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(command_error(error)),
        };
        let mut prefix = vec![0_u8; max_bytes];
        let read = file.read(&mut prefix).map_err(command_error)?;
        prefix.truncate(read);
        Ok(Some(prefix))
    }

    fn remove(&mut self, remote_path: &str) -> Result<(), CommandError> {
        match fs::remove_file(remote_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(command_error(error)),
        }
    }

    fn rename(&mut self, from: &str, to: &str) -> Result<(), CommandError> {
        if Path::new(to).exists() {
            return Err(CommandError::new("final path already exists"));
        }
        if let Some(parent) = Path::new(to).parent() {
            fs::create_dir_all(parent).map_err(command_error)?;
        }
        fs::rename(from, to).map_err(command_error)
    }

    fn append_manifest_record_if_missing(
        &mut self,
        remote_path: &str,
        record_without_newline: &[u8],
    ) -> Result<(), CommandError> {
        let present = read_remote(remote_path)?.is_some_and(|bytes| {
            bytes
                .split(|byte| *byte == b'\n')
                .any(|line| line == record_without_newline)
        });
        if present {
            return Ok(());
        }
        if let Some(parent) = Path::new(remote_path).parent() {
            fs::create_dir_all(parent).map_err(command_error)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(remote_path)
            .map_err(command_error)?;
        file.write_all(record_without_newline)
            .map_err(command_error)?;
        file.write_all(b"\n").map_err(command_error)?;
        file.sync_all().map_err(command_error)
    }

    fn contains_manifest_record(
        &mut self,
        remote_path: &str,
        record_without_newline: &[u8],
    ) -> Result<bool, CommandError> {
        Ok(read_remote(remote_path)?.is_some_and(|bytes| {
            bytes
                .split(|byte| *byte == b'\n')
                .any(|line| line == record_without_newline)
        }))
    }
}

fn command_error(error: impl std::fmt::Display) -> CommandError {
    CommandError::new(error.to_string())
}

fn write_remote(remote_path: &str, bytes: &[u8]) -> Result<(), CommandError> {
    let path = Path::new(remote_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(command_error)?;
    }
    fs::write(path, bytes).map_err(command_error)
}

fn read_remote(remote_path: &str) -> Result<Option<Vec<u8>>, CommandError> {
    match fs::read(remote_path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(command_error(error)),
    }
}

fn normalizer() -> NormalizerVersion {
    NormalizerVersion {
        name: "emoji-normalizer".to_owned(),
        semver: "0.1.0".to_owned(),
        git_rev: "test".to_owned(),
        unicode_version: "16.0".to_owned(),
        emoji_data_version: "16.0".to_owned(),
    }
}

fn metadata() -> Metadata {
    Metadata {
        run_id: "run-1".to_owned(),
        shard: "shard0".to_owned(),
        file_sequence: 42,
        did: "did:plc:test".to_owned(),
        dataset: "raw_archive_posts".to_owned(),
        row_count: 2,
        min_created_at_normalized: Some("2026-06-01T00:00:00Z".to_owned()),
        max_created_at_normalized: Some("2026-06-02T00:00:00Z".to_owned()),
        receipt_hash: "repo-receipt-hash".to_owned(),
        repo_receipt_path: Some("did.repo-receipt-hash.receipt.json".to_owned()),
        normalizer: normalizer(),
        schema_version: 3,
    }
}

fn request() -> Request {
    Request {
        object_path: PathBuf::from("objects/run-1/shard0/42.parquet"),
        receipt_path: PathBuf::from("objects/run-1/shard0/42.receipt.json"),
        manifest_path: PathBuf::from("manifests/raw.jsonl"),
        manifest_mode: ManifestMode::AppendJsonl,
        metadata: metadata(),
    }
}

fn canary_archive_row(rkey: &str, text: &str, emojis: &[&str]) -> ArchivePostRow {
    ArchivePostRow {
        did: CANARY_DID.to_owned(),
        rkey: rkey.to_owned(),
        cid: format!("bafy-{rkey}"),
        normalizer: current_normalizer(),
        account_status: None,
        record_status: None,
        public_content_label: None,
        created_at_raw: Some("2026-06-17T00:00:00Z".to_owned()),
        created_at_normalized: Some("2026-06-17T00:00:00Z".to_owned()),
        created_at_parse_status: CreatedAtParseStatus::Valid,
        text: text.to_owned(),
        langs: vec!["en".to_owned()],
        emoji_sequence: emojis.iter().map(|emoji| (*emoji).to_owned()).collect(),
        extras_json: serde_json::json!({}),
    }
}

fn canary_repo_receipt(rows: &[ArchivePostRow], context: &ArchiveCommitContext) -> RepoReceipt {
    build_repo_receipt(RepoReceiptInput {
        rows,
        observed_at: context.observed_at,
        did: CANARY_DID,
        fetch_method: FetchMethod::GetRepo,
        completeness_class: CompletenessClass::ContentAddressedSnapshot,
        reachable_records_count: u64::try_from(rows.len()).expect("row count should fit u64"),
        reachable_post_records_count: u64::try_from(rows.len()).expect("row count should fit u64"),
        post_decode_error_count: 0,
        profile_row_hash: None,
        mst_root_cid: Some("bafy-mst-canary".to_owned()),
        commit_cid: Some("bafy-commit-canary".to_owned()),
        normalizer: current_normalizer(),
    })
    .expect("repo receipt should build")
}

fn request_from_archive_artifacts(
    archive_root: &Path,
    artifacts: &crate::archive::ArchiveArtifacts,
) -> Request {
    Request {
        object_path: relative_artifact_path(archive_root, &artifacts.parquet_path),
        receipt_path: relative_artifact_path(archive_root, &artifacts.object_receipt_path),
        manifest_path: relative_artifact_path(archive_root, &artifacts.manifest_path),
        manifest_mode: ManifestMode::AppendJsonl,
        metadata: Metadata {
            run_id: artifacts.manifest.run_id.clone(),
            shard: artifacts.manifest.shard.clone(),
            file_sequence: artifacts.manifest.file_sequence,
            did: artifacts.manifest.did.clone(),
            dataset: artifacts.manifest.dataset.clone(),
            row_count: artifacts.manifest.row_count,
            min_created_at_normalized: artifacts.manifest.min_created_at_normalized.clone(),
            max_created_at_normalized: artifacts.manifest.max_created_at_normalized.clone(),
            receipt_hash: artifacts.manifest.receipt_hash.clone(),
            repo_receipt_path: artifacts
                .manifest
                .repo_receipt_path
                .as_ref()
                .map(|path| path.to_string_lossy().into_owned()),
            normalizer: artifacts.manifest.normalizer.clone(),
            schema_version: artifacts.manifest.schema_version,
        },
    }
}

fn relative_artifact_path(root: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(root)
        .expect("artifact should live under archive root")
        .to_path_buf()
}

fn backend(commands: FakeCommands) -> StorageBoxBackend<FakeCommands> {
    let mut config = StorageBoxConfig::new("/storage-box/emojistats");
    config.readback_bytes = 8;
    StorageBoxBackend::new(config, commands)
}

fn ssh_commands() -> SshStorageBoxCommands {
    SshStorageBoxCommands::new(
        StorageBoxSshConfig::new("u123456@u123456.your-storagebox.de")
            .with_ssh_program("/usr/bin/ssh")
            .with_ssh_arg("-p")
            .with_ssh_arg("23"),
    )
}

#[test]
fn remote_only_derive_canary_commits_storage_box_artifacts() {
    let temp = tempfile::TempDir::new().expect("temp dir should be created");
    let local_archive_root = temp.path().join("local-archive");
    let remote_archive_root = temp.path().join("remote-archive");
    let context = ArchiveCommitContext::new("storage-box-canary", "canary0", 1);
    let rows = vec![
        canary_archive_row("a", "hello ✅", &["✅"]),
        canary_archive_row("b", "fire 🔥🔥", &["🔥", "🔥"]),
    ];
    let repo_receipt = canary_repo_receipt(&rows, &context);
    let artifacts = write_local_archive_artifacts(
        &local_archive_root,
        CANARY_DID,
        &context,
        &rows,
        None,
        &repo_receipt,
    )
    .expect("local canary archive should write");
    let request = request_from_archive_artifacts(&local_archive_root, &artifacts);
    let repo_receipt_path = request
        .metadata
        .repo_receipt_path
        .as_ref()
        .map(PathBuf::from)
        .expect("repo receipt path should be advertised");
    let mut storage_config =
        StorageBoxConfig::new(remote_archive_root.to_string_lossy().into_owned());
    storage_config.readback_bytes = 16;
    let mut backend = StorageBoxBackend::new(storage_config, LocalRemoteCommands);

    let remote_repo_receipt_path = backend
        .commit_auxiliary_file(
            &request,
            &repo_receipt_path,
            &artifacts.receipt_path,
            "repo receipt",
        )
        .expect("repo receipt auxiliary should commit");
    let remote_artifact = backend
        .commit_file(&request, &artifacts.parquet_path)
        .expect("parquet object should commit");
    fs::remove_dir_all(&local_archive_root).expect("local staging archive should be removable");

    assert!(Path::new(&remote_artifact.remote_object_path).is_file());
    assert!(Path::new(&remote_artifact.remote_receipt_path).is_file());
    assert!(Path::new(&remote_repo_receipt_path).is_file());
    assert!(Path::new(&remote_artifact.remote_manifest_path).is_file());

    let manifest = File::open(&remote_artifact.remote_manifest_path)
        .expect("remote manifest should be readable");
    let inputs = stream_committed_jsonl(BufReader::new(manifest))
        .map(|item| match item.expect("manifest row should parse") {
            ManifestReadItem::Input(input) => *input,
            ManifestReadItem::Skipped(skip) => panic!("canary manifest skipped: {skip:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(inputs.len(), 1);
    let input = inputs
        .first()
        .expect("canary manifest should contain one input");

    let verified = verify_loader_input_for_streaming(&remote_archive_root, input)
        .expect("streaming derive verifier should start from remote archive root");
    let batch = debug_materialize_clickhouse_batch(&remote_archive_root, input)
        .expect("debug derive batch should load from remote archive root");

    assert_eq!(
        verified.object_path,
        PathBuf::from(&remote_artifact.remote_object_path)
    );
    assert_eq!(verified.repo_receipt.did, CANARY_DID);
    assert_eq!(batch.total_post_counter.posts_processed, 2);
    assert_eq!(batch.total_post_counter.emoji_occurrences, 3);
    assert_eq!(batch.post_rows.len(), 2);
}

#[test]
fn rclone_manifest_append_fails_closed_without_remote_lock() {
    let config = StorageBoxRcloneConfig::new("/run/secrets/rclone.conf", "storagebox");
    let mut commands = RcloneStorageBoxCommands::new(config);
    let error = commands
        .append_manifest_record_if_missing("/storage-box/emojistats/manifest.jsonl", b"{\"x\":1}")
        .expect_err("rclone manifest append should fail closed");

    assert!(
        error
            .to_string()
            .contains("cannot atomically append manifests")
    );
}

#[test]
fn commits_in_verified_remote_order_before_manifest_append() {
    let mut backend = backend(FakeCommands::default());
    let artifact = backend
        .commit_bytes(&request(), b"parquet bytes")
        .expect("remote commit should succeed");
    let commands = backend.into_commands();

    let operation_names: Vec<&str> = commands
        .operations
        .iter()
        .map(|operation| operation.name)
        .collect();
    assert_eq!(
        operation_names,
        vec![
            "upload",
            "stat",
            "sha256",
            "rename",
            "stat",
            "remove",
            "upload",
            "stat",
            "sha256",
            "rename",
            "stat",
            "remove",
            "append_manifest_record_if_missing",
            "contains_manifest_record"
        ]
    );
    assert_eq!(
        commands
            .operations
            .get(3)
            .expect("object rename operation should exist")
            .target
            .as_deref(),
        Some(artifact.remote_object_path.as_str())
    );
    assert_eq!(
        commands
            .operations
            .get(12)
            .expect("manifest append operation should exist")
            .path,
        "/storage-box/emojistats/manifests/raw.jsonl"
    );
    assert!(commands.files.contains_key(&artifact.remote_object_path));
    assert!(commands.files.contains_key(&artifact.remote_receipt_path));

    let manifest_bytes = commands
        .files
        .get(&artifact.remote_manifest_path)
        .expect("manifest should be appended");
    let manifest_line = std::str::from_utf8(manifest_bytes)
        .expect("manifest should be UTF-8")
        .trim_end();
    let manifest_entry: ManifestEntry =
        serde_json::from_str(manifest_line).expect("manifest should decode");
    assert_eq!(manifest_entry, artifact.entry);
    assert_eq!(
        artifact.entry.object_path,
        "objects/run-1/shard0/42.parquet"
    );
    assert!(
        !commands
            .files
            .contains_key(&artifact.remote_temp_object_path)
    );
    assert!(
        !commands
            .files
            .contains_key(&artifact.remote_temp_receipt_path)
    );
}

#[test]
fn commits_local_file_without_buffering_object_in_backend() {
    let mut source = tempfile::NamedTempFile::new().expect("temp source should be created");
    source
        .write_all(b"parquet bytes")
        .expect("temp source should be written");
    source.flush().expect("temp source should be flushed");
    let mut backend = backend(FakeCommands::default());

    let artifact = backend
        .commit_file(&request(), source.path())
        .expect("remote file commit should succeed");

    let commands = backend.into_commands();
    assert_eq!(artifact.entry.bytes, 13);
    assert_eq!(
        commands
            .files
            .get(&artifact.remote_object_path)
            .expect("object should be committed"),
        b"parquet bytes"
    );
    assert!(commands.files.contains_key(&artifact.remote_manifest_path));
}

#[test]
fn commits_auxiliary_file_without_manifest_append() {
    let mut source = tempfile::NamedTempFile::new().expect("temp source should be created");
    source
        .write_all(b"repo receipt")
        .expect("temp source should be written");
    source.flush().expect("temp source should be flushed");
    let mut backend = backend(FakeCommands::default());

    let remote_path = backend
        .commit_auxiliary_file(
            &request(),
            PathBuf::from("did.repo-receipt-hash.receipt.json").as_path(),
            source.path(),
            "repo receipt",
        )
        .expect("auxiliary commit should succeed");

    let commands = backend.into_commands();
    assert_eq!(
        remote_path,
        "/storage-box/emojistats/did.repo-receipt-hash.receipt.json"
    );
    assert_eq!(
        commands
            .files
            .get(&remote_path)
            .expect("repo receipt should be committed"),
        b"repo receipt"
    );
    assert!(
        !commands
            .operations
            .iter()
            .any(|operation| operation.name == "append_manifest_record_if_missing")
    );
}

#[test]
fn final_object_conflict_fails_before_manifest_append() {
    let mut commands = FakeCommands::default();
    commands.files.insert(
        "/storage-box/emojistats/objects/run-1/shard0/42.parquet".to_owned(),
        b"different parquet bytes".to_vec(),
    );
    let mut backend = backend(commands);

    let result = backend.commit_bytes(&request(), b"parquet bytes");

    assert!(matches!(result, Err(Error::FinalExistsConflict { .. })));
    let commands = backend.into_commands();
    assert!(
        !commands
            .operations
            .iter()
            .any(|operation| operation.name == "append_manifest_record_if_missing")
    );
}

#[test]
fn retry_with_existing_final_files_cleans_temps_without_duplicating_manifest() {
    let mut initial_backend = backend(FakeCommands::default());
    let initial_artifact = initial_backend
        .commit_bytes(&request(), b"parquet bytes")
        .expect("initial remote commit should succeed");
    let mut retry_backend = backend(FakeCommands {
        files: initial_backend.into_commands().files,
        ..FakeCommands::default()
    });

    let retry_artifact = retry_backend
        .commit_bytes(&request(), b"parquet bytes")
        .expect("retry should treat exact final files as idempotent");

    let commands = retry_backend.into_commands();
    assert_eq!(retry_artifact.entry, initial_artifact.entry);
    assert!(
        !commands
            .files
            .contains_key(&retry_artifact.remote_temp_object_path)
    );
    assert!(
        !commands
            .files
            .contains_key(&retry_artifact.remote_temp_receipt_path)
    );
    assert_eq!(
        commands
            .operations
            .iter()
            .filter(|operation| operation.name == "remove")
            .count(),
        2
    );
    let manifest = std::str::from_utf8(
        commands
            .files
            .get(&retry_artifact.remote_manifest_path)
            .expect("manifest should exist"),
    )
    .expect("manifest should be UTF-8");
    assert_eq!(manifest.lines().count(), 1);
}

#[test]
fn partial_object_upload_fails_before_rename_or_manifest_append() {
    let mut backend = backend(FakeCommands {
        upload_limit: Some(4),
        ..FakeCommands::default()
    });
    let result = backend.commit_bytes(&request(), b"parquet bytes");
    assert!(matches!(
        result,
        Err(Error::VerifySizeMismatch {
            expected: 13,
            actual: 4,
            ..
        })
    ));
    let commands = backend.into_commands();
    assert!(
        !commands
            .operations
            .iter()
            .any(|operation| operation.name == "rename")
    );
    assert!(
        !commands
            .operations
            .iter()
            .any(|operation| operation.name == "append_manifest_record_if_missing")
    );
    assert!(
        !commands
            .files
            .contains_key("/storage-box/emojistats/objects/run-1/shard0/42.parquet")
    );
    assert!(
        !commands
            .files
            .contains_key("/storage-box/emojistats/manifests/raw.jsonl")
    );
}

#[test]
fn rejects_remote_paths_outside_write_scope() {
    let mut escaping = request();
    escaping.object_path = PathBuf::from("../outside.parquet");
    let mut backend = backend(FakeCommands::default());

    let result = backend.commit_bytes(&escaping, b"parquet bytes");

    assert!(matches!(result, Err(Error::PathEscapesRoot { .. })));
    assert!(backend.into_commands().operations.is_empty());
}

#[test]
fn rejects_dot_components_in_scoped_paths() {
    let mut dot_path = request();
    dot_path.object_path = PathBuf::from("objects/./inside.parquet");
    let mut backend = backend(FakeCommands::default());

    let result = backend.commit_bytes(&dot_path, b"parquet bytes");

    assert!(matches!(result, Err(Error::PathEscapesRoot { .. })));
    assert!(backend.into_commands().operations.is_empty());
}

#[test]
fn rejects_dot_and_dotdot_components_in_remote_root() {
    for root in ["/storage-box/./emojistats", "/storage-box/../emojistats"] {
        let mut backend =
            StorageBoxBackend::new(StorageBoxConfig::new(root), FakeCommands::default());

        let result = backend.commit_bytes(&request(), b"parquet bytes");

        assert!(matches!(result, Err(Error::InvalidRemoteRoot(_))));
        assert!(backend.into_commands().operations.is_empty());
    }
}

#[test]
fn ssh_upload_command_keeps_remote_path_inside_script_argument() {
    let command = ssh_commands()
        .upload_command("/storage-box/emojistats/objects/run 1/quote'$(touch bad);.parquet")
        .expect("upload command should build");

    assert_eq!(command.program, PathBuf::from("/usr/bin/ssh"));
    assert_eq!(
        command.args,
        vec![
            "-p",
            "23",
            "u123456@u123456.your-storagebox.de",
            "umask 077; mkdir -p -- '/storage-box/emojistats/objects/run 1'; cat > '/storage-box/emojistats/objects/run 1/quote'\\''$(touch bad);.parquet'"
        ]
    );
    assert!(command.stdin);
}

#[test]
fn ssh_commands_reject_unsafe_remote_paths() {
    let commands = ssh_commands();

    assert!(commands.upload_command("relative/path").is_err());
    assert!(commands.upload_command("/storage/../outside").is_err());
    assert!(commands.upload_command("/storage/newline\nname").is_err());
}

#[test]
fn ssh_rename_command_does_not_force_overwrite_final_path() {
    let command = ssh_commands()
        .rename_command(
            "/storage-box/emojistats/.tmp/run-1/shard0/42.parquet.tmp",
            "/storage-box/emojistats/objects/run-1/shard0/42.parquet",
        )
        .expect("rename command should build");
    let script = command
        .args
        .last()
        .expect("ssh script should be the final argument");

    assert!(!script.contains("mv -f"));
    assert!(script.contains("mv -n --"));
    assert!(script.contains("final path already exists"));
}

#[test]
fn ssh_manifest_append_command_uses_flock() {
    let command = ssh_commands()
        .append_manifest_record_if_missing_command("/storage-box/emojistats/manifests/raw.jsonl")
        .expect("append command should build");
    let script = command
        .args
        .last()
        .expect("ssh script should be the final argument");

    assert!(command.stdin);
    assert!(script.contains("flock -- /storage-box/emojistats/manifests/raw.jsonl.lock"));
    assert!(script.contains("if [ -e \"$1\" ] && grep -Fqx -- \"$record\" \"$1\""));
    assert!(script.contains("printf \"%s\\n\" \"$record\" >> \"$1\""));
    assert!(!script.contains("cat >> '/storage-box/emojistats/manifests/raw.jsonl'"));
}

#[test]
fn ssh_remove_command_uses_rm_force_for_temp_cleanup() {
    let command = ssh_commands()
        .remove_command("/storage-box/emojistats/.tmp/run-1/shard0/42.parquet.tmp")
        .expect("remove command should build");

    assert_eq!(
        command
            .args
            .last()
            .expect("ssh script should be the final argument"),
        "rm -f -- /storage-box/emojistats/.tmp/run-1/shard0/42.parquet.tmp"
    );
}

#[test]
fn ssh_read_prefix_command_marks_absent_and_present_outputs() {
    let command = ssh_commands()
        .read_prefix_command("/storage-box/emojistats/objects/run-1/42.parquet", 8)
        .expect("read prefix command should build");

    assert_eq!(command.args.len(), 4);
    assert_eq!(
        command
            .args
            .last()
            .expect("ssh script should be the final argument"),
        "if [ -e /storage-box/emojistats/objects/run-1/42.parquet ]; then printf 'present\\n'; head -c 8 -- /storage-box/emojistats/objects/run-1/42.parquet; else printf 'absent\\n'; fi"
    );
}

#[test]
fn ssh_shell_quote_handles_shell_metacharacters() {
    assert_eq!(
        super::ssh::shell_quote("/storage-box/emojistats/path with 'quote'"),
        "'/storage-box/emojistats/path with '\\''quote'\\'''"
    );
}

#[test]
fn ssh_run_command_times_out_and_kills_child() {
    let spec = super::process::CommandSpec {
        operation: "timeout_test",
        program: PathBuf::from("/bin/sh"),
        args: vec!["-c".to_owned(), "sleep 5".to_owned()],
        stdin: false,
        timeout: Duration::from_millis(50),
    };

    let error = super::process::run_command(&spec, None)
        .expect_err("sleeping command should be killed after timeout");

    assert!(error.to_string().contains("timed out"));
    assert!(error.to_string().contains("was killed"));
}
