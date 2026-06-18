use std::path::PathBuf;

use sha2::{Digest, Sha256};

const ARCHIVE_OBJECT_ENCODING_ID: &str = "archive_object_v2";

pub(super) fn stable_artifact_stem(did: &str, dataset: &str, content_hash: &str) -> String {
    format!(
        "{}.{}.{}.{}",
        safe_file_component(did),
        safe_file_component(dataset),
        ARCHIVE_OBJECT_ENCODING_ID,
        content_hash
    )
}

pub(super) fn stable_manifest_path(run_id: &str, shard: &str) -> PathBuf {
    PathBuf::from("manifests")
        .join(safe_file_component(run_id))
        .join(format!("{}.jsonl", safe_file_component(shard)))
}

pub(super) fn stable_repo_receipt_name(did: &str, receipt_hash: &str) -> String {
    format!("{}.{}.receipt.json", safe_file_component(did), receipt_hash)
}

pub(super) fn stable_object_receipt_path(
    artifact_stem: &str,
    receipt_hash: &str,
    suffix: &str,
) -> PathBuf {
    PathBuf::from(format!(
        "{artifact_stem}.receipts/{receipt_hash}.{suffix}.object-receipt.json"
    ))
}

pub(super) fn safe_file_component(value: &str) -> String {
    let mut safe = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            safe.push(ch);
        } else {
            safe.push('_');
        }
    }
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    let digest = hex::encode(hasher.finalize());
    safe.push_str("__");
    safe.extend(digest.chars().take(16));
    safe
}
