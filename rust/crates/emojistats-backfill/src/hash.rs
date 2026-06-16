//! Shared hash helpers for receipts and manifests.

use serde::Serialize;
use sha2::{Digest, Sha256};

/// Hash the exact JSON serialization shape used by receipts and manifest checks.
///
/// # Errors
///
/// Returns [`serde_json::Error`] if `value` cannot be serialized.
pub fn hash_serialized_json<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(value)?);
    Ok(hex::encode(hasher.finalize()))
}
