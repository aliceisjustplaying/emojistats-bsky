use super::{ArchiveError, Serialize};

pub(super) fn json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ArchiveError> {
    Ok(serde_json::to_vec(value)?)
}

pub(super) fn json_string<T: Serialize>(value: &T) -> Result<String, ArchiveError> {
    Ok(serde_json::to_string(value)?)
}

pub(super) fn canonical_json_bytes(value: &serde_json::Value) -> Result<Vec<u8>, ArchiveError> {
    json_bytes(&canonical_json_value(value))
}

pub(super) fn canonical_json_string(value: &serde_json::Value) -> Result<String, ArchiveError> {
    json_string(&canonical_json_value(value))
}

fn canonical_json_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_json_value).collect::<Vec<_>>())
        }
        serde_json::Value::Object(fields) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = fields.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                if let Some(value) = fields.get(key) {
                    sorted.insert(key.clone(), canonical_json_value(value));
                }
            }
            serde_json::Value::Object(sorted)
        }
        other => other.clone(),
    }
}
