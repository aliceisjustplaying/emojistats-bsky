use std::collections::BTreeMap;

use ipld_core::ipld::Ipld;
use serde::Deserialize;

use crate::parse::RawPartialPostRecord;

pub fn from_cbor(record_bytes: &[u8]) -> Option<RawPartialPostRecord> {
    if let Ok(fields) = serde_ipld_dagcbor::from_slice::<FastPostFields>(record_bytes) {
        return from_fast_cbor_fields(fields);
    }
    let ipld = serde_ipld_dagcbor::from_slice::<Ipld>(record_bytes).ok()?;
    let Ipld::Map(fields) = ipld else {
        return None;
    };
    from_ipld_fields(fields)
}

pub fn from_json_value(value: serde_json::Value) -> Option<RawPartialPostRecord> {
    let serde_json::Value::Object(mut fields) = value else {
        return None;
    };
    let typed_decode_failed = json_typed_decode_failed(&fields);
    let created_at_raw = json_created_at(fields.get("createdAt"));
    let text = fields
        .get("text")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let langs = json_string_array(fields.get("langs"));
    let invalid_core = invalid_json_core_fields(&fields);
    remove_core_fields(&mut fields);
    let mut extras_json = serde_json::Value::Object(fields);
    insert_invalid_core(&mut extras_json, invalid_core);
    Some(RawPartialPostRecord {
        typed_decode_failed,
        created_at_raw,
        text,
        langs,
        extras_json,
    })
}

#[derive(Debug, Deserialize)]
struct FastPostFields {
    #[serde(rename = "$type")]
    _record_type: Option<String>,
    #[serde(rename = "createdAt")]
    created_at: String,
    text: String,
    langs: Option<Vec<String>>,
    #[serde(flatten)]
    extras: BTreeMap<String, Ipld>,
}

fn from_fast_cbor_fields(mut fields: FastPostFields) -> Option<RawPartialPostRecord> {
    fields.extras.remove("$type");
    let extras_json = if fields.extras.is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        ipld_to_json(Ipld::Map(fields.extras))?
    };
    Some(RawPartialPostRecord {
        typed_decode_failed: false,
        created_at_raw: Some(fields.created_at),
        text: Some(fields.text),
        langs: fields.langs.unwrap_or_default(),
        extras_json,
    })
}

fn from_ipld_fields(mut fields: BTreeMap<String, Ipld>) -> Option<RawPartialPostRecord> {
    let typed_decode_failed = ipld_typed_decode_failed(&fields);
    let created_at_raw = ipld_created_at(fields.get("createdAt"));
    let text = fields
        .get("text")
        .and_then(ipld_string)
        .map(ToOwned::to_owned);
    let langs = ipld_string_array(fields.get("langs"));
    let invalid_core = invalid_ipld_core_fields(&fields)?;
    for key in ["$type", "createdAt", "langs", "text"] {
        fields.remove(key);
    }
    let mut extras_json = if fields.is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        ipld_to_json(Ipld::Map(fields))?
    };
    insert_invalid_core(&mut extras_json, invalid_core);
    Some(RawPartialPostRecord {
        typed_decode_failed,
        created_at_raw,
        text,
        langs,
        extras_json,
    })
}

fn ipld_typed_decode_failed(fields: &BTreeMap<String, Ipld>) -> bool {
    if !matches!(fields.get("createdAt"), Some(Ipld::String(_))) {
        return true;
    }
    if !matches!(fields.get("text"), Some(Ipld::String(_))) {
        return true;
    }
    ipld_langs_decode_failed(fields.get("langs"))
}

fn json_typed_decode_failed(fields: &serde_json::Map<String, serde_json::Value>) -> bool {
    if !matches!(fields.get("createdAt"), Some(serde_json::Value::String(_))) {
        return true;
    }
    if !matches!(fields.get("text"), Some(serde_json::Value::String(_))) {
        return true;
    }
    json_langs_decode_failed(fields.get("langs"))
}

fn ipld_langs_decode_failed(value: Option<&Ipld>) -> bool {
    let Some(value) = value else {
        return false;
    };
    if matches!(value, Ipld::Null) {
        return false;
    }
    let Ipld::List(values) = value else {
        return true;
    };
    values.iter().any(|value| !matches!(value, Ipld::String(_)))
}

fn json_langs_decode_failed(value: Option<&serde_json::Value>) -> bool {
    let Some(value) = value else {
        return false;
    };
    let serde_json::Value::Array(values) = value else {
        return true;
    };
    values
        .iter()
        .any(|value| !matches!(value, serde_json::Value::String(_)))
}

fn invalid_ipld_core_fields(
    fields: &BTreeMap<String, Ipld>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let mut invalid = serde_json::Map::new();
    if let Some(value) = fields.get("createdAt")
        && !matches!(value, Ipld::String(_))
    {
        invalid.insert("createdAt".to_owned(), ipld_to_json(value.clone())?);
    }
    if let Some(value) = fields.get("text")
        && !matches!(value, Ipld::String(_))
    {
        invalid.insert("text".to_owned(), ipld_to_json(value.clone())?);
    }
    if let Some(value) = fields.get("langs")
        && ipld_langs_decode_failed(Some(value))
    {
        invalid.insert("langs".to_owned(), ipld_to_json(value.clone())?);
    }
    Some(invalid)
}

fn invalid_json_core_fields(
    fields: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    let mut invalid = serde_json::Map::new();
    if let Some(value) = fields.get("createdAt")
        && !matches!(value, serde_json::Value::String(_))
    {
        invalid.insert("createdAt".to_owned(), value.clone());
    }
    if let Some(value) = fields.get("text")
        && !matches!(value, serde_json::Value::String(_))
    {
        invalid.insert("text".to_owned(), value.clone());
    }
    if let Some(value) = fields.get("langs")
        && json_langs_decode_failed(Some(value))
    {
        invalid.insert("langs".to_owned(), value.clone());
    }
    invalid
}

fn insert_invalid_core(
    extras_json: &mut serde_json::Value,
    invalid_core: serde_json::Map<String, serde_json::Value>,
) {
    if invalid_core.is_empty() {
        return;
    }
    let serde_json::Value::Object(fields) = extras_json else {
        return;
    };
    fields.insert(
        "_invalid_core".to_owned(),
        serde_json::Value::Object(invalid_core),
    );
}

fn ipld_created_at(value: Option<&Ipld>) -> Option<String> {
    match value {
        None | Some(Ipld::Null) => None,
        Some(Ipld::String(value)) => Some(value.clone()),
        Some(value) => ipld_to_json(value.clone()).map(|json| json.to_string()),
    }
}

fn json_created_at(value: Option<&serde_json::Value>) -> Option<String> {
    match value {
        None => None,
        Some(serde_json::Value::String(value)) => Some(value.clone()),
        Some(value) => Some(value.to_string()),
    }
}

fn ipld_string_array(value: Option<&Ipld>) -> Vec<String> {
    let Some(Ipld::List(values)) = value else {
        return Vec::new();
    };
    values
        .iter()
        .filter_map(ipld_string)
        .map(ToOwned::to_owned)
        .collect()
}

fn json_string_array(value: Option<&serde_json::Value>) -> Vec<String> {
    let Some(serde_json::Value::Array(values)) = value else {
        return Vec::new();
    };
    values
        .iter()
        .filter_map(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .collect()
}

const fn ipld_string(value: &Ipld) -> Option<&str> {
    match value {
        Ipld::String(value) => Some(value.as_str()),
        _other => None,
    }
}

fn remove_core_fields(fields: &mut serde_json::Map<String, serde_json::Value>) {
    for key in ["$type", "createdAt", "langs", "text"] {
        fields.remove(key);
    }
}

fn ipld_to_json(ipld: Ipld) -> Option<serde_json::Value> {
    match ipld {
        Ipld::Null => Some(serde_json::Value::Null),
        Ipld::Bool(value) => Some(serde_json::Value::Bool(value)),
        Ipld::Integer(value) => i64::try_from(value)
            .map(serde_json::Number::from)
            .map(serde_json::Value::Number)
            .ok(),
        Ipld::Float(value) => serde_json::Number::from_f64(value).map(serde_json::Value::Number),
        Ipld::String(value) => Some(serde_json::Value::String(value)),
        Ipld::Bytes(value) => Some(serde_json::json!({ "$bytes": hex::encode(value) })),
        Ipld::List(values) => values
            .into_iter()
            .map(ipld_to_json)
            .collect::<Option<Vec<_>>>()
            .map(serde_json::Value::Array),
        Ipld::Map(fields) => {
            let mut json_fields = serde_json::Map::new();
            for (key, value) in fields {
                json_fields.insert(key, ipld_to_json(value)?);
            }
            Some(serde_json::Value::Object(json_fields))
        }
        Ipld::Link(cid) => Some(serde_json::json!({ "$link": cid.to_string() })),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn json_partial_post_preserves_core_fields_and_extras() {
        let post = from_json_value(json!({
            "$type": "app.bsky.feed.post",
            "createdAt": "2026-06-16T00:00:00Z",
            "text": "hello",
            "langs": ["en", 7, "fr"],
            "embed": {"kind": "external"}
        }))
        .expect("raw post");

        assert!(post.typed_decode_failed);
        assert_eq!(post.created_at_raw.as_deref(), Some("2026-06-16T00:00:00Z"));
        assert_eq!(post.text.as_deref(), Some("hello"));
        assert_eq!(post.langs, vec!["en", "fr"]);
        assert_eq!(
            post.extras_json,
            json!({
                "_invalid_core": {"langs": ["en", 7, "fr"]},
                "embed": {"kind": "external"}
            })
        );
    }

    #[test]
    fn cbor_fast_partial_post_preserves_core_fields_and_extras() {
        let bytes = serde_ipld_dagcbor::to_vec(&json!({
            "$type": "app.bsky.feed.post",
            "createdAt": "2026-06-16T00:00:00Z",
            "text": "hello",
            "langs": ["en"],
            "reply": {"root": "ignored"}
        }))
        .expect("encode post");

        let post = from_cbor(&bytes).expect("raw post");

        assert!(!post.typed_decode_failed);
        assert_eq!(post.created_at_raw.as_deref(), Some("2026-06-16T00:00:00Z"));
        assert_eq!(post.text.as_deref(), Some("hello"));
        assert_eq!(post.langs, vec!["en"]);
        assert_eq!(post.extras_json, json!({"reply": {"root": "ignored"}}));
    }
}
