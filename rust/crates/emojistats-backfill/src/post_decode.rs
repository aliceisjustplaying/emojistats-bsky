use jacquard_api::app_bsky::feed::post::Post;
use smol_str::SmolStr;

use crate::parse::{PostRecordBody, raw_partial_post};

const POST_TYPE: &str = "app.bsky.feed.post";

pub struct DecodedPostBody {
    pub body: PostRecordBody,
    pub typed_decode_failed: bool,
}

pub fn from_cbor(record_bytes: &[u8]) -> Option<DecodedPostBody> {
    match serde_ipld_dagcbor::from_slice::<Post<SmolStr>>(record_bytes) {
        Ok(post) => Some(DecodedPostBody {
            body: PostRecordBody::Typed(Box::new(post)),
            typed_decode_failed: false,
        }),
        Err(_error) => raw_partial_post::from_cbor(record_bytes).map(|mut post| {
            post.typed_decode_failed = true;
            DecodedPostBody {
                body: PostRecordBody::RawPartial(post),
                typed_decode_failed: true,
            }
        }),
    }
}

pub fn from_json_value(mut value: serde_json::Value) -> Option<DecodedPostBody> {
    if let serde_json::Value::Object(object) = &mut value {
        match object.get("$type").and_then(serde_json::Value::as_str) {
            Some(POST_TYPE) | None => {}
            Some(_other) => return None,
        }
        object
            .entry("$type")
            .or_insert_with(|| serde_json::Value::String(POST_TYPE.to_owned()));
    }
    match serde_json::from_value::<Post<SmolStr>>(value.clone()) {
        Ok(post) => Some(DecodedPostBody {
            body: PostRecordBody::Typed(Box::new(post)),
            typed_decode_failed: false,
        }),
        Err(_error) => raw_partial_post::from_json_value(value).map(|mut post| {
            post.typed_decode_failed = true;
            DecodedPostBody {
                body: PostRecordBody::RawPartial(post),
                typed_decode_failed: true,
            }
        }),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn json_post_uses_typed_decode_first() {
        let decoded = from_json_value(json!({
            "$type": "app.bsky.feed.post",
            "createdAt": "2026-06-16T00:00:00Z",
            "text": "hello",
            "langs": ["en"]
        }))
        .expect("decoded post");

        assert!(!decoded.typed_decode_failed);
        assert!(matches!(decoded.body, PostRecordBody::Typed(_)));
    }

    #[test]
    fn json_post_raw_fallback_preserves_malformed_core() {
        let decoded = from_json_value(json!({
            "$type": "app.bsky.feed.post",
            "createdAt": {"bad": true},
            "text": ["not", "text"],
            "langs": ["en", 7],
            "embed": {"kind": "external"}
        }))
        .expect("decoded post");

        assert!(decoded.typed_decode_failed);
        let PostRecordBody::RawPartial(post) = decoded.body else {
            panic!("expected raw partial post");
        };
        assert_eq!(
            post.extras_json,
            json!({
                "_invalid_core": {
                    "createdAt": {"bad": true},
                    "langs": ["en", 7],
                    "text": ["not", "text"]
                },
                "embed": {"kind": "external"}
            })
        );
    }

    #[test]
    fn json_post_rejects_explicit_non_post_type() {
        let decoded = from_json_value(json!({
            "$type": "app.bsky.actor.profile",
            "createdAt": "2026-06-16T00:00:00Z",
            "text": "not a post"
        }));

        assert!(decoded.is_none());
    }

    #[test]
    fn cbor_post_uses_typed_decode_first() {
        let bytes = serde_ipld_dagcbor::to_vec(&json!({
            "$type": "app.bsky.feed.post",
            "createdAt": "2026-06-16T00:00:00Z",
            "text": "hello"
        }))
        .expect("encode post");

        let decoded = from_cbor(&bytes).expect("decoded post");

        assert!(!decoded.typed_decode_failed);
        assert!(matches!(decoded.body, PostRecordBody::Typed(_)));
    }
}
