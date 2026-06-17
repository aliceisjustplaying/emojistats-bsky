use std::collections::HashSet;

use cid::Cid as IpldCid;
use jacquard_repo::mst::NodeData;

use super::{
    ParseConfig, ParseDeadline, ParseError, ParseVisitError, PostRecord, ProfileRecord, RkeyDigest,
    car::IndexedCarBlockStore,
    checked_add_u64, checked_increment, ensure_u64_at_most,
    record::{DecodeDigest, RecordSinks, extract_known_record, update_digest, validate_repo_key},
};

pub(super) type WalkMstRecordsResult<E> = Result<
    (
        Option<ProfileRecord>,
        Option<String>,
        DecodeDigest,
        RkeyDigest,
    ),
    ParseVisitError<E>,
>;

pub(super) fn walk_mst_records_visit<S, E, F>(
    root: IpldCid,
    store: &IndexedCarBlockStore,
    config: ParseConfig,
    deadline: ParseDeadline,
    state: &mut S,
    visit_post: &mut F,
) -> WalkMstRecordsResult<E>
where
    F: FnMut(&mut S, PostRecord) -> Result<(), E>,
{
    let mut cursor = StreamingMstCursor::new(root, store);
    let mut profile = None;
    let mut profile_decode_error = None;
    let mut decode_digest = DecodeDigest::default();
    let mut digest = RkeyDigest::default();

    while let Some(leaf) = cursor.next_leaf(config)? {
        deadline.ensure_not_exceeded()?;
        let record_bytes = store
            .get_block_bytes(&leaf.cid)
            .map_err(ParseError::Repo)?
            .ok_or_else(|| ParseError::MissingBlock {
                cid: leaf.cid.to_string(),
            })?;
        update_digest(&mut digest, &leaf.key, config)?;
        let mut sinks = RecordSinks {
            state,
            visit_post,
            profile: &mut profile,
            profile_decode_error: &mut profile_decode_error,
            decode_digest: &mut decode_digest,
        };
        extract_known_record(
            &leaf.key,
            leaf.cid,
            record_bytes.as_ref(),
            &mut sinks,
            config,
        )?;
    }

    Ok((profile, profile_decode_error, decode_digest, digest))
}

struct StreamingMstCursor<'a> {
    root: Option<IpldCid>,
    store: &'a IndexedCarBlockStore,
    stack: Vec<StreamingMstFrame>,
    visited_nodes: HashSet<IpldCid>,
    last_leaf_key: Option<String>,
}

impl<'a> StreamingMstCursor<'a> {
    fn new(root: IpldCid, store: &'a IndexedCarBlockStore) -> Self {
        Self {
            root: Some(root),
            store,
            stack: Vec::new(),
            visited_nodes: HashSet::new(),
            last_leaf_key: None,
        }
    }

    fn next_leaf(&mut self, config: ParseConfig) -> Result<Option<StreamingMstLeaf>, ParseError> {
        loop {
            if let Some(root) = self.root.take() {
                self.push_node(root, config)?;
                continue;
            }

            let Some(frame) = self.stack.last_mut() else {
                return Ok(None);
            };
            let Some(item) = frame.next() else {
                self.stack.pop();
                continue;
            };
            match item {
                StreamingMstItem::Tree(cid) => {
                    self.push_node(cid, config)?;
                }
                StreamingMstItem::Leaf { key, cid } => {
                    validate_repo_key(&key)?;
                    self.validate_leaf_order(&key)?;
                    return Ok(Some(StreamingMstLeaf { key, cid }));
                }
            }
        }
    }

    fn push_node(&mut self, cid: IpldCid, config: ParseConfig) -> Result<(), ParseError> {
        if !self.visited_nodes.insert(cid) {
            return Err(ParseError::MalformedCar(format!(
                "MST node CID visited more than once: {cid}"
            )));
        }
        let depth = checked_increment(
            u64::try_from(self.stack.len()).map_err(|_err| ParseError::CarLengthOverflow {
                field: "MST stack depth",
            })?,
            "mst_depth",
        )?;
        ensure_u64_at_most(
            depth,
            config.max_mst_depth,
            "max_mst_depth",
            "raise parser max_mst_depth only after inspecting the repo MST",
        )?;
        let bytes = self
            .store
            .get_block_bytes(&cid)
            .map_err(ParseError::Repo)?
            .ok_or_else(|| ParseError::MissingBlock {
                cid: cid.to_string(),
            })?;
        self.stack
            .push(StreamingMstFrame::decode(bytes.as_ref(), config)?);
        Ok(())
    }

    fn validate_leaf_order(&mut self, key: &str) -> Result<(), ParseError> {
        if let Some(previous) = self.last_leaf_key.as_deref() {
            match key.cmp(previous) {
                std::cmp::Ordering::Less => {
                    return Err(ParseError::MalformedCar(format!(
                        "MST keys out of order: previous={previous}, key={key}"
                    )));
                }
                std::cmp::Ordering::Equal => {
                    return Err(ParseError::MalformedCar(format!(
                        "duplicate MST key: {key}"
                    )));
                }
                std::cmp::Ordering::Greater => {}
            }
        }
        self.last_leaf_key = Some(key.to_owned());
        Ok(())
    }
}

struct StreamingMstFrame {
    items: Vec<StreamingMstItem>,
    index: usize,
}

impl StreamingMstFrame {
    fn decode(bytes: &[u8], config: ParseConfig) -> Result<Self, ParseError> {
        let node: NodeData = serde_ipld_dagcbor::from_slice(bytes).map_err(|source| {
            ParseError::MalformedCar(format!("failed to decode MST node: {source}"))
        })?;
        let mut items = Vec::new();
        let mut decoded_entries = 0_u64;
        let mut decoded_key_bytes = 0_u64;
        if let Some(left) = node.left {
            items.push(StreamingMstItem::Tree(left));
        }
        let mut last_key = String::new();
        for entry in node.entries {
            decoded_entries = checked_increment(decoded_entries, "mst_node_entries")?;
            ensure_u64_at_most(
                decoded_entries,
                config.max_mst_node_entries,
                "max_mst_node_entries",
                "raise parser max_mst_node_entries only after inspecting the repo MST",
            )?;
            let prefix_len = usize::from(entry.prefix_len);
            if !last_key.is_char_boundary(prefix_len) || prefix_len > last_key.len() {
                return Err(ParseError::MalformedCar(
                    "MST entry prefix exceeds previous key".to_owned(),
                ));
            }
            let suffix = std::str::from_utf8(&entry.key_suffix).map_err(|source| {
                ParseError::MalformedCar(format!("invalid UTF-8 in MST key suffix: {source}"))
            })?;
            let key = format!("{}{}", &last_key[..prefix_len], suffix);
            decoded_key_bytes = checked_add_u64(
                decoded_key_bytes,
                u64::try_from(key.len()).map_err(|_err| ParseError::CarLengthOverflow {
                    field: "mst_node_key_bytes",
                })?,
                "mst_node_key_bytes",
            )?;
            ensure_u64_at_most(
                decoded_key_bytes,
                config.max_mst_node_key_bytes,
                "max_mst_node_key_bytes",
                "raise parser max_mst_node_key_bytes only after inspecting the repo MST",
            )?;
            items.push(StreamingMstItem::Leaf {
                key: key.clone(),
                cid: entry.value,
            });
            if let Some(tree) = entry.tree {
                items.push(StreamingMstItem::Tree(tree));
            }
            last_key = key;
        }
        Ok(Self { items, index: 0 })
    }

    fn next(&mut self) -> Option<StreamingMstItem> {
        let item = self.items.get(self.index)?.clone();
        self.index = self.index.checked_add(1)?;
        Some(item)
    }
}

#[derive(Clone)]
enum StreamingMstItem {
    Tree(IpldCid),
    Leaf { key: String, cid: IpldCid },
}

struct StreamingMstLeaf {
    key: String,
    cid: IpldCid,
}
