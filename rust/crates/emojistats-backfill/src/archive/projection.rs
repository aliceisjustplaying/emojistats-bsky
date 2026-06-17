use std::collections::HashMap;

use super::{ArchiveError, ArchivePostRow, EmojiProjectionRow};

/// Borrowed projection row derived from one archive post row.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct BorrowedEmojiProjectionRow<'a> {
    pub did: &'a str,
    pub rkey: &'a str,
    pub cid: &'a str,
    pub created_at_normalized: Option<&'a str>,
    pub created_at_parse_status: super::CreatedAtParseStatus,
    pub emoji: &'a str,
    pub occurrences: u64,
    pub langs: &'a [String],
}

/// Derive compact emoji projection rows for archive post rows.
///
/// # Errors
///
/// Returns [`ArchiveError`] if occurrence counters overflow.
pub fn derive_emoji_projection_rows(
    rows: &[ArchivePostRow],
) -> Result<Vec<EmojiProjectionRow>, ArchiveError> {
    let mut projected = Vec::new();
    for row in rows {
        projected.extend(emoji_projection_rows_for_post(row)?);
    }
    Ok(projected)
}

/// Derive compact emoji projection rows for one archive post row.
///
/// # Errors
///
/// Returns [`ArchiveError`] if occurrence counters overflow.
pub fn emoji_projection_rows_for_post(
    row: &ArchivePostRow,
) -> Result<Vec<EmojiProjectionRow>, ArchiveError> {
    Ok(borrowed_emoji_projection_rows_for_post(row)?
        .into_iter()
        .map(|row| EmojiProjectionRow {
            did: row.did.to_owned(),
            rkey: row.rkey.to_owned(),
            cid: row.cid.to_owned(),
            created_at_normalized: row.created_at_normalized.map(ToOwned::to_owned),
            created_at_parse_status: row.created_at_parse_status,
            emoji: row.emoji.to_owned(),
            occurrences: row.occurrences,
            langs: row.langs.to_vec(),
        })
        .collect())
}

/// Derive borrowed compact emoji projection rows for one archive post row.
///
/// # Errors
///
/// Returns [`ArchiveError`] if occurrence counters overflow.
pub fn borrowed_emoji_projection_rows_for_post(
    row: &ArchivePostRow,
) -> Result<Vec<BorrowedEmojiProjectionRow<'_>>, ArchiveError> {
    const MAP_THRESHOLD: usize = 16;
    let mut rows: Vec<BorrowedEmojiProjectionRow<'_>> = Vec::new();
    if row.emoji_sequence.len() <= MAP_THRESHOLD {
        for emoji in &row.emoji_sequence {
            if let Some(existing) =
                rows.iter_mut()
                    .find(|candidate: &&mut BorrowedEmojiProjectionRow<'_>| {
                        candidate.emoji == emoji.as_str()
                    })
            {
                increment_occurrences(&mut existing.occurrences)?;
            } else {
                rows.push(borrowed_projection_row(row, emoji.as_str(), 1));
            }
        }
        return Ok(rows);
    }

    let mut indexes: HashMap<&str, usize> = HashMap::new();
    for emoji in &row.emoji_sequence {
        if let Some(index) = indexes.get(emoji.as_str()).copied() {
            let existing = rows.get_mut(index).ok_or(ArchiveError::CountOverflow {
                field: "emoji_row_index",
            })?;
            increment_occurrences(&mut existing.occurrences)?;
        } else {
            indexes.insert(emoji.as_str(), rows.len());
            rows.push(borrowed_projection_row(row, emoji.as_str(), 1));
        }
    }

    Ok(rows)
}

fn borrowed_projection_row<'a>(
    row: &'a ArchivePostRow,
    emoji: &'a str,
    occurrences: u64,
) -> BorrowedEmojiProjectionRow<'a> {
    BorrowedEmojiProjectionRow {
        did: row.did.as_str(),
        rkey: row.rkey.as_str(),
        cid: row.cid.as_str(),
        created_at_normalized: row.created_at_normalized.as_deref(),
        created_at_parse_status: row.created_at_parse_status,
        emoji,
        occurrences,
        langs: &row.langs,
    }
}

fn increment_occurrences(value: &mut u64) -> Result<(), ArchiveError> {
    *value = value.checked_add(1).ok_or(ArchiveError::CountOverflow {
        field: "emoji_occurrences",
    })?;
    Ok(())
}
