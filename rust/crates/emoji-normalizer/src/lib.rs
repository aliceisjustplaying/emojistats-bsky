//! Shared emoji extraction and normalization.

use serde::{Deserialize, Serialize};
use unicode_segmentation::UnicodeSegmentation;

const TEXT_PRESENTATION_SELECTOR: char = '\u{fe0e}';
const EMOJI_PRESENTATION_SELECTOR: char = '\u{fe0f}';
const HEART: &str = "\u{2764}";
const HEART_EMOJI_STYLE: &str = "\u{2764}\u{fe0f}";
const NORMALIZER_NAME: &str = "emoji-normalizer";
const EMOJI_DATA_SOURCE: &str = "emojis";
const ZERO_WIDTH_JOINER: char = '\u{200d}';
const COMBINING_ENCLOSING_KEYCAP: char = '\u{20e3}';
const EMOJI_MAX_PER_POST: usize = 300;

/// Version identity for emoji normalization outputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizerVersion {
    pub name: String,
    pub semver: String,
    pub git_rev: String,
    pub unicode_version: String,
    pub emoji_data_version: String,
}

/// Current normalizer identity.
#[must_use]
pub fn version() -> NormalizerVersion {
    let unicode_version = unicode_version_label(emojis::UNICODE_VERSION);
    NormalizerVersion {
        name: NORMALIZER_NAME.to_owned(),
        semver: env!("CARGO_PKG_VERSION").to_owned(),
        git_rev: git_rev().to_owned(),
        unicode_version: unicode_version.clone(),
        emoji_data_version: format!("{EMOJI_DATA_SOURCE}-{unicode_version}"),
    }
}

#[must_use]
fn git_rev() -> &'static str {
    #[cfg(any(debug_assertions, test))]
    {
        option_env!("GIT_REV").unwrap_or("unknown")
    }
    #[cfg(all(not(debug_assertions), not(test)))]
    {
        env!(
            "GIT_REV",
            "release builds must set GIT_REV for archive reproducibility"
        )
    }
}

/// Extract normalized emoji glyph strings, preserving order and repeated occurrences.
#[must_use]
pub fn extract_emoji_sequence(text: &str) -> Vec<String> {
    if !has_emoji_candidate(text) {
        return Vec::new();
    }
    let mut output = Vec::new();
    for glyph in text.graphemes(true) {
        push_normalized_glyph(&mut output, glyph);
        if output.len() >= EMOJI_MAX_PER_POST {
            break;
        }
    }
    output
}

fn push_normalized_glyph(output: &mut Vec<String>, glyph: &str) {
    if let Some(normalized) = normalize_emoji_glyph(glyph) {
        output.push(normalized);
        return;
    }
    for ch in glyph.chars() {
        if output.len() >= EMOJI_MAX_PER_POST {
            break;
        }
        if matches!(
            ch,
            ZERO_WIDTH_JOINER | TEXT_PRESENTATION_SELECTOR | EMOJI_PRESENTATION_SELECTOR
        ) {
            continue;
        }
        let scalar = ch.to_string();
        if let Some(normalized) = normalize_emoji_glyph(&scalar) {
            output.push(normalized);
        }
    }
}

fn has_emoji_candidate(text: &str) -> bool {
    if text.is_ascii() {
        return false;
    }
    text.chars().any(is_emoji_candidate_char)
}

fn is_emoji_candidate_char(ch: char) -> bool {
    matches!(
        u32::from(ch),
        0xa9
            | 0xae
            | 0x203c
            | 0x2049
            | 0x20e3
            | 0x2122
            | 0x2139
            | 0x2194..=0x21aa
            | 0x231a..=0x231b
            | 0x2328
            | 0x23cf
            | 0x23e9..=0x23f3
            | 0x23f8..=0x23fa
            | 0x24c2
            | 0x25aa..=0x25ab
            | 0x25b6
            | 0x25c0
            | 0x25fb..=0x25fe
            | 0x2600..=0x27bf
            | 0x2934..=0x2935
            | 0x2b05..=0x2b55
            | 0x3030
            | 0x303d
            | 0x3297
            | 0x3299
            | 0xfe0e..=0xfe0f
            | 0x1f000..=0x1faff
    )
}

/// Normalize one already-segmented emoji glyph.
#[must_use]
pub fn normalize_emoji_glyph(glyph: &str) -> Option<String> {
    if is_keycap_without_combining_mark(glyph) {
        return None;
    }
    if is_emoji_modifier(glyph) {
        return Some(glyph.to_owned());
    }
    if let Some(heart) = normalize_heart(glyph) {
        return Some(heart);
    }

    if let Some(exact) = emojis::get(glyph) {
        return Some(exact.as_str().to_owned());
    }

    if glyph.contains(TEXT_PRESENTATION_SELECTOR) {
        let emoji_style = glyph.replace(TEXT_PRESENTATION_SELECTOR, "\u{fe0f}");
        if let Some(exact) = emojis::get(&emoji_style) {
            return Some(exact.as_str().to_owned());
        }

        let stripped = glyph.replace(TEXT_PRESENTATION_SELECTOR, "");
        if let Some(exact) = emojis::get(&stripped) {
            return Some(exact.as_str().to_owned());
        }
    }

    None
}

fn is_keycap_without_combining_mark(glyph: &str) -> bool {
    let mut chars = glyph.chars();
    matches!(chars.next(), Some('0'..='9' | '#' | '*'))
        && chars.next() == Some(EMOJI_PRESENTATION_SELECTOR)
        && !glyph.contains(COMBINING_ENCLOSING_KEYCAP)
}

fn is_emoji_modifier(glyph: &str) -> bool {
    let mut chars = glyph.chars();
    matches!(chars.next().map(u32::from), Some(0x1f3fb..=0x1f3ff)) && chars.next().is_none()
}

fn normalize_heart(glyph: &str) -> Option<String> {
    let suffix = glyph.strip_prefix(HEART)?;
    let suffix = suffix
        .strip_prefix(TEXT_PRESENTATION_SELECTOR)
        .or_else(|| suffix.strip_prefix(EMOJI_PRESENTATION_SELECTOR))
        .unwrap_or(suffix);
    let candidate = format!("{HEART_EMOJI_STYLE}{suffix}");
    emojis::get(&candidate).map(|emoji| emoji.as_str().to_owned())
}

fn unicode_version_label(version: emojis::UnicodeVersion) -> String {
    format!("{}.{}", version.major(), version.minor())
}

#[cfg(test)]
mod tests {
    use super::{EMOJI_MAX_PER_POST, extract_emoji_sequence, normalize_emoji_glyph, version};

    #[test]
    fn extracts_a_plain_emoji() {
        assert_eq!(extract_emoji_sequence("hello 😀 world"), vec!["😀"]);
    }

    #[test]
    fn normalizes_text_style_and_emoji_style_hearts_to_the_same_glyph() {
        let text_style = extract_emoji_sequence("I ❤ you");
        let emoji_style = extract_emoji_sequence("I ❤️ you");
        assert_eq!(text_style, vec!["❤️"]);
        assert_eq!(text_style, emoji_style);
    }

    #[test]
    fn keeps_a_skin_tone_modifier_sequence_as_one_glyph() {
        assert_eq!(extract_emoji_sequence("\u{1f44d}\u{1f3fd}"), vec!["👍🏽"]);
    }

    #[test]
    fn keeps_a_zwj_sequence_as_one_glyph() {
        let family = "\u{1f468}‍\u{1f469}‍\u{1f467}‍\u{1f466}";
        assert_eq!(
            extract_emoji_sequence(&format!("our {family}")),
            vec![family]
        );
    }

    #[test]
    fn keeps_repeated_emoji_as_separate_occurrences() {
        assert_eq!(extract_emoji_sequence("😀 and 😀"), vec!["😀", "😀"]);
    }

    #[test]
    fn returns_empty_for_emoji_less_text() {
        assert!(extract_emoji_sequence("just words :) <3").is_empty());
    }

    #[test]
    fn normalizes_text_presentation_selector() {
        assert_eq!(normalize_emoji_glyph("❤︎"), Some("❤️".to_owned()));
    }

    #[test]
    fn normalizes_non_qualified_keycaps_to_qualified_keycaps() {
        assert_eq!(normalize_emoji_glyph("#\u{20e3}"), Some("#️⃣".to_owned()));
        assert_eq!(normalize_emoji_glyph("*\u{20e3}"), Some("*️⃣".to_owned()));
        assert_eq!(normalize_emoji_glyph("1\u{20e3}"), Some("1️⃣".to_owned()));
    }

    #[test]
    fn normalizes_heart_zwj_sequences_to_emoji_style() {
        assert_eq!(normalize_emoji_glyph("❤‍🔥"), Some("❤️‍🔥".to_owned()));
        assert_eq!(normalize_emoji_glyph("❤‍🩹"), Some("❤️‍🩹".to_owned()));
    }

    #[test]
    fn normalizes_zwj_sequences_with_missing_variation_selectors() {
        assert_eq!(normalize_emoji_glyph("⛹‍♀"), Some("⛹️‍♀️".to_owned()));
        assert_eq!(normalize_emoji_glyph("⛹🏽‍♀"), Some("⛹🏽‍♀️".to_owned()));
    }

    #[test]
    fn keeps_regional_indicator_flags_as_one_glyph() {
        assert_eq!(extract_emoji_sequence("flags 🇺🇸🇯🇵"), vec!["🇺🇸", "🇯🇵"]);
    }

    #[test]
    fn preserves_ts_batch_order_and_duplicates_after_normalization() {
        let text = "mix ❤ 1⃣ 🇺🇸 ❤‍🔥 👍🏽 1⃣";
        assert_eq!(
            extract_emoji_sequence(text),
            vec!["❤️", "1️⃣", "🇺🇸", "❤️‍🔥", "👍🏽", "1️⃣"]
        );
    }

    #[test]
    fn exposes_version_metadata() {
        let metadata = version();
        assert_eq!(metadata.name, "emoji-normalizer");
        assert_eq!(metadata.semver, "0.1.0");
        assert_eq!(metadata.unicode_version, "16.0");
        assert_eq!(metadata.emoji_data_version, "emojis-16.0");
    }

    #[test]
    fn keeps_legacy_invalid_zwj_and_modifier_extraction_semantics() {
        assert_eq!(extract_emoji_sequence("😀‍😀"), vec!["😀", "😀"]);
        assert_eq!(extract_emoji_sequence("😀🏽"), vec!["😀", "🏽"]);
        assert_eq!(extract_emoji_sequence("🏽"), vec!["🏽"]);
    }

    #[test]
    fn rejects_digit_variation_selector_without_keycap_mark() {
        assert_eq!(extract_emoji_sequence("1\u{fe0f}"), Vec::<String>::new());
    }

    #[test]
    fn caps_emoji_sequence_at_legacy_limit() {
        assert_eq!(
            extract_emoji_sequence(&"😀".repeat(EMOJI_MAX_PER_POST + 1)).len(),
            EMOJI_MAX_PER_POST
        );
    }
}
