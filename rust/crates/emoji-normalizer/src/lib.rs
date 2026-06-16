//! Shared emoji extraction and normalization.

use serde::{Deserialize, Serialize};
use unicode_segmentation::UnicodeSegmentation;

const TEXT_PRESENTATION_SELECTOR: char = '\u{fe0e}';
const EMOJI_PRESENTATION_SELECTOR: char = '\u{fe0f}';
const HEART: &str = "\u{2764}";
const HEART_EMOJI_STYLE: &str = "\u{2764}\u{fe0f}";
const NORMALIZER_NAME: &str = "emoji-normalizer";
const EMOJI_DATA_SOURCE: &str = "emojis";

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
        git_rev: option_env!("GIT_REV").unwrap_or("unknown").to_owned(),
        unicode_version: unicode_version.clone(),
        emoji_data_version: format!("{EMOJI_DATA_SOURCE}-{unicode_version}"),
    }
}

/// Extract normalized emoji glyph strings, preserving order and repeated occurrences.
#[must_use]
pub fn extract_emoji_sequence(text: &str) -> Vec<String> {
    if text.is_ascii() {
        return Vec::new();
    }
    text.graphemes(true)
        .filter_map(normalize_emoji_glyph)
        .collect()
}

/// Normalize one already-segmented emoji glyph.
#[must_use]
pub fn normalize_emoji_glyph(glyph: &str) -> Option<String> {
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
    use super::{extract_emoji_sequence, normalize_emoji_glyph, version};

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
}
