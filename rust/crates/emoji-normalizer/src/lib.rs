//! Shared emoji extraction and normalization.

use std::{collections::HashMap, sync::LazyLock};

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
const LEGACY_EMOJI_SOURCE: &str = include_str!("../../../../packages/emoji-normalization/emoji.ts");
const LEGACY_VARIATION_SEQUENCE_SOURCE: &str =
    include_str!("../../../../packages/emoji-normalization/emojiVariationSequences.ts");
#[cfg(test)]
const LEGACY_TOP_LEVEL_NON_QUALIFIED_COUNT: usize = 365;
#[cfg(test)]
const LEGACY_VARIATION_SEQUENCE_COUNT: usize = 353;

static LEGACY_NORMALIZATION: LazyLock<HashMap<String, String>> =
    LazyLock::new(build_legacy_normalization);

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
    if let Some(normalized) = legacy_normalize_glyph(glyph) {
        return Some(normalized);
    }
    if is_emoji_modifier(glyph) {
        return Some(glyph.to_owned());
    }
    if is_emoji_component(glyph) {
        return Some(glyph.to_owned());
    }
    if let Some(heart) = normalize_heart(glyph) {
        return Some(heart);
    }

    if emojis::get(glyph).is_some() {
        return Some(glyph.to_owned());
    }

    if glyph.contains(TEXT_PRESENTATION_SELECTOR) {
        let emoji_style = glyph.replace(TEXT_PRESENTATION_SELECTOR, "\u{fe0f}");
        if emojis::get(&emoji_style).is_some() {
            return Some(emoji_style);
        }

        let stripped = glyph.replace(TEXT_PRESENTATION_SELECTOR, "");
        if emojis::get(&stripped).is_some() {
            return Some(stripped);
        }
    }

    None
}

fn legacy_normalize_glyph(glyph: &str) -> Option<String> {
    let key = codepoint_key(glyph);
    let normalized = LEGACY_NORMALIZATION.get(&key)?;
    if normalized == &key {
        return None;
    }
    codepoint_key_to_string(normalized)
}

fn build_legacy_normalization() -> HashMap<String, String> {
    let mut normalization = HashMap::new();
    for block in top_level_object_blocks(LEGACY_VARIATION_SEQUENCE_SOURCE) {
        let (Some(code), Some(text_style), Some(emoji_style)) = (
            quoted_field(&block, "code"),
            quoted_field(&block, "textStyle"),
            quoted_field(&block, "emojiStyle"),
        ) else {
            continue;
        };
        let normalized_code = normalize_codepoint_key(&code);
        if is_keycap_ascii_codepoint(&normalized_code) {
            continue;
        }
        let normalized_text_style = normalize_codepoint_key(&text_style);
        let normalized_emoji_style = normalize_codepoint_key(&emoji_style);
        normalization.insert(normalized_code, normalized_emoji_style.clone());
        normalization.insert(normalized_text_style, normalized_emoji_style);
    }

    for block in top_level_object_blocks(LEGACY_EMOJI_SOURCE) {
        let (Some(unified), Some(non_qualified)) = (
            quoted_field(&block, "unified"),
            quoted_field(&block, "non_qualified"),
        ) else {
            continue;
        };
        normalization.insert(
            normalize_codepoint_key(&non_qualified),
            normalize_codepoint_key(&unified),
        );
    }
    normalization
}

fn top_level_object_blocks(source: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current = Vec::new();
    for line in source.lines() {
        if line == "  {" {
            current.clear();
            current.push(line);
        } else if !current.is_empty() {
            current.push(line);
            if matches!(line, "  }," | "  }") {
                blocks.push(current.join("\n"));
                current.clear();
            }
        }
    }
    blocks
}

fn quoted_field(block: &str, field: &str) -> Option<String> {
    let needle = format!("    {field}: '");
    let line = block.lines().find(|line| line.starts_with(&needle))?;
    let start = needle.len();
    let remainder = line.get(start..)?;
    let end = remainder.find('\'')?;
    Some(remainder.get(..end)?.to_owned())
}

fn normalize_codepoint_key(value: &str) -> String {
    value.replace('-', " ").to_ascii_lowercase()
}

fn is_keycap_ascii_codepoint(value: &str) -> bool {
    matches!(
        value,
        "0023"
            | "002a"
            | "0030"
            | "0031"
            | "0032"
            | "0033"
            | "0034"
            | "0035"
            | "0036"
            | "0037"
            | "0038"
            | "0039"
    )
}

fn codepoint_key(value: &str) -> String {
    value
        .chars()
        .map(|ch| format!("{:04x}", u32::from(ch)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn codepoint_key_to_string(value: &str) -> Option<String> {
    let mut codepoints = Vec::new();
    for part in value.split(' ') {
        let codepoint = u32::from_str_radix(part, 16).ok()?;
        codepoints.push(char::from_u32(codepoint)?);
    }
    Some(codepoints.into_iter().collect())
}

fn is_keycap_without_combining_mark(glyph: &str) -> bool {
    let mut chars = glyph.chars();
    matches!(chars.next(), Some('0'..='9' | '#' | '*'))
        && matches!(
            chars.next(),
            Some(EMOJI_PRESENTATION_SELECTOR | TEXT_PRESENTATION_SELECTOR)
        )
        && !glyph.contains(COMBINING_ENCLOSING_KEYCAP)
}

fn is_emoji_modifier(glyph: &str) -> bool {
    let mut chars = glyph.chars();
    matches!(chars.next().map(u32::from), Some(0x1f3fb..=0x1f3ff)) && chars.next().is_none()
}

fn is_emoji_component(glyph: &str) -> bool {
    let mut chars = glyph.chars();
    matches!(chars.next().map(u32::from), Some(0x1f9b0..=0x1f9b3)) && chars.next().is_none()
}

fn normalize_heart(glyph: &str) -> Option<String> {
    let suffix = glyph.strip_prefix(HEART)?;
    let suffix = suffix
        .strip_prefix(TEXT_PRESENTATION_SELECTOR)
        .or_else(|| suffix.strip_prefix(EMOJI_PRESENTATION_SELECTOR))
        .unwrap_or(suffix);
    let candidate = format!("{HEART_EMOJI_STYLE}{suffix}");
    emojis::get(&candidate).map(|_emoji| candidate)
}

fn unicode_version_label(version: emojis::UnicodeVersion) -> String {
    format!("{}.{}", version.major(), version.minor())
}

#[cfg(test)]
mod tests {
    use super::{
        EMOJI_MAX_PER_POST, LEGACY_EMOJI_SOURCE, LEGACY_NORMALIZATION,
        LEGACY_TOP_LEVEL_NON_QUALIFIED_COUNT, LEGACY_VARIATION_SEQUENCE_COUNT,
        LEGACY_VARIATION_SEQUENCE_SOURCE, codepoint_key_to_string, extract_emoji_sequence,
        is_keycap_ascii_codepoint, normalize_codepoint_key, normalize_emoji_glyph, quoted_field,
        top_level_object_blocks, version,
    };

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
    fn matches_legacy_variation_sequence_normalization() {
        assert_eq!(normalize_emoji_glyph("™"), Some("™️".to_owned()));
        assert_eq!(normalize_emoji_glyph("™︎"), Some("™️".to_owned()));
        assert_eq!(normalize_emoji_glyph("☀"), Some("☀️".to_owned()));
        assert_eq!(normalize_emoji_glyph("☺"), Some("☺️".to_owned()));
    }

    #[test]
    fn normalizes_emoji_default_glyphs_like_legacy_normalization() {
        assert_eq!(normalize_emoji_glyph("☕"), Some("☕️".to_owned()));
        assert_eq!(normalize_emoji_glyph("⭐"), Some("⭐️".to_owned()));
        assert_eq!(normalize_emoji_glyph("👍"), Some("👍️".to_owned()));
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
        assert_eq!(normalize_emoji_glyph("⛹🏽‍♀"), Some("⛹🏽‍♀".to_owned()));
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
        assert_eq!(
            extract_emoji_sequence("🦰🦱🦲🦳"),
            vec!["🦰", "🦱", "🦲", "🦳"]
        );
    }

    #[test]
    fn rejects_digit_variation_selector_without_keycap_mark() {
        assert_eq!(extract_emoji_sequence("1\u{fe0f}"), Vec::<String>::new());
        assert_eq!(extract_emoji_sequence("1\u{fe0e}"), Vec::<String>::new());
    }

    #[test]
    fn caps_emoji_sequence_at_legacy_limit() {
        assert_eq!(
            extract_emoji_sequence(&"😀".repeat(EMOJI_MAX_PER_POST + 1)).len(),
            EMOJI_MAX_PER_POST
        );
    }

    #[test]
    fn normalizes_legacy_datasource_forms_like_typescript_batch_normalizer() {
        let emoji_blocks = top_level_object_blocks(LEGACY_EMOJI_SOURCE);
        let variation_blocks = top_level_object_blocks(LEGACY_VARIATION_SEQUENCE_SOURCE);
        assert_eq!(
            emoji_blocks
                .iter()
                .filter(|block| quoted_field(block, "non_qualified").is_some())
                .count(),
            LEGACY_TOP_LEVEL_NON_QUALIFIED_COUNT
        );
        assert_eq!(
            variation_blocks
                .iter()
                .filter(|block| {
                    quoted_field(block, "code").is_some()
                        && quoted_field(block, "textStyle").is_some()
                        && quoted_field(block, "emojiStyle").is_some()
                })
                .count(),
            LEGACY_VARIATION_SEQUENCE_COUNT
        );

        for block in emoji_blocks {
            if let Some(unified) = quoted_field(&block, "unified") {
                assert_legacy_normalization(&unified);
            }
            if let Some(non_qualified) = quoted_field(&block, "non_qualified") {
                assert_legacy_normalization(&non_qualified);
            }
        }

        for block in variation_blocks {
            for field in ["code", "textStyle", "emojiStyle"] {
                if let Some(codepoints) = quoted_field(&block, field) {
                    if is_keycap_ascii_sequence(&normalize_codepoint_key(&codepoints)) {
                        continue;
                    }
                    assert_legacy_normalization(&codepoints);
                }
            }
        }
    }

    fn assert_legacy_normalization(codepoints: &str) {
        let key = normalize_codepoint_key(codepoints);
        let Some(glyph) = codepoint_key_to_string(&key) else {
            panic!("invalid legacy codepoints: {codepoints}");
        };
        let expected_key = LEGACY_NORMALIZATION.get(&key).unwrap_or(&key);
        let Some(expected) = codepoint_key_to_string(expected_key) else {
            panic!("invalid expected legacy codepoints: {expected_key}");
        };
        assert_eq!(
            normalize_emoji_glyph(&glyph),
            Some(expected),
            "{codepoints}"
        );
    }

    fn is_keycap_ascii_sequence(value: &str) -> bool {
        let Some(first) = value.split(' ').next() else {
            return false;
        };
        is_keycap_ascii_codepoint(first)
    }
}
