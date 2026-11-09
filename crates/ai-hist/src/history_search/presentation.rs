//! Match-centered snippets: the window of a match's text that search
//! surfaces show beside the full text.

/// Default snippet window in codepoints; see [`QueryFilter::max_content_chars`].
pub const DEFAULT_SNIPPET_CHARS: usize = 320;

/// The literal terms a snippet centers on: the query split on whitespace,
/// stripped of FTS operators, quotes and a trailing `*`. Negated terms
/// name no literal text; a column filter contributes its operand.
/// Each term with whether it was a prefix (`*`) query: a plain term
/// centers on a token-boundary occurrence, a prefix term on any substring.
fn snippet_terms(terms: &[String]) -> Vec<(String, bool)> {
    let mut out = Vec::new();
    for raw in terms.iter().flat_map(|term| term.split_whitespace()) {
        let mut token = raw.trim().trim_matches(|c| c == '"' || c == '\'' || c == '(' || c == ')');
        if token.is_empty() || token.starts_with('-') {
            continue;
        }
        // The prefix marker comes off first: for `text:"cat"*` the operand
        // is quoted, and unquoting it before stripping `*` would leave a
        // stray quote in the term.
        let mut prefix = false;
        if let Some(stripped) = token.strip_suffix('*') {
            token = stripped;
            prefix = true;
        }
        // A column filter (`text:tailneedle`) names its literal operand
        // after the colon; that operand is what the snippet centers on.
        if token.contains(':') {
            token = token
                .rsplit(':')
                .next()
                .unwrap_or("")
                .trim_matches(|c| c == '"' || c == '\'');
            if token.is_empty() {
                continue;
            }
        }
        if token.is_empty() {
            continue;
        }
        let upper = token.to_ascii_uppercase();
        if upper == "AND" || upper == "OR" || upper == "NOT" {
            continue;
        }
        out.push((token.to_string(), prefix));
    }
    out
}

/// Fold one char for snippet matching the way the FTS5 unicode61
/// tokenizer indexes text: lowercase, strip diacritic marks (`é` -> `e`),
/// and drop combining marks. Letters the tokenizer keeps distinct stay
/// distinct — `ß` does not become `ss`, `æ` does not become `ae` — or the
/// snippet could center on a word the search never matched. Expansions
/// from lowercasing (`İ` -> `i̇`) yield several chars; the caller maps
/// every yielded char back to the original codepoint.
fn fold_char(ch: char) -> Vec<char> {
    let mut out = Vec::new();
    for lowered in ch.to_lowercase() {
        match lowered {
            'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' | 'ă' | 'ą' => out.push('a'),
            'ç' | 'ć' | 'ĉ' | 'ċ' | 'č' => out.push('c'),
            'ď' => out.push('d'),
            'è' | 'é' | 'ê' | 'ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' => out.push('e'),
            'ì' | 'í' | 'î' | 'ï' | 'ĩ' | 'ī' | 'ĭ' | 'į' => out.push('i'),
            'ñ' | 'ń' | 'ņ' | 'ň' => out.push('n'),
            'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ō' | 'ŏ' | 'ő' => out.push('o'),
            'ù' | 'ú' | 'û' | 'ü' | 'ũ' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' => out.push('u'),
            'ý' | 'ÿ' | 'ŷ' => out.push('y'),
            'ğ' => out.push('g'),
            'ĺ' | 'ľ' => out.push('l'),
            'ŕ' | 'ř' => out.push('r'),
            'ś' | 'š' | 'ş' => out.push('s'),
            'ť' | 'ţ' => out.push('t'),
            'ź' | 'ż' | 'ž' => out.push('z'),
            '\u{300}'..='\u{36f}' => {}
            other => match fold_latin_extended_additional(other) {
                Some(base) => out.push(base),
                None => out.push(other),
            },
        }
    }
    out
}

/// Fold the Latin Extended Additional dotted letters (`ḍ` -> `d`) the
/// same way; split out of `fold_char` to keep each match small.
fn fold_latin_extended_additional(ch: char) -> Option<char> {
    Some(match ch {
        'ḃ' | 'ḅ' => 'b',
        'ḋ' | 'ḍ' => 'd',
        'ḟ' => 'f',
        'ġ' => 'g',
        'ḥ' => 'h',
        'ḳ' => 'k',
        'ḷ' => 'l',
        'ṁ' | 'ṃ' => 'm',
        'ṇ' => 'n',
        'ṗ' => 'p',
        'ṛ' => 'r',
        'ṣ' | 'ṡ' => 's',
        'ṭ' => 't',
        'ṿ' => 'v',
        'ẇ' | 'ẁ' | 'ẅ' => 'w',
        'ẋ' => 'x',
        'ỳ' | 'ẏ' => 'y',
        _ => return None,
    })
}

/// Locate the earliest match of any term in the folded text, returned as
/// (start, length) in original codepoints. FTS matches whole tokens, so
/// a plain term prefers the first occurrence that is a whole token
/// (neither neighbor alphanumeric or `_`) — `needle` centers on the
/// standalone word rather than an earlier `needlepoint`. Prefix queries
/// keep plain substring matching, and a substring occurrence remains the
/// fallback when no whole-token occurrence exists.
fn find_match(
    text_lower: &str,
    folded_chars: &[char],
    lowered_to_original: &[usize],
    terms: &[String],
) -> Option<(usize, usize)> {
    // A whole-token match from any term beats a substring fallback from
    // another (`alpha OR beta` must not center on `alphabet` when a real
    // `beta` token occurs later), so the two classes compete separately.
    let mut best_token: Option<(usize, usize)> = None;
    let mut best_fallback: Option<(usize, usize)> = None;
    for (term, prefix) in snippet_terms(terms) {
        let term_lower: String = term.chars().flat_map(fold_char).collect();
        if term_lower.is_empty() {
            continue;
        }
        let term_len = term_lower.chars().count();
        if let Some((lowered_start, is_token)) =
            choose_occurrence(text_lower, folded_chars, &term_lower, prefix)
        {
            let start = lowered_to_original[lowered_start];
            let len = lowered_to_original[lowered_start + term_len - 1] + 1 - start;
            let slot = if is_token {
                &mut best_token
            } else {
                &mut best_fallback
            };
            let better = match *slot {
                None => true,
                Some((best_start, best_len)) => {
                    start < best_start || (start == best_start && len > best_len)
                }
            };
            if better {
                *slot = Some((start, len));
            }
        }
    }
    best_token.or(best_fallback)
}

/// The folded-codepoint offset of one term's chosen occurrence, and
/// whether it is a whole-token match. A plain term prefers the first
/// whole-token occurrence and falls back to the first substring
/// occurrence; a prefix term prefers the first occurrence at a token
/// start. Token boundaries follow the FTS tokenizer: a neighbor that is
/// not alphanumeric separates tokens (underscore included). The
/// codepoint offset is tracked incrementally as the search advances, so
/// many embedded occurrences do not recount the whole prefix each time.
fn choose_occurrence(
    text_lower: &str,
    folded_chars: &[char],
    term_lower: &str,
    prefix: bool,
) -> Option<(usize, bool)> {
    let term_len = term_lower.chars().count();
    let mut search_from = 0;
    let mut counted_to = 0usize;
    let mut counted_chars = 0usize;
    let mut chosen: Option<usize> = None;
    while let Some(rel) = text_lower[search_from..].find(term_lower) {
        let byte_idx = search_from + rel;
        counted_chars += text_lower[counted_to..byte_idx].chars().count();
        counted_to = byte_idx;
        let lowered_start = counted_chars;
        let starts_token =
            lowered_start == 0 || !folded_chars[lowered_start - 1].is_alphanumeric();
        let ends_token = lowered_start + term_len >= folded_chars.len()
            || !folded_chars[lowered_start + term_len].is_alphanumeric();
        if prefix && starts_token || !prefix && starts_token && ends_token {
            return Some((lowered_start, prefix || ends_token));
        }
        if chosen.is_none() {
            chosen = Some(lowered_start);
        }
        // Advance past this occurrence's first char, in bytes: one byte
        // forward can land inside a multibyte character, and the next
        // slice would panic instead of searching on.
        search_from = byte_idx
            + text_lower[byte_idx..]
                .chars()
                .next()
                .map(char::len_utf8)
                .unwrap_or(1);
    }
    chosen.map(|start| (start, false))
}

/// A match-centered window of `text`, at most `max_content_chars` codepoints
/// with any ellipsis inside that cap. Returns the snippet, whether the full
/// text was truncated, and the codepoint offset of the snippet's first real
/// character in `text` (`None` only for empty text). When no literal term
/// occurs — a project-only match, an empty query, a negation or raw FTS with
/// no literal hit — the window is the head of the text at offset 0.
///
/// This is computed here, the lowest layer that still holds the full text:
/// `SearchRow.text` (and so the `prompt` every surface returns, and what
/// `store::search` feeds `resume`/`pack`) always keeps the full text, and
/// the snippet is an additive field beside it.
pub fn compute_snippet(
    text: &str,
    terms: &[String],
    max_content_chars: usize,
) -> (String, bool, Option<usize>) {
    let max = max_content_chars.max(1);
    let chars: Vec<char> = text.chars().collect();
    let total = chars.len();
    if total == 0 {
        return (String::new(), false, None);
    }
    if total <= max {
        return (text.to_string(), false, Some(0));
    }
    // Lowercasing can expand a char (`İ` becomes `i̇`, two codepoints), so a
    // match position counted in the lowered text is not an offset in the
    // original. Build the folded text char by char and remember which
    // original codepoint each folded codepoint came from. Folding also
    // strips Latin diacritics, matching the FTS tokenizer: `cafe` finds
    // `café`.
    let mut text_lower = String::new();
    let mut lowered_to_original: Vec<usize> = Vec::new();
    for (original_index, ch) in text.chars().enumerate() {
        for lowered in fold_char(ch) {
            text_lower.push(lowered);
            lowered_to_original.push(original_index);
        }
    }
    let folded_chars: Vec<char> = text_lower.chars().collect();
    let best = find_match(&text_lower, &folded_chars, &lowered_to_original, terms);
    let Some((match_start, _)) = best else {
        let mut snippet: String = chars[..max - 1].iter().collect();
        snippet.push('\u{2026}');
        return (snippet, true, Some(0));
    };
    // 40% of the window before the match start, 60% from it onward.
    let mut start = match_start.saturating_sub(max * 2 / 5);
    if start + max > total {
        start = total - max;
    }
    let end = start + max;
    let head_truncated = start > 0;
    let tail_truncated = end < total;
    if max < 3 {
        // Too narrow for ellipses: return the raw window at the match
        // instead of slicing a reversed or match-less range.
        let snippet: String = chars[start..end].iter().collect();
        return (snippet, true, Some(start));
    }
    let mut snippet = String::new();
    match (head_truncated, tail_truncated) {
        (true, true) => {
            snippet.push('\u{2026}');
            snippet.extend(&chars[start + 1..end - 1]);
            snippet.push('\u{2026}');
            (snippet, true, Some(start + 1))
        }
        (true, false) => {
            snippet.push('\u{2026}');
            snippet.extend(&chars[start + 1..end]);
            (snippet, true, Some(start + 1))
        }
        (false, true) => {
            snippet.extend(&chars[..max - 1]);
            snippet.push('\u{2026}');
            (snippet, true, Some(0))
        }
        (false, false) => (text.to_string(), false, Some(0)),
    }
}
