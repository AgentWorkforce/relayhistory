//! Match-centered snippets: the window of a match's text that search
//! surfaces show beside the full text.

/// Default snippet window in codepoints; see [`QueryFilter::max_content_chars`].
pub const DEFAULT_SNIPPET_CHARS: usize = 320;

/// The literal terms a snippet centers on: the query split on whitespace,
/// stripped of FTS operators, quotes and a trailing `*`. Negated terms and
/// column filters name no literal text, so they never center a snippet.
fn snippet_terms(terms: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for raw in terms.iter().flat_map(|term| term.split_whitespace()) {
        let mut token = raw.trim().trim_matches(|c| c == '"' || c == '\'' || c == '(' || c == ')');
        if token.is_empty() || token.starts_with('-') || token.contains(':') {
            continue;
        }
        if let Some(stripped) = token.strip_suffix('*') {
            token = stripped;
        }
        if token.is_empty() {
            continue;
        }
        let upper = token.to_ascii_uppercase();
        if upper == "AND" || upper == "OR" || upper == "NOT" {
            continue;
        }
        out.push(token.to_string());
    }
    out
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
    // original. Build the lowered text char by char and remember which
    // original codepoint each lowered codepoint came from.
    let mut text_lower = String::new();
    let mut lowered_to_original: Vec<usize> = Vec::new();
    for (original_index, ch) in text.chars().enumerate() {
        for lowered in ch.to_lowercase() {
            text_lower.push(lowered);
            lowered_to_original.push(original_index);
        }
    }
    let mut best: Option<(usize, usize)> = None;
    for term in snippet_terms(terms) {
        let term_lower = term.to_lowercase();
        if term_lower.is_empty() {
            continue;
        }
        if let Some(byte_idx) = text_lower.find(&term_lower) {
            let lowered_start = text_lower[..byte_idx].chars().count();
            let lowered_len = term_lower.chars().count();
            let start = lowered_to_original[lowered_start];
            let len = lowered_to_original[lowered_start + lowered_len - 1] + 1 - start;
            let better = match best {
                None => true,
                Some((best_start, best_len)) => {
                    start < best_start || (start == best_start && len > best_len)
                }
            };
            if better {
                best = Some((start, len));
            }
        }
    }
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
