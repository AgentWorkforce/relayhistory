//! The scrub rules: a faithful port of `scrubText` from relayhistory-cloud
//! (`packages/relayhistory/src/lib/scrub.ts`), the reference implementation.
//! The golden vectors in `tests/fixtures/scrub-golden-v2.json` are generated
//! from it and pin this port byte for byte.
//!
//! JavaScript semantics are reproduced explicitly, not approximated:
//! - `\s` is JavaScript's whitespace set ([`JS_WS`]), not Unicode `White_Space`.
//! - `\b` and `\w` are ASCII ([`WB`], `[A-Za-z0-9_]`), as in a non-`u` JS regex.
//! - `(?i)` folds ASCII letters only ([`ascii_case_insensitive`]): a non-`u`
//!   JS regex never matches U+017F to `s` or U+212A to `k`, as Rust's
//!   Unicode-aware `(?i)` would.
//! - Lookarounds and leftmost-backtracking match order come from
//!   `fancy-regex`, which matches the way V8 does.
//! - Lengths that matter (the truncation bound) are UTF-16 code units.
//!
//! Every quantifier in front of a literal is bounded, as in the reference: the
//! reference documents an O(n²) pattern that once took 21 s on a 128 KB input.

use std::sync::LazyLock;

use fancy_regex::RegexBuilder;

/// Backtracking budget per match attempt for the lookaround rules. The
/// reference (V8) has none; every rule is bounded so real work is linear, and
/// this only stops a pathological input from running unbounded. Exceeding it
/// fails closed (see `scrub_text`).
const BACKTRACK_LIMIT: usize = 50_000_000;

pub(crate) const REDACTED: &str = "[REDACTED]";

/// JavaScript `\s` (WhiteSpace + LineTerminator), as a class body.
const JS_WS: &str = r"\t\n\x0B\x0C\r \x{A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}\x{FEFF}";

/// ASCII word boundary (`\b` in a non-`u` JavaScript regex), per engine.
const WB_FAST: &str = r"(?-u:\b)";
const WB_FANCY: &str = r"(?:(?<![A-Za-z0-9_])(?=[A-Za-z0-9_])|(?<=[A-Za-z0-9_])(?![A-Za-z0-9_]))";

/// A compiled rule. Rules without lookarounds use the `regex` crate (a
/// linear-time automaton giving the same leftmost-first matches a
/// backtracking engine gives for such patterns); the few that need
/// lookarounds use `fancy-regex`, which backtracks the way V8 does.
pub(crate) enum Engine {
    Fast(regex::Regex),
    Fancy(fancy_regex::Regex),
}

pub(crate) enum Replace {
    /// `$1`/`$2` templates, as in `String.prototype.replace`.
    Template(&'static str),
    /// A function of the whole match.
    Func(fn(&str) -> String),
}

pub(crate) struct Rule {
    pub(crate) re: Engine,
    pub(crate) replace: Replace,
    /// Cheap literal prefilter: skip the rule when none of these occurs in
    /// the ASCII-lowercased text. Needles are lowercase. Empty means always
    /// run.
    pub(crate) needles: &'static [&'static str],
    /// A cheap exact check that the rule can change `text`; `false` skips it.
    pub(crate) gate: fn(&str) -> bool,
}

fn always(_: &str) -> bool {
    true
}

/// Attach a gate to a compiled rule.
fn gated(rule: Result<Rule, String>, gate: fn(&str) -> bool) -> Result<Rule, String> {
    rule.map(|r| Rule { gate, ..r })
}

/// Compile a lookaround-free rule. `\s` is written only inside character
/// classes (`[\s]`, `[^/\s]`) and is expanded to the JavaScript whitespace
/// set there; `\b` becomes an ASCII word boundary.
fn fast(pattern: &str) -> Result<Engine, String> {
    let pattern = ascii_case_insensitive(pattern);
    let pattern = pattern.replace(r"\s", JS_WS).replace(r"\b", WB_FAST);
    regex::Regex::new(&pattern)
        .map(Engine::Fast)
        .map_err(|e| format!("scrub rule {pattern:?}: {e}"))
}

/// Compile a rule that needs lookarounds.
fn fancy(pattern: &str) -> Result<Engine, String> {
    let pattern = ascii_case_insensitive(pattern);
    let pattern = pattern.replace(r"\s", JS_WS).replace(r"\b", WB_FANCY);
    RegexBuilder::new(&pattern)
        .backtrack_limit(BACKTRACK_LIMIT)
        .build()
        .map(Engine::Fancy)
        .map_err(|e| format!("scrub rule {pattern:?}: {e}"))
}

/// Rewrite a leading `(?i)` as explicit ASCII case pairs, the way a
/// JavaScript regex without the `u` flag folds: `s` becomes `[sS]`, and a
/// letter range in a class gains its other case (`A-Z` becomes `A-Za-z`).
/// Escapes and group flags are copied unchanged. Patterns without `(?i)` are
/// returned as is.
fn ascii_case_insensitive(pattern: &str) -> String {
    let Some(body) = pattern.strip_prefix("(?i)") else {
        return pattern.to_string();
    };
    let c: Vec<char> = body.chars().collect();
    let mut out = String::with_capacity(body.len() * 2);
    let (mut i, mut in_class) = (0, false);
    while i < c.len() {
        match c[i] {
            '\\' => {
                out.extend(&c[i..(i + 2).min(c.len())]);
                i += 2;
                continue;
            }
            '(' if !in_class && c.get(i + 1) == Some(&'?') => {
                out.push_str("(?");
                i += 2;
                while i < c.len() && (c[i].is_ascii_alphabetic() || c[i] == '-') {
                    out.push(c[i]);
                    i += 1;
                }
                continue;
            }
            '[' if !in_class => in_class = true,
            ']' if in_class => in_class = false,
            l if l.is_ascii_alphabetic() && in_class => {
                let range = (c.get(i + 1) == Some(&'-'))
                    .then(|| c.get(i + 2).copied())
                    .flatten()
                    .filter(char::is_ascii_alphabetic);
                let (lo, hi) = range.map_or((l, l), |h| (l, h));
                push_range(&mut out, lo, hi);
                push_range(&mut out, swap_case(lo), swap_case(hi));
                i += if range.is_some() { 3 } else { 1 };
                continue;
            }
            l if l.is_ascii_alphabetic() => {
                out.extend(['[', l, swap_case(l), ']']);
                i += 1;
                continue;
            }
            _ => {}
        }
        out.push(c[i]);
        i += 1;
    }
    out
}

fn swap_case(c: char) -> char {
    if c.is_ascii_uppercase() {
        c.to_ascii_lowercase()
    } else {
        c.to_ascii_uppercase()
    }
}

fn push_range(out: &mut String, lo: char, hi: char) {
    out.push(lo);
    if hi != lo {
        out.push('-');
        out.push(hi);
    }
}

fn is_word(c: Option<char>) -> bool {
    c.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Where a JWT header starts inside `header`: the first `eyJ` that is not
/// preceded by a word character and leaves at least 11 characters.
fn jwt_header_start(header: &str) -> Option<usize> {
    let mut start = header.find("eyJ");
    while let Some(s) = start {
        if is_word(header[..s].chars().next_back()) {
            start = header[s + 1..].find("eyJ").map(|n| n + s + 1);
        } else if header.len() - s < 11 {
            return None;
        } else {
            return Some(s);
        }
    }
    None
}

/// Port of `redactJwtsInRun`: redact each `header.payload.signature` JWT in a
/// dot-joined token run. Run characters are ASCII, so byte lengths equal the
/// reference's UTF-16 lengths.
fn redact_jwts_in_run(run: &str) -> String {
    if !run.contains(".eyJ") {
        return run.to_string();
    }
    let segments: Vec<&str> = run.split('.').collect();
    let mut out: Vec<String> = Vec::with_capacity(segments.len());
    let mut i = 0;
    while i < segments.len() {
        let header = segments[i];
        let is_jwt_tail = match (segments.get(i + 1), segments.get(i + 2)) {
            (Some(p), Some(sig)) => p.starts_with("eyJ") && p.len() >= 11 && sig.len() >= 16,
            _ => false,
        };
        if let Some(s) = is_jwt_tail.then(|| jwt_header_start(header)).flatten() {
            out.push(format!("{}{REDACTED}", &header[..s]));
            i += 3;
        } else {
            out.push(header.to_string());
            i += 1;
        }
    }
    out.join(".")
}

/// Port of the secret-assignment replacer: keep the name and the first `:`
/// or `=`, redact the value.
fn redact_assignment(m: &str) -> String {
    // The pattern always contains a separator; without one, keep the match.
    let Some(idx) = m.find([':', '=']) else {
        return m.to_string();
    };
    format!("{}{}{REDACTED}", &m[..idx], &m[idx..=idx])
}

/// The compiled rules, or the first compilation error (which makes
/// `scrub_text` fail closed; `tests::rules_compile` keeps it unreachable).
pub(crate) static RULES: LazyLock<Result<Vec<Rule>, String>> = LazyLock::new(|| {
    use Replace::*;
    let r = |re_: Result<Engine, String>, replace: Replace, needles: &'static [&'static str]| {
        re_.map(|re| Rule {
            re,
            replace,
            needles,
            gate: always,
        })
    };
    [
        r(
            fast(
                // A block with no footer is redacted to the end of the string.
                r"-----BEGIN [A-Z ]*PRIVATE KEY(?: BLOCK)?-----(?s:.)*?(?:-----END [A-Z ]*PRIVATE KEY(?: BLOCK)?-----|$)",
            ),
            Template(REDACTED),
            &["-----begin"],
        ),
        r(
            fancy(r#"(^|[\s"'(=:/])/Users/[^/\s]+(?=/)"#),
            Template("$1~"),
            &["/users/"],
        ),
        r(
            fancy(r#"(^|[\s"'(=:/])/home/[^/\s]+(?=/)"#),
            Template("$1~"),
            &["/home/"],
        ),
        r(
            fancy(r#"(?i)(^|[\s"'(=])([A-Z]:\\Users\\[^\\\s]+)(?=\\)"#),
            Template("$1~"),
            &[":\\users\\"],
        ),
        r(
            fast(r"(?i)\bBearer[\s]+[A-Za-z0-9._~+/=-]{16,}\b"),
            Template("Bearer [REDACTED]"),
            &["bearer"],
        ),
        r(
            fast(r"\b(?:sk|rk|pk|ak)-[A-Za-z0-9_-]{20,}\b"),
            Template(REDACTED),
            &["k-"],
        ),
        r(
            fast(r"\b(?:(?:sk|rk|pk)_(?:live|test)_|whsec_)[A-Za-z0-9]{10,}\b"),
            Template(REDACTED),
            &["_live_", "_test_", "whsec_"],
        ),
        r(
            fast(r"\bgh[pousr]_[A-Za-z0-9_]{20,}\b"),
            Template(REDACTED),
            &["gh"],
        ),
        r(
            fast(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
            Template(REDACTED),
            &["akia", "asia"],
        ),
        r(
            fast(r"\b(?:xox[baprs]|xapp)-[A-Za-z0-9-]{10,}\b"),
            Template(REDACTED),
            &["xox", "xapp-"],
        ),
        r(
            fast(r"\b(?:capy|crsr)_[A-Za-z0-9_-]{16,}"),
            Template(REDACTED),
            &["capy_", "crsr_"],
        ),
        r(
            fast(r"\brelay_(?:ag|pa|ws|id)_[A-Za-z0-9._-]{16,}"),
            Template(REDACTED),
            &["relay_"],
        ),
        r(
            fast(r"(/webhooks/automations/)[A-Za-z0-9._~-]{7,}[A-Za-z0-9_~-]"),
            Template("$1[REDACTED]"),
            &["/webhooks/automations/"],
        ),
        r(
            fast(r"[A-Za-z0-9_.-]{35,}"),
            Func(redact_jwts_in_run),
            &[".eyj"],
        ),
        r(
            fast(r"\brth_(?:at|rt|st)_[A-Za-z0-9_]{8,}\b"),
            Template(REDACTED),
            &["rth_"],
        ),
        r(
            fast(
                r"(?i)([?&](?:code_challenge|code_verifier|state|client_secret|access_token|id_token|refresh_token|assertion)=)[^&\s]*",
            ),
            Template("$1[REDACTED]"),
            &["="],
        ),
        r(
            fast(r"\bAIza[0-9A-Za-z_-]{30,}\b"),
            Template(REDACTED),
            &["aiza"],
        ),
        r(
            fast(
                r#"(?i)\b(?:[A-Za-z0-9]+_){0,8}(?:api[_-]?key|apikey|key|secret|token|password|passwd|pwd)[\s]*[:=][\s]*["'`]?[^"'`\s,;]+"#,
            ),
            Func(redact_assignment),
            &["key", "secret", "token", "pass", "pwd"],
        ),
        r(
            fast(r"(?i)([a-z][a-z0-9+.-]{0,31}://[^:\s/@]+:)[^@\s/]+(@)"),
            Template("$1[REDACTED]$2"),
            &["://"],
        ),
        r(
            fancy(r"(?i)([a-z][a-z0-9+.-]{0,31}://)(?!git@)[^:\s/@?#]+(@)"),
            Template("$1[REDACTED]$2"),
            &["://"],
        ),
        r(
            fancy(
                r"(?i)(?<!://)\b(?!git@[A-Z0-9.-]{1,255}\.[A-Z]{2,24}:[A-Za-z0-9_~./-])[A-Z0-9._%+-]{1,64}@[A-Z0-9.-]{1,255}\.[A-Z]{2,24}\b(?!\.?[A-Z0-9-])",
            ),
            Template(REDACTED),
            &["@"],
        ),
        // `\d`/`\w` are ASCII in the reference, so they are spelled out here.
        gated(
            r(
                fancy(r"(^|[^A-Za-z0-9_.+-])[0-9]+(?:[ -][0-9]+)*(?![A-Za-z0-9_]|[.-][0-9])"),
                Func(super::numbers::redact_cards),
                &[],
            ),
            super::numbers::has_card_length_run,
        ),
        gated(
            r(
                // Parentheses only around an area code right after the
                // country code, so a match cannot run into the next number.
                fancy(
                    r"(^|[^A-Za-z0-9_+])\+[1-9](?:[0-9]{0,2}[ .-]?\([0-9]{1,4}\)(?:[ .-]?[0-9]){4,12}|(?:[ .-]?[0-9]){7,14})(?![A-Za-z0-9_]|[.-]?[0-9])",
                ),
                Template("$1[REDACTED]"),
                &["+"],
            ),
            super::numbers::has_e164_shape,
        ),
        gated(
            r(
                fancy(
                    r"(^|[^A-Za-z0-9_.+-])(?:\([2-9][0-9]{2}\) ?|[2-9][0-9]{2}[.-])[2-9][0-9]{2}[.-][0-9]{4}(?![A-Za-z0-9_]|[.-]?[0-9])",
                ),
                Template("$1[REDACTED]"),
                &["-", "."],
            ),
            super::numbers::has_nanp_shape,
        ),
    ]
    .into_iter()
    .collect()
});

/// Expand `$1`/`$2` like `String.prototype.replace` (an unmatched group is
/// empty; `$$` is a literal `$`). `group(n)` returns capture group `n`.
pub(crate) fn expand_template<'a>(
    template: &str,
    group: impl Fn(usize) -> Option<&'a str>,
) -> String {
    let mut out = String::with_capacity(template.len() + 16);
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '$' {
            out.push(c);
            continue;
        }
        match chars.peek().copied() {
            Some('$') => {
                chars.next();
                out.push('$');
            }
            Some(d @ '1'..='9') => {
                chars.next();
                let n = d.to_digit(10).unwrap_or(0) as usize;
                if let Some(m) = group(n) {
                    out.push_str(m);
                }
            }
            _ => out.push('$'),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #[test]
    fn rules_compile() {
        let rules = super::RULES.as_ref().expect("every scrub rule compiles");
        assert_eq!(rules.len(), 24);
    }

    #[test]
    fn case_insensitive_folds_ascii_only() {
        use super::ascii_case_insensitive as f;
        assert_eq!(f("abc"), "abc");
        assert_eq!(f("(?i)St"), "[Ss][tT]");
        assert_eq!(f(r"(?i)[A-Z0-9._%+-]\\U"), r"[A-Za-z0-9._%+-]\\[Uu]");
        assert_eq!(f(r"(?i)(?<!://)\b(?!git@)"), r"(?<!://)\b(?![gG][iI][tT]@)");
        assert_eq!(f(r"(?i)[^&\s]x"), r"[^&\s][xX]");
    }
}
