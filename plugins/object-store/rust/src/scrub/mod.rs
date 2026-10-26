//! PII and secret scrubbing for raw session chunks, applied on the client
//! before a chunk reaches object storage.
//!
//! This runs in the background delivery worker when a chunk is sealed, never
//! on the local ingest/read path, so it adds nothing to the < 5 ms local
//! latency budget. The server's `scrubText` remains the compliance boundary
//! and scrubs again downstream; this is the same rule set applied earlier.
//!
//! - [`scrub_text`]: one string, exactly as relayhistory-cloud's `scrubText`.
//! - [`scrub_json`]: every string value **and key** of a JSON value (as the
//!   server's `scrubStructured` does for structured evidence).
//! - [`scrub_chunk`]: a newline-delimited chunk (`jsonl`, `ndjson`,
//!   `sqlite-rows`): each record is parsed, scrubbed, and re-serialized, so the
//!   output is always valid JSON. A record that cannot be parsed, or whose
//!   keys collide after scrubbing, is replaced by a typed placeholder and is
//!   never passed through unscrubbed.

mod rules;

use std::borrow::Cow;

use serde_json::{Map, Value};

use rules::{expand_template, Engine, Replace, Rule, RULES};

/// Identifies this rule set in `manifest.raw.scrub`.
pub const RULESET: &str = "relayhistory-scrub";
/// Bumped whenever the rules or the golden vectors change.
pub const RULESET_VERSION: u32 = 1;

/// Longest string scrubbed in full, in UTF-16 code units (the reference
/// counts JavaScript string length). Longer strings are truncated first.
pub const MAX_SCRUB_CHARS: usize = 262_144;
const SCRUB_CUT_REWIND: usize = 62_144;
const TRUNCATION_NOTE: &str = "[relayhistory: truncated";
/// Replaces a string the rules could not finish within their budget.
pub const SCRUB_LIMIT_PLACEHOLDER: &str = "[REDACTED: relayhistory scrub limit]";

/// Scrub one string. Byte-for-byte equal to the reference `scrubText`.
pub fn scrub_text(value: &str) -> String {
    let Ok(rules) = RULES.as_ref() else {
        // Unreachable (`rules_compile` test); fail closed rather than pass
        // text through unscrubbed.
        return SCRUB_LIMIT_PLACEHOLDER.to_string();
    };
    let mut text = bound_for_scrub(value).into_owned();
    // ASCII-lowercased view for the literal prefilters, refreshed only after a
    // rule actually changes the text.
    let mut lower = text.to_ascii_lowercase();
    for rule in rules {
        if !rule.needles.is_empty() && !rule.needles.iter().any(|n| lower.contains(n)) {
            continue;
        }
        let replaced = apply(rule, &text);
        match replaced {
            Ok(Cow::Owned(s)) => {
                text = s;
                lower = text.to_ascii_lowercase();
            }
            Ok(Cow::Borrowed(_)) => {}
            // Fail closed: a string the rules cannot finish is withheld whole,
            // never passed through partially scrubbed.
            Err(_) => return SCRUB_LIMIT_PLACEHOLDER.to_string(),
        }
    }
    text
}

/// Run one rule over `text`. `Err` only if a lookaround rule exceeded its
/// backtracking budget.
fn apply<'t>(rule: &Rule, text: &'t str) -> Result<Cow<'t, str>, ()> {
    match (&rule.re, &rule.replace) {
        (Engine::Fast(re), Replace::Template(t)) => Ok(re
            .replace_all(text, |c: &regex::Captures<'_>| {
                expand_template(t, |n| c.get(n).map(|m| m.as_str()))
            })),
        (Engine::Fast(re), Replace::Func(f)) => Ok(re
            .replace_all(text, |c: &regex::Captures<'_>| {
                f(c.get(0).map_or("", |m| m.as_str()))
            })),
        (Engine::Fancy(re), Replace::Template(t)) => re
            .try_replacen(text, 0, |c: &fancy_regex::Captures<'_, str>| {
                expand_template(t, |n| c.get(n).map(|m| m.as_str()))
            })
            .map_err(|_| ()),
        (Engine::Fancy(re), Replace::Func(f)) => re
            .try_replacen(text, 0, |c: &fancy_regex::Captures<'_, str>| {
                f(c.get(0).map_or("", |m| m.as_str()))
            })
            .map_err(|_| ()),
    }
}

/// Whether [`scrub_text`] will truncate `value`.
pub fn exceeds_scrub_bound(value: &str) -> bool {
    utf16_len(value) > MAX_SCRUB_CHARS
}

fn utf16_len(s: &str) -> usize {
    s.chars().map(char::len_utf16).sum()
}

/// Port of `boundForScrub`: cut over-long input at whitespace (within the
/// rewind window), never between the halves of a surrogate pair, and announce
/// the truncation in band.
fn bound_for_scrub(value: &str) -> Cow<'_, str> {
    if value.len() <= MAX_SCRUB_CHARS || utf16_len(value) <= MAX_SCRUB_CHARS {
        return Cow::Borrowed(value);
    }
    let units: Vec<u16> = value.encode_utf16().collect();
    let mut cut = MAX_SCRUB_CHARS;
    let floor = MAX_SCRUB_CHARS - SCRUB_CUT_REWIND;
    while cut > floor && !is_js_ws_unit(units[cut - 1]) {
        cut -= 1;
    }
    let is_high = |u: u16| (0xD800..=0xDBFF).contains(&u);
    let is_low = |u: u16| (0xDC00..=0xDFFF).contains(&u);
    if is_high(units[cut - 1]) && units.get(cut).copied().is_some_and(is_low) {
        cut -= 1;
    }
    let dropped = units.len() - cut;
    // The cut never splits a surrogate pair, so this is exact.
    let kept = String::from_utf16_lossy(&units[..cut]);
    Cow::Owned(format!(
        "{kept}\n{TRUNCATION_NOTE} {dropped} characters over the {MAX_SCRUB_CHARS}-character scrub limit]"
    ))
}

/// JavaScript `\s` for one UTF-16 code unit (every member is in the BMP).
fn is_js_ws_unit(u: u16) -> bool {
    matches!(
        u,
        0x09..=0x0D | 0x20 | 0xA0 | 0x1680 | 0x2000..=0x200A | 0x2028 | 0x2029 | 0x202F | 0x205F
            | 0x3000 | 0xFEFF
    )
}

/// Why a record could not be scrubbed in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dropped {
    /// Not valid JSON (or not valid UTF-8).
    Unparseable,
    /// Two keys became identical after scrubbing.
    KeyCollision,
}

impl Dropped {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unparseable => "unparseable_record",
            Self::KeyCollision => "key_collision",
        }
    }
}

/// Scrub every string value and key. Errors if two keys of one object become
/// identical, so no value is silently lost.
pub fn scrub_json(value: Value) -> Result<Value, Dropped> {
    Ok(match value {
        Value::String(s) => Value::String(scrub_text(&s)),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(scrub_json)
                .collect::<Result<_, _>>()?,
        ),
        Value::Object(map) => {
            let mut out = Map::with_capacity(map.len());
            for (k, v) in map {
                if out.insert(scrub_text(&k), scrub_json(v)?).is_some() {
                    return Err(Dropped::KeyCollision);
                }
            }
            Value::Object(out)
        }
        other => other,
    })
}

/// Result of scrubbing one chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrubbedChunk {
    /// Scrubbed, newline-delimited records (what gets compressed and stored).
    pub bytes: Vec<u8>,
    pub records: u64,
    /// Records replaced by a placeholder, by reason.
    pub dropped_unparseable: u64,
    pub dropped_key_collision: u64,
}

/// Scrub a newline-delimited chunk record by record. Record boundaries and
/// the trailing newline are preserved; blank lines stay blank.
pub fn scrub_chunk(raw: &[u8]) -> ScrubbedChunk {
    let mut out = ScrubbedChunk {
        bytes: Vec::with_capacity(raw.len()),
        records: 0,
        dropped_unparseable: 0,
        dropped_key_collision: 0,
    };
    let mut lines = raw.split(|b| *b == b'\n').peekable();
    while let Some(line) = lines.next() {
        let last = lines.peek().is_none();
        if last && line.is_empty() {
            break; // the chunk ended with '\n'
        }
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if !line.iter().all(u8::is_ascii_whitespace) {
            out.records += 1;
            match scrub_record(line) {
                Ok(bytes) => out.bytes.extend_from_slice(&bytes),
                Err(reason) => {
                    match reason {
                        Dropped::Unparseable => out.dropped_unparseable += 1,
                        Dropped::KeyCollision => out.dropped_key_collision += 1,
                    }
                    out.bytes
                        .extend_from_slice(&placeholder(reason, line.len()));
                }
            }
        }
        if !last {
            out.bytes.push(b'\n');
        }
    }
    out
}

/// Scrub one JSON record and re-serialize it compactly (key order kept).
pub fn scrub_record(line: &[u8]) -> Result<Vec<u8>, Dropped> {
    let value: Value = serde_json::from_slice(line).map_err(|_| Dropped::Unparseable)?;
    let scrubbed = scrub_json(value)?;
    serde_json::to_vec(&scrubbed).map_err(|_| Dropped::Unparseable)
}

fn placeholder(reason: Dropped, bytes: usize) -> Vec<u8> {
    format!(
        r#"{{"relayhistory_scrub":"{}","ruleset":"{RULESET}","version":{RULESET_VERSION},"bytes":{bytes}}}"#,
        reason.as_str()
    )
    .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_records_are_scrubbed_and_stay_valid_json() {
        let raw = b"{\"t\":\"user\",\"m\":\"key sk-abcdefghijklmnopqrstuvwxyz0123\"}\n\nnot json\n{\"a\":1}\n";
        let out = scrub_chunk(raw);
        let text = String::from_utf8(out.bytes.clone()).unwrap();
        let lines: Vec<&str> = text.split('\n').collect();
        assert_eq!(lines.len(), 5, "{text:?}");
        assert!(lines[0].contains("[REDACTED]") && !lines[0].contains("sk-abc"));
        assert_eq!(lines[1], "");
        assert!(lines[2].contains("unparseable_record"));
        assert_eq!(lines[3], "{\"a\":1}");
        assert_eq!(lines[4], "");
        assert_eq!((out.records, out.dropped_unparseable), (3, 1));
        for line in lines.iter().filter(|l| !l.is_empty()) {
            serde_json::from_str::<Value>(line).unwrap();
        }
    }

    #[test]
    fn keys_are_scrubbed_and_collisions_drop_the_record() {
        let v = serde_json::json!({"/home/alice/x": 1});
        assert_eq!(scrub_json(v).unwrap(), serde_json::json!({"~/x": 1}));
        let collide = br#"{"/home/a/x":1,"/home/b/x":2}"#;
        assert_eq!(scrub_record(collide), Err(Dropped::KeyCollision));
    }

    #[test]
    fn numbers_and_key_order_survive_reserialization() {
        let line = br#"{"z":1,"a":12345678901234567890123,"f":1.50}"#;
        assert_eq!(scrub_record(line).unwrap(), line.to_vec());
    }
}
