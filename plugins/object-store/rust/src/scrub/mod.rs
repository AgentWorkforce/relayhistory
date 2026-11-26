//! PII and secret scrubbing for raw session chunks, applied on the client
//! before a chunk reaches object storage.
//!
//! Nothing here runs automatically: uploads are not scrubbed unless the
//! caller does it. The delivery worker must call [`scrub_chunk`] on each
//! sealed chunk before hashing, compressing and uploading it. It belongs in
//! that background worker, never on the local ingest/read path, so it adds
//! nothing to the < 5 ms local latency budget. The server's `scrubText` remains the compliance boundary
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
//!
//! Each string is scrubbed on its own: a secret split across two strings,
//! JSON values or records (a token streamed as separate deltas) is not
//! recognized, because neither part matches a rule.

mod numbers;
mod rules;
pub mod seal;

use std::borrow::Cow;

use serde_json::{Map, Value};

use rules::{expand_template, Engine, Replace, Rule, RULES};

/// Identifies this rule set in `manifest.raw.scrub`.
pub const RULESET: &str = "relayhistory-scrub";
/// Bumped whenever the rules or the golden vectors change.
pub const RULESET_VERSION: u32 = 2;

/// Longest string scrubbed in full, in UTF-16 code units (the reference
/// counts JavaScript string length). Longer strings are truncated first.
pub const MAX_SCRUB_CHARS: usize = 262_144;
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
        if !rule.needles.is_empty() && !rule.needles.iter().any(|n| lower.contains(n))
            || !(rule.gate)(&text)
        {
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

/// Port of `boundForScrub`: cut over-long input only right after whitespace,
/// rewinding as far as needed, so a secret is either wholly kept (and then
/// scrubbed) or wholly dropped. A run with no whitespace before the limit
/// (minified JSON, a base64 dump) is dropped whole: a credential can sit
/// anywhere in it. A private-key block the cut leaves without its footer is
/// withheld from its header on, since the PEM rule needs both delimiters. The
/// truncation is announced in band.
///
/// Every JavaScript whitespace unit is in the BMP, so a cut after one never
/// splits a surrogate pair.
fn bound_for_scrub(value: &str) -> Cow<'_, str> {
    if value.len() <= MAX_SCRUB_CHARS || utf16_len(value) <= MAX_SCRUB_CHARS {
        return Cow::Borrowed(value);
    }
    let units: Vec<u16> = value.encode_utf16().collect();
    let mut cut = MAX_SCRUB_CHARS;
    while cut > 0 && !safe_cut(&units, cut) {
        cut -= 1;
    }
    // `cut` follows a BMP whitespace unit (or is 0), so this is exact.
    let mut kept = String::from_utf16_lossy(&units[..cut]);
    if let Some(start) = unclosed_private_key_start(&kept) {
        kept.truncate(start);
    }
    let dropped = units.len() - utf16_len(&kept);
    Cow::Owned(format!(
        "{kept}\n{TRUNCATION_NOTE} {dropped} characters over the {MAX_SCRUB_CHARS}-character scrub limit]"
    ))
}

/// Port of `safeCut`: right after whitespace, and not between two digit
/// groups (a digit or `)` before the whitespace, a digit or `(` after it),
/// since card and phone matches can contain spaces.
fn safe_cut(units: &[u16], at: usize) -> bool {
    let unit = |i: Option<usize>| i.and_then(|i| units.get(i)).copied();
    let is = |u: Option<u16>, extra: u8| {
        u.is_some_and(|u| (u16::from(b'0')..=u16::from(b'9')).contains(&u) || u == u16::from(extra))
    };
    is_js_ws_unit(units[at - 1]) && !(is(unit(at.checked_sub(2)), b')') && is(unit(Some(at)), b'('))
}

/// Byte offset of the first private-key block in `text` that the PEM rule
/// cannot close (no footer after its header), pairing headers and footers left
/// to right as the lazy PEM pattern does.
fn unclosed_private_key_start(text: &str) -> Option<usize> {
    let mut pos = 0;
    loop {
        let (start, after) = pem_marker(text, pos, "-----BEGIN ")?;
        match pem_marker(text, after, "-----END ") {
            Some((_, end)) => pos = end,
            None => return Some(start),
        }
    }
}

/// The leftmost `{lead}[A-Z ]*PRIVATE KEY(?: BLOCK)?-----` at or after `from`, as
/// `(start, end)` byte offsets.
fn pem_marker(text: &str, from: usize, lead: &str) -> Option<(usize, usize)> {
    let mut at = from;
    while let Some(offset) = text.get(at..)?.find(lead) {
        let start = at + offset;
        let rest = &text[start + lead.len()..];
        let run = rest
            .bytes()
            .take_while(|b| b.is_ascii_uppercase() || *b == b' ')
            .count();
        let named =
            rest[..run].ends_with("PRIVATE KEY") || rest[..run].ends_with("PRIVATE KEY BLOCK");
        if named && rest[run..].starts_with("-----") {
            return Some((start, start + lead.len() + run + 5));
        }
        // `lead` starts with an ASCII '-', so `start + 1` is a char boundary.
        at = start + 1;
    }
    None
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Dropped {
    /// Not valid JSON (or not valid UTF-8).
    #[error("record is not valid JSON")]
    Unparseable,
    /// Two keys became identical after scrubbing.
    #[error("two keys of one object became identical after scrubbing")]
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
        Value::Number(n) if is_card_json_number(&n) => Value::String(rules::REDACTED.into()),
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

/// A JSON integer whose digits are a card number. Unlike the reference, which
/// can only check integers `JSON.parse` keeps exact (up to 2^53), this checks
/// every integer: the raw record still has all its digits.
fn is_card_json_number(n: &serde_json::Number) -> bool {
    let text = n.to_string();
    text.bytes().all(|b| b.is_ascii_digit()) && numbers::is_card_number(&text)
}

/// Result of scrubbing one chunk. Opaque: only [`scrub_chunk`] builds one,
/// so holding a `ScrubbedChunk` proves its bytes went through the rules.
/// [`seal::SealedChunk::seal`] turns it into what [`seal::put_raw_chunk`],
/// the only sanctioned upload path for raw chunks, accepts.
#[derive(Clone, PartialEq, Eq)]
pub struct ScrubbedChunk {
    bytes: Vec<u8>,
    source_bytes: u64,
    source_sha256: String,
    records: u64,
    dropped_unparseable: u64,
    dropped_key_collision: u64,
}

/// Lengths and counts only: never the records or a digest of the unscrubbed
/// input, so a logged chunk leaks no session content.
impl std::fmt::Debug for ScrubbedChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScrubbedChunk")
            .field("bytes", &self.bytes.len())
            .field("source_bytes", &self.source_bytes)
            .field("records", &self.records)
            .field("dropped_unparseable", &self.dropped_unparseable)
            .field("dropped_key_collision", &self.dropped_key_collision)
            .finish_non_exhaustive()
    }
}

impl ScrubbedChunk {
    /// Length of the unscrubbed input: the source range's `chunk.bytes`.
    pub fn source_bytes(&self) -> u64 {
        self.source_bytes
    }

    /// SHA-256 of the unscrubbed input: the source range's `chunk.sha256`.
    pub fn source_sha256(&self) -> &str {
        &self.source_sha256
    }

    /// Scrubbed, newline-delimited records (what gets compressed and stored).
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Non-blank records, including any replaced by a placeholder.
    pub fn records(&self) -> u64 {
        self.records
    }

    /// Records replaced by a placeholder because they could not be parsed.
    pub fn dropped_unparseable(&self) -> u64 {
        self.dropped_unparseable
    }

    /// Records replaced by a placeholder because keys collided after scrubbing.
    pub fn dropped_key_collision(&self) -> u64 {
        self.dropped_key_collision
    }
}

/// Scrub a newline-delimited chunk record by record. Record boundaries and
/// the trailing newline are preserved; blank lines stay blank.
pub fn scrub_chunk(raw: &[u8]) -> ScrubbedChunk {
    let mut out = ScrubbedChunk {
        bytes: Vec::with_capacity(raw.len()),
        source_bytes: raw.len() as u64,
        source_sha256: crate::codec::sha256_hex(raw),
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
    fn a_record_split_across_chunks_is_withheld_not_passed_through() {
        let record = br#"{"m":"key sk-abcdefghijklmnopqrstuvwxyz0123"}"#;
        let (a, b) = record.split_at(20);
        for half in [a, b] {
            let out = scrub_chunk(half);
            let text = String::from_utf8(out.bytes).unwrap();
            assert!(text.contains("unparseable_record"), "{text}");
            assert!(!text.contains("sk-") && !text.contains("lmnop"), "{text}");
        }
    }

    #[test]
    fn card_numbers_held_as_json_integers_are_redacted() {
        // 4111111111111111110 is a Luhn-valid 19-digit Visa number above 2^53,
        // which the reference cannot check; its neighbour fails Luhn.
        let line = br#"{"pan":4111111111111111110,"near":4111111111111111111,"short":4111111111111111,"neg":-4111111111111111,"f":4111111111111111.0}"#;
        let out: Value = serde_json::from_slice(&scrub_record(line).unwrap()).unwrap();
        assert_eq!(out["pan"], "[REDACTED]");
        assert_eq!(out["short"], "[REDACTED]");
        assert_eq!(out["near"].to_string(), "4111111111111111111");
        assert_eq!(out["neg"].to_string(), "-4111111111111111");
        assert_eq!(out["f"].to_string(), "4111111111111111.0");
    }

    #[test]
    fn numbers_and_key_order_survive_reserialization() {
        let line = br#"{"z":1,"a":12345678901234567890123,"f":1.50}"#;
        assert_eq!(scrub_record(line).unwrap(), line.to_vec());
    }
}
