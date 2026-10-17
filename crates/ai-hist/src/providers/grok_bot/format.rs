use super::*;
use serde::Deserialize;
use std::collections::HashSet;

const PREFIX: &str = "sand.client.slice.account.";
const REPLICA: &str = ".transcript.replicas.";

pub(super) fn decode_key(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?.strip_suffix(".blob")?;
    let mut bits = 0u32;
    let mut width = 0u8;
    let mut bytes = Vec::with_capacity(name.len() * 5 / 8);
    for ch in name.bytes() {
        let digit = match ch {
            b'a'..=b'z' => ch - b'a',
            b'2'..=b'7' => ch - b'2' + 26,
            _ => return None,
        };
        bits = (bits << 5) | u32::from(digit);
        width += 5;
        if width >= 8 {
            width -= 8;
            bytes.push((bits >> width) as u8);
            bits &= (1 << width) - 1;
        }
    }
    if width >= 5 || bits != 0 {
        return None;
    }
    String::from_utf8(bytes).ok()
}

pub(super) fn encode_key(key: &str) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut out = String::new();
    let mut bits = 0u32;
    let mut width = 0u8;
    for byte in key.bytes() {
        bits = (bits << 8) | u32::from(byte);
        width += 8;
        while width >= 5 {
            width -= 5;
            out.push(ALPHABET[((bits >> width) & 31) as usize] as char);
            bits &= (1 << width) - 1;
        }
    }
    if width > 0 {
        out.push(ALPHABET[(bits << (5 - width)) as usize] as char);
    }
    out
}

pub(super) fn replica_identity(path: &Path) -> Option<(String, String)> {
    let key = decode_key(path)?;
    let suffix = key.strip_prefix(PREFIX)?;
    let (slot, sid) = suffix.split_once(REPLICA)?;
    if slot.is_empty() || sid.is_empty() || sid.contains('.') || sid.chars().any(char::is_control) {
        return None;
    }
    Some((slot.to_owned(), sid.to_owned()))
}

pub(super) fn roster_path(path: &Path, slot: &str) -> PathBuf {
    path.with_file_name(format!(
        "{}.blob",
        encode_key(&format!("{PREFIX}{slot}.roster.last-roster"))
    ))
}

/// The replica's keyable entries. The app's entry union grows between
/// releases, so an entry without a usable kind, id and time, or a repeated id,
/// is skipped instead of failing every other entry in the session.
pub(super) fn valid_replica(value: &Value) -> Result<Vec<&Value>> {
    anyhow::ensure!(
        value["schemaVersion"] == 1,
        "GROK_BOT_UNSUPPORTED_SCHEMA: transcript replica version 1 required"
    );
    let entries = value["value"]["entries"]
        .as_array()
        .context("GROK_BOT_INVALID_REPLICA: entries missing")?;
    let mut ids = HashSet::new();
    Ok(entries
        .iter()
        .filter(|entry| {
            string(entry, "kind").is_ok()
                && timestamp(entry).is_ok()
                && string(entry, "id").is_ok_and(|id| ids.insert(id))
        })
        .collect())
}

/// Parse the final native entry when its opening brace fits in the bounded
/// tail. Nested widget and call objects are skipped because they lack the
/// entry's required kind, id and timestampMs trio.
pub(super) fn last_entry(tail: &[u8]) -> Option<Value> {
    for index in (0..tail.len()).rev().filter(|index| tail[*index] == b'{') {
        let mut stream = serde_json::Deserializer::from_slice(&tail[index..]);
        let Ok(entry) = Value::deserialize(&mut stream) else {
            continue;
        };
        if entry.get("kind").and_then(Value::as_str).is_some()
            && entry.get("id").and_then(Value::as_str).is_some()
            && entry.get("timestampMs").and_then(Value::as_i64).is_some()
        {
            return Some(entry);
        }
    }
    None
}

pub(super) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty() && !s.chars().any(char::is_control))
        .with_context(|| format!("GROK_BOT_INVALID_RECORD: missing/invalid {field}"))
}

pub(super) fn timestamp(value: &Value) -> Result<i64> {
    value["timestampMs"]
        .as_i64()
        .context("GROK_BOT_INVALID_RECORD: timestampMs")
}

pub(super) fn last_activity(entry: &Value) -> Option<i64> {
    let ts = entry["timestampMs"].as_i64()?;
    if entry["kind"] != "voice-call" {
        return Some(ts);
    }
    Some(
        entry["conversation"]["turns"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|turn| matches!(turn["speaker"].as_str(), Some("user" | "assistant")))
            .filter_map(|turn| turn["atMs"].as_i64())
            .fold(ts, i64::max),
    )
}

/// First entry time in the bounded head, for discovery before hydration.
pub(super) fn first_number_hint(bytes: &[u8], field: &str) -> Option<i64> {
    let needle = format!("\"{field}\"");
    let index = bytes
        .windows(needle.len())
        .position(|w| w == needle.as_bytes())?;
    decimal_hint(&bytes[index + needle.len()..])
}

/// Read a decimal field in a bounded JSON head/tail window. This is a hint,
/// never a substitute for full JSON validation during acquisition.
pub(super) fn number_hint(bytes: &[u8], field: &str) -> Option<i64> {
    let needle = format!("\"{field}\"");
    let index = bytes
        .windows(needle.len())
        .rposition(|w| w == needle.as_bytes())?;
    decimal_hint(&bytes[index + needle.len()..])
}

fn decimal_hint(rest: &[u8]) -> Option<i64> {
    let rest = rest.strip_prefix(b":").or_else(|| {
        rest.iter()
            .position(|b| *b == b':')
            .and_then(|i| rest.get(i + 1..))
    })?;
    let start = rest.iter().position(|b| !b.is_ascii_whitespace())?;
    let digits: Vec<u8> = rest[start..]
        .iter()
        .copied()
        .take_while(|b| b.is_ascii_digit())
        .collect();
    std::str::from_utf8(&digits).ok()?.parse().ok()
}
