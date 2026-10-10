use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;

use super::{capture, ContinuityEvidence};

/// Read one newline-delimited record into `raw`; `None` at end of file, else
/// the bytes it consumed.
///
/// Bounded by [`crate::ingest::transcript_cursor::MAX_RECORD_BYTES`]: a record that runs
/// past the ceiling is walked to its newline in fixed-size chunks and `raw` is
/// left empty, so one pathological line cannot cost this walk the file's size
/// in memory. `raw` is reused across calls, so the buffer is grown once.
///
/// The count is what the reader *spent*, not what `raw` ends up holding:
/// `raw` loses the delimiter, and an oversized record is drained and dropped
/// entirely.
pub(super) fn next_record(
    reader: &mut impl std::io::BufRead,
    raw: &mut Vec<u8>,
) -> std::io::Result<Option<u64>> {
    use std::io::{BufRead, Read};
    const CEILING: u64 = crate::ingest::transcript_cursor::MAX_RECORD_BYTES;
    raw.clear();
    // The cap is on the reader rather than a check around it: `read_until`
    // extends `raw` until it finds a newline, so a budget consulted afterwards
    // can only observe an allocation that already happened.
    let consumed = reader.take(CEILING).read_until(b'\n', raw)? as u64;
    if consumed == 0 {
        return Ok(None);
    }
    if raw.last() == Some(&b'\n') {
        raw.pop();
        if raw.last() == Some(&b'\r') {
            raw.pop();
        }
        return Ok(Some(consumed));
    }
    if consumed < CEILING {
        // A genuine tail: the file ends here, under the ceiling.
        return Ok(Some(consumed));
    }
    // The limited read stops at the ceiling whether the record ends there or
    // runs past it, so the ceiling alone cannot tell them apart. The same
    // boundary rule as the ingest reader: one byte decides, and a record that
    // ends exactly on the ceiling is an ordinary record.
    match reader.fill_buf()?.first().copied() {
        // The file ends here: a complete tail, exactly on the ceiling.
        None => Ok(Some(consumed)),
        Some(b'\n') => {
            reader.consume(1);
            Ok(Some(consumed + 1))
        }
        // Over the ceiling. Drop what was read and walk to the newline.
        Some(_) => {
            raw.clear();
            Ok(Some(consumed + skip_past_newline(reader)?))
        }
    }
}

/// Walk to the next newline through the reader's buffer, consuming only up to
/// it: anything after it is the next record and must still be there to read.
/// The bytes consumed, newline included.
fn skip_past_newline(reader: &mut impl std::io::BufRead) -> std::io::Result<u64> {
    let mut consumed = 0;
    loop {
        let available = match reader.fill_buf() {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if available.is_empty() {
            return Ok(consumed);
        }
        if let Some(at) = available.iter().position(|byte| *byte == b'\n') {
            reader.consume(at + 1);
            return Ok(consumed + at as u64 + 1);
        }
        let all = available.len();
        reader.consume(all);
        consumed += all as u64;
    }
}

/// Read one Claude transcript's continuity evidence in a single pass.
///
/// Returns `None` for a transcript with no in-log session id: relayhistory has
/// no identity to attach the evidence to, and inventing one from the file name
/// is exactly what the delegation model already refuses to do.
/// The metadata walk folds this as it goes, so hydration and the global sync
/// never call it. It is the one-time backfill on the *skip* path: a transcript
/// indexed before continuity existed owes its evidence, and no metadata walk
/// runs for a file that is being skipped.
pub fn scan_claude_transcript(path: &Path) -> Result<Option<ContinuityEvidence>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("reading Claude transcript {}", path.display()))?;
    let mut reader = std::io::BufReader::new(file);
    let mut raw = Vec::new();
    let mut evidence = ContinuityEvidence::default();
    let mut first_user_seen = false;
    let mut any = false;
    while next_record(&mut reader, &mut raw)
        .with_context(|| format!("reading Claude transcript {}", path.display()))?
        .is_some()
    {
        crate::ingest::check_capture_cancelled()?;
        // An oversized record is not buffered and an undecodable one is not
        // repaired; both leave `raw` empty or unparseable and take the same
        // skip any other malformed record takes.
        let Ok(line) = std::str::from_utf8(&raw) else {
            continue;
        };
        any |= fold_claude_line(&mut evidence, &mut first_user_seen, line);
    }
    Ok(finish_claude_fold(evidence, any, path))
}

/// Fold one transcript line that is a JSON object; whether it was one.
pub(super) fn fold_claude_line(
    evidence: &mut ContinuityEvidence,
    first_user_seen: &mut bool,
    line: &str,
) -> bool {
    let Ok(Value::Object(object)) = serde_json::from_str::<Value>(line) else {
        return false;
    };
    fold_claude_record(evidence, first_user_seen, &object);
    true
}

/// Settle a folded evidence set into the row it should record, or `None`.
///
/// `any` is whether a record ever folded in: a file that says nothing retracts
/// what it used to say, which is a different answer from a file that has
/// simply not grown since the last pass.
pub(crate) fn finish_claude_fold(
    mut evidence: ContinuityEvidence,
    any: bool,
    path: &Path,
) -> Option<ContinuityEvidence> {
    if !any {
        return None;
    }
    evidence.source = "claude".to_string();
    evidence.locator = path.to_string_lossy().to_string();
    // An explicit `fileSessionId` in the records wins; the file name is the
    // fallback, which is why it is filled in here rather than seeded before
    // the walk.
    if evidence.file_session_id.is_none() {
        evidence.file_session_id = file_session_id_from_path(path);
    }
    evidence.session_id = evidence.in_log_session_ids.first().cloned()?;
    Some(evidence)
}

/// Record continuity evidence a walk has already folded, reading nothing.
pub(crate) fn capture_folded(
    conn: &Connection,
    path: &Path,
    evidence: Option<ContinuityEvidence>,
) -> Result<()> {
    capture(conn, "claude", path, evidence)
}

/// Fold one Claude record into the running continuity evidence.
///
/// Every field is first-wins, last-wins or accumulating, which is what makes
/// the walk resumable: the same records in the same order give the same
/// answer whether they arrive in one pass or several.
pub(crate) fn fold_claude_record(
    evidence: &mut ContinuityEvidence,
    first_user_seen: &mut bool,
    object: &serde_json::Map<String, Value>,
) {
    if let Some(explicit) = string_field(object, &["fileSessionId", "file_session_id"]) {
        evidence.file_session_id = Some(explicit);
    }
    if let Some(session_id) = string_field(object, &["sessionId", "session_id"]) {
        if !evidence.in_log_session_ids.contains(&session_id) {
            evidence.in_log_session_ids.push(session_id);
        }
        if evidence.first_ts_ms.is_none() {
            evidence.first_ts_ms = record_ts_ms(object);
        }
    }
    if evidence.source_version.is_none() {
        evidence.source_version = string_field(object, &["version", "source_version"]);
    }
    if let Some(target) = string_field(
        object,
        &["continuedFromSessionId", "continued_from_session_id"],
    ) {
        if !evidence.explicit_continuation_targets.contains(&target) {
            note_named_at(&mut evidence.explicit_continuation_ts_ms, &target, object);
        }
        push_unique(&mut evidence.explicit_continuation_targets, target);
    }
    if let Some(target) = string_field(object, &["forkSessionId", "fork_session_id"]) {
        if !evidence.explicit_fork_targets.contains(&target) {
            note_named_at(&mut evidence.explicit_fork_ts_ms, &target, object);
        }
        push_unique(&mut evidence.explicit_fork_targets, target);
    }
    if evidence.explicit_source_session_id.is_none() {
        evidence.explicit_source_session_id =
            string_field(object, &["sourceSessionId", "source_session_id"]);
    }
    let sidechain = object
        .get("isSidechain")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let is_user = object.get("type").and_then(Value::as_str) == Some("user");
    if is_user && !sidechain {
        if !*first_user_seen {
            *first_user_seen = true;
            evidence.first_parent_uuid = string_field(object, &["parentUuid", "parent_uuid"]);
        }
        record_resume_marker(evidence, object);
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// The transcript's own file identity, used only to tell two files apart.
///
/// burn derives this from an explicit `fileSessionId` and then the transcript
/// path's basename, deliberately never from the path the parser happened to
/// open. relayhistory only ever has the real on-disk path, so the basename is
/// it — and because this value is never written as a session id, a file named
/// after nothing in particular costs nothing.
fn file_session_id_from_path(path: &Path) -> Option<String> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(str::to_string)
}

/// A `/resume` or `/continue` the human ran, in either form Claude writes.
///
/// Claude Code does not store a slash command as the text the human typed. It
/// stores a control wrapper — `<command-message>resume is running…`,
/// `<command-name>/resume</command-name>`, `<command-args>…</command-args>` —
/// which this crate already recognises as a control prompt and keeps out of
/// prompt history. Matching only bare `/resume` therefore matched the one form
/// a real session never contains, and every actual resume went unrecorded.
///
/// burn reads the same marker off plain user text only, so on the bare form
/// the two agree; the wrapped form is one burn does not detect either.
fn record_resume_marker(
    evidence: &mut ContinuityEvidence,
    object: &serde_json::Map<String, Value>,
) {
    let Some(text) = plain_user_text(object) else {
        return;
    };
    // The same text ingestion classifies: a `<system-reminder>` Claude Code
    // puts ahead of the wrapper is not the record's own text, and left in
    // place it would hide the command from both parsers below.
    let split = crate::ingest::control::split_system_reminders(&text);
    let trimmed = split.prompt.as_str();
    // The wrapped form first: a record that opens with a control tag is never
    // a bare command, and `bare_command` refuses anything not starting with
    // `/`, so an unparseable wrapper falls through to nothing rather than to
    // a false match.
    let Some((command, rest)) = wrapped_command(trimmed).or_else(|| bare_command(trimmed)) else {
        return;
    };
    if command != "resume" && command != "continue" {
        return;
    }
    evidence.has_resume_marker = true;
    if evidence.resume_target.is_some() {
        return;
    }
    let token_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let token = &rest[..token_end];
    if !token.is_empty() {
        evidence.resume_target = Some(token.to_string());
    }
}

/// `/resume <target>` as typed: the form burn matches.
fn bare_command(text: &str) -> Option<(String, &str)> {
    let after_slash = text.strip_prefix('/')?;
    let command_end = after_slash
        .find(char::is_whitespace)
        .unwrap_or(after_slash.len());
    Some((
        after_slash[..command_end].to_lowercase(),
        after_slash[command_end..].trim_start(),
    ))
}

/// The command name and arguments Claude Code's control wrapper carries.
///
/// `<command-args>` is absent when the command took none, and the elements can
/// arrive in either order, so each is read independently rather than by
/// position -- the same read `ingest::control` makes for the `slash_command`
/// marker.
fn wrapped_command(text: &str) -> Option<(String, &str)> {
    if crate::ingest::control::claude_text_control_kind(text)
        != Some(crate::ingest::control::ControlKind::SlashCommandInvocation)
    {
        return None;
    }
    let name = crate::ingest::control::tag_body(text, "command-name")?;
    let command = name.trim().trim_start_matches('/').to_lowercase();
    let args = crate::ingest::control::tag_body(text, "command-args")
        .unwrap_or("")
        .trim_start();
    Some((command, args))
}

/// The user's own typed text, from either content shape. Tool results and
/// structured blocks are not something a human typed a slash command into.
fn plain_user_text(object: &serde_json::Map<String, Value>) -> Option<String> {
    let content = object.get("message").and_then(|m| m.get("content"))?;
    if let Some(text) = content.as_str() {
        return Some(text.to_string());
    }
    let blocks = content.as_array()?;
    let parts: Vec<&str> = blocks
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .collect();
    (!parts.is_empty()).then(|| parts.join("\n"))
}

pub(super) fn record_ts_ms(object: &serde_json::Map<String, Value>) -> Option<i64> {
    object.get("timestamp").and_then(|value| {
        value
            .as_str()
            .and_then(crate::parse_iso_ms)
            .or_else(|| value.as_i64())
    })
}

pub(super) fn string_field(
    object: &serde_json::Map<String, Value>,
    keys: &[&str],
) -> Option<String> {
    keys.iter()
        .filter_map(|key| object.get(*key))
        .filter_map(Value::as_str)
        .map(str::trim)
        .find(|value| !value.is_empty())
        .map(str::to_string)
}

pub(super) fn ts_map(value: &Value, key: &str) -> BTreeMap<String, i64> {
    value
        .get(key)
        .and_then(Value::as_object)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|(target, ts)| Some((target.clone(), ts.as_i64()?)))
                .collect()
        })
        .unwrap_or_default()
}

/// Remember when the record that first names `target` was written. Called
/// only for that first record: one without a timestamp leaves the target
/// undated rather than dated by a later record.
fn note_named_at(
    named: &mut BTreeMap<String, i64>,
    target: &str,
    object: &serde_json::Map<String, Value>,
) {
    if let Some(ts) = record_ts_ms(object) {
        named.insert(target.to_string(), ts);
    }
}

/// When the record naming an explicit target was written.
///
/// A Codex rollout names its targets only on the `session_meta` line that
/// opens it, so its first record is the naming record. A Claude naming record
/// without a timestamp leaves only the transcript's first record to date by.
pub(super) fn named_at(
    named: &BTreeMap<String, i64>,
    target: &str,
    evidence: &ContinuityEvidence,
) -> Option<i64> {
    named.get(target).copied().or(evidence.first_ts_ms)
}

pub(super) fn string_array(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}
