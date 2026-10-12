//! The first records of a rollout, from bytes a cold sweep already holds.

use anyhow::Result;
use serde_json::Value;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::sync::atomic::Ordering as AtomicOrdering;

use super::codex;
use super::cold_read;
use super::{
    codex_is_subagent, codex_parent_session_id, codex_parent_thread_id, parse_iso_ms,
    CodexSessionMeta, SYNC_QUIET,
};

/// How far into a file a bounded head read will look, however few records it
/// has found. Callers set their own record limit; this one stops a file whose
/// first record is enormous, or whose leading lines are all unparseable, from
/// turning a bounded read back into a whole-file read.
pub(super) const LEADING_BYTE_LIMIT: u64 = 1024 * 1024;

/// The first complete records of a JSONL file, and the bytes that cost.
///
/// For metadata that lives at the head of a transcript — a Codex
/// `session_meta` line, the record that dates a Claude sidecar — reading the
/// whole file to use its first line is the same defect as scanning a whole
/// transcript to hydrate its tail, and it hides in the same place: the caller
/// writes `.lines().next()` and looks bounded while the read is not. A
/// trailing record with no newline counts, because the head of a one-record
/// file is that record.
pub(super) fn read_leading_records(path: &Path, limit: usize) -> Result<(Vec<Value>, u64)> {
    read_head_records(path, limit, true)
}

/// The **first physical record** of a file, and the bytes reading it cost.
///
/// Distinct from [`read_leading_records`], which walks past records it cannot
/// parse until it finds one that it can. That is right for a caller looking
/// for the first usable record and wrong for anything that defines identity by
/// position: a rollout whose first line is corrupt, followed by a
/// `session_meta` naming another session, was adopted under that other
/// session's id. `None` here means "the first record is not one I can read",
/// which is a different claim from "there is no such record in the file".
fn read_first_record(path: &Path) -> Result<(Option<Value>, u64)> {
    let (values, bytes_read) = read_head_records(path, 1, false)?;
    Ok((values.into_iter().next(), bytes_read))
}

/// `skip_unparseable` decides what a record that does not parse means: skip
/// past it and keep looking, or stop, having read it.
fn read_head_records(
    path: &Path,
    limit: usize,
    skip_unparseable: bool,
) -> Result<(Vec<Value>, u64)> {
    if let Some(bytes) = cold_read::fresh(path) {
        return collect_head_records(io::Cursor::new(&bytes[..]), limit, skip_unparseable);
    }
    if cold_read::in_sweep() && cold_read::small_file_len(path)?.is_some() {
        let bytes = cold_read::read_all(path)?;
        return collect_head_records(io::Cursor::new(&bytes[..]), limit, skip_unparseable);
    }
    let mut shared = cold_read::open_shared(path)?;
    let reader = BufReader::new(shared.file());
    collect_head_records(reader, limit, skip_unparseable)
}

/// `take` enforces the ceiling. A check before `read_until` only decided
/// whether to start another line, and `read_until` appends until a newline or
/// EOF, so a first record that is the whole file allocated the whole file.
fn collect_head_records(
    reader: impl BufRead,
    limit: usize,
    skip_unparseable: bool,
) -> Result<(Vec<Value>, u64)> {
    let mut reader = reader.take(LEADING_BYTE_LIMIT);
    let mut values = Vec::new();
    let mut raw = Vec::new();
    let mut bytes_read = 0u64;
    while values.len() < limit {
        raw.clear();
        let read = reader.read_until(b'\n', &mut raw)?;
        if read == 0 {
            break;
        }
        bytes_read += read as u64;
        match serde_json::from_slice::<Value>(trim_ascii_end(&raw)) {
            Ok(value) => values.push(value),
            Err(_) if skip_unparseable => continue,
            Err(_) => break,
        }
    }
    Ok((values, bytes_read))
}

fn trim_ascii_end(raw: &[u8]) -> &[u8] {
    let mut end = raw.len();
    while end > 0 && raw[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &raw[..end]
}

/// Read the `session_meta` line that opens every rollout file.
///
/// Sessions key on `payload.id` — the per-thread id. Subagent rollouts can
/// carry their parent in `parent_thread_id`, a structured thread-spawn source,
/// or the legacy `session_id`; keying on any of those would collapse every
/// subagent into its parent. Child threads are detected instead
/// ([`codex_is_subagent`]: a subagent marker *together with* a named parent)
/// and excluded from session registration. A standalone guardian names no
/// parent and is a session of its own.
pub(crate) fn read_codex_session_meta(path: &Path) -> Result<Option<CodexSessionMeta>> {
    Ok(read_codex_session_meta_counted(path)?.0)
}

/// Codex session metadata, and the bytes reading it cost.
///
/// A rollout's identity is its **first** record, so this reads one line rather
/// than the file. It used to `read_to_string` a rollout that reaches hundreds
/// of megabytes on a fleet machine and then take `lines().next()`, so a
/// kilobyte appended to one still read all of it before the cursor reached the
/// tail — the Codex twin of the whole-file Claude metadata scan, and invisible
/// for the same reason: `bytes_read` counted the cursor's work, not this.
pub(super) fn read_codex_session_meta_counted(
    path: &Path,
) -> Result<(Option<CodexSessionMeta>, u64)> {
    // A rollout that cannot be read is a rollout with no metadata, which is
    // what the `read_to_string(path).ok()` this replaced already meant. The
    // bounded reader propagates `File::open` failures, and every caller
    // propagates them further, so one locked or deleted sibling rollout
    // started failing a whole parent hydration or Codex sync — a file that
    // could not be read taking down the files that could. The failure is
    // reported rather than swallowed.
    // The *first physical* record, not the first one that parses. Codex
    // defines a rollout's identity by position, and skipping a corrupt opening
    // line to reach a `session_meta` further down adopts the rollout under
    // whatever session that record names.
    let (value, bytes_read) = match read_first_record(path) {
        Ok(read) => read,
        Err(error) => {
            sync_note!(
                "  [codex] skipping unreadable rollout {}: {error}",
                path.display()
            );
            return Ok((None, 0));
        }
    };
    let meta = value.as_ref().and_then(codex_session_meta_from_record);
    Ok((meta, bytes_read))
}

/// Interpret one `session_meta` record. Pure, so the bounded reader above is
/// the only thing that decides how much of the file is touched.
fn codex_session_meta_from_record(value: &Value) -> Option<CodexSessionMeta> {
    if value.get("type").and_then(Value::as_str) != Some("session_meta") {
        return None;
    }
    let payload_value = value.get("payload");
    let payload = payload_value.and_then(Value::as_object);
    let session_id = payload
        .and_then(|p| p.get("id"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())?;
    let cwd = payload
        .and_then(|p| p.get("cwd"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())?;
    let git_branch = payload
        .and_then(|p| p.get("git"))
        .and_then(Value::as_object)
        .and_then(|g| g.get("branch"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let subagent = payload
        .and_then(|p| p.get("source"))
        .and_then(Value::as_object)
        .and_then(|s| s.get("subagent"));
    let is_subagent = codex_is_subagent(payload_value, session_id);
    let parent_thread_id = is_subagent
        .then(|| codex_parent_thread_id(payload, session_id))
        .flatten();
    let parent_session_id = is_subagent
        .then(|| codex_parent_session_id(payload, session_id))
        .flatten();
    // The label is an object in the observed rollouts (`{"other":"guardian"}`);
    // its value names the agent, and its key is the only thing left when the
    // value is not a string.
    let subagent_label = subagent.and_then(|value| match value {
        Value::String(label) => Some(label.clone()),
        Value::Object(map) => map
            .values()
            .find_map(Value::as_str)
            .map(str::to_string)
            .or_else(|| map.keys().next().cloned()),
        _ => None,
    });
    let meta_ts_ms = value
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_iso_ms);
    let fork_parent_id = codex::fork_parent_id(payload_value, session_id);
    let fork_origin_ms = codex::fork_origin_ms(payload_value, session_id, meta_ts_ms);
    Some(CodexSessionMeta {
        session_id: session_id.to_string(),
        cwd: cwd.to_string(),
        git_branch,
        is_subagent,
        parent_session_id,
        parent_thread_id,
        subagent_label,
        meta_ts_ms,
        fork_parent_id,
        fork_origin_ms,
    })
}
