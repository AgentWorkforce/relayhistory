//! A Claude shallow read: the head and tail folds over a bounded transcript.

use super::{
    claude_session_id_from_bounded, claude_substantive_prompt, claude_timestamp, parse_record,
    push_unique, records, BoundedJsonl, Candidate, ShallowSession,
};
use anyhow::Result;
use std::path::Path;
use crate::ingest::claude_title::ClaudeTitles;
use serde_json::Value;

struct ClaudeHeadFold {
    pub(super) primary_record_seen: bool,
    pub(super) sidechain_records: usize,
    pub(super) identified_records: usize,
    pub(super) parsed_records: usize,
    pub(super) head_records_seen: usize,
}

/// One head record. `false` once every catalog field is settled and reading
/// further head lines cannot change the row.
fn fold_claude_head_line(
    session: &mut ShallowSession,
    models: &mut Vec<String>,
    fold: &mut ClaudeHeadFold,
    line: &[u8],
    sidecar_layout: bool,
) -> bool {
    fold.head_records_seen += 1;
    let Some(value) = parse_record(line) else {
        return true;
    };
    fold.parsed_records += 1;
    if value
        .get("sessionId")
        .and_then(Value::as_str)
        .is_some_and(|id| !id.is_empty())
    {
        fold.identified_records += 1;
        if value.get("isSidechain").and_then(Value::as_bool) == Some(true) {
            fold.sidechain_records += 1;
        } else {
            fold.primary_record_seen = true;
        }
    }
    if session.cwd.is_none() {
        session.cwd = value.get("cwd").and_then(Value::as_str).map(str::to_string);
    }
    if let Some(branch) = value.get("gitBranch").and_then(Value::as_str) {
        session.git_branch = Some(branch.to_string());
    }
    if let Some(version) = value.get("version").and_then(Value::as_str) {
        session.agent_version = Some(version.to_string());
    }
    // `<synthetic>` is the placeholder on a notice Claude Code wrote
    // itself; no model by that name ran in the session.
    push_unique(
        models,
        value
            .pointer("/message/model")
            .and_then(Value::as_str)
            .filter(|model| !crate::ingest::is_claude_synthetic_placeholder_model(model)),
    );
    if let Some(ts) = claude_timestamp(&value) {
        session.first_activity_ms.get_or_insert(ts);
        session.last_activity_ms = Some(ts);
    }
    if session.first_prompt.is_none() {
        session.first_prompt = claude_substantive_prompt(&value);
    }
    // Every observed field is settled, a model has been seen, and the
    // file is not a sidecar -- by its layout, or by a primary record
    // in one laid out as a sidecar: nothing further in the head can
    // change the row (additional models stay best-effort), so stop
    // paying to parse it.
    let settled = (fold.primary_record_seen || !sidecar_layout)
        && !models.is_empty()
        && session.cwd.is_some()
        && session.git_branch.is_some()
        && session.agent_version.is_some()
        && session.first_activity_ms.is_some()
        && session.first_prompt.is_some();
    !settled
}

fn fold_claude_shallow_tail(session: &mut ShallowSession, bounded: &BoundedJsonl) {
    let mut need_last_activity = true;
    let mut need_branch = true;
    for line in bounded.tail_records_rev() {
        if !need_last_activity && !need_branch {
            break;
        }
        let Some(value) = parse_record(line) else {
            continue;
        };
        if need_last_activity {
            if let Some(ts) = claude_timestamp(&value) {
                session.last_activity_ms = Some(ts);
                need_last_activity = false;
            }
        }
        if need_branch {
            if let Some(branch) = value.get("gitBranch").and_then(Value::as_str) {
                session.git_branch = Some(branch.to_string());
                need_branch = false;
            }
        }
    }
}

/// Title records are a few hundred bytes; a line much longer than that is a
/// message, and is not parsed just to find out it is not a title.
const TITLE_RECORD_MAX_BYTES: usize = 8 * 1024;

/// Whether `line` could be a title record, without parsing it.
fn may_hold_title(line: &[u8]) -> bool {
    line.len() <= TITLE_RECORD_MAX_BYTES
        && [&b"\"custom-title\""[..], b"\"ai-title\"", b"\"agent-name\""]
            .iter()
            .any(|needle| line.windows(needle.len()).any(|window| window == *needle))
}

/// The session's title from the bounded read: every title record in the head
/// and the tail, in file order, so the latest one wins (see
/// [`ClaudeTitles`]). For a file inside the head budget the tail walk is the
/// whole file. For a larger one, a rename recorded only between the two
/// regions is missed here and filled in by the full ingest.
fn fold_claude_shallow_title(session: &mut ShallowSession, bounded: &BoundedJsonl) {
    let mut titles = ClaudeTitles::default();
    if !bounded.tail.is_empty() {
        for value in records(&bounded.head)
            .filter(|line| may_hold_title(line))
            .filter_map(parse_record)
        {
            titles.observe(&value);
        }
    }
    let tail: Vec<&[u8]> = bounded
        .tail_records_rev()
        .filter(|line| may_hold_title(line))
        .collect();
    for value in tail.into_iter().rev().filter_map(parse_record) {
        titles.observe(&value);
    }
    session.title = titles.best();
}

pub(super) fn read_claude_shallow(
    candidate: &Candidate,
    path: &Path,
    bounded: &BoundedJsonl,
) -> Result<Option<ShallowSession>> {
    let mut session = ShallowSession {
        source: "claude".into(),
        raw_path: Some(candidate.locator.clone()),
        ..Default::default()
    };
    let mut models = Vec::new();
    let session_id = claude_session_id_from_bounded(bounded)?;
    // A subagent sidecar transcript is its own file whose records carry the
    // *parent's* sessionId (see `ingest_claude_transcript`). Enumerating it
    // as a session would emit the parent twice per run and let the two
    // files fight over one row's raw_path/source_stamp, so the stamp never
    // matched again and one of them was re-read forever.
    let sidecar_layout = crate::ingest::is_claude_sidecar_file(path);
    let mut head = ClaudeHeadFold {
        primary_record_seen: false,
        sidechain_records: 0,
        identified_records: 0,
        parsed_records: 0,
        head_records_seen: 0,
    };
    for line in bounded.head_records() {
        if !fold_claude_head_line(&mut session, &mut models, &mut head, line, sidecar_layout) {
            break;
        }
    }
    fold_claude_shallow_tail(&mut session, bounded);
    fold_claude_shallow_title(&mut session, bounded);
    // A file laid out as a sidecar whose every identified head record is a
    // sidechain row is a subagent's transcript, for a session whose own
    // transcript is enumerated separately. A primary transcript of only
    // sidechain rows -- inline Task traffic from Claude Code versions that
    // wrote it there -- is still that session's transcript.
    if sidecar_layout
        && crate::ingest::claude_records_are_all_sidechain(
            head.identified_records,
            head.sidechain_records,
        )
    {
        return Ok(None);
    }
    // A file with complete records that parse as nothing is corrupt, not a
    // session. Publishing it under its file stem would put a fabricated
    // row in the catalog and hide the corruption; a diagnostic names it.
    // A file with no complete records at all is merely empty (a session
    // that has just started) and is simply not a session yet.
    if head.parsed_records == 0 {
        anyhow::ensure!(
            head.head_records_seen == 0,
            "no parseable JSON records in the first {} record(s)",
            head.head_records_seen
        );
        return Ok(None);
    }
        let Some(session_id) = session_id.or_else(|| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .map(str::to_string)
                .filter(|s| !s.is_empty())
        }) else {
            return Ok(None);
        };
        session.session_id = session_id;
        session.models = models;
        Ok(Some(session))
}
