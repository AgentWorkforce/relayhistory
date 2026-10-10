//! Head and tail folds for a Claude shallow read.

use super::{
    claude_substantive_prompt, claude_timestamp, parse_record, push_unique, BoundedJsonl,
    ShallowSession,
};
use serde_json::Value;

pub(super) struct ClaudeHeadFold {
    pub(super) primary_record_seen: bool,
    pub(super) sidechain_records: usize,
    pub(super) identified_records: usize,
    pub(super) parsed_records: usize,
    pub(super) head_records_seen: usize,
}

/// One head record. `false` once every catalog field is settled and reading
/// further head lines cannot change the row.
pub(super) fn fold_claude_head_line(
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

pub(super) fn fold_claude_shallow_tail(session: &mut ShallowSession, bounded: &BoundedJsonl) {
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
