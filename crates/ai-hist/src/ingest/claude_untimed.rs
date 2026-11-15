//! The time of a Claude record that reports none of its own.
//!
//! Claude writes some records that carry a message body but no `timestamp`
//! at all. The common one is `api-request-blob`, the content-addressed copy
//! of a request's messages, which holds tool calls, tool results and prompts.
//! An earlier rule stored those at 0, which every reader takes for
//! 1970-01-01, so whole tool-call groups were dated there.
//!
//! `session_events.ts_ms` and `history.timestamp_ms` are `NOT NULL`, so such a
//! record cannot simply go without a time. It inherits the time of the nearest
//! preceding record of its session that reported one. Claude writes the copy
//! straight after the records it copies, so that is the moment it describes,
//! and an inherited time never reorders the session. Identity does not
//! change: an id-less record is identified by a hash of its bytes, never by
//! its time, so a re-read upserts the same rows with the new time. The time
//! is never invented. "Now" would move on every re-read.
//!
//! A record with nothing to inherit keeps only its marker (see
//! [`keep_untimed_record`]).

use super::*;

/// A record's own time, and the time it is stored at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ClaudeRecordTime {
    /// The record's own `timestamp`. A time that does not parse, or one that
    /// is not positive, counts as none.
    pub own_ts_ms: Option<i64>,
    /// The time its rows are stored at: its own, else the nearest preceding
    /// one of its session. `None` only when nothing before it reported a
    /// time either.
    pub ts_ms: Option<i64>,
}

impl ClaudeRecordTime {
    /// Resolve `obj`'s time against `last`, the latest reported time per
    /// session in this transcript, and fold its own time into `last`.
    pub(super) fn resolve(
        obj: &Map<String, Value>,
        session_id: &str,
        last: &mut HashMap<String, i64>,
    ) -> Self {
        let own_ts_ms = obj
            .get("timestamp")
            .and_then(|v| v.as_str().and_then(parse_iso_ms).or_else(|| v.as_i64()))
            .filter(|ts| *ts > 0);
        if let Some(ts) = own_ts_ms {
            last.insert(session_id.to_string(), ts);
        }
        Self {
            own_ts_ms,
            ts_ms: own_ts_ms.or_else(|| last.get(session_id).copied()),
        }
    }

    /// What a parser before this rule stored the record at, for matching the
    /// rows it left behind.
    pub(super) fn stored_before(&self) -> i64 {
        self.own_ts_ms.unwrap_or(0)
    }
}

/// A Claude record with no time of its own and none to inherit; see
/// [`keep_untimed_record`].
pub(super) struct UntimedRecord<'a> {
    pub session_id: &'a str,
    pub message_uuid: &'a str,
    pub parent_id: Option<&'a str>,
    pub obj: &'a Map<String, Value>,
    pub message: Option<&'a Map<String, Value>>,
    /// Whether the record has the user role, and whether it is delegated
    /// traffic: only a user-role record that is not writes `history`.
    pub human_role: bool,
    pub sidechain: bool,
    /// Whether the record already wrote its `{uuid}:marker`.
    pub has_marker: bool,
}

/// Index a record that reports no time when nothing before it in its session
/// reported one either. Its event and history rows have no time to take, and
/// 0 would date them 1970-01-01.
///
/// Such a record keeps only its marker, whose time is nullable, so it still
/// leaves a row behind. Rows an earlier parser wrote for it at 0 are retired,
/// so a re-read heals them. When this leaves out content (a message body, or
/// a subagent completion), the pass reports it as `EVIDENCE_PARTIAL`. The
/// report names the record type, never the content, so the gap is counted
/// rather than silent. If that report ever fires in practice, the follow-up is
/// to hold the record until the next timed record of its session.
pub(super) fn keep_untimed_record(conn: &Connection, record: UntimedRecord<'_>) -> Result<()> {
    let UntimedRecord {
        session_id,
        message_uuid,
        parent_id,
        obj,
        message,
        human_role,
        sidechain,
        has_marker,
    } = record;
    let record_type = obj.get("type").and_then(Value::as_str).unwrap_or("");
    heal_claude_record_rows(
        conn,
        session_id,
        message_uuid,
        HealedRows::ExceptNoticeMarker,
    )?;
    let content = message.and_then(|m| m.get("content"));
    if let Some(content) = content.filter(|_| human_role && !sidechain) {
        let raw = claude_user_text(content);
        let split = control::split_system_reminders(&raw);
        retire_stored_at_zero(conn, session_id, &raw, Some(&split.prompt))?;
    }
    if !has_marker {
        insert_unknown_record_marker(conn, session_id, message_uuid, None, parent_id, record_type)?;
    }
    let left_out = content.is_some()
        || (record_type == "system"
            && tool_result_facts::claude_subagent_notification_facts(obj).is_some());
    if left_out {
        report_left_out(session_id, record_type);
    }
    Ok(())
}

fn report_left_out(session_id: &str, record_type: &str) {
    let record_type = if record_type.is_empty() {
        "record"
    } else {
        record_type
    };
    provider_diagnostics::provider_diagnostic(provider_diagnostics::ProviderSkip {
        source: crate::session_store::Source::Claude,
        code: "EVIDENCE_PARTIAL",
        subject: Some(session_id),
        what: format_args!("keeping an untimed record as a marker only"),
        detail: format_args!(
            "a `{record_type}` record reports no timestamp and no earlier record of its session does"
        ),
    });
}

/// Retire the `history` rows an earlier parser stored at 0 for a record with
/// no time of its own: its whole text, and the prompt the current parser takes
/// from it. A record that reports its own time has none to retire.
pub(super) fn retire_history_stored_at_zero(
    conn: &Connection,
    time: &ClaudeRecordTime,
    session_id: &str,
    raw: &str,
    prompt: Option<&str>,
) -> Result<()> {
    if time.own_ts_ms.is_some() {
        return Ok(());
    }
    retire_stored_at_zero(conn, session_id, raw, prompt)
}

fn retire_stored_at_zero(
    conn: &Connection,
    session_id: &str,
    raw: &str,
    prompt: Option<&str>,
) -> Result<()> {
    if !raw.is_empty() {
        retire_claude_history_row(conn, session_id, 0, raw)?;
    }
    if let Some(prompt) = prompt.filter(|prompt| !prompt.is_empty() && *prompt != raw) {
        retire_claude_history_row(conn, session_id, 0, prompt)?;
    }
    Ok(())
}
