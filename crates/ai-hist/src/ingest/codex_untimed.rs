//! The time of a Codex rollout line that reports none of its own.
//!
//! Codex writes a `timestamp` on every rollout line today, but a line that
//! omits it, or carries one that does not parse, used to be stored at 0,
//! which every reader takes for 1970-01-01. `session_events.ts_ms` and
//! `history.timestamp_ms` are `NOT NULL`, so such a line inherits the time of
//! the nearest preceding line of its rollout that reported one: a rollout is
//! one thread written in order, so that is the moment it follows, and an
//! inherited time never reorders the thread. Identity does not change: a
//! Codex row is keyed by its line index, never by its time, so a re-read
//! upserts the same rows with the new time. The time is never invented.
//! "Now" would move on every re-read.
//!
//! A line with nothing to inherit keeps only its marker (see
//! [`keep_untimed_line`]). The rule is the Claude reader's; see
//! `claude_untimed`.

use super::transcript_cursor::{CodexCursorState, TranscriptReader};
use super::*;

/// A line's own time: its `timestamp`, when it parses to a positive time.
fn own_ts_ms(value: &Value) -> Option<i64> {
    value
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_iso_ms)
        .filter(|ts| *ts > 0)
}

/// What a parser before this rule stored a line at, for matching the rows it
/// left behind: the time it parsed, or 0 when it parsed none.
pub(super) fn stored_before(value: &Value) -> i64 {
    value
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_iso_ms)
        .unwrap_or(0)
}

/// The latest time a rollout line reported, as the walk reads it.
pub(super) struct LineClock {
    /// `None` until a line reports a time.
    pub last: Option<i64>,
}

impl LineClock {
    /// The state a pass resumes with from `saved`, the clock it resumes
    /// with, and the bytes read to build it.
    ///
    /// A pass from byte zero starts from nothing. A cursor an earlier build
    /// committed carries no line time, yet resumes past the timed lines an
    /// appended untimed line follows, so the bytes it already read are
    /// folded, once: every line that reports a time takes part, and the walk
    /// resumes holding exactly the time a read from byte zero holds there.
    pub(super) fn resume(
        reader: &mut TranscriptReader,
        saved: Option<&CodexCursorState>,
    ) -> Result<(CodexCursorState, Self, u64)> {
        let start_offset = reader.start_offset();
        let mut resume = if start_offset == 0 {
            CodexCursorState {
                record_ts_from_start: true,
                ..CodexCursorState::default()
            }
        } else {
            saved.cloned().unwrap_or_default()
        };
        let mut read = 0;
        if !resume.record_ts_from_start {
            let last = &mut resume.record_ts;
            read = reader.replay(0, start_offset, |line| {
                // The walk observes a line's time only once it has an
                // object payload; the fold takes part on the same terms.
                if let Ok(value) = serde_json::from_str::<Value>(line.trim()) {
                    if value.get("payload").is_some_and(Value::is_object) {
                        *last = own_ts_ms(&value).or(*last);
                    }
                }
                Ok(())
            })?;
            resume.record_ts_from_start = true;
        }
        let clock = Self {
            last: resume.record_ts,
        };
        Ok((resume, clock, read))
    }

    /// Fold `value`'s own time into the clock and the pass's activity window,
    /// and return `(own, stored)`: its own time, and the time its rows are
    /// stored at, its own else the nearest preceding line's. `stored` is
    /// `None` only when no line before it reported a time either.
    pub(super) fn observe(
        &mut self,
        value: &Value,
        outcome: &mut CodexIngestOutcome,
    ) -> (Option<i64>, Option<i64>) {
        let own = own_ts_ms(value);
        if let Some(ts) = own {
            outcome.first_ts.get_or_insert(ts);
            outcome.last_ts = Some(outcome.last_ts.map_or(ts, |last| last.max(ts)));
            self.last = Some(ts);
        }
        (own, own.or(self.last))
    }
}

/// One Codex rollout line that has not yet been shown to write anything.
pub(super) struct UnwrittenCodexLine {
    pub(super) index: usize,
    /// `None` when the line has no time and none to inherit; never 0.
    pub(super) ts_ms: Option<i64>,
    pub(super) subkind: String,
    pub(super) turn_id: Option<String>,
}

impl UnwrittenCodexLine {
    /// The line to record as an `unknown` marker if it writes nothing, or
    /// `None` for a state-only line, which writes no row by design.
    pub(super) fn unless_state_only(
        index: usize,
        ts_ms: Option<i64>,
        value: &Value,
    ) -> Option<Self> {
        let text = |key: &str| value.get(key).and_then(Value::as_str).unwrap_or("");
        let payload = value.get("payload").and_then(Value::as_object)?;
        let line_type = text("type");
        let payload_type = payload.get("type").and_then(Value::as_str).unwrap_or("");
        if codex_line_is_state_only(line_type, payload_type, payload) {
            return None;
        }
        Some(Self {
            index,
            ts_ms: ts_ms.filter(|ts| *ts > 0),
            subkind: if payload_type.is_empty() {
                line_type.to_string()
            } else {
                payload_type.to_string()
            },
            turn_id: payload
                .get("turn_id")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    }
}

/// The time a line with no time and none to inherit is read at when it
/// carries walk state (see [`resolve_line_ts`]). Every arm such a line
/// reaches stores only markers, and a marker stores 0 as `NULL`, so nothing is
/// stored at it.
pub(super) const STATE_ONLY_TS: i64 = 0;

/// Lines whose arms carry state a later line needs (the usage baseline and
/// request boundary of `token_count`, the turn of `turn_context` and the
/// task lifecycle, the model of `thread_settings_applied`) and write only
/// markers. Skipping one would lose that state: an untimed opening
/// `token_count` left out would charge its whole cumulative total to the
/// next request.
fn carries_state_only(value: &Value) -> bool {
    let line_type = value.get("type").and_then(Value::as_str);
    let payload_type = value.pointer("/payload/type").and_then(Value::as_str);
    match (line_type, payload_type) {
        (Some("turn_context" | "session_meta"), _) => true,
        (Some("event_msg"), Some(kind)) => {
            matches!(
                kind,
                "token_count"
                    | "task_started"
                    | "task_complete"
                    | "turn_aborted"
                    | "thread_settings_applied"
            ) || kind.ends_with("_delta")
        }
        (Some("response_item"), Some(kind)) => kind.ends_with("_delta"),
        _ => false,
    }
}

/// The time the walk reads `value` at, given `line_ts`, its own time else the
/// nearest preceding one, or `None` when the line is left out.
///
/// A line with nothing to inherit that carries only state is read at
/// [`STATE_ONLY_TS`]. Any other such line has event, tool and history rows
/// that have no time to take, and 0 would date them 1970-01-01. It is
/// recorded as an `unknown` marker, whose time is nullable, through
/// `unwritten`, so it still leaves a row behind. Rows an earlier parser wrote
/// for it at 0 are retired, so a re-read heals them, and only those: a row
/// stored with a real time stays, and markers, whose time is never 0, are
/// never among them. The pass reports the line as `EVIDENCE_PARTIAL`, naming
/// its type and never its content, so the gap is counted rather than silent.
pub(super) fn resolve_line_ts(
    conn: &Connection,
    session_id: &str,
    (index, value, line_ts): (usize, &Value, Option<i64>),
    unwritten: &mut Option<UnwrittenCodexLine>,
) -> Result<Option<i64>> {
    if line_ts.is_some() {
        return Ok(line_ts);
    }
    if carries_state_only(value) {
        return Ok(Some(STATE_ONLY_TS));
    }
    let line = UnwrittenCodexLine::unless_state_only(index, None, value);
    if let Some(line) = &line {
        retire_line_rows_at_zero(conn, session_id, index, value)?;
        report_left_out(session_id, &line.subkind);
    }
    *unwritten = line;
    Ok(None)
}

fn retire_line_rows_at_zero(
    conn: &Connection,
    session_id: &str,
    index: usize,
    value: &Value,
) -> Result<()> {
    let prefix = format!("{index}:");
    let next = format!("{index};");
    crate::store::retire_evidence_share(
        conn,
        "session_events",
        "source = 'codex' AND session_id = ?1 AND event_uid >= ?2 AND event_uid < ?3 \
           AND ts_ms = 0",
        params![session_id, prefix, next],
        SessionLocation::Local,
    )?;
    let call_id = value
        .pointer("/payload/call_id")
        .and_then(Value::as_str)
        .unwrap_or("");
    for table in ["tool_calls", "file_edits"] {
        crate::store::retire_evidence_share(
            conn,
            table,
            "source = 'codex' AND session_id = ?1 AND ts_ms = 0 \
               AND ((message_id >= ?2 AND message_id < ?3) \
                    OR (?4 <> '' AND (tool_use_id = ?4 \
                        OR substr(tool_use_id, 1, length(?4) + 1) = ?4 || '#')))",
            params![session_id, prefix, next, call_id],
            SessionLocation::Local,
        )?;
    }
    if let Some(message) = codex::human_message(value) {
        retire_history_at_zero(conn, session_id, None, &message.text)?;
    }
    Ok(())
}

/// Retire the `history` row an earlier parser stored at 0 for `prompt`,
/// when its line has no time of its own (`own_ts` is `None`).
pub(super) fn retire_history_at_zero(
    conn: &Connection,
    session_id: &str,
    own_ts: Option<i64>,
    prompt: &str,
) -> Result<()> {
    if own_ts.is_some() {
        return Ok(());
    }
    conn.execute(
        "DELETE FROM history WHERE source = 'codex' AND session_id = ? \
           AND timestamp_ms = 0 AND prompt = ?",
        params![session_id, prompt],
    )?;
    Ok(())
}

fn report_left_out(session_id: &str, subkind: &str) {
    let subkind = if subkind.is_empty() { "line" } else { subkind };
    provider_diagnostics::provider_diagnostic(provider_diagnostics::ProviderSkip {
        source: crate::session_store::Source::Codex,
        code: "EVIDENCE_PARTIAL",
        subject: Some(session_id),
        what: format_args!("keeping an untimed rollout line as a marker only"),
        detail: format_args!(
            "a `{subkind}` line reports no timestamp and no earlier line of its rollout does"
        ),
    });
}

#[cfg(test)]
mod tests;
