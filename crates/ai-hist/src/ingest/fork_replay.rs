//! The parent history a forked Codex rollout copies: the span the replay gate
//! retires under the child, and the one marker that stands for it.

use anyhow::Result;
use rusqlite::{params, Connection};
use serde_json::{json, Map, Value};

use super::*;

/// The span of a forked rollout that is a copy of its parent's history.
///
/// When Codex forks a thread — a human "fork conversation" or a spawned
/// subagent — it writes the child's own `session_meta` and then **replays the
/// parent's history** into the child's file: the parent's `session_meta`, its
/// turns' `task_started` / `turn_context` / message records, and whatever
/// `token_count` snapshots they carried. Those records are the parent's
/// evidence, already indexed under the parent's id; indexing them again under
/// the child's id duplicates every replayed prompt in `history` and every
/// replayed message in `session_events`.
///
/// Which records form the span is decided by [`codex::ForkReplayGate`], the
/// one statement of the rule, shared with shallow discovery. It is armed only
/// by explicit evidence in a rollout whose opening `session_meta` named its
/// parent in `forked_from_id` or `thread_spawn.parent_thread_id`: the
/// parent's own `session_meta` reappearing, or, for builds that copy the
/// history straight after the child's `session_meta`, a record before the
/// child's first own turn that only a copy can hold. It closes at the first
/// turn it can attribute to the child.
///
/// Every line inside the span writes nothing; one `fork_replay_boundary`
/// marker, keyed by the span's first line, accounts for all of them. A readable `token_count` inside it is the parent's cumulative total,
/// kept as the child's inherited baseline so its first request is not charged
/// the parent's whole context.
pub(super) struct ForkReplaySpan {
    /// Line index of the replayed parent `session_meta`.
    first_line: usize,
    /// Line index of the last replayed line read so far.
    last_line: usize,
    /// The envelope timestamp of the replayed `session_meta`: when the fork
    /// wrote the copy.
    ts_ms: i64,
    /// Tool call ids the replay carried, so rows an earlier parser indexed
    /// under the child for them can be retired.
    call_ids: Vec<String>,
    /// `(timestamp, prompt)` of each replayed human turn, for the same reason
    /// on `history`.
    prompts: Vec<(i64, String)>,
    /// The last readable cumulative snapshot inside the replay.
    pub(super) inherited: Option<CodexTokenTotals>,
    /// The provider's `info` object that `inherited` was read from, as
    /// written.
    inherited_snapshot: Option<Value>,
    /// What opened the span: the marker's `subkind`.
    opening: codex::ReplayOpening,
}

impl ForkReplaySpan {
    pub(super) fn new(first_line: usize, ts_ms: i64, opening: codex::ReplayOpening) -> Self {
        Self {
            opening,
            first_line,
            last_line: first_line,
            ts_ms,
            call_ids: Vec::new(),
            prompts: Vec::new(),
            inherited: None,
            inherited_snapshot: None,
        }
    }

    /// The marker that stands for this span, keyed by its first line.
    pub(super) fn marker_uid(&self) -> String {
        format!("{}:fork_replay", self.first_line)
    }

    /// Remember what a replayed line would have written.
    pub(super) fn absorb(&mut self, index: usize, value: &Value, payload: &Map<String, Value>) {
        self.last_line = index;
        if let Some(call_id) = payload
            .get("call_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        {
            if !self.call_ids.iter().any(|seen| seen == call_id) {
                self.call_ids.push(call_id.to_string());
            }
        }
        if let Some(message) = codex::human_message(value) {
            let ts_ms = value
                .get("timestamp")
                .and_then(Value::as_str)
                .and_then(parse_iso_ms)
                .unwrap_or(0);
            self.prompts.push((ts_ms, message.text));
        }
        if payload.get("type").and_then(Value::as_str) == Some("token_count") {
            let info = payload.get("info");
            if let Some(totals) = info
                .and_then(|info| info.get("total_token_usage"))
                .and_then(CodexTokenTotals::from_usage)
            {
                self.inherited = Some(totals);
                self.inherited_snapshot = info.cloned().map(bound_usage_info);
            }
        }
    }
}

/// Whether the cumulative total a fork inherited from its replay is the
/// baseline of the child's own counter, decided at the child's first readable
/// snapshot. Returns `(kept, basis)`.
///
/// codex-rs seeds a fork's usage from the replay: `record_initial_history`
/// handles `InitialHistory::Forked` by passing the copied items to
/// `last_token_info_from_rollout` and the result to `set_token_info`
/// (`codex-rs/core/src/session/mod.rs`, openai/codex@d5e6526), and each later
/// request adds its usage to that total, so `total_token_usage` grows by
/// exactly `last_token_usage`. A child written that way continues from its
/// parent's total. A build that did not seed it starts from zero, and then
/// its first snapshot's total *is* its last request.
///
/// So `last_token_usage` decides when the snapshot carries it: `total ==
/// last` is a restart and the inherited baseline is dropped; `total ==
/// inherited + last` is a continuation and it is kept. Without that evidence
/// a total below the inherited one can only be a restart, and anything else
/// keeps the baseline. Comparing magnitudes alone misfires when a restarted
/// child's first request exceeds a small parent total.
pub(super) fn inherited_baseline_verdict(
    inherited: CodexTokenTotals,
    total: CodexTokenTotals,
    last: Option<CodexTokenTotals>,
) -> (bool, &'static str) {
    if let Some(last) = last {
        if total == last {
            return (false, "last_token_usage");
        }
        if inherited.plus(&last) == Some(total) {
            return (true, "last_token_usage");
        }
    }
    if total.regressed_from(&inherited) {
        return (false, "regression");
    }
    (true, "no_evidence")
}

/// Write the one marker that stands for a replayed span, and retire whatever
/// an earlier parser indexed under the child for the lines inside it.
///
/// `closed_by` is the child turn that ended the span and the fact that
/// attributed it, or `None` when the readable file ended inside the replay
/// (a live fork whose first own turn is not written yet). The marker is keyed
/// by the span's first line, so the pass that later sees the span close
/// rewrites the same row.
pub(super) fn record_codex_fork_replay(
    conn: &Connection,
    meta: &CodexSessionMeta,
    span: &ForkReplaySpan,
    closed_by: Option<(Option<&str>, &str)>,
    inherited_baseline: Option<&str>,
) -> Result<()> {
    let session_id = meta.session_id.as_str();
    let marker_uid = span.marker_uid();
    let payload_json = json!({
        "parent_session_id": meta.fork_parent_id,
        "first_line": span.first_line,
        "last_line": span.last_line,
        "replayed_lines": span.last_line + 1 - span.first_line,
        "closed_by_turn_id": closed_by.and_then(|(turn, _)| turn),
        "closed_by": closed_by.map(|(_, basis)| basis),
        "inherited_total_tokens": span.inherited.map(|totals| totals.total),
        // The replayed `token_count` `info` that total came from, as written,
        // bounded by `bound_usage_info` (counters are never truncated).
        "inherited_snapshot": span.inherited_snapshot,
        // `pending` until the child's first readable snapshot, then
        // `applied` or `dropped` with `inherited_baseline_basis` (see
        // `inherited_baseline_verdict`); null when nothing was inherited or
        // the span is still open.
        "inherited_baseline": inherited_baseline,
    })
    .to_string();
    insert_session_marker(
        conn,
        "codex",
        session_id,
        &NewSessionMarker {
            marker_uid: &marker_uid,
            ts_ms: (span.ts_ms != 0).then_some(span.ts_ms),
            message_id: None,
            parent_id: meta.fork_parent_id.as_deref(),
            turn_id: closed_by.and_then(|(turn, _)| turn),
            kind: "fork_replay_boundary",
            subkind: Some(span.opening.subkind()),
            text: None,
            payload_json: Some(&payload_json),
        },
    )?;
    // Rows keyed by line index: events and markers from the replayed lines.
    let (first, last) = (span.first_line as i64, span.last_line as i64);
    let in_span = "source = 'codex' AND session_id = ?1 \
                   AND instr({col}, ':') > 1 \
                   AND CAST(substr({col}, 1, instr({col}, ':') - 1) AS INTEGER) BETWEEN ?2 AND ?3";
    crate::store::retire_evidence_share(
        conn,
        "session_events",
        &in_span.replace("{col}", "event_uid"),
        params![session_id, first, last],
        SessionLocation::Local,
    )?;
    crate::store::retire_evidence_share(
        conn,
        "session_markers",
        &format!(
            "{} AND marker_uid != ?4",
            in_span.replace("{col}", "marker_uid")
        ),
        params![session_id, first, last, marker_uid],
        SessionLocation::Local,
    )?;
    for call_id in &span.call_ids {
        crate::store::retire_evidence_share(
            conn,
            "tool_calls",
            "source = 'codex' AND session_id = ? AND tool_use_id = ?",
            params![session_id, call_id],
            SessionLocation::Local,
        )?;
        crate::store::retire_evidence_share(
            conn,
            "file_edits",
            "source = 'codex' AND session_id = ? AND substr(tool_use_id, 1, ?) = ?",
            params![session_id, call_id.len() as i64 + 1, format!("{call_id}#")],
            SessionLocation::Local,
        )?;
    }
    for (ts_ms, prompt) in &span.prompts {
        conn.execute(
            "DELETE FROM history WHERE source = 'codex' AND session_id = ? \
               AND timestamp_ms = ? AND prompt = ?",
            params![session_id, ts_ms, prompt],
        )?;
    }
    Ok(())
}
