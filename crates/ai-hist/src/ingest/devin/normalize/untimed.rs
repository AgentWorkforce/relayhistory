//! The time of a Devin node, or orphan tool call, that records none.
//!
//! A node with no `created_at` takes the session's own time, as it always
//! has, else the time of the nearest preceding node that had one. Neither is
//! invented: "now" would move on every re-read. With nothing to take, it is
//! never stored at 0, which every reader takes for 1970-01-01: it keeps a
//! marker with no time, and the pass reports it as `EVIDENCE_PARTIAL`. Rows
//! an earlier parser stored for it at 0 are not marked present, so the
//! session's reconcile retires them.

use super::*;
use crate::ingest::provider_diagnostics;

/// A Devin time, when it is one: 0, which the CLI can write, is none.
pub(super) fn positive(ts: Option<i64>) -> Option<i64> {
    ts.filter(|ts| *ts > 0)
}

/// The time `node`'s rows are stored at, folding a time it reports into
/// `inherited`. `None` only when nothing before it had a time either.
pub(super) fn node_ts(
    node: &DevinNode,
    base_ts: Option<i64>,
    inherited: &mut Option<i64>,
) -> Option<i64> {
    let own = positive(node.created_ms);
    if own.is_some() {
        *inherited = own;
    }
    own.or(base_ts).or(*inherited)
}

/// The time orphan tool calls are stored at: the session's last activity,
/// else its own time, else the latest node time. `None` when there is none.
pub(super) fn orphan_ts(
    loaded: &DevinSession,
    base_ts: Option<i64>,
    latest_node_ts: Option<i64>,
) -> Option<i64> {
    loaded
        .info
        .last_activity_ms
        .filter(|ts| *ts > 0)
        .or(base_ts)
        .or(latest_node_ts)
}

/// Count the orphan tool calls left out for want of any time.
pub(super) fn report_untimed_orphans(
    session_id: &str,
    loaded: &DevinSession,
    referenced: &BTreeSet<String>,
) {
    let left_out = loaded
        .tools
        .keys()
        .filter(|call_id| !referenced.contains(*call_id))
        .count();
    if left_out > 0 {
        report(
            session_id,
            format_args!("{left_out} orphan tool call(s) and the session record no time"),
        );
    }
}

fn report(session_id: &str, detail: std::fmt::Arguments<'_>) {
    provider_diagnostics::provider_diagnostic(provider_diagnostics::ProviderSkip {
        source: crate::session_store::Source::Devin,
        code: "EVIDENCE_PARTIAL",
        subject: Some(session_id),
        what: format_args!("keeping untimed evidence as a marker only"),
        detail,
    });
}

impl DevinNormalize<'_> {
    /// The tool calls an untimed node names, as the role arms collect them,
    /// so they stay with the node rather than turning into orphans stored at
    /// a later node's time.
    fn note_tool_references(&mut self, node: &DevinNode) {
        let Some(message) = node.message.as_ref() else {
            return;
        };
        let calls = message.get("tool_calls").and_then(Value::as_array);
        for (index, call) in calls.into_iter().flatten().enumerate() {
            let call_id = str_field(call.into(), "id")
                .map(str::to_string)
                .unwrap_or_else(|| format!("n{}:{}", node.node_id, index));
            self.referenced_tool_calls.insert(call_id);
        }
        if let Some(id) = str_field(message.into(), "tool_call_id").filter(|id| !id.is_empty()) {
            self.referenced_tool_calls.insert(id.to_string());
        }
    }

    /// Keep a node with no time and none to take as a marker with no time,
    /// under its own message id, so children that name it as their parent
    /// still anchor to an addressable row.
    pub(super) fn keep_untimed_node(
        &mut self,
        node: &DevinNode,
        message_id: &str,
        parent_id: Option<&str>,
    ) -> Result<()> {
        let uid = format!("n{}:untimed", node.node_id);
        self.present_markers.insert(uid.clone());
        self.counts.markers += insert_session_marker(
            self.conn,
            SOURCE,
            self.session_id,
            &NewSessionMarker {
                marker_uid: &uid,
                ts_ms: None,
                message_id: Some(message_id),
                parent_id,
                turn_id: None,
                kind: "unknown",
                subkind: Some("untimed_node"),
                text: None,
                payload_json: None,
            },
        )?;
        self.note_tool_references(node);
        if node.message.is_some() {
            report(
                self.session_id,
                format_args!("a message node and the session record no time"),
            );
        }
        Ok(())
    }
}
