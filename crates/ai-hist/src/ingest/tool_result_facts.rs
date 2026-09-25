//! Per-tool-result fidelity facts shared by every provider parser.
//!
//! A tool result is stored as a `session_events` row, and until now the only
//! thing that row said about the payload was the (possibly truncated) `text`
//! column. Consumers that rank tools by output size, detect duplicate or
//! reverted payloads, or rebuild a span tree need the *raw* measurements
//! instead: how many bytes the provider actually handed back, whether the
//! harness had already truncated it, a content hash, and where the row sits in
//! the transcript's order.
//!
//! Every parser computes these through one helper so the columns mean the same
//! thing across providers. [`ToolResultFacts::from_payload`] is the shared
//! entry point; the per-provider wrappers below add the status / source /
//! error-signal classification that only that provider's record shape can
//! supply.
//!
//! Byte, hash and truncation measurements deliberately mirror
//! `relayburn-sdk`'s `reader::hash` and `reader::claude::tool_results`: a
//! string payload is measured and hashed over its raw UTF-8 bytes, and any
//! other JSON payload over its stable stringification (object keys sorted, so
//! the hash does not depend on the provider's key order).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

/// A `tool_result` content block inside a provider message.
pub const EVENT_SOURCE_TOOL_RESULT: &str = "tool_result";
/// A harness notification that a delegated subagent finished.
pub const EVENT_SOURCE_SUBAGENT_NOTIFICATION: &str = "subagent_notification";
/// A Codex `function_call_output` / `custom_tool_call_output` response item.
pub const EVENT_SOURCE_FUNCTION_CALL_OUTPUT: &str = "function_call_output";

/// The result is known to have finished without an error.
pub const STATUS_COMPLETED: &str = "completed";
/// The result is known to have failed.
pub const STATUS_ERRORED: &str = "errored";
/// The result was cancelled before it produced an outcome.
pub const STATUS_CANCELLED: &str = "cancelled";
/// The provider has not yet said how the call ended.
pub const STATUS_UNKNOWN: &str = "unknown";
/// The call is still running.
pub const STATUS_RUNNING: &str = "running";

/// Claude's own `is_error` flag on the `tool_result` block.
pub const ERROR_SIGNAL_TOOL_RESULT: &str = "tool_result.is_error";
/// A Codex `exec_command_end` with a non-zero `exit_code`.
pub const ERROR_SIGNAL_EXIT_CODE: &str = "exit_code";
/// A Codex `patch_apply_end` with `success: false`.
pub const ERROR_SIGNAL_PATCH_APPLY: &str = "patch_apply";
/// A Codex `mcp_tool_call_end` whose `result` carries `Err`.
pub const ERROR_SIGNAL_MCP_ERR: &str = "mcp_err";
/// A Muse Code `tool_batch.effect.terminal` whose `outcome.kind` is not
/// `completed`.
pub const ERROR_SIGNAL_MUSE_TOOL_OUTCOME: &str = "tool_batch.effect";
/// A harness subagent notification reporting a failed or cancelled child.
pub const ERROR_SIGNAL_SUBAGENT_STATUS: &str = "subagent_status";

/// Harness truncation markers, matched case-insensitively.
///
/// A provider that truncates a large tool result before writing the transcript
/// leaves one of these in the payload so the model can react to it. Detecting
/// the marker is what separates "this tool genuinely returned 8 KB" from "this
/// tool returned far more and the harness cut it", which is the difference
/// between a real measurement and a floor.
const TRUNCATION_MARKERS: &[&str] = &[
    "<system-truncated>",
    "[truncated]",
    "output truncated",
    "result truncated",
    "response truncated",
    "truncated to ",
];

/// Whether a payload carries a recognized harness truncation marker.
pub fn detect_truncation_marker(payload: &str) -> bool {
    let lower = payload.to_ascii_lowercase();
    TRUNCATION_MARKERS
        .iter()
        .any(|marker| lower.contains(marker))
}

/// Stable JSON stringification: object keys sorted, array order preserved.
///
/// `serde_json`'s default `Map` is already ordered, but the hash contract must
/// not silently depend on which `serde_json` features happen to be enabled in
/// a consumer's build, so the ordering is applied explicitly here.
pub fn stable_stringify(value: &Value) -> String {
    let mut out = String::new();
    write_stable(value, &mut out);
    out
}

fn write_stable(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(key.clone()).to_string());
                out.push(':');
                write_stable(&map[key], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_stable(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

/// sha256 over `bytes`, hex-encoded and truncated to the first 16 characters.
pub fn content_hash(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())[..16].to_string()
}

/// Everything one tool-result row records about its payload and its place in
/// the transcript. Every field is optional: a provider that does not expose a
/// fact leaves it null rather than guessing a value for it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolResultFacts {
    /// Raw UTF-8 byte length of the payload, before any truncation this crate
    /// applies to the `text` column.
    pub payload_bytes: Option<i64>,
    /// `Some(true)` when the harness had already truncated the payload.
    pub payload_truncated: Option<bool>,
    /// First 16 hex characters of the payload's sha256.
    pub payload_hash: Option<String>,
    /// n-th result recorded for the same `tool_use_id`, from zero.
    pub call_index: Option<i64>,
    /// Position of this result in the transcript's tool-result order.
    pub event_index: Option<i64>,
    /// `running` / `completed` / `errored` / `cancelled` / `unknown`.
    pub result_status: Option<String>,
    /// Which rail the row came from; see the `EVENT_SOURCE_*` constants.
    pub event_source: Option<String>,
    /// Which provider signal set the error, when one did.
    pub error_signal: Option<String>,
    /// Delegated child session a subagent notification refers to.
    pub subagent_session_id: Option<String>,
    /// Delegated child agent id a subagent notification refers to.
    pub agent_id: Option<String>,
    /// Provider call id this result answers.
    pub tool_use_id: Option<String>,
}

impl ToolResultFacts {
    /// Measure a raw tool-result payload: byte length, content hash, and
    /// whether the harness truncated it. Ordering, status and provenance are
    /// the caller's to fill in — they cannot be read off the payload.
    ///
    /// A null payload is "nothing was recorded", not "zero bytes", so it
    /// leaves every measurement null.
    pub fn from_payload(content: &Value) -> Self {
        let measured = match content {
            Value::Null => None,
            Value::String(s) => Some(s.clone()),
            other => Some(stable_stringify(other)),
        };
        let Some(text) = measured else {
            return Self::default();
        };
        Self {
            payload_bytes: Some(text.len() as i64),
            payload_truncated: Some(detect_truncation_marker(&text)),
            payload_hash: Some(content_hash(text.as_bytes())),
            ..Self::default()
        }
    }

    /// Record where this result sits in the transcript.
    pub fn with_ordering(mut self, call_index: Option<i64>, event_index: i64) -> Self {
        self.call_index = call_index;
        self.event_index = Some(event_index);
        self
    }
}

/// Assigns the per-`tool_use_id` `call_index` and the transcript-global
/// `event_index` as a parser walks a transcript.
///
/// The counters are per-parse, and every provider parser in this crate
/// re-reads its transcript from the start, so a re-sync reproduces exactly the
/// same indexes rather than advancing them. [`ToolResultIndexer::resume_from`]
/// exists for a parser that resumes mid-file from a hydration checkpoint's
/// `last_tool_result_index` alone. The incremental reader carries the whole
/// indexer on its cursor instead: `calls` counts results per `tool_use_id`,
/// and a call whose results straddle a resume boundary would restart at zero
/// if only the event index came back.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ToolResultIndexer {
    #[serde(default)]
    next_event_index: i64,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    calls: HashMap<String, i64>,
}

impl ToolResultIndexer {
    /// Continue a sequence that a previous parse left at `last_event_index`.
    pub fn resume_from(last_event_index: i64) -> Self {
        Self {
            next_event_index: last_event_index.saturating_add(1),
            calls: HashMap::new(),
        }
    }

    /// Claim the next `(call_index, event_index)` pair. An empty
    /// `tool_use_id` yields no call index — the results of unidentified calls
    /// cannot be counted per call without merging unrelated ones.
    pub fn next(&mut self, tool_use_id: &str) -> (Option<i64>, i64) {
        let event_index = self.next_event_index;
        self.next_event_index += 1;
        if tool_use_id.is_empty() {
            return (None, event_index);
        }
        let entry = self.calls.entry(tool_use_id.to_string()).or_insert(0);
        let call_index = *entry;
        *entry += 1;
        (Some(call_index), event_index)
    }

    /// The highest index this parse assigned, or `None` when it assigned none.
    pub fn last_event_index(&self) -> Option<i64> {
        (self.next_event_index > 0).then(|| self.next_event_index - 1)
    }
}

/// Facts for one Claude `tool_result` content block.
pub fn claude_tool_result_facts(block: &Value) -> ToolResultFacts {
    let content = block.get("content").unwrap_or(&Value::Null);
    let mut facts = ToolResultFacts::from_payload(content);
    facts.event_source = Some(EVENT_SOURCE_TOOL_RESULT.to_string());
    facts.tool_use_id = block
        .get("tool_use_id")
        .or_else(|| block.get("toolUseId"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_string);
    match block.get("is_error").and_then(Value::as_bool) {
        Some(true) => {
            facts.result_status = Some(STATUS_ERRORED.to_string());
            facts.error_signal = Some(ERROR_SIGNAL_TOOL_RESULT.to_string());
        }
        Some(false) => facts.result_status = Some(STATUS_COMPLETED.to_string()),
        // Claude omits the flag on success far more often than it sets it,
        // but "absent" is not the same claim as "succeeded" — a block with no
        // flag is reported as completed only because Claude never writes a
        // result block for a call that has not finished.
        None => facts.result_status = Some(STATUS_COMPLETED.to_string()),
    }
    facts
}

/// Facts for a Claude `type: "system"` subagent notification line.
///
/// Returns `None` for a system line that names no delegated child: those are
/// ordinary harness chatter, not a tool result.
pub fn claude_subagent_notification_facts(line: &Map<String, Value>) -> Option<ToolResultFacts> {
    let tool_use_id = first_str(
        line,
        &[
            "parent_tool_use_id",
            "parentToolUseId",
            "parentToolUseID",
            "tool_use_id",
            "toolUseId",
        ],
    );
    let agent_id = first_str(line, &["agent_id", "agentId"]);
    let subagent_session_id = first_str(line, &["subagent_session_id", "subagentSessionId"]);
    if agent_id.is_none() && subagent_session_id.is_none() {
        return None;
    }
    let content = ["content", "output", "result", "message"]
        .iter()
        .find_map(|key| line.get(*key))
        .unwrap_or(&Value::Null);
    let mut facts = ToolResultFacts::from_payload(content);
    facts.event_source = Some(EVENT_SOURCE_SUBAGENT_NOTIFICATION.to_string());
    facts.tool_use_id = tool_use_id;
    facts.agent_id = agent_id;
    facts.subagent_session_id = subagent_session_id;
    let status = subagent_status(line);
    if status == STATUS_ERRORED || status == STATUS_CANCELLED {
        facts.error_signal = Some(ERROR_SIGNAL_SUBAGENT_STATUS.to_string());
    }
    facts.result_status = Some(status.to_string());
    Some(facts)
}

fn subagent_status(line: &Map<String, Value>) -> &'static str {
    if line.get("is_error").and_then(Value::as_bool) == Some(true)
        || line.get("isError").and_then(Value::as_bool) == Some(true)
    {
        return STATUS_ERRORED;
    }
    let raw = first_str(
        line,
        &["status", "state", "terminal_status", "terminalStatus"],
    );
    match raw.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("completed" | "complete" | "success" | "succeeded" | "ok" | "done") => {
            STATUS_COMPLETED
        }
        Some("errored" | "error" | "failed" | "failure") => STATUS_ERRORED,
        Some("cancelled" | "canceled" | "aborted" | "interrupted") => STATUS_CANCELLED,
        Some("running" | "in_progress" | "started" | "pending") => STATUS_RUNNING,
        _ => {
            // The subtype is the notification's own name; `subagent_completed`
            // is the only one that states an outcome without a status field.
            match line.get("subtype").and_then(Value::as_str) {
                Some("subagent_completed") => STATUS_COMPLETED,
                _ => STATUS_UNKNOWN,
            }
        }
    }
}

/// Facts for a Codex `function_call_output` / `custom_tool_call_output`.
///
/// The status stays `unknown` here on purpose: Codex reports a call's outcome
/// out of band (`exec_command_end`, `patch_apply_end`, `mcp_tool_call_end`),
/// and those events are only guaranteed to have all arrived by `task_complete`.
/// The parser back-patches the row then.
pub fn codex_output_facts(output: &Value, call_id: &str) -> ToolResultFacts {
    let mut facts = ToolResultFacts::from_payload(output);
    facts.event_source = Some(EVENT_SOURCE_FUNCTION_CALL_OUTPUT.to_string());
    facts.result_status = Some(STATUS_UNKNOWN.to_string());
    facts.tool_use_id = (!call_id.is_empty()).then(|| call_id.to_string());
    facts
}

/// Facts for one Muse Code tool result.
///
/// Muse writes no error flag on the result itself. The call's outcome is a
/// separate `tool_batch.effect.terminal` record, passed here as
/// `(outcome.kind, reason)`; without one the status stays `unknown`. A `bash`
/// result that completed as a call but whose command exited non-zero is an
/// error too, the way Codex reads `exit_code`. Only a `bash` result: any
/// other tool's output is the tool's data, and a file that happens to hold
/// `{"exit_code": 1}` is not a failed read.
pub fn muse_tool_result_facts(
    text: Option<&str>,
    call_id: &str,
    tool_name: Option<&str>,
    outcome: Option<(&str, Option<&str>)>,
) -> ToolResultFacts {
    let payload = text.map_or(Value::Null, |text| Value::String(text.to_string()));
    let mut facts = ToolResultFacts::from_payload(&payload);
    facts.event_source = Some(EVENT_SOURCE_TOOL_RESULT.to_string());
    facts.tool_use_id = (!call_id.is_empty()).then(|| call_id.to_string());
    let status = match outcome.map(|(kind, _)| kind) {
        Some("completed") => STATUS_COMPLETED,
        Some("failed") => {
            facts.error_signal = Some(ERROR_SIGNAL_MUSE_TOOL_OUTCOME.to_string());
            STATUS_ERRORED
        }
        Some("cancelled" | "canceled" | "skipped") => {
            facts.error_signal = Some(ERROR_SIGNAL_MUSE_TOOL_OUTCOME.to_string());
            STATUS_CANCELLED
        }
        _ => STATUS_UNKNOWN,
    };
    facts.result_status = Some(status.to_string());
    if status == STATUS_COMPLETED
        && tool_name == Some("bash")
        && text
            .and_then(crate::ingest::muse::bash_exit_code)
            .is_some_and(|code| code != 0)
    {
        facts.result_status = Some(STATUS_ERRORED.to_string());
        facts.error_signal = Some(ERROR_SIGNAL_EXIT_CODE.to_string());
    }
    facts
}

fn first_str(line: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        line.get(*key)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Only a `bash` result's `exit_code` is a failure signal; any other
    /// tool's output is data that may happen to contain the same key.
    #[test]
    fn muse_exit_codes_fail_only_bash_results() {
        let text = Some(r#"{"exit_code":101}"#);
        let completed = Some(("completed", None));
        let bash = muse_tool_result_facts(text, "c1", Some("bash"), completed);
        assert_eq!(bash.result_status.as_deref(), Some(STATUS_ERRORED));
        assert_eq!(bash.error_signal.as_deref(), Some(ERROR_SIGNAL_EXIT_CODE));
        let read = muse_tool_result_facts(text, "c2", Some("read_file"), completed);
        assert_eq!(read.result_status.as_deref(), Some(STATUS_COMPLETED));
        assert_eq!(read.error_signal, None);
    }

    #[test]
    fn stable_stringify_sorts_object_keys_and_keeps_array_order() {
        let value = json!({ "b": 1, "a": 2, "c": [3, { "y": 1, "x": 2 }] });
        assert_eq!(
            stable_stringify(&value),
            r#"{"a":2,"b":1,"c":[3,{"x":2,"y":1}]}"#
        );
        assert_eq!(stable_stringify(&json!([3, 1, 2])), "[3,1,2]");
        assert_eq!(stable_stringify(&json!(null)), "null");
    }

    #[test]
    fn content_hash_matches_the_known_empty_digest() {
        // Same 16-hex truncation relayburn uses, so a hash computed here and
        // one computed there compare equal instead of merely looking alike.
        assert_eq!(content_hash(b""), "e3b0c44298fc1c14");
    }

    #[test]
    fn string_payload_is_measured_over_raw_utf8_bytes() {
        let facts = ToolResultFacts::from_payload(&json!("héllo"));
        assert_eq!(facts.payload_bytes, Some(6));
        assert_eq!(facts.payload_hash, Some(content_hash("héllo".as_bytes())));
        assert_eq!(facts.payload_truncated, Some(false));
    }

    #[test]
    fn structured_payload_hash_is_stable_under_key_reordering() {
        let a = ToolResultFacts::from_payload(&json!({ "a": 1, "b": 2 }));
        let b = ToolResultFacts::from_payload(&json!({ "b": 2, "a": 1 }));
        assert_eq!(a.payload_hash, b.payload_hash);
        assert_eq!(a.payload_bytes, b.payload_bytes);
    }

    #[test]
    fn null_payload_records_nothing_rather_than_zero() {
        let facts = ToolResultFacts::from_payload(&Value::Null);
        assert_eq!(facts.payload_bytes, None);
        assert_eq!(facts.payload_hash, None);
        assert_eq!(facts.payload_truncated, None);
    }

    #[test]
    fn every_documented_truncation_marker_is_detected() {
        for marker in TRUNCATION_MARKERS {
            assert!(
                detect_truncation_marker(&format!("output\n{}\n", marker.to_uppercase())),
                "marker {marker} should be detected case-insensitively"
            );
        }
        assert!(!detect_truncation_marker("a complete result"));
    }

    #[test]
    fn indexer_counts_calls_per_tool_use_id_and_events_globally() {
        let mut indexer = ToolResultIndexer::default();
        assert_eq!(indexer.next("a"), (Some(0), 0));
        assert_eq!(indexer.next("b"), (Some(0), 1));
        assert_eq!(indexer.next("a"), (Some(1), 2));
        assert_eq!(indexer.next(""), (None, 3));
        assert_eq!(indexer.last_event_index(), Some(3));
        assert_eq!(ToolResultIndexer::default().last_event_index(), None);
        assert_eq!(ToolResultIndexer::resume_from(7).next("a"), (Some(0), 8));
    }

    #[test]
    fn claude_error_block_names_the_signal_that_set_it() {
        let facts = claude_tool_result_facts(&json!({
            "type": "tool_result",
            "tool_use_id": "toolu_1",
            "is_error": true,
            "content": "boom",
        }));
        assert_eq!(facts.result_status.as_deref(), Some(STATUS_ERRORED));
        assert_eq!(
            facts.error_signal.as_deref(),
            Some(ERROR_SIGNAL_TOOL_RESULT)
        );
        assert_eq!(
            facts.event_source.as_deref(),
            Some(EVENT_SOURCE_TOOL_RESULT)
        );
        assert_eq!(facts.tool_use_id.as_deref(), Some("toolu_1"));
    }

    #[test]
    fn subagent_notification_needs_a_child_identity() {
        let without = json!({ "type": "system", "subtype": "hook_ran" });
        assert!(claude_subagent_notification_facts(without.as_object().unwrap()).is_none());
        let with = json!({
            "type": "system",
            "subtype": "subagent_completed",
            "parent_tool_use_id": "toolu_system",
            "agent_id": "agent-1",
            "subagent_session_id": "child-1",
            "content": "subagent completed",
        });
        let facts = claude_subagent_notification_facts(with.as_object().unwrap()).unwrap();
        assert_eq!(
            facts.event_source.as_deref(),
            Some(EVENT_SOURCE_SUBAGENT_NOTIFICATION)
        );
        assert_eq!(facts.subagent_session_id.as_deref(), Some("child-1"));
        assert_eq!(facts.agent_id.as_deref(), Some("agent-1"));
        assert_eq!(facts.tool_use_id.as_deref(), Some("toolu_system"));
        assert_eq!(facts.result_status.as_deref(), Some(STATUS_COMPLETED));
        assert_eq!(facts.error_signal, None);
    }

    #[test]
    fn failed_subagent_notification_names_the_status_signal() {
        let line = json!({
            "type": "system",
            "subtype": "subagent_completed",
            "parent_tool_use_id": "toolu_system",
            "agent_id": "agent-1",
            "status": "failed",
        });
        let facts = claude_subagent_notification_facts(line.as_object().unwrap()).unwrap();
        assert_eq!(facts.result_status.as_deref(), Some(STATUS_ERRORED));
        assert_eq!(
            facts.error_signal.as_deref(),
            Some(ERROR_SIGNAL_SUBAGENT_STATUS)
        );
    }

    #[test]
    fn codex_output_defers_its_status_to_the_turns_end() {
        let facts = codex_output_facts(&json!("ok"), "call_1");
        assert_eq!(facts.result_status.as_deref(), Some(STATUS_UNKNOWN));
        assert_eq!(
            facts.event_source.as_deref(),
            Some(EVENT_SOURCE_FUNCTION_CALL_OUTPUT)
        );
        assert_eq!(facts.tool_use_id.as_deref(), Some("call_1"));
        assert_eq!(facts.payload_bytes, Some(2));
    }
}
