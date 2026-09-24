//! Muse Code (Meta's `muse` CLI) session record parsing.
//!
//! One place for the interpretation of
//! `$XDG_DATA_HOME/muse/sessions/YYYY/MM/DD/<session-id>/session.jsonl`
//! (`~/.local/share/muse/sessions/…` when `XDG_DATA_HOME` is unset), so
//! shallow discovery, full sync and targeted hydration cannot drift apart.
//!
//! ## What Muse actually writes
//!
//! `session.jsonl` is an append-only, event-sourced log. Every line is one
//! envelope:
//!
//! ```json
//! {"schema_version": 1, "id": "<record uuid>",
//!  "stream": {"kind": "session", "id": "<session id>"}, "sequence": 1,
//!  "recorded_at": 1788223110587169, "payload_type": "runtime.session", …,
//!  "payload": { … }}
//! ```
//!
//! * `recorded_at` is **microseconds** since the epoch, on every record, so
//!   no timestamp here is ever inferred.
//! * `runtime.session.metadata` (normally the first record, though a
//!   permission frame can precede it) names the session: `stream.id` is the
//!   session id and `payload.record` carries `workspace_root`, `model_id`,
//!   `provider_id` and `build.semver`.
//! * `runtime.session` with `payload.kind == "run"` is the conversation, keyed
//!   by `payload.event.kind`: `started` (`prompt`, the typed message),
//!   `reasoning_committed`, `assistant_message_committed`,
//!   `assistant_tool_calls_committed` (`tool_calls[].args` is a JSON
//!   **string**), `tool_result_batch_committed` (results keyed by
//!   `tool_call_id`), `model_completed` (per model step: `model`, `usage`,
//!   `duration_ms`, `finish_reason`) and `terminal` (run end).
//!   `payload.kind == "task"` is execution bookkeeping and is not read.
//! * `tool_batch.effect.terminal` carries the authoritative outcome of each
//!   call (`outcome.kind` = `completed` / `failed` / …) keyed by `call_id`.
//!   The result text itself carries no error flag.
//! * `session.opened.observed`, `session.resumed` and `session.end` are
//!   process lifecycle; they become markers.
//! * Subagent and reminder runs write their own
//!   `subagent/<child-id>/session.jsonl` beside the parent, and their task
//!   streams are also mirrored into the parent file under a **different**
//!   `stream.id`. Only records on the session's own stream are read, and only
//!   top-level transcripts are sessions.
//!
//! The shapes were characterized from a transcript captured by the real CLI
//! (Muse 0.2.1, checked in under `tests/fixtures/muse/`) and cross-checked
//! against the published Muse Code SDK and independent readers.

use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

/// The `payload_type` of the record that names the session.
pub(crate) const METADATA_PAYLOAD_TYPE: &str = "runtime.session.metadata";

/// The directory name a subagent (or reminder child) transcript lives under.
pub(crate) const SUBAGENT_DIR: &str = "subagent";

/// The file name every Muse transcript has.
pub(crate) const SESSION_FILE: &str = "session.jsonl";

/// What the metadata record says about a session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MuseMetadata {
    pub session_id: String,
    pub workspace_root: Option<String>,
    pub model_id: Option<String>,
    pub provider_id: Option<String>,
    pub cli_version: Option<String>,
    pub recorded_ms: Option<i64>,
}

/// One tool call as `assistant_tool_calls_committed` records it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MuseToolCall {
    /// The provider call id results and outcomes join on.
    pub call_id: String,
    pub name: String,
    /// `args` parsed from its JSON string; the raw string when it does not
    /// parse, so nothing Muse wrote is dropped.
    pub arguments: Value,
}

/// One tool result as `tool_result_batch_committed` records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MuseToolResult {
    pub call_id: String,
    pub text: Option<String>,
}

/// The conversation-level meaning of one record on the session's own stream.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MuseEvent {
    /// A typed prompt that opened a run.
    Prompt {
        text: String,
    },
    /// Thinking. `text` is `None` when Muse wrote only the encrypted trace.
    Reasoning {
        message_id: Option<String>,
        text: Option<String>,
        encrypted: bool,
    },
    AssistantText {
        message_id: Option<String>,
        response_id: Option<String>,
        text: String,
    },
    ToolCalls {
        message_id: Option<String>,
        response_id: Option<String>,
        calls: Vec<MuseToolCall>,
    },
    ToolResults {
        results: Vec<MuseToolResult>,
    },
    /// One model step finished. `usage` is Muse's own object, verbatim.
    ModelCompleted {
        model: Option<String>,
        usage: Option<Value>,
        finish_reason: Option<String>,
        duration_ms: Option<i64>,
    },
    /// The run ended.
    RunTerminal {
        status: Option<String>,
        reason: Option<String>,
    },
    /// The authoritative outcome of one tool call.
    ToolOutcome {
        call_id: String,
        status: String,
        reason: Option<String>,
    },
    SessionOpened {
        resume: bool,
    },
    SessionResumed,
    SessionEnd {
        exit_reason: Option<String>,
    },
    /// A metadata record after the first: a model or provider switch.
    Metadata(MuseMetadata),
}

/// One interpreted record, with the envelope facts every event carries.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MuseRecord {
    /// The envelope `id`: Muse's own, stable record identity.
    pub record_id: Option<String>,
    pub sequence: Option<i64>,
    pub ts_ms: i64,
    pub run_id: Option<String>,
    pub event: MuseEvent,
}

/// A whole transcript, read.
#[derive(Debug, Clone, Default)]
pub(crate) struct MuseTranscript {
    pub metadata: MuseMetadata,
    pub records: Vec<MuseRecord>,
    /// Lines that were not JSON objects. A trailing partial line of a live
    /// session lands here too.
    pub unparsed_lines: usize,
    /// Records on another stream (mirrored subagent / reminder tasks).
    pub foreign_stream_records: usize,
}

impl MuseTranscript {
    pub fn first_ts(&self) -> Option<i64> {
        self.metadata
            .recorded_ms
            .into_iter()
            .chain(self.records.iter().map(|record| record.ts_ms))
            .min()
    }

    pub fn last_ts(&self) -> Option<i64> {
        self.metadata
            .recorded_ms
            .into_iter()
            .chain(self.records.iter().map(|record| record.ts_ms))
            .max()
    }

    /// Every model the session names, metadata first, in first-seen order.
    pub fn models(&self) -> Vec<String> {
        let mut models: Vec<String> = Vec::new();
        let mut push = |model: Option<&str>| {
            if let Some(model) = model.filter(|model| !model.is_empty()) {
                if !models.iter().any(|seen| seen == model) {
                    models.push(model.to_string());
                }
            }
        };
        push(self.metadata.model_id.as_deref());
        for record in &self.records {
            match &record.event {
                MuseEvent::ModelCompleted { model, .. } => push(model.as_deref()),
                MuseEvent::Metadata(metadata) => push(metadata.model_id.as_deref()),
                _ => {}
            }
        }
        models
    }
}

/// `recorded_at` microseconds as epoch milliseconds.
pub(crate) fn recorded_ms(value: &Value) -> Option<i64> {
    value
        .get("recorded_at")
        .and_then(Value::as_i64)
        .filter(|micros| *micros > 0)
        .map(|micros| micros / 1000)
}

fn text_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(str::to_string)
}

fn stream_id(value: &Value) -> Option<&str> {
    value.pointer("/stream/id").and_then(Value::as_str)
}

/// The session a metadata record names, or `None` when the record is not one
/// (or names no session). Identity comes only from `stream.id` on a
/// `session`-kind stream, never from the directory name.
pub(crate) fn parse_metadata(value: &Value) -> Option<MuseMetadata> {
    if value.get("payload_type").and_then(Value::as_str) != Some(METADATA_PAYLOAD_TYPE) {
        return None;
    }
    if value.pointer("/stream/kind").and_then(Value::as_str) != Some("session") {
        return None;
    }
    let session_id = stream_id(value).filter(|id| !id.is_empty())?;
    let record = value.pointer("/payload/record").unwrap_or(&Value::Null);
    Some(MuseMetadata {
        session_id: session_id.to_string(),
        workspace_root: text_field(record, "workspace_root"),
        model_id: text_field(record, "model_id"),
        provider_id: text_field(record, "provider_id"),
        cli_version: record
            .pointer("/build/semver")
            .and_then(Value::as_str)
            .filter(|version| !version.is_empty())
            .map(str::to_string),
        recorded_ms: recorded_ms(value),
    })
}

/// Interpret one record that is already known to be on the session's stream.
/// `None` for the bookkeeping and diagnostics this reader does not keep.
pub(crate) fn parse_event(value: &Value) -> Option<MuseEvent> {
    let payload = value.get("payload")?;
    match value.get("payload_type").and_then(Value::as_str)? {
        METADATA_PAYLOAD_TYPE => parse_metadata(value).map(MuseEvent::Metadata),
        "runtime.session" => {
            if payload.get("kind").and_then(Value::as_str) != Some("run") {
                return None;
            }
            parse_run_event(payload.get("event")?)
        }
        "tool_batch.effect.terminal" => {
            let record = payload.get("record")?;
            let call_id = text_field(record, "call_id")?;
            let outcome = record.get("outcome")?;
            Some(MuseEvent::ToolOutcome {
                call_id,
                status: text_field(outcome, "kind")?,
                reason: text_field(outcome, "reason"),
            })
        }
        "session.opened.observed" => Some(MuseEvent::SessionOpened {
            resume: payload
                .pointer("/record/resume")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }),
        "session.resumed" => Some(MuseEvent::SessionResumed),
        "session.end" => Some(MuseEvent::SessionEnd {
            exit_reason: payload
                .get("record")
                .and_then(|record| text_field(record, "exit_reason")),
        }),
        _ => None,
    }
}

fn parse_run_event(event: &Value) -> Option<MuseEvent> {
    match event.get("kind").and_then(Value::as_str)? {
        "started" => text_field(event, "prompt").map(|text| MuseEvent::Prompt { text }),
        "reasoning_committed" => Some(MuseEvent::Reasoning {
            message_id: text_field(event, "message_id"),
            text: text_field(event, "text"),
            encrypted: event
                .get("encrypted_content")
                .is_some_and(|content| !content.is_null() && content != ""),
        }),
        "assistant_message_committed" => Some(MuseEvent::AssistantText {
            message_id: text_field(event, "message_id"),
            response_id: text_field(event, "response_id"),
            text: text_field(event, "text")?,
        }),
        "assistant_tool_calls_committed" => {
            let calls: Vec<MuseToolCall> = event
                .get("tool_calls")
                .and_then(Value::as_array)
                .map(|calls| calls.iter().filter_map(parse_tool_call).collect())
                .unwrap_or_default();
            (!calls.is_empty()).then(|| MuseEvent::ToolCalls {
                message_id: text_field(event, "message_id"),
                response_id: text_field(event, "response_id"),
                calls,
            })
        }
        "tool_result_batch_committed" => {
            let results: Vec<MuseToolResult> = event
                .get("results")
                .and_then(Value::as_array)
                .map(|results| {
                    results
                        .iter()
                        .filter_map(|result| {
                            Some(MuseToolResult {
                                call_id: text_field(result, "tool_call_id")?,
                                text: result
                                    .get("text")
                                    .and_then(Value::as_str)
                                    .map(str::to_string),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            (!results.is_empty()).then_some(MuseEvent::ToolResults { results })
        }
        "model_completed" => Some(MuseEvent::ModelCompleted {
            model: text_field(event, "model"),
            usage: event
                .get("usage")
                .filter(|usage| usage.is_object())
                .cloned(),
            finish_reason: text_field(event, "finish_reason"),
            duration_ms: event.get("duration_ms").and_then(Value::as_i64),
        }),
        "terminal" => Some(MuseEvent::RunTerminal {
            status: text_field(event, "terminal"),
            reason: text_field(event, "reason"),
        }),
        _ => None,
    }
}

fn parse_tool_call(call: &Value) -> Option<MuseToolCall> {
    // `call_id` is what results and outcomes name; `id` is the provider's
    // response item id and only the fallback.
    let call_id = text_field(call, "call_id").or_else(|| text_field(call, "id"))?;
    let name = text_field(call, "name")?;
    let arguments = match call.get("args") {
        Some(Value::String(raw)) => {
            serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.clone()))
        }
        Some(other) => other.clone(),
        None => Value::Null,
    };
    Some(MuseToolCall {
        call_id,
        name,
        arguments,
    })
}

/// Read a whole transcript. `None` when it names no session — no metadata
/// record on a `session` stream — which is "not a session", not an error.
pub(crate) fn parse_transcript(contents: &str) -> Option<MuseTranscript> {
    let mut values = Vec::new();
    let mut unparsed_lines = 0;
    for line in contents.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(line) {
            Ok(value) if value.is_object() => values.push(value),
            _ => unparsed_lines += 1,
        }
    }
    let metadata = values.iter().find_map(parse_metadata)?;
    let mut transcript = MuseTranscript {
        metadata,
        unparsed_lines,
        ..Default::default()
    };
    let mut seen_metadata = false;
    for value in &values {
        // Mirrored subagent / reminder task records share the file but not
        // the stream. Reading them would present every child objective as a
        // prompt the person typed.
        if stream_id(value) != Some(transcript.metadata.session_id.as_str()) {
            transcript.foreign_stream_records += 1;
            continue;
        }
        let Some(event) = parse_event(value) else {
            continue;
        };
        if matches!(event, MuseEvent::Metadata(_)) && !seen_metadata {
            // The first metadata record is the session's header, not a switch.
            seen_metadata = true;
            continue;
        }
        let Some(ts_ms) = recorded_ms(value) else {
            continue;
        };
        transcript.records.push(MuseRecord {
            record_id: text_field(value, "id"),
            sequence: value.get("sequence").and_then(Value::as_i64),
            ts_ms,
            run_id: value
                .pointer("/payload/run_id")
                .and_then(Value::as_str)
                .map(str::to_string),
            event,
        });
    }
    Some(transcript)
}

/// Every call's authoritative outcome, by call id.
pub(crate) fn tool_outcomes(transcript: &MuseTranscript) -> HashMap<&str, (&str, Option<&str>)> {
    transcript
        .records
        .iter()
        .filter_map(|record| match &record.event {
            MuseEvent::ToolOutcome {
                call_id,
                status,
                reason,
            } => Some((call_id.as_str(), (status.as_str(), reason.as_deref()))),
            _ => None,
        })
        .collect()
}

/// Whether a transcript path is a subagent or reminder child rather than a
/// session of its own: any `subagent` directory between it and the root.
pub(crate) fn is_child_transcript(path: &Path, root: &Path) -> bool {
    path.strip_prefix(root)
        .unwrap_or(path)
        .components()
        .any(|component| component.as_os_str() == SUBAGENT_DIR)
}

/// Tools that write a file.
pub(crate) fn is_file_edit_tool(name: &str) -> bool {
    matches!(name, "write_file" | "edit_file")
}

/// Tools that start another agent.
pub(crate) fn is_subagent_tool(name: &str) -> bool {
    name == "subagent_spawn"
}

/// What a tool call acts on: the file, the command, the pattern.
pub(crate) fn pick_tool_target(name: &str, arguments: &Value) -> Option<String> {
    let object = arguments.as_object()?;
    let get = |key: &str| {
        object
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    match name {
        "bash" => get("command"),
        "bash_input" => get("chars"),
        "search" => get("pattern"),
        "web_search" => get("query"),
        "subagent_spawn" => get("role")
            .or_else(|| get("objective"))
            .or_else(|| get("prompt")),
        _ => get("path")
            .or_else(|| get("file_path"))
            .or_else(|| get("url"))
            .or_else(|| get("query")),
    }
}

/// A `bash` result is a JSON object string; its `exit_code` is the one
/// in-band failure signal Muse writes.
pub(crate) fn bash_exit_code(text: &str) -> Option<i64> {
    serde_json::from_str::<Value>(text)
        .ok()?
        .get("exit_code")
        .and_then(Value::as_i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SID: &str = "74747474-7474-4747-8747-747474747474";

    fn line(sequence: i64, payload_type: &str, payload: Value) -> String {
        json!({
            "schema_version": 1,
            "id": format!("rec-{sequence}"),
            "stream": {"kind": "session", "id": SID},
            "sequence": sequence,
            "recorded_at": 1_788_223_110_000_000i64 + sequence * 1000,
            "payload_type": payload_type,
            "payload": payload,
        })
        .to_string()
    }

    fn run(sequence: i64, event: Value) -> String {
        line(
            sequence,
            "runtime.session",
            json!({"kind": "run", "run_id": "run-1", "event": event}),
        )
    }

    fn metadata() -> String {
        line(
            1,
            METADATA_PAYLOAD_TYPE,
            json!({"kind": "metadata", "record": {
                "workspace_root": "/work", "model_id": "meta/muse-spark",
                "provider_id": "meta", "build": {"semver": "1.3.0"}}}),
        )
    }

    #[test]
    fn a_file_without_metadata_is_not_a_session() {
        assert!(parse_transcript(&run(2, json!({"kind": "started", "prompt": "hi"}))).is_none());
        assert!(parse_transcript("not json\n").is_none());
    }

    #[test]
    fn metadata_may_follow_a_permission_frame() {
        let contents = format!(
            "{}\n{}\n",
            r#"{"retained_frame":"session_permission_transaction","children":[]}"#,
            metadata()
        );
        let transcript = parse_transcript(&contents).unwrap();
        assert_eq!(transcript.metadata.session_id, SID);
        assert_eq!(transcript.metadata.workspace_root.as_deref(), Some("/work"));
        assert_eq!(transcript.metadata.cli_version.as_deref(), Some("1.3.0"));
        assert_eq!(transcript.metadata.recorded_ms, Some(1_788_223_110_001));
        assert_eq!(transcript.unparsed_lines, 0);
    }

    #[test]
    fn reads_the_conversation_and_skips_task_bookkeeping() {
        let contents = [
            metadata(),
            run(2, json!({"kind": "started", "prompt": "fix it"})),
            line(
                3,
                "runtime.session",
                json!({"kind": "task", "run_id": "run-1", "event": {"kind": "started", "task_id": "t"}}),
            ),
            run(
                4,
                json!({"kind": "model_completed", "model": "meta/muse-spark", "duration_ms": 5,
                       "usage": {"input_tokens": 10, "output_tokens": 2}}),
            ),
            run(
                5,
                json!({"kind": "assistant_tool_calls_committed", "message_id": "m1",
                       "tool_calls": [{"id": "fc_1", "call_id": "call_1", "name": "write_file",
                                       "args": "{\"path\":\"a.rs\",\"content\":\"x\"}"}]}),
            ),
            line(
                6,
                "tool_batch.effect.terminal",
                json!({"kind": "tool_batch_effect", "record": {"call_id": "call_1",
                       "outcome": {"kind": "failed", "reason": "denied"}}}),
            ),
            run(
                7,
                json!({"kind": "tool_result_batch_committed",
                       "results": [{"tool_call_id": "call_1", "text": "tool failed: denied"}]}),
            ),
            run(8, json!({"kind": "terminal", "terminal": "completed"})),
        ]
        .join("\n");
        let transcript = parse_transcript(&contents).unwrap();
        let kinds: Vec<_> = transcript
            .records
            .iter()
            .map(|record| std::mem::discriminant(&record.event))
            .collect();
        assert_eq!(kinds.len(), 6, "{:#?}", transcript.records);
        assert_eq!(
            transcript.records[0].event,
            MuseEvent::Prompt {
                text: "fix it".into()
            }
        );
        assert_eq!(transcript.records[0].ts_ms, 1_788_223_110_002);
        assert_eq!(transcript.records[0].run_id.as_deref(), Some("run-1"));
        let MuseEvent::ToolCalls { calls, .. } = &transcript.records[2].event else {
            panic!("expected tool calls");
        };
        assert_eq!(calls[0].call_id, "call_1");
        assert_eq!(calls[0].arguments["path"], "a.rs");
        assert_eq!(
            pick_tool_target(&calls[0].name, &calls[0].arguments).as_deref(),
            Some("a.rs")
        );
        assert_eq!(
            tool_outcomes(&transcript).get("call_1"),
            Some(&("failed", Some("denied")))
        );
        assert_eq!(transcript.models(), vec!["meta/muse-spark".to_string()]);
    }

    #[test]
    fn records_on_another_stream_are_not_the_sessions() {
        let foreign = json!({
            "id": "x", "stream": {"kind": "task", "id": "task-1"}, "recorded_at": 1_788_223_110_000_000i64,
            "payload_type": "runtime.session",
            "payload": {"kind": "run", "event": {"kind": "started", "prompt": "Role: worker Objective: …"}},
        })
        .to_string();
        let transcript = parse_transcript(&format!("{}\n{foreign}\n", metadata())).unwrap();
        assert!(transcript.records.is_empty());
        assert_eq!(transcript.foreign_stream_records, 1);
    }

    #[test]
    fn unparseable_arguments_are_kept_as_the_raw_string() {
        let call =
            parse_tool_call(&json!({"call_id": "c", "name": "bash", "args": "{oops"})).unwrap();
        assert_eq!(call.arguments, json!("{oops"));
    }

    #[test]
    fn child_transcripts_are_recognized_by_their_subagent_directory() {
        let root = Path::new("/data/muse/sessions");
        assert!(is_child_transcript(
            &root.join("2026/09/09/p/subagent/c/session.jsonl"),
            root
        ));
        assert!(!is_child_transcript(
            &root.join("2026/09/09/p/session.jsonl"),
            root
        ));
    }

    #[test]
    fn bash_results_expose_their_exit_code() {
        assert_eq!(bash_exit_code(r#"{"exit_code":2,"output":""}"#), Some(2));
        assert_eq!(bash_exit_code("plain text"), None);
    }
}
