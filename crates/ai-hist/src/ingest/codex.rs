//! Shared parsing for human-authored Codex rollout messages.
//!
//! Codex has emitted the same user turn through two record shapes over time.
//! Keeping their interpretation here prevents shallow discovery and full
//! ingestion from drifting when the rollout schema changes again.

use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HumanMessageFormat {
    EventMessage,
    ResponseItem,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HumanMessage {
    pub text: String,
    pub format: HumanMessageFormat,
    pub message_id: Option<String>,
}

/// Extract one substantive human turn from either supported Codex shape.
///
/// Multiple `input_text` parts are kept in provider order and separated by a
/// newline, matching the way other multipart Codex text is materialized.
/// Application-injected control wrappers are rejected here so every caller
/// applies the same human-message classification.
pub(crate) fn human_message(value: &Value) -> Option<HumanMessage> {
    let payload = value.get("payload")?.as_object()?;
    let (text, format) = match (
        value.get("type").and_then(Value::as_str),
        payload.get("type").and_then(Value::as_str),
    ) {
        (Some("event_msg"), Some("user_message")) => (
            payload.get("message")?.as_str()?.to_string(),
            HumanMessageFormat::EventMessage,
        ),
        (Some("response_item"), Some("message"))
            if payload.get("role").and_then(Value::as_str) == Some("user") =>
        {
            let parts = payload
                .get("content")?
                .as_array()?
                .iter()
                .filter(|part| part.get("type").and_then(Value::as_str) == Some("input_text"))
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>();
            if parts.is_empty() {
                return None;
            }
            (parts.join("\n"), HumanMessageFormat::ResponseItem)
        }
        _ => return None,
    };
    let text = text.trim();
    if text.is_empty() || is_control_context(text) {
        return None;
    }
    Some(HumanMessage {
        text: text.to_string(),
        format,
        message_id: payload
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_string),
    })
}

pub(crate) fn is_control_context(prompt: &str) -> bool {
    let value = prompt.trim_start();
    [
        "<environment_context",
        "<permissions instructions",
        "<app-context",
        "<skills_instructions",
        "<collaboration_mode",
        "<INSTRUCTIONS>",
        "<user_instructions",
        "# AGENTS.md",
    ]
    .iter()
    .any(|prefix| value.starts_with(prefix))
}

/// Suppress only adjacent mirrored encodings of one turn.
///
/// Text is deliberately not deduplicated globally: two equal messages in the
/// same representation, or equal messages separated by any other record, are
/// distinct human turns. Codex mirror pairs are adjacent, have equal text,
/// and use opposite representations.
#[derive(Default)]
pub(crate) struct HumanMessageDeduper {
    previous: Option<(HumanMessageFormat, String)>,
}

impl HumanMessageDeduper {
    pub(crate) fn observe(&mut self, value: &Value) -> Option<HumanMessage> {
        let current = human_message(value);
        let Some(current) = current else {
            self.previous = None;
            return None;
        };
        let mirrored = self
            .previous
            .as_ref()
            .is_some_and(|(format, text)| *format != current.format && text == &current.text);
        if mirrored {
            self.previous = None;
            return None;
        }
        self.previous = Some((current.format, current.text.clone()));
        Some(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_both_user_shapes_and_joins_text_parts() {
        let old = json!({
            "type": "event_msg",
            "payload": {"type": "user_message", "message": "  fix it  "}
        });
        assert_eq!(human_message(&old).unwrap().text, "fix it");

        let current = json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "id": "msg_1",
                "content": [
                    {"type": "input_text", "text": "first"},
                    {"type": "image", "url": "ignored"},
                    {"type": "input_text", "text": "second"}
                ]
            }
        });
        assert_eq!(human_message(&current).unwrap().text, "first\nsecond");
        assert_eq!(
            human_message(&current).unwrap().message_id.as_deref(),
            Some("msg_1")
        );
    }

    #[test]
    fn rejects_assistant_and_control_messages() {
        let assistant = json!({
            "type": "response_item",
            "payload": {"type": "message", "role": "assistant", "content": [
                {"type": "output_text", "text": "done"}
            ]}
        });
        assert!(human_message(&assistant).is_none());

        let context = json!({
            "type": "response_item",
            "payload": {"type": "message", "role": "user", "content": [
                {"type": "input_text", "text": "<environment_context>injected</environment_context>"}
            ]}
        });
        assert!(human_message(&context).is_none());
    }

    #[test]
    fn deduplicates_only_adjacent_opposite_representations() {
        let response = json!({
            "type": "response_item",
            "payload": {"type": "message", "role": "user", "content": [
                {"type": "input_text", "text": "retry"}
            ]}
        });
        let event = json!({
            "type": "event_msg",
            "payload": {"type": "user_message", "message": "retry"}
        });
        let assistant = json!({
            "type": "event_msg",
            "payload": {"type": "agent_message", "message": "working"}
        });
        let mut deduper = HumanMessageDeduper::default();
        assert!(deduper.observe(&response).is_some());
        assert!(deduper.observe(&event).is_none());
        assert!(deduper.observe(&event).is_some());
        assert!(deduper.observe(&assistant).is_none());
        assert!(deduper.observe(&event).is_some());
    }
}
