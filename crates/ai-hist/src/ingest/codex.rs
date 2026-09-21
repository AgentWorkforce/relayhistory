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
    /// `Some` when the text is context the app injected rather than a turn
    /// the human typed. Such a message is stored as a `session_events` row
    /// carrying this kind, and kept out of `history` and `first_prompt`.
    pub control: Option<super::control::ControlKind>,
}

/// Extract one substantive human turn from either supported Codex shape.
///
/// Multiple `input_text` parts are kept in provider order and separated by a
/// newline, matching the way other multipart Codex text is materialized.
/// Application-injected control wrappers are rejected here so every caller
/// applies the same human-message classification; [`user_message`] is the
/// read that keeps them, for the walk that stores them as control rows.
pub(crate) fn human_message(value: &Value) -> Option<HumanMessage> {
    user_message(value).filter(|message| message.control.is_none())
}

/// Every user-role message in either shape, control wrappers included.
///
/// The one place both readers classify a user record, so the row the rollout
/// walk stores as `codex_context_wrapper` is exactly the row shallow
/// discovery refuses as a prompt.
pub(crate) fn user_message(value: &Value) -> Option<HumanMessage> {
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
    if text.is_empty() {
        return None;
    }
    Some(HumanMessage {
        text: text.to_string(),
        format,
        control: super::control::codex_text_control_kind(text),
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

/// What [`HumanMessageDeduper::observe`] did with a record.
///
/// `Suppressed` and `Rejected` both mean "no message came back", but they are
/// opposite facts about the ledger: a suppressed mirror was already stored
/// under its twin, while a rejected record was stored by nobody. Collapsing
/// them into one `None` is what let an image-only user turn -- a `message`
/// with no `input_text` part -- disappear from both tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HumanMessageOutcome {
    /// A human turn to store.
    Stored(HumanMessage),
    /// The mirrored encoding of the turn just stored; its twin wrote the row.
    Suppressed,
    /// Not a storable user turn: no text part, or not a user message at all.
    /// Nothing wrote a row on its behalf. A control wrapper is *not* rejected:
    /// it comes back `Stored` with `HumanMessage::control` set, because it is
    /// a row the ledger keeps, just not a prompt.
    Rejected,
}

impl HumanMessageDeduper {
    /// Rebuild the one-record memory a resumed pass left behind.
    ///
    /// The mirror pair that has to be suppressed can straddle the byte offset
    /// a pass committed at, so the deduper's single previous message is part
    /// of the rollout's resume state. `(true, text)` is a `response_item`.
    pub(crate) fn restore(previous: Option<(bool, String)>) -> Self {
        Self {
            previous: previous.map(|(response_item, text)| {
                let format = if response_item {
                    HumanMessageFormat::ResponseItem
                } else {
                    HumanMessageFormat::EventMessage
                };
                (format, text)
            }),
        }
    }

    /// The memory to carry across a resume, in [`Self::restore`]'s shape.
    pub(crate) fn remembered(&self) -> Option<(bool, String)> {
        self.previous
            .as_ref()
            .map(|(format, text)| (*format == HumanMessageFormat::ResponseItem, text.clone()))
    }

    pub(crate) fn observe(&mut self, value: &Value) -> HumanMessageOutcome {
        let current = user_message(value);
        let Some(current) = current else {
            self.previous = None;
            return HumanMessageOutcome::Rejected;
        };
        let mirrored = self
            .previous
            .as_ref()
            .is_some_and(|(format, text)| *format != current.format && text == &current.text);
        if mirrored {
            self.previous = None;
            return HumanMessageOutcome::Suppressed;
        }
        self.previous = Some((current.format, current.text.clone()));
        HumanMessageOutcome::Stored(current)
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
        // The same record is still a user message the walk stores, typed.
        let stored = user_message(&context).expect("a control wrapper is a stored row");
        assert_eq!(
            stored.control,
            Some(super::super::control::ControlKind::CodexContextWrapper)
        );
        assert_eq!(
            stored.text,
            "<environment_context>injected</environment_context>"
        );
    }

    /// The app writes its context wrapper in both shapes too, so the mirror
    /// pair is collapsed exactly as a human turn's is.
    #[test]
    fn a_mirrored_control_wrapper_is_stored_once() {
        let response = json!({
            "type": "response_item",
            "payload": {"type": "message", "role": "user", "content": [
                {"type": "input_text", "text": "<environment_context>injected</environment_context>"}
            ]}
        });
        let event = json!({
            "type": "event_msg",
            "payload": {"type": "user_message", "message": "<environment_context>injected</environment_context>"}
        });
        let mut deduper = HumanMessageDeduper::default();
        match deduper.observe(&response) {
            HumanMessageOutcome::Stored(message) => assert!(message.control.is_some()),
            other => panic!("expected a stored control row, got {other:?}"),
        }
        assert_eq!(deduper.observe(&event), HumanMessageOutcome::Suppressed);
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
        assert!(matches!(
            deduper.observe(&response),
            HumanMessageOutcome::Stored(_)
        ));
        // The mirrored encoding of the same turn: silent, but only because
        // its twin wrote the row.
        assert_eq!(deduper.observe(&event), HumanMessageOutcome::Suppressed);
        assert!(matches!(
            deduper.observe(&event),
            HumanMessageOutcome::Stored(_)
        ));
        // Not a human turn at all, so nothing wrote on its behalf -- a
        // different fact from the suppression above, and the caller has to be
        // able to tell them apart.
        assert_eq!(deduper.observe(&assistant), HumanMessageOutcome::Rejected);
        assert!(matches!(
            deduper.observe(&event),
            HumanMessageOutcome::Stored(_)
        ));
    }

    /// A user turn whose content carries no `input_text` part -- an
    /// image-only message -- is rejected, not suppressed. Reporting it as a
    /// suppression would claim a row had been written for it.
    #[test]
    fn an_image_only_user_message_is_rejected_rather_than_suppressed() {
        let image_only = json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_image", "image_url": "data:image/png;base64,AAAA"}]
            }
        });
        let mut deduper = HumanMessageDeduper::default();
        assert_eq!(deduper.observe(&image_only), HumanMessageOutcome::Rejected);
    }
}
