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

/// The millisecond timestamp a UUIDv7 carries in its first 48 bits, or `None`
/// for anything that is not a well-formed version-7 UUID.
///
/// Codex mints thread and turn ids as UUIDv7, so the id itself says when the
/// thread or turn began. A legacy UUIDv4 id carries no time and reads `None`.
pub(crate) fn uuid_v7_ms(id: &str) -> Option<i64> {
    let hex: String = id.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    if hex.as_bytes()[12] != b'7' {
        return None;
    }
    i64::from_str_radix(&hex[..12], 16).ok()
}

/// Why a turn in a forked rollout was judged to be the child's own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForkedTurnOwner {
    /// A turn the parent ran, copied into the child's file when it forked.
    Replay,
    /// The child's own turn, and which explicit fact said so.
    Child(&'static str),
    /// Nothing explicit orders this turn against the fork.
    Undecided,
}

/// Decide whether a turn inside a forked rollout's replayed prefix belongs to
/// the parent (a replay) or to the child, from explicit ordering facts only.
///
/// `fork_origin_ms` is when the child thread began: its UUIDv7 id's
/// timestamp, else its `session_meta` timestamp. A parent's turn necessarily
/// started before the fork that copied it, so:
///
/// - a UUIDv7 `turn_id` that is **earlier** than the fork origin is a replay,
///   and one at or after it is the child's. Equal milliseconds count as the
///   child's: a parent turn cannot begin in the same millisecond as a fork its
///   own model output had to request, and erring the other way would drop the
///   child's evidence rather than keep a duplicate.
/// - a legacy (non-v7) `turn_id` falls back to `task_started.started_at`
///   (unix seconds): earlier than the fork's second is a replay, the same
///   second or later is the child's.
/// - anything else is [`ForkedTurnOwner::Undecided`], and the caller stops
///   gating rather than guess.
///
/// This deliberately does not use `task_started` *presence* to tell the two
/// apart: rollouts written by Codex 0.155 replay the parent's `task_started`
/// records verbatim, so their presence proves nothing.
pub(crate) fn forked_turn_owner(
    turn_id: Option<&str>,
    started_at: Option<i64>,
    fork_origin_ms: i64,
) -> ForkedTurnOwner {
    if let Some(turn_ms) = turn_id.and_then(uuid_v7_ms) {
        return if turn_ms < fork_origin_ms {
            ForkedTurnOwner::Replay
        } else {
            ForkedTurnOwner::Child("turn_id")
        };
    }
    if let Some(started_at) = started_at {
        return if started_at < fork_origin_ms.div_euclid(1000) {
            ForkedTurnOwner::Replay
        } else {
            ForkedTurnOwner::Child("task_started.started_at")
        };
    }
    ForkedTurnOwner::Undecided
}

/// The thread a rollout was forked from, when Codex names it outright:
/// `forked_from_id` for a human fork, else
/// `source.subagent.thread_spawn.parent_thread_id` for a spawned subagent.
/// These are the same two fields the continuity scanner records `fork` edges
/// from, and the only ones that arm the replay gate.
pub(crate) fn fork_parent_id(payload: Option<&Value>, session_id: &str) -> Option<String> {
    let named = |value: Option<&Value>| {
        value
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty() && *id != session_id)
            .map(str::to_string)
    };
    let payload = payload?;
    named(payload.get("forked_from_id")).or_else(|| {
        named(
            payload
                .get("source")
                .and_then(|source| source.get("subagent"))
                .and_then(|subagent| subagent.get("thread_spawn"))
                .and_then(|spawn| spawn.get("parent_thread_id")),
        )
    })
}

/// When a thread began, for ordering its turns against its fork: the UUIDv7
/// thread id's timestamp, else `payload.timestamp`, else the `session_meta`
/// record's own timestamp.
pub(crate) fn fork_origin_ms(
    payload: Option<&Value>,
    session_id: &str,
    record_ts_ms: Option<i64>,
) -> Option<i64> {
    uuid_v7_ms(session_id)
        .or_else(|| {
            payload
                .and_then(|p| p.get("timestamp"))
                .and_then(Value::as_str)
                .and_then(crate::parse_iso_ms)
        })
        .or(record_ts_ms)
}

/// What [`ForkReplayGate::step`] decided about one record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReplayStep {
    /// Not inside a replay: index the record as usual.
    Outside,
    /// Part of the parent history the fork copied in. The first record of a
    /// span (the replayed parent `session_meta`) is `Replay` too.
    Replay,
    /// The child's own first turn: the span ended just before this record,
    /// which is indexed as usual. Carries the turn and the rule that
    /// attributed it.
    Closed {
        turn_id: Option<String>,
        basis: &'static str,
    },
}

/// The one statement of the forked-rollout replay rule, shared by the rollout
/// walk and shallow discovery so the two cannot drift.
///
/// When Codex forks a thread it writes the child's `session_meta` and then
/// copies the parent's history into the child's file, starting with the
/// parent's own `session_meta`. A span **opens** at a `session_meta` after the
/// file's first record whose `payload.id` is the parent the opening record
/// named ([`fork_parent_id`]), and **closes** at the first `task_started` or
/// `turn_context` that [`forked_turn_owner`] does not call a replay. Nothing
/// else opens or closes it: a rollout without an explicit fork field, or a
/// fork that opens on something other than its parent's `session_meta`, is
/// never gated.
#[derive(Debug, Clone, Default)]
pub(crate) struct ForkReplayGate {
    parent: Option<String>,
    origin_ms: Option<i64>,
    replaying: bool,
}

impl ForkReplayGate {
    pub(crate) fn new(parent: Option<String>, origin_ms: Option<i64>) -> Self {
        Self {
            // Without a fork origin no turn can be ordered, so the gate is
            // never armed rather than armed and immediately undecided.
            parent: parent.filter(|_| origin_ms.is_some()),
            origin_ms,
            replaying: false,
        }
    }

    /// Classify one record. `first_record` is true only for the file's
    /// opening record, which is the child's own identity and never a replay.
    pub(crate) fn step(&mut self, first_record: bool, value: &Value) -> ReplayStep {
        let Some(parent) = self.parent.as_deref() else {
            return ReplayStep::Outside;
        };
        let line_type = value.get("type").and_then(Value::as_str);
        let payload = value.get("payload");
        if !self.replaying {
            let opens = !first_record
                && line_type == Some("session_meta")
                && payload.and_then(|p| p.get("id")).and_then(Value::as_str) == Some(parent);
            if !opens {
                return ReplayStep::Outside;
            }
            self.replaying = true;
            return ReplayStep::Replay;
        }
        let payload_type = payload.and_then(|p| p.get("type")).and_then(Value::as_str);
        let is_turn = line_type == Some("turn_context")
            || (line_type == Some("event_msg") && payload_type == Some("task_started"));
        if !is_turn {
            return ReplayStep::Replay;
        }
        let turn_id = payload
            .and_then(|p| p.get("turn_id"))
            .and_then(Value::as_str);
        let started_at = payload
            .and_then(|p| p.get("started_at"))
            .and_then(Value::as_i64);
        let basis = match forked_turn_owner(turn_id, started_at, self.origin_ms.unwrap_or_default())
        {
            ForkedTurnOwner::Replay => return ReplayStep::Replay,
            ForkedTurnOwner::Child(basis) => basis,
            ForkedTurnOwner::Undecided => "undecided",
        };
        self.replaying = false;
        ReplayStep::Closed {
            turn_id: turn_id.map(str::to_string),
            basis,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn uuid_v7_ms_reads_only_version_seven_ids() {
        // 0x01a0c210ec76 ms.
        assert_eq!(
            uuid_v7_ms("01a0c210-ec76-71c2-8585-47285eab37cc"),
            Some(0x01a0_c210_ec76)
        );
        assert_eq!(uuid_v7_ms("01a0c210-ec76-41c2-8585-47285eab37cc"), None);
        assert_eq!(uuid_v7_ms("sess_child"), None);
        assert_eq!(uuid_v7_ms(""), None);
    }

    #[test]
    fn forked_turn_owner_orders_turns_against_the_fork() {
        let origin = uuid_v7_ms("01a0c210-ec76-71c2-8585-47285eab37cc").unwrap();
        // The parent's turns, copied into the child's file.
        assert_eq!(
            forked_turn_owner(Some("01a0c20f-64c5-7e82-bd2b-e08bab70470c"), None, origin),
            ForkedTurnOwner::Replay
        );
        // The child's first turn, a few milliseconds after the fork.
        assert_eq!(
            forked_turn_owner(Some("01a0c210-ecae-71e2-9c2e-a0801741c6a4"), None, origin),
            ForkedTurnOwner::Child("turn_id")
        );
        // The same millisecond is the child's.
        assert_eq!(
            forked_turn_owner(Some("01a0c210-ec76-7000-8000-000000000000"), None, origin),
            ForkedTurnOwner::Child("turn_id")
        );
        // A legacy id falls back to `started_at`, in seconds.
        let second = origin / 1000;
        assert_eq!(
            forked_turn_owner(Some("turn-legacy"), Some(second - 1), origin),
            ForkedTurnOwner::Replay
        );
        assert_eq!(
            forked_turn_owner(Some("turn-legacy"), Some(second), origin),
            ForkedTurnOwner::Child("task_started.started_at")
        );
        assert_eq!(
            forked_turn_owner(Some("turn-legacy"), None, origin),
            ForkedTurnOwner::Undecided
        );
        assert_eq!(
            forked_turn_owner(None, None, origin),
            ForkedTurnOwner::Undecided
        );
    }

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
