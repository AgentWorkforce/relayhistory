//! Embedded images are self-contained Markdown content blocks. Never follow a
//! transcript's file paths or remote URLs: only copy bytes the harness recorded.
use super::*;
use base64::Engine;

/// Whether the rollout behind a stamp-map record was last read by a parser
/// older than the state-marker generation.
///
/// Recorded on the rollout's own stamp record, not inferred from the markers
/// a session holds: a rollout with no `token_count` legitimately has no
/// `usage_snapshot`, a marker observed only remotely says nothing about the
/// local file, and either would make a marker probe re-read the rollout on
/// every sync for as long as the backfill stays open.
pub(super) fn rollout_owes_capture(
    record: Option<&Map<String, Value>>,
    state_markers_pending: bool,
) -> bool {
    (state_markers_pending && rollout_owes_state_markers(record))
        || !record.is_some_and(|r| r.contains_key(CODEX_IMAGE_CAPTURE_KEY))
}

fn rollout_owes_state_markers(record: Option<&Map<String, Value>>) -> bool {
    record
        .and_then(|r| r.get("state_markers"))
        .and_then(Value::as_i64)
        .unwrap_or(0)
        < CODEX_STATE_MARKER_GENERATION
}

pub(super) fn capture_rollout(
    conn: &Connection,
    meta: &CodexSessionMeta,
    value: &Value,
    position: (usize, i64, Option<&str>),
) -> Result<usize> {
    capture_codex(
        conn,
        &meta.session_id,
        Some(&meta.cwd),
        meta.git_branch.as_deref(),
        position.0,
        position.1,
        position.2,
        value,
    )
}

const MAX_IMAGE_BYTES: usize = 1_048_576;

fn image_markdown(url: &str) -> Option<String> {
    if url.len() > MAX_IMAGE_BYTES.div_ceil(3) * 4 + 64 {
        return None;
    }
    let (header, data) = url.split_once(",")?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .ok()?;
    if bytes.len() > MAX_IMAGE_BYTES {
        return None;
    }
    let valid = match header {
        "data:image/png;base64" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
        "data:image/jpeg;base64" => bytes.starts_with(&[0xff, 0xd8, 0xff]),
        "data:image/gif;base64" => bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"),
        "data:image/webp;base64" => bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP"),
        _ => false,
    };
    valid.then(|| {
        format!(
            "![Attached image]({header},{})",
            base64::engine::general_purpose::STANDARD.encode(bytes)
        )
    })
}

pub(super) fn capture_codex(
    conn: &Connection,
    session_id: &str,
    cwd: Option<&str>,
    branch: Option<&str>,
    index: usize,
    ts_ms: i64,
    turn_id: Option<&str>,
    value: &Value,
) -> Result<usize> {
    if value["type"] != "response_item"
        || value["payload"]["type"] != "message"
        || value["payload"]["role"] != "user"
    {
        return Ok(0);
    }
    let Some(parts) = value["payload"]["content"].as_array() else {
        return Ok(0);
    };
    if !parts.iter().any(|part| part["type"] == "input_image") {
        return Ok(0);
    }
    let message = codex::user_message(value);
    let control_kind = message
        .as_ref()
        .and_then(|m| m.control)
        .map(control::ControlKind::as_str);
    let mirrored = if let Some(message) = &message {
        conn.query_row(
            "SELECT message_id, ts_ms FROM session_events WHERE source='codex' AND session_id=?1 AND event_uid=?2 AND text=?3",
            params![session_id, format!("{}:user_message", index.saturating_sub(1)), message.text],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        ).optional()?
    } else {
        None
    };
    // All blocks belong to the same human message, including a response-item
    // mirror whose event_msg already supplied the prompt row.
    let ts_ms = mirrored.as_ref().map_or(ts_ms, |(_, ts)| *ts);
    let message_id = mirrored
        .map(|(id, _)| id)
        .or_else(|| {
            value["payload"]["id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| format!("{index}:response_item_user_message"));
    let mut captured = 0;
    for (part_index, part) in parts.iter().enumerate() {
        if part["type"] != "input_image" {
            continue;
        }
        let markdown = part["image_url"]
            .as_str()
            .and_then(image_markdown)
            .unwrap_or_else(|| {
                "[Image unavailable: the recording has no supported embedded image under 1 MiB.]"
                    .into()
            });
        let uid = format!("{index}:input_image:{part_index}");
        insert_session_event(
            conn,
            &EventRow {
                source: "codex",
                session_id,
                cwd,
                project: cwd,
                git_branch: branch,
                message_id: &message_id,
                ts_ms,
                role: "user",
                kind: "text",
                text: Some(&markdown),
                event_uid: &uid,
                raw_kind: Some("input_image"),
                raw_facts: RawMessageFacts {
                    control_kind,
                    turn_id,
                    ..Default::default()
                },
                ..Default::default()
            },
        )?;
        captured += 1;
    }
    Ok(captured)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injected_context_images_do_not_create_human_usage_boundaries() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_db(&conn).unwrap();
        for (id, ts, role, text, control, tokens) in [
            ("human", 100, "user", "Explain this", None, None),
            (
                "context",
                150,
                "user",
                "<environment_context>injected</environment_context>",
                Some("codex_context_wrapper"),
                None,
            ),
            (
                "answer",
                200,
                "assistant",
                "Answer",
                None,
                Some(r#"{"input_tokens":10,"output_tokens":2}"#),
            ),
        ] {
            insert_session_event(
                &conn,
                &EventRow {
                    source: "codex",
                    session_id: "s",
                    message_id: id,
                    event_uid: id,
                    ts_ms: ts,
                    role,
                    kind: "text",
                    text: Some(text),
                    token_json: tokens,
                    raw_facts: RawMessageFacts {
                        control_kind: control,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .unwrap();
        }
        let value = json!({"type":"response_item","payload":{"type":"message","role":"user","id":"context","content":[{"type":"input_text","text":"<environment_context>injected</environment_context>"},{"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgo="}]}});
        assert!(codex::user_message(&value).unwrap().control.is_some());
        capture_codex(&conn, "s", None, None, 4, 150, None, &value).unwrap();
        let events = crate::store::session_events(&conn, "s", Some("codex")).unwrap();
        let attributed = crate::usage::attribute_usage_to_prompts(&events, "codex");
        assert!(
            attributed.contains_key(&(100, "Explain this".into())),
            "an injected context image must not steal the parentless answer's prompt"
        );
        assert_eq!(
            events
                .iter()
                .find(|e| e.raw_kind.as_deref() == Some("input_image"))
                .unwrap()
                .control_kind
                .as_deref(),
            Some("codex_context_wrapper")
        );
    }

    #[test]
    fn empty_native_ids_use_the_prompt_fallback_identity() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_db(&conn).unwrap();
        let value = json!({"type":"response_item","payload":{"type":"message","role":"user","id":"","content":[{"type":"input_text","text":"Look"},{"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgo="}]}});
        capture_codex(&conn, "s", None, None, 4, 101, None, &value).unwrap();
        assert_eq!(
            conn.query_row("SELECT message_id FROM session_events", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "4:response_item_user_message"
        );
    }

    #[test]
    fn copies_only_bounded_embedded_raster_bytes() {
        let png = "data:image/png;base64,iVBORw0KGgo=";
        assert_eq!(
            image_markdown(png),
            Some(format!("![Attached image]({png})"))
        );
        for url in [
            "file:///etc/passwd",
            "https://example.com/image.png",
            "data:image/svg+xml;base64,PHN2Zz4=",
            "data:image/png;base64,bm90IGEgcG5n",
        ] {
            assert!(image_markdown(url).is_none());
        }
        assert!(
            image_markdown(&format!("data:image/png;base64,{}", "A".repeat(2_000_000))).is_none()
        );
    }

    #[test]
    fn capture_is_idempotent_and_keeps_images_out_of_prompt_history() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_db(&conn).unwrap();
        insert_session_event(
            &conn,
            &EventRow {
                source: "codex",
                session_id: "s",
                message_id: "human-message",
                event_uid: "3:user_message",
                ts_ms: 100,
                role: "user",
                kind: "text",
                text: Some("Look at this"),
                ..Default::default()
            },
        )
        .unwrap();
        let value = json!({"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"Look at this"},{"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgo="}]}});
        for _ in 0..2 {
            capture_codex(&conn, "s", None, None, 4, 101, None, &value).unwrap();
        }
        assert_eq!(
            conn.query_row(
                "SELECT message_id FROM session_events WHERE raw_kind='input_image'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "human-message"
        );
        assert_eq!(conn.query_row("SELECT count(*) FROM session_events WHERE raw_kind='input_image' AND text='![Attached image](data:image/png;base64,iVBORw0KGgo=)'", [], |r| r.get::<_,i64>(0)).unwrap(), 1);
        assert_eq!(
            conn.query_row(
                "SELECT ts_ms FROM session_events WHERE raw_kind='input_image'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            100
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM history", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
}
