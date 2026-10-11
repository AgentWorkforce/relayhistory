//! Embedded images are self-contained Markdown content blocks. Never follow a
//! transcript's file paths or remote URLs: only copy bytes the harness recorded.
use super::*;
use base64::Engine;

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
    let mirrored_id = if let Some(message) = &message {
        conn.query_row(
            "SELECT message_id FROM session_events WHERE source='codex' AND session_id=?1 AND event_uid=?2 AND text=?3",
            params![session_id, format!("{}:user_message", index.saturating_sub(1)), message.text],
            |row| row.get::<_, String>(0),
        ).optional()?
    } else {
        None
    };
    // All blocks belong to the same human message, including a response-item
    // mirror whose event_msg already supplied the prompt row.
    let message_id = mirrored_id
        .or_else(|| value["payload"]["id"].as_str().map(str::to_string))
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
            capture_codex(&conn, "s", None, None, 4, 100, None, &value).unwrap();
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
            conn.query_row("SELECT count(*) FROM history", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
}
