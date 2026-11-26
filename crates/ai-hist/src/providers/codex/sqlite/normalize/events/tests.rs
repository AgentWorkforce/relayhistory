use super::*;
use rusqlite::StatementStatus;

mod freshness;
mod regressions;
mod scaling;

fn record_text(
    conn: &Connection,
    t: &Thread,
    item: &Item,
    uid: &str,
    (role, kind): (&str, &str),
    text: Option<&str>,
    facts: Option<&ToolResultFacts>,
) -> Result<bool> {
    assert_eq!(uid, item.id);
    assert_eq!(kind, "text");
    assert!(facts.is_none());
    text_event(conn, t, item, role, text, &mut Overlaps::load(conn, &t.id)?)
}

fn thread() -> Thread {
    serde_json::from_value(json!({
        "id":"thread", "created":1, "updated":2, "archived":false,
        "next_ordinal":1, "next_offset":100
    }))
    .unwrap()
}

fn item() -> Item {
    Item {
        id: "native-item".into(),
        turn_id: None,
        ts: 10_000,
        kind: "agentMessage".into(),
        value: json!({"id":"native-item","type":"agentMessage","text":"answer"}),
    }
}

#[test]
fn native_overlap_checks_both_identity_columns_in_the_same_session() {
    for column in ["message_id", "provider_message_id"] {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
        conn.execute(
            &format!("INSERT INTO session_events(source,session_id,event_uid,role,kind,ts_ms,{column}) \
                      VALUES ('codex','other','other-event','assistant','text',10000,'native-item')"), [],
        ).unwrap();
        assert!(record_text(
            &conn,
            &thread(),
            &item(),
            "native-item",
            ("assistant", "text"),
            Some("answer"),
            None
        )
        .unwrap());
        conn.execute("DELETE FROM session_events WHERE session_id='thread'", [])
            .unwrap();
        conn.execute("UPDATE session_events SET session_id='thread'", [])
            .unwrap();
        assert!(!record_text(
            &conn,
            &thread(),
            &item(),
            "native-item",
            ("assistant", "text"),
            Some("answer"),
            None
        )
        .unwrap());
    }
}

#[test]
fn idless_overlap_keeps_inclusive_timestamp_boundaries() {
    for delta in [-5001_i64, -5000, 5000, 5001] {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
        conn.execute(
            "INSERT INTO session_events(source,session_id,event_uid,message_id,role,kind,ts_ms,text) \
             VALUES ('codex','thread','rollout','rollout','assistant','text',?1,'answer')", [10_000 + delta],
        ).unwrap();
        let inserted = record_text(
            &conn,
            &thread(),
            &item(),
            "native-item",
            ("assistant", "text"),
            Some("answer"),
            None,
        )
        .unwrap();
        assert_eq!(
            inserted,
            delta.abs() > IDLESS_ROLLOUT_TIMESTAMP_TOLERANCE_MS,
            "delta={delta}"
        );
    }
}

fn image_item(id: &str, ts: i64) -> Item {
    Item {
        id: id.into(),
        turn_id: None,
        ts,
        kind: "userMessage".into(),
        value: json!({"id":id,"type":"userMessage","content":[{
            "type":"input_image", "image_url":"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
        }]}),
    }
}

fn image_markdown(item: &Item) -> String {
    crate::ingest::image_markdown(item.value["content"][0]["image_url"].as_str().unwrap()).unwrap()
}

fn normalize_image(conn: &Connection, item: &Item) {
    super::super::items::normalize_item(
        conn,
        &thread(),
        item,
        &mut ToolResultIndexer::default(),
        false,
        &mut Vec::new(),
        &mut Overlaps::load(conn, "thread").unwrap(),
    )
    .unwrap();
}

#[test]
fn text_and_image_overlap_handle_negative_and_extreme_timestamps() {
    let cases = [
        (-10_000, -15_000, true),
        (-10_000, -15_001, false),
        (i64::MIN, i64::MIN, true),
        (i64::MIN, i64::MIN + 5000, true),
        (i64::MIN, i64::MIN + 5001, false),
        (i64::MAX, i64::MAX, true),
        (i64::MAX, i64::MAX - 5000, true),
        (i64::MAX, i64::MAX - 5001, false),
        (0, i64::MIN, false),
        (i64::MIN, 0, false),
        (i64::MIN, i64::MAX, false),
    ];
    for image in [false, true] {
        for (ts, rollout_ts, overlaps) in cases {
            let dir = tempfile::tempdir().unwrap();
            let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
            let mut item = if image {
                image_item("native-item", ts)
            } else {
                item()
            };
            item.ts = ts;
            let text = if image {
                image_markdown(&item)
            } else {
                "answer".into()
            };
            conn.execute(
                "INSERT INTO session_events(source,session_id,event_uid,message_id,role,kind,ts_ms,text,raw_kind) \
                 VALUES ('codex','thread',?1,?2,?3,'text',?4,?5,?6)",
                params![if image {"1:input_image:0"} else {"rollout"}, if image {"1:response_item_user_message"} else {"rollout"}, if image { "user" } else { "assistant" }, rollout_ts, text, image.then_some("input_image")],
            ).unwrap();
            if image {
                normalize_image(&conn, &item);
            } else {
                let inserted = record_text(
                    &conn,
                    &thread(),
                    &item,
                    "native-item",
                    ("assistant", "text"),
                    Some(&text),
                    None,
                )
                .unwrap();
                assert_eq!(inserted, !overlaps, "ts={ts}, rollout_ts={rollout_ts}");
            }
            let count: i64 = conn
                .query_row("SELECT count(*) FROM session_events", [], |r| r.get(0))
                .unwrap();
            assert_eq!(
                count,
                if overlaps { 1 } else { 2 },
                "image={image}, ts={ts}, rollout_ts={rollout_ts}"
            );
        }
    }
}

#[test]
fn image_overlap_keeps_one_to_one_matching_and_row_id_ties() {
    let dir = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    let image = image_item("first", 10_000);
    let text = image_markdown(&image);
    for (uid, message, ts) in [
        ("1:input_image:0", "1:response_item_user_message", 5000),
        ("2:input_image:0", "2:response_item_user_message", 15_000),
    ] {
        conn.execute(
            "INSERT INTO session_events(source,session_id,event_uid,message_id,role,kind,ts_ms,text,raw_kind) \
             VALUES ('codex','thread',?1,?2,'user','text',?3,?4,'input_image')",
            params![uid, message, ts, text],
        ).unwrap();
    }
    normalize_image(&conn, &image);
    normalize_image(&conn, &image_item("second", 10_000));
    normalize_image(&conn, &image_item("third", 10_000));
    normalize_image(&conn, &image);
    for (native, rollout) in [("first", "1:input_image:0"), ("second", "2:input_image:0")] {
        let chosen: String = conn.query_row(
            "SELECT json_extract(payload_json,'$.rollout_event_uid') FROM session_markers WHERE marker_uid=?1",
            [format!("sqlite:overlap:{native}:input_image:0")], |r| r.get(0),
        ).unwrap();
        assert_eq!(chosen, rollout);
    }
    let count: i64 = conn
        .query_row("SELECT count(*) FROM session_events", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn overlap_lookups_do_not_walk_unrelated_session_items() {
    let dir = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    conn.execute_batch(
        "WITH RECURSIVE ord(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM ord WHERE n<9999) \
         INSERT INTO session_events(source,session_id,event_uid,message_id,role,kind,ts_ms,text,raw_kind) \
         SELECT 'codex','thread','item-'||n,'item-'||n,'assistant','text',n*1000,'answer','agentMessage' FROM ord;",
    ).unwrap();
    let mut native = conn.prepare(NATIVE_ITEM_SQL).unwrap();
    assert!(!native
        .query_row(
            params!["thread", "missing", "assistant", "text", "missing"],
            |r| r.get::<_, bool>(0)
        )
        .unwrap());
    assert!(native.get_status(StatementStatus::VmStep) < 1000);
    let mut idless = conn.prepare(IDLESS_TEXT_SQL).unwrap();
    let found = idless
        .query_row(
            params![
                "thread",
                "assistant",
                "answer",
                5_000_000,
                5000,
                4_995_000,
                5_005_000
            ],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .unwrap();
    assert!(found.is_none());
    assert!(idless.get_status(StatementStatus::VmStep) < 1000);
}
