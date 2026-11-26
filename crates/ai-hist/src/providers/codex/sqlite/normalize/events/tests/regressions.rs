use super::*;

pub(super) fn ingest_rollout(conn: &Connection, directory: &Path, role: &str, id: Option<&str>) {
    ingest_message(
        conn,
        directory,
        role,
        id,
        vec![json!({
            "type":if role == "user" {"input_text"} else {"output_text"}, "text":"answer"
        })],
        false,
    );
}

pub(super) fn ingest_message(
    conn: &Connection,
    directory: &Path,
    role: &str,
    id: Option<&str>,
    content: Vec<Value>,
    mirror: bool,
) {
    let path = directory.join("rollout.jsonl");
    let mut message = json!({
        "timestamp":"1970-01-01T00:00:10Z", "type":"response_item",
        "payload":{"type":"message", "role":role, "content":content}
    });
    if let Some(id) = id {
        message["payload"]["id"] = json!(id);
    }
    let meta = json!({
        "timestamp":"1970-01-01T00:00:01Z", "type":"session_meta",
        "payload":{"id":"thread", "cwd":directory.to_string_lossy()}
    });
    let mut lines = format!("{meta}\n");
    if mirror {
        let mut mirrored = json!({"timestamp":"1970-01-01T00:00:10Z","type":"event_msg",
            "payload":{"type":"user_message","message":"answer"}});
        if let Some(id) = id {
            mirrored["payload"]["id"] = json!(id);
        }
        lines.push_str(&format!("{mirrored}\n"));
    }
    lines.push_str(&format!("{message}\n"));
    std::fs::write(&path, lines).unwrap();
    let meta = crate::ingest::read_codex_session_meta(&path)
        .unwrap()
        .unwrap();
    crate::ingest::ingest_codex_rollout(conn, &path, &meta).unwrap();
}

pub(super) fn sqlite_item(id: &str, role: &str, ts: i64) -> Item {
    let kind = if role == "user" {
        "userMessage"
    } else {
        "agentMessage"
    };
    let value = if role == "user" {
        json!({"id":id,"type":kind,"content":[{"type":"text","text":"answer"}]})
    } else {
        json!({"id":id,"type":kind,"text":"answer"})
    };
    Item {
        id: id.into(),
        turn_id: None,
        ts,
        kind: kind.into(),
        value,
    }
}

pub(super) fn capture(items: Vec<Item>) -> Capture {
    Capture {
        thread: thread(),
        items,
        turns: Vec::new(),
        edges: Vec::new(),
        related: Vec::new(),
        related_rollouts: Vec::new(),
        related_incomplete: false,
    }
}

pub(super) fn apply_capture(conn: &Connection, directory: &Path, capture: &Capture) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    super::super::super::reconcile(&tx, capture, &directory.join("state_5.sqlite"), false)?;
    tx.commit()?;
    Ok(())
}

fn identified_rollout_cannot_hide_a_distinct_sqlite_message(role: &str) {
    let directory = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&directory.path().join("history.db")).unwrap();
    ingest_rollout(&conn, directory.path(), role, Some("first"));
    let (message_id, event_uid, provider_id): (String, String, Option<String>) = conn
        .query_row(
            "SELECT message_id,event_uid,provider_message_id FROM session_events WHERE role=?1 AND text='answer'",
            [role],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    if role == "user" {
        assert_eq!(message_id, "first");
        assert_ne!(message_id, event_uid);
    } else {
        assert_eq!(message_id, event_uid);
        assert_eq!(provider_id.as_deref(), Some("first"));
    }
    apply_capture(
        &conn,
        directory.path(),
        &capture(vec![
            sqlite_item("first", role, 10_000),
            sqlite_item("second", role, 11_000),
        ]),
    )
    .unwrap();
    let events: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_events WHERE role=?1 AND text='answer'",
            [role],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        events, 2,
        "role={role}: native IDs prove these are distinct messages"
    );
    if role == "user" {
        let prompts: i64 = conn
            .query_row(
                "SELECT count(*) FROM history WHERE session_id='thread'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(prompts, 2);
    }
}

#[test]
fn identified_user_rollout_cannot_hide_a_distinct_sqlite_message() {
    identified_rollout_cannot_hide_a_distinct_sqlite_message("user");
}

#[test]
fn identified_assistant_rollout_cannot_hide_a_distinct_sqlite_message() {
    identified_rollout_cannot_hide_a_distinct_sqlite_message("assistant");
}

#[test]
fn stale_overlap_after_retention_cannot_hide_unchanged_sqlite_capture() {
    use crate::retention::{RetentionOptions, RetentionPolicy};
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("history.db");
    let conn = crate::open_db(&db).unwrap();
    ingest_rollout(&conn, directory.path(), "assistant", None);
    let capture = Capture {
        thread: thread(),
        items: vec![sqlite_item("native-item", "assistant", 10_000)],
        turns: Vec::new(),
        edges: Vec::new(),
        related: Vec::new(),
        related_rollouts: Vec::new(),
        related_incomplete: false,
    };
    apply_capture(&conn, directory.path(), &capture).unwrap();
    let history = crate::SessionStore::open(crate::StoreOptions::default().db_path(db)).unwrap();
    history
        .set_retention_policy(RetentionPolicy::from_days(7))
        .unwrap();
    history.register_retention_consumer("overlap-test").unwrap();
    history
        .acknowledge_retention("overlap-test", history.head_revision().unwrap())
        .unwrap();
    let pruned = history
        .prune_expired(RetentionOptions {
            max_rows: 1,
            min_interval_ms: 0,
            allow_unuploaded: false,
        })
        .unwrap();
    assert_eq!(pruned.rows, 1);
    let target_exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE session_id='thread' AND text='answer')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!target_exists);
    let marker_exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM session_markers WHERE marker_uid='sqlite:overlap:native-item')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        marker_exists,
        "bounded retention must leave the later marker to reproduce the bug"
    );
    apply_capture(&conn, directory.path(), &capture).unwrap();
    let restored: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE session_id='thread' AND event_uid='native-item' AND text='answer')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        restored,
        "a dangling overlap marker is not evidence that a message exists"
    );
}

#[test]
fn real_image_rollouts_keep_native_identity_and_legacy_mirror_matching() {
    for native in [false, true] {
        for mirror in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let conn = crate::open_db(&directory.path().join("history.db")).unwrap();
            let first = image_item("first", 10_000);
            let mut content = vec![first.value["content"][0].clone()];
            if mirror {
                content.insert(0, json!({"type":"input_text","text":"answer"}));
            }
            ingest_message(
                &conn,
                directory.path(),
                "user",
                native.then_some("first"),
                content,
                mirror,
            );
            let (uid, parent): (String, String) = conn
                .query_row(
                    "SELECT event_uid,message_id FROM session_events WHERE raw_kind='input_image'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            assert_ne!(
                uid, parent,
                "images retain their parent message identity separately"
            );
            if native {
                assert_eq!(parent, "first");
            } else if mirror {
                assert_eq!(parent, "1:user_message");
            } else {
                assert_eq!(parent, "1:response_item_user_message");
            }
            let captured = capture(vec![first, image_item("second", 11_000)]);
            apply_capture(&conn, directory.path(), &captured).unwrap();
            apply_capture(&conn, directory.path(), &captured).unwrap();
            let count: i64 = conn
                .query_row(
                    "SELECT count(*) FROM session_events WHERE raw_kind='input_image'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(count, 2, "native={native},mirror={mirror}");
            let claims: i64 = conn
                .query_row(
                    "SELECT count(*) FROM session_markers WHERE kind='rollout_overlap'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(claims, if native { 0 } else { 1 });
        }
    }
}

#[test]
fn missing_image_target_is_repaired_during_an_authorized_reconciliation() {
    let directory = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&directory.path().join("history.db")).unwrap();
    let first = image_item("first", 10_000);
    ingest_message(
        &conn,
        directory.path(),
        "user",
        None,
        vec![first.value["content"][0].clone()],
        false,
    );
    let captured = capture(vec![first]);
    apply_capture(&conn, directory.path(), &captured).unwrap();
    conn.execute(
        "DELETE FROM session_events WHERE raw_kind='input_image'",
        [],
    )
    .unwrap();
    apply_capture(&conn, directory.path(), &captured).unwrap();
    let restored: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE event_uid='first:input_image:0')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(restored);
    let stale: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM session_markers WHERE marker_uid='sqlite:overlap:first:input_image:0')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!stale);
}

#[test]
fn a_failed_marker_write_does_not_claim_the_rollout_in_memory() {
    let directory = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&directory.path().join("history.db")).unwrap();
    ingest_rollout(&conn, directory.path(), "assistant", None);
    let uid: String = conn
        .query_row(
            "SELECT event_uid FROM session_events WHERE text='answer'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let mut overlaps = Overlaps::load(&conn, "thread").unwrap();
    conn.execute_batch("CREATE TRIGGER fail_overlap BEFORE INSERT ON session_markers WHEN NEW.kind='rollout_overlap' BEGIN SELECT RAISE(ABORT,'injected claim failure'); END;").unwrap();
    assert!(text_event(
        &conn,
        &thread(),
        &sqlite_item("first", "assistant", 10_000),
        "assistant",
        Some("answer"),
        &mut overlaps
    )
    .is_err());
    assert!(!overlaps.claimed(&uid));
    conn.execute_batch("DROP TRIGGER fail_overlap").unwrap();
    assert!(!text_event(
        &conn,
        &thread(),
        &sqlite_item("second", "assistant", 11_000),
        "assistant",
        Some("answer"),
        &mut overlaps
    )
    .unwrap());
    assert!(overlaps.claimed(&uid));
}

#[test]
fn failed_reconciliation_rolls_back_claims_before_a_fresh_retry() {
    use std::io::Write;
    let directory = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&directory.path().join("history.db")).unwrap();
    ingest_rollout(&conn, directory.path(), "assistant", None);
    let path = directory.path().join("rollout.jsonl");
    let next = json!({"timestamp":"1970-01-01T00:00:20Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}});
    writeln!(
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap(),
        "{next}"
    )
    .unwrap();
    let meta = crate::ingest::read_codex_session_meta(&path)
        .unwrap()
        .unwrap();
    crate::ingest::ingest_codex_rollout(&conn, &path, &meta).unwrap();
    let captured = capture(vec![
        sqlite_item("first", "assistant", 10_000),
        sqlite_item("second", "assistant", 20_000),
    ]);
    conn.execute_batch("CREATE TRIGGER fail_second_overlap BEFORE INSERT ON session_markers WHEN NEW.marker_uid='sqlite:overlap:second' BEGIN SELECT RAISE(ABORT,'injected claim failure'); END;").unwrap();
    assert!(apply_capture(&conn, directory.path(), &captured).is_err());
    let claims: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_markers WHERE kind='rollout_overlap'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        claims, 0,
        "the caller transaction rolls back the earlier successful claim too"
    );
    conn.execute_batch("DROP TRIGGER fail_second_overlap")
        .unwrap();
    apply_capture(&conn, directory.path(), &captured).unwrap();
    let claims: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_markers WHERE kind='rollout_overlap'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(claims, 2);
    let events: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_events WHERE role='assistant' AND kind='text'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(events, 2);
}

#[test]
fn an_old_claim_to_an_identified_message_is_repaired_without_rewriting_the_capture() {
    for role in ["user", "assistant"] {
        let directory = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&directory.path().join("history.db")).unwrap();
        ingest_rollout(&conn, directory.path(), role, Some("first"));
        let target: String = conn
            .query_row(
                "SELECT event_uid FROM session_events WHERE text='answer'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let captured = capture(vec![
            sqlite_item("first", role, 10_000),
            sqlite_item("second", role, 11_000),
        ]);
        apply_capture(&conn, directory.path(), &captured).unwrap();
        conn.execute("DELETE FROM session_events WHERE event_uid='second'", [])
            .unwrap();
        super::super::super::marker(
            &conn,
            &thread(),
            "sqlite:overlap:second",
            11_000,
            "rollout_overlap",
            &json!({"rollout_event_uid":target}),
            None,
        )
        .unwrap();
        apply_capture(&conn, directory.path(), &captured).unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM session_events WHERE text='answer' AND role=?1",
                [role],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 2, "role={role}");
        let bad: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM session_markers WHERE marker_uid='sqlite:overlap:second')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!bad);
    }
}
