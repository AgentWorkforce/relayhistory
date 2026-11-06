use super::regressions::{apply_capture, capture, ingest_message, ingest_rollout, sqlite_item};
use super::*;

#[test]
fn a_rewritten_target_outside_the_window_cannot_keep_a_cached_binding() {
    for image in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&directory.path().join("history.db")).unwrap();
        let item = if image {
            image_item("native-item", 0)
        } else {
            sqlite_item("native-item", "assistant", 0)
        };
        if image {
            ingest_message(
                &conn,
                directory.path(),
                "user",
                None,
                vec![item.value["content"][0].clone()],
                false,
            );
        } else {
            ingest_rollout(&conn, directory.path(), "assistant", None);
        }
        let target: String = conn
            .query_row(
                "SELECT event_uid FROM session_events WHERE kind='text'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute(
            "UPDATE session_events SET ts_ms=0 WHERE event_uid=?1",
            [&target],
        )
        .unwrap();
        let captured = capture(vec![item]);
        apply_capture(&conn, directory.path(), &captured).unwrap();
        conn.execute(
            "UPDATE session_events SET ts_ms=?1 WHERE event_uid=?2",
            params![i64::MIN, target],
        )
        .unwrap();
        apply_capture(&conn, directory.path(), &captured).unwrap();
        let uid = if image {
            "native-item:input_image:0"
        } else {
            "native-item"
        };
        let restored: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM session_events WHERE event_uid=?1 AND ts_ms=0)",
                [uid],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            restored,
            "image={image}: an out-of-range target is not a duplicate"
        );
    }
}

#[test]
fn stale_local_claim_cleanup_preserves_a_remote_share_and_other_session_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&directory.path().join("history.db")).unwrap();
    ingest_rollout(&conn, directory.path(), "assistant", None);
    let captured = capture(vec![sqlite_item("native-item", "assistant", 10_000)]);
    apply_capture(&conn, directory.path(), &captured).unwrap();
    conn.execute(
        "UPDATE session_markers SET location='both' WHERE marker_uid='sqlite:overlap:native-item'",
        [],
    )
    .unwrap();
    conn.execute("UPDATE session_events SET session_id='other-session'", [])
        .unwrap();
    apply_capture(&conn, directory.path(), &captured).unwrap();
    let location: String = conn
        .query_row(
            "SELECT location FROM session_markers WHERE marker_uid='sqlite:overlap:native-item'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(location, "remote");
    let sessions: i64 = conn
        .query_row(
            "SELECT count(DISTINCT session_id) FROM session_events WHERE text='answer'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        sessions, 2,
        "the other session's event is preserved and cannot suppress this one"
    );
}
