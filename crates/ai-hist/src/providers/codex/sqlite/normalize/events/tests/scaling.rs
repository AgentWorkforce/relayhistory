use super::regressions::{apply_capture, capture};
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct Steps<'a> {
    conn: &'a Connection,
    count: Arc<AtomicUsize>,
}

impl<'a> Steps<'a> {
    fn start(conn: &'a Connection) -> Self {
        let count = Arc::new(AtomicUsize::new(0));
        let measured = Arc::clone(&count);
        conn.progress_handler(
            1,
            Some(move || {
                measured.fetch_add(1, Ordering::Relaxed);
                false
            }),
        );
        Self { conn, count }
    }
}

impl Drop for Steps<'_> {
    fn drop(&mut self) {
        self.conn.progress_handler(0, None::<fn() -> bool>);
    }
}

fn reconcile_work(count: usize, image: bool, analyze: bool) -> usize {
    let directory = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&directory.path().join("history.db")).unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    let mut items = Vec::new();
    for index in 0..2 * count {
        let ts = (index as i64 + 1) * 10_000;
        let native = format!("native-{index}");
        let (uid, message, raw, role, text, item) = if image {
            let item = image_item(&native, ts);
            (
                format!("{}:input_image:0", index + 1),
                format!("{}:response_item_user_message", index + 1),
                Some("input_image"),
                "user",
                image_markdown(&item),
                item,
            )
        } else {
            let uid = format!("{}:agent_message", index + 1);
            let text = format!("answer-{index}");
            let value = json!({"id":native,"type":"agentMessage","text":text});
            let item = Item {
                id: native,
                turn_id: None,
                ts,
                kind: "agentMessage".into(),
                value,
            };
            (uid.clone(), uid, None, "assistant", text, item)
        };
        tx.execute(
            "INSERT INTO session_events(source,session_id,event_uid,message_id,role,kind,ts_ms,text,raw_kind) \
             VALUES ('codex','thread',?1,?2,?3,'text',?4,?5,?6)",
            params![uid,message,role,ts,text,raw],
        ).unwrap();
        items.push(item);
    }
    tx.commit().unwrap();
    let captured = capture(items);
    apply_capture(&conn, directory.path(), &captured).unwrap();
    // Keep the capture hash unchanged while half its live bindings are absent.
    // The measured pass loads existing claims and creates the missing half.
    for index in count..2 * count {
        let uid = if image {
            format!("sqlite:overlap:native-{index}:input_image:0")
        } else {
            format!("sqlite:overlap:native-{index}")
        };
        conn.execute("DELETE FROM session_markers WHERE marker_uid=?1", [uid])
            .unwrap();
    }
    let initial: usize = conn
        .query_row(
            "SELECT count(*) FROM session_markers WHERE kind='rollout_overlap'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        initial, count,
        "the timed cache load must include existing live claims"
    );
    if analyze {
        conn.execute_batch("ANALYZE").unwrap();
    }
    let steps = Steps::start(&conn);
    apply_capture(&conn, directory.path(), &captured).unwrap();
    let measured = steps.count.load(Ordering::Relaxed);
    drop(steps);
    let (claims, targets): (usize, usize) = conn
        .query_row(
            "SELECT count(*),count(DISTINCT json_extract(payload_json,'$.rollout_event_uid')) \
         FROM session_markers WHERE kind='rollout_overlap'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(claims, 2 * count);
    assert_eq!(targets, 2 * count);
    let events: usize = conn
        .query_row(
            "SELECT count(*) FROM session_events WHERE session_id='thread'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        events,
        2 * count,
        "successful matches must keep only the rollout events"
    );
    measured
}

#[test]
fn successful_overlap_work_scales_with_live_claims_and_new_matches() {
    for image in [false, true] {
        for analyze in [false, true] {
            let small = reconcile_work(256, image, analyze);
            let large = reconcile_work(512, image, analyze);
            eprintln!(
                "overlap-work image={image} analyze={analyze} claims=256:{small},512:{large}"
            );
            assert!(large*4 < small*11,"doubling claims/matches must avoid a quadratic marker scan: image={image},analyze={analyze},small={small},large={large}");
        }
    }
}
