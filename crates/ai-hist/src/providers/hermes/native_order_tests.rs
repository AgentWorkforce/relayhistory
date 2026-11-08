use super::*;

#[test]
fn a_complete_hermes_native_reread_enriches_unknown_order_in_place() {
    let (home, roots, path, _db) = fixture();
    let mut native = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .into_iter()
        .find(|s| s.native_id == NATIVE)
        .unwrap();
    native.messages.truncate(2);
    for (index, message) in native.messages.iter_mut().enumerate() {
        message.id = if index == 0 { 2 } else { 10 };
        message.role = "assistant".into();
        message.content = Some(format!("native-{index}"));
        message.timestamp = 123;
        message.tool_calls = None;
        message.tool_call_id = None;
        message.reasoning = None;
        message.reasoning_content = None;
    }
    native.messages.reverse();
    let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
    normalize::session(&conn, &native, &path).unwrap();
    conn.execute(
        "UPDATE session_events SET source_order=NULL WHERE source='hermes'",
        [],
    )
    .unwrap();
    let rows = || {
        conn.prepare("SELECT id,event_uid,ts_ms,text FROM session_events WHERE source='hermes' ORDER BY event_uid").unwrap().query_map([],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,String>(3)?))).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap()
    };
    let before = rows();
    native.messages.reverse();
    normalize::session(&conn, &native, &path).unwrap();
    assert_eq!(rows(), before);
    let order:Vec<(String,i64)>=conn.prepare("SELECT event_uid,source_order FROM session_events WHERE source='hermes' ORDER BY source_order").unwrap().query_map([],|r|Ok((r.get(0)?,r.get(1)?))).unwrap().collect::<rusqlite::Result<_>>().unwrap();
    assert_eq!(order, vec![("2:text".into(), 0), ("10:text".into(), 1)]);
    let revisions = || {
        conn.prepare("SELECT revision FROM session_events WHERE source='hermes' ORDER BY event_uid")
            .unwrap()
            .query_map([], |r| r.get::<_, i64>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    let after = revisions();
    normalize::session(&conn, &native, &path).unwrap();
    assert_eq!(revisions(), after);
}
