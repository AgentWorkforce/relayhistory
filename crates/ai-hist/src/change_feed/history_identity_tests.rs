//! Native prompt identity survives feed upgrades and session-scoped deletes.
use super::super::*;
use crate::StoreOptions;

fn history(store: &SessionStore, session: &str) -> Vec<Change> {
    store
        .changes_since(
            Watermark::START,
            ChangeQuery::default()
                .kinds([ChangeKind::History])
                .session("deepagents", session),
        )
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn upgrading_history_triggers_preserves_native_keys_and_session_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.db");
    let conn = crate::open_db(&path).unwrap();
    conn.execute("INSERT INTO history(source,session_id,event_uid,prompt,timestamp_ms) VALUES('deepagents','old','already-deleted','retry',1)",[]).unwrap();
    conn.execute("DELETE FROM history", []).unwrap();
    // Emulate the previous identity-aware feed's sessionless tombstone, plus
    // insert/delete triggers predating native identity on an additive schema.
    conn.execute(
        "UPDATE evidence_tombstones SET session_id='' WHERE kind='history'",
        [],
    )
    .unwrap();
    for trigger in schema::trigger_names(ChangeKind::History) {
        let sql = schema::trigger_sql(&conn, &trigger).unwrap().unwrap();
        let sql = sql
            .replace(
                &super::record_key_sql("NEW"),
                "json_array(NEW.timestamp_ms,NEW.prompt)",
            )
            .replace(
                &super::record_key_sql("OLD"),
                "json_array(OLD.timestamp_ms,OLD.prompt)",
            );
        conn.execute_batch(&format!("DROP TRIGGER {trigger}; {sql}"))
            .unwrap();
    }
    conn.execute(
        "DELETE FROM schema_migrations WHERE name='history_feed_identity_v2'",
        [],
    )
    .unwrap();
    drop(conn);
    let store = SessionStore::open(StoreOptions::default().db_path(&path)).unwrap();
    let old = history(&store, "old");
    assert_eq!(old.len(), 1);
    assert_eq!(
        old[0].key,
        serde_json::json!(["history", "deepagents", "event", "old", "already-deleted"])
            .as_array()
            .unwrap()
            .clone()
    );
    assert!(matches!(old[0].op, ChangeOp::Delete));
    let conn = crate::open_db(&path).unwrap();
    conn.execute("INSERT INTO history(source,session_id,event_uid,prompt,timestamp_ms) VALUES('deepagents','a','native','retry',1)",[]).unwrap();
    let native = history(&store, "a");
    assert_eq!(native.len(), 1);
    let original_key = native[0].key.clone();
    conn.execute(
        "UPDATE history SET session_id='b' WHERE event_uid='native'",
        [],
    )
    .unwrap();
    let a = history(&store, "a");
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].key, original_key);
    assert!(matches!(a[0].op, ChangeOp::Delete));
    let bkey = history(&store, "b")[0].key.clone();
    conn.execute("DELETE FROM history WHERE event_uid='native'", [])
        .unwrap();
    let b = history(&store, "b");
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].key, bkey);
    assert!(matches!(b[0].op, ChangeOp::Delete));
    // Reinsertion clears the same native tombstone, rather than leaving an
    // unmatched delete beside its live upsert.
    conn.execute("INSERT INTO history(source,session_id,event_uid,prompt,timestamp_ms) VALUES('deepagents','b','native','retry',1)",[]).unwrap();
    assert_eq!(history(&store, "b").len(), 1);
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM evidence_tombstones WHERE kind='history' AND session_id='b'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}
