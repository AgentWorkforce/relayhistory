use super::tests::fixture;
use super::*;

#[test]
fn repeated_native_prompts_replay_rewrite_and_remove_by_observed_identity() {
    let (home, roots, path, _source) = fixture();
    let mut session = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .sessions
        .into_iter()
        .find(|s| s.native_id.ends_with("443"))
        .unwrap();
    for m in session.messages.iter_mut().filter(|m| m.role == "user") {
        m.content = "retry".into();
    }
    let ledger = crate::open_db(&home.path().join("ledger.db")).unwrap();
    ledger
        .execute(
            "INSERT INTO history(source,session_id,prompt,timestamp_ms) VALUES(?1,?2,'retry',?3)",
            rusqlite::params![ID, session.id, session.started],
        )
        .unwrap();
    normalize::session(&ledger, &session, &path).unwrap();
    let rows = |conn: &Connection| -> Vec<(i64, String, i64)> {
        conn.prepare("SELECT id,event_uid,revision FROM history WHERE source=?1 AND session_id=?2 ORDER BY event_uid").unwrap().query_map(rusqlite::params![ID,session.id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap().collect::<rusqlite::Result<_>>().unwrap()
    };
    let before = rows(&ledger);
    assert_eq!(
        before.len(),
        2,
        "anonymous local projection must not remain as a third prompt"
    );
    normalize::session(&ledger, &session, &path).unwrap();
    assert_eq!(rows(&ledger), before);
    session
        .messages
        .iter_mut()
        .find(|m| m.id == "human-1")
        .unwrap()
        .content = "revised".into();
    normalize::session(&ledger, &session, &path).unwrap();
    let revised = rows(&ledger);
    assert_eq!(
        revised.iter().map(|r| (&r.0, &r.1)).collect::<Vec<_>>(),
        before.iter().map(|r| (&r.0, &r.1)).collect::<Vec<_>>()
    );
    session.messages.retain(|m| m.id != "human-1");
    normalize::session(&ledger, &session, &path).unwrap();
    let remaining = rows(&ledger);
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0], before[1]);
    assert_eq!(
        ledger
            .query_row(
                "SELECT timestamp_ms FROM history WHERE source=?1 AND session_id=?2",
                rusqlite::params![ID, session.id],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        session.started
    );
}
