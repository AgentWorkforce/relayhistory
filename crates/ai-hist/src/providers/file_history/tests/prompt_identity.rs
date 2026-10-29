//! Session-scoped prompt identity cannot be handed off by equal text/time.
use super::*;
use crate::{
    EvidenceKind, HistoryEntry, NormalizedSourceEvidence, ObservationKey, SessionLocation,
    SessionStore, ShallowSession, SourceEvidenceSubmission, SourceObservation,
    SourceObservationBatch, StoreOptions,
};

fn native(conn: &Connection, sid: &str, uid: &str) {
    conn.execute("INSERT INTO sessions(source,session_id,cwd,discovery_state) VALUES('deepagents',?1,?1,'full')",[sid]).unwrap();
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,message_id,ts_ms,role,kind,text,project,cwd,source_order) VALUES('deepagents',?1,?2,?1,123,'user','text','retry',?1,?1,0)",rusqlite::params![sid,uid]).unwrap();
    crate::insert_history(
        conn,
        &HistoryEntry {
            event_uid: Some(uid.into()),
            id: 0,
            source: "deepagents".into(),
            session_id: Some(sid.into()),
            project: Some(sid.into()),
            prompt: "retry".into(),
            prompt_hash: None,
            timestamp_ms: 123,
        },
    )
    .unwrap();
}

fn retain_remote(store: &SessionStore, conn: &Connection) {
    store
        .apply_source_observations(SourceObservationBatch::new(
            "transport",
            "one",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "deepagents".into(),
                    session_id: "a".into(),
                    cwd: Some("a".into()),
                    source_stamp: Some("remote".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    let key = ObservationKey {
        source: "deepagents".into(),
        session_id: "a".into(),
        location: SessionLocation::Remote,
        connector_id: "transport".into(),
        connector_instance: "one".into(),
    };
    let kinds = vec![EvidenceKind::History];
    let records = crate::source_evidence::read_session(conn, "deepagents", "a", &kinds).unwrap();
    let revision = store.source_observation(&key).unwrap().revision.unwrap();
    store
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "remote".into(),
                    source_bytes: 0,
                    covered_kinds: kinds,
                    records,
                },
            )
            .include_related(false),
        )
        .unwrap();
}

fn prompts(conn: &Connection) -> Vec<(String, String)> {
    conn.prepare("SELECT session_id,event_uid FROM history WHERE source='deepagents' ORDER BY session_id,event_uid").unwrap().query_map([],|r|Ok((r.get(0)?,r.get(1)?))).unwrap().collect::<rusqlite::Result<_>>().unwrap()
}

fn retirement(remote: bool) {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("ledger.db");
    let store = SessionStore::open(StoreOptions::default().db_path(&path)).unwrap();
    let conn = crate::open_db(&path).unwrap();
    native(&conn, "a", "a:0");
    native(&conn, "b", "b:0");
    if remote {
        retain_remote(&store, &conn);
    }
    let sibling = conn
        .query_row(
            "SELECT id,revision,project FROM history WHERE session_id='b'",
            [],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    retire_local_session(&tx, "deepagents", "a").unwrap();
    tx.commit().unwrap();
    let expected = if remote {
        vec![("a".into(), "a:0".into()), ("b".into(), "b:0".into())]
    } else {
        vec![("b".into(), "b:0".into())]
    };
    assert_eq!(
        prompts(&conn),
        expected,
        "an event UID must never migrate to an equal-text/time sibling"
    );
    assert_eq!(
        conn.query_row(
            "SELECT id,revision,project FROM history WHERE session_id='b'",
            [],
            |r| Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Option<String>>(2)?
            ))
        )
        .unwrap(),
        sibling
    );
    assert_eq!(
        conn.query_row(
            "SELECT event_uid,location FROM session_events WHERE session_id='b'",
            [],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        )
        .unwrap(),
        ("b:0".into(), "local".into())
    );
    let a_events: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_events WHERE session_id='a' AND event_uid='a:0'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        a_events, 0,
        "history-only remote proof does not invent event backing"
    );
}

#[test]
fn local_identified_prompt_retires_without_migrating_to_equal_text_sibling() {
    retirement(false);
}
#[test]
fn remote_supported_identified_prompt_keeps_original_session_and_event_identity() {
    retirement(true);
}

#[test]
fn identified_rewrite_ignores_anonymous_equal_text_keep_tokens() {
    let conn = Connection::open_in_memory().unwrap();
    crate::init_db(&conn).unwrap();
    native(&conn, "a", "a:0");
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,message_id,ts_ms,role,kind,text,project,cwd) SELECT source,session_id,'a:1','next-message',ts_ms,role,kind,text,project,cwd FROM session_events WHERE event_uid='a:0'",[]).unwrap();
    conn.execute("INSERT INTO history(source,session_id,event_uid,project,prompt,prompt_hash,timestamp_ms) SELECT source,session_id,'a:1',project,prompt,prompt_hash,timestamp_ms FROM history WHERE event_uid='a:0'",[]).unwrap();
    let mut keep = Keys::default();
    keep.events.insert("a:1".into());
    keep.prompts
        .insert(format!("123:{}", crate::prompt_hash("retry")));
    let tx = conn.unchecked_transaction().unwrap();
    retire(&tx, "deepagents", "a", &keep).unwrap();
    tx.commit().unwrap();
    assert_eq!(prompts(&conn), vec![("a".into(), "a:1".into())]);
}
