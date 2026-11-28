//! Public writer boundaries must respect a capture's cross-transaction fence.
use crate::*;
use rusqlite::{params, Connection};
use serde_json::json;

fn fixture() -> (tempfile::TempDir, SessionStore, Connection) {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStore::open(StoreOptions::default().db_path(home.path().join("history.db")))
        .unwrap();
    let conn = open_db(store.db_path()).unwrap();
    (home, store, conn)
}
fn key(source: &str) -> ObservationKey {
    ObservationKey {
        source: source.into(),
        session_id: "fenced".into(),
        location: SessionLocation::Remote,
        connector_id: "fence-test".into(),
        connector_instance: "one".into(),
    }
}
fn observe(store: &SessionStore, source: &str) -> String {
    store
        .apply_source_observations(SourceObservationBatch::new(
            "fence-test",
            "one",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: source.into(),
                    session_id: "fenced".into(),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    store
        .source_observation(&key(source))
        .unwrap()
        .revision
        .unwrap()
}
fn submission(source: &str, revision: String) -> SourceEvidenceSubmission {
    SourceEvidenceSubmission::new(key(source), revision, NormalizedSourceEvidence { source_stamp: "new".into(), source_bytes: 1,
        covered_kinds: vec![EvidenceKind::SessionEvent], records: vec![EvidenceRecord { kind: EvidenceKind::SessionEvent,
            payload: json!({"source":source,"session_id":"fenced","event_uid":"new","message_id":"new","ts_ms":1,"role":"assistant","kind":"text","text":"new","source_order":0}).as_object().unwrap().clone(), record_id: None, revision_id: None }] }).include_related(false)
}
fn old_session(conn: &Connection) {
    conn.execute("INSERT INTO sessions(source,session_id,last_activity_ms,discovery_state) VALUES('claude','fenced',1,'full')", []).unwrap();
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,message_id,ts_ms,role,kind,text,source_order) VALUES('claude','fenced','old','old',1,'assistant','text','old',0)", []).unwrap();
}
fn event_count(conn: &Connection, source: &str) -> i64 {
    conn.query_row(
        "SELECT count(*) FROM session_events WHERE source=?1",
        params![source],
        |row| row.get(0),
    )
    .unwrap()
}

#[test]
fn public_intake_fence_rejects_claude_without_mutation_but_allows_codex() {
    let (_home, store, conn) = fixture();
    let claude_revision = observe(&store, "claude");
    let codex_revision = observe(&store, "codex");
    let before = store.head_revision().unwrap();
    let guard = crate::ingest::claude_write_fence::acquire(&conn, "claude").unwrap();
    let error = store
        .apply_source_evidence(submission("claude", claude_revision.clone()))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Claude source is being captured"),
        "{error}"
    );
    assert_eq!(event_count(&conn, "claude"), 0);
    assert_eq!(store.head_revision().unwrap(), before);
    assert_eq!(
        store
            .source_observation(&key("claude"))
            .unwrap()
            .revision
            .as_deref(),
        Some(claude_revision.as_str())
    );
    store
        .apply_source_evidence(submission("codex", codex_revision))
        .unwrap();
    assert_eq!(event_count(&conn, "codex"), 1);
    drop(guard);
    store
        .apply_source_evidence(submission("claude", claude_revision))
        .unwrap();
    assert_eq!(event_count(&conn, "claude"), 1);
}

#[test]
fn public_forget_fence_preserves_evidence_and_generation_until_release() {
    let (_home, store, conn) = fixture();
    old_session(&conn);
    let before = store.head_revision().unwrap();
    let generation = store.retention_generation().unwrap();
    let scope = ForgetScope::Sessions(vec![SessionRef::id(Source::Claude, "fenced")]);
    let opts = ForgetOptions::default().include_unrecoverable(true);
    let guard = crate::ingest::claude_write_fence::acquire(&conn, "claude").unwrap();
    let error = store
        .forget_evidence(scope.clone(), opts.clone())
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Claude source is being captured"),
        "{error}"
    );
    assert_eq!(event_count(&conn, "claude"), 1);
    assert_eq!(store.head_revision().unwrap(), before);
    assert_eq!(store.retention_generation().unwrap(), generation);
    drop(guard);
    store.forget_evidence(scope, opts).unwrap();
    assert_eq!(event_count(&conn, "claude"), 0);
}

#[test]
fn public_retention_fence_reports_busy_without_eviction_until_release() {
    let (_home, store, conn) = fixture();
    old_session(&conn);
    store
        .set_retention_policy(retention::RetentionPolicy::from_days(1))
        .unwrap();
    let before = store.head_revision().unwrap();
    let generation = store.retention_generation().unwrap();
    let opts = retention::RetentionOptions {
        allow_unuploaded: true,
        min_interval_ms: 0,
        ..Default::default()
    };
    let guard = crate::ingest::claude_write_fence::acquire(&conn, "claude").unwrap();
    let report = store.prune_expired(opts.clone()).unwrap();
    assert!(report.busy);
    assert_eq!(report.rows, 0);
    assert_eq!(event_count(&conn, "claude"), 1);
    assert_eq!(store.head_revision().unwrap(), before);
    assert_eq!(store.retention_generation().unwrap(), generation);
    drop(guard);
    assert!(store.prune_expired(opts).unwrap().rows > 0);
    assert_eq!(event_count(&conn, "claude"), 0);
}

#[test]
fn public_codex_only_forget_does_not_take_the_claude_fence() {
    let (_home, store, conn) = fixture();
    old_session(&conn);
    conn.execute("INSERT INTO sessions(source,session_id,last_activity_ms,discovery_state) VALUES('codex','fenced',1,'full')",[]).unwrap();
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES('codex','fenced','codex',1,'assistant','text','codex')",[]).unwrap();
    let _guard = crate::ingest::claude_write_fence::acquire(&conn, "claude").unwrap();
    let report = store
        .forget_evidence(
            ForgetScope::Sessions(vec![SessionRef::id(Source::Codex, "fenced")]),
            ForgetOptions::default().include_unrecoverable(true),
        )
        .unwrap();
    assert_eq!(report.events, 1);
    assert_eq!(event_count(&conn, "codex"), 0);
    assert_eq!(event_count(&conn, "claude"), 1);
}
