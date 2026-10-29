use super::*;
use crate::{init_db, open_db, prompt_hash};

fn entry(uid: Option<&str>, session: &str) -> HistoryEntry {
    HistoryEntry {
        id: 0,
        source: "deepagents".into(),
        session_id: Some(session.into()),
        event_uid: uid.map(str::to_owned),
        project: None,
        prompt: "retry".into(),
        prompt_hash: Some(prompt_hash("retry")),
        timestamp_ms: 123,
    }
}

#[test]
fn native_identity_replays_rewrites_and_scopes_without_changing_time() {
    let conn = Connection::open_in_memory().unwrap();
    init_db(&conn).unwrap();
    for (uid, sid) in [("a", "s"), ("b", "s"), ("a", "other")] {
        assert_eq!(
            insert(&conn, &entry(Some(uid), sid), SessionLocation::Local).unwrap(),
            1
        );
    }
    let id: i64 = conn
        .query_row(
            "SELECT id FROM history WHERE session_id='s' AND event_uid='a'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        insert(&conn, &entry(Some("a"), "s"), SessionLocation::Local).unwrap(),
        0
    );
    let mut changed = entry(Some("a"), "s");
    changed.prompt = "revised".into();
    changed.timestamp_ms = 456;
    insert(&conn, &changed, SessionLocation::Local).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT id FROM history WHERE session_id='s' AND event_uid='a'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        id
    );
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM history WHERE timestamp_ms=123",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
    insert(&conn, &entry(None, "legacy"), SessionLocation::Local).unwrap();
    insert(&conn, &entry(None, "legacy-other"), SessionLocation::Local).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM history", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        4
    );
    for bad in [entry(Some(""), "s"), entry(Some("a"), "")] {
        assert!(insert(&conn, &bad, SessionLocation::Local).is_err());
    }
}

#[test]
fn migration_retains_row_ids_revisions_sequence_fts_and_legacy_keys() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("ledger.db");
    let conn = open_db(&path).unwrap();
    insert(&conn, &entry(None, "s"), SessionLocation::Local).unwrap();
    let before: (i64, i64) = conn
        .query_row("SELECT id,revision FROM history", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    legacy_shape(&conn);
    init_db(&conn).unwrap();
    assert_eq!(
        conn.query_row("SELECT id,revision FROM history", [], |r| Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?
        )))
        .unwrap(),
        before
    );
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM history_fts WHERE history_fts MATCH 'retry'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    insert(&conn, &entry(Some("a"), "s"), SessionLocation::Local).unwrap();
    assert!(
        conn.query_row("SELECT id FROM history WHERE event_uid='a'", [], |r| r
            .get::<_, i64>(0))
            .unwrap()
            > 1000
    );
    conn.execute("DELETE FROM history WHERE event_uid IS NULL", [])
        .unwrap();
    let key: String = conn
        .query_row(
            "SELECT record_key FROM evidence_tombstones WHERE kind='history'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&key).unwrap(),
        serde_json::json!([123, "retry"])
    );
    drop(conn);
    let conn = open_db(&path).unwrap();
    assert_eq!(
        conn.query_row("SELECT event_uid FROM history", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "a"
    );
}

fn legacy_shape(conn: &Connection) {
    let triggers: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE tbl_name='history' AND type='trigger'")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    for trigger in triggers {
        conn.execute_batch(&format!("DROP TRIGGER {trigger}"))
            .unwrap();
    }
    conn.execute_batch("CREATE TABLE old_history(id INTEGER PRIMARY KEY AUTOINCREMENT,source TEXT NOT NULL,session_id TEXT,project TEXT,prompt TEXT NOT NULL,prompt_hash TEXT,timestamp_ms INTEGER NOT NULL,git_branch TEXT,revision INTEGER,UNIQUE(source, timestamp_ms, prompt));
        INSERT INTO old_history SELECT id,source,session_id,project,prompt,prompt_hash,timestamp_ms,git_branch,revision FROM history;
        DROP TABLE history; ALTER TABLE old_history RENAME TO history;
        UPDATE sqlite_sequence SET seq=1000 WHERE name='history';
        DELETE FROM schema_migrations WHERE name='history_event_identity_v1';").unwrap();
    // Fingerprint the real legacy exported column order, as an old writer did.
    use sha2::{Digest, Sha256};
    let columns: Vec<(String, String)> = conn
        .prepare("PRAGMA table_info(history)")
        .unwrap()
        .query_map([], |r| Ok((r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .into_iter()
        .filter(|(name, _)| name != "revision")
        .collect();
    let hash = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&("relayhistory-change-feed-export-schema-v2", columns)).unwrap()
        )
    );
    conn.execute("UPDATE change_feed_store SET export_schema_digest=json_set(export_schema_digest,'$.history',?1)",[hash]).unwrap();
}

#[test]
fn a_legacy_owned_snapshot_keeps_its_identity_and_can_withdraw_after_migration() {
    use crate::{
        EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey, SessionStore,
        ShallowSession, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch,
        StoreOptions,
    };
    use serde_json::json;
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("ledger.db");
    let store = SessionStore::open(StoreOptions::default().db_path(&path)).unwrap();
    store
        .apply_source_observations(SourceObservationBatch::new(
            "transport",
            "one",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "deepagents".into(),
                    session_id: "s".into(),
                    source_stamp: Some("native".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    let key = ObservationKey {
        source: "deepagents".into(),
        session_id: "s".into(),
        location: SessionLocation::Remote,
        connector_id: "transport".into(),
        connector_instance: "one".into(),
    };
    let revision = store.source_observation(&key).unwrap().revision.unwrap();
    let record = EvidenceRecord {
        kind: EvidenceKind::History,
        payload:
            json!({"source":"deepagents","session_id":"s","prompt":"retry","timestamp_ms":123})
                .as_object()
                .unwrap()
                .clone(),
        record_id: None,
        revision_id: None,
    };
    store
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key.clone(),
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "native".into(),
                    source_bytes: 0,
                    covered_kinds: vec![EvidenceKind::History],
                    records: vec![record],
                },
            )
            .include_related(false),
        )
        .unwrap();
    drop(store);
    let conn = open_db(&path).unwrap();
    conn.execute("UPDATE observation_evidence SET payload_json=json_remove(payload_json,'$.item.record.payload.event_uid')",[]).unwrap();
    let snapshot: String = conn
        .query_row("SELECT payload_json FROM observation_evidence", [], |r| {
            r.get(0)
        })
        .unwrap();
    legacy_shape(&conn);
    init_db(&conn).unwrap();
    assert_eq!(
        conn.query_row("SELECT payload_json FROM observation_evidence", [], |r| {
            r.get::<_, String>(0)
        })
        .unwrap(),
        snapshot
    );
    drop(conn);
    let store = SessionStore::open(StoreOptions::default().db_path(&path)).unwrap();
    let revision = store.source_observation(&key).unwrap().revision.unwrap();
    store
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "empty".into(),
                    source_bytes: 0,
                    covered_kinds: vec![EvidenceKind::History],
                    records: vec![],
                },
            )
            .include_related(false),
        )
        .unwrap();
    let conn = open_db(&path).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM history", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}
