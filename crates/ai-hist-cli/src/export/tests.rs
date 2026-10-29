use super::*;

fn fresh() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    ai_hist::init_db(&conn).unwrap();
    conn
}

type PromptIdentity = (String, Option<String>, Option<String>, String, i64);

fn identities(conn: &Connection) -> Vec<PromptIdentity> {
    conn.prepare("SELECT source,session_id,event_uid,prompt,timestamp_ms FROM history ORDER BY source,session_id,event_uid")
        .unwrap().query_map([], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)))
        .unwrap().collect::<rusqlite::Result<_>>().unwrap()
}

#[test]
fn native_repeated_prompts_round_trip_jsonl_gzip_and_sqlite_without_losing_identity() {
    let dir = tempfile::tempdir().unwrap();
    let source = fresh();
    for uid in ["native-user-1", "native-user-2"] {
        assert_eq!(
            insert_history(
                &source,
                &HistoryEntry {
                    id: 0,
                    source: "deepagents".into(),
                    session_id: Some("session".into()),
                    event_uid: Some(uid.into()),
                    project: None,
                    prompt: "repeat exactly".into(),
                    prompt_hash: None,
                    timestamp_ms: 1,
                }
            )
            .unwrap(),
            1
        );
    }
    insert_history(
        &source,
        &HistoryEntry {
            id: 0,
            source: "claude".into(),
            session_id: None,
            event_uid: None,
            project: None,
            prompt: "anonymous legacy prompt".into(),
            prompt_hash: None,
            timestamp_ms: 1,
        },
    )
    .unwrap();
    let expected = identities(&source);
    assert_eq!(expected.len(), 3);
    for (name, format) in [
        ("history.jsonl", "jsonl"),
        ("history.jsonl.gz", "jsonl"),
        ("history.db", "sqlite"),
    ] {
        let path = dir.path().join(name);
        export_history(
            &source,
            Path::new(":memory:"),
            Some(&path),
            format,
            None,
            None,
            None,
        )
        .unwrap();
        let restored = fresh();
        crate::import::import_history(&restored, &path, false).unwrap();
        assert_eq!(
            identities(&restored),
            expected,
            "{name} lost source identity"
        );
        crate::import::import_history(&restored, &path, false).unwrap();
        assert_eq!(
            identities(&restored),
            expected,
            "{name} replay duplicated native prompts"
        );
    }
}

#[test]
fn legacy_sqlite_import_keeps_anonymous_rows_without_an_identity_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    let legacy = Connection::open(&path).unwrap();
    legacy.execute_batch("CREATE TABLE history(id INTEGER PRIMARY KEY,source TEXT,session_id TEXT,project TEXT,prompt TEXT,timestamp_ms INTEGER); INSERT INTO history VALUES(1,'claude',NULL,NULL,'anonymous legacy prompt',1);").unwrap();
    drop(legacy);
    let restored = fresh();
    crate::import::import_history(&restored, &path, false).unwrap();
    assert_eq!(
        identities(&restored),
        vec![(
            "claude".into(),
            None,
            None,
            "anonymous legacy prompt".into(),
            1
        )]
    );
    crate::import::import_history(&restored, &path, false).unwrap();
    assert_eq!(identities(&restored).len(), 1);
}
