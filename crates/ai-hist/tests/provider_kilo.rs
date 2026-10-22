mod provider_support;
#[cfg(feature = "export")]
use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot, SessionIdentity};
use ai_hist::{
    CatalogQuery, DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionQuery, SessionRef,
    SessionStore, Source, StoreOptions, SyncOptions,
};
use provider_support::{assert_history_conformance, HistoryCase};
const SID: &str = "default:7365735f6b696c6f546f6f6c303032";
fn path(home: &std::path::Path) -> std::path::PathBuf {
    home.join(".local/share/kilo/kilo.db")
}
fn stage(home: &std::path::Path, sql: &str) {
    let path = path(home);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(sql).unwrap();
}
fn open(home: &std::path::Path) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("ledger.db"))
            .roots(ProviderRoots::from_home(
                home.into(),
                home.join("opencode.db"),
            )),
    )
    .unwrap()
}
#[test]
fn kilo_native_cli_7_8_15_public_history() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let evidence = assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::Kilo,
            session_id: SID,
            messages: &[
                (Role::User, "Find the synthetic Kilo needle"),
                (Role::Assistant, "Found the synthetic Kilo needle."),
                (Role::User, "Continue after restart"),
                (Role::Assistant, "Historical Kilo answer persists."),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_calls[0].tool_use_id, "call_kiloNative001");
    assert_eq!(evidence.tool_results.len(), 1);
    assert_eq!(
        evidence.tool_results[0].tool_use_id.as_deref(),
        Some("call_kiloNative001")
    );
    assert!(evidence.requests.iter().all(|r| r.usage.is_none()));
    let oracle: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/kilo/after-resume-official-export.json"
    ))
    .unwrap();
    assert_eq!(oracle["messages"].as_array().unwrap().len(), 4);
    assert_eq!(
        oracle["messages"][1]["parts"][1]["callID"],
        "call_kiloNative001"
    );
}
#[test]
fn kilo_wal_only_append_is_visible_after_reopen() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/before-resume.sql"));
    let store = open(home.path());
    let reference = SessionRef::id(Source::Kilo, SID);
    store
        .discover(DiscoveryOptions::default().sources([Source::Kilo]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let before = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(before.messages.len(), 2);
    drop(store);
    let writer = rusqlite::Connection::open(path(home.path())).unwrap();
    writer.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    let after = rusqlite::Connection::open_in_memory().unwrap();
    after
        .execute_batch(include_str!("fixtures/kilo/schema-7-8-15.sql"))
        .unwrap();
    for table in ["message", "part"] {
        let sql = format!("SELECT * FROM {table} WHERE id LIKE '%Resume%'");
        let mut statement = after.prepare(&sql).unwrap();
        let columns = (0..statement.column_count())
            .map(|i| statement.column_name(i).unwrap().to_owned())
            .collect::<Vec<_>>();
        let rows = statement
            .query_map([], |row| {
                Ok((0..columns.len())
                    .map(|i| row.get::<_, rusqlite::types::Value>(i).unwrap())
                    .collect::<Vec<_>>())
            })
            .unwrap();
        for row in rows {
            let row = row.unwrap();
            let placeholders = vec!["?"; row.len()].join(",");
            writer
                .execute(
                    &format!(
                        "INSERT INTO {table} ({}) VALUES ({placeholders})",
                        columns.join(",")
                    ),
                    rusqlite::params_from_iter(row),
                )
                .unwrap();
        }
    }
    let wal = path(home.path()).with_file_name("kilo.db-wal");
    assert!(wal.exists());
    let reopened = open(home.path());
    reopened
        .discover(DiscoveryOptions::default().sources([Source::Kilo]))
        .unwrap();
    reopened
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let evidence = reopened
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(evidence.messages.len(), 4);
}

#[test]
fn kilo_rewrite_updates_text_retires_tool_and_keeps_unaffected_native_ids() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let store = open(home.path());
    let reference = SessionRef::id(Source::Kilo, SID);
    store
        .discover(DiscoveryOptions::default().sources([Source::Kilo]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let first = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    let user = first
        .messages
        .iter()
        .find(|m| m.message_id.as_deref() == Some("msg_kiloToolUser002"))
        .unwrap();
    let stable_user_uid = user.blocks[0].event_uid.clone();
    assert_eq!(first.tool_calls.len(), 1);
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    native
        .execute(
            "UPDATE part SET data=?1 WHERE id='prt_kiloToolAssistantText002'",
            [serde_json::json!({"type":"text","text":"Rewritten Kilo answer."}).to_string()],
        )
        .unwrap();
    native
        .execute("DELETE FROM part WHERE id='prt_kiloToolPart002'", [])
        .unwrap();
    native.execute("UPDATE session SET title='Renamed Kilo session',time_updated=time_updated+1 WHERE id='ses_kiloTool002'",[]).unwrap();
    drop(native);
    store
        .discover(DiscoveryOptions::default().sources([Source::Kilo]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let changed = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(changed.messages.iter().any(|m| m
        .blocks
        .iter()
        .any(|b| b.text.as_deref() == Some("Rewritten Kilo answer."))));
    assert!(!changed.messages.iter().any(|m| m
        .blocks
        .iter()
        .any(|b| b.text.as_deref() == Some("Found the synthetic Kilo needle."))));
    assert!(changed.tool_calls.is_empty());
    assert!(changed.tool_results.is_empty());
    let user = changed
        .messages
        .iter()
        .find(|m| m.message_id.as_deref() == Some("msg_kiloToolUser002"))
        .unwrap();
    assert_eq!(user.blocks[0].event_uid, stable_user_uid);
    drop(store);
    let reopened = open(home.path());
    let evidence = reopened
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(evidence.tool_calls.is_empty());
}

#[test]
fn kilo_malformed_sibling_preserves_last_good_and_replays_healthy_sibling() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    native.execute("INSERT INTO session(id,project_id,slug,directory,title,version,time_created,time_updated) VALUES('ses_kiloBad','global','bad','/fixture','Bad sibling','7.8.15',1792653000000,1792653001000)",[]).unwrap();
    native.execute("INSERT INTO message(id,session_id,time_created,time_updated,data) VALUES('msg_kiloBad','ses_kiloBad',1792653001000,1792653001000,'{\"role\":\"user\"}')",[]).unwrap();
    native.execute("INSERT INTO part(id,message_id,session_id,time_created,time_updated,data) VALUES('prt_kiloBad','msg_kiloBad','ses_kiloBad',1792653001000,1792653001000,'{\"type\":\"text\",\"text\":\"Old good sibling text\"}')",[]).unwrap();
    drop(native);
    let store = open(home.path());
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let bad = SessionRef::id(Source::Kilo, "default:7365735f6b696c6f426164");
    let good = SessionRef::id(Source::Kilo, SID);
    assert!(store
        .session(&bad, SessionQuery::default())
        .unwrap()
        .unwrap()
        .messages
        .iter()
        .any(|m| m
            .blocks
            .iter()
            .any(|b| b.text.as_deref() == Some("Old good sibling text"))));
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    native
        .execute(
            "UPDATE part SET data='broken-json' WHERE id='prt_kiloBad'",
            [],
        )
        .unwrap();
    native
        .execute(
            "UPDATE part SET data=?1 WHERE id='prt_kiloResumeAssistantText003'",
            [serde_json::json!({"type":"text","text":"Healthy sibling advanced"}).to_string()],
        )
        .unwrap();
    drop(native);
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let retained = store
        .session(&bad, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(retained.messages.iter().any(|m| m
        .blocks
        .iter()
        .any(|b| b.text.as_deref() == Some("Old good sibling text"))));
    let changed = store
        .session(&good, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(changed.messages.iter().any(|m| m
        .blocks
        .iter()
        .any(|b| b.text.as_deref() == Some("Healthy sibling advanced"))));
}

#[test]
fn kilo_native_part_ids_survive_same_timestamp_insert_and_delete() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let store = open(home.path());
    let reference = SessionRef::id(Source::Kilo, SID);
    store
        .discover(DiscoveryOptions::default().sources([Source::Kilo]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let before = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    let assistant = before
        .messages
        .iter()
        .find(|m| m.message_id.as_deref() == Some("msg_kiloToolAssistant002"))
        .unwrap();
    let old_uid = assistant
        .blocks
        .iter()
        .find(|b| b.text.as_deref() == Some("Found the synthetic Kilo needle."))
        .unwrap()
        .event_uid
        .clone();
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    native.execute("INSERT INTO part(id,message_id,session_id,time_created,time_updated,data) VALUES('prt_kiloToolAssistantText001','msg_kiloToolAssistant002','ses_kiloTool002',1792653002000,1792653002000,?1)",[serde_json::json!({"type":"text","text":"Inserted before native part"}).to_string()]).unwrap();
    drop(native);
    store
        .discover(DiscoveryOptions::default().sources([Source::Kilo]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let inserted = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    let assistant = inserted
        .messages
        .iter()
        .find(|m| m.message_id.as_deref() == Some("msg_kiloToolAssistant002"))
        .unwrap();
    assert_eq!(
        assistant.blocks[0].text.as_deref(),
        Some("Inserted before native part")
    );
    assert_eq!(assistant.blocks[1].event_uid, old_uid);
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    native
        .execute(
            "DELETE FROM part WHERE id='prt_kiloToolAssistantText001'",
            [],
        )
        .unwrap();
    drop(native);
    store
        .discover(DiscoveryOptions::default().sources([Source::Kilo]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let restored = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    let assistant = restored
        .messages
        .iter()
        .find(|m| m.message_id.as_deref() == Some("msg_kiloToolAssistant002"))
        .unwrap();
    assert_eq!(assistant.blocks[0].event_uid, old_uid);
    assert!(!assistant
        .blocks
        .iter()
        .any(|b| b.text.as_deref() == Some("Inserted before native part")));
}

#[test]
fn kilo_configured_database_namespace_separates_same_native_id() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let custom = home.path().join("profile/kilo.db");
    std::fs::create_dir_all(custom.parent().unwrap()).unwrap();
    std::fs::copy(path(home.path()), &custom).unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let mut wire = serde_json::to_value(roots).unwrap();
    wire["provider_roots"]["kilo"] = serde_json::json!([path(home.path()), custom]);
    let roots: ProviderRoots = serde_json::from_value(wire).unwrap();
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("ledger.db"))
            .roots(roots),
    )
    .unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Kilo]))
        .unwrap();
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Kilo]);
    let ids: Vec<_> = store
        .sessions(query)
        .map(|s| s.unwrap().session_id)
        .collect();
    assert!(ids.iter().any(|id| id == SID));
    assert!(ids
        .iter()
        .any(|id| id.starts_with("root:") && id.ends_with("7365735f6b696c6f546f6f6c303032")));
}

#[test]
fn kilo_bad_configured_root_does_not_block_healthy_root() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let bad = home.path().join("bad-kilo.db");
    std::fs::create_dir(&bad).unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let mut wire = serde_json::to_value(roots).unwrap();
    wire["provider_roots"]["kilo"] = serde_json::json!([bad, path(home.path())]);
    let roots: ProviderRoots = serde_json::from_value(wire).unwrap();
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("ledger.db"))
            .roots(roots),
    )
    .unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Kilo]))
        .unwrap();
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    assert!(store
        .session(&SessionRef::id(Source::Kilo, SID), SessionQuery::default())
        .unwrap()
        .is_some());
}

#[test]
fn kilo_shallow_first_prompt_joins_native_text_parts_and_preserves_project() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    native.execute("INSERT INTO part(id,message_id,session_id,time_created,time_updated,data) VALUES('prt_kiloToolUserText003','msg_kiloToolUser002','ses_kiloTool002',1792653182039,1792653182039,?1)",[serde_json::json!({"type":"text","text":"Second shallow line"}).to_string()]).unwrap();
    drop(native);
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Kilo]))
        .unwrap();
    let conn = rusqlite::Connection::open(home.path().join("ledger.db")).unwrap();
    let shallow: String = conn
        .query_row(
            "SELECT first_prompt FROM sessions WHERE source='kilo' AND session_id=?1",
            [SID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        shallow,
        "Find the synthetic Kilo needle\nSecond shallow line"
    );
    store
        .hydrate(
            &SessionRef::id(Source::Kilo, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let (prompt,project): (String,Option<String>) = conn.query_row("SELECT prompt,project FROM history WHERE source='kilo' AND session_id=?1 ORDER BY timestamp_ms LIMIT 1",[SID],|row|Ok((row.get(0)?,row.get(1)?))).unwrap();
    assert_eq!(prompt, shallow);
    assert_eq!(project.as_deref(), Some("/private/tmp/rh-kilo-capture"));
    let missing_project: i64 = conn.query_row("SELECT count(*) FROM session_events WHERE source='kilo' AND session_id=?1 AND kind IN ('text','tool_use','tool_result') AND project IS NULL",[SID],|row|row.get(0)).unwrap();
    assert_eq!(missing_project, 0);
}

#[test]
fn kilo_missing_database_retires_only_after_parent_is_confirmed() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let store = open(home.path());
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let reference = SessionRef::id(Source::Kilo, SID);
    assert!(store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .is_some());
    let db = path(home.path());
    let unavailable = db.parent().unwrap().with_file_name("kilo-hidden");
    std::fs::rename(db.parent().unwrap(), &unavailable).unwrap();
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    assert!(store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .is_some());
    std::fs::rename(&unavailable, db.parent().unwrap()).unwrap();
    std::fs::remove_file(&db).unwrap();
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let evidence = store.session(&reference, SessionQuery::default()).unwrap();
    assert!(evidence.is_none() || evidence.unwrap().messages.is_empty());
}

#[test]
fn kilo_missing_primary_with_sidecar_preserves_local_evidence() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let store = open(home.path());
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let db = path(home.path());
    let hidden = db.with_file_name("kilo-hidden.db");
    std::fs::rename(&db, &hidden).unwrap();
    std::fs::write(db.with_file_name("kilo.db-wal"), b"transient").unwrap();
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    assert!(store
        .session(&SessionRef::id(Source::Kilo, SID), SessionQuery::default())
        .unwrap()
        .is_some());
}

#[test]
fn kilo_oversized_part_isolated_from_healthy_sibling() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    native.execute("INSERT INTO session(id,project_id,slug,directory,title,version,time_created,time_updated) VALUES('ses_kiloHuge','global','huge','/fixture','Huge sibling','7.8.15',1792653000000,1792653001000)",[]).unwrap();
    native.execute("INSERT INTO message(id,session_id,time_created,time_updated,data) VALUES('msg_kiloHuge','ses_kiloHuge',1792653001000,1792653001000,'{\"role\":\"user\"}')",[]).unwrap();
    native.execute("INSERT INTO part(id,message_id,session_id,time_created,time_updated,data) VALUES('prt_kiloHuge','msg_kiloHuge','ses_kiloHuge',1792653001000,1792653001000,'{\"type\":\"text\",\"text\":\"old\"}')",[]).unwrap();
    drop(native);
    let store = open(home.path());
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let bad = SessionRef::id(Source::Kilo, "default:7365735f6b696c6f48756765");
    assert!(store
        .session(&bad, SessionQuery::default())
        .unwrap()
        .is_some());
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    native
        .execute(
            "UPDATE part SET data=?1 WHERE id='prt_kiloHuge'",
            ["x".repeat(1_048_577)],
        )
        .unwrap();
    native
        .execute(
            "UPDATE part SET data=?1 WHERE id='prt_kiloResumeAssistantText003'",
            [serde_json::json!({"type":"text","text":"Healthy after huge sibling"}).to_string()],
        )
        .unwrap();
    drop(native);
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    assert!(store
        .session(&bad, SessionQuery::default())
        .unwrap()
        .is_some());
    let good = store
        .session(&SessionRef::id(Source::Kilo, SID), SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(good.messages.iter().any(|m| m
        .blocks
        .iter()
        .any(|b| b.text.as_deref() == Some("Healthy after huge sibling"))));
}

#[test]
fn kilo_oversized_session_metadata_isolated_from_healthy_sibling() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    native.execute("INSERT INTO session(id,project_id,slug,directory,title,version,time_created,time_updated) VALUES('ses_kiloHugeMeta','global','huge','/fixture','Earlier title','7.8.15',1792653000000,1792653001000)",[]).unwrap();
    drop(native);
    let store = open(home.path());
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let bad = SessionRef::id(Source::Kilo, "default:7365735f6b696c6f487567654d657461");
    assert!(store
        .session(&bad, SessionQuery::default())
        .unwrap()
        .is_some());
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    native
        .execute(
            "UPDATE session SET title=?1 WHERE id='ses_kiloHugeMeta'",
            ["x".repeat(20_000)],
        )
        .unwrap();
    native
        .execute(
            "UPDATE part SET data=?1 WHERE id='prt_kiloResumeAssistantText003'",
            [serde_json::json!({"type":"text","text":"Healthy after huge metadata"}).to_string()],
        )
        .unwrap();
    drop(native);
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    assert!(store
        .session(&bad, SessionQuery::default())
        .unwrap()
        .is_some());
    let good = store
        .session(&SessionRef::id(Source::Kilo, SID), SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(good.messages.iter().any(|m| m
        .blocks
        .iter()
        .any(|b| b.text.as_deref() == Some("Healthy after huge metadata"))));
}

#[test]
fn kilo_multitext_prompt_reorders_and_retires_by_native_part_id() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    native.execute("INSERT INTO part(id,message_id,session_id,time_created,time_updated,data) VALUES('prt_kiloToolUserText003','msg_kiloToolUser002','ses_kiloTool002',1792653001000,1792653001000,?1)",[serde_json::json!({"type":"text","text":"Second prompt line"}).to_string()]).unwrap();
    drop(native);
    let store = open(home.path());
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let prompts = || {
        let ledger = rusqlite::Connection::open(home.path().join("ledger.db")).unwrap();
        let mut stmt = ledger.prepare("SELECT prompt FROM history WHERE source='kilo' AND session_id=?1 ORDER BY timestamp_ms").unwrap();
        stmt.query_map([SID], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    assert!(prompts().contains(&"Find the synthetic Kilo needle\nSecond prompt line".to_string()));
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    native
        .execute("DELETE FROM part WHERE id='prt_kiloToolUserText003'", [])
        .unwrap();
    native.execute("INSERT INTO part(id,message_id,session_id,time_created,time_updated,data) VALUES('prt_kiloToolUserText000','msg_kiloToolUser002','ses_kiloTool002',1792653001000,1792653001000,?1)",[serde_json::json!({"type":"text","text":"First prompt line"}).to_string()]).unwrap();
    drop(native);
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let current = prompts();
    assert!(current.contains(&"First prompt line\nFind the synthetic Kilo needle".to_string()));
    assert!(!current.contains(&"Find the synthetic Kilo needle\nSecond prompt line".to_string()));
}

#[cfg(feature = "export")]
#[test]
fn kilo_local_retirement_preserves_prompt_supported_by_remote_share() {
    use ai_hist::{
        EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey, SessionLocation,
        ShallowSession, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch,
    };
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let add_large_parts = |conn: &rusqlite::Connection| {
        let data = serde_json::json!({"type":"text","text":format!("Remote supported second line {}", "\\".repeat(300_000))}).to_string();
        for index in 3..12 {
            let id = format!("prt_kiloToolUserText{index:03}");
            conn.execute("INSERT INTO part(id,message_id,session_id,time_created,time_updated,data) VALUES(?1,'msg_kiloToolUser002','ses_kiloTool002',1792653182039,1792653182039,?2)", rusqlite::params![id,data]).unwrap();
        }
    };
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    add_large_parts(&native);
    drop(native);
    let store = open(home.path());
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let reference = SessionRef::id(Source::Kilo, SID);
    let ledger = home.path().join("ledger.db");
    let expected_prompt: String = rusqlite::Connection::open(&ledger).unwrap().query_row("SELECT prompt FROM history WHERE source='kilo' AND session_id=?1 ORDER BY timestamp_ms LIMIT 1", [SID], |row| row.get(0)).unwrap();
    assert!(expected_prompt.contains("[Native Kilo text projection]"));
    assert!(expected_prompt.contains("\"input_basis\":\"normalized_parts\""));
    let mut export = ExportSnapshot::open(
        rusqlite::Connection::open(&ledger).unwrap(),
        &ExportSelection {
            sessions: vec![SessionIdentity {
                source: "kilo".into(),
                session_id: SID.into(),
            }],
            kinds: vec!["history".into(), "session_event".into()],
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(export.handle().cursor);
    let mut records = Vec::new();
    while let Some(value) = cursor {
        let page = export.page(&value, 1_000).unwrap();
        cursor = page.next_cursor;
        records.extend(page.records);
    }
    let records: Vec<EvidenceRecord> = records
        .into_iter()
        .map(|record| {
            let mut payload = record.payload.as_object().unwrap().clone();
            payload.remove("id");
            payload.remove("location");
            EvidenceRecord {
                kind: serde_json::from_value(serde_json::json!(record.kind)).unwrap(),
                payload,
                record_id: Some(record.record_id),
                revision_id: Some(record.revision_id),
            }
        })
        .collect();
    assert!(
        records
            .iter()
            .filter(|record| record.kind == EvidenceKind::SessionEvent
                && record
                    .payload
                    .get("message_id")
                    .and_then(serde_json::Value::as_str)
                    == Some("msg_kiloToolUser002"))
            .count()
            >= 2
    );
    let receiver_home = tempfile::tempdir().unwrap();
    stage(
        receiver_home.path(),
        include_str!("fixtures/kilo/schema-7-8-15.sql"),
    );
    let native = rusqlite::Connection::open(path(receiver_home.path())).unwrap();
    add_large_parts(&native);
    drop(native);
    let store = open(receiver_home.path());
    let home = receiver_home;
    let ledger = home.path().join("ledger.db");
    let key = ObservationKey {
        source: "kilo".into(),
        session_id: SID.into(),
        location: SessionLocation::Remote,
        connector_id: "kilo-test".into(),
        connector_instance: "fixture".into(),
    };
    store
        .apply_source_observations(SourceObservationBatch::new(
            "kilo-test",
            "fixture",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "kilo".into(),
                    session_id: SID.into(),
                    source_stamp: Some("remote-1".into()),
                    ..Default::default()
                },
                raw_locator: Some("remote://kilo/multipart".into()),
            }],
        ))
        .unwrap();
    let revision = store.source_observation(&key).unwrap().revision.unwrap();
    let remote_apply = store
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "remote-1".into(),
                    source_bytes: 0,
                    covered_kinds: vec![EvidenceKind::History, EvidenceKind::SessionEvent],
                    records,
                },
            )
            .include_related(false),
        )
        .unwrap();
    assert_eq!(remote_apply.status, "hydrated");
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    std::fs::remove_file(path(home.path())).unwrap();
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let ledger = rusqlite::Connection::open(&ledger).unwrap();
    let count: i64 = ledger
        .query_row(
            "SELECT count(*) FROM history WHERE source='kilo' AND session_id=?1 AND prompt=?2",
            rusqlite::params![SID, expected_prompt],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    let evidence = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(evidence.messages.iter().any(|m| m
        .blocks
        .iter()
        .any(|b| b.text.as_deref() == Some("Find the synthetic Kilo needle"))));
}

#[cfg(feature = "export")]
#[test]
fn kilo_large_decoded_result_searches_and_survives_export_intake() {
    use ai_hist::{
        EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey, SessionLocation,
        ShallowSession, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch,
    };
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    let output = format!("alphaunique\nneedleunique {}", "\"".repeat(300_000));
    let payload = serde_json::json!({"type":"tool","callID":"call_kiloNative001","tool":"read","state":{"status":"completed","input":{"filePath":"/synthetic.txt"},"output":output,"title":"Read synthetic file","metadata":{},"time":{"start":1792653002000_i64,"end":1792653003000_i64}}});
    native
        .execute(
            "UPDATE part SET data=?1 WHERE id='prt_kiloToolPart002'",
            [payload.to_string()],
        )
        .unwrap();
    drop(native);
    let origin = open(home.path());
    origin
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let local = origin
        .session(&SessionRef::id(Source::Kilo, SID), SessionQuery::default())
        .unwrap()
        .unwrap();
    let result = local.tool_results[0].text.as_deref().unwrap();
    assert!(result.starts_with("alphaunique\nneedleunique "));
    assert!(result.contains("[Native Kilo result projection]"));
    assert!(
        serde_json::Value::String(result.to_owned())
            .to_string()
            .len()
            <= 512 * 1024
    );
    let search = |ledger: &std::path::Path, phrase: &str| {
        let conn = rusqlite::Connection::open(ledger).unwrap();
        let indexed: i64 = conn.query_row("SELECT count(*) FROM session_events_fts f JOIN session_events e ON e.id=f.rowid WHERE session_events_fts MATCH ?1 AND e.source='kilo' AND e.session_id=?2 AND e.kind='tool_result'", rusqlite::params![phrase,SID], |row| row.get(0)).unwrap();
        #[cfg(feature = "unstable-internal")]
        {
            let hits = ai_hist::history_search::search_all(
                &conn,
                &[phrase.to_owned()],
                true,
                &ai_hist::QueryFilter {
                    source: Some("kilo".into()),
                    scope: ai_hist::SessionScope::All,
                    ..Default::default()
                },
                ai_hist::history_search::SearchRole::All,
            )
            .unwrap();
            assert_eq!(
                hits.iter()
                    .filter(
                        |hit| hit.session_id.as_deref() == Some(SID) && hit.kind == "tool_result"
                    )
                    .count() as i64,
                indexed
            );
        }
        indexed
    };
    let ledger = home.path().join("ledger.db");
    assert_eq!(search(&ledger, "needleunique"), 1);
    assert_eq!(search(&ledger, "\"alphaunique needleunique\""), 1);
    let mut export = ExportSnapshot::open(
        rusqlite::Connection::open(&ledger).unwrap(),
        &ExportSelection {
            sessions: vec![SessionIdentity {
                source: "kilo".into(),
                session_id: SID.into(),
            }],
            kinds: vec!["session_event".into()],
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let page = export.page(&export.handle().cursor, 1_000).unwrap();
    let record = page
        .records
        .into_iter()
        .find(|record| record.kind == "session_event" && record.payload["kind"] == "tool_result")
        .unwrap();
    let receiver_home = tempfile::tempdir().unwrap();
    let receiver = open(receiver_home.path());
    let key = ObservationKey {
        source: "kilo".into(),
        session_id: SID.into(),
        location: SessionLocation::Remote,
        connector_id: "kilo-result".into(),
        connector_instance: "fixture".into(),
    };
    receiver
        .apply_source_observations(SourceObservationBatch::new(
            "kilo-result",
            "fixture",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "kilo".into(),
                    session_id: SID.into(),
                    source_stamp: Some("result-1".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    let revision = receiver.source_observation(&key).unwrap().revision.unwrap();
    let mut payload = record.payload.as_object().unwrap().clone();
    payload.remove("id");
    payload.remove("location");
    receiver
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "result-1".into(),
                    source_bytes: 0,
                    covered_kinds: vec![EvidenceKind::SessionEvent],
                    records: vec![EvidenceRecord {
                        kind: EvidenceKind::SessionEvent,
                        payload,
                        record_id: Some(record.record_id),
                        revision_id: Some(record.revision_id),
                    }],
                },
            )
            .include_related(false),
        )
        .unwrap();
    let remote = receiver
        .session(&SessionRef::id(Source::Kilo, SID), SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(remote.tool_results[0].text.as_deref(), Some(result));
    let remote_ledger = receiver_home.path().join("ledger.db");
    assert_eq!(search(&remote_ledger, "needleunique"), 1);
    assert_eq!(search(&remote_ledger, "\"alphaunique needleunique\""), 1);
}

#[cfg(feature = "export")]
#[test]
fn kilo_large_tool_arguments_fit_default_export_page() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    let input = serde_json::json!({"query": "\\".repeat(300_000)});
    let payload = serde_json::json!({"type":"tool","callID":"call_kiloNative001","tool":"read","state":{"status":"completed","input":input,"output":"ok","title":"Read synthetic file","metadata":{},"time":{"start":1792653002000_i64,"end":1792653003000_i64}}});
    native
        .execute(
            "UPDATE part SET data=?1 WHERE id='prt_kiloToolPart002'",
            [payload.to_string()],
        )
        .unwrap();
    drop(native);
    let origin = open(home.path());
    origin
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let local = origin
        .session(&SessionRef::id(Source::Kilo, SID), SessionQuery::default())
        .unwrap()
        .unwrap();
    let call = &local.tool_calls[0];
    assert_eq!(call.tool_use_id, "call_kiloNative001");
    assert_eq!(call.name, "read");
    assert_eq!(
        call.args.as_ref().unwrap()["projection"],
        "kilo-tool-arguments-v1"
    );
    assert_eq!(call.args.as_ref().unwrap()["truncated"], true);
    assert!(
        call.args.as_ref().unwrap()["native_payload_hash"]
            .as_str()
            .unwrap()
            .len()
            == 64
    );
    assert!(call.raw_args().unwrap().len() < 32 * 1024);
    let ledger = home.path().join("ledger.db");
    let mut export = ExportSnapshot::open(
        rusqlite::Connection::open(&ledger).unwrap(),
        &ExportSelection {
            sessions: vec![SessionIdentity {
                source: "kilo".into(),
                session_id: SID.into(),
            }],
            kinds: vec!["tool_call".into(), "session_event".into()],
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let page = export.page(&export.handle().cursor, 1_000).unwrap();
    assert!(page.records.iter().any(|record| record.kind == "tool_call"
        && record.payload["tool_use_id"] == "call_kiloNative001"));
    assert!(page
        .records
        .iter()
        .any(|record| record.kind == "session_event" && record.payload["kind"] == "tool_use"));
}

#[cfg(feature = "export")]
#[test]
fn kilo_large_text_prompt_and_event_fit_default_export_page() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    let text = format!("largeunique\nneedleunique {}", "\\".repeat(300_000));
    let payload = serde_json::json!({"type":"text","text":text});
    native
        .execute(
            "UPDATE part SET data=?1 WHERE id='prt_kiloToolUserText002'",
            [payload.to_string()],
        )
        .unwrap();
    drop(native);
    let origin = open(home.path());
    origin
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let local = origin
        .session(&SessionRef::id(Source::Kilo, SID), SessionQuery::default())
        .unwrap()
        .unwrap();
    let projected = local
        .messages
        .iter()
        .flat_map(|message| &message.blocks)
        .filter_map(|block| block.text.as_deref())
        .find(|text| text.starts_with("largeunique\nneedleunique"))
        .unwrap();
    assert!(projected.contains("[Native Kilo text projection]"));
    assert!(
        serde_json::Value::String(projected.into())
            .to_string()
            .len()
            < 512 * 1024
    );
    let ledger = home.path().join("ledger.db");
    let conn = rusqlite::Connection::open(&ledger).unwrap();
    let prompt: String = conn.query_row("SELECT prompt FROM history WHERE source='kilo' AND session_id=?1 ORDER BY timestamp_ms LIMIT 1", [SID], |row| row.get(0)).unwrap();
    assert_eq!(prompt, projected);
    let mut export = ExportSnapshot::open(
        conn,
        &ExportSelection {
            sessions: vec![SessionIdentity {
                source: "kilo".into(),
                session_id: SID.into(),
            }],
            kinds: vec!["history".into(), "session_event".into()],
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(export.handle().cursor);
    let mut kinds = std::collections::BTreeSet::new();
    while let Some(current) = cursor {
        let page = export.page(&current, 1_000).unwrap();
        for record in &page.records {
            assert!(serde_json::to_vec(record).unwrap().len() < 1_048_576);
            kinds.insert(record.kind.as_str().to_owned());
        }
        cursor = page.next_cursor;
    }
    assert!(kinds.contains("history"));
    assert!(kinds.contains("session_event"));
}

#[test]
fn kilo_pending_tool_rewrite_clears_prior_error_status() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/kilo/schema-7-8-15.sql"));
    let native = rusqlite::Connection::open(path(home.path())).unwrap();
    native.execute("UPDATE part SET data=json_set(data,'$.state.status','error') WHERE id='prt_kiloToolPart002'", []).unwrap();
    let store = open(home.path());
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let reference = SessionRef::id(Source::Kilo, SID);
    let errored = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(errored.tool_calls[0].is_error, Some(true));
    native.execute("UPDATE part SET data=json_remove(json_set(data,'$.state.status','running'),'$.state.output') WHERE id='prt_kiloToolPart002'", []).unwrap();
    store
        .sync(SyncOptions::default().sources([Source::Kilo]))
        .unwrap();
    let pending = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(pending.tool_calls[0].is_error, None);
    assert!(pending.tool_results.is_empty());
}
