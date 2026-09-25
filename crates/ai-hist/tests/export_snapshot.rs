#![cfg(feature = "export")]
//! Local export snapshots over a store opened the way an embedder opens one.
use ai_hist::export::{
    self, ExportLimits, ExportSelection, HistoryExportPage, HistoryExportRecord, SessionIdentity,
};
use ai_hist::{ChangeOp, ChangeQuery, SessionStore, StoreOptions, Watermark};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

#[allow(clippy::field_reassign_with_default)]
fn store(dir: &Path) -> (SessionStore, Connection) {
    let mut options = StoreOptions::default();
    options.db_path = Some(dir.join("ai-history.db"));
    let store = SessionStore::open(options).unwrap();
    let conn = Connection::open(dir.join("ai-history.db")).unwrap();
    (store, conn)
}

fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn all_sessions(kinds: &[&str]) -> ExportSelection {
    ExportSelection {
        all_sources: true,
        kinds: kinds.iter().map(|kind| kind.to_string()).collect(),
        ..Default::default()
    }
}

fn pages(conn: &Connection, cursor: String) -> Vec<HistoryExportPage> {
    let mut pages = Vec::new();
    let mut cursor = Some(cursor);
    while let Some(value) = cursor {
        let page = export::export_page(conn, &value, now()).unwrap();
        cursor = page.next_cursor.clone();
        pages.push(page);
    }
    pages
}

fn export_all(
    conn: &Connection,
    selection: &ExportSelection,
    limits: &ExportLimits,
) -> Vec<HistoryExportRecord> {
    let handle = export::create_export(conn, selection, limits, 60_000, now()).unwrap();
    let records = pages(conn, handle.cursor)
        .into_iter()
        .flat_map(|page| page.records)
        .collect();
    export::close_export(conn, &handle.snapshot_id).unwrap();
    records
}

fn seed(conn: &Connection) {
    conn.execute_batch(
        r#"
INSERT INTO sessions (source, session_id, first_prompt) VALUES
    ('claude', 'one', 'first'), ('claude', 'two', 'second'), ('codex', 'three', 'third');
INSERT INTO session_events (source, session_id, ts_ms, role, kind, text, event_uid) VALUES
    ('claude', 'one', 1, 'user', 'text', 'hello', 'e1'),
    ('claude', 'two', 2, 'user', 'text', 'there', 'e2'),
    ('codex', 'three', 3, 'user', 'text', 'again', 'e3');
INSERT INTO session_relationships (source, parent_session_id, relationship_uid,
    child_session_id, relationship, identity_status, evidence_kind, child_has_events,
    created_ms, updated_ms)
    VALUES ('claude', 'one', 'r1', 'two', 'delegated', 'observed', 'sidecar', 1, 1, 1);
"#,
    )
    .unwrap();
}

/// A snapshot covers the rows present when it was created, read as they
/// stand when their page is read.
#[test]
fn a_snapshot_covers_the_rows_present_at_creation() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn) = store(dir.path());
    seed(&conn);
    let handle = export::create_export(
        &conn,
        &all_sessions(&["session"]),
        &ExportLimits::default(),
        60_000,
        now(),
    )
    .unwrap();
    conn.execute_batch(
        "INSERT INTO sessions (source, session_id) VALUES ('claude', 'later');
         UPDATE sessions SET first_prompt = 'rewritten' WHERE session_id = 'one';
         DELETE FROM sessions WHERE session_id = 'two';",
    )
    .unwrap();
    let records: Vec<_> = pages(&conn, handle.cursor)
        .into_iter()
        .flat_map(|page| page.records)
        .collect();
    let prompts: BTreeMap<_, _> = records
        .iter()
        .map(|record| {
            (
                record.session_id.clone().unwrap(),
                record.payload["first_prompt"].clone(),
            )
        })
        .collect();
    assert_eq!(
        prompts,
        BTreeMap::from([
            ("one".to_string(), "rewritten".into()),
            ("three".to_string(), "third".into()),
        ])
    );
}

/// A record names the row the way the change feed does: its id is the
/// hash of the feed's key, its revision the feed's revision, its payload
/// the feed's stored row, and its origin the store's epoch.
#[test]
fn a_record_carries_the_change_feeds_identity_and_revision() {
    let dir = tempfile::tempdir().unwrap();
    let (store, conn) = store(dir.path());
    seed(&conn);
    let records = export_all(
        &conn,
        &all_sessions(&["session", "session_event", "relationship"]),
        &ExportLimits::default(),
    );
    assert_eq!(records.len(), 7);
    let head = store.head_revision().unwrap();
    let feed: BTreeMap<String, _> = store
        .changes_since(Watermark::START, ChangeQuery::default())
        .unwrap()
        .map(|change| change.unwrap())
        .filter(|change| matches!(change.op, ChangeOp::Upsert(_)))
        .map(|change| {
            let id = format!(
                "{:x}",
                Sha256::digest(serde_json::to_string(&change.key).unwrap())
            );
            (id, change)
        })
        .collect();
    for record in &records {
        assert_eq!(record.schema_version, export::EXPORT_SCHEMA_VERSION);
        assert_eq!(record.origin_id, format!("{:016x}", head.epoch));
        assert_eq!(record.operation, "upsert");
        let change = feed
            .get(&record.record_id)
            .unwrap_or_else(|| panic!("{} is not a feed key", record.record_id));
        assert_eq!(record.kind, change.kind.as_str());
        assert_eq!(record.revision as u64, change.revision);
        assert_eq!(
            record.payload,
            serde_json::to_value(change.columns.as_ref().unwrap()).unwrap()
        );
        assert!(record.payload.get("revision").is_none());
    }
}

/// Pages are bounded, and a retried cursor returns the page it returned
/// before rather than the next one.
#[test]
fn pages_are_bounded_and_a_retried_cursor_repeats_its_page() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn) = store(dir.path());
    seed(&conn);
    let limits = ExportLimits {
        max_batch_records: 2,
        ..ExportLimits::default()
    };
    let handle = export::create_export(
        &conn,
        &all_sessions(&["session", "session_event"]),
        &limits,
        60_000,
        now(),
    )
    .unwrap();
    let first = export::export_page(&conn, &handle.cursor, now()).unwrap();
    assert_eq!(first.records.len(), 2);
    assert_eq!(
        export::export_page(&conn, &handle.cursor, now()).unwrap(),
        first
    );
    let rest: Vec<_> = pages(&conn, first.next_cursor.clone().unwrap());
    assert!(rest.iter().all(|page| page.records.len() <= 2));
    let total = first.records.len() + rest.iter().map(|p| p.records.len()).sum::<usize>();
    assert_eq!(total, 6);

    let tight = ExportLimits {
        max_batch_bytes: 4096,
        ..ExportLimits::default()
    };
    for page in pages(
        &conn,
        export::create_export(&conn, &all_sessions(&["session"]), &tight, 60_000, now())
            .unwrap()
            .cursor,
    ) {
        assert!(serde_json::to_vec(&page).unwrap().len() <= 4096);
    }
}

/// Sources and sessions select; an excluded session leaves the snapshot,
/// and so does a relationship that names it at either end.
#[test]
fn a_selection_includes_and_excludes_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn) = store(dir.path());
    seed(&conn);
    let identity = |source: &str, session: &str| SessionIdentity {
        source: source.into(),
        session_id: session.into(),
    };
    let sessions = |records: Vec<HistoryExportRecord>| {
        records
            .into_iter()
            .map(|record| (record.kind, record.session_id.unwrap()))
            .collect::<Vec<_>>()
    };

    let by_source = ExportSelection {
        sources: vec!["codex".into()],
        kinds: vec!["session".into(), "session_event".into()],
        ..Default::default()
    };
    assert_eq!(
        sessions(export_all(&conn, &by_source, &ExportLimits::default())),
        [
            ("session_event".to_string(), "three".to_string()),
            ("session".to_string(), "three".to_string())
        ]
    );

    let by_session = ExportSelection {
        sessions: vec![identity("claude", "one")],
        kinds: vec!["session".into(), "relationship".into()],
        ..Default::default()
    };
    assert_eq!(
        sessions(export_all(&conn, &by_session, &ExportLimits::default())),
        [
            ("session".to_string(), "one".to_string()),
            ("relationship".to_string(), "one".to_string())
        ]
    );

    let excluding_child = ExportSelection {
        excluded_sessions: vec![identity("claude", "two")],
        ..all_sessions(&["session", "relationship"])
    };
    assert_eq!(
        sessions(export_all(
            &conn,
            &excluding_child,
            &ExportLimits::default()
        )),
        [
            ("session".to_string(), "one".to_string()),
            ("session".to_string(), "three".to_string())
        ],
        "the relationship names the excluded child, so it leaves with it"
    );
}

/// A snapshot opened by an earlier release has no bounds to read by: it is
/// refused, and closing it releases it.
#[test]
fn a_snapshot_without_bounds_is_refused_and_can_be_closed() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn) = store(dir.path());
    conn.execute(
        "INSERT INTO history_exports(id,selection_json,limits_json,cutoff,expires_at_ms,cursor) \
         VALUES ('old', ?1, ?2, 7, ?3, 'old-cursor')",
        rusqlite::params![
            serde_json::to_string(&all_sessions(&["session"])).unwrap(),
            serde_json::to_string(&ExportLimits::default()).unwrap(),
            now() + 60_000
        ],
    )
    .unwrap();
    let error = export::export_page(&conn, "old-cursor", now()).unwrap_err();
    assert!(error.to_string().contains("earlier release"), "{error:#}");
    export::close_export(&conn, "old").unwrap();
    assert!(export::export_page(&conn, "old-cursor", now()).is_err());
}

/// Expired snapshots stop serving pages and are released oldest first.
#[test]
fn expired_snapshots_are_refused_and_released() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn) = store(dir.path());
    seed(&conn);
    let handle = export::create_export(
        &conn,
        &all_sessions(&["session"]),
        &ExportLimits::default(),
        1_000,
        now(),
    )
    .unwrap();
    let later = handle.expires_at_ms + 1;
    assert!(export::export_page(&conn, &handle.cursor, later).is_err());
    assert_eq!(export::expire_exports(&conn, later, 32).unwrap(), 1);
    let left: i64 = conn
        .query_row(
            "SELECT (SELECT COUNT(*) FROM history_exports) \
                  + (SELECT COUNT(*) FROM history_export_bounds) \
                  + (SELECT COUNT(*) FROM history_export_pages)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(left, 0);
}

/// Exporting needs nothing the upload journal kept: a new store has none of
/// its tables, triggers or indexes.
#[test]
fn a_new_store_exports_without_any_upload_state() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn) = store(dir.path());
    seed(&conn);
    assert_eq!(
        export_all(&conn, &all_sessions(&["session"]), &ExportLimits::default()).len(),
        3
    );
    let upload_objects: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_master \
             WHERE name LIKE 'delivery%' OR name LIKE 'history_subscription%' \
                OR name = 'history_compaction'",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(upload_objects, Vec::<String>::new());
}
