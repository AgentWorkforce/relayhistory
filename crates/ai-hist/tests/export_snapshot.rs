#![cfg(feature = "export")]
//! Local export snapshots over a store opened the way an embedder opens one.
use ai_hist::export::{
    self, ExportLimits, ExportSelection, ExportSnapshot, HistoryExportPage, HistoryExportRecord,
    SessionIdentity,
};
use ai_hist::{ChangeOp, ChangeQuery, SessionStore, StoreOptions, Watermark};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn store(dir: &Path) -> (SessionStore, Connection, PathBuf) {
    let db = dir.join("ai-history.db");
    let store = SessionStore::open(StoreOptions::default().db_path(&db)).unwrap();
    let conn = Connection::open(&db).unwrap();
    (store, conn, db)
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

fn open(
    db: &Path,
    selection: &ExportSelection,
    limits: &ExportLimits,
    ttl_ms: i64,
) -> ExportSnapshot {
    ExportSnapshot::open(
        Connection::open(db).unwrap(),
        selection,
        limits,
        ttl_ms,
        now(),
    )
    .unwrap()
}

fn pages(snapshot: &mut ExportSnapshot) -> Vec<HistoryExportPage> {
    let mut pages = Vec::new();
    let mut cursor = Some(snapshot.handle().cursor);
    while let Some(value) = cursor {
        let page = snapshot.page(&value, now()).unwrap();
        cursor = page.next_cursor.clone();
        pages.push(page);
    }
    pages
}

fn records(snapshot: &mut ExportSnapshot) -> Vec<HistoryExportRecord> {
    pages(snapshot)
        .into_iter()
        .flat_map(|page| page.records)
        .collect()
}

fn export_all(
    db: &Path,
    selection: &ExportSelection,
    limits: &ExportLimits,
) -> Vec<HistoryExportRecord> {
    records(&mut open(db, selection, limits, 60_000))
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

fn prompts(records: &[HistoryExportRecord]) -> BTreeMap<String, serde_json::Value> {
    records
        .iter()
        .map(|record| {
            (
                record.session_id.clone().unwrap(),
                record.payload["first_prompt"].clone(),
            )
        })
        .collect()
}

/// The export is the store as it stood when the snapshot opened, whatever
/// is written while it is read: a row rewritten keeps its old values, a row
/// deleted is still exported, a row added is not -- including one that takes
/// the rowid of the deleted row at the top of `sessions`, a table without
/// `AUTOINCREMENT` -- and no row is exported twice.
#[test]
fn writes_after_opening_do_not_change_the_export() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn, db) = store(dir.path());
    seed(&conn);
    let top: i64 = conn
        .query_row("SELECT MAX(rowid) FROM sessions", [], |row| row.get(0))
        .unwrap();
    let limits = ExportLimits {
        max_batch_records: 1,
        ..ExportLimits::default()
    };
    let mut snapshot = open(&db, &all_sessions(&["session"]), &limits, 60_000);
    // One page read before the writes, the rest after.
    let first = snapshot.page(&snapshot.handle().cursor, now()).unwrap();
    conn.execute_batch(
        "UPDATE sessions SET first_prompt = 'rewritten' WHERE session_id = 'three';
         DELETE FROM sessions WHERE session_id = 'three';
         INSERT INTO sessions (source, session_id, first_prompt) VALUES ('claude', 'later', 'new');
         UPDATE sessions SET first_prompt = 'rewritten' WHERE session_id = 'two';",
    )
    .unwrap();
    let reused: i64 = conn
        .query_row(
            "SELECT rowid FROM sessions WHERE session_id = 'later'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reused, top, "the new row took the deleted row's rowid");
    let mut exported = first.records.clone();
    let mut cursor = first.next_cursor.clone();
    while let Some(value) = cursor {
        let page = snapshot.page(&value, now()).unwrap();
        cursor = page.next_cursor.clone();
        exported.extend(page.records);
    }
    assert_eq!(exported.len(), 3, "each row once: {exported:?}");
    assert_eq!(
        prompts(&exported),
        BTreeMap::from([
            ("one".to_string(), "first".into()),
            ("two".to_string(), "second".into()),
            ("three".to_string(), "third".into()),
        ])
    );
    drop(snapshot);
    // A snapshot opened now sees the writes.
    let now_records = export_all(&db, &all_sessions(&["session"]), &ExportLimits::default());
    assert_eq!(
        prompts(&now_records),
        BTreeMap::from([
            ("one".to_string(), "first".into()),
            ("two".to_string(), "rewritten".into()),
            ("later".to_string(), "new".into()),
        ])
    );
}

/// A record names the row the way the change feed does: its id is the
/// hash of the feed's key, its revision the feed's revision, its payload
/// the feed's stored row, and its origin the store's epoch.
#[test]
fn a_record_carries_the_change_feeds_identity_and_revision() {
    let dir = tempfile::tempdir().unwrap();
    let (store, conn, db) = store(dir.path());
    seed(&conn);
    let records = export_all(
        &db,
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

/// Pages are bounded by record count and by serialized size, envelope
/// included, and the cursor of the page just served serves it again rather
/// than the next one. A cursor already advanced past is refused.
#[test]
fn pages_are_bounded_and_a_retried_cursor_repeats_its_page() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn, db) = store(dir.path());
    seed(&conn);
    let limits = ExportLimits {
        max_batch_records: 2,
        ..ExportLimits::default()
    };
    let mut snapshot = open(
        &db,
        &all_sessions(&["session", "session_event"]),
        &limits,
        60_000,
    );
    let start = snapshot.handle().cursor;
    let first = snapshot.page(&start, now()).unwrap();
    assert_eq!(first.records.len(), 2);
    assert_eq!(snapshot.page(&start, now()).unwrap(), first);
    let second_cursor = first.next_cursor.clone().unwrap();
    let second = snapshot.page(&second_cursor, now()).unwrap();
    assert!(
        snapshot.page(&start, now()).is_err(),
        "a cursor advanced past"
    );
    assert_eq!(snapshot.page(&second_cursor, now()).unwrap(), second);
    let mut total = first.records.len() + second.records.len();
    let mut cursor = second.next_cursor.clone();
    while let Some(value) = cursor {
        let page = snapshot.page(&value, now()).unwrap();
        assert!(page.records.len() <= 2);
        total += page.records.len();
        cursor = page.next_cursor.clone();
    }
    assert_eq!(total, 6);

    let record_bytes = serde_json::to_vec(
        &export_all(&db, &all_sessions(&["session"]), &ExportLimits::default())[0],
    )
    .unwrap()
    .len();
    let tight = ExportLimits {
        // Room for the envelope and one record, not two.
        max_batch_bytes: record_bytes + 200,
        ..ExportLimits::default()
    };
    let pages = pages(&mut open(&db, &all_sessions(&["session"]), &tight, 60_000));
    assert!(pages.iter().all(|page| page.records.len() <= 1));
    for page in &pages {
        assert!(serde_json::to_vec(page).unwrap().len() <= tight.max_batch_bytes);
    }
    assert_eq!(pages.iter().map(|p| p.records.len()).sum::<usize>(), 3);
}

/// Sources and sessions select; an excluded session leaves the snapshot,
/// and so does a relationship that names it at either end.
#[test]
fn a_selection_includes_and_excludes_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn, db) = store(dir.path());
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
    let pair = |kind: &str, session: &str| (kind.to_string(), session.to_string());

    let by_source = ExportSelection {
        sources: vec!["codex".into()],
        kinds: vec!["session".into(), "session_event".into()],
        ..Default::default()
    };
    assert_eq!(
        sessions(export_all(&db, &by_source, &ExportLimits::default())),
        [pair("session_event", "three"), pair("session", "three")]
    );

    let by_session = ExportSelection {
        sessions: vec![identity("claude", "one")],
        kinds: vec!["session".into(), "relationship".into()],
        ..Default::default()
    };
    assert_eq!(
        sessions(export_all(&db, &by_session, &ExportLimits::default())),
        [pair("session", "one"), pair("relationship", "one")]
    );

    let excluding_child = ExportSelection {
        excluded_sessions: vec![identity("claude", "two")],
        ..all_sessions(&["session", "relationship"])
    };
    assert_eq!(
        sessions(export_all(&db, &excluding_child, &ExportLimits::default())),
        [pair("session", "one"), pair("session", "three")],
        "the relationship names the excluded child, so it leaves with it"
    );
}

/// An expired snapshot serves nothing, not even the page it served last.
#[test]
fn an_expired_snapshot_serves_no_page() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn, db) = store(dir.path());
    seed(&conn);
    let mut snapshot = open(
        &db,
        &all_sessions(&["session"]),
        &ExportLimits::default(),
        1_000,
    );
    let handle = snapshot.handle();
    snapshot.page(&handle.cursor, now()).unwrap();
    let later = handle.expires_at_ms + 1;
    assert!(snapshot.expired(later));
    let error = snapshot.page(&handle.cursor, later).unwrap_err();
    assert!(error.to_string().contains("expired"), "{error:#}");
}

/// An open snapshot does not hold up the writer, and dropping it ends its
/// transaction.
#[test]
fn an_open_snapshot_never_blocks_a_writer() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn, db) = store(dir.path());
    seed(&conn);
    let mut snapshot = open(
        &db,
        &all_sessions(&["session"]),
        &ExportLimits::default(),
        60_000,
    );
    snapshot.page(&snapshot.handle().cursor, now()).unwrap();
    conn.busy_timeout(std::time::Duration::ZERO).unwrap();
    conn.execute(
        "INSERT INTO sessions (source, session_id) VALUES ('claude', 'during')",
        [],
    )
    .expect("the writer is not blocked by the open snapshot");
    drop(snapshot);
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
}

/// Exporting needs nothing stored: a new store has no export or upload
/// table, trigger or index.
#[test]
fn a_new_store_exports_without_any_stored_state() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn, db) = store(dir.path());
    seed(&conn);
    assert_eq!(
        export_all(&db, &all_sessions(&["session"]), &ExportLimits::default()).len(),
        3
    );
    let stored: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_master \
             WHERE name LIKE 'delivery%' OR name LIKE 'history_export%' \
                OR name LIKE 'history_subscription%' OR name = 'history_compaction'",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(stored, Vec::<String>::new());
}

/// `rows` session events, every `selected_every`-th one in session `picked`
/// and the rest in `other`, each carrying `payload` bytes of text.
fn seed_events(conn: &Connection, rows: usize, selected_every: usize, payload: usize) {
    let tx = conn.unchecked_transaction().unwrap();
    let text = "x".repeat(payload);
    for n in 0..rows {
        let session = if n % selected_every == 0 {
            "picked"
        } else {
            "other"
        };
        tx.execute(
            "INSERT INTO session_events (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', ?1, ?2, 'user', 'text', ?3, ?4)",
            rusqlite::params![session, n as i64, text, format!("e{n}")],
        )
        .unwrap();
    }
    tx.commit().unwrap();
}

/// Every page and the SQLite VM steps the whole export took: a count of rows
/// read that does not depend on the speed of the machine.
fn export_counting(
    db: &Path,
    selection: &ExportSelection,
    limits: &ExportLimits,
) -> (Vec<HistoryExportPage>, u64) {
    let conn = Connection::open(db).unwrap();
    let steps = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let tick = std::sync::Arc::clone(&steps);
    conn.progress_handler(
        1,
        Some(move || {
            tick.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            false
        }),
    );
    let mut snapshot = ExportSnapshot::open(conn, selection, limits, 60_000, now()).unwrap();
    let pages = pages(&mut snapshot);
    (pages, steps.load(std::sync::atomic::Ordering::Relaxed))
}

fn flatten(pages: &[HistoryExportPage]) -> Vec<HistoryExportRecord> {
    pages.iter().flat_map(|page| page.records.clone()).collect()
}

/// A page reads only the rows it serves. With one record per page, a scan
/// budget of 10,000 costs what a budget of one does: rows past the page
/// boundary are not read, decoded and thrown away to be read again by the
/// next page. Eagerly fetching the whole budget made 600 rows cost
/// 600 + 599 + ... + 1 row reads (#308).
#[test]
fn a_page_does_not_read_rows_past_its_own_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn, db) = store(dir.path());
    seed_events(&conn, 600, 1, 10);
    let selection = all_sessions(&["session_event"]);
    let limits = |scan: usize| ExportLimits {
        max_batch_records: 1,
        max_scan_records: scan,
        ..ExportLimits::default()
    };
    let (narrow, narrow_steps) = export_counting(&db, &selection, &limits(1));
    let (wide, wide_steps) = export_counting(&db, &selection, &limits(10_000));
    assert_eq!(flatten(&narrow).len(), 600);
    assert_eq!(flatten(&wide), flatten(&narrow));
    eprintln!("batch 1: scan 1 = {narrow_steps} steps, scan 10000 = {wide_steps} steps");
    assert!(
        wide_steps < narrow_steps * 2,
        "a 10,000-row scan budget cost {wide_steps} steps against {narrow_steps} for one"
    );
}

/// Rows that break a page on bytes are not read ahead either, and every
/// record still arrives exactly once, in order, within the byte limit.
#[test]
fn a_byte_bounded_page_of_large_payloads_reads_only_what_it_serves() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn, db) = store(dir.path());
    seed_events(&conn, 200, 1, 20_000);
    let selection = all_sessions(&["session_event"]);
    let tight = ExportLimits {
        // Two of these records fit, three do not.
        max_batch_bytes: 50_000,
        max_batch_records: 10_000,
        max_scan_records: 10_000,
    };
    let (pages, steps) = export_counting(&db, &selection, &tight);
    assert!(pages
        .iter()
        .all(|page| serde_json::to_vec(page).unwrap().len() <= tight.max_batch_bytes));
    assert!(pages.iter().all(|page| page.records.len() <= 2));
    let (reference, reference_steps) = export_counting(
        &db,
        &selection,
        &ExportLimits {
            max_batch_records: 1,
            max_scan_records: 1,
            ..tight.clone()
        },
    );
    assert_eq!(flatten(&pages), flatten(&reference));
    eprintln!("large payloads: {steps} steps, one row per page {reference_steps}");
    assert!(steps < reference_steps * 2, "{steps} vs {reference_steps}");
}

/// A sparse selection still honours the scan budget -- a page examines at
/// most `max_scan_records` rows, selected or not -- and exports exactly the
/// selected rows whatever the budget.
#[test]
fn a_sparse_selection_keeps_the_scan_budget() {
    let dir = tempfile::tempdir().unwrap();
    let (_store, conn, db) = store(dir.path());
    seed_events(&conn, 1_000, 25, 1_000);
    let selection = ExportSelection {
        sessions: vec![SessionIdentity {
            source: "claude".into(),
            session_id: "picked".into(),
        }],
        kinds: vec!["session_event".into()],
        ..Default::default()
    };
    let mut exported = Vec::new();
    for scan in [1, 7, 100, 10_000] {
        let limits = ExportLimits {
            max_scan_records: scan,
            ..ExportLimits::default()
        };
        let (pages, _) = export_counting(&db, &selection, &limits);
        let records = flatten(&pages);
        assert_eq!(records.len(), 40, "scan {scan}");
        assert!(records
            .iter()
            .all(|record| record.session_id.as_deref() == Some("picked")));
        // Each page examined at most `scan` rows, so walking 1,000 rows took
        // at least 1,000 / scan pages.
        assert!(
            pages.len() >= 1_000 / scan,
            "scan {scan}: {} pages",
            pages.len()
        );
        exported.push(records);
    }
    assert!(exported.windows(2).all(|pair| pair[0] == pair[1]));
}
