use super::*;
use crate::store::open_db;
use crate::{ChangeKind, ChangeQuery, SessionStore, Source, StoreOptions, Watermark};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Instant;

use MachineIdStrategy::{HostnameSlug as A, HostnameSlugWithInstallSuffix as B};

/// A migrated database whose identity row is cleared, as a database is
/// before its first run would have it, so a test chooses the hostname.
fn blank(path: &Path) -> Connection {
    let conn = open_db(path).unwrap();
    conn.execute_batch("DELETE FROM machine_identity; DELETE FROM machine_aliases;")
        .unwrap();
    conn
}

fn first_run(path: &Path, hostname: &str, strategy: MachineIdStrategy) -> MachineIdentity {
    ensure_with(&blank(path), hostname, strategy, MachineNamePrivacy::Slug).unwrap()
}

fn store(path: &Path) -> SessionStore {
    SessionStore::open(StoreOptions {
        db_path: Some(path.to_path_buf()),
        home: path.parent().map(Path::to_path_buf),
        ..StoreOptions::default()
    })
    .unwrap()
}

fn db(dir: &tempfile::TempDir, name: &str) -> PathBuf {
    dir.path().join(name)
}

fn is_uuid_v7(value: &str) -> bool {
    let groups: Vec<&str> = value.split('-').collect();
    let lengths: Vec<usize> = groups.iter().map(|group| group.len()).collect();
    lengths == [8, 4, 4, 4, 12]
        && value
            .chars()
            .all(|ch| ch == '-' || ch.is_ascii_digit() || ('a'..='f').contains(&ch))
        && groups[2].starts_with('7')
        && groups[3].starts_with(['8', '9', 'a', 'b'])
}

fn uuid_v7_ms(value: &str) -> i64 {
    let hex: String = value.split('-').take(2).collect();
    i64::from_str_radix(&hex, 16).unwrap()
}

#[test]
fn canonical_name_is_a_lowercase_dns_label() {
    for (raw, slug) in [
        ("Khaliqs-MacBook-Pro.local", "khaliqs-macbook-pro"),
        ("Khaliqs-MacBook-Pro.LOCAL.", "khaliqs-macbook-pro"),
        ("Khaliq's MacBook Pro", "khaliqs-macbook-pro"),
        ("Khaliq\u{2019}s MacBook Pro", "khaliqs-macbook-pro"),
        ("build-01.corp.example.com", "build-01"),
        ("  linuyc-desktop\n", "linuyc-desktop"),
        ("DESKTOP_7F3A__X", "desktop-7f3a-x"),
        ("--edge--", "edge"),
        ("", UNKNOWN_MACHINE_NAME),
        ("...", UNKNOWN_MACHINE_NAME),
        ("ñandú", "and"),
    ] {
        assert_eq!(canonical_machine_name(raw), slug, "{raw:?}");
    }
    let long = canonical_machine_name(&"a-".repeat(100));
    assert!(long.len() <= 63 && !long.ends_with('-'), "{long}");
}

#[test]
fn machine_id_is_the_one_strategy_function() {
    let install = "7f3a9c12-0000-4000-8000-000000000000";
    assert_eq!(
        machine_id(A, MachineNamePrivacy::Slug, "khaliq-mbp", install),
        "khaliq-mbp"
    );
    assert_eq!(
        machine_id(B, MachineNamePrivacy::Slug, "khaliq-mbp", install),
        "khaliq-mbp-7f3a9c"
    );
    let hashed = machine_id(A, MachineNamePrivacy::Hashed, "khaliq-mbp", install);
    assert!(hashed.starts_with('h') && hashed.len() == 13, "{hashed}");
    assert!(!hashed.contains("khaliq"));
    // Deterministic, so strategy A keeps its re-image property when hashed.
    assert_eq!(
        hashed,
        machine_id(A, MachineNamePrivacy::Hashed, "khaliq-mbp", "other")
    );
    assert_eq!(
        machine_id(B, MachineNamePrivacy::Hashed, "khaliq-mbp", install),
        format!("{hashed}-7f3a9c")
    );
}

#[test]
fn first_run_creates_one_identity_with_the_default_strategy() {
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    let identity = store(&path).machine_identity().unwrap().unwrap();
    assert_eq!(identity.strategy, DEFAULT_MACHINE_ID_STRATEGY);
    assert_eq!(identity.name_privacy, DEFAULT_MACHINE_NAME_PRIVACY);
    assert_eq!(
        identity.machine_name,
        canonical_machine_name(&local_hostname())
    );
    assert_eq!(identity.aliases, vec![identity.machine_name.clone()]);
    assert_eq!(
        identity.machine_id,
        machine_id(
            identity.strategy,
            identity.name_privacy,
            &identity.machine_name,
            &identity.install_id
        )
    );
    let install = identity.install_id.split('-').collect::<Vec<_>>();
    assert_eq!(install[2].chars().next(), Some('4'), "{:?}", identity);
}

#[test]
fn same_hostname_collides_under_a_and_not_under_b() {
    let dir = tempfile::tempdir().unwrap();
    let laptop = |name: &str, strategy| first_run(&db(&dir, name), "MacBook-Pro.local", strategy);

    let (one, two) = (laptop("a1.db", A), laptop("a2.db", A));
    assert_eq!(one.machine_id, "macbook-pro");
    assert_eq!(one.machine_id, two.machine_id, "A: two machines, one id");
    assert_ne!(one.install_id, two.install_id);

    let (one, two) = (laptop("b1.db", B), laptop("b2.db", B));
    assert_ne!(one.machine_id, two.machine_id, "B: two machines, two ids");
    for identity in [&one, &two] {
        let suffix = identity.machine_id.strip_prefix("macbook-pro-").unwrap();
        assert_eq!(suffix.len(), INSTALL_SUFFIX_HEX_DIGITS);
        assert!(identity.install_id.starts_with(suffix));
    }
}

#[test]
fn hostname_rename_updates_the_name_and_records_an_alias_never_the_id() {
    for strategy in [A, B] {
        let dir = tempfile::tempdir().unwrap();
        let path = db(&dir, "ai-history.db");
        let before = first_run(&path, "khaliq-mbp", strategy);
        let conn = open_db(&path).unwrap();
        let after = ensure_with(
            &conn,
            "Khaliqs-MacBook-Air.local",
            B,
            MachineNamePrivacy::Hashed,
        )
        .unwrap();
        assert_eq!(after.machine_id, before.machine_id, "{strategy:?}");
        assert_eq!(after.install_id, before.install_id);
        assert_eq!(after.strategy, strategy, "a later strategy never re-keys");
        assert_eq!(after.machine_name, "khaliqs-macbook-air");
        assert_eq!(after.aliases, ["khaliq-mbp", "khaliqs-macbook-air"]);
        assert_eq!(read(&conn).unwrap().unwrap(), after);
        // Renaming back reuses the alias rather than adding a third.
        let back = ensure_with(&conn, "khaliq-mbp", strategy, MachineNamePrivacy::Slug).unwrap();
        assert_eq!(back.machine_name, "khaliq-mbp");
        assert_eq!(back.aliases.len(), 2);
    }
}

#[test]
fn identity_is_stable_across_restarts_and_a_current_open_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    let first = store(&path).machine_identity().unwrap().unwrap();
    let changes_before: i64 = open_db(&path)
        .unwrap()
        .query_row("SELECT total_changes()", [], |row| row.get(0))
        .unwrap();
    assert_eq!(changes_before, 0, "a current open is read-only work");
    for _ in 0..3 {
        let reopened = store(&path);
        assert_eq!(reopened.machine_identity().unwrap().unwrap(), first);
    }
    let conn = open_db(&path).unwrap();
    ensure(&conn).unwrap();
    let changes: i64 = conn
        .query_row("SELECT total_changes()", [], |row| row.get(0))
        .unwrap();
    assert_eq!(changes, 0, "an unchanged hostname is not a write");

    let options = StoreOptions {
        db_path: Some(path.clone()),
        ..StoreOptions::default()
    };
    let read_only = SessionStore::open(options.read_only(true)).unwrap();
    assert_eq!(read_only.machine_identity().unwrap().unwrap(), first);
}

#[test]
fn reimage_keeps_the_id_under_a_and_mints_a_new_one_under_b() {
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    for (strategy, same) in [(A, true), (B, false)] {
        let before = first_run(&path, "khaliq-mbp", strategy);
        // A re-image is a new disk: no database, same hostname.
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
        let after = first_run(&path, "khaliq-mbp", strategy);
        assert_ne!(after.install_id, before.install_id);
        assert_eq!(after.machine_id == before.machine_id, same, "{strategy:?}");
    }
}

#[test]
fn identity_never_reaches_for_the_network() {
    // Structural: the module names no transport, process or socket API.
    let source = include_str!("../machine_identity.rs");
    for forbidden in [
        "ureq",
        "reqwest",
        "hyper",
        "std::net",
        "TcpStream",
        "UdpSocket",
        "process::Command",
        "to_socket_addrs",
    ] {
        assert!(!source.contains(forbidden), "{forbidden}");
    }
    // And fast: a hostname lookup that went to DNS or a cloud API would not
    // fit in this budget a hundred times over.
    let dir = tempfile::tempdir().unwrap();
    let conn = open_db(&db(&dir, "ai-history.db")).unwrap();
    let started = Instant::now();
    for _ in 0..100 {
        ensure(&conn).unwrap();
    }
    let per_call = started.elapsed() / 100;
    assert!(per_call.as_millis() < 5, "{per_call:?} per identity read");
}

fn insert_session(conn: &Connection, id: &str) {
    conn.execute(
        "INSERT INTO sessions (session_id, source, cwd) VALUES (?1, 'claude', '/p') \
         ON CONFLICT(session_id, source) DO UPDATE SET cwd = excluded.cwd",
        [id],
    )
    .unwrap();
}

fn trace(conn: &Connection, id: &str) -> Option<String> {
    conn.query_row(
        "SELECT trace_id FROM sessions WHERE source = 'claude' AND session_id = ?1",
        [id],
        |row| row.get(0),
    )
    .unwrap()
}

#[test]
fn a_new_session_gets_a_uuid_v7_trace_id_that_upserts_keep() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open_db(&db(&dir, "ai-history.db")).unwrap();
    let before = now_ms();
    insert_session(&conn, "s1");
    let first = trace(&conn, "s1").unwrap();
    assert!(is_uuid_v7(&first), "{first}");
    let minted = uuid_v7_ms(&first);
    assert!(
        (before - 1..=now_ms() + 1).contains(&minted),
        "{minted} not within [{before}, now]"
    );
    insert_session(&conn, "s1");
    conn.execute(
        "UPDATE sessions SET title = 't' WHERE session_id = 's1'",
        [],
    )
    .unwrap();
    assert_eq!(trace(&conn, "s1").unwrap(), first, "upserts keep it");
    // An explicit trace id -- one an import carries -- is kept as given.
    conn.execute(
        "INSERT INTO sessions (session_id, source, trace_id) VALUES ('s2', 'claude', 'given')",
        [],
    )
    .unwrap();
    assert_eq!(trace(&conn, "s2").unwrap(), "given");

    let ids: BTreeSet<String> = (0..2_000)
        .map(|n| {
            let id = format!("bulk-{n}");
            insert_session(&conn, &id);
            trace(&conn, &id).unwrap()
        })
        .collect();
    assert_eq!(ids.len(), 2_000, "every trace id distinct");
    assert!(ids.iter().all(|id| is_uuid_v7(id)));

    let store = store(&db(&dir, "ai-history.db"));
    assert_eq!(store.trace_id(Source::Claude, "s1").unwrap(), Some(first));
    assert_eq!(store.trace_id(Source::Claude, "missing").unwrap(), None);
}

#[test]
fn a_database_from_before_trace_ids_backfills_every_row_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    let head_before = {
        let conn = open_db(&path).unwrap();
        for n in 0..500 {
            insert_session(&conn, &format!("old-{n}"));
        }
        // The shape an older release left: no column, no trigger, no
        // marker, and a change feed whose fingerprint of the session row
        // predates the column.
        conn.execute_batch(&format!(
            "DROP TRIGGER {TRACE_ID_TRIGGER}; DROP TRIGGER change_feed_sessions_update; \
             ALTER TABLE sessions DROP COLUMN trace_id; \
             DELETE FROM schema_migrations WHERE name = '{TRACE_ID_MIGRATION}';"
        ))
        .unwrap();
        crate::change_feed::init_schema(&conn).unwrap();
        assert!(!crate::store::schema_is_current(&conn).unwrap());
        crate::change_feed::read_head(&conn).unwrap()
    };
    let conn = open_db(&path).unwrap();
    let ids: Vec<String> = conn
        .prepare("SELECT trace_id FROM sessions")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(ids.len(), 500);
    assert!(ids.iter().all(|id| is_uuid_v7(id)));
    assert_eq!(ids.iter().collect::<BTreeSet<_>>().len(), 500);
    assert!(migration_applied(&conn, TRACE_ID_MIGRATION));

    // The feed reports each backfilled row once, above the old head, with
    // its trace id in the stored row.
    let store = store(&path);
    let query = ChangeQuery {
        kinds: Some(vec![ChangeKind::Session]),
        ..ChangeQuery::default()
    };
    let changes: Vec<_> = store
        .changes_since(head_before, query)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(changes.len(), 500);
    for change in &changes {
        let columns = change.columns.as_ref().unwrap();
        let id = columns.get("trace_id").and_then(|value| value.as_str());
        assert!(id.is_some_and(is_uuid_v7), "{change:?}");
    }
}

fn migration_applied(conn: &Connection, name: &str) -> bool {
    crate::store::migration_applied(conn, name).unwrap()
}

#[test]
fn the_change_feed_carries_the_trace_id_from_the_first_report() {
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    let conn = open_db(&path).unwrap();
    insert_session(&conn, "s1");
    let minted = trace(&conn, "s1").unwrap();
    let changes: Vec<_> = store(&path)
        .changes_since(Watermark::START, ChangeQuery::default())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let session: Vec<_> = changes
        .iter()
        .filter(|change| change.kind == ChangeKind::Session)
        .collect();
    assert_eq!(session.len(), 1, "one report, not one per stamp");
    let columns = session[0].columns.as_ref().unwrap();
    assert_eq!(
        columns.get("trace_id").and_then(|value| value.as_str()),
        Some(minted.as_str())
    );
}

#[cfg(feature = "export")]
#[test]
fn the_export_carries_the_trace_id_in_the_session_payload() {
    use crate::export::{ExportLimits, ExportSelection, ExportSnapshot};
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    let conn = open_db(&path).unwrap();
    insert_session(&conn, "s1");
    let minted = trace(&conn, "s1").unwrap();
    let selection = ExportSelection {
        all_sources: true,
        kinds: vec!["session".into()],
        ..ExportSelection::default()
    };
    let mut snapshot = ExportSnapshot::open(
        Connection::open(&path).unwrap(),
        &selection,
        &ExportLimits::default(),
        60_000,
        now_ms(),
    )
    .unwrap();
    let handle_cursor = snapshot.handle().cursor.clone();
    let page = snapshot.page(&handle_cursor, now_ms()).unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].payload["trace_id"], minted.as_str());
}

/// The trace id is minted in the insert's own statement: what it costs a
/// new session row, measured against the same insert with the trigger gone.
/// Run with `--nocapture` to see the numbers.
#[test]
fn minting_a_trace_id_stays_far_inside_the_ingest_budget() {
    const ROWS: usize = 2_000;
    let time = |with_trigger: bool| {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_db(&db(&dir, "ai-history.db")).unwrap();
        if !with_trigger {
            conn.execute_batch(&format!("DROP TRIGGER {TRACE_ID_TRIGGER};"))
                .unwrap();
        }
        conn.execute_batch("BEGIN").unwrap();
        let started = Instant::now();
        for n in 0..ROWS {
            insert_session(&conn, &format!("s-{n}"));
        }
        let elapsed = started.elapsed();
        conn.execute_batch("COMMIT").unwrap();
        elapsed / u32::try_from(ROWS).unwrap()
    };
    let best = |with_trigger| (0..3).map(|_| time(with_trigger)).min().unwrap();
    let (without, with) = (best(false), best(true));
    eprintln!("new session insert: {without:?} without trace id, {with:?} with");
    assert!(with.as_millis() < 5, "{with:?} per new session");
}
