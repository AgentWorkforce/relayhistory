use super::*;
use crate::store::open_db;
use crate::{ChangeKind, ChangeQuery, SessionStore, Source, StoreOptions, Watermark};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// A migrated database whose identity row is cleared, as a database is
/// before its first run would have it, so a test chooses the hostname.
fn blank(path: &Path) -> Connection {
    let conn = open_db(path).unwrap();
    conn.execute_batch("DELETE FROM machine_identity; DELETE FROM hostname_aliases;")
        .unwrap();
    conn
}

fn first_run(path: &Path, hostname: &str) -> MachineIdentity {
    ensure_with(&blank(path), hostname, MachineNamePrivacy::Slug).unwrap()
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
fn canonical_hostname_folds_odd_spellings_into_one_dns_label() {
    // Every spelling of one Mac its tools report, and other odd forms.
    for spellings in [
        &[
            "Khaliqs-MacBook-Pro",
            "Khaliqs-MacBook-Pro.local",
            "khaliqs-macbook-pro.LOCAL.",
            "KHALIQS_MACBOOK_PRO",
            "Khaliqs MacBook Pro",
            "  Khaliqs--MacBook__Pro  ",
            "-Khaliqs-MacBook-Pro-",
            "Khaliqs-MacBook-Pro.corp.example.com",
        ][..],
        &[
            "Khaliq's MacBook Pro",
            "Khaliq\u{2019}s MacBook Pro",
            "khaliq-s-macbook-pro",
        ],
        &["build_01", "build-01.corp.example.com", "BUILD 01"],
        &["", "   ", "...", "'", "___", "\u{1F600}"],
    ] {
        let canonical: BTreeSet<String> = spellings
            .iter()
            .map(|raw| canonical_hostname(raw))
            .collect();
        assert_eq!(canonical.len(), 1, "{spellings:?} -> {canonical:?}");
    }
    for (raw, slug) in [
        ("Khaliqs-MacBook-Pro.local", "khaliqs-macbook-pro"),
        ("Khaliq's MacBook Pro", "khaliq-s-macbook-pro"),
        ("khaliqs.macbook.pro", "khaliqs"),
        ("DESKTOP_7F3A__X", "desktop-7f3a-x"),
        ("ñandú-box", "and-box"),
        ("linuyc-desktop\n", "linuyc-desktop"),
        ("ip-10-0-0-1.ec2.internal", "ip-10-0-0-1"),
        ("", UNKNOWN_HOSTNAME),
    ] {
        assert_eq!(canonical_hostname(raw), slug, "{raw:?}");
    }
    let long = canonical_hostname(&"a-".repeat(100));
    assert!(long.len() <= 63 && !long.ends_with('-'), "{long}");
    // Idempotent: a canonical name is its own canonical form.
    for raw in ["Khaliq's MacBook Pro", "build_01.x", "ñandú", ""] {
        let once = canonical_hostname(raw);
        assert_eq!(canonical_hostname(&once), once);
    }
}

#[test]
fn machine_id_is_the_hostname_plus_an_install_suffix() {
    let install = "7f3a9c12-0000-4000-8000-000000000000";
    assert_eq!(
        machine_id(MachineNamePrivacy::Slug, "khaliq-mbp", install),
        "khaliq-mbp-7f3a9c"
    );
    let hashed = machine_id(MachineNamePrivacy::Hashed, "khaliq-mbp", install);
    let (name, suffix) = hashed.rsplit_once('-').unwrap();
    assert!(name.starts_with('h') && name.len() == 13, "{hashed}");
    assert!(!hashed.contains("khaliq"));
    assert_eq!(suffix, "7f3a9c");
}

#[test]
fn first_run_creates_one_identity_with_the_current_scheme() {
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    let identity = store(&path).machine_identity().unwrap().unwrap();
    assert_eq!(identity.id_scheme, MACHINE_ID_SCHEME);
    assert_eq!(identity.name_privacy, DEFAULT_MACHINE_NAME_PRIVACY);
    assert_eq!(identity.machine_name, canonical_hostname(&local_hostname()));
    let [alias] = &identity.hostname_aliases[..] else {
        panic!("{identity:?}");
    };
    assert_eq!(alias.observed, local_hostname().trim());
    assert_eq!(alias.canonical, identity.machine_name);
    assert_eq!(
        identity.machine_id,
        machine_id(
            identity.name_privacy,
            &identity.machine_name,
            &identity.install_id
        )
    );
    assert_ne!(
        identity.machine_id, identity.machine_name,
        "never the hostname alone"
    );
    let install = identity.install_id.split('-').collect::<Vec<_>>();
    assert_eq!(install[2].chars().next(), Some('4'), "{:?}", identity);
}

#[test]
fn two_machines_with_one_hostname_get_two_ids() {
    let dir = tempfile::tempdir().unwrap();
    let laptop = |name: &str| first_run(&db(&dir, name), "MacBook-Pro.local");
    let (one, two) = (laptop("one.db"), laptop("two.db"));
    assert_eq!(one.machine_name, two.machine_name);
    assert_ne!(one.machine_id, two.machine_id);
    for identity in [&one, &two] {
        let suffix = identity.machine_id.strip_prefix("macbook-pro-").unwrap();
        assert_eq!(suffix.len(), INSTALL_SUFFIX_HEX_DIGITS);
        assert!(identity.install_id.starts_with(suffix));
    }
}

#[test]
fn hostname_rename_updates_the_name_and_records_an_alias_never_the_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    let before = first_run(&path, "khaliq-mbp");
    let conn = open_db(&path).unwrap();
    // A different privacy on a later run changes nothing about the id either.
    let after = ensure_with(
        &conn,
        "Khaliqs-MacBook-Air.local",
        MachineNamePrivacy::Hashed,
    )
    .unwrap();
    assert_eq!(after.machine_id, before.machine_id);
    assert_eq!(after.install_id, before.install_id);
    assert_eq!(after.name_privacy, MachineNamePrivacy::Slug);
    assert_eq!(after.machine_name, "khaliqs-macbook-air");
    let observed: Vec<_> = after.hostname_aliases.iter().map(|a| &a.observed).collect();
    assert_eq!(observed, ["khaliq-mbp", "Khaliqs-MacBook-Air.local"]);
    assert_eq!(after.hostname_aliases[1].canonical, "khaliqs-macbook-air");
    assert_eq!(read(&conn).unwrap().unwrap(), after);
    // Renaming back reuses the alias rather than adding a third.
    let back = ensure_with(&conn, "khaliq-mbp", MachineNamePrivacy::Slug).unwrap();
    assert_eq!(back.machine_name, "khaliq-mbp");
    assert_eq!(back.machine_id, before.machine_id);
    assert_eq!(back.hostname_aliases.len(), 2);
}

#[test]
fn every_spelling_of_one_hostname_resolves_to_one_machine() {
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    let first = first_run(&path, "Khaliqs-MacBook-Pro.local");
    let conn = open_db(&path).unwrap();
    let spellings = [
        "Khaliqs-MacBook-Pro",
        "KHALIQS_MACBOOK_PRO",
        "khaliqs macbook pro",
    ];
    for spelling in spellings {
        let seen = ensure_with(&conn, spelling, MachineNamePrivacy::Slug).unwrap();
        assert_eq!(seen.machine_id, first.machine_id, "{spelling}");
        assert_eq!(seen.machine_name, "khaliqs-macbook-pro");
        assert_eq!(
            seen.updated_ms, first.updated_ms,
            "no rename for a respelling"
        );
    }
    let stored = read(&conn).unwrap().unwrap();
    assert_eq!(stored.hostname_aliases.len(), 1 + spellings.len());
    assert!(stored
        .hostname_aliases
        .iter()
        .all(|alias| alias.canonical == "khaliqs-macbook-pro"));
    // A spelling already recorded is not a write.
    let changes = || -> i64 {
        conn.query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap()
    };
    let before = changes();
    ensure_with(&conn, "KHALIQS_MACBOOK_PRO", MachineNamePrivacy::Slug).unwrap();
    assert_eq!(changes(), before);
}

#[test]
fn a_stored_machine_id_never_changes_with_canonicalization() {
    // An id written under another rule keeps its value and its references:
    // canonicalization only names, it never re-keys.
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    let conn = blank(&path);
    conn.execute(
        "INSERT INTO machine_identity (singleton, machine_id, machine_name, install_id, \
         name_rule_version, id_scheme, name_privacy, created_ms, updated_ms) \
         VALUES (1, 'Khaliqs_MBP-abc', 'Khaliqs_MBP', 'abc', 0, 'earlier', 'slug', 1, 1)",
        [],
    )
    .unwrap();
    let seen = ensure_with(&conn, "Khaliqs_MBP", MachineNamePrivacy::Hashed).unwrap();
    assert_eq!(seen.machine_id, "Khaliqs_MBP-abc");
    assert_eq!(seen.install_id, "abc");
    assert_eq!(seen.id_scheme, "earlier");
    assert_eq!(seen.machine_name, "khaliqs-mbp");
}

#[test]
fn names_record_their_rule_and_an_older_rule_is_recanonicalized() {
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    let first = first_run(&path, "Khaliqs_MBP.local");
    assert_eq!(first.name_rule_version, CANONICAL_HOSTNAME_RULE_VERSION);
    assert!(first
        .hostname_aliases
        .iter()
        .all(|alias| alias.rule_version == CANONICAL_HOSTNAME_RULE_VERSION));
    let conn = open_db(&path).unwrap();
    ensure_with(&conn, "Old_Laptop", MachineNamePrivacy::Slug).unwrap();
    // What a database written under an earlier rule would hold: every
    // name, the historical spelling's included, at rule 0.
    conn.execute_batch(
        "UPDATE machine_identity SET machine_name = 'Khaliqs_MBP', name_rule_version = 0; \
         UPDATE hostname_aliases SET canonical_hostname = observed_hostname, rule_version = 0;",
    )
    .unwrap();
    let firsts: Vec<i64> = read(&conn)
        .unwrap()
        .unwrap()
        .hostname_aliases
        .iter()
        .map(|alias| alias.first_seen_ms)
        .collect();
    let seen = ensure_with(&conn, "Khaliqs_MBP.local", MachineNamePrivacy::Slug).unwrap();
    assert_eq!(seen.machine_id, first.machine_id, "the id stays frozen");
    assert_eq!(seen.machine_name, "khaliqs-mbp");
    assert_eq!(seen.name_rule_version, CANONICAL_HOSTNAME_RULE_VERSION);
    let aliases: Vec<_> = seen
        .hostname_aliases
        .iter()
        .map(|alias| {
            (
                alias.observed.as_str(),
                alias.canonical.as_str(),
                alias.rule_version,
            )
        })
        .collect();
    let rule = CANONICAL_HOSTNAME_RULE_VERSION;
    assert_eq!(
        aliases,
        [
            ("Khaliqs_MBP.local", "khaliqs-mbp", rule),
            ("Old_Laptop", "old-laptop", rule)
        ]
    );
    let after: Vec<i64> = seen
        .hostname_aliases
        .iter()
        .map(|a| a.first_seen_ms)
        .collect();
    assert_eq!(after, firsts, "first sightings are kept");
}

#[test]
fn identity_tables_from_an_earlier_shape_are_upgraded_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    let first = store(&path).machine_identity().unwrap().unwrap();
    {
        // The identity tables as an earlier revision created them: no rule
        // versions, and a migration pending for another reason.
        let conn = open_db(&path).unwrap();
        conn.execute_batch(
            "ALTER TABLE machine_identity DROP COLUMN name_rule_version; \
             ALTER TABLE hostname_aliases DROP COLUMN rule_version; \
             DROP TRIGGER change_feed_sessions_insert;",
        )
        .unwrap();
        assert!(!crate::store::schema_is_current(&conn).unwrap());
    }
    let upgraded = store(&path).machine_identity().unwrap().unwrap();
    assert_eq!(upgraded.machine_id, first.machine_id);
    assert_eq!(upgraded.install_id, first.install_id);
    assert_eq!(upgraded.name_rule_version, CANONICAL_HOSTNAME_RULE_VERSION);
    assert!(upgraded
        .hostname_aliases
        .iter()
        .all(|alias| alias.rule_version == CANONICAL_HOSTNAME_RULE_VERSION));
    assert!(crate::store::schema_is_current(&open_db(&path).unwrap()).unwrap());
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
fn a_reimage_is_a_new_install_and_a_new_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    let before = first_run(&path, "khaliq-mbp");
    // A re-image is a new disk: no database, same hostname.
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    let after = first_run(&path, "khaliq-mbp");
    assert_eq!(after.machine_name, before.machine_name);
    assert_ne!(after.install_id, before.install_id);
    assert_ne!(after.machine_id, before.machine_id);
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
        // The shape an older release left: no column, a feed whose
        // triggers and fingerprint of the session row predate it, no marker.
        conn.execute_batch(&format!(
            "DROP TRIGGER change_feed_sessions_insert; DROP TRIGGER change_feed_sessions_update; \
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
fn a_repaired_trace_id_takes_a_revision_of_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let path = db(&dir, "ai-history.db");
    let conn = open_db(&path).unwrap();
    insert_session(&conn, "s1");
    conn.execute("UPDATE sessions SET trace_id = NULL", [])
        .unwrap();
    let cleared = crate::change_feed::read_head(&conn).unwrap();
    conn.execute(
        "UPDATE sessions SET trace_id = 'repaired' WHERE session_id = 's1'",
        [],
    )
    .unwrap();
    let query = ChangeQuery {
        kinds: Some(vec![ChangeKind::Session]),
        ..ChangeQuery::default()
    };
    let changes: Vec<_> = store(&path)
        .changes_since(cleared, query)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        changes.len(),
        1,
        "a consumer past the clear sees the repair"
    );
    let columns = changes[0].columns.as_ref().unwrap();
    assert_eq!(
        columns.get("trace_id").and_then(|value| value.as_str()),
        Some("repaired")
    );
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

/// The trace id is minted in the insert's own stamp: what it costs a new
/// session row, measured against the same insert carrying its own id, which
/// `COALESCE` passes through without evaluating the mint. Run with
/// `--nocapture` to see the numbers.
#[test]
fn minting_a_trace_id_stays_far_inside_the_ingest_budget() {
    const ROWS: usize = 2_000;
    let time = |mint: bool| {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_db(&db(&dir, "ai-history.db")).unwrap();
        conn.execute_batch("BEGIN").unwrap();
        let started = Instant::now();
        for n in 0..ROWS {
            let id = format!("s-{n}");
            conn.execute(
                "INSERT INTO sessions (session_id, source, cwd, trace_id) \
                 VALUES (?1, 'claude', '/p', ?2)",
                rusqlite::params![id, (!mint).then(|| id.clone())],
            )
            .unwrap();
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
