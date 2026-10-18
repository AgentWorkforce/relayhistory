//! Review regressions: captured OpenClaw history never retires on
//! incomplete evidence (a failed import, a scoped sweep, a partial rewrite,
//! an unreadable store), and discovery stays bounded.
use super::tests_legacy::*;
use super::*;
use crate::ingest::path_scope::PathScope;

// Review regressions: captured history never retires on incomplete evidence.

fn history(ledger: &Ledger, id: &str) -> i64 {
    ledger.count(&format!(
        "SELECT count(*) FROM history WHERE session_id='{id}'"
    ))
}
fn events(ledger: &Ledger, id: &str) -> i64 {
    ledger.count(&format!(
        "SELECT count(*) FROM session_events WHERE session_id='{id}'"
    ))
}
/// The doctor's archive move of one imported transcript.
fn archive(home: &Path, dir: &Path, name: &str) {
    let archive = home.join(".openclaw/agents/main/session-sqlite-import-archive");
    std::fs::create_dir_all(&archive).unwrap();
    std::fs::rename(dir.join(name), archive.join(format!("{name}.imported-1"))).unwrap();
}

#[test]
fn review_a_failed_database_import_keeps_its_jsonl_history_after_the_archive_move() {
    let (home, roots) = home();
    let dir = legacy(home.path());
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None);
    let before = events(&ledger, TELEGRAM);
    // The database holds the session, but one of its rows cannot be decoded.
    let (_db, conn) = database(home.path(), SCHEMA24);
    conn.execute_batch(
        "UPDATE transcript_events SET event_json='{not json' WHERE session_id='fixture-telegram-0001' AND seq=1",
    )
    .unwrap();
    assert!(ledger.sweep(&roots, None).unread >= 1);
    archive(home.path(), &dir, "fixture-telegram-0001.jsonl");
    ledger.sweep(&roots, None);
    assert_eq!(history(&ledger, TELEGRAM), 1);
    assert_eq!(events(&ledger, TELEGRAM), before);
    assert!(crate::session_locations(&ledger.conn, ID, TELEGRAM)
        .unwrap()
        .contains(&"local".into()));
}

#[test]
fn review_a_scoped_sweep_of_the_sessions_directory_never_retires_an_imported_session() {
    let (home, roots) = home();
    let dir = legacy(home.path());
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None);
    let before = events(&ledger, TELEGRAM);
    // Imported and archived between sweeps; only the sessions watch fires.
    let (db, _conn) = database(home.path(), SCHEMA24);
    archive(home.path(), &dir, "fixture-telegram-0001.jsonl");
    let scope = PathScope::from_paths([dir.clone()]).unwrap();
    ledger.sweep(&roots, Some(&scope));
    assert_eq!(history(&ledger, TELEGRAM), 1);
    assert_eq!(events(&ledger, TELEGRAM), before);
    // The row follows the database, which then re-reads it.
    assert_eq!(ledger.raw_path(TELEGRAM), Some(db.to_string_lossy().into()));
    ledger.sweep(&roots, None);
    assert_eq!(history(&ledger, TELEGRAM), 1);
}

#[test]
fn review_a_partial_rewrite_never_retires_indexed_evidence() {
    let (home, roots) = home();
    let dir = legacy(home.path());
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None);
    let before = events(&ledger, TELEGRAM);
    let path = dir.join("fixture-telegram-0001.jsonl");
    let text = std::fs::read_to_string(&path).unwrap();
    let mut lines = text.lines();
    let header = lines.next().unwrap();
    let message = lines.next().unwrap();
    // Mid-rewrite: the header, then half of the first message.
    std::fs::write(&path, format!("{header}\n{}", &message[..40])).unwrap();
    assert_eq!(ledger.sweep(&roots, None).unread, 1);
    assert_eq!(events(&ledger, TELEGRAM), before);
    assert_eq!(history(&ledger, TELEGRAM), 1);
    // A selected capture of the partial file is equally non-destructive.
    let acquired = storage::capture(&context(&roots), &path, TELEGRAM).unwrap();
    let s: Session = serde_json::from_value(acquired.payload).unwrap();
    assert!(s.partial);
    let tx = ledger.conn.unchecked_transaction().unwrap();
    legacy::complete_or_append(&tx, &s, &path).unwrap();
    tx.commit().unwrap();
    assert_eq!(events(&ledger, TELEGRAM), before);
    // Once complete again, the transcript is authoritative.
    std::fs::write(&path, text).unwrap();
    assert_eq!(ledger.sweep(&roots, None).unread, 0);
    assert_eq!(events(&ledger, TELEGRAM), before);
}

#[test]
fn review_an_unreadable_database_protects_jsonl_rows_after_its_sessions_directory_is_gone() {
    let (home, roots) = home();
    let dir = legacy(home.path());
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None);
    let (_db, conn) = database(home.path(), SCHEMA24);
    conn.execute_batch("UPDATE schema_meta SET schema_version=32; PRAGMA user_version=32")
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    ledger.sweep(&roots, None);
    assert_eq!(history(&ledger, MAIN), 1);
    assert_eq!(history(&ledger, TELEGRAM), 1);
    // The other agent, whose store was readable, is unaffected either way.
    assert_eq!(history(&ledger, "ops:fixture-relay-0001"), 1);
}

#[test]
fn review_bounded_discovery_never_offers_a_database_held_session_from_jsonl() {
    let (home, roots) = home();
    let dir = legacy(home.path());
    let (_db, conn) = database(home.path(), SCHEMA24);
    // Two newer database sessions fill a limit of one plus its look-ahead.
    for id in ["zz-new-1", "zz-new-2"] {
        conn.execute_batch(&format!(
            "PRAGMA foreign_keys=OFF; \
             CREATE TEMP TABLE w AS SELECT * FROM session_windows WHERE session_id='fixture/main:α'; \
             UPDATE w SET session_id='{id}'; INSERT INTO session_windows SELECT * FROM w; DROP TABLE w; \
             CREATE TEMP TABLE e AS SELECT * FROM transcript_events WHERE session_id='fixture/main:α'; \
             UPDATE e SET session_id='{id}', created_at=created_at+100000000; \
             INSERT INTO transcript_events SELECT * FROM e; DROP TABLE e;"
        ))
        .unwrap();
    }
    // The pre-import transcript looks newest of all.
    let jsonl = dir.join("fixture-telegram-0001.jsonl");
    std::fs::File::options()
        .write(true)
        .open(&jsonl)
        .unwrap()
        .set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(86_400))
        .unwrap();
    for limit in [1, 2, 3] {
        let found = storage::enumerate(&roots, Some(limit)).unwrap();
        assert!(
            found.iter().all(
                |c| c.session_id.as_deref() != Some(TELEGRAM) || !c.locator.ends_with(".jsonl")
            ),
            "limit {limit}: {:?}",
            found.iter().map(|c| &c.locator).collect::<Vec<_>>()
        );
    }
}

#[test]
fn review_shallow_discovery_parses_each_index_once() {
    let (home, roots) = home();
    legacy(home.path());
    let catalog = Connection::open_in_memory().unwrap();
    let env = DiscoveryEnv::with_provider_roots(&catalog, roots.clone());
    let candidates = storage::enumerate(&roots, None).unwrap();
    let scan = env.scan();
    legacy::reset_index_parses();
    for c in candidates.iter().filter(|c| c.locator.contains("/main/")) {
        assert!(storage::shallow(&scan, c).unwrap().is_some());
    }
    assert_eq!(legacy::index_parses(), 1);
    // A rewritten index is parsed again.
    let index = home
        .path()
        .join(".openclaw/agents/main/sessions/sessions.json");
    let text = std::fs::read_to_string(&index).unwrap();
    std::fs::write(&index, text.replace("Fixture main session", "Renamed")).unwrap();
    let main = candidates
        .iter()
        .find(|c| c.session_id.as_deref() == Some(MAIN))
        .unwrap();
    let shallow = storage::shallow(&scan, main).unwrap().unwrap();
    assert_eq!(shallow.title.as_deref(), Some("Renamed"));
    assert_eq!(legacy::index_parses(), 2);
}

#[test]
fn review_an_oversized_transcript_is_unread_and_keeps_its_history() {
    let (home, roots) = home();
    let dir = legacy(home.path());
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None);
    let before = events(&ledger, MAIN);
    let path = dir.join("fixture-main-0001.jsonl");
    let mut text = std::fs::read_to_string(&path).unwrap();
    let filler = r#"{"type":"custom","id":"pad-N","parentId":null,"timestamp":"2026-09-21T14:14:00.000Z","customType":"x","data":"PAD"}"#
        .replace("PAD", &"x".repeat(1024));
    for i in 0..(legacy::MAX_TRANSCRIPT_BYTES / 1024) {
        text.push_str(&filler.replace("pad-N", &format!("pad-{i}")));
        text.push('\n');
    }
    std::fs::write(&path, text).unwrap();
    assert_eq!(ledger.sweep(&roots, None).unread, 1);
    assert_eq!(events(&ledger, MAIN), before);
    assert!(format!(
        "{:#}",
        storage::capture(&context(&roots), &path, MAIN).unwrap_err()
    )
    .contains("exceeds"));
}

#[test]
fn review_one_unlistable_sessions_directory_does_not_fail_the_sweep() {
    let (home, roots) = home();
    legacy(home.path());
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None);
    // The ops agent's sessions directory now escapes the root.
    let ops = home.path().join(".openclaw/agents/ops/sessions");
    let outside = home.path().join("outside");
    std::fs::rename(&ops, &outside).unwrap();
    std::os::unix::fs::symlink(&outside, &ops).unwrap();
    let swept = ledger.sweep(&roots, None);
    assert!(swept.unread >= 1);
    // Its rows and stamps survive; the main agent still syncs.
    assert_eq!(history(&ledger, "ops:fixture-relay-0001"), 1);
    assert!(ledger.state["openclaw_legacy_stamps"]
        .as_object()
        .unwrap()
        .keys()
        .any(|k| k.contains("/ops/")));
    assert_eq!(history(&ledger, MAIN), 1);
}
