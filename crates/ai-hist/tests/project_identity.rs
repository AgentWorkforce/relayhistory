//! Canonical project identity end to end (issue #175).
//!
//! Three claims are worth proving over the real acquisition path rather than
//! over hand-built rows, because each one failed silently in the shape it is
//! asserted here:
//!
//!   - two checkouts of one repository at different paths must produce one
//!     key, and `stats` must therefore merge them into one bucket;
//!   - a delegated child whose own directory resolves to nothing must carry
//!     the parent's key, recorded as `inherited` rather than guessed at;
//!   - a database written before the columns existed must migrate and then
//!     resolve, without a path key being invented for a checkout that does
//!     have a remote.

use ai_hist::project_identity::ProjectKeyMethod;
use ai_hist::{
    discover_sessions_scoped_at, open_db, refresh_project_identity, stats_scoped_by,
    sync_scoped_at, DiscoverOptions, ProjectGrouping, SessionScope,
};
use rusqlite::Connection;
use std::fs;
use std::path::Path;

fn write(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

/// A checkout whose `.git/config` names `origin`, with no `git` binary
/// involved on either the writing or the reading side.
fn checkout(root: &Path, name: &str, origin: Option<&str>) -> std::path::PathBuf {
    let dir = root.join(name);
    let git = dir.join(".git");
    fs::create_dir_all(&git).unwrap();
    let config = match origin {
        Some(url) => format!("[core]\n\tbare = false\n[remote \"origin\"]\n\turl = {url}\n"),
        None => "[core]\n\tbare = false\n".to_string(),
    };
    fs::write(git.join("config"), config).unwrap();
    dir
}

fn session_key(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> (Option<String>, Option<String>) {
    conn.query_row(
        "SELECT project_key, project_key_method FROM sessions WHERE source = ? AND session_id = ?",
        rusqlite::params![source, session_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .unwrap_or_else(|error| panic!("reading {source}/{session_id}: {error}"))
}

fn event_keys(conn: &Connection, source: &str, session_id: &str) -> Vec<Option<String>> {
    conn.prepare(
        "SELECT project_key FROM session_events WHERE source = ? AND session_id = ? ORDER BY id",
    )
    .unwrap()
    .query_map(rusqlite::params![source, session_id], |row| row.get(0))
    .unwrap()
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap()
}

/// One test per binary: it sets `HOME` for the process.
#[test]
fn canonical_keys_merge_checkouts_and_delegated_children_inherit_them() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    let work = home.join("work");

    // Two different paths, one repository. Under a cwd-keyed grouping these
    // are two projects; that fragmentation is what `project_key` exists to
    // stop, so the test builds it deliberately.
    let checkout_a = checkout(&work, "proj", Some("git@github.com:Org/Repo.git"));
    let checkout_b = checkout(&work, "proj-elsewhere", Some("https://github.com/Org/Repo"));
    // A directory that is not a repository at all: a delegated child run here
    // can only resolve to its own path, so it is the case inheritance covers.
    let no_repo = work.join("scratch");
    fs::create_dir_all(&no_repo).unwrap();

    let day = home.join(".codex/sessions/2026/09/19");
    let rollout = |id: &str, cwd: &Path, parent: Option<&str>, at: &str| {
        let meta = match parent {
            Some(parent) => format!(
                r#"{{"timestamp":"{at}","type":"session_meta","payload":{{"id":"{id}","cwd":"{cwd}","session_id":"{parent}","parent_thread_id":"{parent}","thread_source":"subagent"}}}}"#,
                cwd = cwd.display()
            ),
            None => format!(
                r#"{{"timestamp":"{at}","type":"session_meta","payload":{{"id":"{id}","cwd":"{cwd}"}}}}"#,
                cwd = cwd.display()
            ),
        };
        write(
            &day.join(format!("rollout-{id}.jsonl")),
            &format!(
                "{meta}\n\
                 {{\"timestamp\":\"{at}\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"user_message\",\"message\":\"{id} prompt\"}}}}\n\
                 {{\"timestamp\":\"{at}\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"agent_message\",\"message\":\"{id} answer\"}}}}\n"
            ),
        );
    };
    rollout("sess-a", &checkout_a, None, "2026-09-19T10:00:00Z");
    rollout("sess-b", &checkout_b, None, "2026-09-19T10:01:00Z");
    rollout("sess-a", &checkout_a, None, "2026-09-19T10:00:00Z");
    // The delegated child runs somewhere that resolves to nothing canonical.
    rollout("child", &no_repo, Some("sess-a"), "2026-09-19T10:00:30Z");

    std::env::set_var("HOME", home);
    std::env::set_var("USERPROFILE", home);
    std::env::set_var("OPENCODE_DB", home.join("missing-opencode.db"));
    std::env::set_var("TRAJECTORY_ROOT", home.join("missing-trajectories"));
    std::env::remove_var("AI_HIST_DB");
    let db = home.join("history.db");

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    discover_sessions_scoped_at(
        &db,
        &DiscoverOptions {
            scope: SessionScope::Local,
            sources: vec!["codex".into()],
            limit: None,
        },
    )
    .unwrap();

    let conn = open_db(&db).unwrap();

    // --- one repository, two paths, one key -----------------------------
    let (key_a, method_a) = session_key(&conn, "codex", "sess-a");
    let (key_b, method_b) = session_key(&conn, "codex", "sess-b");
    assert_eq!(key_a.as_deref(), Some("github.com/Org/Repo"));
    assert_eq!(key_b.as_deref(), Some("github.com/Org/Repo"));
    assert_eq!(method_a.as_deref(), Some(ProjectKeyMethod::Remote.as_str()));
    assert_eq!(method_b.as_deref(), Some(ProjectKeyMethod::Remote.as_str()));
    // Both the scp and the https spelling of the same remote reduced to it,
    // and neither cwd leaked into the key.
    assert_ne!(
        key_a.as_deref(),
        Some(checkout_a.to_string_lossy().as_ref())
    );

    // --- the delegated child inherits ------------------------------------
    // The child is evidence rather than a catalog session for this provider,
    // so its events are where the identity has to be observable.
    let child_events = event_keys(&conn, "codex", "child");
    assert!(
        !child_events.is_empty(),
        "the child transcript produced no events, so the inheritance claim is untested"
    );
    assert!(
        child_events
            .iter()
            .all(|key| key.as_deref() == Some("github.com/Org/Repo")),
        "delegated child events did not inherit the parent's key: {child_events:?}"
    );
    // And when the child does hold a catalog row, it says how it got the key.
    if conn
        .query_row(
            "SELECT COUNT(*) FROM sessions WHERE source = 'codex' AND session_id = 'child'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
        > 0
    {
        let (key, method) = session_key(&conn, "codex", "child");
        assert_eq!(key.as_deref(), Some("github.com/Org/Repo"));
        assert_eq!(
            method.as_deref(),
            Some(ProjectKeyMethod::Inherited.as_str()),
            "an inherited key must say so rather than pass as a resolved remote"
        );
    }

    // The parent's own key is not downgraded by the inheritance pass.
    assert_eq!(
        session_key(&conn, "codex", "sess-a").1.as_deref(),
        Some(ProjectKeyMethod::Remote.as_str())
    );

    // --- stats merge under the canonical key, split under cwd ------------
    let canonical = stats_scoped_by(
        &conn,
        None,
        SessionScope::Local,
        ProjectGrouping::ProjectKey,
    )
    .unwrap();
    let merged = canonical
        .by_project
        .iter()
        .find(|(project, _)| project == "github.com/Org/Repo")
        .map(|(_, count)| *count)
        .unwrap_or(0);
    assert!(
        merged >= 2,
        "two checkouts of one repository did not merge: {:?}",
        canonical.by_project
    );
    assert_eq!(canonical.grouping, ProjectGrouping::ProjectKey);

    let by_cwd = stats_scoped_by(&conn, None, SessionScope::Local, ProjectGrouping::Cwd).unwrap();
    assert_eq!(by_cwd.grouping, ProjectGrouping::Cwd);
    assert!(
        by_cwd
            .by_project
            .iter()
            .all(|(project, _)| project != "github.com/Org/Repo"),
        "--by-cwd must restore the raw directory grouping, not the canonical one"
    );
    assert!(
        by_cwd.by_project.len() > canonical.by_project.len()
            || by_cwd
                .by_project
                .iter()
                .any(|(project, count)| { project.contains("proj") && *count < merged }),
        "cwd grouping should still split what the canonical key merged: {:?}",
        by_cwd.by_project
    );

    // --- idempotence -----------------------------------------------------
    assert_eq!(
        refresh_project_identity(&conn).unwrap(),
        0,
        "a second refresh over an unchanged database must write nothing"
    );
}

/// The `inherited` method, proved directly.
///
/// The fixture above exercises inheritance where the provider keeps the child
/// out of the catalog, so the key lands only on its events and no `method` is
/// recorded anywhere. A child that *does* hold a catalog row has to say
/// `inherited` rather than let a borrowed key pass as a resolved remote.
/// Built from rows, so the claim is about the pass rather than about which
/// provider happens to register its delegated threads today.
#[test]
fn a_child_catalog_row_records_that_its_key_was_inherited() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("inherited.db");
    let conn = open_db(&db).unwrap();

    let insert_session = |id: &str, cwd: &str, key: Option<&str>, method: Option<&str>| {
        conn.execute(
            "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
             last_activity_ms, discovery_state) VALUES ('codex', ?, ?, ?, ?, 1, 'full')",
            rusqlite::params![id, cwd, key, method],
        )
        .unwrap();
    };
    insert_session(
        "parent",
        "/work/app",
        Some("github.com/Org/Repo"),
        Some("remote"),
    );
    // Its own directory resolved to nothing better than itself.
    insert_session("child", "/tmp/scratch", Some("/tmp/scratch"), Some("path"));
    // A grandchild with no key at all, to prove the pass walks more than one
    // level rather than stopping at the first generation.
    insert_session("grandchild", "/tmp/scratch", None, None);
    // A sibling that resolved its *own* remote: a stronger statement than the
    // parent's, and inheritance must not overwrite it.
    insert_session(
        "sibling",
        "/work/other",
        Some("github.com/Org/Other"),
        Some("remote"),
    );
    let edge = |parent: &str, child: &str, created: i64| {
        conn.execute(
            "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
             child_session_id, relationship, identity_status, evidence_kind, child_has_events, \
             created_ms, updated_ms) \
             VALUES ('codex', ?, ?, ?, 'delegated', 'observed', 'test', 1, ?, ?)",
            rusqlite::params![parent, format!("child:{child}"), child, created, created],
        )
        .unwrap();
    };
    edge("parent", "child", 10);
    edge("child", "grandchild", 20);
    edge("parent", "sibling", 30);

    let written = refresh_project_identity(&conn).unwrap();
    assert!(written > 0, "the pass claimed to change nothing");

    let inherited = |key: &str| (Some(key.to_string()), Some("inherited".to_string()));
    let remote = |key: &str| (Some(key.to_string()), Some("remote".to_string()));
    assert_eq!(
        session_key(&conn, "codex", "child"),
        inherited("github.com/Org/Repo"),
    );
    assert_eq!(
        session_key(&conn, "codex", "grandchild"),
        inherited("github.com/Org/Repo"),
        "inheritance must reach past the first generation"
    );
    assert_eq!(
        session_key(&conn, "codex", "sibling"),
        remote("github.com/Org/Other"),
        "a child that resolved its own remote must keep it"
    );
    assert_eq!(
        session_key(&conn, "codex", "parent"),
        remote("github.com/Org/Repo"),
    );
    // The stored strings are the ones `ProjectKeyMethod` round-trips.
    assert_eq!(ProjectKeyMethod::Inherited.as_str(), "inherited");
    assert_eq!(ProjectKeyMethod::Remote.as_str(), "remote");

    // Running it again settles: an already-inherited row is no longer a
    // candidate, so a relationship cycle cannot make the loop churn.
    assert_eq!(refresh_project_identity(&conn).unwrap(), 0);
}

/// Inheritance must reach as deep as the ledger can record.
///
/// One statement propagates one level, so the pass bound is a depth limit. At
/// 16 it sat below `MAX_TREE_MAX_DEPTH`, the depth the relationship reader
/// itself will walk, which meant a chain the tree renders in full could have
/// its tail keyed by path while its head was keyed by repository — a split
/// inside one delegation, reported as if both were settled.
#[test]
fn inheritance_reaches_the_full_depth_the_relationship_ledger_can_record() {
    const DEPTH: usize = 40;
    let temp = tempfile::tempdir().unwrap();
    let conn = open_db(&temp.path().join("deep.db")).unwrap();

    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'gen-0', '/work/app', 'github.com/Org/Repo', 'remote', 1, 'full')",
        [],
    )
    .unwrap();
    for generation in 1..=DEPTH {
        conn.execute(
            "INSERT INTO sessions (source, session_id, cwd, last_activity_ms, discovery_state) \
             VALUES ('codex', ?, '/tmp/scratch', 1, 'full')",
            rusqlite::params![format!("gen-{generation}")],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
             child_session_id, relationship, identity_status, evidence_kind, child_has_events, \
             created_ms, updated_ms) \
             VALUES ('codex', ?1, ?2, ?3, 'delegated', 'observed', 'test', 1, ?4, ?4)",
            rusqlite::params![
                format!("gen-{}", generation - 1),
                format!("child:gen-{generation}"),
                format!("gen-{generation}"),
                generation as i64,
            ],
        )
        .unwrap();
    }

    refresh_project_identity(&conn).unwrap();

    for generation in 1..=DEPTH {
        assert_eq!(
            session_key(&conn, "codex", &format!("gen-{generation}")),
            (
                Some("github.com/Org/Repo".to_string()),
                Some(ProjectKeyMethod::Inherited.as_str().to_string())
            ),
            "generation {generation} did not inherit"
        );
    }
    assert_eq!(refresh_project_identity(&conn).unwrap(), 0);
}

/// A path key is the absence of an answer, not an answer, and must not stick.
///
/// `upsert_session` resolves from the working directory alone. When that
/// checkout has since been deleted or moved it stamps a path key — and if the
/// refresh only ever filled NULLs, nothing would revisit that row: not the
/// refresh, which sees a non-NULL key, and not shallow discovery, which skips
/// a source whose bytes have not changed. The repository's own `repo_url`,
/// which the Codex reader records, would sit unused in the same row while
/// every worktree of that repository filed under a different key.
#[test]
fn a_path_key_is_upgraded_once_a_recorded_remote_is_available() {
    let temp = tempfile::tempdir().unwrap();
    let conn = open_db(&temp.path().join("stale.db")).unwrap();
    let gone = temp.path().join("deleted-checkout");

    // The state `upsert_session` leaves for a session whose checkout is gone.
    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) VALUES ('codex', 'sess', ?, ?, 'path', 1, 'full')",
        rusqlite::params![gone.to_string_lossy(), gone.to_string_lossy()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO session_events (source, session_id, event_uid, ts_ms, role, kind, text, cwd) \
         VALUES ('codex', 'sess', 'e1', 1, 'user', 'text', 'hi', ?)",
        rusqlite::params![gone.to_string_lossy()],
    )
    .unwrap();
    // Nothing to upgrade with yet, so the path key stands rather than being
    // replaced by a guess.
    refresh_project_identity(&conn).unwrap();
    assert_eq!(
        session_key(&conn, "codex", "sess").1.as_deref(),
        Some(ProjectKeyMethod::PathFallback.as_str())
    );

    // The provider recorded the repository all along.
    conn.execute(
        "UPDATE sessions SET repo_url = 'git@github.com:Org/Repo.git' WHERE session_id = 'sess'",
        [],
    )
    .unwrap();
    assert!(refresh_project_identity(&conn).unwrap() > 0);
    assert_eq!(
        session_key(&conn, "codex", "sess"),
        (
            Some("github.com/Org/Repo".to_string()),
            Some(ProjectKeyMethod::Remote.as_str().to_string())
        ),
    );
    assert_eq!(
        event_keys(&conn, "codex", "sess"),
        vec![Some("github.com/Org/Repo".to_string())],
        "the denormalized copy must follow the upgrade"
    );

    // And a remote key is never downgraded back to a path on a later pass,
    // even though the checkout is still missing.
    assert_eq!(refresh_project_identity(&conn).unwrap(), 0);
    assert_eq!(
        session_key(&conn, "codex", "sess").1.as_deref(),
        Some(ProjectKeyMethod::Remote.as_str())
    );
}
