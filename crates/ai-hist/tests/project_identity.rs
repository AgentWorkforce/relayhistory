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
    // And a delegated child that runs in a repository of its own, which is the
    // case inheritance must keep its hands off.
    let own_repo = checkout(&work, "subagent-repo", Some("git@github.com:Org/Other.git"));

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
    // Its sibling was delegated by the same session but ran in another
    // repository entirely.
    rollout(
        "child-own",
        &own_repo,
        Some("sess-a"),
        "2026-09-19T10:00:45Z",
    );

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

    // --- a child with a repository of its own keeps it --------------------
    //
    // Inheritance is a loan for a child that resolved to nothing, not a
    // correction applied to one that resolved to something. This child's
    // events worked out `github.com/Org/Other` from the directory it ran in,
    // which is a stronger statement about where the work happened than its
    // delegator's repository. Overwriting it files a subagent under a project
    // it never touched, and — because the denormalizing pass runs on every
    // sync — does it again after every correction.
    let own_events = event_keys(&conn, "codex", "child-own");
    assert!(
        !own_events.is_empty(),
        "the sibling transcript produced no events, so the claim is untested"
    );
    assert!(
        own_events
            .iter()
            .all(|key| key.as_deref() == Some("github.com/Org/Other")),
        "a delegated child's own repository was replaced by its delegator's: {own_events:?}"
    );

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

/// An inherited key is a stand-in, not a settled answer.
///
/// A delegated child wears its parent's repository because its own directory
/// resolved to nothing canonical. If that directory later gains an `origin`,
/// the borrowed key is no longer the best thing known about the session -- and
/// because the child's transcript never changes, nothing would ever revisit
/// it. The child would be filed under the parent's repository forever, which
/// is wrong in exactly the way that looks right.
#[test]
fn an_inherited_key_yields_to_the_child_gaining_its_own_remote() {
    let temp = tempfile::tempdir().unwrap();
    let conn = open_db(&temp.path().join("inherit.db")).unwrap();

    // The child's own directory: not a repository yet.
    let own = temp.path().join("child-checkout");
    fs::create_dir_all(&own).unwrap();

    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'child', ?, 'github.com/acme/parent', 'inherited', 1, 'full')",
        rusqlite::params![own.to_string_lossy()],
    )
    .unwrap();

    ai_hist::project_identity::begin_acquisition_pass();
    // Nothing better is available, so the borrowed key stands. A path key must
    // never displace an inherited one: the parent's repository says more about
    // the session than the directory it happened to run in.
    refresh_project_identity(&conn).unwrap();
    assert_eq!(
        session_key(&conn, "codex", "child"),
        (
            Some("github.com/acme/parent".to_string()),
            Some(ProjectKeyMethod::Inherited.as_str().to_string())
        ),
        "an inherited key was traded for a path"
    );

    // `git init` and an origin of its own, some time later.
    let git_dir = own.join(".git");
    fs::create_dir_all(&git_dir).unwrap();
    fs::write(
        git_dir.join("config"),
        "[remote \"origin\"]\n\turl = git@github.com:acme/child.git\n",
    )
    .unwrap();

    // A new acquisition pass, exactly as the next sync or discovery opens:
    // the resolver's cache is only valid within one, and the directory it
    // cached as "not a repository" has since become one.
    ai_hist::project_identity::begin_acquisition_pass();

    assert!(refresh_project_identity(&conn).unwrap() > 0);
    assert_eq!(
        session_key(&conn, "codex", "child"),
        (
            Some("github.com/acme/child".to_string()),
            Some(ProjectKeyMethod::Remote.as_str().to_string())
        ),
        "the child kept the inherited key after gaining its own remote"
    );

    // Settled: the inheritance pass does not claw a `remote` key back, and a
    // further refresh writes nothing.
    assert_eq!(refresh_project_identity(&conn).unwrap(), 0);
}

/// A borrowed key follows the session it was borrowed from.
///
/// Pass 1 can promote a parent off a key it had itself borrowed and onto a
/// `remote` of its own. Every child wearing the old stand-in is then filed
/// under a repository no session in the database claims any more — and pass 2
/// used to skip anything already marked `inherited`, so nothing would ever
/// revisit them. The two sessions were one project a moment ago and there is
/// nothing anywhere to say they still are.
#[test]
fn a_child_re_inherits_when_its_parent_moves_to_a_key_of_its_own() {
    let temp = tempfile::tempdir().unwrap();
    let conn = open_db(&temp.path().join("reinherit.db")).unwrap();

    // The parent's directory is not a repository yet, so the key it wears was
    // lent to it by something outside this database.
    let parent_dir = temp.path().join("parent-checkout");
    fs::create_dir_all(&parent_dir).unwrap();
    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'parent', ?, 'github.com/acme/grandparent', 'inherited', 1, 'full')",
        rusqlite::params![parent_dir.to_string_lossy()],
    )
    .unwrap();
    // The child borrowed that same stand-in.
    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'child', '/tmp/scratch', 'github.com/acme/grandparent', 'inherited', 2, 'full')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
         child_session_id, relationship, identity_status, evidence_kind, created_ms, updated_ms) \
         VALUES ('codex', 'parent', 'rel', 'child', 'delegation', 'observed', 'fixture', 1, 1)",
        [],
    )
    .unwrap();

    // Settled: nothing to do while the borrowed key is the best available.
    ai_hist::project_identity::begin_acquisition_pass();
    assert_eq!(refresh_project_identity(&conn).unwrap(), 0);

    // The parent's checkout gains an origin of its own.
    let git_dir = parent_dir.join(".git");
    fs::create_dir_all(&git_dir).unwrap();
    fs::write(
        git_dir.join("config"),
        "[remote \"origin\"]\n\turl = git@github.com:acme/parent.git\n",
    )
    .unwrap();

    ai_hist::project_identity::begin_acquisition_pass();
    assert!(refresh_project_identity(&conn).unwrap() > 0);
    assert_eq!(
        session_key(&conn, "codex", "parent"),
        (
            Some("github.com/acme/parent".to_string()),
            Some(ProjectKeyMethod::Remote.as_str().to_string())
        ),
        "pass 1 did not promote the parent, so the claim below is untested"
    );
    assert_eq!(
        session_key(&conn, "codex", "child"),
        (
            Some("github.com/acme/parent".to_string()),
            Some(ProjectKeyMethod::Inherited.as_str().to_string())
        ),
        "the child kept a key its parent no longer wears"
    );

    // And it settles: the rewritten row is not rewritten again.
    ai_hist::project_identity::begin_acquisition_pass();
    assert_eq!(
        refresh_project_identity(&conn).unwrap(),
        0,
        "re-inheritance must reach a fixed point, not churn on every sync"
    );
}

/// A delegated thread that delegates again is still the root's work.
///
/// Neither middle generation need hold a catalog row: a Codex subagent
/// rollout and a Claude sidechain are evidence, not sessions. Joining the
/// relationship parent straight to `sessions` therefore stops at the first
/// uncataloged generation, and the grandchild's events — the deepest, most
/// specialized work in the tree — are the one place in the database with no
/// project at all. The deeper the delegation, the more certainly its work
/// disappears from the repository it was done for.
#[test]
fn events_of_a_nested_delegated_thread_reach_the_nearest_cataloged_ancestor() {
    let temp = tempfile::tempdir().unwrap();
    let conn = open_db(&temp.path().join("nested.db")).unwrap();

    // Only the root is cataloged.
    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'r', '/work/app', 'github.com/acme/app', 'remote', 1, 'full')",
        [],
    )
    .unwrap();
    let edge = |parent: &str, child: &str, created: i64| {
        conn.execute(
            "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
             child_session_id, relationship, identity_status, evidence_kind, created_ms, \
             updated_ms) \
             VALUES ('codex', ?1, ?2, ?3, 'delegation', 'observed', 'fixture', ?4, ?4)",
            rusqlite::params![parent, format!("{parent}->{child}"), child, created],
        )
        .unwrap();
    };
    edge("r", "c", 1);
    edge("c", "g", 2);

    // The grandchild's transcript produced events and no catalog row.
    for (uid, text) in [("g-1", "deep work"), ("g-2", "more of it")] {
        conn.execute(
            "INSERT INTO session_events (source, session_id, event_uid, ts_ms, role, kind, text) \
             VALUES ('codex', 'g', ?1, 1, 'assistant', 'text', ?2)",
            rusqlite::params![uid, text],
        )
        .unwrap();
    }

    assert!(refresh_project_identity(&conn).unwrap() > 0);
    let keyed: Vec<(Option<String>, Option<String>)> = conn
        .prepare(
            "SELECT project_key, project_key_method FROM session_events \
             WHERE source = 'codex' AND session_id = 'g' ORDER BY event_uid",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        keyed,
        vec![
            (
                Some("github.com/acme/app".to_string()),
                Some(ProjectKeyMethod::Inherited.as_str().to_string())
            );
            2
        ],
        "a twice-delegated thread's events never reached the root's repository"
    );

    // Settled, and a `remote` an evidence-only thread resolved for itself is
    // not displaced by the loan.
    assert_eq!(refresh_project_identity(&conn).unwrap(), 0);
    conn.execute(
        "INSERT INTO session_events (source, session_id, event_uid, ts_ms, role, kind, text, \
         project_key, project_key_method) \
         VALUES ('codex', 'g', 'g-own', 1, 'assistant', 'text', 'elsewhere', \
                 'github.com/acme/other', 'remote')",
        [],
    )
    .unwrap();
    refresh_project_identity(&conn).unwrap();
    let own: (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT project_key, project_key_method FROM session_events WHERE event_uid = 'g-own'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        own,
        (
            Some("github.com/acme/other".to_string()),
            Some(ProjectKeyMethod::Remote.as_str().to_string())
        ),
        "the loan displaced a repository the thread had resolved for itself"
    );
}

/// A path key is not an answer, so the walk must climb past one and keep
/// looking — including past it to the *next* parent.
///
/// `path` is the absence of a canonical key, which is why
/// [`inheritable_parent_key_sql`] lends `remote` and `inherited` and nothing
/// else. A walk that stopped at the first non-null ancestor key would hand a
/// machine-local directory down to every descendant *labelled* `inherited`,
/// which reads as a resolved answer and is not one — and it would do so in
/// preference to a real repository sitting one parent along, so the two passes
/// would key the same tree differently.
#[test]
fn a_path_keyed_ancestor_does_not_stop_the_walk() {
    let temp = tempfile::tempdir().unwrap();
    let conn = open_db(&temp.path().join("path-ancestor.db")).unwrap();

    // The earlier-recorded parent resolved to nothing but its own directory,
    // and has no ancestor to improve on it. The later one carries the
    // repository.
    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'alt', '/tmp/scratch', '/tmp/scratch', 'path', 1, 'full')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'root', '/work/app', 'github.com/acme/app', 'remote', 2, 'full')",
        [],
    )
    .unwrap();
    let edge = |parent: &str, child: &str, created: i64| {
        conn.execute(
            "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
             child_session_id, relationship, identity_status, evidence_kind, created_ms, \
             updated_ms) \
             VALUES ('codex', ?1, ?2, ?3, 'delegation', 'observed', 'fixture', ?4, ?4)",
            rusqlite::params![parent, format!("{parent}->{child}"), child, created],
        )
        .unwrap();
    };
    edge("alt", "leaf", 1);
    edge("root", "leaf", 2);
    // The leaf is evidence only: events, no catalog row.
    conn.execute(
        "INSERT INTO session_events (source, session_id, event_uid, ts_ms, role, kind, text) \
         VALUES ('codex', 'leaf', 'leaf-1', 1, 'assistant', 'text', 'deep work')",
        [],
    )
    .unwrap();

    refresh_project_identity(&conn).unwrap();

    let keyed: (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT project_key, project_key_method FROM session_events \
             WHERE source = 'codex' AND session_id = 'leaf'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        keyed,
        (
            Some("github.com/acme/app".to_string()),
            Some(ProjectKeyMethod::Inherited.as_str().to_string())
        ),
        "a path key was passed down as if it were a project"
    );
    // The path-keyed parent is left as it is: it has nothing better available.
    assert_eq!(
        session_key(&conn, "codex", "alt"),
        (
            Some("/tmp/scratch".to_string()),
            Some(ProjectKeyMethod::PathFallback.as_str().to_string())
        ),
    );
    assert_eq!(refresh_project_identity(&conn).unwrap(), 0);
}

/// A generation the catalog does not hold must not strand the one below it.
///
/// `root -> middle -> leaf` with `middle` evidence-only leaves `leaf` joined
/// to nothing by pass 2's single statement, so its *row* keeps a path key or
/// none while the root carries the repository. Its events then cannot be keyed
/// either without the two disagreeing.
#[test]
fn a_cataloged_session_inherits_across_an_uncataloged_generation() {
    let temp = tempfile::tempdir().unwrap();
    let conn = open_db(&temp.path().join("across.db")).unwrap();

    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'root', '/work/app', 'github.com/acme/app', 'remote', 1, 'full')",
        [],
    )
    .unwrap();
    // The leaf is cataloged; the generation between them is not.
    conn.execute(
        "INSERT INTO sessions (source, session_id, last_activity_ms, discovery_state) \
         VALUES ('codex', 'leaf', 2, 'shallow')",
        [],
    )
    .unwrap();
    for (parent, child, created) in [("root", "middle", 1), ("middle", "leaf", 2)] {
        conn.execute(
            "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
             child_session_id, relationship, identity_status, evidence_kind, created_ms, \
             updated_ms) \
             VALUES ('codex', ?1, ?2, ?3, 'delegation', 'observed', 'fixture', ?4, ?4)",
            rusqlite::params![parent, format!("{parent}->{child}"), child, created],
        )
        .unwrap();
    }
    conn.execute(
        "INSERT INTO session_events (source, session_id, event_uid, ts_ms, role, kind, text) \
         VALUES ('codex', 'leaf', 'leaf-1', 1, 'assistant', 'text', 'work')",
        [],
    )
    .unwrap();

    refresh_project_identity(&conn).unwrap();
    assert_eq!(
        session_key(&conn, "codex", "leaf"),
        (
            Some("github.com/acme/app".to_string()),
            Some(ProjectKeyMethod::Inherited.as_str().to_string())
        ),
        "a cataloged session was stranded by an uncataloged generation above it"
    );
    assert_eq!(refresh_project_identity(&conn).unwrap(), 0);
}

/// A session the catalog holds and its events must name one project.
///
/// "This session's key reads NULL" and "the catalog does not hold this
/// session" are different questions, and only the second belongs to the event
/// pass. Answering the first with an ancestor's key keys the events while the
/// row stays empty — two rows of the same database naming different projects,
/// which pass 3 cannot reconcile afterwards because it may not lower a key's
/// rank.
///
/// Here nothing in the tree has a canonical key at all: the root resolved only
/// to its own directory. The right answer is that the leaf says nothing,
/// because a path key is not a project — and, whatever the answer, the row and
/// its events must give the same one.
#[test]
fn a_cataloged_session_and_its_events_never_name_different_projects() {
    let temp = tempfile::tempdir().unwrap();
    let conn = open_db(&temp.path().join("cataloged-null.db")).unwrap();

    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'root', '/tmp/scratch', '/tmp/scratch', 'path', 1, 'full')",
        [],
    )
    .unwrap();
    // Cataloged, with no key yet and nothing of its own to resolve one from.
    conn.execute(
        "INSERT INTO sessions (source, session_id, last_activity_ms, discovery_state) \
         VALUES ('codex', 'leaf', 2, 'shallow')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
         child_session_id, relationship, identity_status, evidence_kind, created_ms, updated_ms) \
         VALUES ('codex', 'root', 'rel', 'leaf', 'delegation', 'observed', 'fixture', 1, 1)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO session_events (source, session_id, event_uid, ts_ms, role, kind, text) \
         VALUES ('codex', 'leaf', 'leaf-1', 1, 'assistant', 'text', 'work')",
        [],
    )
    .unwrap();

    refresh_project_identity(&conn).unwrap();

    let row = session_key(&conn, "codex", "leaf");
    let event: (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT project_key, project_key_method FROM session_events \
             WHERE source = 'codex' AND session_id = 'leaf'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        row, event,
        "a cataloged session and its events disagreed about the project"
    );
    assert_eq!(
        row,
        (None, None),
        "a machine-local path was reported as this session's project"
    );
    assert_eq!(refresh_project_identity(&conn).unwrap(), 0);
}

/// A key borrowed *across* an uncataloged generation follows its ancestor too.
///
/// The statement pass 2 runs joins a child to its parent's catalog row, so it
/// can neither lend nor re-lend across a generation the catalog does not hold.
/// The walk covers the lending; without covering the re-lending it freezes the
/// first answer it ever gave: promote the ancestor and the grandchild keeps a
/// stand-in that ancestor has stopped wearing, in a project no session claims
/// any more. Same defect as the one fixed for direct children, one generation
/// further out.
#[test]
fn a_key_borrowed_across_an_uncataloged_generation_follows_its_ancestor() {
    let temp = tempfile::tempdir().unwrap();
    let conn = open_db(&temp.path().join("relend.db")).unwrap();

    // The root wears a stand-in of its own: its directory is not a repository
    // yet, so the key it holds came from somewhere outside this database.
    let root_dir = temp.path().join("root-checkout");
    fs::create_dir_all(&root_dir).unwrap();
    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'root', ?, 'github.com/acme/old', 'inherited', 1, 'full')",
        rusqlite::params![root_dir.to_string_lossy()],
    )
    .unwrap();
    // The leaf is cataloged; the generation between them is not.
    conn.execute(
        "INSERT INTO sessions (source, session_id, last_activity_ms, discovery_state) \
         VALUES ('codex', 'leaf', 2, 'shallow')",
        [],
    )
    .unwrap();
    for (parent, child, created) in [("root", "middle", 1), ("middle", "leaf", 2)] {
        conn.execute(
            "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
             child_session_id, relationship, identity_status, evidence_kind, created_ms, \
             updated_ms) \
             VALUES ('codex', ?1, ?2, ?3, 'delegation', 'observed', 'fixture', ?4, ?4)",
            rusqlite::params![parent, format!("{parent}->{child}"), child, created],
        )
        .unwrap();
    }
    conn.execute(
        "INSERT INTO session_events (source, session_id, event_uid, ts_ms, role, kind, text) \
         VALUES ('codex', 'leaf', 'leaf-1', 1, 'assistant', 'text', 'work')",
        [],
    )
    .unwrap();

    let event_key = || -> (Option<String>, Option<String>) {
        conn.query_row(
            "SELECT project_key, project_key_method FROM session_events \
             WHERE source = 'codex' AND session_id = 'leaf'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    };

    // The stand-in reaches the leaf and its events first — the state this test
    // is about disturbing.
    ai_hist::project_identity::begin_acquisition_pass();
    assert!(refresh_project_identity(&conn).unwrap() > 0);
    let borrowed = (
        Some("github.com/acme/old".to_string()),
        Some(ProjectKeyMethod::Inherited.as_str().to_string()),
    );
    assert_eq!(
        session_key(&conn, "codex", "leaf"),
        borrowed,
        "the leaf did not borrow across the uncataloged generation at all"
    );
    assert_eq!(event_key(), borrowed);

    // The root's checkout gains an origin, so pass 1 promotes it.
    let git_dir = root_dir.join(".git");
    fs::create_dir_all(&git_dir).unwrap();
    fs::write(
        git_dir.join("config"),
        "[remote \"origin\"]\n\turl = git@github.com:acme/new.git\n",
    )
    .unwrap();

    ai_hist::project_identity::begin_acquisition_pass();
    assert!(refresh_project_identity(&conn).unwrap() > 0);
    assert_eq!(
        session_key(&conn, "codex", "root"),
        (
            Some("github.com/acme/new".to_string()),
            Some(ProjectKeyMethod::Remote.as_str().to_string())
        ),
        "pass 1 did not promote the root, so the claim below is untested"
    );
    let promoted = (
        Some("github.com/acme/new".to_string()),
        Some(ProjectKeyMethod::Inherited.as_str().to_string()),
    );
    assert_eq!(
        session_key(&conn, "codex", "leaf"),
        promoted,
        "the leaf kept a stand-in its ancestor no longer wears"
    );
    assert_eq!(event_key(), promoted, "and its events kept it too");

    // Settled: no churn on the next sync.
    ai_hist::project_identity::begin_acquisition_pass();
    assert_eq!(refresh_project_identity(&conn).unwrap(), 0);
}

/// A delegation cycle must settle, not spin.
///
/// The ledger does not forbid a cycle, and a set-at-a-time UPDATE reads every
/// parent from one pre-statement snapshot — so in an inherited-only cycle
/// `A -> B -> C -> A` the three keys *rotate* on each iteration. The bound
/// ends the call, but not at a fixed point: every later refresh rotates them
/// again and denormalizes the new arrangement onto every event, which is
/// endless write traffic that each pass reports as work done. Worse, it is
/// silent — the keys look plausible at every moment, just never the same.
#[test]
fn a_delegation_cycle_settles_instead_of_rotating_its_keys() {
    let temp = tempfile::tempdir().unwrap();
    let conn = open_db(&temp.path().join("cycle.db")).unwrap();

    for (id, key) in [
        ("a", "github.com/acme/one"),
        ("b", "github.com/acme/two"),
        ("c", "github.com/acme/three"),
    ] {
        conn.execute(
            "INSERT INTO sessions (source, session_id, project_key, project_key_method, \
             last_activity_ms, discovery_state) VALUES ('codex', ?1, ?2, 'inherited', 1, 'full')",
            rusqlite::params![id, key],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events (source, session_id, event_uid, ts_ms, role, kind, text, \
             project_key, project_key_method) \
             VALUES ('codex', ?1, ?1 || '-1', 1, 'assistant', 'text', 'work', ?2, 'inherited')",
            rusqlite::params![id, key],
        )
        .unwrap();
    }
    for (parent, child) in [("a", "b"), ("b", "c"), ("c", "a")] {
        conn.execute(
            "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
             child_session_id, relationship, identity_status, evidence_kind, created_ms, \
             updated_ms) \
             VALUES ('codex', ?1, ?2, ?3, 'delegation', 'observed', 'fixture', 1, 1)",
            rusqlite::params![parent, format!("{parent}->{child}"), child],
        )
        .unwrap();
    }

    let keys = || -> Vec<(Option<String>, Option<String>)> {
        ["a", "b", "c"]
            .iter()
            .map(|id| session_key(&conn, "codex", id))
            .collect()
    };
    let before = keys();

    assert_eq!(
        refresh_project_identity(&conn).unwrap(),
        0,
        "a cycle with nothing settled above it has no better key to reach, so \
         the first refresh must already write nothing"
    );
    assert_eq!(keys(), before, "the keys rotated inside the cycle");
    assert_eq!(
        refresh_project_identity(&conn).unwrap(),
        0,
        "a second refresh must write nothing: a cycle has to reach a fixed point"
    );
    assert_eq!(keys(), before);

    // A cycle is not an excuse to stop lending: hang an acyclic chain off it
    // from a session that *is* settled, and that chain still inherits — and
    // also converges.
    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'root', '/work/app', 'github.com/acme/app', 'remote', 1, 'full')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sessions (source, session_id, last_activity_ms, discovery_state) \
         VALUES ('codex', 'child', 2, 'shallow')",
        [],
    )
    .unwrap();
    for (parent, child) in [("root", "middle"), ("middle", "child")] {
        conn.execute(
            "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
             child_session_id, relationship, identity_status, evidence_kind, created_ms, \
             updated_ms) \
             VALUES ('codex', ?1, ?2, ?3, 'delegation', 'observed', 'fixture', 2, 2)",
            rusqlite::params![parent, format!("{parent}->{child}"), child],
        )
        .unwrap();
    }

    assert!(refresh_project_identity(&conn).unwrap() > 0);
    assert_eq!(
        session_key(&conn, "codex", "child"),
        (
            Some("github.com/acme/app".to_string()),
            Some(ProjectKeyMethod::Inherited.as_str().to_string())
        ),
        "the acyclic chain stopped inheriting"
    );
    assert_eq!(keys(), before, "the cycle moved when the chain was added");
    assert_eq!(
        refresh_project_identity(&conn).unwrap(),
        0,
        "chain and cycle together must still converge to no writes"
    );
}
