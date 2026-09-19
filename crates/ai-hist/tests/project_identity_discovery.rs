//! Discovery's unchanged-candidate shortcut and project identity.
//!
//! Its own test binary because it sets `HOME` for the process, which no test
//! running beside it could tolerate.

use ai_hist::project_identity::ProjectKeyMethod;
use ai_hist::{discover_sessions_scoped_at, open_db, DiscoverOptions, SessionScope};
use rusqlite::Connection;
use std::fs;
use std::path::Path;

fn write(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
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

/// Discovery's stamp shortcut must not also skip project-identity resolution.
///
/// An unchanged candidate is served from the catalog without reaching the
/// shallow upsert, which is right for everything the transcript says and wrong
/// for a key derived from the filesystem beside it. A session first discovered
/// before its checkout had an `origin` would otherwise keep its path key on
/// every later `sessions discover`, forever: the transcript never changes, so
/// the row is never revisited, and the wrong answer looks exactly like a
/// settled one.
#[test]
fn discovery_upgrades_a_path_key_once_the_checkout_gains_a_remote() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    let work = home.join("work/app");
    fs::create_dir_all(&work).unwrap();

    let day = home.join(".codex/sessions/2026/09/19");
    write(
        &day.join("rollout-later.jsonl"),
        &format!(
            "{}\n{}\n",
            format_args!(
                r#"{{"timestamp":"2026-09-19T12:00:00Z","type":"session_meta","payload":{{"id":"later","cwd":"{}"}}}}"#,
                work.display()
            ),
            r#"{"timestamp":"2026-09-19T12:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"before the remote existed"}}"#,
        ),
    );

    std::env::set_var("HOME", home);
    std::env::set_var("USERPROFILE", home);
    std::env::set_var("OPENCODE_DB", home.join("missing-opencode.db"));
    std::env::set_var("TRAJECTORY_ROOT", home.join("missing-trajectories"));
    std::env::remove_var("AI_HIST_DB");
    let db = home.join("discovery.db");
    let discover = || {
        discover_sessions_scoped_at(
            &db,
            &DiscoverOptions {
                scope: SessionScope::Local,
                sources: vec!["codex".into()],
                limit: None,
            },
        )
        .unwrap()
    };

    // Discovered while the directory is not a repository at all.
    discover();
    {
        let conn = open_db(&db).unwrap();
        assert_eq!(
            session_key(&conn, "codex", "later"),
            (
                Some(work.to_string_lossy().to_string()),
                Some(ProjectKeyMethod::PathFallback.as_str().to_string())
            ),
        );
    }

    // `git init` and a remote, some time later. The transcript is untouched,
    // so the next pass serves this row from the catalog by its stamp.
    let git_dir = work.join(".git");
    fs::create_dir_all(&git_dir).unwrap();
    fs::write(
        git_dir.join("config"),
        "[remote \"origin\"]\n\turl = git@github.com:Org/Repo.git\n",
    )
    .unwrap();

    let (rows, summary) = discover();
    assert!(
        summary.skipped_unchanged > 0,
        "the premise of this test is that the row is served from the catalog, \
         not re-read; {summary:?}"
    );

    // The emitted row, not only the catalog. Discovery streams rows to its
    // caller as each window is decided — `sessions discover --json` prints
    // them as it goes — so a correction applied at the end of the pass would
    // fix the database and still have handed every consumer the stale path
    // key. The JSONL a consumer parses would disagree with the row the
    // database holds, and nothing in either would say so.
    let emitted = rows
        .iter()
        .find(|row| row.session_id == "later")
        .expect("the session was not emitted");
    assert_eq!(
        (
            emitted.project_key.clone(),
            emitted.project_key_method.clone()
        ),
        (
            Some("github.com/Org/Repo".to_string()),
            Some(ProjectKeyMethod::Remote.as_str().to_string())
        ),
        "discovery streamed a stale path key for a checkout that has a remote"
    );

    let conn = open_db(&db).unwrap();
    assert_eq!(
        session_key(&conn, "codex", "later"),
        (
            Some("github.com/Org/Repo".to_string()),
            Some(ProjectKeyMethod::Remote.as_str().to_string())
        ),
        "a cached row kept its path key after the checkout gained a remote"
    );
    assert_eq!(
        emitted.project_key,
        session_key(&conn, "codex", "later").0,
        "the streamed row and the stored row must not disagree"
    );
}
