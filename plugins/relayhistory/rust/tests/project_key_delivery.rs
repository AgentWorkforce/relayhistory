//! Canonical project identity must not cost a second delivery upsert.
//!
//! `session_events.project_key` is denormalized, so there are two ways to fill
//! it: stamp it as the row is inserted, or sweep for it afterwards. The sweep
//! is not free — an `UPDATE` over `session_events` is a change every durable
//! delivery subscriber has to be told about, so it journals a second upsert
//! for every event of every session on every sync, doubling what a destination
//! receives to carry a value that was already derivable at insert time.
//!
//! The Codex walk is where this bites: it writes a rollout's events *before*
//! upserting the session they belong to, so an implementation that only looked
//! the key up on the `sessions` row would find nothing and hand every Codex
//! event straight back to the sweep. This asserts the journal, not the
//! intention.

use ai_hist::{sync_scoped_at, SessionScope};
use relayhistory_plugin::delivery::*;
use std::{fs, path::Path};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}

fn config() -> DeliveryJobConfig {
    DeliveryJobConfig {
        destination_id: "fixture".into(),
        instance_id: "one".into(),
        account_id: "account".into(),
        mapping_version: "1".into(),
        selection: ExportSelection {
            all_sources: true,
            kinds: SUPPORTED_KINDS.iter().map(|s| (*s).into()).collect(),
            ..ExportSelection::default()
        },
        limits: DeliveryLimits::default(),
    }
}

/// The only test in this binary: it sets `HOME` for the process.
#[test]
fn codex_events_are_keyed_at_insert_and_journaled_once() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();

    // A real checkout, so the key is a remote rather than a path: the stronger
    // key is the one an ordering mistake would fail to reproduce at insert.
    let checkout = home.join("work/app");
    fs::create_dir_all(checkout.join(".git")).unwrap();
    fs::write(
        checkout.join(".git/config"),
        "[remote \"origin\"]\n\turl = git@github.com:Org/Repo.git\n",
    )
    .unwrap();

    let day = home.join(".codex/sessions/2026/09/19");
    write(
        &day.join("rollout-sess.jsonl"),
        &format!(
            "{}\n{}\n{}\n",
            format_args!(
                r#"{{"timestamp":"2026-09-19T10:00:00Z","type":"session_meta","payload":{{"id":"sess","cwd":"{}"}}}}"#,
                checkout.display()
            ),
            r#"{"timestamp":"2026-09-19T10:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"do the thing"}}"#,
            r#"{"timestamp":"2026-09-19T10:00:02Z","type":"event_msg","payload":{"type":"agent_message","message":"done"}}"#,
        ),
    );
    // A delegated child, running somewhere that resolves to nothing
    // canonical. It gets no catalog row of its own, so its events can only be
    // keyed by inheriting the parent's -- and a re-ingest must not undo that.
    let scratch = home.join("scratch");
    fs::create_dir_all(&scratch).unwrap();
    write(
        &day.join("rollout-child.jsonl"),
        &format!(
            "{}\n{}\n",
            format_args!(
                r#"{{"timestamp":"2026-09-19T10:00:03Z","type":"session_meta","payload":{{"id":"child","cwd":"{}","session_id":"sess","parent_thread_id":"sess","thread_source":"subagent"}}}}"#,
                scratch.display()
            ),
            r#"{"timestamp":"2026-09-19T10:00:04Z","type":"event_msg","payload":{"type":"agent_message","message":"child answer"}}"#,
        ),
    );

    std::env::set_var("HOME", home);
    std::env::set_var("USERPROFILE", home);
    std::env::set_var("OPENCODE_DB", home.join("missing-opencode.db"));
    std::env::set_var("TRAJECTORY_ROOT", home.join("missing-trajectories"));
    std::env::remove_var("AI_HIST_DB");

    let db = home.join("history.db");
    let conn = open_db(&db).unwrap();
    create_job(&conn, &config(), 0).unwrap();
    drop(conn);

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();

    let keys: Vec<Option<String>> = conn
        .prepare("SELECT project_key FROM session_events WHERE source = 'codex' ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert!(
        !keys.is_empty(),
        "the fixture produced no Codex events, so nothing here is tested"
    );
    assert!(
        keys.iter()
            .all(|key| key.as_deref() == Some("github.com/Org/Repo")),
        "Codex events did not carry the canonical key: {keys:?}"
    );

    let upserts = |conn: &rusqlite::Connection, session: &str| -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM delivery_journal \
             WHERE kind = 'session_event' AND operation = 'upsert' AND session_id = ?",
            [session],
            |row| row.get(0),
        )
        .unwrap()
    };
    // The parent's key is derivable at insert, so its events are journaled
    // exactly once. A second upsert per event would mean the key was swept in
    // afterwards -- the cost this design exists to avoid.
    assert_eq!(
        upserts(&conn, "sess"),
        2,
        "the parent's events must be journaled once each"
    );
    // The child's cannot be: its own directory resolves to a path, and the
    // parent's key only reaches it once the relationship has been recorded. So
    // exactly one correction is expected, and no more.
    assert_eq!(
        upserts(&conn, "child"),
        2,
        "the child's single event should be journaled once at insert and once \
         when inheritance settles, not repeatedly"
    );

    // The delegated child has no catalog row, so its events hold the parent's
    // key only because the inheritance pass put it there.
    assert!(
        conn.query_row(
            "SELECT COUNT(*) FROM sessions WHERE source = 'codex' AND session_id = 'child'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
            == 0,
        "the premise of this test is that a delegated thread is not a catalog session"
    );
    let child_keys = |conn: &rusqlite::Connection| -> Vec<Option<String>> {
        conn.prepare(
            "SELECT project_key FROM session_events \
             WHERE source = 'codex' AND session_id = 'child' ORDER BY id",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
    };
    assert_eq!(
        child_keys(&conn),
        vec![Some("github.com/Org/Repo".to_string())],
        "the delegated child's events did not inherit the parent's key"
    );

    // And the refresh has nothing left to correct, which is the same claim
    // read from the other side.
    assert_eq!(ai_hist::refresh_project_identity(&conn).unwrap(), 0);

    // --- re-ingest ------------------------------------------------------
    //
    // The child's own directory resolves to nothing canonical, so the value
    // the insert can derive for it is a machine-local path. On a re-ingest
    // that value must lose to the key already stored: preferring the incoming
    // one would overwrite the repository with a path, and then hand every one
    // of those events back to the denormalizing sweep to put right, journaling
    // a second delivery upsert each time.
    //
    // The transcript has to actually grow for this to be exercised: an
    // unchanged rollout is skipped by its stamp, so the already-stored event
    // would never reach the conflict clause and the test would pass without
    // testing anything. A subagent thread that says one more thing re-parses
    // the whole file, which puts its existing event through `ON CONFLICT`.
    drop(conn);
    let grown = fs::read_to_string(day.join("rollout-child.jsonl")).unwrap()
        + r#"{"timestamp":"2026-09-19T10:00:05Z","type":"event_msg","payload":{"type":"agent_message","message":"child again"}}"#
        + "\n";
    fs::write(day.join("rollout-child.jsonl"), grown).unwrap();
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();

    assert_eq!(
        child_keys(&conn),
        vec![
            Some("github.com/Org/Repo".to_string()),
            Some("github.com/Org/Repo".to_string()),
        ],
        "a re-ingest downgraded an inherited repository key to a path"
    );
    // The final key alone would not have caught this: the sweep at the end of
    // every sync silently puts a downgraded key back, so the damage shows up
    // only as delivery traffic. Four is the whole honest budget for this
    // child -- one insert and one inheritance correction on the first sync,
    // then one re-upsert of the existing event and one insert of the new one.
    // A conflict clause that preferred the incoming cwd-derived value scores
    // six: the two extra are the sweep undoing the downgrade, and a
    // destination receives them for no reason at all.
    assert_eq!(
        upserts(&conn, "child"),
        4,
        "a re-ingest downgraded the key and made the sweep repair it, which \
         costs a delivery upsert per event every time"
    );
    assert_eq!(
        upserts(&conn, "sess"),
        2,
        "the parent's untouched transcript must not be re-journaled at all"
    );
    assert_eq!(
        ai_hist::refresh_project_identity(&conn).unwrap(),
        0,
        "the sweep still had work to do after a re-ingest"
    );
    assert_eq!(ai_hist::refresh_project_identity(&conn).unwrap(), 0);
}
