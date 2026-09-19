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

use ai_hist::{delivery::*, open_db, sync_scoped_at, SessionScope};
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

    let upserts: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM delivery_journal \
             WHERE kind = 'session_event' AND operation = 'upsert'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        upserts,
        keys.len() as i64,
        "each event must be journaled once; a second upsert per event means the \
         key was swept in afterwards rather than stamped at insert"
    );

    // And the refresh has nothing left to correct, which is the same claim
    // read from the other side.
    assert_eq!(ai_hist::refresh_project_identity(&conn).unwrap(), 0);
}
