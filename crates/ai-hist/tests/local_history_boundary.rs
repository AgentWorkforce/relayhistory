//! Characterization before the local history/cloud boundary migration.
//!
//! The collision test deliberately records a known limitation of the current
//! schema. Stage 3 must replace it with independent connector observations.

use ai_hist::{init_db, session_locations, upsert_session_presence, SessionLocation};
use rusqlite::Connection;

#[test]
fn characterization_remote_connectors_overwrite_each_other_in_either_scan_order() {
    let provider = ("codex-cloud://session/shared", "provider-revision", "full");
    let recall = (
        "relayhistory://session/shared",
        "recall-revision",
        "shallow",
    );

    for observations in [[provider, recall], [recall, provider]] {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO sessions (source, session_id) VALUES ('codex', 'shared')",
            [],
        )
        .unwrap();
        upsert_session_presence(
            &conn,
            "codex",
            "shared",
            SessionLocation::Local,
            Some("/fixture/rollout.jsonl"),
            Some("local-revision"),
            Some("shallow"),
        )
        .unwrap();
        for (locator, stamp, state) in observations {
            upsert_session_presence(
                &conn,
                "codex",
                "shared",
                SessionLocation::Remote,
                Some(locator),
                Some(stamp),
                Some(state),
            )
            .unwrap();
        }

        let metadata: (String, String, String) = conn
            .query_row(
                "SELECT raw_locator, source_stamp, discovery_state FROM session_presences \
                 WHERE source = 'codex' AND session_id = 'shared' AND location = 'remote'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(metadata.0, observations[1].0);
        assert_eq!(metadata.1, observations[1].1);
        assert_eq!(
            metadata.2, "full",
            "full state survives even when a different connector replaces its locator"
        );
        assert_eq!(
            session_locations(&conn, "codex", "shared").unwrap(),
            ["local", "remote"]
        );
        let local: (String, String) = conn
            .query_row(
                "SELECT raw_locator, source_stamp FROM session_presences \
                 WHERE source = 'codex' AND session_id = 'shared' AND location = 'local'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            local,
            ("/fixture/rollout.jsonl".into(), "local-revision".into())
        );
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            count, 1,
            "acquisition must retain canonical session identity"
        );
    }
}

#[test]
fn characterization_hydration_checkpoint_identity_has_no_connector_dimension() {
    let conn = Connection::open_in_memory().unwrap();
    init_db(&conn).unwrap();
    let mut statement = conn
        .prepare("SELECT name FROM pragma_table_info('session_hydration_checkpoints') WHERE pk > 0 ORDER BY pk")
        .unwrap();
    let primary_key: Vec<String> = statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(primary_key, ["source", "session_id", "location"]);
}
