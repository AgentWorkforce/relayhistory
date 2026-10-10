use anyhow::{ensure, Result};
use rusqlite::Connection;

pub(crate) fn schema_is_current(conn: &Connection) -> Result<bool> {
    for name in [
        "session_observations",
        "observation_hydration_checkpoints",
        "observation_discovery_skips",
        "observation_evidence",
        "canonical_evidence_protection",
        "delete_canonical_evidence_protection",
        "idx_observation_locator",
        "delete_session_observations",
        "observation_versions",
        "observation_clock",
        "delete_observation_version",
    ] {
        if !conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name=?)",
            [name],
            |r| r.get::<_, bool>(0),
        )? {
            return Ok(false);
        }
    }
    for (table, required) in [
        ("session_observations", "source,session_id,location,connector_id,connector_instance,raw_locator,source_stamp,discovery_state,access_state,updated_ms,first_prompt,last_assistant_text"),
        ("observation_hydration_checkpoints", "source,session_id,location,connector_id,connector_instance,source_stamp,parser_version,last_event_at_ms,source_bytes,records_parsed,include_related,updated_ms,committed_offset,prefix_hash,dev_ino,parser_state_json"),
        ("observation_discovery_skips", "source,location,connector_id,connector_instance,locator,stamp,updated_ms"),
        ("observation_evidence", "source,session_id,location,connector_id,connector_instance,evidence_uid,payload_json"),
        ("canonical_evidence_protection", "source,session_id,record_identity"),
    ] {
        let columns=conn.prepare(&format!("PRAGMA table_info({table})"))?.query_map([], |row| row.get::<_,String>(1))?.collect::<rusqlite::Result<Vec<_>>>()?;
        if required.split(',').any(|column| !columns.iter().any(|present|present==column)) { return Ok(false); }
    }
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name='connector_observations_v1')",
        [],
        |r| r.get(0),
    )?)
}

pub(crate) fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(r#"
CREATE TABLE IF NOT EXISTS session_observations (
 source TEXT NOT NULL, session_id TEXT NOT NULL,
 location TEXT NOT NULL CHECK(location IN ('local','remote')),
 connector_id TEXT NOT NULL CHECK(length(trim(connector_id)) > 0),
 connector_instance TEXT NOT NULL CHECK(length(trim(connector_instance)) > 0),
 raw_locator TEXT, source_stamp TEXT, discovery_state TEXT NOT NULL DEFAULT 'shallow' CHECK(discovery_state IN ('shallow','full')),
 access_state TEXT NOT NULL DEFAULT 'available' CHECK(access_state IN ('available','unavailable','withdrawn')),
 updated_ms INTEGER NOT NULL,
 first_prompt TEXT, last_assistant_text TEXT,
 PRIMARY KEY(source,session_id,location,connector_id,connector_instance)
);
CREATE INDEX IF NOT EXISTS idx_observation_locator ON session_observations(source,location,connector_id,connector_instance,raw_locator);
CREATE TABLE IF NOT EXISTS observation_clock(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL);
INSERT OR IGNORE INTO observation_clock(singleton,version) VALUES(1,0);
CREATE TABLE IF NOT EXISTS observation_versions(source TEXT NOT NULL,session_id TEXT NOT NULL,location TEXT NOT NULL,connector_id TEXT NOT NULL,connector_instance TEXT NOT NULL,version INTEGER NOT NULL,PRIMARY KEY(source,session_id,location,connector_id,connector_instance));
CREATE TRIGGER IF NOT EXISTS delete_observation_version AFTER DELETE ON session_observations BEGIN
 DELETE FROM observation_versions WHERE source=OLD.source AND session_id=OLD.session_id AND location=OLD.location AND connector_id=OLD.connector_id AND connector_instance=OLD.connector_instance;
END;
CREATE TABLE IF NOT EXISTS observation_hydration_checkpoints (
 source TEXT NOT NULL,session_id TEXT NOT NULL,location TEXT NOT NULL,connector_id TEXT NOT NULL,connector_instance TEXT NOT NULL,
 source_stamp TEXT,parser_version INTEGER NOT NULL,last_event_at_ms INTEGER,source_bytes INTEGER NOT NULL,records_parsed INTEGER NOT NULL,include_related INTEGER NOT NULL,updated_ms INTEGER NOT NULL,
 committed_offset INTEGER NOT NULL DEFAULT 0,prefix_hash TEXT,dev_ino TEXT,parser_state_json TEXT,
 PRIMARY KEY(source,session_id,location,connector_id,connector_instance),
 FOREIGN KEY(source,session_id,location,connector_id,connector_instance) REFERENCES session_observations(source,session_id,location,connector_id,connector_instance) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS observation_discovery_skips (
 source TEXT NOT NULL,location TEXT NOT NULL,connector_id TEXT NOT NULL,connector_instance TEXT NOT NULL,locator TEXT NOT NULL,stamp TEXT NOT NULL,updated_ms INTEGER NOT NULL,
 PRIMARY KEY(source,location,connector_id,connector_instance,locator)
);
CREATE TABLE IF NOT EXISTS canonical_evidence_protection (
 source TEXT NOT NULL, session_id TEXT NOT NULL, record_identity TEXT NOT NULL,
 PRIMARY KEY(source,session_id,record_identity)
);
CREATE TRIGGER IF NOT EXISTS delete_canonical_evidence_protection AFTER DELETE ON sessions BEGIN
 DELETE FROM canonical_evidence_protection WHERE source=OLD.source AND session_id=OLD.session_id;
END;
CREATE TABLE IF NOT EXISTS observation_evidence (
 source TEXT NOT NULL,session_id TEXT NOT NULL,location TEXT NOT NULL,connector_id TEXT NOT NULL,connector_instance TEXT NOT NULL,
 evidence_uid TEXT NOT NULL, payload_json TEXT NOT NULL,
 PRIMARY KEY(source,session_id,location,connector_id,connector_instance,evidence_uid),
 FOREIGN KEY(source,session_id,location,connector_id,connector_instance) REFERENCES session_observations(source,session_id,location,connector_id,connector_instance) ON DELETE CASCADE
);
CREATE TRIGGER IF NOT EXISTS delete_session_observations AFTER DELETE ON sessions BEGIN
 DELETE FROM observation_hydration_checkpoints WHERE source=OLD.source AND session_id=OLD.session_id;
 DELETE FROM observation_evidence WHERE source=OLD.source AND session_id=OLD.session_id;
 DELETE FROM session_observations WHERE source=OLD.source AND session_id=OLD.session_id;
END;
INSERT OR IGNORE INTO session_observations(source,session_id,location,connector_id,connector_instance,raw_locator,source_stamp,discovery_state,access_state,updated_ms)
 SELECT source,session_id,location,'legacy-unknown','default',raw_locator,source_stamp,COALESCE(discovery_state,'shallow'),'available',0
 FROM session_presences WHERE NOT EXISTS(SELECT 1 FROM schema_migrations WHERE name='connector_observations_v1');
INSERT OR IGNORE INTO schema_migrations(name) VALUES ('connector_observations_v1');
INSERT OR IGNORE INTO observation_versions(source,session_id,location,connector_id,connector_instance,version) SELECT source,session_id,location,connector_id,connector_instance,rowid+(SELECT version FROM observation_clock WHERE singleton=1) FROM session_observations;
UPDATE observation_clock SET version=MAX(version,COALESCE((SELECT MAX(version) FROM observation_versions),0)) WHERE singleton=1;

"#)?;
    // The checkpoint's byte-cursor columns post-date the table, and
    // `CREATE TABLE IF NOT EXISTS` is a no-op on a database that already has
    // it, so an existing install gains them here. A fresh database gets the
    // same shape from the DDL above and these are skipped.
    let existing: Vec<String> = conn
        .prepare("PRAGMA table_info(observation_hydration_checkpoints)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<_>>()?;
    for (column, declaration) in [
        ("committed_offset", "INTEGER NOT NULL DEFAULT 0"),
        ("prefix_hash", "TEXT"),
        ("dev_ino", "TEXT"),
        ("parser_state_json", "TEXT"),
    ] {
        if !existing.iter().any(|present| present == column) {
            conn.execute(
                &format!(
                    "ALTER TABLE observation_hydration_checkpoints ADD COLUMN {column} {declaration}"
                ),
                [],
            )?;
        }
    }
    // Preview provenance post-dates the table the same way: a fresh database
    // has the columns from the DDL above, an existing one gains them here.
    // Rows predating them keep NULL previews, which a location retirement
    // reads as "no surviving preview" — the non-leaking choice for text that
    // cannot be attributed.
    let existing: Vec<String> = conn
        .prepare("PRAGMA table_info(session_observations)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for column in ["first_prompt", "last_assistant_text"] {
        if !existing.iter().any(|present| present == column) {
            conn.execute(
                &format!("ALTER TABLE session_observations ADD COLUMN {column} TEXT"),
                [],
            )?;
        }
    }
    ensure!(
        schema_is_current(conn)?,
        "incomplete connector observation schema"
    );
    Ok(())
}
