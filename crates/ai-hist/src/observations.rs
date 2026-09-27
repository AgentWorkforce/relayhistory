//! Connector-owned acquisition state. Canonical sessions and location membership
//! remain shared; locators, stamps, failures and hydration checkpoints do not.
//!
//! Upgrade with older acquisition writers stopped. Legacy aggregate rows cannot
//! recover overwritten provenance and are marked `legacy-unknown`; old hydration
//! checkpoints are deliberately not assigned to a guessed connector.
use crate::SessionLocation;
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationKey {
    pub source: String,
    pub session_id: String,
    pub location: SessionLocation,
    pub connector_id: String,
    pub connector_instance: String,
}

impl ObservationKey {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            crate::SOURCE_CHOICES.contains(&self.source.as_str()),
            "invalid observation source"
        );
        for value in [
            &self.session_id,
            &self.connector_id,
            &self.connector_instance,
        ] {
            ensure!(
                !value.is_empty()
                    && value.trim() == value
                    && value.len() <= 1024
                    && !value.chars().any(char::is_control),
                "invalid observation identity"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionObservation {
    pub key: ObservationKey,
    /// Non-secret acquisition locator. Keep tokens and auth outside the catalog.
    pub raw_locator: Option<String>,
    pub source_stamp: Option<String>,
    pub discovery_state: String,
    pub access_state: String,
    pub updated_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationCheckpoint {
    pub source_stamp: Option<String>,
    pub parser_version: i64,
    pub last_event_at_ms: Option<i64>,
    pub source_bytes: i64,
    pub records_parsed: i64,
    pub include_related: bool,
    pub updated_ms: i64,
    /// The observed transcript's byte cursor, mirroring the one targeted
    /// hydration writes to `session_hydration_checkpoints`. See
    /// `ingest::cursor`: `parser_state_json` is the cursor document and the
    /// other three are projections of it.
    #[serde(default)]
    pub committed_offset: i64,
    #[serde(default)]
    pub prefix_hash: Option<String>,
    #[serde(default)]
    pub dev_ino: Option<String>,
    #[serde(default)]
    pub parser_state_json: Option<String>,
}

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
        ("session_observations", "source,session_id,location,connector_id,connector_instance,raw_locator,source_stamp,discovery_state,access_state,updated_ms"),
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
    ensure!(
        schema_is_current(conn)?,
        "incomplete connector observation schema"
    );
    Ok(())
}

fn read_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionObservation> {
    Ok(SessionObservation {
        key: ObservationKey {
            source: row.get(0)?,
            session_id: row.get(1)?,
            location: if row.get::<_, String>(2)? == "local" {
                SessionLocation::Local
            } else {
                SessionLocation::Remote
            },
            connector_id: row.get(3)?,
            connector_instance: row.get(4)?,
        },
        raw_locator: row.get(5)?,
        source_stamp: row.get(6)?,
        discovery_state: row.get(7)?,
        access_state: row.get(8)?,
        updated_ms: row.get(9)?,
    })
}
const COLUMNS: &str = "source,session_id,location,connector_id,connector_instance,raw_locator,source_stamp,discovery_state,access_state,updated_ms";

pub fn list(conn: &Connection, source: &str, session: &str) -> Result<Vec<SessionObservation>> {
    Ok(conn.prepare(&format!("SELECT {COLUMNS} FROM session_observations WHERE source=? AND session_id=? ORDER BY location,connector_id,connector_instance"))?.query_map(params![source,session],read_row)?.collect::<rusqlite::Result<_>>()?)
}

pub fn get(conn: &Connection, key: &ObservationKey) -> Result<Option<SessionObservation>> {
    key.validate()?;
    Ok(conn.query_row(&format!("SELECT {COLUMNS} FROM session_observations WHERE source=? AND session_id=? AND location=? AND connector_id=? AND connector_instance=?"), params![key.source,key.session_id,key.location.as_str(),key.connector_id,key.connector_instance],read_row).optional()?)
}

/// Persist one connector's observation and refresh only the aggregate projection.
/// The caller's transaction includes the canonical session write and this state.
pub fn upsert(conn: &Connection, observation: &SessionObservation) -> Result<()> {
    let savepoint = if conn.is_autocommit() {
        Some(conn.unchecked_transaction()?)
    } else {
        None
    };
    let result = upsert_inner(conn, observation);
    match (savepoint, result) {
        (Some(tx), Ok(())) => {
            tx.commit()?;
            Ok(())
        }
        (_, result) => result,
    }
}

fn upsert_inner(conn: &Connection, observation: &SessionObservation) -> Result<()> {
    let k = &observation.key;
    k.validate()?;
    ensure!(
        ["available", "unavailable", "withdrawn"].contains(&observation.access_state.as_str()),
        "invalid observation access state"
    );
    ensure!(
        ["shallow", "full"].contains(&observation.discovery_state.as_str()),
        "invalid observation discovery state"
    );
    conn.execute("INSERT INTO session_observations(source,session_id,location,connector_id,connector_instance,raw_locator,source_stamp,discovery_state,access_state,updated_ms) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source,session_id,location,connector_id,connector_instance) DO UPDATE SET raw_locator=excluded.raw_locator,source_stamp=excluded.source_stamp,discovery_state=CASE WHEN session_observations.discovery_state='full' THEN 'full' ELSE excluded.discovery_state END,access_state=excluded.access_state,updated_ms=excluded.updated_ms",params![k.source,k.session_id,k.location.as_str(),k.connector_id,k.connector_instance,observation.raw_locator,observation.source_stamp,observation.discovery_state,observation.access_state,observation.updated_ms])?;
    refresh_projection(conn, k)?;
    bump_revision(conn, k)
}

/// Withdrawal is an access change, not deletion of cached evidence or provenance.
pub fn set_access(conn: &Connection, key: &ObservationKey, state: &str) -> Result<()> {
    if let Some(mut observation) = get(conn, key)? {
        observation.access_state = state.into();
        observation.updated_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64;
        upsert(conn, &observation)?;
    }
    Ok(())
}

fn refresh_projection(conn: &Connection, key: &ObservationKey) -> Result<()> {
    conn.execute("INSERT INTO session_presences(source,session_id,location,raw_locator,source_stamp,discovery_state) SELECT source,session_id,location,raw_locator,source_stamp,CASE WHEN EXISTS(SELECT 1 FROM session_observations o WHERE o.source=?1 AND o.session_id=?2 AND o.location=?3 AND o.discovery_state='full') OR EXISTS(SELECT 1 FROM session_presences p WHERE p.source=?1 AND p.session_id=?2 AND p.location=?3 AND p.discovery_state='full') THEN 'full' ELSE discovery_state END FROM session_observations WHERE source=?1 AND session_id=?2 AND location=?3 ORDER BY connector_id='legacy-unknown',access_state!='available',connector_id,connector_instance LIMIT 1 ON CONFLICT(source,session_id,location) DO UPDATE SET raw_locator=excluded.raw_locator,source_stamp=excluded.source_stamp,discovery_state=excluded.discovery_state",params![key.source,key.session_id,key.location.as_str()])?;
    Ok(())
}

pub fn checkpoint(
    conn: &Connection,
    key: &ObservationKey,
) -> Result<Option<ObservationCheckpoint>> {
    Ok(conn.query_row("SELECT source_stamp,parser_version,last_event_at_ms,source_bytes,records_parsed,include_related,updated_ms,committed_offset,prefix_hash,dev_ino,parser_state_json FROM observation_hydration_checkpoints WHERE source=? AND session_id=? AND location=? AND connector_id=? AND connector_instance=?",params![key.source,key.session_id,key.location.as_str(),key.connector_id,key.connector_instance],|r|Ok(ObservationCheckpoint{source_stamp:r.get(0)?,parser_version:r.get(1)?,last_event_at_ms:r.get(2)?,source_bytes:r.get(3)?,records_parsed:r.get(4)?,include_related:r.get(5)?,updated_ms:r.get(6)?,committed_offset:r.get(7)?,prefix_hash:r.get(8)?,dev_ino:r.get(9)?,parser_state_json:r.get(10)?})).optional()?)
}

pub fn write_checkpoint(
    conn: &Connection,
    key: &ObservationKey,
    checkpoint: &ObservationCheckpoint,
) -> Result<()> {
    let transaction = if conn.is_autocommit() {
        Some(conn.unchecked_transaction()?)
    } else {
        None
    };
    write_checkpoint_inner(conn, key, checkpoint)?;
    if let Some(transaction) = transaction {
        transaction.commit()?;
    }
    Ok(())
}
fn write_checkpoint_inner(
    conn: &Connection,
    key: &ObservationKey,
    checkpoint: &ObservationCheckpoint,
) -> Result<()> {
    key.validate()?;
    ensure!(
        get(conn, key)?.is_some(),
        "observation checkpoint requires an observation"
    );
    conn.execute("INSERT INTO observation_hydration_checkpoints(source,session_id,location,connector_id,connector_instance,source_stamp,parser_version,last_event_at_ms,source_bytes,records_parsed,include_related,updated_ms,committed_offset,prefix_hash,dev_ino,parser_state_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source,session_id,location,connector_id,connector_instance) DO UPDATE SET source_stamp=excluded.source_stamp,parser_version=excluded.parser_version,last_event_at_ms=excluded.last_event_at_ms,source_bytes=excluded.source_bytes,records_parsed=excluded.records_parsed,include_related=excluded.include_related,updated_ms=excluded.updated_ms,committed_offset=excluded.committed_offset,prefix_hash=excluded.prefix_hash,dev_ino=excluded.dev_ino,parser_state_json=excluded.parser_state_json",params![key.source,key.session_id,key.location.as_str(),key.connector_id,key.connector_instance,checkpoint.source_stamp,checkpoint.parser_version,checkpoint.last_event_at_ms,checkpoint.source_bytes,checkpoint.records_parsed,checkpoint.include_related,checkpoint.updated_ms,checkpoint.committed_offset,checkpoint.prefix_hash,checkpoint.dev_ino,checkpoint.parser_state_json])?;
    bump_revision(conn, key)
}

/// Replace one successful snapshot as independently deliverable records. Long
/// transcripts do not become one oversized export record. An individual event
/// can still exceed a destination's explicit record limit, just like its
/// canonical session_event row; that failure never advances delivery progress.
pub fn save_evidence(
    conn: &Connection,
    key: &ObservationKey,
    payload: &serde_json::Value,
) -> Result<()> {
    let transaction = if conn.is_autocommit() {
        Some(conn.unchecked_transaction()?)
    } else {
        None
    };
    save_evidence_inner(conn, key, payload)?;
    if let Some(transaction) = transaction {
        transaction.commit()?;
    }
    Ok(())
}

fn save_evidence_inner(
    conn: &Connection,
    key: &ObservationKey,
    payload: &serde_json::Value,
) -> Result<()> {
    use serde_json::{json, Value};
    ensure!(
        get(conn, key)?.is_some(),
        "evidence requires an observation"
    );
    let format = payload
        .get("format")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("observation evidence format is required"))?;
    let mut records = std::collections::BTreeMap::new();
    match format {
        "records" => {
            use sha2::{Digest, Sha256};
            let items = payload
                .get("records")
                .and_then(Value::as_array)
                .context("normalized records are required")?;
            records.insert(
                "manifest".into(),
                // `acquired_kinds` rides the manifest beside the accumulated
                // set: the shredded rows carry records, and a pass that
                // narrowed coverage is only detectable against what the last
                // acquisition covered.
                json!({"format":format,"covered_kinds":payload["covered_kinds"],"acquired_kinds":payload["acquired_kinds"]}),
            );
            for (index, item) in items.iter().enumerate() {
                let record: crate::source_evidence::EvidenceRecord =
                    serde_json::from_value(item["record"].clone())?;
                let uid = format!("record:{:x}", Sha256::digest(record.identity()));
                ensure!(
                    records
                        .insert(uid, json!({"format":format,"index":index,"item":item}))
                        .is_none(),
                    "duplicate normalized observation record"
                );
            }
        }
        "events" => {
            let events = payload
                .get("events")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow::anyhow!("observation events are required"))?;
            let managed = payload
                .get("managed")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect::<std::collections::HashSet<_>>();
            for (index, event) in events.iter().enumerate() {
                let uid = event
                    .get("event_uid")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("event identity is required"))?;
                ensure!(
                    !uid.is_empty() && uid.len() <= 4096,
                    "invalid event identity"
                );
                ensure!(records.insert(format!("event:{uid}"),json!({"format":format,"index":index,"event":event,"managed":managed.contains(uid)})).is_none(),"duplicate observation event");
            }
        }
        "claude" => {
            let values = payload
                .get("records")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow::anyhow!("provider records are required"))?;
            for (index, record) in values.iter().enumerate() {
                records.insert(
                    format!("record:{index:020}"),
                    json!({"format":format,"index":index,"record":record}),
                );
            }
        }
        "codex-diff" => {
            let diff = payload
                .get("diff")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("provider diff is required"))?;
            let mut start = 0;
            let mut index = 0;
            while start < diff.len() {
                let mut end = (start + 64 * 1024).min(diff.len());
                while !diff.is_char_boundary(end) {
                    end -= 1;
                }
                records.insert(
                    format!("chunk:{index:020}"),
                    json!({"format":format,"index":index,"chunk":&diff[start..end]}),
                );
                start = end;
                index += 1;
            }
        }
        _ => anyhow::bail!("unsupported observation evidence format"),
    }
    if records.is_empty() {
        records.insert("empty".into(), json!({"format":format,"empty":true}));
    }
    let prior=conn.prepare("SELECT evidence_uid FROM observation_evidence WHERE source=? AND session_id=? AND location=? AND connector_id=? AND connector_instance=?")?.query_map(params![key.source,key.session_id,key.location.as_str(),key.connector_id,key.connector_instance],|row|row.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    for uid in prior {
        if !records.contains_key(&uid) {
            conn.execute("DELETE FROM observation_evidence WHERE source=? AND session_id=? AND location=? AND connector_id=? AND connector_instance=? AND evidence_uid=?",params![key.source,key.session_id,key.location.as_str(),key.connector_id,key.connector_instance,uid])?;
        }
    }
    for (uid, payload) in records {
        conn.execute("INSERT INTO observation_evidence(source,session_id,location,connector_id,connector_instance,evidence_uid,payload_json) VALUES(?,?,?,?,?,?,?) ON CONFLICT(source,session_id,location,connector_id,connector_instance,evidence_uid) DO UPDATE SET payload_json=excluded.payload_json WHERE observation_evidence.payload_json!=excluded.payload_json",params![key.source,key.session_id,key.location.as_str(),key.connector_id,key.connector_instance,uid,serde_json::to_string(&payload)?])?;
    }
    bump_revision(conn, key)
}

/// Reconstruct one connector's snapshot for canonical evidence reconciliation.
pub fn evidence(conn: &Connection, key: &ObservationKey) -> Result<Option<serde_json::Value>> {
    use serde_json::{json, Value};
    key.validate()?;
    let payloads=conn.prepare("SELECT payload_json FROM observation_evidence WHERE source=? AND session_id=? AND location=? AND connector_id=? AND connector_instance=? ORDER BY evidence_uid")?.query_map(params![key.source,key.session_id,key.location.as_str(),key.connector_id,key.connector_instance],|row|row.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut rows = payloads
        .into_iter()
        .map(|payload| serde_json::from_str::<Value>(&payload))
        .collect::<serde_json::Result<Vec<_>>>()?;
    if rows.is_empty() {
        return Ok(None);
    }
    rows.sort_by_key(|row| row.get("index").and_then(Value::as_u64).unwrap_or_default());
    let format = rows[0]
        .get("format")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("malformed observation evidence format"))?;
    ensure!(
        rows.iter()
            .all(|row| row.get("format").and_then(Value::as_str) == Some(format)),
        "mixed observation evidence formats"
    );
    Ok(Some(match format {
        "records" => {
            json!({"format":format,"covered_kinds":rows.iter().find_map(|row|row.get("covered_kinds")).context("missing snapshot manifest")?,"acquired_kinds":rows.iter().find_map(|row|row.get("acquired_kinds")).cloned().unwrap_or(Value::Null),"records":rows.iter().filter_map(|row|row.get("item")).collect::<Vec<_>>()})
        }
        "events" => {
            json!({"format":format,"events":rows.iter().filter_map(|row|row.get("event")).collect::<Vec<_>>(),"managed":rows.iter().filter(|row|row.get("managed").and_then(Value::as_bool)==Some(true)).filter_map(|row|row.get("event").and_then(|event|event.get("event_uid"))).collect::<Vec<_>>()})
        }
        "claude" => {
            json!({"format":format,"records":rows.iter().filter_map(|row|row.get("record")).collect::<Vec<_>>()})
        }
        "codex-diff" => {
            json!({"format":format,"diff":rows.iter().filter_map(|row|row.get("chunk").and_then(Value::as_str)).collect::<String>()})
        }
        _ => anyhow::bail!("unsupported stored observation evidence format"),
    }))
}

/// Opaque persisted revision for optimistic acquisition outside the engine.
/// The database-wide monotonic clock prevents ABA after delete/recreate and
/// distinguishes updates sharing the same millisecond timestamp.
pub fn revision(conn: &Connection, key: &ObservationKey) -> Result<Option<String>> {
    key.validate()?;
    let version:Option<i64>=conn.query_row("SELECT version FROM observation_versions WHERE source=? AND session_id=? AND location=? AND connector_id=? AND connector_instance=?",params![key.source,key.session_id,key.location.as_str(),key.connector_id,key.connector_instance],|row|row.get(0)).optional()?;
    Ok(version.map(|version| format!("v1:{version}")))
}
fn bump_revision(conn: &Connection, key: &ObservationKey) -> Result<()> {
    let version: i64 = conn.query_row(
        "UPDATE observation_clock SET version=version+1 WHERE singleton=1 RETURNING version",
        [],
        |row| row.get(0),
    )?;
    conn.execute("INSERT INTO observation_versions(source,session_id,location,connector_id,connector_instance,version) VALUES(?,?,?,?,?,?) ON CONFLICT(source,session_id,location,connector_id,connector_instance) DO UPDATE SET version=excluded.version",params![key.source,key.session_id,key.location.as_str(),key.connector_id,key.connector_instance,version])?;
    Ok(())
}

/// Canonical rows with local or unknown ownership cannot be changed by a remote
/// projection. This local reconciliation state is not connector evidence and
/// must not advance observation revisions or enter delivery capture.
pub fn protected_canonical_evidence(
    conn: &Connection,
    key: &ObservationKey,
) -> Result<std::collections::BTreeSet<String>> {
    let mut statement = conn.prepare(
        "SELECT record_identity FROM canonical_evidence_protection WHERE source=? AND session_id=?",
    )?;
    let rows = statement.query_map(params![key.source, key.session_id], |row| row.get(0))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Persist protection independently of the lifetime of any source observation.
pub fn protect_canonical_evidence(
    conn: &Connection,
    key: &ObservationKey,
    identity: &str,
) -> Result<()> {
    conn.execute("INSERT OR IGNORE INTO canonical_evidence_protection(source,session_id,record_identity) VALUES(?,?,?)", params![key.source,key.session_id,identity])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn observation(id: &str, instance: &str) -> SessionObservation {
        SessionObservation {
            key: ObservationKey {
                source: "claude".into(),
                session_id: "s".into(),
                location: SessionLocation::Remote,
                connector_id: id.into(),
                connector_instance: instance.into(),
            },
            raw_locator: Some(format!("{id}/{instance}/s")),
            source_stamp: Some(format!("{id}-v1")),
            discovery_state: "shallow".into(),
            access_state: "available".into(),
            updated_ms: 1,
        }
    }
    #[test]
    fn independent_observations_and_checkpoints_survive_order_withdrawal_and_reopen() -> Result<()>
    {
        for reverse in [false, true] {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("history.db");
            let conn = crate::open_db(&path)?;
            let a = observation("a", "account1");
            let b = observation("b", "account2");
            let order = if reverse { [&b, &a] } else { [&a, &b] };
            for value in order {
                upsert(&conn, value)?;
            }
            write_checkpoint(
                &conn,
                &a.key,
                &ObservationCheckpoint {
                    source_stamp: Some("hydrated-a".into()),
                    parser_version: 2,
                    last_event_at_ms: None,
                    source_bytes: 123,
                    records_parsed: 1,
                    include_related: false,
                    updated_ms: 3,
                    committed_offset: 0,
                    prefix_hash: None,
                    dev_ino: None,
                    parser_state_json: None,
                },
            )?;
            save_evidence(
                &conn,
                &a.key,
                &serde_json::json!({"format":"claude","records":[{"uuid":"a"}]}),
            )?;
            set_access(&conn, &b.key, "withdrawn")?;
            drop(conn);
            let conn = crate::open_db(&path)?;
            assert_eq!(list(&conn, "claude", "s")?.len(), 2);
            assert_eq!(checkpoint(&conn, &a.key)?.unwrap().source_bytes, 123);
            assert!(checkpoint(&conn, &b.key)?.is_none());
            assert_eq!(get(&conn, &b.key)?.unwrap().access_state, "withdrawn");
            assert_eq!(
                evidence(&conn, &a.key)?,
                Some(serde_json::json!({"format":"claude","records":[{"uuid":"a"}]}))
            );
            let locator:String=conn.query_row("SELECT raw_locator FROM session_presences WHERE source='claude' AND session_id='s'",[],|r|r.get(0))?;
            assert_eq!(locator, "a/account1/s");
        }
        Ok(())
    }
    #[test]
    fn legacy_migration_keeps_unknown_provenance_without_trusting_old_checkpoint() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::init_db(&conn)?;
        conn.execute(
            "DELETE FROM schema_migrations WHERE name='connector_observations_v1'",
            [],
        )?;
        conn.execute("INSERT INTO session_presences(source,session_id,location,raw_locator,source_stamp,discovery_state) VALUES('claude','s','remote','old-locator','old-stamp','full')",[])?;
        init_schema(&conn)?;
        init_schema(&conn)?;
        let rows = list(&conn, "claude", "s")?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key.connector_id, "legacy-unknown");
        assert_eq!(rows[0].raw_locator.as_deref(), Some("old-locator"));
        assert!(checkpoint(&conn, &rows[0].key)?.is_none());
        upsert(&conn, &observation("a", "default"))?;
        init_schema(&conn)?;
        assert_eq!(list(&conn, "claude", "s")?.len(), 2);
        Ok(())
    }
    #[test]
    fn malformed_identity_and_projection_failure_leave_no_partial_observation() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::init_db(&conn)?;
        let mut value = observation("a", "default");
        value.key.connector_instance = " ".into();
        assert!(upsert(&conn, &value).is_err());
        value.key.connector_instance = "default".into();
        conn.execute_batch("CREATE TRIGGER fail_projection BEFORE INSERT ON session_presences BEGIN SELECT RAISE(ABORT,'capture full'); END;")?;
        assert!(upsert(&conn, &value).is_err());
        assert!(list(&conn, "claude", "s")?.is_empty());
        Ok(())
    }
    #[test]
    fn partial_upgrade_repairs_missing_auxiliary_schema_and_rejects_malformed_tables() -> Result<()>
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("history.db");
        let conn = crate::open_db(&path)?;
        upsert(&conn, &observation("a", "default"))?;
        conn.execute_batch("DROP INDEX idx_observation_locator; DROP TRIGGER delete_session_observations; DROP TABLE observation_hydration_checkpoints;")?;
        drop(conn);
        let conn = crate::open_db(&path)?;
        assert!(schema_is_current(&conn)?);
        assert_eq!(list(&conn, "claude", "s")?.len(), 1);
        conn.execute_batch(
            "DROP TABLE observation_evidence; CREATE TABLE observation_evidence(source TEXT);",
        )?;
        assert!(!schema_is_current(&conn)?);
        drop(conn);
        assert!(crate::open_db(&path).is_err());
        Ok(())
    }
    #[test]
    fn observation_evidence_splits_large_diff_and_reconstructs_exact_unicode_bytes() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::init_db(&conn)?;
        let observation = observation("a", "default");
        upsert(&conn, &observation)?;
        let diff = "λ\n".repeat(500_000);
        let payload = serde_json::json!({"format":"codex-diff","diff":diff});
        save_evidence(&conn, &observation.key, &payload)?;
        assert_eq!(evidence(&conn, &observation.key)?, Some(payload));
        let (count, largest): (i64, i64) = conn.query_row(
            "SELECT COUNT(*),MAX(length(CAST(payload_json AS BLOB))) FROM observation_evidence",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert!(count > 20);
        assert!(largest < 100_000);
        save_evidence(
            &conn,
            &observation.key,
            &serde_json::json!({"format":"codex-diff","diff":"short"}),
        )?;
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM observation_evidence", [], |row| row
                .get::<_, i64>(
                0
            ))?,
            1
        );
        Ok(())
    }
    #[test]
    fn deleting_a_canonical_session_removes_its_observations_and_owned_progress() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        crate::init_db(&conn)?;
        conn.execute(
            "INSERT INTO sessions(source,session_id) VALUES('claude','s')",
            [],
        )?;
        let observation = observation("a", "default");
        upsert(&conn, &observation)?;
        save_evidence(
            &conn,
            &observation.key,
            &serde_json::json!({"format":"claude","records":[]}),
        )?;
        write_checkpoint(
            &conn,
            &observation.key,
            &ObservationCheckpoint {
                source_stamp: None,
                parser_version: 1,
                last_event_at_ms: None,
                source_bytes: 0,
                records_parsed: 0,
                include_related: false,
                updated_ms: 1,
                committed_offset: 0,
                prefix_hash: None,
                dev_ino: None,
                parser_state_json: None,
            },
        )?;
        conn.execute(
            "DELETE FROM sessions WHERE source='claude' AND session_id='s'",
            [],
        )?;
        assert!(list(&conn, "claude", "s")?.is_empty());
        assert!(checkpoint(&conn, &observation.key)?.is_none());
        assert!(evidence(&conn, &observation.key)?.is_none());
        Ok(())
    }
}
