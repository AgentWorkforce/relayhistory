use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};

pub(super) struct Table {
    pub name: &'static str,
    pub kind: &'static str,
    pub source: &'static str,
    pub session: &'static str,
    pub key: &'static [&'static str],
}

/// Every consumer a capture trigger has to serve: live delivery jobs and
/// unexpired file exports. Named once so the preimage sweep below cannot drift
/// from the triggers it stands in for.
const CONSUMERS: &str = "SELECT id,state,bootstrap_done,bootstrap_kind,bootstrap_rowid FROM delivery_jobs WHERE state <> 'cancelled' UNION ALL SELECT id,'active',bootstrap_done,bootstrap_kind,bootstrap_rowid FROM history_exports WHERE expires_at_ms > CAST(unixepoch('subsec')*1000 AS INTEGER)";

pub(super) const TABLES: &[Table] = &[
    Table {
        name: "history",
        kind: "history",
        source: "source",
        session: "session_id",
        key: &["source", "timestamp_ms", "prompt"],
    },
    Table {
        name: "session_events",
        kind: "session_event",
        source: "source",
        session: "session_id",
        key: &["source", "session_id", "event_uid"],
    },
    Table {
        name: "tool_calls",
        kind: "tool_call",
        source: "source",
        session: "session_id",
        key: &["source", "session_id", "tool_use_id"],
    },
    Table {
        name: "file_edits",
        kind: "file_edit",
        source: "source",
        session: "session_id",
        key: &["source", "session_id", "tool_use_id"],
    },
    Table {
        name: "sessions",
        kind: "session",
        source: "source",
        session: "session_id",
        key: &["source", "session_id"],
    },
    Table {
        name: "session_presences",
        kind: "presence",
        source: "source",
        session: "session_id",
        key: &["source", "session_id", "location"],
    },
    Table {
        name: "session_relationships",
        kind: "relationship",
        source: "source",
        session: "parent_session_id",
        key: &["source", "parent_session_id", "relationship_uid"],
    },
    Table {
        name: "session_commit_links",
        kind: "commit_link",
        source: "source",
        session: "session_id",
        key: &["source", "session_id", "commit_sha", "match_method"],
    },
    Table {
        name: "trajectories",
        kind: "trajectory",
        source: "'trajectory'",
        session: "id",
        key: &["id"],
    },
    // Append new kinds: bootstrap_kind is a persisted index into this list.
    Table {
        name: "session_observations",
        kind: "source_observation",
        source: "source",
        session: "session_id",
        key: &[
            "source",
            "session_id",
            "location",
            "connector_id",
            "connector_instance",
        ],
    },
    Table {
        name: "observation_evidence",
        kind: "observation_evidence",
        source: "source",
        session: "session_id",
        key: &[
            "source",
            "session_id",
            "location",
            "connector_id",
            "connector_instance",
            "evidence_uid",
        ],
    },
    Table {
        name: "session_markers",
        kind: "session_marker",
        source: "source",
        session: "session_id",
        key: &["source", "session_id", "marker_uid"],
    },
];

impl Table {
    pub fn source(&self, row: &str) -> String {
        if self.source.starts_with('\'') {
            self.source.into()
        } else {
            format!("{row}.{}", self.source)
        }
    }
    pub fn key(&self, row: &str) -> String {
        format!(
            "json_array('{}',{})",
            self.kind,
            self.key
                .iter()
                .map(|key| format!("{row}.{key}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    }
    pub fn payload(&self, conn: &Connection, row: &str) -> Result<String> {
        let columns = conn
            .prepare(&format!(
                "SELECT name FROM pragma_table_info('{}')",
                self.name
            ))?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(format!(
            "json_object({})",
            columns
                .iter()
                .map(|column| format!("'{column}',{row}.\"{column}\""))
                .collect::<Vec<_>>()
                .join(",")
        ))
    }
}

/// Drop a table's capture triggers when they predate one of its columns.
///
/// Each trigger's payload is a `json_object` over the column list as it stood
/// when the trigger was created, and every trigger is created `IF NOT EXISTS`.
/// So a migration that adds a column to a captured table would otherwise leave
/// the old trigger in place and deliver that column's value to no one, for the
/// life of the database — a delivery that keeps reporting success while
/// shipping an incomplete row. Dropping the stale trigger here is what makes
/// adding a column to a captured table a one-place change.
///
/// Only genuine drift drops anything: the common case reads one `sqlite_master`
/// row per table and leaves it alone.
fn drop_triggers_that_predate_a_column(conn: &Connection, table: &Table) -> Result<()> {
    let name = table.name;
    let existing: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
            [format!("delivery_{name}_insert")],
            |row| row.get(0),
        )
        .optional()?;
    let Some(existing) = existing else {
        return Ok(());
    };
    let columns = conn
        .prepare(&format!("SELECT name FROM pragma_table_info('{name}')"))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if columns
        .iter()
        .all(|column| existing.contains(&format!("'{column}',NEW.\"{column}\"")))
    {
        return Ok(());
    }
    conn.execute_batch(&format!(
        "DROP TRIGGER IF EXISTS delivery_{name}_insert; \
         DROP TRIGGER IF EXISTS delivery_{name}_update; \
         DROP TRIGGER IF EXISTS delivery_{name}_delete;"
    ))?;
    Ok(())
}

pub(crate) fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!(r#"
CREATE TABLE IF NOT EXISTS delivery_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1), origin_id TEXT NOT NULL,
    retained_bytes INTEGER NOT NULL DEFAULT 0, max_retained_bytes INTEGER NOT NULL DEFAULT {retention_limit}
);
INSERT OR IGNORE INTO delivery_state(singleton, origin_id) VALUES (1, lower(hex(randomblob(16))));
CREATE TABLE IF NOT EXISTS delivery_jobs (
    id TEXT PRIMARY KEY, destination_id TEXT NOT NULL, instance_id TEXT NOT NULL, account_id TEXT NOT NULL,
    generation INTEGER NOT NULL, config_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('active','paused','blocked','cancelled')),
    created_ms INTEGER NOT NULL, cutoff INTEGER NOT NULL, journal_cursor INTEGER NOT NULL,
    bootstrap_kind INTEGER NOT NULL DEFAULT 0, bootstrap_rowid INTEGER NOT NULL DEFAULT 0,
    bootstrap_done INTEGER NOT NULL DEFAULT 0, acknowledged_cursor INTEGER NOT NULL DEFAULT 0,
    fence INTEGER NOT NULL DEFAULT 0, worker_id TEXT, lease_until_ms INTEGER,
    next_attempt_ms INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
    last_attempt_ms INTEGER, last_acknowledged_ms INTEGER, acceptance_level TEXT, failure TEXT,
    suppressed_records INTEGER NOT NULL DEFAULT 0, acknowledged_records INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS delivery_active_instance ON delivery_jobs(destination_id,instance_id,account_id) WHERE state <> 'cancelled';
CREATE TABLE IF NOT EXISTS delivery_bootstrap_bounds (
    job_id TEXT NOT NULL, kind TEXT NOT NULL, max_rowid INTEGER NOT NULL,
    PRIMARY KEY(job_id,kind)
);
CREATE TABLE IF NOT EXISTS delivery_shadow (
    job_id TEXT NOT NULL, kind TEXT NOT NULL, row_id INTEGER NOT NULL,
    source TEXT NOT NULL, session_id TEXT, record_key TEXT NOT NULL, payload TEXT,
    PRIMARY KEY(job_id,kind,row_id)
);
CREATE TABLE IF NOT EXISTS delivery_journal (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, source TEXT NOT NULL,
    session_id TEXT, record_key TEXT NOT NULL, operation TEXT NOT NULL, payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS delivery_batches (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, job_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','leased','retry_wait','blocked','acknowledged','suppressed','cancelled')),
    payload TEXT, prepared TEXT, records INTEGER NOT NULL, bytes INTEGER NOT NULL,
    journal_end INTEGER NOT NULL, created_ms INTEGER NOT NULL, accepted_records INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS delivery_one_pending_batch ON delivery_batches(job_id) WHERE state IN ('pending','leased','retry_wait','blocked');
CREATE INDEX IF NOT EXISTS delivery_batch_job ON delivery_batches(job_id, seq);
CREATE TABLE IF NOT EXISTS history_exports (
 id TEXT PRIMARY KEY, selection_json TEXT NOT NULL, limits_json TEXT NOT NULL,
 cutoff INTEGER NOT NULL, bootstrap_kind INTEGER NOT NULL DEFAULT 0,
 bootstrap_rowid INTEGER NOT NULL DEFAULT 0, bootstrap_done INTEGER NOT NULL DEFAULT 0,
 expires_at_ms INTEGER NOT NULL, cursor TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS history_export_pages (
 cursor TEXT PRIMARY KEY, export_id TEXT NOT NULL, payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS history_export_page_owner ON history_export_pages(export_id);
CREATE TABLE IF NOT EXISTS delivery_exclusions (
    source TEXT NOT NULL, session_id TEXT NOT NULL, PRIMARY KEY(source,session_id)
);
"#, retention_limit = super::DEFAULT_RETENTION_LIMIT_BYTES))?;
    // A pre-upgrade job/export has no snapshot boundary for newly introduced
    // tables. Give it an empty historical snapshot; new observation writes are
    // captured by the journal. A newly created generation exports current rows.
    for kind in [
        "source_observation",
        "observation_evidence",
        "session_marker",
    ] {
        conn.execute("INSERT OR IGNORE INTO delivery_bootstrap_bounds(job_id,kind,max_rowid) SELECT id,?,0 FROM delivery_jobs UNION ALL SELECT id,?,0 FROM history_exports",[kind,kind])?;
    }
    // Count retained logical bytes, including key/payload duplication and row
    // overhead. This is a retention cap, not a promise about SQLite page size.
    for (table, size) in [
        ("history_export_pages", "length(CAST(payload AS BLOB))+512"),
        ("delivery_journal", "length(CAST(payload AS BLOB))+length(CAST(record_key AS BLOB))+512"),
        ("delivery_shadow", "coalesce(length(CAST(payload AS BLOB)),0)+length(CAST(record_key AS BLOB))+512"),
        ("delivery_batches", "coalesce(length(CAST(payload AS BLOB)),0)+coalesce(length(CAST(prepared AS BLOB)),0)+512"),
    ] {
        let qualified = |row: &str| size.replace("payload", &format!("{row}.payload")).replace("record_key", &format!("{row}.record_key")).replace("prepared", &format!("{row}.prepared"));
        let new = qualified("NEW");
        let old = qualified("OLD");
        conn.execute_batch(&format!(r#"
CREATE TRIGGER IF NOT EXISTS {table}_cap_insert BEFORE INSERT ON {table} BEGIN
 SELECT CASE WHEN (SELECT retained_bytes+({new})>max_retained_bytes FROM delivery_state WHERE singleton=1) THEN RAISE(ABORT,'delivery retention limit exceeded; compact consumed data or raise the retention cap') END;
END;
CREATE TRIGGER IF NOT EXISTS {table}_count_insert AFTER INSERT ON {table} BEGIN
 UPDATE delivery_state SET retained_bytes=retained_bytes+({new}) WHERE singleton=1;
END;
CREATE TRIGGER IF NOT EXISTS {table}_cap_update BEFORE UPDATE ON {table} BEGIN
 SELECT CASE WHEN (SELECT retained_bytes+({new})-({old})>max_retained_bytes FROM delivery_state WHERE singleton=1) THEN RAISE(ABORT,'delivery retention limit exceeded; compact consumed data or raise the retention cap') END;
END;
CREATE TRIGGER IF NOT EXISTS {table}_count_update AFTER UPDATE ON {table} BEGIN
 UPDATE delivery_state SET retained_bytes=retained_bytes+({new})-({old}) WHERE singleton=1;
END;
CREATE TRIGGER IF NOT EXISTS {table}_count_delete AFTER DELETE ON {table} BEGIN
 UPDATE delivery_state SET retained_bytes=retained_bytes-({old}) WHERE singleton=1;
END;
"#))?;
    }
    let consumers = CONSUMERS;
    for table in TABLES {
        drop_triggers_that_predate_a_column(conn, table)?;
    }
    for (index, table) in TABLES.iter().enumerate() {
        let old_payload = table.payload(conn, "OLD")?;
        let new_payload = table.payload(conn, "NEW")?;
        let old_key = table.key("OLD");
        let new_key = table.key("NEW");
        let old_source = table.source("OLD");
        let new_source = table.source("NEW");
        let name = table.name;
        let kind = table.kind;
        let session = table.session;
        let shadow = |row: &str, payload: &str| {
            format!(
                r#"
 INSERT OR IGNORE INTO delivery_shadow(job_id,kind,row_id,source,session_id,record_key,payload)
 SELECT j.id,'{kind}',{row}.rowid,{source},{row}.{session},{key},{payload}
 FROM ({consumers}) j JOIN delivery_bootstrap_bounds b ON b.job_id=j.id AND b.kind='{kind}'
 WHERE j.state <> 'cancelled' AND j.bootstrap_done=0 AND {row}.rowid <= b.max_rowid
 AND (j.bootstrap_kind < {index} OR (j.bootstrap_kind={index} AND j.bootstrap_rowid < {row}.rowid))
 AND NOT EXISTS(SELECT 1 FROM delivery_shadow s WHERE s.job_id=j.id AND s.kind='{kind}' AND s.row_id={row}.rowid);
"#,
                source = table.source(row),
                key = table.key(row)
            )
        };
        let insert_shadow = shadow("NEW", "NULL");
        let before_shadow = shadow("OLD", &old_payload);
        // A capture trigger embeds the column list the table had when it was
        // created. `CREATE TRIGGER IF NOT EXISTS` leaves that in place, so a
        // table that later gains a column keeps being captured in the old
        // shape: delivery goes on reporting success while the new field never
        // reaches the destination. Rebuild any trigger whose payload no longer
        // matches the table it captures.
        let stale: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master \
             WHERE type='trigger' AND name=?1 AND instr(sql, ?2)=0)",
            rusqlite::params![format!("delivery_{name}_insert"), new_payload.as_str()],
            |row| row.get(0),
        )?;
        if stale {
            for operation in ["insert", "update", "delete"] {
                conn.execute_batch(&format!(
                    "DROP TRIGGER IF EXISTS delivery_{name}_{operation};"
                ))?;
            }
        }
        conn.execute_batch(&format!(r#"
CREATE TRIGGER IF NOT EXISTS delivery_{name}_insert AFTER INSERT ON {name}
WHEN EXISTS(SELECT 1 FROM ({consumers})) BEGIN
 {insert_shadow}
 INSERT INTO delivery_journal(kind,source,session_id,record_key,operation,payload)
 SELECT '{kind}',{new_source},NEW.{session},{new_key},'upsert',{new_payload} WHERE EXISTS(SELECT 1 FROM delivery_jobs WHERE state <> 'cancelled');
END;
CREATE TRIGGER IF NOT EXISTS delivery_{name}_update AFTER UPDATE ON {name}
WHEN EXISTS(SELECT 1 FROM ({consumers})) AND {old_payload} <> {new_payload} BEGIN
 {before_shadow}
 INSERT OR IGNORE INTO delivery_shadow(job_id,kind,row_id,source,session_id,record_key,payload)
 SELECT j.id,'{kind}',NEW.rowid,{new_source},NEW.{session},{new_key},NULL
 FROM ({consumers}) j JOIN delivery_bootstrap_bounds b ON b.job_id=j.id AND b.kind='{kind}'
 WHERE OLD.rowid <> NEW.rowid AND j.state <> 'cancelled' AND j.bootstrap_done=0 AND NEW.rowid <= b.max_rowid
 AND (j.bootstrap_kind < {index} OR (j.bootstrap_kind={index} AND j.bootstrap_rowid < NEW.rowid))
 AND NOT EXISTS(SELECT 1 FROM delivery_shadow s WHERE s.job_id=j.id AND s.kind='{kind}' AND s.row_id=NEW.rowid);
 INSERT INTO delivery_journal(kind,source,session_id,record_key,operation,payload)
 SELECT '{kind}',{old_source},OLD.{session},{old_key},'delete','null' WHERE {old_key} <> {new_key} AND EXISTS(SELECT 1 FROM delivery_jobs WHERE state <> 'cancelled');
 INSERT INTO delivery_journal(kind,source,session_id,record_key,operation,payload)
 SELECT '{kind}',{new_source},NEW.{session},{new_key},'upsert',{new_payload} WHERE EXISTS(SELECT 1 FROM delivery_jobs WHERE state <> 'cancelled');
END;
CREATE TRIGGER IF NOT EXISTS delivery_{name}_delete AFTER DELETE ON {name}
WHEN EXISTS(SELECT 1 FROM ({consumers})) BEGIN
 {before_shadow}
 INSERT INTO delivery_journal(kind,source,session_id,record_key,operation,payload)
 SELECT '{kind}',{old_source},OLD.{session},{old_key},'delete','null' WHERE EXISTS(SELECT 1 FROM delivery_jobs WHERE state <> 'cancelled');
END;
"#))?;
    }
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations(name) VALUES ('delivery_v1')",
        [],
    )?;
    Ok(())
}

/// Freeze the rows a migration is about to rewrite, for consumers mid-snapshot.
///
/// A bootstrap or an export fixes a rowid cutoff and reads its pages over time,
/// and the capture triggers keep that view immutable by writing the OLD row
/// into `delivery_shadow` whenever a row inside an unread bound changes. A
/// migration has to retire those triggers before it can drop a column, so the
/// rows it rewrites are exactly the rows nothing shadows.
///
/// Unlike the journal, this cannot be repaired afterwards: once the column is
/// gone the preimage exists nowhere to reconstruct from. So it is taken here,
/// before the first write, while the old shape is still the shape.
///
/// The predicate is the triggers' own, down to `INSERT OR IGNORE` and the
/// not-already-shadowed clause -- a consumer that has already shadowed this row
/// keeps the copy it took, and one that has read past it is not owed anything.
pub(crate) fn shadow_preimages(conn: &Connection, table_name: &str) -> Result<()> {
    let Some((index, table)) = TABLES
        .iter()
        .enumerate()
        .find(|(_, table)| table.name == table_name)
    else {
        return Ok(());
    };
    // The delivery schema is opt-in and this runs from `init_db`, before
    // `init_schema` has had a chance to create it -- on a database no
    // delivery-enabled build has ever opened, these tables are simply absent.
    // A statement naming a missing table fails when it is prepared, so the
    // check has to come first. Absent means no consumer, which means nothing
    // is owed.
    // `history_exports` is in the probe because `CONSUMERS` reads it. It is
    // created in the same batch as the other three today, so a database
    // carrying them without it is an older delivery-enabled install -- and
    // this runs from `init_db`, before `init_schema` puts the table back. A
    // statement naming a missing table fails when it is *prepared*, so
    // omitting it here did not degrade the sweep: it failed every open of that
    // database. Same crash class as `delivery_journal` and `delivery_shadow`,
    // reached through the fourth table.
    let ready: bool = conn
        .prepare(
            "SELECT count(*) = 4 FROM sqlite_master WHERE type = 'table' \
             AND name IN ('delivery_shadow','delivery_bootstrap_bounds','delivery_jobs',\
             'history_exports')",
        )?
        .query_row([], |row| row.get(0))?;
    if !ready {
        return Ok(());
    }
    let payload = table.payload(conn, "m")?;
    let key = table.key("m");
    let source = table.source("m");
    let kind = table.kind;
    let session = table.session;
    let name = table.name;
    conn.execute(
        &format!(
            "INSERT OR IGNORE INTO delivery_shadow\
             (job_id,kind,row_id,source,session_id,record_key,payload) \
             SELECT j.id,'{kind}',m.rowid,{source},m.{session},{key},{payload} \
             FROM {name} m \
             JOIN ({consumers}) j \
             JOIN delivery_bootstrap_bounds b ON b.job_id=j.id AND b.kind='{kind}' \
             WHERE j.state <> 'cancelled' AND j.bootstrap_done=0 AND m.rowid <= b.max_rowid \
             AND (j.bootstrap_kind < {index} \
                  OR (j.bootstrap_kind={index} AND j.bootstrap_rowid < m.rowid)) \
             AND NOT EXISTS(SELECT 1 FROM delivery_shadow s \
                            WHERE s.job_id=j.id AND s.kind='{kind}' AND s.row_id=m.rowid)",
            consumers = CONSUMERS,
        ),
        [],
    )?;
    Ok(())
}

/// Journal rows a migration rewrote behind the capture triggers' back.
///
/// A column migration has to retire the triggers first -- SQLite validates
/// them when a column is dropped -- so the writes it makes are invisible to
/// capture. They are also invisible to the rebuilt triggers afterwards, which
/// only see future writes, and a no-op touch cannot wake them either: their
/// `WHEN old_payload <> new_payload` guard is false for a row whose values did
/// not change. So the rows are journalled here, explicitly, once.
///
/// The payload is built by the same [`Table::payload`] the triggers use, from
/// `pragma_table_info` at call time, so it is the post-migration shape by
/// construction and cannot drift from what capture emits for the same row.
pub(crate) fn journal_migrated_rows(conn: &Connection, table_name: &str) -> Result<()> {
    let Some(table) = TABLES.iter().find(|table| table.name == table_name) else {
        return Ok(());
    };
    let payload = table.payload(conn, "m")?;
    let key = table.key("m");
    let source = table.source("m");
    let kind = table.kind;
    let session = table.session;
    // The interpolated name is the resolved entry's own `&'static str`, not
    // the caller's argument. Behaviour is identical -- the lookup above already
    // restricts it to `TABLES` -- but the safety is then visible in this one
    // statement rather than inferred from a return three lines up.
    let name = table.name;
    // Guarded by the same predicate the triggers use, so a database with no
    // subscriber writes nothing: a job created later bootstraps from the table
    // itself and already sees these rows.
    conn.execute(
        &format!(
            "INSERT INTO delivery_journal(kind,source,session_id,record_key,operation,payload) \
             SELECT '{kind}',{source},m.{session},{key},'upsert',{payload} \
             FROM {name} m \
             WHERE EXISTS(SELECT 1 FROM delivery_jobs WHERE state <> 'cancelled')",
        ),
        [],
    )?;
    Ok(())
}

/// Whether a retained capture trigger still emits the column list its table
/// has now.
///
/// Trigger *names* are what [`schema_is_current`] can check cheaply, but a
/// name says nothing about the payload: a trigger embeds the column list its
/// table had when it was created. Delivery is an optional feature, and that is
/// what opens the hole -- a database can carry delivery tables and triggers
/// from a delivery-enabled build, be opened by a `--no-default-features` build
/// that adds a column without compiling the rebuild in, and then come back to
/// a delivery-enabled build whose fast path the names alone satisfy. The
/// rebuild is never reached and delivery goes on reporting success while the
/// new field never leaves the machine.
///
/// Asked the same way the rebuild in `init_schema` asks it, so the two cannot
/// disagree about what "current" means.
fn capture_payload_is_current(conn: &Connection, table: &Table) -> Result<bool> {
    let name = table.name;
    let payload = table.payload(conn, "NEW")?;
    let stale: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master \
         WHERE type='trigger' AND name=?1 AND instr(sql, ?2)=0)",
        rusqlite::params![format!("delivery_{name}_insert"), payload.as_str()],
        |row| row.get(0),
    )?;
    Ok(!stale)
}

pub(crate) fn schema_is_current(conn: &Connection) -> Result<bool> {
    let mut names = Vec::new();
    for table in TABLES {
        for operation in ["insert", "update", "delete"] {
            names.push(format!("'delivery_{}_{}'", table.name, operation));
        }
    }
    for table in [
        "delivery_journal",
        "delivery_shadow",
        "delivery_batches",
        "history_export_pages",
    ] {
        for suffix in [
            "cap_insert",
            "count_insert",
            "cap_update",
            "count_update",
            "count_delete",
        ] {
            names.push(format!("'{table}_{suffix}'"));
        }
    }
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name IN ({})",
            names.join(",")
        ),
        [],
        |r| r.get(0),
    )?;
    if count as usize != names.len() {
        return Ok(false);
    }
    // A trigger can carry the right name and a payload that predates a column
    // its table has since gained; see `capture_payload_is_current`.
    for table in TABLES {
        if !capture_payload_is_current(conn, table)? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_db;

    /// A capture trigger that predates a column must not survive the upgrade
    /// that adds it: it would keep delivering rows that look complete and are
    /// missing a field, with nothing in the result to say so.
    #[test]
    fn a_capture_trigger_predating_a_column_is_rebuilt_on_the_next_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        let conn = open_db(&path).unwrap();
        // Recreate the session_relationships capture as it stood before
        // `origin_session_id`, which is exactly what an older release left.
        let stale = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='trigger' \
                 AND name='delivery_session_relationships_insert'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
            .replace("'origin_session_id',NEW.\"origin_session_id\",", "")
            .replace(",'origin_session_id',NEW.\"origin_session_id\"", "");
        conn.execute_batch("DROP TRIGGER delivery_session_relationships_insert;")
            .unwrap();
        conn.execute_batch(&stale).unwrap();
        assert!(!schema_is_current(&conn).unwrap());
        drop(conn);

        let conn = open_db(&path).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        let rebuilt: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='trigger' \
                 AND name='delivery_session_relationships_insert'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(rebuilt.contains("'origin_session_id',NEW.\"origin_session_id\""));
    }
}
