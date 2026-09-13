use anyhow::Result;
use rusqlite::Connection;

pub(super) struct Table {
    pub name: &'static str,
    pub kind: &'static str,
    pub source: &'static str,
    pub session: &'static str,
    pub key: &'static [&'static str],
}

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

pub(crate) fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(r#"
CREATE TABLE IF NOT EXISTS delivery_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1), origin_id TEXT NOT NULL,
    retained_bytes INTEGER NOT NULL DEFAULT 0, max_retained_bytes INTEGER NOT NULL DEFAULT 268435456
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
"#)?;
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
    let consumers = "SELECT id,state,bootstrap_done,bootstrap_kind,bootstrap_rowid FROM delivery_jobs WHERE state <> 'cancelled' UNION ALL SELECT id,'active',bootstrap_done,bootstrap_kind,bootstrap_rowid FROM history_exports WHERE expires_at_ms > CAST(unixepoch('subsec')*1000 AS INTEGER)";
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
    Ok(count as usize == names.len())
}
