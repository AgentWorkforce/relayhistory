//! Transaction-scoped evidence reads; callers own upload policy.
pub use super::schema::shareable;
use super::*;
pub use super::{make_record, snapshot_record, RawRecord};

/// [`shareable`] evaluated for one identity: the consent check a reader
/// applies to a record it is about to prepare, claim or dispatch.
pub fn is_shareable(conn: &Connection, source: &str, session_id: &str) -> Result<bool> {
    let sql = format!("SELECT {}", shareable("?1", "?2"));
    Ok(conn.query_row(&sql, params![source, session_id], |r| r.get(0))?)
}
pub fn session_snapshot_record(
    conn: &Connection,
    id: &str,
    identity: &SessionIdentity,
    kind: usize,
    after: i64,
) -> Result<Option<RawRecord>> {
    let table = schema::TABLES
        .get(kind)
        .context("unknown evidence kind index")?;
    let maximum: i64 = conn.query_row(
        "SELECT max_rowid FROM delivery_bootstrap_bounds WHERE job_id=? AND kind=?",
        params![id, table.kind],
        |r| r.get(0),
    )?;
    // Two indexed seeks, each returning at most one rowid. Resolve preimages
    // after choosing the candidate: an OLD session identity still belongs to
    // this snapshot even when the live row moved to a different session.
    let sql = format!(
        r#"
WITH current_id AS (SELECT rowid AS row_id FROM {table} r WHERE {source}=?5 AND r.{session}=?6 AND rowid>?1 AND rowid<=?2 ORDER BY rowid LIMIT 1),
shadow_id AS (SELECT row_id FROM delivery_shadow INDEXED BY delivery_shadow_session WHERE job_id=?3 AND kind=?4 AND source=?5 AND session_id=?6 AND row_id>?1 AND row_id<=?2 ORDER BY row_id LIMIT 1),
candidate AS (SELECT row_id FROM (SELECT row_id FROM current_id UNION ALL SELECT row_id FROM shadow_id) ORDER BY row_id LIMIT 1)
SELECT c.row_id,CASE WHEN s.row_id IS NOT NULL THEN s.source ELSE {source} END,
CASE WHEN s.row_id IS NOT NULL THEN s.session_id ELSE r.{session} END,
CASE WHEN s.row_id IS NOT NULL THEN s.record_key ELSE {key} END,
CASE WHEN s.row_id IS NOT NULL THEN s.payload ELSE {payload} END
FROM candidate c LEFT JOIN {table} r ON r.rowid=c.row_id
LEFT JOIN delivery_shadow s ON s.job_id=?3 AND s.kind=?4 AND s.row_id=c.row_id
"#,
        table = table.name,
        source = table.source("r"),
        session = table.session,
        key = table.key("r"),
        payload = table.payload(conn, "r")?
    );
    Ok(conn
        .query_row(
            &sql,
            params![
                after,
                maximum,
                id,
                table.kind,
                identity.source,
                identity.session_id
            ],
            |r| {
                let payload: Option<String> = r.get(4)?;
                Ok(RawRecord {
                    position: r.get(0)?,
                    kind: table.kind.into(),
                    source: r.get(1)?,
                    session: r.get(2)?,
                    key: r.get(3)?,
                    operation: if payload.is_some() {
                        "upsert"
                    } else {
                        "absent"
                    }
                    .into(),
                    payload: payload.unwrap_or_else(|| "null".into()),
                })
            },
        )
        .optional()?)
}

/// Initialize storage capture inside the caller's schema transaction.
pub fn initialize(conn: &Connection) -> Result<()> {
    schema::init_schema(conn)
}

/// An external reader's durable progress. No destination or upload state.
pub struct Subscription<'a> {
    pub id: &'a str,
    pub session: Option<&'a SessionIdentity>,
    pub cursor: i64,
    pub kind: usize,
    pub rowid: i64,
    pub complete: bool,
}
/// Caller holds the transaction containing its own state change.
pub fn save_subscription(conn: &Connection, value: &Subscription<'_>) -> Result<()> {
    ensure!(
        !conn.is_autocommit(),
        "subscription update requires a transaction"
    );
    conn.execute("INSERT INTO history_subscriptions(id,source,session_id,journal_cursor,bootstrap_kind,bootstrap_rowid,bootstrap_done) VALUES (?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET journal_cursor=excluded.journal_cursor,bootstrap_kind=excluded.bootstrap_kind,bootstrap_rowid=excluded.bootstrap_rowid,bootstrap_done=excluded.bootstrap_done", params![value.id,value.session.map(|s|s.source.as_str()),value.session.map(|s|s.session_id.as_str()),value.cursor,value.kind,value.rowid,value.complete])?;
    Ok(())
}
pub fn reserve_revision(conn: &Connection) -> Result<i64> {
    ensure!(
        !conn.is_autocommit(),
        "revision reservation requires a transaction"
    );
    conn.execute("INSERT INTO delivery_journal(kind,source,record_key,operation,payload) VALUES ('__cutoff','','','checkpoint','null')", [])?;
    Ok(conn.last_insert_rowid())
}
pub fn snapshot_bounds(conn: &Connection, id: &str) -> Result<()> {
    for table in schema::TABLES {
        conn.execute(&format!("INSERT INTO delivery_bootstrap_bounds(job_id,kind,max_rowid) SELECT ?,?,COALESCE(MAX(rowid),0) FROM {}",table.name),params![id,table.kind])?;
    }
    Ok(())
}
pub fn release_subscription(conn: &Connection, id: &str) -> Result<()> {
    ensure!(
        !conn.is_autocommit(),
        "subscription removal requires a transaction"
    );
    conn.execute("DELETE FROM delivery_shadow WHERE job_id=?", [id])?;
    conn.execute("DELETE FROM delivery_bootstrap_bounds WHERE job_id=?", [id])?;
    conn.execute("DELETE FROM history_subscriptions WHERE id=?", [id])?;
    Ok(())
}
pub fn kind(index: usize) -> Option<&'static str> {
    schema::TABLES.get(index).map(|t| t.kind)
}
pub fn kind_count() -> usize {
    schema::TABLES.len()
}
pub fn origin_id(conn: &Connection) -> Result<String> {
    Ok(conn.query_row(
        "SELECT origin_id FROM delivery_state WHERE singleton=1",
        [],
        |r| r.get(0),
    )?)
}
pub fn latest_revision(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COALESCE(MAX(seq),0) FROM delivery_journal",
        [],
        |r| r.get(0),
    )?)
}
pub fn next_change(
    conn: &Connection,
    after: i64,
    session: Option<&SessionIdentity>,
) -> Result<Option<RawRecord>> {
    let read = |r: &rusqlite::Row<'_>| {
        Ok(RawRecord {
            position: r.get(0)?,
            kind: r.get(1)?,
            source: r.get(2)?,
            session: r.get(3)?,
            key: r.get(4)?,
            operation: r.get(5)?,
            payload: r.get(6)?,
        })
    };
    Ok(if let Some(s) = session {
        conn.query_row("SELECT seq,kind,source,session_id,record_key,operation,payload FROM delivery_journal INDEXED BY delivery_journal_session WHERE source=? AND session_id=? AND seq>? ORDER BY seq LIMIT 1",params![s.source,s.session_id,after],read).optional()?
    } else {
        conn.query_row("SELECT seq,kind,source,session_id,record_key,operation,payload FROM delivery_journal WHERE seq>? ORDER BY seq LIMIT 1",[after],read).optional()?
    })
}
pub fn discard_read_preimages(conn: &Connection, id: &str, kind: &str, through: i64) -> Result<()> {
    conn.execute(
        "DELETE FROM delivery_shadow WHERE job_id=? AND kind=? AND row_id<=?",
        params![id, kind, through],
    )?;
    Ok(())
}
pub fn clear_preimages(conn: &Connection, id: &str) -> Result<()> {
    conn.execute("DELETE FROM delivery_shadow WHERE job_id=?", [id])?;
    Ok(())
}
/// Inherit one identity from an existing immutable snapshot without replaying it.
pub fn clone_session_snapshot(
    conn: &Connection,
    from: &str,
    to: &str,
    identity: &SessionIdentity,
) -> Result<()> {
    conn.execute("INSERT INTO delivery_bootstrap_bounds(job_id,kind,max_rowid) SELECT ?,kind,max_rowid FROM delivery_bootstrap_bounds WHERE job_id=?",params![to,from])?;
    for table in schema::TABLES {
        conn.execute(&format!("INSERT INTO delivery_shadow(job_id,kind,row_id,source,session_id,record_key,payload) SELECT ?1,s.kind,s.row_id,s.source,s.session_id,s.record_key,s.payload FROM delivery_shadow s LEFT JOIN {} r ON r.rowid=s.row_id WHERE s.job_id=?2 AND s.kind=?5 AND ((s.source=?3 AND s.session_id=?4) OR ({}=?3 AND r.{}=?4))",table.name,table.source("r"),table.session),params![to,from,identity.source,identity.session_id,table.kind])?;
    }
    Ok(())
}
pub fn incoming_relationships(
    conn: &Connection,
    child: &SessionIdentity,
) -> Result<Vec<RawRecord>> {
    let table = schema::TABLES
        .iter()
        .find(|t| t.kind == "relationship")
        .unwrap();
    Ok(conn.prepare(&format!("SELECT r.rowid,r.source,r.parent_session_id,{},{} FROM session_relationships r WHERE r.source=? AND r.child_session_id=?",table.key("r"),table.payload(conn,"r")?))?.query_map(params![child.source,child.session_id],|r|Ok(RawRecord{position:r.get(0)?,kind:"relationship".into(),source:r.get(1)?,session:r.get(2)?,key:r.get(3)?,operation:"upsert".into(),payload:r.get(4)?}))?.collect::<rusqlite::Result<Vec<_>>>()?)
}
pub fn append_revision(conn: &Connection, record: &RawRecord) -> Result<i64> {
    ensure!(
        !conn.is_autocommit(),
        "revision append requires a transaction"
    );
    ensure!(
        SUPPORTED_KINDS.contains(&record.kind.as_str()),
        "unsupported evidence kind"
    );
    conn.execute("INSERT INTO delivery_journal(kind,source,session_id,record_key,operation,payload) VALUES (?,?,?,?,?,?)",params![record.kind,record.source,record.session,record.key,record.operation,record.payload])?;
    Ok(conn.last_insert_rowid())
}
