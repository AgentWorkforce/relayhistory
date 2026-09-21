//! Normalized, incremental session membership. Each inclusion gets an immutable
//! snapshot revision; the destination job still owns batches, fences and leases.
use super::*;

/// Create an initially empty, deny-by-default session job. The configuration
/// describes allowed kinds; membership is changed only through `set_job_session`.
/// Its configuration and destination generation remain immutable.
pub fn create_session_job(
    conn: &Connection,
    config: &DeliveryJobConfig,
    now_ms: i64,
) -> Result<DeliveryStatus> {
    ensure!(
        config.selection.all_sources
            && config.selection.sources.is_empty()
            && config.selection.sessions.is_empty(),
        "session jobs require an all-sources kind policy and normalized membership"
    );
    create_job_inner(conn, config, now_ms, true)
}
pub fn is_session_job(conn: &Connection, job_id: &str) -> Result<bool> {
    // Read-only bridge commands can be the first entry after a binary upgrade.
    // They must still understand the old job until a writable open migrates it.
    if !conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='delivery_session_jobs')",
        [],
        |r| r.get::<_, bool>(0),
    )? {
        return Ok(false);
    }

    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM delivery_session_jobs WHERE job_id=?)",
        [job_id],
        |r| r.get(0),
    )?)
}
/// Read the explicit members; unrelated catalog identities are never visited.
pub fn job_sessions(conn: &Connection, job_id: &str) -> Result<Vec<SessionIdentity>> {
    Ok(conn.prepare("SELECT source,session_id FROM delivery_session_members WHERE job_id=? ORDER BY source,session_id")?.query_map([job_id], |r| Ok(SessionIdentity {source:r.get(0)?, session_id:r.get(1)?}))?.collect::<rusqlite::Result<_>>()?)
}
/// Atomically add/remove one member. Repeated requests perform no writes.
/// Re-inclusion starts a fresh per-session snapshot, never reuses a skipped
/// cursor. Global exclusions remain authoritative; this API does not remove
/// them. Membership is account/instance scoped through its owning job.
pub fn set_job_session(
    conn: &Connection,
    job_id: &str,
    session: &SessionIdentity,
    include: bool,
) -> Result<bool> {
    ensure!(
        !session.source.is_empty() && !session.session_id.is_empty(),
        "invalid session identity"
    );
    let tx = write_transaction(conn)?;
    let root = job(&tx, job_id)?;
    ensure!(
        root.state != "cancelled" && is_session_job(&tx, job_id)?,
        "active session job required"
    );
    let existing: Option<String> = tx
        .query_row(
            "SELECT id FROM delivery_session_members WHERE job_id=? AND source=? AND session_id=?",
            params![job_id, session.source, session.session_id],
            |r| r.get(0),
        )
        .optional()?;
    if existing.is_some() == include {
        tx.commit()?;
        return Ok(false);
    }
    if let Some(id) = existing {
        tx.execute("DELETE FROM delivery_shadow WHERE job_id=?", [&id])?;
        tx.execute(
            "DELETE FROM delivery_bootstrap_bounds WHERE job_id=?",
            [&id],
        )?;
        tx.execute("DELETE FROM delivery_session_members WHERE id=?", [&id])?;
    } else {
        let id: String = tx.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?;
        tx.execute("INSERT INTO delivery_journal(kind,source,record_key,operation,payload) VALUES ('__cutoff','','','checkpoint','null')", [])?;
        let cutoff = tx.last_insert_rowid();
        tx.execute("INSERT INTO delivery_session_members(id,job_id,source,session_id,cutoff,cursor) VALUES (?,?,?,?,?,?)", params![id,job_id,session.source,session.session_id,cutoff,cutoff])?;
        for table in schema::TABLES {
            tx.execute(&format!("INSERT INTO delivery_bootstrap_bounds(job_id,kind,max_rowid) SELECT ?,?,COALESCE(MAX(rowid),0) FROM {}", table.name), params![id,table.kind])?;
        }
    }
    if include
        && root
            .config
            .selection
            .kinds
            .iter()
            .any(|kind| kind == "relationship")
    {
        // A parent may have skipped this private child earlier. Capture only
        // those now-eligible edges as fresh revisions; do not restart the
        // parent's completed event/history backfill.
        let table = schema::TABLES
            .iter()
            .find(|t| t.kind == "relationship")
            .unwrap();
        tx.execute(&format!("INSERT INTO delivery_journal(kind,source,session_id,record_key,operation,payload) SELECT 'relationship',r.source,r.parent_session_id,{},'upsert',{} FROM session_relationships r WHERE r.source=?1 AND r.child_session_id=?2 AND EXISTS(SELECT 1 FROM delivery_session_members m WHERE m.job_id=?3 AND m.source=r.source AND m.session_id=r.parent_session_id)",table.key("r"),table.payload(&tx,"r")?),params![session.source,session.session_id,job_id])?;
    }
    // Invalidate dispatch authorization, but keep immutable pending work and
    // retry/failure state. Claim filters removed/re-included snapshot records.
    tx.execute(
        "UPDATE delivery_jobs SET fence=fence+1,worker_id=NULL,lease_until_ms=NULL WHERE id=?",
        [job_id],
    )?;
    tx.execute(
        "UPDATE delivery_batches SET state='pending' WHERE job_id=? AND state='leased'",
        [job_id],
    )?;
    tx.commit()?;
    Ok(true)
}

pub(super) fn record_allowed(
    conn: &Connection,
    job_id: &str,
    record: &HistoryExportRecord,
) -> Result<bool> {
    if !is_session_job(conn, job_id)? {
        return Ok(true);
    }
    let allowed = |session: Option<&str>| -> Result<bool> {
        Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM delivery_session_members WHERE job_id=? AND source=? AND session_id=? AND cutoff<=?)", params![job_id,record.source,session,record.revision], |r| r.get(0))?)
    };
    if !allowed(record.session_id.as_deref())? {
        return Ok(false);
    }
    // A relationship must not disclose a private child through a public parent.
    if record.kind == "relationship" {
        if let Some(child) = record
            .payload
            .get("child_session_id")
            .and_then(|v| v.as_str())
        {
            // Both endpoints must still belong to the inclusion that admitted
            // this revision. Child re-inclusion journals a fresh eligible edge.
            return allowed(Some(child));
        }
    }
    Ok(true)
}

pub(super) fn cancel(conn: &Connection, job_id: &str) -> Result<()> {
    conn.execute("DELETE FROM delivery_shadow WHERE job_id IN (SELECT id FROM delivery_session_members WHERE job_id=?)", [job_id])?;
    conn.execute("DELETE FROM delivery_bootstrap_bounds WHERE job_id IN (SELECT id FROM delivery_session_members WHERE job_id=?)", [job_id])?;
    conn.execute(
        "DELETE FROM delivery_session_members WHERE job_id=?",
        [job_id],
    )?;
    Ok(())
}

fn snapshot(
    conn: &Connection,
    id: &str,
    identity: &SessionIdentity,
    kind: usize,
    after: i64,
) -> Result<Option<RawRecord>> {
    let table = &schema::TABLES[kind];
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

pub(super) fn prepare(conn: &Connection, root: &Job, now_ms: i64) -> Result<PrepareResult> {
    let existing: Option<String> = conn.query_row("SELECT id FROM delivery_batches WHERE job_id=? AND state IN ('pending','leased','retry_wait','blocked')", [&root.id], |r| r.get(0)).optional()?;
    let done = || -> Result<bool> {
        Ok(!conn.query_row("SELECT EXISTS(SELECT 1 FROM delivery_session_members WHERE job_id=? AND bootstrap_done=0)", [&root.id], |r| r.get::<_,bool>(0))?)
    };
    if existing.is_some() {
        return Ok(PrepareResult {
            batch_id: existing,
            scanned_records: 0,
            bootstrap_complete: done()?,
        });
    }
    let mut batch = batch_template(conn, root)?;
    let mut scanned = 0;
    let mut suppressed = 0;
    let high: i64 = conn.query_row(
        "SELECT COALESCE(MAX(seq),0) FROM delivery_journal",
        [],
        |r| r.get(0),
    )?;
    while scanned < root.config.limits.max_scan_records
        && batch.records.len() < root.config.limits.max_batch_records
    {
        let member = conn.query_row("SELECT id,source,session_id,cutoff,cursor,bootstrap_kind,bootstrap_rowid,bootstrap_done FROM delivery_session_members WHERE job_id=? AND ready=1 ORDER BY id LIMIT 1", [&root.id], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,i64>(3)?,r.get::<_,i64>(4)?,r.get::<_,usize>(5)?,r.get::<_,i64>(6)?,r.get::<_,bool>(7)?))).optional()?;
        let Some((id, source, session_id, cutoff, mut cursor, mut kind, mut rowid, mut complete)) =
            member
        else {
            break;
        };
        let identity = SessionIdentity { source, session_id };
        let mut raw = None;
        while !complete {
            if kind >= schema::TABLES.len() {
                complete = true;
                break;
            }
            if root
                .config
                .selection
                .kinds
                .iter()
                .any(|k| k == schema::TABLES[kind].kind)
            {
                raw = snapshot(conn, &id, &identity, kind, rowid)?;
                if raw.is_some() {
                    break;
                }
            }
            kind += 1;
            rowid = 0;
        }
        if complete {
            raw = conn.query_row("SELECT seq,kind,source,session_id,record_key,operation,payload FROM delivery_journal INDEXED BY delivery_journal_session WHERE source=? AND session_id=? AND seq>? ORDER BY seq LIMIT 1", params![identity.source,identity.session_id,cursor], |r| Ok(RawRecord{position:r.get(0)?,kind:r.get(1)?,source:r.get(2)?,session:r.get(3)?,key:r.get(4)?,operation:r.get(5)?,payload:r.get(6)?})).optional()?;
        }
        let ready = raw.is_some();
        if let Some(raw) = raw {
            let revision = if complete { raw.position } else { cutoff };
            let eligible = raw.operation != "absent"
                && raw.source == identity.source
                && raw.session.as_deref() == Some(&identity.session_id)
                && selected(
                    &root.config.selection,
                    &raw.kind,
                    &raw.source,
                    raw.session.as_deref(),
                );
            if eligible {
                let record = make_record(&batch.origin_id, revision, &raw)?;
                if !raw_excluded(conn, &root.config.selection, &raw)?
                    && record_allowed(conn, &root.id, &record)?
                {
                    batch.records.push(record);
                    if serde_json::to_vec(&batch)?.len() > root.config.limits.max_batch_bytes {
                        batch.records.pop();
                        ensure!(
                            !batch.records.is_empty(),
                            "delivery record exceeds configured batch byte limit"
                        );
                        break;
                    }
                } else {
                    suppressed += 1;
                }
            }
            scanned += 1;
            if complete {
                cursor = raw.position;
            } else {
                rowid = raw.position;
                conn.execute(
                    "DELETE FROM delivery_shadow WHERE job_id=? AND kind=? AND row_id<=?",
                    params![id, raw.kind, rowid],
                )?;
            }
        } else {
            cursor = high;
        }
        conn.execute("UPDATE delivery_session_members SET cursor=?,bootstrap_kind=?,bootstrap_rowid=?,bootstrap_done=?,ready=? WHERE id=?", params![cursor,kind,rowid,complete,ready,id])?;
        if complete {
            conn.execute("DELETE FROM delivery_shadow WHERE job_id=?", [&id])?;
        }
    }
    let floor: i64 = conn.query_row(
        "SELECT COALESCE(MIN(cursor),?2) FROM delivery_session_members WHERE job_id=?1 AND ready=1",
        params![root.id, high],
        |r| r.get(0),
    )?;
    conn.execute("UPDATE delivery_jobs SET journal_cursor=?,suppressed_records=suppressed_records+? WHERE id=?",params![floor,suppressed,root.id])?;
    let batch_id = if batch.records.is_empty() {
        None
    } else {
        insert_batch(conn, &batch, floor, now_ms)?;
        Some(batch.batch_id)
    };
    Ok(PrepareResult {
        batch_id,
        scanned_records: scanned,
        bootstrap_complete: done()?,
    })
}

/// Upgrade a legacy explicit session policy without discarding queued records,
/// immutable preimages, retry state, or cursor positions. Caller supplies the
/// already-authorized members, never the whole catalog. Repeating is a no-op.
pub fn adopt_session_job(
    conn: &Connection,
    job_id: &str,
    members: &[SessionIdentity],
) -> Result<()> {
    let tx = write_transaction(conn)?;
    if is_session_job(&tx, job_id)? {
        tx.commit()?;
        return Ok(());
    }
    let root = job(&tx, job_id)?;
    ensure!(root.state != "cancelled", "cannot adopt cancelled job");
    ensure!(
        root.config.selection.all_sources,
        "legacy source policy cannot be adopted"
    );
    tx.execute(
        "INSERT INTO delivery_session_jobs(job_id) VALUES (?)",
        [job_id],
    )?;
    for identity in members {
        let id: String = tx.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?;
        if tx.execute("INSERT OR IGNORE INTO delivery_session_members(id,job_id,source,session_id,cutoff,cursor,bootstrap_kind,bootstrap_rowid,bootstrap_done) VALUES (?,?,?,?,?,?,?,?,?)",params![id,job_id,identity.source,identity.session_id,root.cutoff,root.cursor,root.bootstrap_kind,root.bootstrap_rowid,root.bootstrap_done])? == 0 { continue; }
        tx.execute("INSERT INTO delivery_bootstrap_bounds(job_id,kind,max_rowid) SELECT ?,kind,max_rowid FROM delivery_bootstrap_bounds WHERE job_id=?",params![id,job_id])?;
        for table in schema::TABLES {
            tx.execute(&format!("INSERT INTO delivery_shadow(job_id,kind,row_id,source,session_id,record_key,payload) SELECT ?1,s.kind,s.row_id,s.source,s.session_id,s.record_key,s.payload FROM delivery_shadow s LEFT JOIN {} r ON r.rowid=s.row_id WHERE s.job_id=?2 AND s.kind=?5 AND ((s.source=?3 AND s.session_id=?4) OR ({}=?3 AND r.{}=?4))",table.name,table.source("r"),table.session),params![id,job_id,identity.source,identity.session_id,table.kind])?;
        }
    }
    tx.execute("DELETE FROM delivery_shadow WHERE job_id=?", [job_id])?;
    tx.execute("UPDATE delivery_jobs SET bootstrap_done=1,fence=fence+1,worker_id=NULL,lease_until_ms=NULL WHERE id=?",[job_id])?;
    tx.execute(
        "UPDATE delivery_batches SET state='pending' WHERE job_id=? AND state='leased'",
        [job_id],
    )?;
    tx.commit()?;
    Ok(())
}

/// Indexed membership lookup for controls and paged UIs.
pub fn job_session_included(
    conn: &Connection,
    job_id: &str,
    session: &SessionIdentity,
) -> Result<bool> {
    Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM delivery_session_members WHERE job_id=? AND source=? AND session_id=?)",params![job_id,session.source,session.session_id],|r|r.get(0))?)
}
