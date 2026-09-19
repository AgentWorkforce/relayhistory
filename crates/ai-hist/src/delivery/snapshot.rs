//! Durable bounded file/pipe snapshots, with no destination or acceptance state.
use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExportHandle {
    pub snapshot_id: String,
    pub cursor: String,
    pub expires_at_ms: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HistoryExportPage {
    pub schema_version: u32,
    pub origin_id: String,
    pub records: Vec<HistoryExportRecord>,
    pub next_cursor: Option<String>,
}

/// Explicit consistent export snapshot. TTL is bounded to one day; abandoned
/// snapshots stop retaining new preimages after expiry and can be cleaned up.
/// Pass a real Unix millisecond clock, matching SQLite's clock used by capture.
pub fn create_export(
    conn: &Connection,
    selection: &ExportSelection,
    limits: &DeliveryLimits,
    ttl_ms: i64,
    now_ms: i64,
) -> Result<ExportHandle> {
    validate(&DeliveryJobConfig {
        destination_id: "export".into(),
        instance_id: "export".into(),
        account_id: "local".into(),
        mapping_version: "1".into(),
        selection: selection.clone(),
        limits: limits.clone(),
    })?;
    ensure!(
        (1..=86_400_000).contains(&ttl_ms) && now_ms >= 0,
        "invalid export TTL/clock"
    );
    let expires_at_ms = now_ms
        .checked_add(ttl_ms)
        .context("export clock overflow")?;
    let tx = write_transaction(conn)?;
    let count: i64 = tx.query_row("SELECT COUNT(*) FROM history_exports", [], |row| row.get(0))?;
    ensure!(
        count < 32,
        "maximum retained exports reached; close or expire old snapshots"
    );
    let (snapshot_id, cursor): (String, String) = tx.query_row(
        "SELECT lower(hex(randomblob(16))),lower(hex(randomblob(16)))",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    tx.execute("INSERT INTO delivery_journal(kind,source,record_key,operation,payload) VALUES ('__cutoff','','','checkpoint','null')",[])?;
    let cutoff = tx.last_insert_rowid();
    tx.execute("INSERT INTO history_exports(id,selection_json,limits_json,cutoff,expires_at_ms,cursor) VALUES (?,?,?,?,?,?)",params![snapshot_id,serde_json::to_string(selection)?,serde_json::to_string(limits)?,cutoff,expires_at_ms,cursor])?;
    for table in schema::TABLES {
        tx.execute(&format!("INSERT INTO delivery_bootstrap_bounds(job_id,kind,max_rowid) SELECT ?,?,COALESCE(MAX(rowid),0) FROM {}",table.name),params![snapshot_id,table.kind])?;
    }
    tx.commit()?;
    Ok(ExportHandle {
        snapshot_id,
        cursor,
        expires_at_ms,
    })
}

/// An opaque continuation over immutable historical records. Retrying a cursor
/// repeats that page; this means file/stdout clients must advance only after
/// writing the page successfully. No remote-delivery success is implied.
pub fn export_page(conn: &Connection, cursor: &str, now_ms: i64) -> Result<HistoryExportPage> {
    let tx = write_transaction(conn)?;
    let row=tx.query_row("SELECT id,selection_json,limits_json,cutoff,bootstrap_kind,bootstrap_rowid,expires_at_ms FROM history_exports WHERE cursor=? OR id=(SELECT export_id FROM history_export_pages WHERE cursor=?)",params![cursor,cursor],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,i64>(3)?,r.get::<_,i64>(4)?,r.get::<_,i64>(5)?,r.get::<_,i64>(6)?))).optional()?.context("export cursor not found")?;
    let (id, selection_json, limits_json, cutoff, kind, rowid, expires) = row;
    ensure!(expires > now_ms, "export snapshot expired");
    let selection: ExportSelection = serde_json::from_str(&selection_json)?;
    if let Some(payload) = tx
        .query_row(
            "SELECT payload FROM history_export_pages WHERE cursor=?",
            [cursor],
            |r| r.get::<_, String>(0),
        )
        .optional()?
    {
        let mut page: HistoryExportPage = serde_json::from_str(&payload)?;
        let mut records = Vec::new();
        for record in page.records {
            if !record_excluded(&tx, &selection, &record)? {
                records.push(record);
            }
        }
        page.records = records;
        return Ok(page);
    }
    let limits: DeliveryLimits = serde_json::from_str(&limits_json)?;
    let origin_id = tx.query_row(
        "SELECT origin_id FROM delivery_state WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    let next: String = tx.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?;
    let mut page = HistoryExportPage {
        schema_version: EXPORT_SCHEMA_VERSION,
        origin_id,
        records: Vec::new(),
        next_cursor: Some(next.clone()),
    };
    let mut kind = kind as usize;
    let mut rowid = rowid;
    let mut scanned = 0;
    while kind < schema::TABLES.len()
        && scanned < limits.max_scan_records
        && page.records.len() < limits.max_batch_records
    {
        let Some(raw) = snapshot_record(&tx, &id, kind, rowid)? else {
            kind += 1;
            rowid = 0;
            continue;
        };
        if raw.operation != "absent"
            && selected(&selection, &raw.kind, &raw.source, raw.session.as_deref())
            && !raw_excluded(&tx, &selection, &raw)?
        {
            page.records
                .push(make_record(&page.origin_id, cutoff, &raw)?);
            if serde_json::to_vec(&page)?.len() > limits.max_batch_bytes {
                page.records.pop();
                ensure!(
                    !page.records.is_empty(),
                    "export record exceeds configured page byte limit"
                );
                break;
            }
        }
        scanned += 1;
        rowid = raw.position;
        tx.execute(
            "DELETE FROM delivery_shadow WHERE job_id=? AND kind=? AND row_id<=?",
            params![id, raw.kind, rowid],
        )?;
    }
    let complete = kind >= schema::TABLES.len();
    if complete {
        page.next_cursor = None;
    }
    tx.execute("UPDATE history_exports SET bootstrap_kind=?,bootstrap_rowid=?,bootstrap_done=?,cursor=? WHERE id=?",params![kind as i64,rowid,complete,next,id])?;
    tx.execute(
        "INSERT INTO history_export_pages(cursor,export_id,payload) VALUES (?,?,?)",
        params![cursor, id, serde_json::to_string(&page)?],
    )?;
    tx.commit()?;
    Ok(page)
}
fn close(conn: &Connection, id: &str) -> Result<()> {
    conn.execute("DELETE FROM delivery_shadow WHERE job_id=?", [id])?;
    conn.execute("DELETE FROM delivery_bootstrap_bounds WHERE job_id=?", [id])?;
    conn.execute("DELETE FROM history_export_pages WHERE export_id=?", [id])?;
    conn.execute("DELETE FROM history_exports WHERE id=?", [id])?;
    Ok(())
}
pub fn close_export(conn: &Connection, snapshot_id: &str) -> Result<()> {
    let tx = write_transaction(conn)?;
    close(&tx, snapshot_id)?;
    tx.commit()?;
    Ok(())
}
pub fn expire_exports(conn: &Connection, now_ms: i64, limit: usize) -> Result<usize> {
    ensure!((1..=32).contains(&limit), "invalid export cleanup limit");
    let tx = write_transaction(conn)?;
    let ids = tx
        .prepare(
            "SELECT id FROM history_exports WHERE expires_at_ms<=? ORDER BY expires_at_ms LIMIT ?",
        )?
        .query_map(params![now_ms, limit as i64], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for id in &ids {
        close(&tx, id)?;
    }
    tx.commit()?;
    Ok(ids.len())
}
