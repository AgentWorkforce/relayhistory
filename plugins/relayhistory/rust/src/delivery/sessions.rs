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
    let tx = write_transaction(conn)?;
    let changed = set_job_session_in_transaction(&tx, job_id, session, include)?;
    tx.commit()?;
    Ok(changed)
}

/// Include a session with a fresh baseline when a legacy global exclusion must
/// be lifted. Consent, subscription, relationships and fencing commit together.
/// Other affected jobs still require a new generation before broadening access.
pub fn include_job_session(
    conn: &Connection,
    job_id: &str,
    session: &SessionIdentity,
) -> Result<bool> {
    let tx = write_transaction(conn)?;
    let excluded: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM delivery_exclusions WHERE source=? AND session_id=?)",
        params![session.source, session.session_id],
        |r| r.get(0),
    )?;
    if excluded {
        // Validate the scoped job and retire any old membership before lifting
        // its exclusion. A failed guard or snapshot rolls all of this back.
        set_job_session_in_transaction(&tx, job_id, session, false)?;
        set_session_excluded_in_transaction(&tx, session, false, Some(job_id))?;
    }
    let changed = set_job_session_in_transaction(&tx, job_id, session, true)?;
    tx.commit()?;
    Ok(changed)
}

fn set_job_session_in_transaction(
    conn: &Connection,
    job_id: &str,
    session: &SessionIdentity,
    include: bool,
) -> Result<bool> {
    ensure!(
        !session.source.is_empty() && !session.session_id.is_empty(),
        "invalid session identity"
    );
    let root = job(conn, job_id)?;
    ensure!(
        root.state != "cancelled" && is_session_job(conn, job_id)?,
        "active session job required"
    );
    let existing: Option<String> = conn
        .query_row(
            "SELECT id FROM delivery_session_members WHERE job_id=? AND source=? AND session_id=?",
            params![job_id, session.source, session.session_id],
            |r| r.get(0),
        )
        .optional()?;
    if existing.is_some() == include {
        return Ok(false);
    }
    if let Some(id) = existing {
        capture::release_subscription(conn, &id)?;
        conn.execute("DELETE FROM delivery_session_members WHERE id=?", [&id])?;
    } else {
        let id: String = conn.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?;
        let cutoff = capture::reserve_revision(conn)?;
        conn.execute("INSERT INTO delivery_session_members(id,job_id,source,session_id,cutoff,cursor) VALUES (?,?,?,?,?,?)", params![id,job_id,session.source,session.session_id,cutoff,cutoff])?;
        capture::snapshot_bounds(conn, &id)?;
        capture::save_subscription(
            conn,
            &capture::Subscription {
                id: &id,
                session: Some(session),
                cursor: cutoff,
                kind: 0,
                rowid: 0,
                complete: false,
            },
        )?;
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
        for record in capture::incoming_relationships(conn, session)? {
            if let Some(parent) = &record.session {
                if job_session_included(
                    conn,
                    job_id,
                    &SessionIdentity {
                        source: record.source.clone(),
                        session_id: parent.clone(),
                    },
                )? {
                    capture::append_revision(conn, &record)?;
                }
            }
        }
    }
    // Invalidate dispatch authorization, but keep immutable pending work and
    // retry/failure state. Claim filters removed/re-included snapshot records.
    conn.execute(
        "UPDATE delivery_jobs SET fence=fence+1,worker_id=NULL,lease_until_ms=NULL WHERE id=?",
        [job_id],
    )?;
    conn.execute(
        "UPDATE delivery_batches SET state='pending' WHERE job_id=? AND state='leased'",
        [job_id],
    )?;
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
    let ids = conn
        .prepare("SELECT id FROM delivery_session_members WHERE job_id=?")?
        .query_map([job_id], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for id in ids {
        capture::release_subscription(conn, &id)?;
    }
    conn.execute(
        "DELETE FROM delivery_session_members WHERE job_id=?",
        [job_id],
    )?;
    Ok(())
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
    let high = capture::latest_revision(conn)?;
    let mut last_member: String = conn.query_row(
        "SELECT last_member FROM delivery_session_jobs WHERE job_id=?",
        [&root.id],
        |r| r.get(0),
    )?;
    let mut visited = 0;

    while visited < root.config.limits.max_scan_records
        && batch.records.len() < root.config.limits.max_batch_records
    {
        let read_member = |after: &str| -> Result<_> {
            Ok(conn.query_row("SELECT id,source,session_id,cutoff,cursor,bootstrap_kind,bootstrap_rowid,bootstrap_done FROM delivery_session_members WHERE job_id=? AND ready=1 AND id>? ORDER BY id LIMIT 1", params![root.id,after], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,i64>(3)?,r.get::<_,i64>(4)?,r.get::<_,usize>(5)?,r.get::<_,i64>(6)?,r.get::<_,bool>(7)?))).optional()?)
        };
        let member = match read_member(&last_member)? {
            Some(member) => Some(member),
            None => read_member("")?,
        };
        let Some((id, source, session_id, cutoff, mut cursor, mut kind, mut rowid, mut complete)) =
            member
        else {
            break;
        };
        visited += 1;
        let identity = SessionIdentity { source, session_id };
        let mut raw = None;
        while !complete {
            if kind >= capture::kind_count() {
                complete = true;
                break;
            }
            if root
                .config
                .selection
                .kinds
                .iter()
                .any(|k| k == capture::kind(kind).unwrap())
            {
                raw = capture::session_snapshot_record(conn, &id, &identity, kind, rowid)?;
                if raw.is_some() {
                    break;
                }
            }
            kind += 1;
            rowid = 0;
        }
        if complete {
            raw = capture::next_change(conn, cursor, Some(&identity))?;
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
                capture::discard_read_preimages(conn, &id, &raw.kind, rowid)?;
            }
        } else {
            cursor = high;
        }
        conn.execute("UPDATE delivery_session_members SET cursor=?,bootstrap_kind=?,bootstrap_rowid=?,bootstrap_done=?,ready=? WHERE id=?", params![cursor,kind,rowid,complete,ready,id])?;
        capture::save_subscription(
            conn,
            &capture::Subscription {
                id: &id,
                session: Some(&identity),
                cursor,
                kind,
                rowid,
                complete,
            },
        )?;
        if complete {
            capture::clear_preimages(conn, &id)?;
        }
        last_member = id;
    }
    conn.execute(
        "UPDATE delivery_session_jobs SET last_member=? WHERE job_id=?",
        params![last_member, root.id],
    )?;
    let floor: i64 = conn.query_row(
        "SELECT COALESCE(MIN(cursor),?2) FROM delivery_session_members WHERE job_id=?1 AND ready=1",
        params![root.id, high],
        |r| r.get(0),
    )?;
    conn.execute("UPDATE delivery_jobs SET journal_cursor=?,suppressed_records=suppressed_records+? WHERE id=?",params![floor,suppressed,root.id])?;
    update_root_subscription(conn, &root.id)?;
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
    loop {
        match adopt_session_job_once(conn, job_id, members) {
            // Adoption temporarily duplicates selected snapshot preimages.
            // A legacy collector may have filled retention before its worker
            // could compact consumed revisions. Retry only after reclaiming
            // data that every consumer has already copied; never raise the
            // cap, discard pending batches, or advance an unread cursor.
            // Scan a complete bounded pass: a pinned page can remove nothing
            // even when later journal pages contain reclaimable records.
            Err(error) if is_retention_limit(&error) => {
                if ai_hist::export::compact_journal_pass(conn, 1_000)? == 0 {
                    return Err(error);
                }
            }
            result => return result,
        }
    }
}

fn adopt_session_job_once(
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
        capture::clone_session_snapshot(&tx, job_id, &id, identity)?;
        capture::save_subscription(
            &tx,
            &capture::Subscription {
                id: &id,
                session: Some(identity),
                cursor: root.cursor,
                kind: root.bootstrap_kind,
                rowid: root.bootstrap_rowid,
                complete: root.bootstrap_done,
            },
        )?;
    }
    capture::clear_preimages(&tx, job_id)?;
    tx.execute("UPDATE delivery_jobs SET bootstrap_done=1,fence=fence+1,worker_id=NULL,lease_until_ms=NULL WHERE id=?",[job_id])?;
    tx.execute(
        "UPDATE delivery_batches SET state='pending' WHERE job_id=? AND state='leased'",
        [job_id],
    )?;
    update_root_subscription(&tx, job_id)?;
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
