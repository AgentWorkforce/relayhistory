use super::{targets, RetentionOptions, RetentionReport};
use crate::{Error, SessionStore};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::time::Duration;

const DAY_MS: i64 = 86_400_000;

pub(super) fn run(
    store: &SessionStore,
    opts: &RetentionOptions,
    now: i64,
) -> Result<RetentionReport, Error> {
    let (plan, after) = match prepare(store, opts, now)? {
        Prepared::Skip(report) => return Ok(report),
        Prepared::Ready(plan, after) => (plan, after),
    };
    // The SessionStore already migrated the schema. Do not run init/migrations
    // or wait behind a writer on the maintenance path.
    let mut conn = Connection::open(store.db_path()).map_err(Error::sql)?;
    conn.busy_timeout(Duration::ZERO).map_err(Error::sql)?;
    conn.pragma_update(None, "recursive_triggers", true)
        .map_err(Error::sql)?;
    warm_connection(&conn).map_err(Error::query)?;
    let lock = crate::ingest::try_acquire_sync_lock(store.db_path()).map_err(Error::query)?;
    let Some(_lock) = lock else {
        return Ok(RetentionReport {
            busy: true,
            ..Default::default()
        });
    };
    let tx = match conn.transaction_with_behavior(TransactionBehavior::Immediate) {
        Ok(tx) => tx,
        Err(error) if busy(&error) => {
            return Ok(RetentionReport {
                busy: true,
                ..Default::default()
            })
        }
        Err(error) => return Err(Error::sql(error)),
    };
    // Settings/cursor can change while planning. Start over instead of applying
    // an obsolete (possibly shorter) policy or skipping another worker's batch.
    if super::policy(&tx).map_err(Error::query)?.days != Some(plan.days)
        || targets::cursor(&tx).map_err(Error::query)? != after
    {
        return Ok(RetentionReport::default());
    }
    if cooling_down(&tx, opts, now).map_err(Error::query)? {
        return Ok(RetentionReport {
            deferred: true,
            ..Default::default()
        });
    }
    let head = crate::change_feed::read_head(&tx).map_err(Error::query)?;
    let report = apply_batch(&tx, &plan, opts, now)?;
    // This IMMEDIATE transaction is the sole writer since head was read.
    // Exactly its eviction tombstones are removed; older real deletions stay.
    tx.execute(
        "DELETE FROM evidence_tombstones WHERE revision>?1",
        [head.revision as i64],
    )
    .map_err(Error::sql)?;
    if !report.awaiting_upload_ack {
        tx.execute("INSERT INTO store_settings(key,value) VALUES ('retention.last_batch_ms',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [crate::now_ms().max(now).to_string()]).map_err(Error::sql)?;
    }
    tx.commit().map_err(Error::sql)?;
    Ok(report)
}

enum Prepared {
    Skip(RetentionReport),
    Ready(Plan, targets::Key),
}
fn prepare(store: &SessionStore, opts: &RetentionOptions, now: i64) -> Result<Prepared, Error> {
    if store.read_only() {
        return Err(Error::read_only("prune_expired"));
    }
    if opts.max_rows == 0 || opts.max_rows > 1024 {
        return Err(Error::InvalidArgument(
            "retention max_rows must be 1..=1024".into(),
        ));
    }
    // Do not borrow the foreground pool: maintenance can otherwise take its
    // warm connection and force a simultaneous catalog read through cold open.
    let reader = crate::store::open_db_readonly(store.db_path()).map_err(Error::query)?;
    let Some(days) = super::policy(&reader).map_err(Error::query)?.days else {
        return Ok(Prepared::Skip(RetentionReport {
            cycle_complete: true,
            ..Default::default()
        }));
    };
    if cooling_down(&reader, opts, now).map_err(Error::query)? {
        return Ok(Prepared::Skip(RetentionReport {
            deferred: true,
            ..Default::default()
        }));
    }
    let after = targets::cursor(&reader).map_err(Error::query)?;
    let plan = plan(&reader, &after, days, now).map_err(Error::query)?;
    drop(reader);
    Ok(Prepared::Ready(plan, after))
}

fn cooling_down(conn: &Connection, opts: &RetentionOptions, now: i64) -> anyhow::Result<bool> {
    if opts.min_interval_ms == 0 {
        return Ok(false);
    }
    let last: Option<String> = conn
        .query_row(
            "SELECT value FROM store_settings WHERE key='retention.last_batch_ms'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(last.is_some_and(|last| {
        last.parse::<i64>()
            .is_ok_and(|last| now.saturating_sub(last) < i64::from(opts.min_interval_ms))
    }))
}

struct Plan {
    days: std::num::NonZeroU32,
    candidate: Option<targets::Key>,
    scanned_through: Option<targets::Key>,
}

fn plan(
    conn: &Connection,
    after: &targets::Key,
    days: std::num::NonZeroU32,
    now: i64,
) -> anyhow::Result<Plan> {
    let cutoff = now.saturating_sub(i64::from(days.get()) * DAY_MS);
    let mut plan = Plan {
        days,
        candidate: None,
        scanned_through: None,
    };
    for key in targets::candidates(conn, after)? {
        plan.scanned_through = Some(key.clone());
        if targets::expired(conn, &key, cutoff)? {
            plan.candidate = Some(key);
            break;
        }
    }
    Ok(plan)
}

fn process_session(
    conn: &Connection,
    key: &targets::Key,
    cutoff: i64,
    floor: i64,
    opts: &RetentionOptions,
    report: &mut RetentionReport,
) -> Result<bool, Error> {
    if !targets::expired(conn, key, cutoff).map_err(Error::query)? {
        return Ok(true);
    }
    if !opts.allow_unuploaded && !targets::uploaded(conn, key, floor).map_err(Error::query)? {
        report.protected_sessions += 1;
        return Ok(true);
    }
    evict(conn, key, opts.max_rows as u64, report).map_err(Error::query)
}

fn apply_batch(
    conn: &Connection,
    plan: &Plan,
    opts: &RetentionOptions,
    now: i64,
) -> Result<RetentionReport, Error> {
    let head = crate::change_feed::read_head(conn).map_err(Error::query)?;
    let floor = upload_floor(conn, head.epoch).map_err(Error::query)?;
    if !opts.allow_unuploaded && floor.is_none() {
        return Ok(RetentionReport {
            awaiting_upload_ack: true,
            ..Default::default()
        });
    }
    let cutoff = now.saturating_sub(i64::from(plan.days.get()) * DAY_MS);
    let mut report = RetentionReport::default();
    if let Some(key) = &plan.candidate {
        if !process_session(conn, key, cutoff, floor.unwrap_or(0), opts, &mut report)? {
            return Ok(report);
        }
    }
    if let Some(key) = &plan.scanned_through {
        targets::advance(conn, key).map_err(Error::query)?;
    } else {
        report.rows += anonymous_prompts(
            conn,
            cutoff,
            if opts.allow_unuploaded {
                head.revision as i64
            } else {
                floor.unwrap_or(0)
            },
            opts.max_rows.min(32),
        )
        .map_err(Error::query)?;
        report.cycle_complete = report.rows < u64::from(opts.max_rows.min(32));
        // Drain anonymous rows too before declaring this cycle finished.
        if report.cycle_complete {
            conn.execute("DELETE FROM retention_sweep", [])
                .map_err(Error::sql)?;
        }
    }
    Ok(report)
}

fn busy(error: &rusqlite::Error) -> bool {
    matches!(error, rusqlite::Error::SqliteFailure(code, _) if matches!(code.code, rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked))
}
fn upload_floor(conn: &Connection, epoch: u64) -> anyhow::Result<Option<i64>> {
    let (count, valid, floor): (i64, i64, Option<i64>) = conn.query_row(
        "SELECT COUNT(*),COALESCE(SUM(epoch=?1),0),MIN(revision) FROM retention_consumers",
        [epoch as i64],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    Ok(if count > 0 && count == valid {
        floor
    } else {
        None
    })
}

/// Split large sessions across transactions. Do not advance the sweep cursor
/// until every payload row is gone, so restart resumes the same session.
pub(super) fn evict(
    conn: &Connection,
    key: &targets::Key,
    budget: u64,
    report: &mut RetentionReport,
) -> anyhow::Result<bool> {
    if let Some(activity) = targets::activity(conn, key)? {
        conn.execute("INSERT INTO retention_pending VALUES (?1,?2,?3) ON CONFLICT(source,session_id) DO UPDATE SET last_activity_ms=MAX(last_activity_ms,excluded.last_activity_ms)", params![key.0,key.1,activity])?;
    }
    let mut remaining = budget;
    for (table, _) in targets::PAYLOADS {
        if !delete_part(conn, table, "session_id", key, &mut remaining, report)? {
            return Ok(false);
        }
    }
    for (table, column) in AUXILIARY {
        if !delete_part(conn, table, column, key, &mut remaining, report)? {
            return Ok(false);
        }
    }
    if key.0 == "grok" && !delete_grok_state(conn, key, &mut remaining, report)? {
        return Ok(false);
    }
    // Only fixed-size catalog/presence rows remain. All potentially large
    // trigger cascades were drained above. Keep user tags/commit links and
    // provider byte cursors: they are not session logs.
    for table in ["sessions", "session_presences", "retention_pending"] {
        conn.prepare_cached(&format!(
            "DELETE FROM {table} WHERE source=?1 AND session_id=?2"
        ))?
        .execute(params![key.0, key.1])?;
    }
    report.sessions += 1;
    Ok(true)
}

const AUXILIARY: &[(&str, &str)] = &[
    ("session_hydration_checkpoints", "session_id"),
    ("observation_hydration_checkpoints", "session_id"),
    ("canonical_evidence_protection", "session_id"),
    ("session_continuity_evidence", "session_id"),
    ("session_observations", "session_id"),
    ("session_relationships", "parent_session_id"),
    ("session_relationships", "child_session_id"),
    ("session_identity_correlations", "local_session_id"),
];
fn delete_sql(table: &str, column: &str) -> String {
    format!("DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE source=?1 AND {column}=?2 LIMIT ?3)")
}

/// Compile the trigger-heavy deletes and parse the schema before taking either
/// writer lock. SQLite statement compilation, not deletion, can dominate a batch.
fn warm_connection(conn: &Connection) -> anyhow::Result<()> {
    conn.set_prepared_statement_cache_capacity(128);
    let unused = ("[retention-warmup]".into(), String::new());
    targets::activity(conn, &unused)?;
    targets::uploaded(conn, &unused, 0)?;
    for (table, _) in targets::PAYLOADS {
        drop(conn.prepare_cached(&delete_sql(table, "session_id"))?);
    }
    for (table, column) in AUXILIARY {
        drop(conn.prepare_cached(&delete_sql(table, column))?);
    }
    for table in ["sessions", "session_presences", "retention_pending"] {
        drop(conn.prepare_cached(&format!(
            "DELETE FROM {table} WHERE source=?1 AND session_id=?2"
        ))?);
    }
    for table in ["grok_unified_usage", "grok_session_turns"] {
        drop(conn.prepare_cached(&format!("DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE session_id=?1 LIMIT ?2)"))?);
    }
    Ok(())
}

fn delete_part(
    conn: &Connection,
    table: &str,
    column: &str,
    key: &targets::Key,
    remaining: &mut u64,
    report: &mut RetentionReport,
) -> anyhow::Result<bool> {
    let limit = (*remaining).min(32);
    let removed = conn
        .prepare_cached(&delete_sql(table, column))?
        .execute(params![key.0, key.1, limit])? as u64;
    report.rows += removed;
    *remaining -= removed;
    // A full chunk yields even when the total budget is larger, bounding
    // each SQL statement as well as the total payload work per transaction.
    Ok(removed < limit)
}
fn delete_grok_state(
    conn: &Connection,
    key: &targets::Key,
    remaining: &mut u64,
    report: &mut RetentionReport,
) -> anyhow::Result<bool> {
    for table in ["grok_unified_usage", "grok_session_turns"] {
        let limit = (*remaining).min(32);
        let removed = conn.prepare_cached(&format!("DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE session_id=?1 LIMIT ?2)"))?.execute(params![key.1,limit])? as u64;
        report.rows += removed;
        *remaining -= removed;
        if removed == limit {
            return Ok(false);
        }
    }
    Ok(true)
}

fn anonymous_prompts(
    conn: &Connection,
    cutoff: i64,
    floor: i64,
    limit: u32,
) -> anyhow::Result<u64> {
    Ok(conn.execute("DELETE FROM history WHERE id IN (SELECT id FROM history WHERE session_id IS NULL AND timestamp_ms<?1 AND revision<=?2 LIMIT ?3)", params![cutoff,floor,limit])? as u64)
}
