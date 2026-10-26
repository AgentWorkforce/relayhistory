use super::*;
use crate::ingest::transcript_cursor::{load_cursor, CursorKey, TranscriptCursorState};
use rusqlite::OptionalExtension;
use serde_json::Value;

const COMPLETE_LOCATOR_KEY: &str = "junieCompleteLocatorV1";

pub(super) fn set_complete_locator(cursor: &mut TranscriptCursorState, path: &Path) {
    cursor.extra.insert(
        COMPLETE_LOCATOR_KEY.into(),
        Value::String(path.to_string_lossy().into_owned()),
    );
}

pub(super) fn preserve_complete_locator(
    conn: &Connection,
    sid: &str,
    cursor: &mut TranscriptCursorState,
) -> Result<()> {
    let stored = load_cursor(
        conn,
        &CursorKey::Session {
            source: ID,
            session_id: sid,
            location: "local",
        },
    )?;
    if let Some(locator) = stored.extra.get(COMPLETE_LOCATOR_KEY) {
        cursor
            .extra
            .insert(COMPLETE_LOCATOR_KEY.into(), locator.clone());
    }
    Ok(())
}

pub(super) fn invalidate_relocated_local_capture(
    conn: &Connection,
    sid: &str,
    path: &Path,
) -> Result<()> {
    let moved = conn.execute(
        "UPDATE session_presences SET discovery_state='shallow' \
         WHERE source=?1 AND session_id=?2 AND location='local' \
         AND discovery_state='full' AND raw_locator IS NOT NULL AND raw_locator<>?3",
        rusqlite::params![ID, sid, path.to_string_lossy()],
    )?;
    if moved > 0 {
        // The observation projection otherwise promotes the just-demoted
        // presence back to `full` from the old observation during this same
        // discovery transaction. Neither row proves capture at the new path.
        conn.execute(
            "UPDATE session_observations SET discovery_state='shallow' \
             WHERE source=?1 AND session_id=?2 AND location='local' \
             AND discovery_state='full' AND raw_locator IS NOT ?3",
            rusqlite::params![ID, sid, path.to_string_lossy()],
        )?;
        conn.execute(
            "UPDATE sessions SET discovery_state=CASE WHEN EXISTS ( \
               SELECT 1 FROM session_presences p WHERE p.source=?1 \
               AND p.session_id=?2 AND p.location='remote' AND p.discovery_state='full' \
             ) OR EXISTS ( \
               SELECT 1 FROM session_observations o WHERE o.source=?1 \
               AND o.session_id=?2 AND o.location='remote' AND o.discovery_state='full' \
             ) THEN 'full' ELSE 'shallow' END \
             WHERE source=?1 AND session_id=?2",
            rusqlite::params![ID, sid],
        )?;
        // A remote presence may own the merged catalog cwd. Only retract the
        // old local projection when there is no independent remote presence.
        conn.execute(
            "UPDATE sessions SET cwd=NULL, project_key=NULL, project_key_method=NULL \
             WHERE source=?1 AND session_id=?2 \
             AND NOT EXISTS (SELECT 1 FROM session_presences p WHERE p.source=?1 \
               AND p.session_id=?2 AND p.location='remote')",
            rusqlite::params![ID, sid],
        )?;
    }
    Ok(())
}

fn project_dir(
    conn: &Connection,
    sid: &str,
    path: &Path,
    summary: &Value,
) -> Result<Option<String>> {
    if let Some(cwd) = summary["projectDir"].as_str() {
        return Ok(Some(cwd.to_owned()));
    }
    if !summary.is_null() {
        // A present native summary without a project is authoritative absence.
        return Ok(None);
    }
    let complete_locator: Option<String> = conn
        .query_row(
            "SELECT parser_state_json FROM session_hydration_checkpoints \
             WHERE source=?1 AND session_id=?2 AND location='local'",
            rusqlite::params![ID, sid],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    let cursor = TranscriptCursorState::decode(complete_locator.as_deref());
    let complete_locator = cursor
        .extra
        .get(COMPLETE_LOCATOR_KEY)
        .and_then(Value::as_str);
    if complete_locator != Some(path.to_string_lossy().as_ref()) {
        return Ok(None);
    }
    // Missing/unreadable optional metadata cannot retract the project from a
    // prior complete normalization at this same native locator. The generic
    // hydration wrapper marks presence full even when an incomplete journal
    // deferred replacement, so presence state alone cannot prove provenance.
    // The shared catalog cwd may also have been replaced by a remote
    // observation: take only a unique local or shared-local event fact.
    Ok(conn
        .query_row(
            "SELECT MIN(e.cwd) FROM session_events e \
             JOIN session_presences p USING (source,session_id) \
             WHERE e.source=?1 AND e.session_id=?2 AND p.location='local' \
             AND p.discovery_state='full' AND p.raw_locator=?3 \
             AND e.location IN ('local','both') AND e.cwd IS NOT NULL \
             GROUP BY e.source,e.session_id HAVING COUNT(DISTINCT e.cwd)=1",
            rusqlite::params![ID, sid, path.to_string_lossy()],
            |row| row.get(0),
        )
        .optional()?)
}

pub(super) fn record_complete_capture(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    let mut cursor = load_cursor(
        conn,
        &CursorKey::Session {
            source: ID,
            session_id: sid,
            location: "local",
        },
    )?;
    set_complete_locator(&mut cursor, path);
    conn.execute(
        "INSERT INTO session_hydration_checkpoints \
         (source,session_id,location,parser_version,updated_ms,parser_state_json) \
         VALUES (?1,?2,'local',?3,CAST(strftime('%s','now') AS INTEGER)*1000,?4) \
         ON CONFLICT(source,session_id,location) DO UPDATE SET \
           parser_state_json=excluded.parser_state_json",
        rusqlite::params![
            ID,
            sid,
            crate::ingest::hydrate::HYDRATION_PARSER_VERSION,
            cursor.encode()?
        ],
    )?;
    Ok(())
}

fn clear_unknown_local_project_keys(conn: &Connection, sid: &str, cwd: Option<&str>) -> Result<()> {
    if cwd.is_none() {
        // The rebuilt catalog upsert keeps existing values when its incoming
        // cwd is NULL. Retract only the local-only projection; a remote
        // presence is independent authority for the merged catalog row.
        conn.execute(
            "UPDATE sessions SET cwd=NULL, project_key=NULL, project_key_method=NULL \
             WHERE source=?1 AND session_id=?2 AND NOT EXISTS ( \
               SELECT 1 FROM session_presences p WHERE p.source=?1 \
               AND p.session_id=?2 AND p.location='remote')",
            rusqlite::params![ID, sid],
        )?;
        // Generic event upserts retain a stored project key when the incoming
        // one is absent. A complete Junie capture with no project cannot keep
        // that key on local-only evidence from an earlier journal.
        conn.execute(
            "UPDATE session_events SET project_key=NULL, project_key_method=NULL \
             WHERE source=?1 AND session_id=?2 AND location='local' \
             AND project_key IS NOT NULL",
            rusqlite::params![ID, sid],
        )?;
    }
    Ok(())
}

pub(super) fn project_for_capture(
    conn: &Connection,
    sid: &str,
    path: &Path,
    summary: &Value,
) -> Result<Option<String>> {
    let cwd = project_dir(conn, sid, path, summary)?;
    clear_unknown_local_project_keys(conn, sid, cwd.as_deref())?;
    Ok(cwd)
}
