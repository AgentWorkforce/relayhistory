//! Shared reconciliation for providers whose sessions are one JSONL file
//! each: native identity ownership, retirement of evidence a complete rewrite
//! dropped, and retirement of a locator a full sweep confirmed missing.
//! Every function is keyed on the provider's canonical source id.
mod order;
pub(crate) use order::reconcile_event_order;

use super::AcquisitionContext;
use crate::ProviderRoots;
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The evidence keys one complete normalization wrote, so a rewrite retires
/// exactly the rows the provider no longer records.
#[derive(Default)]
pub(crate) struct Keys {
    pub events: BTreeSet<String>,
    pub tools: BTreeSet<String>,
    pub results: BTreeSet<String>,
    pub markers: BTreeSet<String>,
    pub prompts: BTreeSet<String>,
}

/// Reject a native session ID already owned by a different live locator.
pub(crate) fn check_identity(
    conn: &Connection,
    source: &str,
    code: &str,
    sid: &str,
    path: &Path,
) -> Result<()> {
    let old: Option<String> = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source = ?1 AND session_id = ?2",
            [source, sid],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    if let Some(old) = old {
        let same = Path::new(&old) == path
            || std::fs::canonicalize(&old)
                .ok()
                .zip(std::fs::canonicalize(path).ok())
                .is_some_and(|(a, b)| a == b);
        anyhow::ensure!(
            same || !Path::new(&old).exists(),
            "{code}_ID_COLLISION: native session {sid:?} at {} conflicts with {old}",
            path.display()
        );
    }
    Ok(())
}

/// Retire the local evidence a complete normalization did not write again.
pub(crate) fn retire(conn: &Connection, source: &str, sid: &str, keys: &Keys) -> Result<()> {
    let prompts = serde_json::to_string(&keys.prompts)?;
    let results = serde_json::to_string(&keys.results)?;
    conn.execute(
        "UPDATE tool_calls SET is_error = NULL WHERE source = ?1 AND session_id = ?2 \
         AND location = 'local' AND is_error IS NOT NULL \
         AND tool_use_id NOT IN (SELECT value FROM json_each(?3))",
        rusqlite::params![source, sid, results],
    )?;
    for (table, key, present) in [
        ("session_events", "event_uid", &keys.events),
        ("tool_calls", "tool_use_id", &keys.tools),
        ("session_markers", "marker_uid", &keys.markers),
    ] {
        let condition = format!(
            "source = ?1 AND session_id = ?2 AND {key} NOT IN (SELECT value FROM json_each(?3))"
        );
        let keep = serde_json::to_string(present)?;
        crate::store::retire_evidence_share(
            conn,
            table,
            &condition,
            &[&source, &sid, &keep],
            crate::SessionLocation::Local,
        )?;
    }
    reconcile_prompts(conn, source, sid, &prompts)
}

// Reconcile after local shares retire: surviving remote events in this session
// and other sessions still support prompt rows. Compare complete ordered user
// text projections, and prefer the existing owner when its evidence survives.
// A control row (harness-written user text) never supports a prompt.
fn reconcile_prompts(conn: &Connection, source: &str, sid: &str, keep: &str) -> Result<()> {
    // `+e.source` keeps the planner on the timestamp index: without
    // statistics it would otherwise walk every event of the source to
    // satisfy the GROUP BY, once per prompt row.
    let reference = "SELECT e.session_id FROM session_events e \
        WHERE +e.source = ?1 AND e.role = 'user' \
          AND e.kind = 'text' AND e.control_kind IS NULL AND e.ts_ms = history.timestamp_ms \
        GROUP BY e.session_id, e.message_id \
        HAVING (SELECT group_concat(text, char(10)) FROM \
          (SELECT text FROM session_events b WHERE b.source = ?1 \
           AND b.session_id = e.session_id AND b.message_id = e.message_id \
           AND b.role = 'user' AND b.kind = 'text' AND b.control_kind IS NULL \
           ORDER BY CAST(substr(b.event_uid, length(b.message_id) + 2) AS INTEGER))) = history.prompt \
        ORDER BY (e.session_id = ?2) DESC, e.session_id LIMIT 1";
    let project = reference.replacen("SELECT e.session_id", "SELECT e.project", 1);
    // Retained prompt rows keep their owner; refresh that owner's finalized
    // project projection even when INSERT OR IGNORE saw the same prompt again.
    conn.execute(&format!("UPDATE history SET project = ({project}) WHERE source = ?1 AND session_id = ?2 AND EXISTS ({reference}) AND project IS NOT ({project})"), [source, sid])?;
    conn.execute(&format!("UPDATE history SET project = ({project}), session_id = ({reference}) \
        WHERE source = ?1 AND session_id = ?2 \
        AND (timestamp_ms || ':' || coalesce(prompt_hash, '')) NOT IN (SELECT value FROM json_each(?3)) \
        AND EXISTS ({reference}) AND session_id IS NOT ({reference})"), rusqlite::params![source, sid, keep])?;
    conn.execute(&format!("DELETE FROM history WHERE source = ?1 AND session_id = ?2 \
        AND (timestamp_ms || ':' || coalesce(prompt_hash, '')) NOT IN (SELECT value FROM json_each(?3)) \
        AND NOT EXISTS ({reference})"), rusqlite::params![source, sid, keep])?;
    Ok(())
}

/// Retire every local share of the sessions recorded at `path`.
pub(crate) fn retire_missing_locator(conn: &Connection, source: &str, path: &Path) -> Result<()> {
    retire_locator_sessions(conn, source, path, None)
}

/// Retire the local share of every session recorded at `path` except `keep`:
/// the sessions a locator no longer backs.
pub(crate) fn retire_locator_sessions(
    conn: &Connection,
    source: &str,
    path: &Path,
    keep: Option<&str>,
) -> Result<()> {
    let local = local_footprint();
    let mut stmt = conn.prepare(&format!(
        "SELECT session_id FROM sessions s WHERE source = ?1 AND raw_path = ?2 \
         AND session_id IS NOT ?3 AND ({local})"
    ))?;
    let sessions = stmt
        .query_map(
            rusqlite::params![source, path.to_string_lossy(), keep],
            |r| r.get::<_, String>(0),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for sid in sessions {
        retire_local_session(conn, source, &sid)?;
    }
    Ok(())
}

/// Retire the local share of one session: its local-only evidence, prompt
/// ownership, local presence, and the catalog row once nothing backs it.
pub(crate) fn retire_local_session(conn: &Connection, source: &str, sid: &str) -> Result<()> {
    crate::ingest::retire_local_session_evidence(conn, source, sid)?;
    reconcile_prompts(conn, source, sid, "[]")?;
    for table in [
        "session_observations",
        "session_hydration_checkpoints",
        "session_presences",
    ] {
        conn.execute(
            &format!(
                "DELETE FROM {table} WHERE source = ?1 AND session_id = ?2 AND location = 'local'"
            ),
            [source, sid],
        )?;
    }
    conn.execute("DELETE FROM sessions WHERE source=?1 AND session_id=?2 AND NOT EXISTS(SELECT 1 FROM session_observations WHERE source=?1 AND session_id=?2 AND location='remote') AND NOT EXISTS(SELECT 1 FROM session_presences WHERE source=?1 AND session_id=?2 AND location='remote') AND NOT EXISTS(SELECT 1 FROM session_events WHERE source=?1 AND session_id=?2) AND NOT EXISTS(SELECT 1 FROM tool_calls WHERE source=?1 AND session_id=?2) AND NOT EXISTS(SELECT 1 FROM session_markers WHERE source=?1 AND session_id=?2) AND NOT EXISTS(SELECT 1 FROM file_edits WHERE source=?1 AND session_id=?2)", [source, sid])?;
    refresh_remote_catalog(conn, source, sid)
}

fn refresh_remote_catalog(conn: &Connection, source: &str, sid: &str) -> Result<()> {
    for (column, recorded, fallback) in [
        ("raw_path", "raw_locator", "SELECT raw_locator FROM session_presences WHERE source=?1 AND session_id=?2 AND location='remote' LIMIT 1"),
        ("source_stamp", "source_stamp", "SELECT source_stamp FROM session_presences WHERE source=?1 AND session_id=?2 AND location='remote' LIMIT 1"),
        ("first_prompt", "first_prompt", "SELECT prompt FROM history h WHERE source=?1 AND session_id=?2 AND EXISTS(SELECT 1 FROM session_events e WHERE e.source=h.source AND e.session_id=h.session_id AND e.role='user' AND e.location='remote' AND e.ts_ms=h.timestamp_ms) ORDER BY timestamp_ms,id LIMIT 1"),
        ("last_assistant_text", "last_assistant_text", "SELECT text FROM session_events WHERE source=?1 AND session_id=?2 AND location='remote' AND role='assistant' AND kind='text' ORDER BY ts_ms DESC,id DESC LIMIT 1"),
    ] {
        let value = format!("COALESCE((SELECT {recorded} FROM session_observations WHERE source=?1 AND session_id=?2 AND location='remote' AND {recorded} IS NOT NULL ORDER BY connector_id='legacy-unknown',access_state!='available',connector_id,connector_instance LIMIT 1),({fallback}))");
        conn.execute(&format!("UPDATE sessions SET {column}={value} WHERE source=?1 AND session_id=?2 AND {column} IS NOT {value}"), [source, sid])?;
    }
    conn.execute("UPDATE sessions SET discovery_state=CASE WHEN EXISTS(SELECT 1 FROM session_events WHERE source=?1 AND session_id=?2) OR EXISTS(SELECT 1 FROM session_observations WHERE source=?1 AND session_id=?2 AND location='remote' AND discovery_state='full') OR EXISTS(SELECT 1 FROM session_presences WHERE source=?1 AND session_id=?2 AND location='remote' AND discovery_state='full') THEN 'full' ELSE 'shallow' END WHERE source=?1 AND session_id=?2", [source, sid])?;
    Ok(())
}

/// Retire locators a complete, accessible enumeration no longer finds.
/// Called only after successful full enumeration and normalization of survivors.
pub(crate) fn retire_missing(conn: &Connection, source: &str, roots: &ProviderRoots) -> Result<()> {
    let local = local_footprint();
    let mut stmt = conn.prepare(&format!("SELECT DISTINCT raw_path FROM sessions s WHERE source=?1 AND raw_path IS NOT NULL AND ({local})"))?;
    let paths = stmt
        .query_map([source], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for path in paths {
        crate::ingest::check_capture_cancelled()?;
        let path = PathBuf::from(path);
        if confirmed_missing(source, roots, &path)? {
            let tx = conn.unchecked_transaction()?;
            retire_missing_locator(&tx, source, &path)?;
            tx.commit()?;
        }
    }
    Ok(())
}

fn confirmed_missing(source: &str, roots: &ProviderRoots, path: &Path) -> Result<bool> {
    if path.extension().is_none_or(|e| e != "jsonl") {
        return Ok(false);
    }
    let context = AcquisitionContext::new(source, roots);
    for root in roots.for_provider(source) {
        let Ok(relative) = path.strip_prefix(root) else {
            continue;
        };
        if !relative
            .components()
            .all(|p| matches!(p, std::path::Component::Normal(_)))
            || !root.is_dir()
        {
            continue;
        }
        match std::fs::symlink_metadata(path) {
            Ok(_) => {
                context.validate(path)?;
                return Ok(false);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        context.validate(root)?;
        for entry in std::fs::read_dir(root)? {
            entry?;
        }
        let mut parent = path.parent();
        while let Some(dir) = parent {
            if dir == root {
                break;
            }
            validate_directory(&context, dir)?;
            parent = dir.parent();
        }
        // Revalidate the required root before accepting the locator's absence.
        context.validate(root)?;
        std::fs::read_dir(root)?;
        return Ok(!path.try_exists()?);
    }
    Ok(false)
}

fn validate_directory(context: &AcquisitionContext<'_>, dir: &Path) -> Result<()> {
    match std::fs::symlink_metadata(dir) {
        Ok(_) => {
            context.validate(dir)?;
            for entry in std::fs::read_dir(dir)? {
                entry?;
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

fn local_footprint() -> String {
    let evidence = crate::store::EVIDENCE_LOCATION_TABLES.iter().map(|table|
        format!("EXISTS(SELECT 1 FROM {table} e WHERE e.source=s.source AND e.session_id=s.session_id AND e.location IN ('local','both'))"))
        .collect::<Vec<_>>().join(" OR ");
    format!("EXISTS(SELECT 1 FROM session_presences p WHERE p.source=s.source AND p.session_id=s.session_id AND p.location='local') OR EXISTS(SELECT 1 FROM session_observations o WHERE o.source=s.source AND o.session_id=s.session_id AND o.location='local') OR {evidence}")
}
