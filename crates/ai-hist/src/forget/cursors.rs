use super::{ObservationKey, SessionLocation, Target};
use rusqlite::{params, Connection};
use std::path::Path;

/// Drop the byte cursors into the session's own transcript and the sidecar
/// directory beside it (`<transcript stem>/`, where Claude keeps subagent
/// transcripts). A transcript other sessions also live in — an OpenCode or
/// Devin database — keeps its cursor: it positions their evidence too.
pub(super) fn forget_cursors(
    tx: &Connection,
    target: &Target,
    cursor_sources: &[String],
) -> rusqlite::Result<u64> {
    if cursor_sources.is_empty() {
        return Ok(0);
    }
    let (source, session_id) = (target.key.0.as_str(), target.key.1.as_str());
    let mut locators = local_locators(tx, source, session_id)?;
    locators.extend(target.child_locators.iter().cloned());
    let mut removed = 0;
    for locator in locators {
        let shared: bool = tx
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE source = ?2 AND raw_path = ?1 \
                   AND session_id <> ?3)",
            )?
            .query_row(params![locator, source, session_id], |row| row.get(0))?;
        if shared {
            continue;
        }
        let sidecars = sidecar_prefix(&locator);
        let sidecars_end = prefix_end(&sidecars);
        // A Claude subagent transcript's metadata document keeps a cursor of
        // its own beside it.
        let metadata = Path::new(&locator)
            .with_extension("meta.json")
            .to_string_lossy()
            .into_owned();
        for cursor_source in cursor_sources {
            removed += tx
                .prepare_cached(
                    "DELETE FROM transcript_cursors WHERE source = ?1 \
                     AND (locator = ?2 OR locator = ?5 OR (locator >= ?3 AND locator < ?4))",
                )?
                .execute(params![
                    cursor_source,
                    locator,
                    sidecars,
                    sidecars_end,
                    metadata
                ])? as u64;
        }
    }
    Ok(removed)
}

/// The session's own local transcript locators, as the catalog records them.
fn local_locators(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> rusqlite::Result<Vec<String>> {
    conn.prepare_cached(
        "SELECT raw_path FROM sessions WHERE source = ?1 AND session_id = ?2 \
           AND raw_path IS NOT NULL \
         UNION SELECT raw_locator FROM session_presences \
           WHERE source = ?1 AND session_id = ?2 AND location = ?3 \
           AND raw_locator IS NOT NULL \
         UNION SELECT raw_locator FROM session_observations \
           WHERE source = ?1 AND session_id = ?2 AND location = ?3 \
           AND raw_locator IS NOT NULL",
    )?
    .query_map(
        params![source, session_id, SessionLocation::Local.as_str()],
        |row| row.get(0),
    )?
    .collect()
}

/// `<dir>/<stem>/` for `<dir>/<stem>.<ext>`.
fn sidecar_prefix(locator: &str) -> String {
    let path = Path::new(locator);
    let stem = path.with_extension("");
    format!("{}{}", stem.to_string_lossy(), std::path::MAIN_SEPARATOR)
}

/// The smallest string greater than every string starting with `prefix`,
/// which ends in an ASCII separator.
fn prefix_end(prefix: &str) -> String {
    let mut end = prefix.to_string();
    if let Some(last) = end.pop() {
        end.push((last as u8 + 1) as char);
    }
    end
}

pub(super) fn observation_keys(
    tx: &Connection,
    source: &str,
    session_id: &str,
) -> rusqlite::Result<Vec<ObservationKey>> {
    tx.prepare_cached(
        "SELECT location, connector_id, connector_instance FROM session_observations \
         WHERE source = ? AND session_id = ?",
    )?
    .query_map(params![source, session_id], |row| {
        let location: String = row.get(0)?;
        Ok(ObservationKey {
            source: source.to_string(),
            session_id: session_id.to_string(),
            location: if location == "remote" {
                SessionLocation::Remote
            } else {
                SessionLocation::Local
            },
            connector_id: row.get(1)?,
            connector_instance: row.get(2)?,
        })
    })?
    .collect()
}
