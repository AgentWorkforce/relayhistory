//! The transcript a catalog row already names keeps that row.
//!
//! Forks and resumed sessions are sibling transcripts that share one session
//! id. While `sessions.raw_path` names a file that is still there, a different
//! transcript does not replace the row's identity. The same file still updates
//! it, and a missing file lets the transcript being read claim the row.

use crate::discover::ShallowSession;
use crate::observations::ObservationKey;
use crate::SessionLocation;
use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

/// Whether `incoming` is a different transcript from the one `sessions.raw_path`
/// already names, and that file is still present.
///
/// One stat of the stored path, and only when the paths differ. Same path, no
/// stored path, and a stored path that is no longer a file all return false so
/// the caller writes normally.
pub(crate) fn incoming_must_not_replace(
    conn: &Connection,
    source: &str,
    session_id: &str,
    incoming: Option<&str>,
) -> Result<bool> {
    let Some(incoming) = incoming.filter(|path| !path.is_empty()) else {
        return Ok(false);
    };
    let stored: Option<String> = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source = ?1 AND session_id = ?2",
            params![source, session_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(stored) = stored.filter(|path| !path.is_empty()) else {
        return Ok(false);
    };
    if stored == incoming {
        return Ok(false);
    }
    Ok(Path::new(&stored).is_file())
}

/// Whether the catalog row's `raw_path` is `path`.
///
/// The catalog upsert has already kept or claimed the row, so this is a
/// compare and not another stat.
pub(crate) fn recorded_path_is(
    conn: &Connection,
    source: &str,
    session_id: &str,
    path: &str,
) -> Result<bool> {
    let stored: Option<String> = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source = ?1 AND session_id = ?2",
            params![source, session_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(stored.as_deref() == Some(path))
}

/// SQL fragments for a full-ingest catalog upsert.
///
/// When `keep` is set, the recorded path and its assistant excerpt stay. The
/// caller's activity assignment is unchanged either way.
pub(crate) fn session_assigns<'a>(
    keep: bool,
    assistant_text: &'a str,
    raw_path: Option<&'a str>,
) -> (&'a str, &'static str, Option<&'a str>) {
    if keep {
        (
            "last_assistant_text = sessions.last_assistant_text",
            "raw_path = sessions.raw_path",
            None,
        )
    } else {
        (
            assistant_text,
            "raw_path = COALESCE(excluded.raw_path, sessions.raw_path)",
            raw_path,
        )
    }
}

/// The shallow row to write, or `None` when this read may update identity.
///
/// Only a local transcript can own `sessions.raw_path`. A remote upsert keeps
/// the locator it carried.
pub(crate) fn withhold_shallow(
    conn: &Connection,
    session: &ShallowSession,
    location: SessionLocation,
) -> Result<Option<ShallowSession>> {
    if location == SessionLocation::Local
        && incoming_must_not_replace(
            conn,
            &session.source,
            &session.session_id,
            session.raw_path.as_deref(),
        )?
    {
        Ok(Some(without_foreign_identity(session)))
    } else {
        Ok(None)
    }
}

/// A shallow row that leaves stored identity columns alone.
///
/// `COALESCE(excluded, stored)` keeps the stored value when the incoming one
/// is null, which is how a sibling read avoids replacing the recorded file.
pub(crate) fn without_foreign_identity(session: &ShallowSession) -> ShallowSession {
    ShallowSession {
        raw_path: None,
        first_prompt: None,
        title: None,
        models: Vec::new(),
        agent_version: None,
        source_stamp: None,
        last_assistant_text: None,
        ..session.clone()
    }
}

/// Locator, stamp, and previews to write when the observation already points
/// at the catalog file and the incoming locator is a different transcript.
///
/// No stat: the catalog upsert in this same write decided whether that file
/// still owns the row. An observation whose locator is not the catalog path
/// (a database session id, a remote connector) is left alone, as is a missing
/// observation, so the incoming locator is inserted.
pub(crate) struct RetainedObservation {
    pub(crate) raw_locator: Option<String>,
    pub(crate) source_stamp: Option<String>,
    pub(crate) first_prompt: Option<String>,
    pub(crate) last_assistant_text: Option<String>,
}

pub(crate) fn retained_observation(
    conn: &Connection,
    key: &ObservationKey,
    incoming_locator: Option<&str>,
) -> Result<Option<RetainedObservation>> {
    let incoming = incoming_locator.unwrap_or("");
    conn.query_row(
        "SELECT o.raw_locator, o.source_stamp, o.first_prompt, o.last_assistant_text \
         FROM session_observations o \
         JOIN sessions s ON s.source = o.source AND s.session_id = o.session_id \
         WHERE o.source = ?1 AND o.session_id = ?2 AND o.location = ?3 \
           AND o.connector_id = ?4 AND o.connector_instance = ?5 \
           AND o.raw_locator IS NOT NULL \
           AND o.raw_locator = s.raw_path \
           AND o.raw_locator <> ?6",
        params![
            key.source,
            key.session_id,
            key.location.as_str(),
            key.connector_id,
            key.connector_instance,
            incoming
        ],
        |row| {
            Ok(RetainedObservation {
                raw_locator: row.get(0)?,
                source_stamp: row.get(1)?,
                first_prompt: row.get(2)?,
                last_assistant_text: row.get(3)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}
