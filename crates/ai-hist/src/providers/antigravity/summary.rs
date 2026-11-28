use super::*;
use rusqlite::OptionalExtension;
use std::collections::HashMap;

pub(super) fn scope_mentions(scope: &crate::path_scope::PathScope, brain_root: &Path) -> bool {
    roots::summary_db_paths(brain_root)
        .iter()
        .any(|path| scope.mentions(path))
}

/// Shallow candidate stamp version for Antigravity.
///
/// Bumped when Antigravity's shallow prompt/title extraction changes so cached
/// shallow rows for unchanged, untitled transcripts are re-read without
/// invalidating every other provider's shallow cache.
const SHALLOW_STAMP_VERSION: &str = "v4";

pub(super) fn stamp_with_title(base: String, title: Option<&str>) -> String {
    match title {
        Some(title) => format!(
            "{base}:{SHALLOW_STAMP_VERSION}:title:{:x}",
            Sha256::digest(title.as_bytes())
        ),
        None => format!("{base}:{SHALLOW_STAMP_VERSION}"),
    }
}

/// Apply current titles from `conversation_summaries.db` directly to
/// already-cataloged sessions under `brain_root`, without replaying
/// transcripts.
pub(super) fn reconcile_titles(
    conn: &Connection,
    brain_root: &Path,
    context: &AcquisitionContext<'_>,
) -> Result<usize> {
    let titles = read_titles_for_brain(brain_root, context);
    let mut updated = 0;
    for (native_id, title) in titles {
        crate::ingest::check_capture_cancelled()?;
        if native_id.is_empty()
            || Path::new(&native_id).components().count() != 1
            || matches!(native_id.as_str(), "." | "..")
        {
            continue;
        }
        let sid = roots::identity_for_native(context.roots, brain_root, &native_id)?;
        if crate::ingest::set_session_title(conn, ID, &sid, Some(&title))? {
            updated += 1;
        }
    }
    Ok(updated)
}

pub(super) fn read_titles_for_brain(
    brain_root: &Path,
    context: &AcquisitionContext<'_>,
) -> HashMap<String, String> {
    let Some(db) = validated_summary_db(brain_root, context) else {
        return HashMap::new();
    };
    let loaded = crate::providers::sqlite_snapshot(context, &db, |conn| {
        if !has_summaries_table(conn)? {
            return Ok(HashMap::new());
        }
        let mut stmt = conn.prepare(
            "SELECT conversation_id, title FROM conversation_summaries WHERE title != ''",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut out = HashMap::new();
        for row in rows {
            let (id, raw) = row?;
            if let Some(title) = crate::ingest::titles::non_blank_title(Some(&raw)) {
                out.insert(id, title);
            }
        }
        Ok(out)
    });
    loaded.unwrap_or_default()
}

pub(super) fn read_title_for_transcript(
    path: &Path,
    context: &AcquisitionContext<'_>,
) -> (Option<String>, Option<PathBuf>) {
    let Ok((brain_root, native_id)) = roots::locate(context.roots, path) else {
        return (None, None);
    };
    let Some(db) = validated_summary_db(brain_root, context) else {
        return (None, None);
    };
    let loaded = crate::providers::sqlite_snapshot(context, &db, |conn| {
        if !has_summaries_table(conn)? {
            return Ok(None);
        }
        let raw: Option<String> = conn
            .query_row(
                "SELECT title FROM conversation_summaries WHERE conversation_id = ?1",
                [native_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        Ok(crate::ingest::titles::non_blank_title(raw.as_deref()))
    });
    match loaded {
        Ok(title) => (title, Some(db)),
        Err(_) => (None, None),
    }
}

pub(super) fn fingerprint_candidates(env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
    let mut out = Vec::new();
    for brain_root in env.provider_roots.for_provider(ID) {
        let Some(db) = roots::summary_db_for_brain(brain_root) else {
            continue;
        };
        for suffix in ["", "-wal"] {
            let mut path = db.as_os_str().to_os_string();
            path.push(suffix);
            let path = PathBuf::from(path);
            let Ok((stamp, recency_hint_ms)) = crate::file_stamp_and_modified(&path) else {
                continue;
            };
            out.push(Candidate {
                source: ID,
                locator: path.to_string_lossy().into_owned(),
                session_id: None,
                recency_hint_ms,
                stamp,
            });
        }
    }
    Ok(out)
}

fn validated_summary_db(brain_root: &Path, context: &AcquisitionContext<'_>) -> Option<PathBuf> {
    let db = roots::summary_db_for_brain(brain_root)?;
    if !db.try_exists().ok()? {
        return None;
    }
    let parent = std::fs::canonicalize(db.parent()?).ok()?;
    for path in roots::summary_db_paths(brain_root) {
        if !path.try_exists().ok()? {
            continue;
        }
        let canonical = std::fs::canonicalize(&path).ok()?;
        if !canonical.starts_with(&parent) {
            return None;
        }
        context.validate(&path).ok()?;
    }
    Some(db)
}

fn has_summaries_table(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='conversation_summaries')",
        [],
        |row| row.get(0),
    )?)
}
