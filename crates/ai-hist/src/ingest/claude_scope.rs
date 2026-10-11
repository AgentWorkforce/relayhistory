//! Path-scoped Claude bookkeeping that must not look like a full sweep.
//!
//! A tick that read one transcript still has to forget that transcript's
//! cursor when the file is gone, and it still has to leave a held-record
//! flag standing when it did not visit the file that set it. Neither of
//! those may scan the cursor table or clear a flag the tick did not check.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::Result;
use rusqlite::{params, Connection};
use serde_json::{Map, Value};

use super::super::path_scope::{self, PathScope, RootWalk};

/// The value of `claude_holding_records` after this walk.
///
/// A full walk may clear the flag: it visited every transcript. A partial
/// walk may set it, and otherwise keeps a stored `true`. Clearing it after
/// reading one other file is how the next backstop skips a message this
/// walk never opened.
pub(crate) fn claude_holding_flag(
    paths: Option<&PathScope>,
    root: &Path,
    seen_holding: bool,
    state: &Map<String, Value>,
) -> bool {
    seen_holding
        || (!PathScope::walk_is_full(paths, root)
            && super::super::incremental::sweep_left_records_held(state))
}

/// Claude cursors this tick may drop.
///
/// A skip returns nothing and does not query. A full walk reads the source's
/// locators, which is the deletion check it already did. A partial walk
/// names the transcripts the scope could have listed and point-looks those
/// up, or range-scans one directory when a sidecar or a directory event
/// cannot name a deleted sibling.
pub(crate) fn claude_missing_cursors(
    conn: &Connection,
    paths: Option<&PathScope>,
    root: &Path,
    observed: &HashSet<String>,
) -> Result<Vec<String>> {
    match PathScope::walk_of(paths, root) {
        RootWalk::Skip => Ok(Vec::new()),
        RootWalk::Full => missing_anywhere(conn, root, observed),
        RootWalk::Partial(scope) => missing_in_scope(conn, root, scope, observed),
    }
}

fn missing_anywhere(
    conn: &Connection,
    root: &Path,
    observed: &HashSet<String>,
) -> Result<Vec<String>> {
    Ok(
        super::super::transcript_cursor::known_locators(conn, "claude")?
            .into_iter()
            .filter(|locator| {
                super::super::path_key_is_under(locator, root) && !observed.contains(locator)
            })
            .collect(),
    )
}

fn missing_in_scope(
    conn: &Connection,
    root: &Path,
    scope: &PathScope,
    observed: &HashSet<String>,
) -> Result<Vec<String>> {
    let mut missing = Vec::new();
    let mut seen = HashSet::new();
    for changed in scope.paths() {
        if !changed.starts_with(root) {
            continue;
        }
        if path_scope::is_collected(changed, "", "jsonl") {
            note_absent_cursor(&mut missing, &mut seen, conn, root, changed, observed)?;
            for candidate in stem_session_files(root, changed) {
                note_stem_cursor(&mut missing, &mut seen, conn, root, &candidate, observed)?;
            }
        } else {
            // The range is inside the directory. The session file sits beside
            // that directory (`…/<id>/` → `…/<id>.jsonl`), which is the same
            // stem match the listing uses. A vanished transcript loses its
            // cursor; one that is still on disk keeps it, because a removed
            // directory is not a path the listing can walk to that file.
            note_absent_prefix(
                &mut missing,
                &mut seen,
                conn,
                root,
                scope,
                scan_dir(changed),
                observed,
            )?;
            for candidate in stem_session_files(root, changed) {
                note_stem_cursor(&mut missing, &mut seen, conn, root, &candidate, observed)?;
            }
        }
    }
    Ok(missing)
}

/// The `<name>.jsonl` above a nested change. A collected file matches itself
/// by equality, so this only names the session file a directory component
/// points at (`…/p/one/subagents/a.jsonl` → `…/p/one.jsonl`).
fn stem_session_files(root: &Path, changed: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut dir = changed.parent();
    while let Some(current) = dir {
        if current != root && !current.starts_with(root) {
            break;
        }
        if let Some(name) = changed
            .strip_prefix(current)
            .ok()
            .and_then(|rest| rest.components().next())
            .and_then(|component| component.as_os_str().to_str())
        {
            if Path::new(name).extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
                out.push(current.join(format!("{name}.jsonl")));
            }
        }
        if current == root {
            break;
        }
        dir = current.parent();
    }
    out
}

/// The directory whose cursors this path can have deleted.
///
/// A directory event, including one whose directory is already gone, names
/// that directory. A sidecar names the transcripts beside it, so the scan is
/// its parent. `is_dir` runs only for a path that already has an extension,
/// which a transcript append is not: those take the point lookup above.
fn scan_dir(changed: &Path) -> &Path {
    if changed.extension().is_none() || changed.is_dir() {
        changed
    } else {
        changed.parent().unwrap_or(changed)
    }
}

/// A stem file the listing did not return. The listing reaches it only by
/// walking the directory of the same name, so a removed directory leaves a
/// live transcript unlisted. Forgetting that cursor would reread the file
/// from byte zero. A stat here runs only for a path the listing missed.
fn note_stem_cursor(
    missing: &mut Vec<String>,
    seen: &mut HashSet<String>,
    conn: &Connection,
    root: &Path,
    path: &Path,
    observed: &HashSet<String>,
) -> Result<()> {
    // Already listed: the file was walked, and this stat would be a second one.
    if observed.contains(path.to_string_lossy().as_ref()) {
        return Ok(());
    }
    if path.is_file() {
        return Ok(());
    }
    note_absent_cursor(missing, seen, conn, root, path, observed)
}

fn note_absent_cursor(
    missing: &mut Vec<String>,
    seen: &mut HashSet<String>,
    conn: &Connection,
    root: &Path,
    path: &Path,
    observed: &HashSet<String>,
) -> Result<()> {
    let locator = path.to_string_lossy().into_owned();
    // In the listing already: the file is here, and this is the append hot
    // path, so it costs no query. `seen` skips a second lookup of the same
    // path from another changed file.
    if !super::super::path_key_is_under(&locator, root)
        || observed.contains(&locator)
        || !seen.insert(locator.clone())
    {
        return Ok(());
    }
    if super::super::transcript_cursor::locator_cursor_exists(conn, "claude", path)? {
        missing.push(locator);
    }
    Ok(())
}

fn note_absent_prefix(
    missing: &mut Vec<String>,
    seen: &mut HashSet<String>,
    conn: &Connection,
    root: &Path,
    scope: &PathScope,
    dir: &Path,
    observed: &HashSet<String>,
) -> Result<()> {
    let Some((prefix, upper)) = locator_prefix_bounds(dir) else {
        return Ok(());
    };
    // `(source, locator)` is the primary key. The bounds are the directory
    // plus a trailing separator, and the next string after it, so this is a
    // range of that directory rather than a scan of every Claude locator.
    // `LIKE` is ASCII-case-insensitive and would forget the wrong row.
    let mut statement = conn.prepare_cached(
        "SELECT locator FROM transcript_cursors \
         WHERE source = ?1 AND locator >= ?2 AND locator < ?3",
    )?;
    let rows = statement
        .query_map(params!["claude", prefix, upper], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    for locator in rows {
        if super::super::path_key_is_under(&locator, root)
            && !observed.contains(&locator)
            && scope.include_file(Path::new(&locator), "", "jsonl")
            && seen.insert(locator.clone())
        {
            missing.push(locator);
        }
    }
    Ok(())
}

/// `dir/` and the first string no locator inside `dir` can be. The bound
/// always ends on the separator, so the increment is one ASCII byte.
fn locator_prefix_bounds(dir: &Path) -> Option<(String, String)> {
    let mut prefix = dir.to_string_lossy().into_owned();
    if !prefix.ends_with(std::path::MAIN_SEPARATOR) {
        prefix.push(std::path::MAIN_SEPARATOR);
    }
    let mut upper: Vec<char> = prefix.chars().collect();
    let last = upper.pop()?;
    upper.push(char::from_u32(u32::from(last).checked_add(1)?)?);
    Some((prefix, upper.into_iter().collect()))
}
