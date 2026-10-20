//! Deletion is proven only by a complete snapshot or by confirmed absence
//! inside an accessible configured root; remote shares always survive.
use super::*;
use crate::providers::file_history;

/// Retire local evidence of sessions recorded at `path` that a complete
/// snapshot no longer contains.
pub(super) fn retire_absent(conn: &Connection, path: &Path, ids: &[String]) -> Result<()> {
    let keep: std::collections::BTreeSet<&str> = ids.iter().map(String::as_str).collect();
    let mut stmt =
        conn.prepare("SELECT session_id FROM sessions WHERE source=?1 AND raw_path=?2")?;
    let recorded = stmt
        .query_map(rusqlite::params![ID, path.to_string_lossy()], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for id in recorded {
        if !keep.contains(id.as_str()) {
            crate::ingest::check_capture_cancelled()?;
            file_history::retire_local_session(conn, ID, &id)?;
        }
    }
    Ok(())
}
/// Retire sessions recorded at an agent database or legacy transcript that
/// is confirmed gone, for each kind only when every store of it was read.
pub(super) fn retire_missing_locators(
    ctx: &SweepContext<'_>,
    databases: bool,
    transcripts: bool,
) -> Result<()> {
    // A missing configured root is unavailable, not proof of deletion.
    let mut stmt = ctx.conn.prepare(
        "SELECT DISTINCT raw_path FROM sessions WHERE source=?1 AND raw_path IS NOT NULL",
    )?;
    let paths = stmt
        .query_map([ID], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut held = Held::new();
    for path in paths {
        let path = PathBuf::from(path);
        let proven = if path.file_name().is_some_and(|n| n == storage::DATABASE) {
            databases
        } else {
            transcripts
        };
        if proven && in_scope(&path, ctx.paths) && confirmed_missing(ctx.roots, &path)? {
            crate::ingest::check_capture_cancelled()?;
            let tx = ctx.conn.unchecked_transaction()?;
            if path.file_name().is_some_and(|n| n == storage::DATABASE) {
                retire_absent(&tx, &path, &[])?;
            } else {
                retire_transcript(&tx, ctx.roots, &path, &mut held)?;
            }
            tx.commit()?;
        }
    }
    Ok(())
}

/// Agent directory -> the native IDs its database holds, or None when that
/// database is unreadable.
type Held = std::collections::BTreeMap<PathBuf, Option<std::collections::BTreeSet<String>>>;

/// A JSONL transcript that is gone: its sessions retire unless the agent's
/// database holds them (the doctor imported, then archived, the file), in
/// which case they retire into the database's conversation once it holds
/// that window. While that
/// database cannot be read nothing retires: it may hold any of them. This
/// holds whether or not the database was swept, normalized cleanly, or its
/// `sessions/` directory still exists.
fn retire_transcript(
    conn: &Connection,
    roots: &ProviderRoots,
    path: &Path,
    held: &mut Held,
) -> Result<()> {
    let Some(agent) = path.parent().and_then(Path::parent) else {
        return Ok(());
    };
    let ids = held.entry(agent.to_path_buf()).or_insert_with(|| {
        stores::held(&AcquisitionContext::new(ID, roots), agent)
            .inspect_err(|error| eprintln!("OpenClaw {}: {error:#}", agent.display()))
            .ok()
    });
    let Some(ids) = ids else {
        return Ok(());
    };
    let prefix = format!("{}:", storage::namespace(roots, path)?);
    let mut stmt =
        conn.prepare("SELECT session_id FROM sessions WHERE source=?1 AND raw_path=?2")?;
    let recorded = stmt
        .query_map(rusqlite::params![ID, path.to_string_lossy()], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for id in recorded {
        crate::ingest::check_capture_cancelled()?;
        let native = id.strip_prefix(&prefix).unwrap_or_default();
        if ids.contains(native) {
            // The database's conversation owns it once that window is in the
            // ledger; until then the row keeps its evidence.
            legacy::supersede(conn, &id, native)?;
        } else {
            file_history::retire_local_session(conn, ID, &id)?;
        }
    }
    Ok(())
}

pub(super) fn in_scope(path: &Path, scope: Option<&crate::ingest::path_scope::PathScope>) -> bool {
    scope.is_none_or(|scope| {
        scope.paths().iter().any(|changed| {
            path.starts_with(changed)
                || ["", "-wal", "-shm", "-journal"]
                    .iter()
                    .any(|suffix| *changed == storage::sidecar(path, suffix))
        })
    })
}

// Absence is authoritative only inside an accessible configured root with the
// native layout. Missing roots and escaped symlinks never prove deletion.
pub(super) fn confirmed_missing(roots: &ProviderRoots, path: &Path) -> Result<bool> {
    let context = AcquisitionContext::new(ID, roots);
    for root in roots.for_provider(ID) {
        let Ok(relative) = path.strip_prefix(root) else {
            continue;
        };
        if !(native_database_path(relative) || legacy::native_path(relative))
            || !root.try_exists()?
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
            validate_existing_directory(&context, dir)?;
            parent = dir.parent();
        }
        return Ok(!path.try_exists()?);
    }
    Ok(false)
}
fn native_database_path(relative: &Path) -> bool {
    let parts: Vec<_> = relative.components().collect();
    parts.len() == 4
        && parts[0].as_os_str() == "agents"
        && matches!(parts[1], std::path::Component::Normal(_))
        && parts[2].as_os_str() == "agent"
        && parts[3].as_os_str() == storage::DATABASE
}
fn validate_existing_directory(context: &AcquisitionContext<'_>, dir: &Path) -> Result<()> {
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
