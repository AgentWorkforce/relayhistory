//! `export`: history rows to a JSONL stream or a standalone SQLite file,
//! staged beside the destination and never written over the active database.

use super::*;

/// Where `export --format sqlite` writes when no destination is given.
pub(crate) const DEFAULT_SQLITE_EXPORT: &str = "ai-hist-export.db";

/// Exports history rows to `output` (stdout for JSONL when absent).
///
/// `active_db` is the database this invocation actually opened, `--db`
/// included. A destination that names it, or one of its SQLite sidecars, is
/// refused for every format before anything is read or written. The export is
/// staged in a sibling temporary file and renamed over the destination only
/// once it is complete, so a failed export leaves an existing file untouched.
pub(crate) fn export_history(
    conn: &Connection,
    active_db: &Path,
    output: Option<&Path>,
    format: &str,
    source: Option<&str>,
    project: Option<&str>,
    since: Option<&str>,
) -> Result<()> {
    anyhow::ensure!(
        matches!(format, "sqlite" | "jsonl"),
        "unsupported export format '{format}'"
    );
    let dest = match (format, output) {
        ("sqlite", None) => Some(Path::new(DEFAULT_SQLITE_EXPORT)),
        (_, output) => output,
    };
    // An existing symlink destination is written through, to its target, as
    // a direct write would; the rename below would otherwise replace the link.
    let target = dest.map(follow_destination_symlink);
    if let (Some(dest), Some(target)) = (dest, target.as_deref()) {
        ensure_export_spares_active_database(conn, active_db, dest, target)?;
    }
    let rows = export_rows(conn, source, project, since)?;
    if rows.is_empty() {
        anyhow::bail!("No entries matched the export filters.");
    }
    if format == "sqlite" {
        let dest = dest.expect("a sqlite export always has a destination");
        let target = target.as_deref().expect("resolved alongside dest");
        let inserted = write_sqlite_export(&rows, target)?;
        println!("Exported {inserted} entries to {}", dest.display());
        return Ok(());
    }
    let mut body = Vec::new();
    for entry in &rows {
        let row = json!({
            "source": entry.source,
            "session_id": entry.session_id,
            "project": entry.project,
            "prompt": entry.prompt,
            "prompt_hash": entry.prompt_hash.clone().unwrap_or_else(|| prompt_hash(&entry.prompt)),
            "timestamp_ms": entry.timestamp_ms,
        });
        writeln!(&mut body, "{}", serde_json::to_string(&row)?)?;
    }
    if let (Some(path), Some(target)) = (dest, target.as_deref()) {
        let mut staged = staged_export_file(target)?;
        if path.extension().and_then(|s| s.to_str()) == Some("gz") {
            let mut enc = GzEncoder::new(staged.as_file_mut(), Compression::default());
            enc.write_all(&body)?;
            enc.finish()?;
        } else {
            staged.write_all(&body)?;
        }
        staged.as_file().sync_all()?;
        persist_export(staged, target)?;
        eprintln!("Exported {} entries to {}", rows.len(), path.display());
    } else {
        io::stdout().write_all(&body)?;
    }
    Ok(())
}

/// Refuse an export whose destination, or the file it resolves to, is the
/// database being read.
fn ensure_export_spares_active_database(
    conn: &Connection,
    active_db: &Path,
    dest: &Path,
    target: &Path,
) -> Result<()> {
    // `--db` is handed to SQLite as given, so a `file:` URI names a database
    // whose filesystem path is not the argument's text. The connection knows
    // the file it actually opened; both are guarded.
    let opened = conn
        .path()
        .filter(|path| !path.is_empty())
        .map(PathBuf::from);
    let protected: Vec<&Path> = std::iter::once(active_db)
        .chain(opened.as_deref())
        .collect();
    anyhow::ensure!(
        !protected.iter().any(|active| {
            names_active_database(dest, active) || names_active_database(target, active)
        }),
        "Refusing to export over the active database {} (destination {}).",
        active_db.display(),
        dest.display()
    );
    Ok(())
}

/// Write `rows` to a fresh SQLite database installed at `target`, returning
/// how many were inserted.
fn write_sqlite_export(rows: &[HistoryEntry], target: &Path) -> Result<usize> {
    let staged = staged_export_file(target)?;
    let inserted = {
        let dst = Connection::open(staged.path())?;
        ai_hist::init_db(&dst)?;
        let mut inserted = 0;
        for entry in rows {
            inserted += insert_history(&dst, entry)?;
        }
        // A self-contained single file: nothing left in a -wal sidecar
        // that the rename below would not carry along.
        dst.query_row("PRAGMA journal_mode=DELETE", [], |_| Ok(()))?;
        dst.close().map_err(|(_, error)| error)?;
        inserted
    };
    install_sqlite_export(staged, target, persist_export)?;
    Ok(inserted)
}

/// A temporary file beside `dest`, so the final rename stays on one filesystem.
fn staged_export_file(dest: &Path) -> Result<tempfile::NamedTempFile> {
    let dir = match dest.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let name = dest
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "ai-hist-export".into());
    tempfile::Builder::new()
        .prefix(&format!(".{name}."))
        .suffix(".partial")
        .tempfile_in(dir)
        .with_context(|| format!("create a temporary export file in {}", dir.display()))
}

pub(crate) fn persist_export(staged: tempfile::NamedTempFile, dest: &Path) -> Result<()> {
    staged
        .persist(dest)
        .map_err(|error| error.error)
        .with_context(|| format!("move the finished export into {}", dest.display()))?;
    Ok(())
}

/// `dest` itself, or the file an existing symlink at `dest` points to.
fn follow_destination_symlink(dest: &Path) -> PathBuf {
    match fs::symlink_metadata(dest) {
        Ok(meta) if meta.file_type().is_symlink() => {
            fs::canonicalize(dest).unwrap_or_else(|_| match fs::read_link(dest) {
                // A dangling link: create its target, as a direct write would.
                Ok(link) if link.is_absolute() => link,
                Ok(link) => dest.parent().unwrap_or_else(|| Path::new("")).join(link),
                Err(_) => dest.to_path_buf(),
            })
        }
        _ => dest.to_path_buf(),
    }
}

const SQLITE_SIDECARS: [&str; 3] = ["-wal", "-shm", "-journal"];

/// Renames the staged SQLite export over `dest` without letting a previous
/// database's sidecars pair with the new file, and without losing them if the
/// replacement fails.
///
/// Existing sidecars are first parked under a private name (any failure other
/// than "not found" aborts the export with everything restored), then
/// `persist` installs the new file. On success the parked sidecars are
/// discarded; on failure they are renamed back beside the untouched old file.
pub(crate) fn install_sqlite_export(
    staged: tempfile::NamedTempFile,
    dest: &Path,
    persist: impl FnOnce(tempfile::NamedTempFile, &Path) -> Result<()>,
) -> Result<()> {
    let mut parked: Vec<(PathBuf, PathBuf)> = Vec::new();
    for suffix in SQLITE_SIDECARS {
        let sidecar = sqlite_sidecar(dest, suffix);
        let aside = parked_sidecar(dest, suffix);
        match fs::rename(&sidecar, &aside) {
            Ok(()) => parked.push((sidecar, aside)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                restore_parked_sidecars(&parked);
                return Err(error).with_context(|| {
                    format!(
                        "move the existing SQLite sidecar {} out of the way",
                        sidecar.display()
                    )
                });
            }
        }
    }
    match persist(staged, dest) {
        Ok(()) => {
            // Renamed away from the database name, SQLite can no longer pair
            // these with anything, so a failed removal is harmless.
            for (_, aside) in &parked {
                let _ = fs::remove_file(aside);
            }
            Ok(())
        }
        Err(error) => {
            restore_parked_sidecars(&parked);
            Err(error)
        }
    }
}

pub(crate) fn parked_sidecar(dest: &Path, suffix: &str) -> PathBuf {
    sqlite_sidecar(
        dest,
        &format!("{suffix}.ai-hist-{}.replaced", std::process::id()),
    )
}

fn restore_parked_sidecars(parked: &[(PathBuf, PathBuf)]) {
    for (sidecar, aside) in parked {
        if let Err(error) = fs::rename(aside, sidecar) {
            eprintln!(
                "warning: could not restore {} from {}: {error}",
                sidecar.display(),
                aside.display()
            );
        }
    }
}

pub(crate) fn sqlite_sidecar(db: &Path, suffix: &str) -> PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// Whether writing `candidate` would overwrite the database at `active` or
/// one of its SQLite sidecars. Paths are compared after resolving `.`/`..`,
/// symlinks and relative spellings; on Unix, two existing paths that are hard
/// links to one file also match.
fn names_active_database(candidate: &Path, active: &Path) -> bool {
    let candidate_resolved = resolve_export_path(candidate);
    ["", "-wal", "-shm", "-journal"].iter().any(|suffix| {
        let protected = sqlite_sidecar(active, suffix);
        candidate_resolved == resolve_export_path(&protected) || same_file(candidate, &protected)
    })
}

/// The canonical form of `path`, or, when it does not exist yet, its
/// canonical parent joined with its file name.
fn resolve_export_path(path: &Path) -> PathBuf {
    if let Ok(resolved) = fs::canonicalize(path) {
        return resolved;
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    match (absolute.parent(), absolute.file_name()) {
        (Some(parent), Some(name)) => fs::canonicalize(parent)
            .map(|parent| parent.join(name))
            .unwrap_or(absolute),
        _ => absolute,
    }
}

#[cfg(unix)]
fn same_file(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (fs::metadata(a), fs::metadata(b)) {
        (Ok(a), Ok(b)) => a.dev() == b.dev() && a.ino() == b.ino(),
        _ => false,
    }
}

#[cfg(not(unix))]
fn same_file(_a: &Path, _b: &Path) -> bool {
    false
}

fn export_rows(
    conn: &Connection,
    source: Option<&str>,
    project: Option<&str>,
    since: Option<&str>,
) -> Result<Vec<HistoryEntry>> {
    let mut sql =
        "SELECT id, source, session_id, project, prompt, timestamp_ms FROM history WHERE 1=1"
            .to_string();
    let mut params_vec = Vec::new();
    if let Some(source) = source {
        sql.push_str(" AND source = ?");
        params_vec.push(source.to_string());
    }
    if let Some(project) = project {
        sql.push_str(" AND project LIKE ?");
        params_vec.push(format!("%{project}%"));
    }
    if let Some(since) = since {
        sql.push_str(" AND timestamp_ms >= ?");
        params_vec.push(parse_date_ms(since)?.to_string());
    }
    sql.push_str(" ORDER BY timestamp_ms ASC");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params_vec), row_to_entry)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}
