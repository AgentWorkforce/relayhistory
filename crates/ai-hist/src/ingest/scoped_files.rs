//! Which files one sweep reads.
//!
//! A full sweep lists a root the way it always has. A path-scoped tick jumps
//! to the paths that changed. Both go through here so the sync functions stay
//! the walks they were.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering as AtomicOrdering;

use super::SYNC_QUIET;

use anyhow::{Context, Result};
use rusqlite::Connection;

use super::path_scope::{self, PathScope, RootWalk};

/// Every Claude transcript under `root` that this scope names: each `*.jsonl`
/// except the subagent workflow journals, which are metadata and never a
/// transcript. No scope lists the whole root.
///
/// Discovery and the sync walk both enumerate through this, so neither can
/// list a journal the other has excluded.
pub(crate) fn claude_transcript_files_scoped(
    root: &Path,
    scope: Option<&PathScope>,
) -> Result<Vec<PathBuf>> {
    let mut files = match PathScope::walk_of(scope, root) {
        RootWalk::Skip => return Ok(Vec::new()),
        RootWalk::Full => collect_matching_files(root, "", "jsonl")?,
        RootWalk::Partial(scope) => collect_matching_files_scoped(root, "", "jsonl", Some(scope))?,
    };
    #[cfg(test)]
    if super::LEGACY_WALKS_WORKFLOW_JOURNALS.with(std::cell::Cell::get) {
        return Ok(files);
    }
    files.retain(|path| !super::is_claude_workflow_journal(root, path));
    Ok(files)
}

pub(crate) fn collect_matching_files(root: &Path, prefix: &str, ext: &str) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    collect_matching_files_inner(root, prefix, ext, &mut out)?;
    out.sort();
    Ok(out)
}

/// `collect_matching_files` restricted to `scope` when the scope is strictly
/// inside `root`. Anything else, including no scope, is the full walk.
pub(crate) fn collect_matching_files_scoped(
    root: &Path,
    prefix: &str,
    ext: &str,
    scope: Option<&PathScope>,
) -> Result<Vec<PathBuf>> {
    let Some(scope) = scope.filter(|scope| scope.partial_under(root)) else {
        return collect_matching_files(root, prefix, ext);
    };
    let mut out = Vec::new();
    if !collect_scoped(root, prefix, ext, scope, &mut out)? {
        return collect_matching_files(root, prefix, ext);
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// `false` when the scope names nothing the walk can jump to, so the caller
/// lists the whole root instead of returning an empty tree for a change it
/// could not place.
fn collect_scoped(
    dir: &Path,
    prefix: &str,
    ext: &str,
    scope: &PathScope,
    out: &mut Vec<PathBuf>,
) -> Result<bool> {
    if scope.needs_sibling_scan(dir, prefix, ext) {
        list_scoped(dir, prefix, ext, scope, out)?;
        return Ok(true);
    }
    let children = scope.children_under(dir);
    if children.is_empty() {
        return Ok(false);
    }
    for child in children {
        super::check_capture_cancelled()?;
        let meta = match fs::symlink_metadata(&child) {
            Ok(meta) => meta,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("stat {}", child.display()));
            }
        };
        let is_dir = if meta.file_type().is_symlink() {
            child.is_dir()
        } else {
            meta.is_dir()
        };
        visit_scoped(&child, is_dir, prefix, ext, scope, out)?;
    }
    Ok(true)
}

fn visit_scoped(
    path: &Path,
    is_dir: bool,
    prefix: &str,
    ext: &str,
    scope: &PathScope,
    out: &mut Vec<PathBuf>,
) -> Result<()> {
    if is_dir {
        if !scope.enter_dir(path) {
            return Ok(());
        }
        push_stem_transcript(path, prefix, ext, scope, out);
        if scope.partial_under(path) {
            if !collect_scoped(path, prefix, ext, scope, out)? {
                collect_matching_files_inner(path, prefix, ext, out)?;
            }
        } else {
            collect_matching_files_inner(path, prefix, ext, out)?;
        }
        return Ok(());
    }
    if path_scope::is_collected(path, prefix, ext) && scope.include_file(path, prefix, ext) {
        out.push(path.to_path_buf());
    }
    Ok(())
}

/// The `<stem>.<ext>` beside a session directory, when this scope names
/// something under that directory. One stat, and only of a name that would
/// be collected.
fn push_stem_transcript(
    dir: &Path,
    prefix: &str,
    ext: &str,
    scope: &PathScope,
    out: &mut Vec<PathBuf>,
) {
    let Some(name) = dir.file_name() else {
        return;
    };
    let mut file_name = name.to_os_string();
    file_name.push(".");
    file_name.push(ext);
    let candidate = dir.with_file_name(file_name);
    if path_scope::is_collected(&candidate, prefix, ext)
        && scope.include_file(&candidate, prefix, ext)
        && candidate.is_file()
    {
        out.push(candidate);
    }
}

fn list_scoped(
    dir: &Path,
    prefix: &str,
    ext: &str,
    scope: &PathScope,
    out: &mut Vec<PathBuf>,
) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    note_dir_read();
    for entry in fs::read_dir(dir)? {
        super::check_capture_cancelled()?;
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        let is_dir = if file_type.is_symlink() {
            path.is_dir()
        } else {
            file_type.is_dir()
        };
        visit_scoped(&path, is_dir, prefix, ext, scope, out)?;
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    static DIRS_READ: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[inline]
fn note_dir_read() {
    #[cfg(test)]
    DIRS_READ.with(|cell| cell.set(cell.get() + 1));
}

#[cfg(test)]
pub(crate) fn take_dirs_read() -> u64 {
    DIRS_READ.with(|cell| cell.replace(0))
}

fn collect_matching_files_inner(
    root: &Path,
    prefix: &str,
    ext: &str,
    out: &mut Vec<PathBuf>,
) -> Result<()> {
    if !root.is_dir() {
        return Ok(());
    }
    note_dir_read();
    for entry in fs::read_dir(root)? {
        super::check_capture_cancelled()?;
        let entry = entry?;
        // The directory read already knows each entry's type; only a symlink
        // needs a stat to see what it points at.
        let file_type = entry.file_type()?;
        let path = entry.path();
        let is_dir = if file_type.is_symlink() {
            path.is_dir()
        } else {
            file_type.is_dir()
        };
        if is_dir {
            collect_matching_files_inner(&path, prefix, ext, out)?;
        } else if path
            .file_name()
            .and_then(|s| s.to_str())
            .is_some_and(|name| name.starts_with(prefix))
            && path.extension().and_then(|s| s.to_str()) == Some(ext)
        {
            out.push(path);
        }
    }
    Ok(())
}

pub(crate) fn cursor_transcript_paths(
    root: &Path,
    scope: Option<&PathScope>,
) -> Result<Vec<PathBuf>> {
    Ok(cursor_transcripts(root, scope)?
        .into_iter()
        .map(|(path, _, _)| path)
        .collect())
}

pub(super) fn cursor_transcripts(
    root: &Path,
    scope: Option<&PathScope>,
) -> Result<Vec<(PathBuf, String, String)>> {
    match PathScope::walk_of(scope, root) {
        RootWalk::Skip => Ok(Vec::new()),
        RootWalk::Full => cursor_transcripts_full(root),
        RootWalk::Partial(scope) => cursor_transcripts_partial(root, scope),
    }
}

fn cursor_transcripts_full(root: &Path) -> Result<Vec<(PathBuf, String, String)>> {
    let mut files = Vec::new();
    for project_dir in super::sorted_dirs(root)? {
        super::check_capture_cancelled()?;
        files.extend(cursor_project_transcripts(&project_dir)?);
    }
    Ok(files)
}

fn cursor_project_transcripts(project_dir: &Path) -> Result<Vec<(PathBuf, String, String)>> {
    let mut files = Vec::new();
    let ts_root = project_dir.join("agent-transcripts");
    if !ts_root.is_dir() {
        return Ok(files);
    }
    let project_path = super::decode_cursor_project(
        project_dir
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default(),
    );
    for session_dir in super::sorted_dirs(&ts_root)? {
        super::check_capture_cancelled()?;
        let session_id = session_dir
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
        let jsonl = session_dir.join(format!("{session_id}.jsonl"));
        if jsonl.is_file() {
            files.push((jsonl, session_id, project_path.clone()));
        }
    }
    Ok(files)
}

/// The session file a changed path names, without listing its siblings.
///
/// The shape is checked before the file is stat'd, so a tick that names one
/// transcript does not stat the other sessions in the project.
fn cursor_transcripts_partial(
    root: &Path,
    scope: &PathScope,
) -> Result<Vec<(PathBuf, String, String)>> {
    let mut files = Vec::new();
    let mut seen = HashSet::new();
    let mut missed = false;
    for changed in scope.paths() {
        super::check_capture_cancelled()?;
        if !changed.starts_with(root) {
            continue;
        }
        match cursor_changed_target(root, changed) {
            Some(CursorTarget::File(jsonl, session_id, project)) => {
                if seen.insert(jsonl.clone()) && jsonl.is_file() {
                    files.push((jsonl, session_id, project));
                }
            }
            Some(CursorTarget::Project(project_dir)) => {
                for (jsonl, session_id, project) in cursor_project_transcripts(&project_dir)? {
                    if seen.insert(jsonl.clone()) {
                        files.push((jsonl, session_id, project));
                    }
                }
            }
            // A path under the root that is not a transcript shape. Listing
            // the root is the walk that cannot miss it.
            None => missed = true,
        }
    }
    if missed {
        return cursor_transcripts_full(root);
    }
    files.sort();
    Ok(files)
}

enum CursorTarget {
    File(PathBuf, String, String),
    Project(PathBuf),
}

fn cursor_changed_target(root: &Path, changed: &Path) -> Option<CursorTarget> {
    let rel = changed.strip_prefix(root).ok()?;
    let parts: Vec<_> = rel.components().collect();
    if parts.is_empty() {
        return None;
    }
    let project_dir = root.join(parts[0]);
    let project = super::decode_cursor_project(parts[0].as_os_str().to_str()?);
    if parts.len() == 1 || (parts.len() == 2 && parts[1].as_os_str() == "agent-transcripts") {
        return Some(CursorTarget::Project(project_dir));
    }
    if parts.len() >= 3 && parts[1].as_os_str() == "agent-transcripts" {
        let session_id = parts[2].as_os_str().to_str()?.to_string();
        let jsonl = project_dir
            .join("agent-transcripts")
            .join(&session_id)
            .join(format!("{session_id}.jsonl"));
        // A path that is not this session's transcript and is not inside its
        // directory is not evidence. Inside the directory, the transcript is.
        if parts.len() == 3 || changed == jsonl || changed.starts_with(jsonl.parent()?) {
            return Some(CursorTarget::File(jsonl, session_id, project));
        }
    }
    None
}

pub(crate) fn collect_muse_transcripts(root: &Path) -> Result<Vec<PathBuf>> {
    collect_muse_transcripts_scoped(root, None)
}

pub(crate) fn collect_muse_transcripts_scoped(
    root: &Path,
    scope: Option<&PathScope>,
) -> Result<Vec<PathBuf>> {
    let prefix = super::muse::SESSION_FILE.trim_end_matches(".jsonl");
    let mut files = match PathScope::walk_of(scope, root) {
        RootWalk::Skip => return Ok(Vec::new()),
        RootWalk::Full => collect_matching_files(root, prefix, "jsonl")?,
        RootWalk::Partial(scope) => {
            let mut files = collect_matching_files_scoped(root, prefix, "jsonl", Some(scope))?;
            // A child log is not the session. The session file above it is.
            files.extend(scope.ancestor_files(root, super::muse::SESSION_FILE));
            files.sort();
            files.dedup();
            files
        }
    };
    files.retain(|path| {
        path.file_name().and_then(|name| name.to_str()) == Some(super::muse::SESSION_FILE)
            && !super::muse::is_child_transcript(path, root)
    });
    Ok(files)
}

/// Grok `chat_history.jsonl` files this tick reads.
///
/// A full listing is the sweep's cached inventory. A path-scoped listing
/// never reads or writes that cache: storing the subset would make the next
/// pass believe the tree was that small.
pub(crate) fn grok_transcripts_for(root: &Path, scope: Option<&PathScope>) -> Result<Vec<PathBuf>> {
    match PathScope::walk_of(scope, root) {
        RootWalk::Skip => Ok(Vec::new()),
        RootWalk::Full => crate::discover::sweep_inventory::grok_transcripts(root),
        RootWalk::Partial(scope) => {
            let mut files =
                collect_matching_files_scoped(root, "chat_history", "jsonl", Some(scope))?;
            files.extend(scope.ancestor_files(root, "chat_history.jsonl"));
            files.sort();
            files.dedup();
            Ok(files)
        }
    }
}

/// Rollouts under one Codex root, and whether that root was listed whole.
///
/// Skip yields nothing and reports the root incomplete: the caller must not
/// forget stamps it did not visit.
pub(super) fn codex_rollouts_for_root(
    root: &Path,
    paths: Option<&PathScope>,
) -> Result<(Vec<PathBuf>, bool)> {
    match PathScope::walk_of(paths, root) {
        RootWalk::Skip => Ok((Vec::new(), false)),
        RootWalk::Partial(scope) => Ok((
            collect_matching_files_scoped(root, "rollout-", "jsonl", Some(scope))?,
            false,
        )),
        RootWalk::Full => Ok((collect_matching_files(root, "rollout-", "jsonl")?, true)),
    }
}

/// Whether this root may count as fully seen for a backfill generation.
///
/// A partial listing never does. A skipped root does only when the stamp map
/// names nothing under it, which is the same answer a missing root gives.
pub(super) fn codex_root_complete(
    paths: Option<&PathScope>,
    root: &Path,
    known_here: bool,
) -> bool {
    match PathScope::walk_of(paths, root) {
        RootWalk::Full => true,
        RootWalk::Partial(_) => false,
        RootWalk::Skip => !known_here,
    }
}

/// Stamps under `root` this listing did not see. A skip forgets nothing.
pub(super) fn codex_unobserved(
    seen: &serde_json::Map<String, serde_json::Value>,
    root: &Path,
    rollouts: &[PathBuf],
    paths: Option<&PathScope>,
) -> Vec<String> {
    match PathScope::walk_of(paths, root) {
        RootWalk::Skip => Vec::new(),
        RootWalk::Partial(scope) => {
            let mut missing = super::unobserved_known_paths(seen, root, rollouts);
            missing.retain(|key| scope.include_file(Path::new(key), "rollout-", "jsonl"));
            missing
        }
        RootWalk::Full => super::unobserved_known_paths(seen, root, rollouts),
    }
}

/// The sessions a sweep will repair, and the destination snapshot the seal
/// compares against. A path-scoped tick repairs nothing and takes no
/// snapshot: naming every short session walks the catalog, and that tick
/// does not seal. Those sessions wait for the backstop.
pub(super) fn sweep_repairs(
    conn: &Connection,
    state: &serde_json::Map<String, serde_json::Value>,
    scope: &super::SweepScope,
) -> (
    super::SweepRepairs,
    bool,
    Option<super::DestinationSnapshot>,
) {
    if scope.paths().is_some() {
        return (super::SweepRepairs::default(), true, None);
    }
    match super::destination_shortfall_counted(conn, state, None) {
        Ok((repairs, counted)) => (repairs, true, counted),
        Err(error) => {
            sync_note!("  [sync] could not name the sessions to repair: {error:#}");
            (super::SweepRepairs::default(), false, None)
        }
    }
}

/// Claude cursors this tick may drop. A skip returns nothing and does not
/// query: the history log is not a reason to read the cursor table.
pub(super) fn claude_missing_cursors(
    conn: &Connection,
    paths: Option<&PathScope>,
    root: &Path,
    observed: &HashSet<String>,
) -> Result<Vec<String>> {
    let scope = match PathScope::walk_of(paths, root) {
        RootWalk::Skip => return Ok(Vec::new()),
        RootWalk::Partial(scope) => Some(scope),
        RootWalk::Full => None,
    };
    Ok(super::transcript_cursor::known_locators(conn, "claude")?
        .into_iter()
        .filter(|locator| super::path_key_is_under(locator, root) && !observed.contains(locator))
        .filter(|locator| {
            scope.is_none_or(|scope| scope.include_file(Path::new(locator), "", "jsonl"))
        })
        .collect())
}
