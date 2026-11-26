//! Which Cursor transcripts one sweep reads.
//!
//! A top-level transcript is `agent-transcripts/<id>/<id>.jsonl`; a subagent
//! it spawned is `agent-transcripts/<id>/subagents/<child>.jsonl`. The full
//! walk lists both, and a path-scoped tick jumps to the ones a changed path
//! names.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::ingest::{
    self,
    path_scope::{PathScope, RootWalk},
};

pub(crate) fn cursor_transcripts(
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
    for project_dir in ingest::sorted_dirs(root)? {
        ingest::check_capture_cancelled()?;
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
    let project_path = ingest::decode_cursor_project(
        project_dir
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default(),
    );
    for session_dir in ingest::sorted_dirs(&ts_root)? {
        ingest::check_capture_cancelled()?;
        files.extend(cursor_session_transcripts(&session_dir, &project_path)?);
    }
    Ok(files)
}

/// One session directory's transcript, then the subagent transcripts it holds.
fn cursor_session_transcripts(
    session_dir: &Path,
    project: &str,
) -> Result<Vec<(PathBuf, String, String)>> {
    let mut files = Vec::new();
    if let Some(session_id) = session_dir.file_name().and_then(|name| name.to_str()) {
        let jsonl = session_dir.join(format!("{session_id}.jsonl"));
        if jsonl.is_file() {
            files.push((jsonl, session_id.to_string(), project.to_string()));
        }
    }
    files.extend(cursor_subagent_transcripts(session_dir, project)?);
    Ok(files)
}

/// The subagent transcripts under `<session>/subagents/`, each its own session
/// named by its file stem. A session that spawned none has no such directory.
pub(crate) fn cursor_subagent_transcripts(
    session_dir: &Path,
    project: &str,
) -> Result<Vec<(PathBuf, String, String)>> {
    let dir = session_dir.join(ingest::cursor::SUBAGENTS_DIR);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) if error.kind() == io::ErrorKind::NotADirectory => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("list {}", dir.display()));
        }
    };
    let mut files = Vec::new();
    for entry in entries {
        let path = entry?.path();
        let Some((_, child)) = ingest::cursor::subagent_identity(&path) else {
            continue;
        };
        let child = child.to_string();
        if path.is_file() {
            files.push((path, child, project.to_string()));
        }
    }
    files.sort();
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
        ingest::check_capture_cancelled()?;
        if !changed.starts_with(root) {
            continue;
        }
        let named = match cursor_changed_target(root, changed) {
            Some(CursorTarget::File(jsonl, session_id, project)) => {
                if jsonl.is_file() {
                    vec![(jsonl, session_id, project)]
                } else {
                    Vec::new()
                }
            }
            Some(CursorTarget::Session(session_dir, project)) => {
                cursor_session_transcripts(&session_dir, &project)?
            }
            Some(CursorTarget::Subagents(session_dir, project)) => {
                cursor_subagent_transcripts(&session_dir, &project)?
            }
            Some(CursorTarget::Project(project_dir)) => cursor_project_transcripts(&project_dir)?,
            // A path under the root that is not a transcript shape. Listing
            // the root is the walk that cannot miss it.
            None => {
                missed = true;
                Vec::new()
            }
        };
        for file in named {
            if seen.insert(file.0.clone()) {
                files.push(file);
            }
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
    /// A session directory: its transcript and its subagent transcripts.
    Session(PathBuf, String),
    /// A session's `subagents/` directory, or a path in it that is not a
    /// transcript.
    Subagents(PathBuf, String),
    Project(PathBuf),
}

fn cursor_changed_target(root: &Path, changed: &Path) -> Option<CursorTarget> {
    let rel = changed.strip_prefix(root).ok()?;
    let parts: Vec<_> = rel.components().collect();
    if parts.is_empty() {
        return None;
    }
    let project_dir = root.join(parts[0]);
    let project = ingest::decode_cursor_project(parts[0].as_os_str().to_str()?);
    if parts.len() == 1 || (parts.len() == 2 && parts[1].as_os_str() == "agent-transcripts") {
        return Some(CursorTarget::Project(project_dir));
    }
    if parts.len() >= 3 && parts[1].as_os_str() == "agent-transcripts" {
        let session_id = parts[2].as_os_str().to_str()?.to_string();
        let session_dir = project_dir.join("agent-transcripts").join(&session_id);
        // A changed session directory may be a new subagent landing in it, so
        // its subagent transcripts are listed along with its own.
        if parts.len() == 3 {
            return Some(CursorTarget::Session(session_dir, project));
        }
        if parts[3].as_os_str() == ingest::cursor::SUBAGENTS_DIR {
            return Some(cursor_subagent_target(changed, session_dir, project));
        }
        let jsonl = session_dir.join(format!("{session_id}.jsonl"));
        // A path that is not this session's transcript and is not inside its
        // directory is not evidence. Inside the directory, the transcript is.
        if changed == jsonl || changed.starts_with(&session_dir) {
            return Some(CursorTarget::File(jsonl, session_id, project));
        }
    }
    None
}

/// A changed path inside `<session>/subagents/`: the one subagent transcript
/// it names, or the directory's listing when it names none.
fn cursor_subagent_target(changed: &Path, session_dir: PathBuf, project: String) -> CursorTarget {
    match ingest::cursor::subagent_identity(changed) {
        Some((_, child)) => CursorTarget::File(changed.to_path_buf(), child.to_string(), project),
        None => CursorTarget::Subagents(session_dir, project),
    }
}

#[cfg(test)]
mod cursor_subagent_tests {
    use super::*;

    fn touch(path: &Path) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, b"{}\n").unwrap();
    }

    fn ids(files: &[(PathBuf, String, String)]) -> Vec<&str> {
        files.iter().map(|(_, id, _)| id.as_str()).collect()
    }

    /// Cursor writes a subagent's transcript at
    /// `agent-transcripts/<parent>/subagents/<child>.jsonl`. The walk matched
    /// only `<id>/<id>.jsonl`, so every subagent was skipped.
    ///
    /// Positive control: before the fix the full walk listed `["parent"]`.
    #[test]
    fn the_full_walk_lists_subagent_transcripts_as_their_own_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("projects");
        let session = root.join("home-dev-app/agent-transcripts/parent");
        touch(&session.join("parent.jsonl"));
        touch(&session.join("subagents/child-b.jsonl"));
        touch(&session.join("subagents/child-a.jsonl"));
        touch(&session.join("subagents/notes.txt"));
        touch(&session.join("subagents/nested/deeper.jsonl"));

        let files = cursor_transcripts(&root, None).unwrap();
        assert_eq!(ids(&files), vec!["parent", "child-a", "child-b"]);
        assert_eq!(files[1].0, session.join("subagents/child-a.jsonl"));
        assert!(files
            .iter()
            .all(|(_, _, project)| project == "/home/dev/app"));
    }

    /// A change-triggered tick reaches a subagent transcript from any of the
    /// paths an fs event can name for it, and does not list its siblings when
    /// it names one file.
    #[test]
    fn a_scoped_tick_reaches_subagent_transcripts() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("projects");
        let session = root.join("app/agent-transcripts/parent");
        let child = session.join("subagents/child-a.jsonl");
        touch(&session.join("parent.jsonl"));
        touch(&child);
        touch(&session.join("subagents/child-b.jsonl"));
        touch(&root.join("app/agent-transcripts/other/other.jsonl"));

        let listed = |paths: Vec<PathBuf>| {
            let scope = PathScope::from_paths(paths).unwrap();
            cursor_transcripts(&root, Some(&scope)).unwrap()
        };
        let one = listed(vec![child.clone()]);
        assert_eq!(ids(&one), vec!["child-a"]);
        assert_eq!(one[0].0, child);
        assert_eq!(
            ids(&listed(vec![session.join("subagents")])),
            vec!["child-a", "child-b"]
        );
        assert_eq!(
            ids(&listed(vec![session.clone()])),
            vec!["parent", "child-a", "child-b"]
        );
        // The parent's own transcript still names only the parent.
        assert_eq!(
            ids(&listed(vec![session.join("parent.jsonl")])),
            vec!["parent"]
        );
    }

    #[test]
    fn only_the_exact_subagent_layout_is_a_subagent() {
        fn identity(path: &str) -> Option<(&str, &str)> {
            ingest::cursor::subagent_identity(Path::new(path))
        }
        assert_eq!(
            identity("/h/.cursor/projects/p/agent-transcripts/parent/subagents/child.jsonl"),
            Some(("parent", "child"))
        );
        assert_eq!(
            identity("/h/.cursor/projects/p/agent-transcripts/parent/parent.jsonl"),
            None
        );
        assert_eq!(
            identity("/h/.cursor/projects/p/agent-transcripts/parent/subagents/child.txt"),
            None
        );
        assert_eq!(
            identity("/h/.cursor/projects/p/agent-transcripts/parent/subagents/x/child.jsonl"),
            None
        );
    }
}
