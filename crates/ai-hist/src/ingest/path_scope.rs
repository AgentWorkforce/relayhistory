//! The files a watch tick actually has to read.
//!
//! A forced tick already names the provider roots that fired. The paths
//! under those roots are what the filesystem reported, and a tick that
//! carries them reads those transcripts instead of the provider's whole
//! tree. An empty scope is not a path scope: the caller walks the provider
//! the way a full sweep does.
//!
//! A path the tick cannot place never becomes a scope. Skipping a file the
//! tick cannot prove it may skip is how an append disappears until the
//! backstop.

use std::path::{Path, PathBuf};

/// Paths a tick holds, spelled the way the sweep walks them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PathScope {
    paths: Vec<PathBuf>,
}

/// How one listing root relates to a [`PathScope`].
pub(crate) enum RootWalk<'a> {
    /// No path scope, or the scope covers this root: list it all.
    Full,
    /// Some changed path is strictly inside this root.
    Partial(&'a PathScope),
    /// The scope names paths, and none of them are this root. Nothing here
    /// changed.
    Skip,
}

impl PathScope {
    /// `None` when `paths` is empty: the caller walks whole providers.
    pub(crate) fn from_paths(paths: impl IntoIterator<Item = PathBuf>) -> Option<Self> {
        let mut paths: Vec<PathBuf> = paths.into_iter().collect();
        if paths.is_empty() {
            return None;
        }
        paths.sort();
        paths.dedup();
        Some(Self { paths })
    }

    pub(crate) fn paths(&self) -> &[PathBuf] {
        &self.paths
    }

    /// Whether `root` is this scope, contains a changed path, or sits inside one.
    pub(crate) fn mentions(&self, root: &Path) -> bool {
        self.paths
            .iter()
            .any(|path| path.starts_with(root) || root.starts_with(path))
    }

    /// Whether some changed path is `root` or an ancestor of it, so a walk of
    /// `root` sees everything under it.
    pub(crate) fn covers_root(&self, root: &Path) -> bool {
        self.paths.iter().any(|path| root.starts_with(path))
    }

    /// Whether some changed path is strictly inside `root`.
    pub(crate) fn narrows(&self, root: &Path) -> bool {
        self.paths
            .iter()
            .any(|path| path != root && path.starts_with(root))
    }

    /// A walk of `root` would both miss files outside the scope and be
    /// incomplete inside it. Generations that mean "every file was seen"
    /// stay unrecorded.
    pub(crate) fn partial_under(&self, root: &Path) -> bool {
        self.narrows(root) && !self.covers_root(root)
    }

    pub(crate) fn walk_of<'a>(scope: Option<&'a Self>, root: &Path) -> RootWalk<'a> {
        let Some(scope) = scope else {
            return RootWalk::Full;
        };
        if !scope.mentions(root) {
            return RootWalk::Skip;
        }
        if scope.partial_under(root) {
            RootWalk::Partial(scope)
        } else {
            RootWalk::Full
        }
    }

    /// Whether a walk of `root` is the whole tree. A missing scope is.
    pub(crate) fn walk_is_full(scope: Option<&Self>, root: &Path) -> bool {
        matches!(Self::walk_of(scope, root), RootWalk::Full)
    }

    /// The next component under `dir` for every changed path inside it.
    pub(crate) fn children_under(&self, dir: &Path) -> Vec<PathBuf> {
        let mut children = Vec::new();
        for path in &self.paths {
            let Ok(rest) = path.strip_prefix(dir) else {
                continue;
            };
            let Some(next) = rest.components().next() else {
                continue;
            };
            let child = dir.join(next);
            if !children.contains(&child) {
                children.push(child);
            }
        }
        children
    }

    /// Whether a collected file belongs in this tick.
    ///
    /// A changed transcript is itself. A changed directory takes the files
    /// under it. A sidecar that is not itself a transcript (`*.meta.json`,
    /// `summary.json`) takes the transcripts in its own directory, and a
    /// Claude session directory takes the `<id>.jsonl` beside it. A sibling
    /// transcript does not: one append must not rescan the rest of the project.
    pub(crate) fn include_file(&self, file: &Path, prefix: &str, ext: &str) -> bool {
        let Some(parent) = file.parent() else {
            return false;
        };
        let stem = file.file_stem();
        self.paths.iter().any(|changed| {
            if file == changed || file.starts_with(changed) {
                return true;
            }
            if changed.parent() == Some(parent) && !is_collected(changed, prefix, ext) {
                return true;
            }
            stem.is_some_and(|stem| {
                changed
                    .strip_prefix(parent)
                    .ok()
                    .and_then(|rest| rest.components().next())
                    .is_some_and(|component| component.as_os_str() == stem)
            })
        })
    }

    /// Whether the walk descends into `dir`.
    pub(crate) fn enter_dir(&self, dir: &Path) -> bool {
        self.paths
            .iter()
            .any(|changed| changed.starts_with(dir) || dir.starts_with(changed))
    }

    /// Whether `dir` holds a changed file that is not a collected transcript,
    /// so the transcripts beside it have to be listed by name.
    ///
    /// A changed directory is not one of those files: the walk enters it
    /// instead of listing every sibling of it. The `is_dir` stat runs only
    /// for a path that is already not a transcript, so an append of a
    /// transcript does not stat anything here.
    pub(crate) fn needs_sibling_scan(&self, dir: &Path, prefix: &str, ext: &str) -> bool {
        self.paths.iter().any(|changed| {
            changed.parent() == Some(dir)
                && !is_collected(changed, prefix, ext)
                && !changed.is_dir()
        })
    }

    /// Transcripts named `file_name` that own a changed path underneath them.
    ///
    /// Grok's `chat_history.jsonl` and Muse's `session.jsonl` are the session;
    /// a write under `subagents/` or `subagent/` still has to re-read that
    /// file. A few `is_file` stats, and only when a scope exists. Claude does
    /// not use this: a nested change belongs to one session id, and listing
    /// every transcript in the project would rescan the siblings.
    pub(crate) fn ancestor_files(&self, root: &Path, file_name: &str) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for changed in &self.paths {
            if !changed.starts_with(root) {
                continue;
            }
            let mut dir = changed.parent();
            while let Some(current) = dir {
                if !current.starts_with(root) {
                    break;
                }
                let candidate = current.join(file_name);
                if candidate != *changed && candidate.is_file() {
                    out.push(candidate);
                }
                if current == root {
                    break;
                }
                dir = current.parent();
            }
        }
        out
    }
}

pub(crate) fn is_collected(path: &Path, prefix: &str, ext: &str) -> bool {
    path.extension().and_then(|ext| ext.to_str()) == Some(ext)
        && path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(prefix))
}

/// Whether `file` can have changed. `None` reads it: there is no path scope.
pub(crate) fn file_in_scope(scope: Option<&PathScope>, file: &Path) -> bool {
    scope.is_none_or(|scope| scope.mentions(file))
}

/// Whether a walk of these roots would leave any of them unread.
pub(crate) fn roots_are_partial(scope: Option<&PathScope>, roots: &[PathBuf]) -> bool {
    roots
        .iter()
        .any(|root| !matches!(PathScope::walk_of(scope, root), RootWalk::Full))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn scope(paths: &[&str]) -> PathScope {
        PathScope::from_paths(paths.iter().map(PathBuf::from)).unwrap()
    }

    #[test]
    fn an_append_is_that_transcript_and_not_its_siblings() {
        let scope = scope(&["/proj/p/one.jsonl"]);
        assert!(scope.include_file(Path::new("/proj/p/one.jsonl"), "", "jsonl"));
        assert!(!scope.include_file(Path::new("/proj/p/two.jsonl"), "", "jsonl"));
        assert!(scope.partial_under(Path::new("/proj")));
        assert!(!scope.covers_root(Path::new("/proj")));
    }

    #[test]
    fn a_sidecar_includes_the_transcripts_beside_it() {
        let scope = scope(&["/proj/p/agent-1.meta.json"]);
        assert!(scope.include_file(Path::new("/proj/p/one.jsonl"), "", "jsonl"));
        assert!(scope.needs_sibling_scan(Path::new("/proj/p"), "", "jsonl"));
    }

    #[test]
    fn a_transcript_append_does_not_ask_for_a_sibling_listing() {
        let scope = scope(&["/proj/p/one.jsonl"]);
        assert!(!scope.needs_sibling_scan(Path::new("/proj/p"), "", "jsonl"));
    }

    #[test]
    fn a_nested_claude_change_includes_its_session_file_only() {
        let scope = scope(&["/proj/p/one/subagents/agent.jsonl"]);
        assert!(scope.include_file(Path::new("/proj/p/one.jsonl"), "", "jsonl"));
        assert!(!scope.include_file(Path::new("/proj/p/two.jsonl"), "", "jsonl"));
        assert!(scope.enter_dir(Path::new("/proj/p/one")));
        assert!(!scope.enter_dir(Path::new("/proj/p/two")));
    }

    #[test]
    fn a_muse_child_does_not_match_its_parent_by_stem() {
        let scope = scope(&["/muse/id/subagent/child/session.jsonl"]);
        assert!(!scope.include_file(Path::new("/muse/id/session.jsonl"), "session", "jsonl"));
        assert!(scope.include_file(
            Path::new("/muse/id/subagent/child/session.jsonl"),
            "session",
            "jsonl"
        ));
    }

    #[test]
    fn a_root_the_scope_does_not_mention_is_skipped() {
        let scope = scope(&["/home/.claude/history.jsonl"]);
        assert!(matches!(
            PathScope::walk_of(Some(&scope), Path::new("/home/.claude/projects")),
            RootWalk::Skip
        ));
        assert!(matches!(
            PathScope::walk_of(None, Path::new("/home/.claude/projects")),
            RootWalk::Full
        ));
        assert!(file_in_scope(
            Some(&scope),
            Path::new("/home/.claude/history.jsonl")
        ));
        assert!(!file_in_scope(
            Some(&scope),
            Path::new("/home/.codex/history.jsonl")
        ));
    }

    #[test]
    fn an_ancestor_transcript_is_the_session_file_above_a_child() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("sessions");
        let session = root.join("id");
        std::fs::create_dir_all(session.join("subagents/child")).unwrap();
        let chat = session.join("chat_history.jsonl");
        std::fs::write(&chat, "{}\n").unwrap();
        std::fs::write(session.join("subagents/child/notes.jsonl"), "{}\n").unwrap();
        let scope = PathScope::from_paths([session.join("subagents/child/notes.jsonl")]).unwrap();
        assert_eq!(
            scope.ancestor_files(&root, "chat_history.jsonl"),
            vec![chat]
        );
    }

    #[test]
    fn a_scoped_walk_does_not_list_sibling_projects() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        for index in 0..200 {
            let dir = root.join(format!("p{index}"));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("s.jsonl"), "{}\n").unwrap();
        }
        let changed = root.join("p0/s.jsonl");
        let scope = PathScope::from_paths([changed.clone()]).unwrap();
        let _ = super::super::take_dirs_read();
        let scoped =
            super::super::collect_matching_files_scoped(&root, "", "jsonl", Some(&scope)).unwrap();
        let scoped_dirs = super::super::take_dirs_read();
        assert_eq!(scoped, vec![changed]);
        assert_eq!(scoped_dirs, 0, "an append stats the file; it does not list");

        let _ = super::super::collect_matching_files(&root, "", "jsonl").unwrap();
        let full_dirs = super::super::take_dirs_read();
        assert!(full_dirs > 200, "full walk listed {full_dirs} directories");
    }

    #[test]
    fn a_directory_scope_lists_that_directory_only() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        for name in ["one", "two"] {
            let dir = root.join(name);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("s.jsonl"), "{}\n").unwrap();
        }
        let scope = PathScope::from_paths([root.join("one")]).unwrap();
        let _ = super::super::take_dirs_read();
        let found =
            super::super::collect_matching_files_scoped(&root, "", "jsonl", Some(&scope)).unwrap();
        assert_eq!(found, vec![root.join("one/s.jsonl")]);
        assert!(super::super::take_dirs_read() < 5);
    }

    #[test]
    fn a_meta_sidecar_lists_the_transcripts_in_its_directory() {
        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("projects/p");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join("one.jsonl"), "{}\n").unwrap();
        std::fs::write(project.join("two.jsonl"), "{}\n").unwrap();
        std::fs::create_dir_all(temp.path().join("projects/other")).unwrap();
        std::fs::write(temp.path().join("projects/other/s.jsonl"), "{}\n").unwrap();
        let scope = PathScope::from_paths([project.join("agent.meta.json")]).unwrap();
        let found = super::super::collect_matching_files_scoped(
            &temp.path().join("projects"),
            "",
            "jsonl",
            Some(&scope),
        )
        .unwrap();
        assert_eq!(
            found,
            vec![project.join("one.jsonl"), project.join("two.jsonl")]
        );
    }

    fn claude_line(id: &str, text: &str) -> String {
        format!(
            r#"{{"sessionId":"{id}","uuid":"{id}-u","cwd":"/work","type":"user","message":{{"role":"user","content":"{text}"}},"timestamp":"2026-08-31T11:00:00Z"}}"#
        )
    }

    fn write_transcript(path: &std::path::Path, id: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, claude_line(id, id)).unwrap();
    }

    struct Home {
        _dir: tempfile::TempDir,
        db: std::path::PathBuf,
        roots: crate::ProviderRoots,
        conn: rusqlite::Connection,
    }

    fn home() -> Home {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("store/ai-history.db");
        std::fs::create_dir_all(db.parent().unwrap()).unwrap();
        let roots = crate::ProviderRoots::from_home(
            dir.path().to_path_buf(),
            dir.path().join("opencode.db"),
        );
        let conn = crate::open_db(&db).unwrap();
        Home {
            _dir: dir,
            db,
            roots,
            conn,
        }
    }

    fn claude_ids(conn: &rusqlite::Connection) -> Vec<String> {
        let mut stmt = conn
            .prepare("SELECT session_id FROM sessions WHERE source = 'claude' ORDER BY session_id")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .map(|id| id.unwrap())
            .collect()
    }

    fn sync_paths(home: &Home, paths: &[&std::path::Path]) {
        let scope = super::super::SweepScope::only(["claude"]).with_paths(PathScope::from_paths(
            paths.iter().map(|path| path.to_path_buf()),
        ));
        assert!(super::super::sync_basic(&home.conn, &home.db, &home.roots, true, &scope).unwrap());
    }

    #[test]
    fn a_path_scoped_tick_ingests_only_the_transcript_that_changed() {
        let home = home();
        let projects = home.roots.claude.join("projects");
        let one = projects.join("p/one.jsonl");
        let two = projects.join("q/two.jsonl");
        write_transcript(&one, "one");
        write_transcript(&two, "two");

        sync_paths(&home, &[&one]);
        assert_eq!(claude_ids(&home.conn), vec!["one".to_string()]);

        let state =
            std::fs::read_to_string(home.db.parent().unwrap().join(".sync-state.json")).unwrap();
        let state: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&state).unwrap();
        assert!(
            state
                .get(super::super::CLAUDE_RECORD_ATTRIBUTION_KEY)
                .is_none(),
            "a partial walk does not seal the attribution generation"
        );

        assert!(super::super::sync_basic(
            &home.conn,
            &home.db,
            &home.roots,
            true,
            &super::super::SweepScope::only(["claude"])
        )
        .unwrap());
        assert_eq!(
            claude_ids(&home.conn),
            vec!["one".to_string(), "two".to_string()]
        );
        let state =
            std::fs::read_to_string(home.db.parent().unwrap().join(".sync-state.json")).unwrap();
        let state: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&state).unwrap();
        assert!(state
            .get(super::super::CLAUDE_RECORD_ATTRIBUTION_KEY)
            .is_some());
    }

    #[test]
    fn deleting_one_transcript_forgets_only_its_cursor() {
        let home = home();
        let projects = home.roots.claude.join("projects");
        let one = projects.join("p/one.jsonl");
        let two = projects.join("q/two.jsonl");
        write_transcript(&one, "one");
        write_transcript(&two, "two");
        assert!(super::super::sync_basic(
            &home.conn,
            &home.db,
            &home.roots,
            true,
            &super::super::SweepScope::only(["claude"])
        )
        .unwrap());
        std::fs::remove_file(&one).unwrap();
        sync_paths(&home, &[&one]);
        let locators: Vec<String> = home
            .conn
            .prepare(
                "SELECT locator FROM transcript_cursors WHERE source = 'claude' ORDER BY locator",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(|locator| locator.unwrap())
            .collect();
        assert!(
            locators
                .iter()
                .all(|locator| !locator.ends_with("one.jsonl")),
            "the deleted cursor is gone: {locators:?}"
        );
        assert!(
            locators
                .iter()
                .any(|locator| locator.ends_with("two.jsonl")),
            "the other cursor stays: {locators:?}"
        );
    }

    #[test]
    fn a_codex_sessions_change_does_not_walk_archived_rollouts() {
        let home = home();
        let sessions = home.roots.codex.join("sessions/2026/08/01");
        let archived = home.roots.codex.join("archived_sessions/2026/08/01");
        std::fs::create_dir_all(&sessions).unwrap();
        std::fs::create_dir_all(&archived).unwrap();
        let live = sessions.join("rollout-live.jsonl");
        let old = archived.join("rollout-old.jsonl");
        let line = |id: &str| {
            format!(
                r#"{{"timestamp":"2026-08-01T00:00:00.000Z","type":"session_meta","payload":{{"id":"{id}","cwd":"/work","originator":"codex_cli_rs","cli_version":"0.148.0"}}}}"#
            )
        };
        std::fs::write(&live, line("live")).unwrap();
        std::fs::write(&old, line("old")).unwrap();
        let scope = super::super::SweepScope::only(["codex"])
            .with_paths(PathScope::from_paths([live.clone()]));
        assert!(super::super::sync_basic(&home.conn, &home.db, &home.roots, true, &scope).unwrap());
        let state =
            std::fs::read_to_string(home.db.parent().unwrap().join(".sync-state.json")).unwrap();
        let state: serde_json::Value = serde_json::from_str(&state).unwrap();
        let stamps = state
            .get("codex_rollouts_v7")
            .and_then(|value| value.as_object());
        let stamps = stamps.expect("live rollout stamped");
        assert!(stamps.keys().any(|key| key.ends_with("rollout-live.jsonl")));
        assert!(stamps.keys().all(|key| !key.ends_with("rollout-old.jsonl")));
    }
}
