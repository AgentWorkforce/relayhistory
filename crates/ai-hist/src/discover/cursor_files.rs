//! Cursor's shallow file list. A path-scoped tick names the transcripts under
//! the paths that changed; anything else is the project walk.

use anyhow::Result;

use super::{Candidate, DiscoveryEnv};

pub(super) fn candidates(env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
    let root = env.home.join(".cursor/projects");
    match crate::ingest::path_scope::PathScope::walk_of(env.path_scope(), &root) {
        crate::ingest::path_scope::RootWalk::Partial(scope) => {
            // `read_shallow` drops a candidate with no session id and the
            // engine then remembers the file as not a session. The id is the
            // directory name; the partial walk already has it.
            return Ok(stamped(crate::ingest::scoped_files::cursor_transcripts(
                &root,
                Some(scope),
            )?));
        }
        crate::ingest::path_scope::RootWalk::Skip => return Ok(Vec::new()),
        crate::ingest::path_scope::RootWalk::Full => {}
    }
    let mut out = Vec::new();
    for project_dir in crate::sorted_dirs(&root)? {
        let transcripts = project_dir.join("agent-transcripts");
        if !transcripts.is_dir() {
            continue;
        }
        for session_dir in crate::sorted_dirs(&transcripts)? {
            let Some(session_id) = session_dir.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            let jsonl = session_dir.join(format!("{session_id}.jsonl"));
            if let Ok((stamp, recency_hint_ms)) = crate::file_stamp_and_modified(&jsonl) {
                out.push(Candidate {
                    source: "cursor",
                    locator: jsonl.to_string_lossy().into_owned(),
                    session_id: Some(session_id.to_string()),
                    recency_hint_ms,
                    stamp,
                });
            }
            // A subagent is its own session, under its own file stem. The
            // project is decoded again by `read_shallow`, so none is passed.
            out.extend(stamped(
                crate::ingest::scoped_files::cursor_subagent_transcripts(&session_dir, "")?,
            ));
        }
    }
    Ok(out)
}

fn stamped(files: Vec<(std::path::PathBuf, String, String)>) -> Vec<Candidate> {
    let mut out = Vec::with_capacity(files.len());
    for (path, session_id, _) in files {
        let Ok((stamp, recency_hint_ms)) = crate::file_stamp_and_modified(&path) else {
            continue;
        };
        out.push(Candidate {
            source: "cursor",
            locator: path.to_string_lossy().into_owned(),
            session_id: Some(session_id),
            recency_hint_ms,
            stamp,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::super::ShallowSessionProvider;
    use super::*;

    #[test]
    fn a_path_scoped_cursor_candidate_keeps_its_session_id() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let changed = home.join(".cursor/projects/work-app/agent-transcripts/cursor-1/cursor-1.jsonl");
        let other = home.join(".cursor/projects/other/agent-transcripts/cursor-2/cursor-2.jsonl");
        for path in [&changed, &other] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(
                path,
                "{\"role\":\"user\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"<user_query>\\nfix\\n</user_query>\"}]}}\n",
            )
            .unwrap();
        }
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::init_db(&conn).unwrap();
        let env = DiscoveryEnv::with_roots(
            &conn,
            home.to_path_buf(),
            home.join("opencode.db"),
        )
        .with_path_scope(crate::ingest::path_scope::PathScope::from_paths([
            changed.clone(),
        ]));
        let found = candidates(&env).unwrap();
        assert_eq!(found.len(), 1, "the other project is not enumerated");
        assert_eq!(found[0].session_id.as_deref(), Some("cursor-1"));

        let session = super::super::CursorProvider
            .read_shallow(&env.scan(), Some(&conn), &found[0])
            .unwrap();
        assert_eq!(
            session.as_ref().map(|row| row.session_id.as_str()),
            Some("cursor-1")
        );
    }

    /// The full walk lists a subagent transcript as its own candidate, and its
    /// shallow row takes the project from the directory under `projects`,
    /// which for a subagent is one level further up than for its parent.
    ///
    /// Positive control: before the fix the walk found only `parent-1`, and
    /// the fixed-depth cwd lookup decoded `/agent/transcripts` for the child.
    #[test]
    fn the_full_walk_discovers_subagent_transcripts() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let session = home.join(".cursor/projects/work-app/agent-transcripts/parent-1");
        let parent = session.join("parent-1.jsonl");
        let child = session.join("subagents/child-1.jsonl");
        for path in [&parent, &child] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(
                path,
                "{\"role\":\"user\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"<user_query>\\nfix\\n</user_query>\"}]}}\n",
            )
            .unwrap();
        }
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::init_db(&conn).unwrap();
        let env = DiscoveryEnv::with_roots(&conn, home.to_path_buf(), home.join("opencode.db"));
        let found = candidates(&env).unwrap();
        assert_eq!(
            found
                .iter()
                .map(|candidate| candidate.session_id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            vec!["parent-1", "child-1"]
        );
        assert_eq!(found[1].locator, child.to_string_lossy());

        let session = super::super::CursorProvider
            .read_shallow(&env.scan(), Some(&conn), &found[1])
            .unwrap()
            .unwrap();
        assert_eq!(session.session_id, "child-1");
        assert_eq!(session.cwd.as_deref(), Some("/work/app"));
    }
}
