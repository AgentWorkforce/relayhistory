//! Cursor's shallow file list. A path-scoped tick names the transcripts under
//! the paths that changed; anything else is the project walk.

use anyhow::Result;

use super::{file_candidates, Candidate, DiscoveryEnv};

pub(super) fn candidates(env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
    let root = env.home.join(".cursor/projects");
    match crate::ingest::path_scope::PathScope::walk_of(env.path_scope(), &root) {
        crate::ingest::path_scope::RootWalk::Partial(scope) => {
            return file_candidates(
                "cursor",
                crate::cursor_transcript_paths(&root, Some(scope))?,
                crate::file_stamp_and_modified,
            );
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
            let Ok((stamp, recency_hint_ms)) = crate::file_stamp_and_modified(&jsonl) else {
                continue;
            };
            out.push(Candidate {
                source: "cursor",
                locator: jsonl.to_string_lossy().into_owned(),
                session_id: Some(session_id.to_string()),
                recency_hint_ms,
                stamp,
            });
        }
    }
    Ok(out)
}
