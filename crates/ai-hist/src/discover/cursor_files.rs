//! Cursor's shallow provider and its file list: agent transcripts, and the
//! chats Cursor kept only in its own chat store. A path-scoped tick names the
//! files under the paths that changed; anything else is the full walk.

use super::*;

pub(super) fn candidates(env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
    let mut out = transcript_candidates(env)?;
    out.extend(chat_store_candidates(env)?);
    Ok(out)
}

/// The chats Cursor kept only in its own chat store: those with no
/// transcript, so no chat is offered twice.
fn chat_store_candidates(env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
    let mut out = Vec::new();
    for (store, session_id) in crate::ingest::cursor_store::chat_stores(&env.home, env.path_scope())? {
        let Ok((stamp, recency_hint_ms)) =
            crate::ingest::cursor_store::chat_store_stamp_and_modified(&store)
        else {
            continue;
        };
        out.push(Candidate {
            source: "cursor",
            locator: store.to_string_lossy().into_owned(),
            session_id: Some(session_id),
            recency_hint_ms,
            stamp,
        });
    }
    Ok(out)
}

fn transcript_candidates(env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
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

// ---------------------------------------------------------------------------
// the shallow provider
// ---------------------------------------------------------------------------

pub(crate) struct CursorProvider;

impl ShallowSessionProvider for CursorProvider {
    fn acquire(
        &self,
        _home: &Path,
        _observation: &crate::observations::SessionObservation,
    ) -> Result<crate::sources::AcquiredEvidence> {
        Ok(crate::sources::AcquiredEvidence::LocalFiles)
    }
    fn source(&self) -> &'static str {
        "cursor"
    }
    /// The transcript parser writes prompts, events, tool calls and file edits.
    /// A `Task` block names no child transcript, so delegation is recorded
    /// from the other side: a subagent transcript at
    /// `<parent>/subagents/<child>.jsonl` writes the edge to its parent.
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        &[
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::ToolCall,
            EvidenceKind::FileEdit,
            EvidenceKind::Relationship,
        ]
    }

    fn watch_roots(&self, roots: &ProviderRoots<'_>) -> Vec<WatchRoot> {
        vec![
            WatchRoot::tree(roots.home.join(".cursor/projects")),
            // Cursor's own chat store, for the chats it wrote no transcript for.
            WatchRoot::tree(roots.home.join(crate::ingest::cursor_store::CHATS_DIR)),
        ]
    }

    fn enumerate(
        &self,
        env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        candidates(env)
    }

    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        _catalog: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let path = PathBuf::from(&candidate.locator);
        let Some(session_id) = candidate.session_id.clone() else {
            return Ok(None);
        };
        // A chat Cursor kept only in its chat store is read as the transcript
        // records its two ends render to, so both kinds of chat catalog alike
        // and a shallow read stays bounded.
        let chat_store = if crate::ingest::cursor_store::is_chat_store(&path) {
            match crate::ingest::cursor_store::read_chat_store_preview(&path)? {
                Some(store) => Some(store),
                None => return Ok(None),
            }
        } else {
            None
        };
        let bounded = match &chat_store {
            Some(store) => bounded_jsonl_from_bytes(store.transcript.as_bytes()),
            None => read_bounded_jsonl(scan, &path)?,
        };
        let first_prompt = bounded
            .head_records()
            .filter_map(|line| {
                let line = String::from_utf8_lossy(line);
                crate::parse_cursor_text(&line).ok().flatten()
            })
            .map(|prompt| excerpt(&prompt))
            .find(|prompt| !prompt.is_empty());
        // Cursor writes no timestamp field and no model on its records. The
        // only time signal in the file is the localized `<timestamp>` tag its
        // client injects into a human turn, so the head read looks for that
        // and reports nothing when the build did not write one. `models` is
        // read from `message.model` for the builds that write it; an empty
        // list means "not seen", never "no model".
        let mut head_times = bounded.head_records().filter_map(cursor_record_time);
        let first_activity_ms = head_times.next();
        // For a transcript past the head budget the tail is a separate region,
        // so a turn time that only appears in the head is unreachable from the
        // tail scan. A long run of assistant and tool records after one dated
        // human turn is the ordinary shape of that: the tail finds no tag,
        // because only a human turn carries one, and falling straight to the
        // mtime reported a session as having last spoken "now". Full ingestion
        // disagrees — those records inherit the open turn's time — and because
        // the discovery upsert merges `last_activity_ms` with `MAX`, the mtime
        // would also re-expand a window a rebuild had just retracted. The last
        // time the head could read is the best recorded evidence there is; the
        // mtime stays for a transcript with no readable turn time at all.
        let last_head_ms = head_times.last().or(first_activity_ms);
        let last_activity_ms = bounded
            .tail_records_rev()
            .find_map(cursor_record_time)
            .or(last_head_ms)
            .or_else(|| chat_store.as_ref().and_then(|store| store.updated_ms))
            .or_else(|| crate::file_modified_ms(&path));
        let mut models = Vec::new();
        for model in bounded.head_records().filter_map(cursor_record_model) {
            if !models.contains(&model) {
                models.push(model);
            }
        }
        let last_assistant_text = bounded
            .tail_records_rev()
            .find_map(cursor_assistant_text)
            .map(|text| excerpt(&text));
        // Found by walking up rather than at a fixed depth: a subagent
        // transcript sits one directory deeper than its parent's.
        let cwd = match &chat_store {
            Some(store) => store.cwd.clone(),
            None => crate::ingest::cursor::project_dir_name(&path).map(crate::decode_cursor_project),
        };
        let first_activity_ms =
            first_activity_ms.or_else(|| chat_store.as_ref().and_then(|store| store.created_ms));
        Ok(Some(ShallowSession {
            source: "cursor".into(),
            session_id,
            cwd,
            first_activity_ms,
            last_activity_ms,
            first_prompt,
            last_assistant_text,
            models,
            raw_path: Some(candidate.locator.clone()),
            ..Default::default()
        }))
    }
}

/// Epoch milliseconds for one Cursor record, from the injected `<timestamp>`
/// tag or from a record `timestamp` field if a build writes one.
fn cursor_record_time(line: &[u8]) -> Option<i64> {
    let value = parse_record(line)?;
    let obj = value.as_object()?;
    if let Some(ts) = obj.get("timestamp").and_then(|v| {
        v.as_str()
            .and_then(crate::parse_iso_ms)
            .or_else(|| v.as_i64())
    }) {
        return Some(ts);
    }
    crate::ingest::cursor::injected_turn_time(
        crate::ingest::cursor::record_role(obj),
        &crate::ingest::cursor::record_blocks(obj),
    )
}

/// `message.model` for the Cursor builds that record one.
fn cursor_record_model(line: &[u8]) -> Option<String> {
    let value = parse_record(line)?;
    let model = value.get("message")?.get("model")?.as_str()?.trim();
    (!model.is_empty()).then(|| model.to_string())
}

/// The assistant prose in one Cursor record, ignoring tool and marker blocks.
fn cursor_assistant_text(line: &[u8]) -> Option<String> {
    let value = parse_record(line)?;
    let obj = value.as_object()?;
    if crate::ingest::cursor::record_role(obj) != Some("assistant") {
        return None;
    }
    // One assistant record can hold several text blocks — prose, a tool call,
    // then more prose. Full ingestion walks them in order and keeps the last
    // non-empty one, so the summary is the reply's closing line. Taking the
    // first here instead would make the catalog advertise the opening line and
    // hydration silently rewrite it.
    crate::ingest::cursor::record_blocks(obj)
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .map(str::trim)
        .rfind(|text| !text.is_empty())
        .map(str::to_string)
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
