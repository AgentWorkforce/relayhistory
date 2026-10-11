//! Codex's shallow scanner: one bounded read per rollout, plus the thread
//! names from `session_index.jsonl`.

use super::*;

/// Codex's rollout scanner. Thread names live outside the rollouts, in
/// `session_index.jsonl`; the index is read once per pass, on the first
/// rollout the pass actually reads, so an unchanged rescan reads nothing.
#[derive(Default)]
pub(crate) struct CodexProvider {
    thread_names: Mutex<CodexThreadNames>,
}

#[derive(Default)]
struct CodexThreadNames {
    index: Option<PathBuf>,
    names: Option<HashMap<String, String>>,
}

impl CodexProvider {
    fn thread_name(&self, session_id: &str) -> Result<Option<String>> {
        let mut state = self.thread_names.lock().expect("codex thread names lock");
        if state.names.is_none() {
            let names = match state.index.as_deref() {
                Some(index) if index.exists() => {
                    crate::ingest::codex_thread_names::read_thread_names(index)?
                }
                _ => HashMap::new(),
            };
            state.names = Some(names);
        }
        Ok(state.names.as_ref().and_then(|names| names.get(session_id).cloned()))
    }
}

impl ShallowSessionProvider for CodexProvider {
    fn acquire(
        &self,
        _home: &Path,
        _observation: &crate::observations::SessionObservation,
    ) -> Result<crate::sources::AcquiredEvidence> {
        Ok(crate::sources::AcquiredEvidence::LocalFiles)
    }
    fn source(&self) -> &'static str {
        "codex"
    }
    /// The rollout parser writes prompts, events, tool calls, file edits and
    /// child-thread relationships: every kind a full session is made of.
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        FULL_SESSION_KINDS
    }

    fn watch_roots(&self, roots: &ProviderRoots<'_>) -> Vec<WatchRoot> {
        vec![
            WatchRoot::tree(roots.codex.join("sessions")),
            WatchRoot::tree(roots.codex.join("archived_sessions")),
        ]
    }

    fn enumerate(
        &self,
        env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        *self.thread_names.lock().expect("codex thread names lock") = CodexThreadNames {
            index: Some(crate::ingest::codex_thread_names::session_index_path(&env.codex_home)),
            names: None,
        };
        let mut files = Vec::new();
        for root in [
            env.codex_home.join("sessions"),
            env.codex_home.join("archived_sessions"),
        ] {
            files.extend(crate::collect_matching_files(&root, "rollout-", "jsonl")?);
        }
        file_candidates("codex", files, crate::file_stamp_and_modified)
    }

    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        _catalog: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let path = PathBuf::from(&candidate.locator);
        let bounded = read_bounded_jsonl(scan, &path)?;
        let Some(meta) = bounded.head_records().next().and_then(|line| {
            parse_record(line)
                .filter(|v| v.get("type").and_then(Value::as_str) == Some("session_meta"))
        }) else {
            return Ok(None);
        };
        let payload = meta.get("payload");
        let Some(session_id) = payload
            .and_then(|p| p.get("id"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        // Linked subagent threads are real rollouts but not root sessions;
        // standalone guardian rollouts may carry `source.subagent` without a
        // parent and are cataloged under their own payload.id.
        let is_subagent = crate::codex_is_subagent(payload, session_id);
        if is_subagent {
            return Ok(None);
        }
        let git = payload.and_then(|p| p.get("git"));
        let string_field = |owner: Option<&Value>, key: &str| {
            owner
                .and_then(|o| o.get(key))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        };
        let mut models = Vec::new();
        push_unique(
            &mut models,
            payload.and_then(|p| p.get("model")).and_then(Value::as_str),
        );
        let mut first_prompt = None;
        let mut first_activity_ms = claude_timestamp(&meta);
        let mut last_activity_ms = first_activity_ms;
        // A fork's copy of its parent's history is the parent's, so its
        // prompts are not this session's first prompt -- the same rule the
        // rollout walk applies to `history`.
        let mut replay_gate = crate::codex::ForkReplayGate::new(
            crate::codex::fork_parent_id(payload, session_id),
            crate::codex::fork_origin_ms(payload, session_id, claude_timestamp(&meta)),
        );
        for (index, line) in bounded.head_records().enumerate() {
            let Some(value) = parse_record(line) else {
                continue;
            };
            if let Some(ts) = claude_timestamp(&value) {
                first_activity_ms.get_or_insert(ts);
                last_activity_ms = Some(ts);
            }
            if replay_gate.step(index == 0, &value) == crate::codex::ReplayStep::Replay {
                continue;
            }
            if value.get("type").and_then(Value::as_str) == Some("turn_context") {
                push_unique(
                    &mut models,
                    value.pointer("/payload/model").and_then(Value::as_str),
                );
            }
            if first_prompt.is_none() {
                first_prompt = codex_substantive_prompt(&value);
            }
            // The first prompt and first timestamp are settled; the tail owns
            // the last timestamp and models stay best-effort, so nothing
            // further in the head can change the row.
            if first_prompt.is_some() && first_activity_ms.is_some() {
                break;
            }
        }
        for line in bounded.tail_records_rev() {
            let Some(value) = parse_record(line) else {
                continue;
            };
            if let Some(ts) = claude_timestamp(&value) {
                last_activity_ms = Some(ts);
                break;
            }
        }
        let mtime = crate::file_modified_ms(&path);
        Ok(Some(ShallowSession {
            source: "codex".into(),
            session_id: session_id.to_string(),
            cwd: string_field(payload, "cwd"),
            git_branch: string_field(git, "branch"),
            first_activity_ms,
            last_activity_ms: last_activity_ms.or(mtime),
            first_prompt,
            models,
            originator: string_field(payload, "originator"),
            agent_version: string_field(payload, "cli_version"),
            repo_url: string_field(git, "repository_url")
                .or_else(|| string_field(git, "remote_url")),
            initial_commit: string_field(git, "commit_hash"),
            workspace_roots: string_list(payload.and_then(|p| p.get("workspace_roots"))),
            title: self.thread_name(session_id)?,
            raw_path: Some(candidate.locator.clone()),
            ..Default::default()
        }))
    }
}

fn codex_substantive_prompt(value: &Value) -> Option<String> {
    crate::codex::human_message(value).map(|message| excerpt(&message.text))
}

fn string_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}
