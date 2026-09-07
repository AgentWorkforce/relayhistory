//! WS-9 cloud-sync: map the local recall store onto the WS-1 convergence envelope.
//!
//! This is the schema-coupled surface of the relayhistory cloud-sync lens (Agent Relay
//! Loop). It is **additive** to the local history and query core.
//!
//! Contract source of truth: `relayhistory-cloud/docs/decisions/2026-06-21-normalized-agent-event-schema.md`
//! (WS-1, human-ratified 2026-06-21). The CLI emits these envelopes in the heterogeneous
//! `records[]` of `POST /v1/ingest`; the server owns tenancy (`orgId`/`workspaceId`/`machineId`)
//! from auth context and is the compliance boundary for scrubbing + `toBasisPoints`.
//!
//! v1 trajectory scope (ratified): distilled `decisions` + `retrospective` from the local
//! store only. The raw chapter-event stream (`trajevent:*`) is the WS-6/Pair (b) delta and
//! requires re-parsing the source file via the `path` column — out of scope here.

use crate::HistoryEntry;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Identity of the syncing machine/capture source. `id` is the WS-1 `machineId`
/// (sub-tenant); the server still owns `orgId`/`workspaceId` from auth context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct MachineIdentity {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os: Option<String>,
    #[serde(rename = "cliVersion", skip_serializing_if = "Option::is_none")]
    pub cli_version: Option<String>,
}

/// Body of `POST /v1/ingest` (one heterogeneous batch). `batchId` is the client-generated
/// idempotency key; `cursors` carry per-source resume watermarks. `orgId` is never sent.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct IngestRequest {
    pub machine: MachineIdentity,
    #[serde(rename = "batchId")]
    pub batch_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursors: Option<Value>,
    pub records: Vec<ConvergenceEnvelope>,
}

/// Response from `POST /v1/ingest`. `cursors` are the server-confirmed watermarks the
/// outbox advances local state to (durable-outbox resume point).
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct IngestResponse {
    #[serde(rename = "batchId")]
    pub batch_id: String,
    pub received: u64,
    pub accepted: u64,
    #[serde(default)]
    pub cursors: Option<Value>,
}

/// Token categories sent to convergence. Input excludes cache reads; cost is owned by burn.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsage {
    pub input: u64,
    pub output: u64,
    pub reasoning: u64,
    pub cache_read: u64,
    pub cache_create: u64,
}

/// One heterogeneous convergence record in `POST /v1/ingest` `records[]`.
///
/// Tenancy fields (`orgId`/`workspaceId`/`machineId`) are intentionally absent — the
/// server derives them from auth context; the client never asserts them.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ConvergenceEnvelope {
    pub v: u32,
    /// In the WS-1 PK. Retrospective kinds: learnings/challenges → `finding`,
    /// suggestions/summary/approach → `reflection`, decisions → `decision`.
    pub kind: String,
    /// Upstream capture tool/harness where known (in the WS-1 PK) — e.g. `claude`,
    /// `codex`; `trajectories` for trajectory rows whose originating harness is
    /// unrecoverable from the local store.
    pub source: String,
    /// Non-PK provenance facet distinguishing the convergence lens (WS-1 ruling):
    /// `history` (prompts), `trajectories` (decisions/retro), `burn` (cost — not emitted
    /// here). Lets Learn/Plan filter by lens without overloading `source`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lens: Option<String>,
    #[serde(rename = "sessionId")]
    pub session_id: String,
    /// Deterministic, kind-namespaced, collision-free. Never relies on server fallback.
    #[serde(rename = "eventId")]
    pub event_id: String,
    /// ISO-8601 UTC (the wire format). Source epoch-ms is converted here.
    pub ts: String,
    #[serde(rename = "type")]
    pub event_type: String,
    /// Scrubbed, readable text — the pgvector embedding input.
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<TokenUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub significance: Option<String>,
    /// Source-native float 0..1. Server owns `toBasisPoints`. Emitted as `null` (not
    /// skipped) when absent, per the WS-1 contract — the event is never dropped for a
    /// missing confidence.
    pub confidence: Option<f64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(rename = "actorName", skip_serializing_if = "Option::is_none")]
    pub actor_name: Option<String>,
    /// Grouping facet (typed server field).
    #[serde(rename = "projectId", skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Task context (typed server fields). The **server** enriches `content` with
    /// `Task: <taskTitle>` at ingest — the client sends these structured fields and must
    /// NOT pre-fold the prefix into `content` (would double it).
    #[serde(rename = "taskTitle", skip_serializing_if = "Option::is_none")]
    pub task_title: Option<String>,
    #[serde(rename = "taskDescription", skip_serializing_if = "Option::is_none")]
    pub task_description: Option<String>,
    /// `active|completed|abandoned` — filterable Learn/Plan facet (indexed server-side).
    #[serde(rename = "taskStatus", skip_serializing_if = "Option::is_none")]
    pub task_status: Option<String>,
    /// Bounded `{system, id}` work-item ref — cross-lens correlation seed (None until
    /// ai-hist persists `task.source`).
    #[serde(rename = "taskRef", skip_serializing_if = "Option::is_none")]
    pub task_ref: Option<Value>,
    /// Minimized/scrubbed bounded provenance. `raw` is dropped wholesale; promoted scalars
    /// (confidence) are not shadow-stored here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<Value>,
    /// Structural file-overlap signal (WS-1 optional). Populated from `file_edits`.
    #[serde(rename = "filesTouched", skip_serializing_if = "Vec::is_empty")]
    pub files_touched: Vec<String>,
    /// `kind=session_outcome` fields — the cloud ingest path already accepts these.
    #[serde(rename = "commitSha", skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(rename = "matchMethod", skip_serializing_if = "Option::is_none")]
    pub match_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub numstat: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files: Option<Value>,
    #[serde(rename = "shippedAt", skip_serializing_if = "Option::is_none")]
    pub shipped_at: Option<String>,
}

/// Explicit project id when nothing resolvable is known. Never omitted (`None`) on
/// the wire — the cloud Pair filter drops NULL/empty `projectId` once capture lands.
pub const UNKNOWN_PROJECT: &str = "unknown";

/// Canonical project id for the cloud `project_aliases` table: a repo slug
/// (`owner/repo` or a short name), never a filesystem path.
///
/// Precedence, in order:
///
/// 1. an EXPLICIT `project` that is not a filesystem path — a deliberate label
///    (`trajectories.project_id`) always wins, because a human chose it
/// 2. the session's `git_remote`, which yields `owner/repo`
/// 3. the `project` path's last component
/// 4. [`UNKNOWN_PROJECT`]
///
/// A path used to win outright, and that is what fragmented the store. In practice
/// `project` is the harness's cwd, so the remote was resolved on every call and then
/// discarded, and `normalize_project_id` reduced the path to its last segment. Every
/// worktree and every subdirectory of one repo therefore became its own project:
/// `relayfile`, `relayfile-6e8d1a5c`, `relayfile-fork`, `relayfile-dev-collab` — and
/// an agent working in `cloud/packages/web` filed its work under `web`. On
/// 2026-09-04 the production store held 45,394 distinct ids for a few dozen repos,
/// 142 of them for `cloud` alone, while only 128 events out of 274,264 carried the
/// `owner/repo` form that `GET /v1/sessions?project=` is queried with.
///
/// The remote is the one identifier that is stable across worktrees, checkouts and
/// subdirectories, so it now outranks a path. It does NOT outrank an explicit label:
/// overriding a deliberately chosen project id with the repo it happens to live in
/// would lose information rather than canonicalise it.
pub fn resolve_project_id(project: Option<&str>, git_remote: Option<&str>) -> String {
    let project = nonempty(project);

    // An explicit, non-path label is a deliberate choice — keep it.
    if let Some(project) = project {
        if !is_filesystem_path(project) {
            return normalize_project_id(project);
        }
    }

    // Otherwise prefer the remote, which is stable across worktrees and subdirectories.
    //
    // `slug_from_git_remote`, NOT `normalize_project_id`: only a value that actually
    // parses as a repository remote may outrank a usable path. `normalize_project_id`
    // accepts any non-path string, so a malformed remote (`not-a-git-remote`, or a local
    // origin like `../repo.git`) would be taken as the project id and would discard a
    // perfectly good cwd — trading one noncanonical id for another.
    if let Some(remote) = nonempty(git_remote) {
        if let Some(slug) = slug_from_git_remote(remote) {
            return slug;
        }
    }

    if let Some(project) = project {
        return normalize_project_id(project);
    }
    UNKNOWN_PROJECT.to_string()
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|s| !s.is_empty())
}

fn normalize_project_id(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return UNKNOWN_PROJECT.to_string();
    }
    if let Some(slug) = slug_from_git_remote(raw) {
        return slug;
    }
    if is_filesystem_path(raw) {
        let slug = last_path_component(raw);
        return if slug.is_empty() || slug == "." || slug == ".." {
            UNKNOWN_PROJECT.to_string()
        } else {
            slug
        };
    }
    raw.trim_end_matches('/').to_string()
}

pub(crate) fn is_filesystem_path(s: &str) -> bool {
    let bytes = s.as_bytes();
    s.starts_with('/')
        || s.starts_with('~')
        || s == "."
        || s == ".."
        || s.starts_with("./")
        || s.starts_with("../")
        || s.starts_with(".\\")
        || s.starts_with("..\\")
        || s.starts_with("\\\\")
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && (bytes[2] == b'\\' || bytes[2] == b'/'))
}

fn last_path_component(s: &str) -> String {
    s.replace('\\', "/")
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string()
}

/// `git@host:owner/repo.git` / `https://host/owner/repo.git` → `owner/repo`.
fn slug_from_git_remote(raw: &str) -> Option<String> {
    let s = raw.trim();
    let path = if let Some(rest) = s.strip_prefix("git@") {
        rest.split_once(':')?.1.to_string()
    } else if s.contains("://") {
        let after_scheme = s.split_once("://")?.1;
        let host_and_path = after_scheme
            .rsplit_once('@')
            .map(|(_, host)| host)
            .unwrap_or(after_scheme);
        let (_, path) = host_and_path.split_once('/')?;
        path.to_string()
    } else if s.contains('@') && s.contains(':') && !s.contains("://") {
        s.split_once(':')?.1.to_string()
    } else {
        return None;
    };
    let mut path = path;
    if let Some(cut) = path.find(['?', '#']) {
        path.truncate(cut);
    }
    let path = path.trim_start_matches('/').trim_end_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
    if path.is_empty() {
        None
    } else {
        Some(path.to_string())
    }
}

/// Canonical retrospective `kind`s (ratified WS-1 ADR). The eventId prefix equals the
/// kind, and the `:<arrayName>:<i>` segment is the load-bearing collision invariant:
///   learnings/challenges → `finding`; suggestions/summary/approach → `reflection`;
///   decisions → `decision`.
const KIND_REFLECTION: &str = "reflection";
const KIND_FINDING: &str = "finding";

/// Map a `HistoryEntry` prompt row into a `prompt` convergence event.
///
/// eventId is deterministic from `(timestamp_ms, prompt_hash)` so re-syncs are idempotent
/// even when the row carries no session id. `projectId` is always a slug (or
/// [`UNKNOWN_PROJECT`]), never `None`.
pub fn map_history_entry(entry: &HistoryEntry) -> ConvergenceEnvelope {
    map_history_entry_with(entry, None, Vec::new(), None, None)
}

/// The `taskRef` a prompt belongs to, as `{system:"git", id:"<project>@<branch>"}`.
///
/// A branch is the closest thing to a unit of work that exists while the work is being
/// done — a PR number does not exist yet when the session runs, and a commit sha does not
/// exist until the end. Keying on `<project>@<branch>` lets a PR (or a person) ask for
/// every session behind a branch after the fact.
///
/// Returns `None` rather than a placeholder when the branch is unknown or detached: the
/// server stores `{}` for absent, and a synthetic ref would make unrelated sessions
/// collide under one task.
pub fn git_task_ref(project_id: &str, branch: Option<&str>) -> Option<serde_json::Value> {
    let branch = branch.map(str::trim).filter(|b| {
        // A detached HEAD is not a task, and neither is an empty string.
        !b.is_empty() && *b != "HEAD" && *b != "(detached)"
    })?;
    if project_id.is_empty() || project_id == UNKNOWN_PROJECT {
        return None;
    }
    Some(serde_json::json!({
        "system": "git",
        "id": format!("{project_id}@{branch}"),
    }))
}

/// Like [`map_history_entry`], enriched with the session's repo, files, branch,
/// and token usage attributed specifically to this prompt.
pub fn map_history_entry_with(
    entry: &HistoryEntry,
    git_remote: Option<&str>,
    files_touched: Vec<String>,
    git_branch: Option<&str>,
    usage: Option<TokenUsage>,
) -> ConvergenceEnvelope {
    let session_id = entry
        .session_id
        .clone()
        .unwrap_or_else(|| "unsessioned".to_string());
    let hash = entry
        .prompt_hash
        .clone()
        .unwrap_or_else(|| crate::prompt_hash(&entry.prompt));
    ConvergenceEnvelope {
        v: 1,
        kind: "prompt".to_string(),
        source: entry.source.clone(),
        lens: Some("history".to_string()),
        session_id,
        event_id: format!("prompt:{}:{}", entry.timestamp_ms, hash),
        ts: epoch_ms_to_iso(entry.timestamp_ms),
        event_type: "prompt".to_string(),
        content: normalize_home_path(entry.prompt.trim()),
        usage,
        significance: None,
        confidence: None,
        tags: Vec::new(),
        actor_name: None,
        project_id: Some(resolve_project_id(entry.project.as_deref(), git_remote)),
        task_title: None,
        task_description: None,
        task_status: None,
        task_ref: git_task_ref(
            &resolve_project_id(entry.project.as_deref(), git_remote),
            git_branch,
        ),
        record: None,
        files_touched: normalize_files_touched(files_touched),
        commit_sha: None,
        repo: None,
        branch: None,
        match_method: None,
        numstat: None,
        files: None,
        shipped_at: None,
    }
}

/// A `session_commit_links` row, mapped to a `kind=session_outcome` envelope.
pub struct SessionCommitLink<'a> {
    pub source: &'a str,
    pub session_id: &'a str,
    pub repo: Option<&'a str>,
    pub branch: Option<&'a str>,
    pub commit_sha: &'a str,
    pub match_method: &'a str,
    pub confidence: f64,
    pub files_json: Option<&'a str>,
    pub numstat_json: Option<&'a str>,
    pub evidence_json: Option<&'a str>,
    pub created_at_ms: i64,
}

/// One envelope per linked commit. Shape matches cloud `OutcomeEnvelope`
/// (`commitSha`, `matchMethod`, `numstat`, `files`) plus WS-1 `projectId` /
/// `filesTouched`.
pub fn map_session_outcome(
    link: &SessionCommitLink<'_>,
    project_id: &str,
    files_touched: Vec<String>,
) -> ConvergenceEnvelope {
    let sha = link.commit_sha.trim();
    let method = link.match_method.trim();
    let source = link.source.trim();
    let shipped_ms = commit_time_ms(link.evidence_json).unwrap_or(link.created_at_ms);
    ConvergenceEnvelope {
        v: 1,
        kind: "session_outcome".to_string(),
        source: source.to_string(),
        lens: Some("history".to_string()),
        session_id: link.session_id.to_string(),
        event_id: format!(
            "session_outcome:{source}:{}:{sha}:{method}",
            link.session_id
        ),
        ts: epoch_ms_to_iso(link.created_at_ms),
        event_type: "session_outcome".to_string(),
        content: format!("session linked to commit {sha} via {method}"),
        usage: None,
        significance: None,
        confidence: Some(link.confidence),
        tags: Vec::new(),
        actor_name: None,
        project_id: Some(resolve_project_id(Some(project_id), None)),
        task_title: None,
        task_description: None,
        task_status: None,
        task_ref: None,
        record: None,
        files_touched: normalize_files_touched(files_touched),
        commit_sha: Some(sha.to_string()),
        repo: nonempty(link.repo).map(normalize_home_path),
        branch: nonempty(link.branch).map(str::to_string),
        match_method: Some(method.to_string()),
        numstat: wrap_numstat(link.numstat_json),
        files: parse_files_json(link.files_json),
        shipped_at: Some(epoch_ms_to_iso(shipped_ms)),
    }
}

/// Commit time from `session_commit_links.evidence_json`, when the hook stored it.
fn commit_time_ms(evidence_json: Option<&str>) -> Option<i64> {
    let parsed: Value = serde_json::from_str(nonempty(evidence_json)?).ok()?;
    parsed
        .get("commit_time_ms")
        .and_then(Value::as_i64)
        .filter(|ms| *ms > 0)
}

fn normalize_files_touched(files: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for file in files {
        let normalized = normalize_home_path(file.trim());
        if !normalized.is_empty() && !out.iter().any(|existing| existing == &normalized) {
            out.push(normalized);
        }
    }
    out
}

fn wrap_numstat(raw: Option<&str>) -> Option<Value> {
    let parsed: Value = serde_json::from_str(nonempty(raw)?).ok()?;
    match parsed {
        Value::Array(_) => Some(json!({ "files": parsed })),
        Value::Object(_) => Some(parsed),
        _ => None,
    }
}

fn parse_files_json(raw: Option<&str>) -> Option<Value> {
    let parsed: Value = serde_json::from_str(nonempty(raw)?).ok()?;
    match parsed {
        Value::Array(items) => Some(Value::Array(
            items.into_iter().map(normalize_file_value).collect(),
        )),
        _ => None,
    }
}

fn normalize_file_value(value: Value) -> Value {
    match value {
        Value::String(path) => Value::String(normalize_home_path(&path)),
        Value::Object(mut map) => {
            if let Some(Value::String(path)) = map.get("path").cloned() {
                map.insert("path".into(), json!(normalize_home_path(&path)));
            }
            Value::Object(map)
        }
        other => other,
    }
}

/// A trajectory row from the local `trajectories` table (distilled lens).
pub struct TrajectoryRow<'a> {
    pub id: &'a str,
    pub persona_id: Option<&'a str>,
    /// Grouping provenance (carried into `record` for Learn/Plan facets).
    pub project_id: Option<&'a str>,
    /// What the trajectory was about — prime Plan/WS-5 retrieval signal. Sent as the
    /// structured `taskTitle` field; the **server** folds it into `content` at ingest.
    pub task_title: Option<&'a str>,
    pub task_description: Option<&'a str>,
    pub status: Option<&'a str>,
    /// Work-item reference `task.source.{system,id}` (e.g. `("github","123")`) — the
    /// highest-leverage cross-lens correlation seed (WS-4/WS-6): trajectories sharing a
    /// task id are the same work, and once burn stamps the same id the join is deterministic.
    /// Emitted as `taskRef` provenance when present.
    ///
    /// NOTE: ai-hist's local `trajectories` table currently persists only
    /// `task_title`/`task_description` — **not** `task.source`. Populating this requires
    /// extending the trajectory sync to store `task.source.{system,id}` (a small ingest
    /// change), or sourcing it during the deferred (b) `path` re-parse. Forward-compatible
    /// here so the mapper is ready the moment ingest provides it.
    pub task_ref: Option<TaskRef<'a>>,
    pub decisions_json: &'a str,
    pub retrospective_json: &'a str,
    pub timestamp_ms: i64,
}

/// Work-item reference (`task.source` in the trajectory schema).
#[derive(Debug, Clone, Copy)]
pub struct TaskRef<'a> {
    pub system: &'a str,
    pub id: &'a str,
}

/// Fan a trajectory's distilled `decisions` + `retrospective` blobs into convergence events.
///
/// Implements the v1 ratified scheme + trajectories-expert's six blob edge cases:
/// 1. `Decision.alternatives` union (`string[]` | `{option,reason}[]`) handled.
/// 2. confidence optional → emitted as `null`, event never dropped.
/// 3. empty arrays → zero events, no error.
/// 4. indices follow natural stored order (no sort/dedupe/filter) → stable PKs across re-sync.
/// 5. missing `trajectoryId` → skip (never emit `…:<undefined>:…`).
/// 6. decision `content` includes question/chosen/reasoning/alternatives for retrieval.
pub fn map_trajectory(row: &TrajectoryRow<'_>) -> Vec<ConvergenceEnvelope> {
    // (5) trajectoryId is the key for every retro event; without it, skip the whole row.
    let traj_id = row.id.trim();
    if traj_id.is_empty() {
        return Vec::new();
    }
    let ts = epoch_ms_to_iso(row.timestamp_ms);
    let actor = row
        .persona_id
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    // Structured task fields sent top-level (typed server fields). The server folds
    // `Task: <taskTitle>` into `content` at ingest — the client must NOT pre-fold it.
    let owned = |s: Option<&str>| {
        s.map(str::trim)
            .filter(|x| !x.is_empty())
            .map(str::to_string)
    };
    let task_title = owned(row.task_title);
    let task_description = owned(row.task_description);
    let task_status = owned(row.status);
    let project_id = Some(resolve_project_id(row.project_id, None));
    // taskRef: bounded cross-lens correlation seed (None until ai-hist persists task.source).
    let task_ref_json = row
        .task_ref
        .map(|tr| json!({ "system": tr.system, "id": tr.id }));

    if let Ok(compacted) = serde_json::from_str::<Value>(row.retrospective_json) {
        if is_compacted_rollup(&compacted) {
            return map_compacted_trajectory(
                traj_id,
                &ts,
                actor.as_deref(),
                project_id.as_deref(),
                task_title.as_deref(),
                task_description.as_deref(),
                task_status.as_deref(),
                task_ref_json.as_ref(),
                &compacted,
            );
        }
    }

    let mut out = Vec::new();

    // --- top-level decisions → decision:<trajectoryId>:<i> ---
    if let Ok(Value::Array(decisions)) = serde_json::from_str::<Value>(row.decisions_json) {
        for (i, d) in decisions.iter().enumerate() {
            let content = decision_content(d);
            if content.is_empty() {
                continue;
            }
            out.push(ConvergenceEnvelope {
                v: 1,
                kind: "decision".to_string(),
                source: "trajectories".to_string(),
                lens: Some("trajectories".to_string()),
                session_id: traj_id.to_string(),
                event_id: format!("decision:{traj_id}:{i}"),
                ts: ts.clone(),
                event_type: "decision".to_string(),
                content, // raw text only — server folds `Task: <title>` at ingest
                usage: None,
                significance: None,
                confidence: number_field(d, "confidence"), // (2) optional → null
                tags: Vec::new(),
                actor_name: actor.clone(),
                project_id: project_id.clone(),
                task_title: task_title.clone(),
                task_description: task_description.clone(),
                task_status: task_status.clone(),
                task_ref: task_ref_json.clone(),
                record: decision_record(d), // chosen/alternatives only
                files_touched: Vec::new(),
                commit_sha: None,
                repo: None,
                branch: None,
                match_method: None,
                numstat: None,
                files: None,
                shipped_at: None,
            });
        }
    }

    // --- retrospective fan-out ---
    if let Ok(retro) = serde_json::from_str::<Value>(row.retrospective_json) {
        let retro_conf = number_field(&retro, "confidence"); // required on Retrospective

        let push_retro =
            |kind: &str, event_id: String, content: String, conf: Option<f64>, out: &mut Vec<_>| {
                let item = normalize_home_path(content.trim());
                if item.is_empty() {
                    return;
                }
                out.push(ConvergenceEnvelope {
                    v: 1,
                    kind: kind.to_string(),
                    source: "trajectories".to_string(),
                    lens: Some("trajectories".to_string()),
                    session_id: traj_id.to_string(),
                    event_id,
                    ts: ts.clone(),
                    event_type: kind.to_string(),
                    content: item, // raw text only — server folds `Task: <title>` at ingest
                    usage: None,
                    significance: None,
                    confidence: conf,
                    tags: Vec::new(),
                    actor_name: actor.clone(),
                    project_id: project_id.clone(),
                    task_title: task_title.clone(),
                    task_description: task_description.clone(),
                    task_status: task_status.clone(),
                    task_ref: task_ref_json.clone(),
                    record: None,
                    files_touched: Vec::new(),
                    commit_sha: None,
                    repo: None,
                    branch: None,
                    match_method: None,
                    numstat: None,
                    files: None,
                    shipped_at: None,
                });
            };

        // single-value narrative fields (prime Plan/WS-5 targets); carry retro confidence
        if let Some(summary) = string_field(&retro, "summary") {
            push_retro(
                KIND_REFLECTION,
                format!("{KIND_REFLECTION}:{traj_id}:summary"),
                summary,
                retro_conf,
                &mut out,
            );
        }
        if let Some(approach) = string_field(&retro, "approach") {
            push_retro(
                KIND_REFLECTION,
                format!("{KIND_REFLECTION}:{traj_id}:approach"),
                approach,
                retro_conf,
                &mut out,
            );
        }

        // (3)(4) multi-item arrays: natural order, empty → nothing, array-name namespaced.
        // Canonical kinds: learnings/challenges → finding; suggestions → reflection.
        for (i, text) in string_array(&retro, "learnings").into_iter().enumerate() {
            push_retro(
                KIND_FINDING,
                format!("{KIND_FINDING}:{traj_id}:learning:{i}"),
                text,
                None,
                &mut out,
            );
        }
        for (i, text) in string_array(&retro, "suggestions").into_iter().enumerate() {
            push_retro(
                KIND_REFLECTION,
                format!("{KIND_REFLECTION}:{traj_id}:suggestion:{i}"),
                text,
                None,
                &mut out,
            );
        }
        for (i, text) in string_array(&retro, "challenges").into_iter().enumerate() {
            push_retro(
                KIND_FINDING,
                format!("{KIND_FINDING}:{traj_id}:challenge:{i}"),
                text,
                None,
                &mut out,
            );
        }
    }

    out
}

fn is_compacted_rollup(v: &Value) -> bool {
    v.get("type").and_then(Value::as_str) == Some("compacted")
        && v.get("sourceTrajectories")
            .and_then(Value::as_array)
            .is_some()
}

#[allow(clippy::too_many_arguments)]
fn compacted_event(
    traj_id: &str,
    ts: &str,
    actor: Option<&str>,
    project_id: Option<&str>,
    task_title: Option<&str>,
    task_description: Option<&str>,
    task_status: Option<&str>,
    task_ref: Option<&Value>,
    source: &str,
    lens: &str,
    tags: &[String],
    kind: &str,
    event_id: String,
    content: String,
    record: Option<Value>,
) -> Option<ConvergenceEnvelope> {
    let content = normalize_home_path(content.trim());
    if content.is_empty() {
        return None;
    }
    Some(ConvergenceEnvelope {
        v: 1,
        kind: kind.to_string(),
        source: source.to_string(),
        lens: Some(lens.to_string()),
        session_id: traj_id.to_string(),
        event_id,
        ts: ts.to_string(),
        event_type: kind.to_string(),
        content,
        usage: None,
        significance: None,
        confidence: None,
        tags: tags.to_vec(),
        actor_name: actor.map(str::to_string),
        project_id: project_id.map(str::to_string),
        task_title: task_title.map(str::to_string),
        task_description: task_description.map(str::to_string),
        task_status: task_status.map(str::to_string),
        task_ref: task_ref.cloned(),
        record,
        files_touched: Vec::new(),
        commit_sha: None,
        repo: None,
        branch: None,
        match_method: None,
        numstat: None,
        files: None,
        shipped_at: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn map_compacted_trajectory(
    traj_id: &str,
    ts: &str,
    actor: Option<&str>,
    project_id: Option<&str>,
    task_title: Option<&str>,
    task_description: Option<&str>,
    task_status: Option<&str>,
    task_ref: Option<&Value>,
    compacted: &Value,
) -> Vec<ConvergenceEnvelope> {
    let mut out = Vec::new();
    let source = compacted
        .get("source")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::trim)
        .unwrap_or("trajectories");
    let lens = compacted
        .get("lens")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::trim)
        .unwrap_or(source);
    let tags = compacted_tags(compacted);
    let is_learn = tags.iter().any(|tag| tag == "learn");
    let mut push = |kind: &str, event_id: String, content: String, record: Option<Value>| {
        if let Some(event) = compacted_event(
            traj_id,
            ts,
            actor,
            project_id,
            task_title,
            task_description,
            task_status,
            task_ref,
            source,
            lens,
            &tags,
            kind,
            event_id,
            content,
            record,
        ) {
            out.push(event);
        }
    };

    if let Some(decisions) = compacted.get("decisions").and_then(Value::as_array) {
        for (i, decision) in decisions.iter().enumerate() {
            push(
                "decision",
                format!("decision:{traj_id}:{i}"),
                compacted_decision_content(decision),
                compacted_decision_record(decision),
            );
        }
    }

    for (i, lesson) in compacted_object_array(compacted, "lessons")
        .into_iter()
        .enumerate()
    {
        push(
            KIND_REFLECTION,
            format!("{KIND_REFLECTION}:{traj_id}:lesson:{i}"),
            labeled_fields(
                &lesson,
                &[
                    ("Context", "context"),
                    ("Lesson", "lesson"),
                    ("Recommendation", "recommendation"),
                ],
            ),
            None,
        );
    }

    for (i, text) in string_array(compacted, "keyFindings")
        .into_iter()
        .enumerate()
    {
        push(
            KIND_FINDING,
            format!("{KIND_FINDING}:{traj_id}:keyfinding:{i}"),
            text,
            None,
        );
    }

    for (i, text) in string_array(compacted, "keyLearnings")
        .into_iter()
        .enumerate()
    {
        push(
            KIND_FINDING,
            format!("{KIND_FINDING}:{traj_id}:learning:{i}"),
            text,
            None,
        );
    }

    for (i, convention) in compacted_object_array(compacted, "conventions")
        .into_iter()
        .enumerate()
    {
        push(
            KIND_REFLECTION,
            format!("{KIND_REFLECTION}:{traj_id}:convention:{i}"),
            labeled_fields(
                &convention,
                &[
                    ("Pattern", "pattern"),
                    ("Rationale", "rationale"),
                    ("Scope", "scope"),
                ],
            ),
            None,
        );
    }

    if !is_learn {
        if let Some(narrative) = string_field(compacted, "narrative") {
            push(
                KIND_REFLECTION,
                format!("{KIND_REFLECTION}:{traj_id}:summary"),
                narrative,
                None,
            );
        }
    }

    for (i, text) in string_array(compacted, "openQuestions")
        .into_iter()
        .enumerate()
    {
        push(
            KIND_FINDING,
            format!("{KIND_FINDING}:{traj_id}:openquestion:{i}"),
            text,
            None,
        );
    }

    out
}

fn compacted_tags(compacted: &Value) -> Vec<String> {
    let tags = compacted
        .get("tags")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if tags.is_empty() {
        vec!["compacted".to_string()]
    } else {
        tags
    }
}

fn compacted_object_array(v: &Value, key: &str) -> Vec<Value> {
    v.get(key)
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter(|item| item.is_object())
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn compacted_decision_content(d: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(q) = string_field(d, "question") {
        parts.push(format!("Question: {q}"));
    }
    if let Some(c) = string_field(d, "chosen") {
        parts.push(format!("Chose: {c}"));
    }
    if let Some(r) = string_field(d, "reasoning") {
        parts.push(format!("Because: {r}"));
    }
    if let Some(impact) = string_field(d, "impact") {
        parts.push(format!("Impact: {impact}"));
    }
    normalize_home_path(parts.join("\n").trim())
}

fn compacted_decision_record(d: &Value) -> Option<Value> {
    let mut map = serde_json::Map::new();
    for key in ["chosen", "impact"] {
        if let Some(value) = string_field(d, key) {
            map.insert(key.to_string(), json!(normalize_home_path(&value)));
        }
    }
    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map))
    }
}

fn labeled_fields(v: &Value, fields: &[(&str, &str)]) -> String {
    fields
        .iter()
        .filter_map(|(label, key)| string_field(v, key).map(|value| format!("{label}: {value}")))
        .collect::<Vec<_>>()
        .join("\n")
}

/// (6) Build a decision's readable embedding text from question/chosen/reasoning/alternatives.
fn decision_content(d: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(q) = string_field(d, "question") {
        parts.push(format!("Question: {q}"));
    }
    if let Some(c) = string_field(d, "chosen") {
        parts.push(format!("Chose: {c}"));
    }
    if let Some(r) = string_field(d, "reasoning") {
        parts.push(format!("Because: {r}"));
    }
    let alts = alternatives_text(d);
    if !alts.is_empty() {
        parts.push(format!("Alternatives: {}", alts.join("; ")));
    }
    normalize_home_path(parts.join("\n").trim())
}

/// Minimized decision provenance for `record` (bounded; no raw passthrough).
fn decision_record(d: &Value) -> Option<Value> {
    let alts = alternatives_text(d);
    let mut map = serde_json::Map::new();
    if let Some(c) = string_field(d, "chosen") {
        map.insert("chosen".into(), json!(normalize_home_path(&c)));
    }
    if !alts.is_empty() {
        let normalized: Vec<String> = alts.iter().map(|a| normalize_home_path(a)).collect();
        map.insert("alternatives".into(), json!(normalized));
    }
    // confidence is promoted to the typed column — never shadow-stored in record.
    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map))
    }
}

/// `Decision.alternatives` union: `string[]` OR `{option, reason}[]` → readable strings.
fn alternatives_text(d: &Value) -> Vec<String> {
    let Some(arr) = d.get("alternatives").and_then(Value::as_array) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|a| match a {
            Value::String(s) => {
                let t = s.trim();
                (!t.is_empty()).then(|| t.to_string())
            }
            Value::Object(_) => {
                let option = a.get("option").and_then(Value::as_str).unwrap_or("").trim();
                let reason = a.get("reason").and_then(Value::as_str).unwrap_or("").trim();
                match (option.is_empty(), reason.is_empty()) {
                    (true, true) => None,
                    (false, true) => Some(option.to_string()),
                    (true, false) => Some(reason.to_string()),
                    (false, false) => Some(format!("{option} ({reason})")),
                }
            }
            _ => None,
        })
        .collect()
}

fn string_field(v: &Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn number_field(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(Value::as_f64)
}

/// Extract a `string[]` field; tolerates array-of-objects by pulling a text-ish field.
fn string_array(v: &Value, key: &str) -> Vec<String> {
    let Some(arr) = v.get(key).and_then(Value::as_array) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|item| match item {
            Value::String(s) => {
                let t = s.trim();
                (!t.is_empty()).then(|| t.to_string())
            }
            Value::Object(_) => ["text", "summary", "description", "value"]
                .iter()
                .find_map(|k| string_field(item, k)),
            _ => None,
        })
        .collect()
}

/// Strip the username segment from home-dir paths anywhere in `s`, preserving path shape.
/// Client-side defense-in-depth preflight; server-side WS-3 scrub remains the boundary.
///   `/Users/<name>/…` → `/Users/~/…`, `/home/<name>/…` → `/home/~/…`,
///   `C:\Users\<name>\…` → `C:\Users\~\…`
pub fn normalize_home_path(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let lower = s.to_ascii_lowercase();
    let mut i = 0;
    while i < s.len() {
        // POSIX: /Users/<name>/  or  /home/<name>/
        let posix = ["/users/", "/home/"]
            .iter()
            .find(|p| lower[i..].starts_with(*p))
            .copied();
        if let Some(prefix) = posix {
            let plen = prefix.len();
            let after = i + plen;
            // username runs until the next '/' or end
            let end = s[after..].find('/').map(|o| after + o).unwrap_or(s.len());
            if end > after {
                out.push_str(&s[i..after]); // keep "/Users/" original case
                out.push('~');
                i = end;
                continue;
            }
        }
        // Windows: C:\Users\<name>\
        if lower[i..].starts_with("\\users\\") {
            let after = i + "\\users\\".len();
            let end = s[after..].find('\\').map(|o| after + o).unwrap_or(s.len());
            if end > after {
                out.push_str(&s[i..after]);
                out.push('~');
                i = end;
                continue;
            }
        }
        // advance one char (UTF-8 safe)
        let ch_len = utf8_len(bytes[i]);
        out.push_str(&s[i..i + ch_len]);
        i += ch_len;
    }
    out
}

fn utf8_len(b: u8) -> usize {
    match b {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

/// Epoch milliseconds (UTC) → ISO-8601 `YYYY-MM-DDTHH:MM:SS.mmmZ`.
/// Self-contained (no chrono): civil-from-days per Howard Hinnant.
pub fn epoch_ms_to_iso(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let mut rem = secs.rem_euclid(86_400);
    let hour = rem / 3600;
    rem %= 3600;
    let min = rem / 60;
    let sec = rem % 60;
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}.{millis:03}Z")
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_conversion_matches_known_epochs() {
        assert_eq!(epoch_ms_to_iso(0), "1970-01-01T00:00:00.000Z");
        // 2026-06-21T10:00:00.000Z = 1_782_036_000_000 ms
        assert_eq!(
            epoch_ms_to_iso(1_782_036_000_000),
            "2026-06-21T10:00:00.000Z"
        );
        assert_eq!(
            epoch_ms_to_iso(1_782_036_000_123),
            "2026-06-21T10:00:00.123Z"
        );
    }

    #[test]
    fn home_path_normalization_strips_username() {
        assert_eq!(
            normalize_home_path("/Users/khaliqgant/Projects/burn/file.ts"),
            "/Users/~/Projects/burn/file.ts"
        );
        assert_eq!(normalize_home_path("/home/alice/repo"), "/home/~/repo");
        assert_eq!(
            normalize_home_path(r"C:\Users\alice\repo\file.ts"),
            r"C:\Users\~\repo\file.ts"
        );
        // mid-string + multiple occurrences
        assert_eq!(
            normalize_home_path("see /Users/bob/a and /home/carol/b"),
            "see /Users/~/a and /home/~/b"
        );
        // no home path → unchanged
        assert_eq!(
            normalize_home_path("github.com/org/repo"),
            "github.com/org/repo"
        );
    }

    fn traj(decisions: &str, retro: &str) -> Vec<ConvergenceEnvelope> {
        map_trajectory(&TrajectoryRow {
            id: "traj-1",
            persona_id: Some("planner"),
            project_id: None,
            task_title: None,
            task_description: None,
            status: None,
            task_ref: None,
            decisions_json: decisions,
            retrospective_json: retro,
            timestamp_ms: 1_782_036_000_000,
        })
    }

    #[test]
    fn retrospective_fans_out_collision_free() {
        let events = traj(
            "[]",
            r#"{"summary":"shipped X","approach":"TDD","learnings":["L0","L1"],
                "suggestions":["S0"],"challenges":["C0"],"confidence":0.8}"#,
        );
        let ids: Vec<&str> = events.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                "reflection:traj-1:summary",
                "reflection:traj-1:approach",
                "finding:traj-1:learning:0",
                "finding:traj-1:learning:1",
                "reflection:traj-1:suggestion:0",
                "finding:traj-1:challenge:0",
            ]
        );
        // canonical kinds: learnings/challenges → finding; suggestions/summary/approach → reflection
        assert_eq!(events[0].kind, "reflection"); // summary
        assert_eq!(events[2].kind, "finding"); // learning
        assert_eq!(events[4].kind, "reflection"); // suggestion
        assert_eq!(events[5].kind, "finding"); // challenge
                                               // learning[0] and suggestion[0] do NOT collide (the bug we fixed)
        assert_ne!(ids[2], ids[4]);
        // summary/approach carry retro confidence; individual items emit null (not dropped)
        assert_eq!(events[0].confidence, Some(0.8));
        assert_eq!(events[2].confidence, None);
        // ts converted to ISO; actor from persona_id
        assert_eq!(events[0].ts, "2026-06-21T10:00:00.000Z");
        assert_eq!(events[0].actor_name.as_deref(), Some("planner"));
    }

    #[test]
    fn threads_task_context_source_lens_and_taskref() {
        let events = map_trajectory(&TrajectoryRow {
            id: "traj-9",
            persona_id: Some("planner"),
            project_id: Some("agent-workforce"),
            task_title: Some("Build React forms"),
            task_description: Some("a reusable form lib"),
            status: Some("completed"),
            task_ref: Some(TaskRef {
                system: "github",
                id: "123",
            }),
            decisions_json: r#"[{"chosen":"Formik","reasoning":"less boilerplate"}]"#,
            retrospective_json: r#"{"summary":"shipped the form library","confidence":0.9}"#,
            timestamp_ms: 1_782_036_000_000,
        });
        // C: trajectory rows use canonical plural source + non-PK lens facet
        assert!(events.iter().all(|e| e.source == "trajectories"));
        assert!(events
            .iter()
            .all(|e| e.lens.as_deref() == Some("trajectories")));
        // B: structured task fields sent top-level; content is NOT pre-prefixed
        // (the server folds `Task: <title>` at ingest — client must not double it).
        assert!(events.iter().all(|e| !e.content.starts_with("Task:")));
        assert!(events
            .iter()
            .all(|e| e.task_title.as_deref() == Some("Build React forms")));
        assert!(events
            .iter()
            .all(|e| e.task_description.as_deref() == Some("a reusable form lib")));
        assert!(events
            .iter()
            .all(|e| e.task_status.as_deref() == Some("completed")));
        assert!(events
            .iter()
            .all(|e| e.project_id.as_deref() == Some("agent-workforce")));
        // taskRef as bounded top-level field
        let tr = events[0].task_ref.as_ref().unwrap();
        assert_eq!(tr["system"], "github");
        assert_eq!(tr["id"], "123");
        // decision record holds only chosen/alternatives (no task provenance shadow)
        let rec = events[0].record.as_ref().unwrap();
        assert_eq!(rec["chosen"], "Formik");
        assert!(rec.get("projectId").is_none());
    }

    /// Client half of trajectories-expert's end-to-end acceptance table: this exact
    /// fixture must produce these 6 events. The server then applies ×10000 (→
    /// `confidence_basis_points`) and `Task:` content enrichment on top.
    #[test]
    fn matches_trajectory_expert_acceptance_fixture() {
        let events = map_trajectory(&TrajectoryRow {
            id: "traj_abc",
            persona_id: Some("planner"),
            project_id: None,
            task_title: Some("Build WS-1 schema"),
            task_description: None,
            status: None,
            task_ref: None,
            decisions_json: r#"[{"question":"Which DB?","chosen":"Neon","reasoning":"pgvector",
                "alternatives":["D1",{"option":"Aurora","reason":"heavier"}],"confidence":0.9}]"#,
            retrospective_json: r#"{"summary":"Shipped schema","approach":"TDD",
                "learnings":["kind in PK"],"suggestions":["scrub paths"],
                "challenges":["union parsing"],"confidence":0.8}"#,
            timestamp_ms: 1_782_036_000_000,
        });

        // (eventId, kind, confidence-float — server multiplies ×10000)
        let got: Vec<(&str, &str, Option<f64>)> = events
            .iter()
            .map(|e| (e.event_id.as_str(), e.kind.as_str(), e.confidence))
            .collect();
        assert_eq!(
            got,
            vec![
                ("decision:traj_abc:0", "decision", Some(0.9)),
                ("reflection:traj_abc:summary", "reflection", Some(0.8)),
                ("reflection:traj_abc:approach", "reflection", Some(0.8)),
                ("finding:traj_abc:learning:0", "finding", None),
                ("reflection:traj_abc:suggestion:0", "reflection", None),
                ("finding:traj_abc:challenge:0", "finding", None),
            ]
        );
        // all rows: trajectories source/lens + task title threaded; content NOT pre-prefixed
        assert!(events
            .iter()
            .all(|e| e.source == "trajectories" && e.lens.as_deref() == Some("trajectories")));
        assert!(events
            .iter()
            .all(|e| e.task_title.as_deref() == Some("Build WS-1 schema")));
        assert!(events.iter().all(|e| !e.content.starts_with("Task:")));
        // decision content renders both alternative shapes (string + {option,reason})
        assert!(events[0].content.contains("Chose: Neon"));
        assert!(events[0].content.contains("D1"));
        assert!(events[0].content.contains("Aurora (heavier)"));
    }

    #[test]
    fn maps_compacted_rollup_acceptance_fixture() {
        let compacted = r#"{
            "id":"compact_fixture",
            "type":"compacted",
            "version":1,
            "sourceTrajectories":["traj_a","traj_b"],
            "compactedAt":"2026-06-21T10:00:00.000Z",
            "decisions":[
                {"question":"Which store?","chosen":"Neon","reasoning":"Needs pgvector","impact":"Pair can rank prior work"},
                {"question":"How to auth?","chosen":"Server-derived org","reasoning":"Avoid client tenancy claims","impact":"Tenant boundary is enforceable"}
            ],
            "lessons":[
                {"context":"Deploys","lesson":"Secrets can appear in run notes","recommendation":"Scrub ghp_FAKE0000000000000000000000000000abcd before surfacing snippets"},
                {"context":"Pair ranking","lesson":"Suggestions are actionable","recommendation":"Surface recommendations ahead of comparable findings"}
            ],
            "keyFindings":["Kind belongs in the primary key","Prompt events should not warn"],
            "keyLearnings":["Use ts_rank_cd normalization"],
            "conventions":[
                {"pattern":"Use server-derived tenancy","rationale":"Files are untrusted input","scope":"all cloud sync"}
            ],
            "narrative":"Compacted roll-up captured durable Pair guidance.",
            "openQuestions":["Should chapter streams become v1.2?"],
            "decisionGroups":[{"category":"storage","decisions":[0]}]
        }"#;
        let events = map_trajectory(&TrajectoryRow {
            id: "compact_fixture",
            persona_id: None,
            project_id: Some("relayhistory"),
            task_title: None,
            task_description: None,
            status: None,
            task_ref: None,
            decisions_json: "[]",
            retrospective_json: compacted,
            timestamp_ms: 1_782_036_000_000,
        });

        let got: Vec<(&str, &str)> = events
            .iter()
            .map(|e| (e.event_id.as_str(), e.kind.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("decision:compact_fixture:0", "decision"),
                ("decision:compact_fixture:1", "decision"),
                ("reflection:compact_fixture:lesson:0", "reflection"),
                ("reflection:compact_fixture:lesson:1", "reflection"),
                ("finding:compact_fixture:keyfinding:0", "finding"),
                ("finding:compact_fixture:keyfinding:1", "finding"),
                ("finding:compact_fixture:learning:0", "finding"),
                ("reflection:compact_fixture:convention:0", "reflection"),
                ("reflection:compact_fixture:summary", "reflection"),
                ("finding:compact_fixture:openquestion:0", "finding"),
            ]
        );
        assert!(events.iter().all(|e| e.source == "trajectories"));
        assert!(events
            .iter()
            .all(|e| e.lens.as_deref() == Some("trajectories")));
        assert!(events.iter().all(|e| e.tags == vec!["compacted"]));
        assert!(events.iter().all(|e| e.confidence.is_none()));
        assert!(events
            .iter()
            .all(|e| e.project_id.as_deref() == Some("relayhistory")));
        assert!(events.iter().all(|e| e.task_title.is_none()));
        assert!(events
            .iter()
            .all(|e| e.record.as_ref().is_none_or(|r| {
                r.get("sourceTrajectories").is_none() && r.get("raw").is_none()
            })));
        assert!(events[0]
            .content
            .contains("Impact: Pair can rank prior work"));
        assert!(events[2].content.contains("Recommendation: Scrub ghp_FAKE"));
        assert!(events[7]
            .content
            .contains("Pattern: Use server-derived tenancy"));
        assert!(!events.iter().any(|e| e.event_id.contains("decisionGroups")));
    }

    #[test]
    fn empty_arrays_emit_zero_events() {
        let events = traj(
            "[]",
            r#"{"learnings":[],"suggestions":[],"confidence":1.0}"#,
        );
        assert!(events.is_empty());
    }

    #[test]
    fn decision_alternatives_union_both_shapes() {
        // string[] shape
        let a = traj(
            r#"[{"question":"DB?","chosen":"Neon","reasoning":"FTS5","alternatives":["D1","SQLite"]}]"#,
            "{}",
        );
        assert_eq!(a.len(), 1);
        assert_eq!(a[0].event_id, "decision:traj-1:0");
        assert_eq!(a[0].kind, "decision");
        assert!(a[0].content.contains("Chose: Neon"));
        assert!(a[0].content.contains("Because: FTS5"));
        assert!(a[0].content.contains("D1; SQLite"));
        // object[] shape {option, reason}
        let b = traj(
            r#"[{"chosen":"Neon","alternatives":[{"option":"D1","reason":"no FTS5"}],"confidence":0.9}]"#,
            "{}",
        );
        assert!(b[0].content.contains("D1 (no FTS5)"));
        assert_eq!(b[0].confidence, Some(0.9));
        // confidence stripped from record shadow, alternatives retained
        let rec = b[0].record.as_ref().unwrap();
        assert!(rec.get("confidence").is_none());
        assert!(rec.get("alternatives").is_some());
    }

    #[test]
    fn missing_trajectory_id_is_skipped() {
        let events = map_trajectory(&TrajectoryRow {
            id: "  ",
            persona_id: None,
            project_id: None,
            task_title: None,
            task_description: None,
            status: None,
            task_ref: None,
            decisions_json: r#"[{"chosen":"X"}]"#,
            retrospective_json: r#"{"summary":"y"}"#,
            timestamp_ms: 0,
        });
        assert!(events.is_empty());
    }

    #[test]
    fn malformed_blobs_do_not_panic() {
        // garbage JSON → no events, no panic (robust against schema drift)
        let events = traj("not json", "also not json");
        assert!(events.is_empty());
    }

    #[test]
    fn indices_follow_natural_order_for_idempotency() {
        let events = traj("[]", r#"{"learnings":["first","second","third"]}"#);
        assert_eq!(events[0].event_id, "finding:traj-1:learning:0");
        assert_eq!(events[0].content, "first");
        assert_eq!(events[2].event_id, "finding:traj-1:learning:2");
        assert_eq!(events[2].content, "third");
    }

    #[test]
    fn history_entry_maps_to_prompt_event() {
        let e = HistoryEntry {
            id: 1,
            source: "claude".into(),
            session_id: Some("s1".into()),
            project: Some("/Users/khaliqgant/p".into()),
            prompt: "fix the auth bug".into(),
            prompt_hash: Some("abc123".into()),
            timestamp_ms: 1_782_036_000_000,
        };
        let env = map_history_entry(&e);
        assert_eq!(env.kind, "prompt");
        assert_eq!(env.session_id, "s1");
        assert_eq!(env.event_id, "prompt:1782036000000:abc123");
        assert_eq!(env.ts, "2026-06-21T10:00:00.000Z");
        assert_eq!(env.content, "fix the auth bug");
        assert_eq!(env.project_id.as_deref(), Some("p"));
        assert!(env.project_id.is_some());
    }

    #[test]
    fn project_id_mapping_path_git_remote_and_unknown() {
        // path project → last component, never a filesystem path on the wire
        assert_eq!(
            resolve_project_id(Some("/Users/khaliqgant/Projects/relayhistory"), None),
            "relayhistory"
        );
        assert_eq!(
            resolve_project_id(Some(r"C:\Users\alice\Projects\my-app"), None),
            "my-app"
        );
        // relative path forms → last component, never a path on the wire
        assert_eq!(resolve_project_id(Some("./repo"), None), "repo");
        assert_eq!(
            resolve_project_id(Some("../relayhistory"), None),
            "relayhistory"
        );
        assert_eq!(resolve_project_id(Some("./foo/bar"), None), "bar");
        assert_eq!(resolve_project_id(Some(r".\repo"), None), "repo");
        assert_eq!(resolve_project_id(Some("."), None), UNKNOWN_PROJECT);
        assert_eq!(resolve_project_id(Some(".."), None), UNKNOWN_PROJECT);
        // git-remote project → owner/repo slug
        assert_eq!(
            resolve_project_id(None, Some("git@github.com:AgentWorkforce/relayhistory.git")),
            "AgentWorkforce/relayhistory"
        );
        assert_eq!(
            resolve_project_id(
                None,
                Some("https://github.com/AgentWorkforce/relayhistory.git")
            ),
            "AgentWorkforce/relayhistory"
        );
        // unknown → explicit "unknown", never NULL
        assert_eq!(resolve_project_id(None, None), UNKNOWN_PROJECT);
        assert_eq!(resolve_project_id(Some("  "), Some("")), UNKNOWN_PROJECT);
        let unknown = map_history_entry(&HistoryEntry {
            id: 1,
            source: "claude".into(),
            session_id: Some("s".into()),
            project: None,
            prompt: "hi".into(),
            prompt_hash: Some("h".into()),
            timestamp_ms: 0,
        });
        assert_eq!(unknown.kind, "prompt");
        assert_eq!(unknown.project_id.as_deref(), Some(UNKNOWN_PROJECT));
        let v = serde_json::to_value(&unknown).unwrap();
        assert_eq!(v["projectId"], UNKNOWN_PROJECT);
        assert!(!v["projectId"].is_null());
        // The git remote wins over a cwd PATH. This inverted a previous assertion
        // ("history.project wins over git remote"), deliberately: a path is the
        // harness's working directory, so preferring it made every worktree and
        // subdirectory its own project and left the store with 45,394 distinct ids
        // for a few dozen repos.
        assert_eq!(
            resolve_project_id(
                Some("/tmp/other-app"),
                Some("git@github.com:AgentWorkforce/relayhistory.git")
            ),
            "AgentWorkforce/relayhistory"
        );
        // An explicit, non-path label still wins — that half of the old contract
        // survives, because a chosen id carries intent a repo name does not.
        assert_eq!(
            resolve_project_id(
                Some("other-app"),
                Some("git@github.com:AgentWorkforce/relayhistory.git")
            ),
            "other-app"
        );
    }

    #[test]
    fn session_outcome_envelope_shape() {
        let env = map_session_outcome(
            &SessionCommitLink {
                source: "claude",
                session_id: "s1",
                repo: Some("/Users/khaliqgant/Projects/relayhistory"),
                branch: Some("feat/x"),
                commit_sha: "abc123def",
                match_method: "cwd+branch",
                confidence: 0.91,
                files_json: Some(r#"["src/lib.rs","src/main.rs"]"#),
                numstat_json: Some(r#"[{"path":"src/lib.rs","additions":3,"deletions":1}]"#),
                evidence_json: None,
                created_at_ms: 1_782_036_000_000,
            },
            "relayhistory",
            vec!["/Users/khaliqgant/Projects/relayhistory/src/lib.rs".into()],
        );
        assert_eq!(env.kind, "session_outcome");
        assert_eq!(env.source, "claude");
        assert_eq!(env.session_id, "s1");
        assert_eq!(
            env.event_id,
            "session_outcome:claude:s1:abc123def:cwd+branch"
        );
        assert_eq!(env.project_id.as_deref(), Some("relayhistory"));
        assert_eq!(env.commit_sha.as_deref(), Some("abc123def"));
        assert_eq!(env.match_method.as_deref(), Some("cwd+branch"));
        assert_eq!(env.confidence, Some(0.91));
        assert_eq!(
            env.files_touched,
            vec!["/Users/~/Projects/relayhistory/src/lib.rs"]
        );
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(v["kind"], "session_outcome");
        assert_eq!(v["commitSha"], "abc123def");
        assert_eq!(v["matchMethod"], "cwd+branch");
        assert_eq!(v["projectId"], "relayhistory");
        assert_eq!(
            v["filesTouched"][0],
            "/Users/~/Projects/relayhistory/src/lib.rs"
        );
        assert_eq!(v["files"][0], "src/lib.rs");
        assert_eq!(v["numstat"]["files"][0]["path"], "src/lib.rs");
        assert_eq!(v["repo"], "/Users/~/Projects/relayhistory");
        assert_eq!(v["shippedAt"], "2026-06-21T10:00:00.000Z");
    }

    #[test]
    fn session_outcome_shipped_at_uses_commit_time_not_link_time() {
        let env = map_session_outcome(
            &SessionCommitLink {
                source: "claude",
                session_id: "s1",
                repo: Some("/tmp/repo"),
                branch: Some("main"),
                commit_sha: "abc123def",
                match_method: "cwd",
                confidence: 0.9,
                files_json: None,
                numstat_json: None,
                evidence_json: Some(r#"{"commit_time_ms":1000}"#),
                created_at_ms: 1_782_036_000_000,
            },
            "repo",
            Vec::new(),
        );
        assert_eq!(env.shipped_at.as_deref(), Some("1970-01-01T00:00:01.000Z"));
        assert_eq!(env.ts, "2026-06-21T10:00:00.000Z");
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(v["shippedAt"], "1970-01-01T00:00:01.000Z");
        assert_ne!(v["shippedAt"], v["ts"]);
    }

    #[test]
    fn session_outcome_event_id_includes_source_and_match_method() {
        let a = map_session_outcome(
            &SessionCommitLink {
                source: "claude",
                session_id: "s1",
                repo: None,
                branch: None,
                commit_sha: "abc123def",
                match_method: "cwd",
                confidence: 0.5,
                files_json: None,
                numstat_json: None,
                evidence_json: None,
                created_at_ms: 1,
            },
            "repo",
            Vec::new(),
        );
        let b = map_session_outcome(
            &SessionCommitLink {
                source: "claude",
                session_id: "s1",
                repo: None,
                branch: None,
                commit_sha: "abc123def",
                match_method: "cwd+branch",
                confidence: 0.9,
                files_json: None,
                numstat_json: None,
                evidence_json: None,
                created_at_ms: 2,
            },
            "repo",
            Vec::new(),
        );
        assert_eq!(a.event_id, "session_outcome:claude:s1:abc123def:cwd");
        assert_eq!(b.event_id, "session_outcome:claude:s1:abc123def:cwd+branch");
        assert_ne!(a.event_id, b.event_id);
    }

    #[test]
    fn ingest_request_serializes_to_wire_shape() {
        let req = IngestRequest {
            machine: MachineIdentity {
                id: "machine-1".into(),
                ..Default::default()
            },
            batch_id: "batch-abc".into(),
            cursors: Some(json!({ "trajectories": 42 })),
            records: vec![map_history_entry(&HistoryEntry {
                id: 1,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: None,
                prompt: "hi".into(),
                prompt_hash: Some("h".into()),
                timestamp_ms: 0,
            })],
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["machine"]["id"], "machine-1");
        assert_eq!(v["batchId"], "batch-abc"); // camelCase, not batch_id
        assert_eq!(v["cursors"]["trajectories"], 42);
        assert_eq!(v["records"][0]["eventId"], "prompt:0:h");
        // orgId is never sent — server owns tenancy
        assert!(v["machine"].get("orgId").is_none());
        assert!(v.get("orgId").is_none());

        // response round-trips, including server-confirmed cursors
        let resp: IngestResponse = serde_json::from_str(
            r#"{"batchId":"batch-abc","received":1,"accepted":1,"cursors":{"trajectories":43}}"#,
        )
        .unwrap();
        assert_eq!(resp.accepted, 1);
        assert_eq!(resp.cursors.unwrap()["trajectories"], 43);
    }

    #[test]
    fn envelope_serializes_with_camelcase_wire_names() {
        let env = map_history_entry(&HistoryEntry {
            id: 1,
            source: "codex".into(),
            session_id: None,
            project: None,
            prompt: "hi".into(),
            prompt_hash: None,
            timestamp_ms: 0,
        });
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(v["sessionId"], "unsessioned");
        assert!(v["eventId"].as_str().unwrap().starts_with("prompt:0:"));
        assert_eq!(v["type"], "prompt");
        // confidence emitted as null (not skipped)
        assert!(v.get("confidence").is_some());
        assert!(v["confidence"].is_null());
        // projectId is always present (never NULL)
        assert_eq!(v["projectId"], UNKNOWN_PROJECT);
    }
    #[test]
    fn task_ref_names_the_branch_the_work_happened_on() {
        let value = git_task_ref("AgentWorkforce/relayhistory", Some("feat/turns")).unwrap();
        assert_eq!(value["system"], "git");
        assert_eq!(value["id"], "AgentWorkforce/relayhistory@feat/turns");
    }

    /// A synthetic ref would make unrelated sessions collide under one task, which is
    /// worse than no ref at all — the server stores `{}` for absent and the filter simply
    /// does not match.
    #[test]
    fn task_ref_is_absent_rather_than_synthetic_when_there_is_no_real_branch() {
        assert!(git_task_ref("owner/repo", None).is_none());
        assert!(git_task_ref("owner/repo", Some("")).is_none());
        assert!(git_task_ref("owner/repo", Some("   ")).is_none());
        // A detached HEAD is a position, not a unit of work.
        assert!(git_task_ref("owner/repo", Some("HEAD")).is_none());
        // An unknown project would group every unattributable session together.
        assert!(git_task_ref(UNKNOWN_PROJECT, Some("main")).is_none());
    }

    #[test]
    fn prompt_envelopes_carry_the_task_ref() {
        let entry = HistoryEntry {
            id: 1,
            source: "claude".into(),
            session_id: Some("s1".into()),
            project: Some("AgentWorkforce/relayhistory".into()),
            prompt: "why".into(),
            prompt_hash: Some("h".into()),
            timestamp_ms: 1_700_000_000_000,
        };
        let env = map_history_entry_with(&entry, None, Vec::new(), Some("fix/scrub"), None);
        let task_ref = env
            .task_ref
            .expect("branch known, so a taskRef is expected");
        assert_eq!(task_ref["id"], "AgentWorkforce/relayhistory@fix/scrub");

        let without = map_history_entry_with(&entry, None, Vec::new(), None, None);
        assert!(without.task_ref.is_none());
    }

    /// The production store held 45,394 distinct project ids for a few dozen repos
    /// because the harness cwd outranked the git remote and was then reduced to its
    /// last path segment. These pin the precedence that fixes it.
    mod project_id_precedence {
        use super::super::{resolve_project_id, UNKNOWN_PROJECT};

        const REMOTE: &str = "git@github.com:AgentWorkforce/relayfile.git";

        #[test]
        fn a_worktree_path_resolves_to_the_repo_not_the_directory() {
            // The bug: `relayfile-live-review-vhs`, `relayfile-6e8d1a5c` and
            // `relayfile-fork` were three different projects to every consumer.
            for cwd in [
                "/Users/k/Projects/AgentWorkforce/relayfile",
                "/Users/k/Projects/AgentWorkforce/relayfile-6e8d1a5c",
                "/Users/k/Projects/AgentWorkforce/relayfile-live-review-vhs",
                "/Users/k/Projects/AgentWorkforce/relayfile-fork",
            ] {
                assert_eq!(
                    resolve_project_id(Some(cwd), Some(REMOTE)),
                    "AgentWorkforce/relayfile",
                    "worktree {cwd} must resolve to the repo"
                );
            }
        }

        #[test]
        fn a_subdirectory_resolves_to_the_repo_not_the_subdirectory() {
            // An agent working in cloud/packages/web filed its work under `web`,
            // which is why `web` and `sdk` appear as top-level projects.
            assert_eq!(
                resolve_project_id(
                    Some("/Users/k/Projects/AgentWorkforce/cloud/packages/web"),
                    Some("git@github.com:AgentWorkforce/cloud.git"),
                ),
                "AgentWorkforce/cloud"
            );
        }

        #[test]
        fn an_explicit_label_still_wins_over_the_remote() {
            // A deliberately chosen project id carries intent the repo name does not.
            // Overriding it would lose information rather than canonicalise it.
            assert_eq!(
                resolve_project_id(Some("relayfile-demo-readiness-0901"), Some(REMOTE)),
                "relayfile-demo-readiness-0901"
            );
        }

        #[test]
        fn a_path_falls_back_to_its_last_component_when_there_is_no_remote() {
            // Not every checkout has an origin. Previous behaviour is preserved.
            assert_eq!(
                resolve_project_id(Some("/Users/k/Projects/AgentWorkforce/relayfile"), None),
                "relayfile"
            );
        }

        #[test]
        fn an_unparseable_remote_does_not_swallow_the_path() {
            // NON-EMPTY malformed remotes. The earlier version of this test passed
            // whitespace, which `nonempty` strips before the remote branch is reached —
            // it asserted nothing about the behaviour it named.
            //
            // `normalize_project_id` accepts any non-path string, so without requiring a
            // parsed slug each of these would become the project id and discard a
            // perfectly usable path — trading one noncanonical id for another.
            for bogus in ["not-a-git-remote", "../repo.git", "origin", "   "] {
                assert_eq!(
                    resolve_project_id(
                        Some("/Users/k/Projects/AgentWorkforce/relayfile"),
                        Some(bogus)
                    ),
                    "relayfile",
                    "remote {bogus:?} does not parse as a repo slug and must not win"
                );
            }
        }

        #[test]
        fn nothing_resolvable_is_still_unknown() {
            assert_eq!(resolve_project_id(None, None), UNKNOWN_PROJECT);
        }

        #[test]
        fn https_and_ssh_remotes_agree() {
            // The same checkout must not change identity with the clone URL used.
            assert_eq!(
                resolve_project_id(
                    Some("/tmp/wt"),
                    Some("https://github.com/AgentWorkforce/relayfile.git")
                ),
                resolve_project_id(Some("/tmp/wt"), Some(REMOTE)),
            );
        }
    }
}
