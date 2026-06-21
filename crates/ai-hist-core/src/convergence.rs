//! WS-9 cloud-sync: map the local recall store onto the WS-1 convergence envelope.
//!
//! This is the schema-coupled surface of the relayhistory cloud-sync lens (Agent Relay
//! Loop). It is **additive** to the parity-gated Phase-1 core — it ports no existing
//! behavior, so it does not block the Python→Rust cutover.
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
/// even when the row carries no session id.
pub fn map_history_entry(entry: &HistoryEntry) -> ConvergenceEnvelope {
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
        significance: None,
        confidence: None,
        tags: Vec::new(),
        actor_name: None,
        project_id: None,
        task_title: None,
        task_description: None,
        task_status: None,
        task_ref: None,
        record: None,
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
    let owned = |s: Option<&str>| s.map(str::trim).filter(|x| !x.is_empty()).map(str::to_string);
    let task_title = owned(row.task_title);
    let task_description = owned(row.task_description);
    let task_status = owned(row.status);
    let project_id = owned(row.project_id);
    // taskRef: bounded cross-lens correlation seed (None until ai-hist persists task.source).
    let task_ref_json = row
        .task_ref
        .map(|tr| json!({ "system": tr.system, "id": tr.id }));

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
            let end = s[after..]
                .find('/')
                .map(|o| after + o)
                .unwrap_or(s.len());
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
            let end = s[after..]
                .find('\\')
                .map(|o| after + o)
                .unwrap_or(s.len());
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
        assert_eq!(epoch_ms_to_iso(1_782_036_000_000), "2026-06-21T10:00:00.000Z");
        assert_eq!(epoch_ms_to_iso(1_782_036_000_123), "2026-06-21T10:00:00.123Z");
    }

    #[test]
    fn home_path_normalization_strips_username() {
        assert_eq!(
            normalize_home_path("/Users/khaliqgant/Projects/burn/file.ts"),
            "/Users/~/Projects/burn/file.ts"
        );
        assert_eq!(
            normalize_home_path("/home/alice/repo"),
            "/home/~/repo"
        );
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
        assert_eq!(normalize_home_path("github.com/org/repo"), "github.com/org/repo");
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
        assert!(events.iter().all(|e| e.lens.as_deref() == Some("trajectories")));
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
    fn empty_arrays_emit_zero_events() {
        let events = traj("[]", r#"{"learnings":[],"suggestions":[],"confidence":1.0}"#);
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
    }
}
