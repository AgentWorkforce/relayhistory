use ai_hist::privacy::normalize_home_path;
use anyhow::{bail, Context, Result};
use chrono::{TimeZone, Utc};
use rusqlite::{params, Connection};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::time::Duration;

mod provider;

use provider::{complete, learn_rollup_from_output, resolve_provider};

const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com";
const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";
const DEFAULT_OPENAI_MODEL: &str = "gpt-4o-mini";
const DEFAULT_ANTHROPIC_MODEL: &str = "claude-3-5-haiku-latest";
const LLM_TIMEOUT: Duration = Duration::from_secs(300);

const LEARN_SYSTEM_PROMPT: &str = r#"You are a technical analyst distilling AI coding sessions into durable team memory.
Your job is to produce concise, high-signal guidance that captures:
- Key decisions and their reasoning
- Patterns/conventions that should carry forward
- Lessons learned from challenges and failures
- Concrete recommendations for future work
- Open questions or unresolved issues

Be specific. Reference actual file paths, function names, commands, and technical details when they matter.
Prefer actionable lessons, decisions, and conventions. Avoid generic summaries.
Do not include secrets verbatim; if sensitive material appears, describe the issue generically."#;

const LEARN_OUTPUT_SCHEMA: &str = r#"{
  "narrative": "string",
  "decisions": [
    {
      "question": "string",
      "chosen": "string",
      "reasoning": "string",
      "impact": "string"
    }
  ],
  "conventions": [
    {
      "pattern": "string",
      "rationale": "string",
      "scope": "string"
    }
  ],
  "lessons": [
    {
      "lesson": "string",
      "context": "string",
      "recommendation": "string"
    }
  ],
  "keyFindings": ["string"],
  "keyLearnings": ["string"],
  "openQuestions": ["string"]
}"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LearnProvider {
    Auto,
    OpenAi,
    Anthropic,
}

#[derive(Debug, Clone)]
pub struct LearnDistillOptions {
    pub source: Option<String>,
    pub session_id: Option<String>,
    pub limit: usize,
    pub max_chars: usize,
    pub max_output_tokens: usize,
    pub provider: LearnProvider,
    pub model: Option<String>,
    pub base_url: Option<String>,
    pub allow_cloud_llm: bool,
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LearnDistillReport {
    pub scanned: usize,
    pub distilled: usize,
    pub skipped: usize,
    pub rows: Vec<LearnRowReport>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LearnRowReport {
    pub id: String,
    pub source: String,
    pub session_id: String,
    pub events_estimate: usize,
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
struct SessionCandidate {
    source: String,
    session_id: String,
    project: Option<String>,
    first_ms: i64,
    last_ms: i64,
    raw_path: Option<String>,
    last_assistant_text: Option<String>,
}

#[derive(Debug, Clone)]
struct SessionTranscript {
    candidate: SessionCandidate,
    text: String,
    stable_hash: String,
}

pub fn distill_sessions(
    conn: &Connection,
    options: &LearnDistillOptions,
) -> Result<LearnDistillReport> {
    let provider = resolve_provider(options)?;
    let candidates = session_candidates(conn, options)?;
    let mut report = LearnDistillReport {
        scanned: candidates.len(),
        distilled: 0,
        skipped: 0,
        rows: Vec::new(),
    };

    for candidate in candidates {
        let transcript = build_session_transcript(conn, candidate, options.max_chars)?;
        if transcript.text.trim().is_empty() {
            report.skipped += 1;
            continue;
        }
        let id = format!("learn_{}", transcript.stable_hash);
        let prompt = build_learn_prompt(&transcript.text, options.max_output_tokens);
        let output =
            complete(&provider, &prompt, options.max_output_tokens).with_context(|| {
                format!(
                    "learn distill failed for {}",
                    transcript.candidate.session_id
                )
            })?;
        let compacted = learn_rollup_from_output(&id, &transcript, &output)?;
        let events_estimate = estimate_surfaceable_events(&compacted);
        if events_estimate == 0 {
            report.skipped += 1;
            continue;
        }
        if !options.dry_run {
            upsert_learn_rollup(conn, &transcript, &compacted)?;
        }
        report.distilled += 1;
        report.rows.push(LearnRowReport {
            id,
            source: transcript.candidate.source,
            session_id: transcript.candidate.session_id,
            events_estimate,
            dry_run: options.dry_run,
        });
    }

    Ok(report)
}

fn session_candidates(
    conn: &Connection,
    options: &LearnDistillOptions,
) -> Result<Vec<SessionCandidate>> {
    let mut sql = String::from(
        "SELECT h.source, h.session_id, MIN(h.project), MIN(h.timestamp_ms), MAX(h.timestamp_ms), \
         s.raw_path, s.last_assistant_text \
         FROM history h \
         LEFT JOIN sessions s ON s.source = h.source AND s.session_id = h.session_id \
         WHERE h.session_id IS NOT NULL AND h.session_id != ''",
    );
    let mut values = Vec::new();
    if let Some(source) = options.source.as_deref().filter(|s| !s.trim().is_empty()) {
        sql.push_str(" AND h.source = ?");
        values.push(source.to_string());
    }
    if let Some(session_id) = options
        .session_id
        .as_deref()
        .filter(|s| !s.trim().is_empty())
    {
        sql.push_str(" AND h.session_id = ?");
        values.push(session_id.to_string());
    }
    sql.push_str(
        " GROUP BY h.source, h.session_id \
         ORDER BY MAX(h.timestamp_ms) DESC LIMIT ?",
    );
    values.push(options.limit.max(1).to_string());
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(values.iter()), |row| {
        Ok(SessionCandidate {
            source: row.get(0)?,
            session_id: row.get(1)?,
            project: row.get(2)?,
            first_ms: row.get(3)?,
            last_ms: row.get(4)?,
            raw_path: row.get(5)?,
            last_assistant_text: row.get(6)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn build_session_transcript(
    conn: &Connection,
    candidate: SessionCandidate,
    max_chars: usize,
) -> Result<SessionTranscript> {
    let max_chars = max_chars.max(1_000);
    let mut parts = Vec::new();
    if let Some(path) = candidate
        .raw_path
        .as_deref()
        .filter(|p| !p.trim().is_empty())
    {
        if let Ok(raw) = extract_raw_session_text(Path::new(path), max_chars) {
            if !raw.trim().is_empty() {
                parts.push(format!(
                    "## Raw session transcript ({})\n{raw}",
                    candidate.source
                ));
            }
        }
    }

    if parts.is_empty() {
        let mut stmt = conn.prepare(
            "SELECT id, prompt, timestamp_ms FROM history \
             WHERE source = ? AND session_id = ? ORDER BY timestamp_ms ASC, id ASC",
        )?;
        let rows = stmt.query_map(params![candidate.source, candidate.session_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        let mut prompts = Vec::new();
        for row in rows {
            let (id, prompt, ts) = row?;
            prompts.push(format!(
                "### User prompt #{id} @ {}\n{}",
                iso(ts),
                normalize_home_path(&prompt)
            ));
        }
        if let Some(text) = candidate
            .last_assistant_text
            .as_deref()
            .filter(|s| !s.trim().is_empty())
        {
            prompts.push(format!(
                "### Last assistant response excerpt\n{}",
                normalize_home_path(text)
            ));
        }
        parts.push(prompts.join("\n\n"));
    }

    let mut text = parts.join("\n\n");
    if text.chars().count() > max_chars {
        text = text.chars().take(max_chars).collect();
    }
    let stable_hash = stable_hash(&format!(
        "{}\n{}\n{}\n{}",
        candidate.source, candidate.session_id, candidate.first_ms, text
    ));
    Ok(SessionTranscript {
        candidate,
        text,
        stable_hash,
    })
}

fn extract_raw_session_text(path: &Path, max_chars: usize) -> Result<String> {
    let raw = fs::read_to_string(path)?;
    let mut out = Vec::new();
    let mut used = 0usize;
    for line in raw.lines() {
        if used >= max_chars {
            break;
        }
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let role = value
            .get("type")
            .or_else(|| value.get("role"))
            .and_then(Value::as_str)
            .unwrap_or("event");
        let ts = value
            .get("timestamp")
            .and_then(|v| {
                v.as_str()
                    .map(str::to_string)
                    .or_else(|| v.as_i64().map(iso))
            })
            .unwrap_or_default();
        let mut text = Vec::new();
        collect_text_fields(&value, &mut text, 0);
        let text = text.join("\n").trim().to_string();
        if text.is_empty() {
            continue;
        }
        let chunk = format!("### {role} {ts}\n{}", normalize_home_path(&text));
        used += chunk.chars().count();
        out.push(chunk);
    }
    Ok(out.join("\n\n"))
}

fn collect_text_fields(value: &Value, out: &mut Vec<String>, depth: usize) {
    if depth > 8 || out.len() > 200 {
        return;
    }
    match value {
        Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.len() > 2 {
                out.push(trimmed.chars().take(4_000).collect());
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_text_fields(item, out, depth + 1);
            }
        }
        Value::Object(map) => {
            for key in [
                "message",
                "content",
                "text",
                "display",
                "result",
                "stdout",
                "stderr",
                "command",
                "input",
                "summary",
                "toolUseResult",
            ] {
                if let Some(v) = map.get(key) {
                    collect_text_fields(v, out, depth + 1);
                }
            }
        }
        _ => {}
    }
}

fn build_learn_prompt(transcript: &str, max_output_tokens: usize) -> Vec<(String, String)> {
    let user = [
        "Review the following serialized AI coding session and return a single JSON object.",
        "The JSON must match this schema exactly:",
        LEARN_OUTPUT_SCHEMA,
        "",
        "Requirements:",
        "- Output raw JSON only. Do not wrap it in markdown fences.",
        "- Prefer decisions, lessons, conventions, and actionable recommendations over generic summaries.",
        "- Keep `narrative` short; it is context only and may not be surfaced as a warning.",
        "- Use concrete file paths, symbols, commands, and implementation details where helpful.",
        "- If the transcript contains a secret or token, do not repeat it verbatim.",
        &format!(
            "- Keep the full response within approximately {max_output_tokens} tokens while preserving technical specificity."
        ),
        "",
        "Serialized session:",
        transcript.trim(),
    ]
    .join("\n");
    vec![
        ("system".to_string(), LEARN_SYSTEM_PROMPT.to_string()),
        ("user".to_string(), user),
    ]
}

fn estimate_surfaceable_events(compacted: &Value) -> usize {
    compacted
        .get("decisions")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0)
        + compacted
            .get("lessons")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0)
        + compacted
            .get("conventions")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0)
        + compacted
            .get("keyFindings")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0)
        + compacted
            .get("keyLearnings")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0)
}

fn upsert_learn_rollup(
    conn: &Connection,
    transcript: &SessionTranscript,
    compacted: &Value,
) -> Result<()> {
    let id = compacted
        .get("id")
        .and_then(Value::as_str)
        .context("learn rollup missing id")?;
    let retrospective_json = serde_json::to_string(compacted)?;
    let search_text = learn_search_text(compacted);
    let path = format!(
        "learn://{}/{}",
        transcript.candidate.source, transcript.candidate.session_id
    );
    conn.execute(
        "INSERT INTO trajectories \
         (id, version, persona_id, project_id, task_title, task_description, status, started_at, completed_at, decisions_json, retrospective_json, search_text, path, updated_ms, timestamp_ms) \
         VALUES (?, 1, ?, ?, ?, ?, 'completed', ?, ?, '[]', ?, ?, ?, ?, ?) \
         ON CONFLICT(id) DO UPDATE SET \
         project_id = excluded.project_id, task_title = excluded.task_title, task_description = excluded.task_description, \
         retrospective_json = excluded.retrospective_json, search_text = excluded.search_text, path = excluded.path, \
         updated_ms = excluded.updated_ms, timestamp_ms = excluded.timestamp_ms",
        params![
            id,
            transcript.candidate.source,
            transcript.candidate.project,
            format!("Learn distill: {}", transcript.candidate.session_id),
            format!("Distilled {} session history into Pair-surfaceable events", transcript.candidate.source),
            iso(transcript.candidate.first_ms),
            iso(transcript.candidate.last_ms),
            retrospective_json,
            search_text,
            path,
            Utc::now().timestamp_millis(),
            transcript.candidate.last_ms,
        ],
    )?;
    Ok(())
}

fn learn_search_text(compacted: &Value) -> String {
    let mut parts = Vec::new();
    collect_text_fields(compacted, &mut parts, 0);
    parts.join("\n")
}

fn stable_hash(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())[..16].to_string()
}

fn iso(ms: i64) -> String {
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| "1970-01-01T00:00:00+00:00".to_string())
}

pub fn provider_from_str(value: &str) -> Result<LearnProvider> {
    match value.to_ascii_lowercase().as_str() {
        "auto" => Ok(LearnProvider::Auto),
        "openai" => Ok(LearnProvider::OpenAi),
        "anthropic" => Ok(LearnProvider::Anthropic),
        other => bail!("invalid Learn provider '{other}' (choose auto, openai, anthropic)"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ai_hist::{init_db, insert_history, prompt_hash, HistoryEntry};
    use tempfile::tempdir;

    fn mem() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn
    }

    #[test]
    fn locality_gate_rejects_default_cloud_without_opt_in() {
        let options = LearnDistillOptions {
            source: None,
            session_id: None,
            limit: 1,
            max_chars: 1_000,
            max_output_tokens: 500,
            provider: LearnProvider::OpenAi,
            model: None,
            base_url: Some(DEFAULT_OPENAI_BASE_URL.to_string()),
            allow_cloud_llm: false,
            dry_run: true,
        };
        let err = resolve_provider(&options).unwrap_err().to_string();
        assert!(err.contains("refuses cloud LLM by default"));
    }

    #[test]
    fn raw_session_adapter_includes_tool_output_bait() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("s.jsonl");
        fs::write(
            &path,
            r#"{"type":"user","timestamp":"2026-06-22T10:00:00Z","message":{"content":"edit auth middleware"}}
{"type":"tool_result","timestamp":"2026-06-22T10:01:00Z","content":"deploy token ghp_FAKE0000000000000000000000000000abcd in env output"}
"#,
        )
        .unwrap();
        let text = extract_raw_session_text(&path, 10_000).unwrap();
        assert!(text.contains("edit auth middleware"));
        assert!(text.contains("ghp_FAKE0000000000000000000000000000abcd"));
    }

    #[test]
    fn learn_rollup_persists_distilled_evidence_without_raw_prompt() {
        let conn = mem();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/repo".into()),
                prompt: "Refactor auth middleware".into(),
                prompt_hash: Some(prompt_hash("Refactor auth middleware")),
                timestamp_ms: 1_782_036_000_000,
            },
        )
        .unwrap();
        let candidate = session_candidates(
            &conn,
            &LearnDistillOptions {
                source: Some("claude".into()),
                session_id: Some("s1".into()),
                limit: 1,
                max_chars: 10_000,
                max_output_tokens: 500,
                provider: LearnProvider::OpenAi,
                model: None,
                base_url: Some("http://localhost:11434".into()),
                allow_cloud_llm: false,
                dry_run: true,
            },
        )
        .unwrap()
        .remove(0);
        let transcript = build_session_transcript(&conn, candidate, 10_000).unwrap();
        let id = format!("learn_{}", transcript.stable_hash);
        let output = r#"{
          "narrative":"Generic summary should not become a Pair warning.",
          "decisions":[{"question":"How auth should validate tokens?","chosen":"Centralize middleware validation","reasoning":"Avoid route drift","impact":"Consistent auth"}],
          "conventions":[],
          "lessons":[{"context":"auth middleware","lesson":"Token rotation must accompany middleware edits","recommendation":"Rotate ghp_FAKE0000000000000000000000000000abcd before deploy"}],
          "keyFindings":["Auth middleware edits affect deploy credentials"],
          "keyLearnings":[],
          "openQuestions":[]
        }"#;
        let compacted = learn_rollup_from_output(&id, &transcript, output).unwrap();
        upsert_learn_rollup(&conn, &transcript, &compacted).unwrap();

        let stored: Vec<(String, String)> = conn
            .prepare("SELECT id, retrospective_json FROM trajectories ORDER BY rowid")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].0, id);
        assert!(stored[0]
            .1
            .contains("Token rotation must accompany middleware edits"));
        assert!(!serde_json::to_string(&compacted)
            .unwrap()
            .contains("Refactor auth middleware"));
    }
}
