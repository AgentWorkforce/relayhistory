//! `cursor-cloud` hydration: one Cloud Agent's conversation as normalized
//! evidence, each turn timed by the v1 run it started.

use std::collections::HashSet;
use std::path::Path;

use ai_hist::{EvidenceKind, EvidenceRecord};

use anyhow::{Context, Result};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use super::cursor_cloud::{
    cursor_agent_id, cursor_auth, get_json, iso_ms, page, page_url, CursorAuth, CursorTransport,
    CURSOR_PAGE_LIMIT,
};
use super::{string_field, RemoteSessionEvidence, MAX_LIST_PAGES, MAX_REMOTE_EVIDENCE_BYTES};

/// One run's span, oldest first: the turn a user message opened.
#[derive(Clone, Copy)]
struct RunSpan {
    started_ms: i64,
    ended_ms: i64,
}

/// Every run of one agent, oldest first. The endpoint lists newest first.
fn agent_runs(
    transport: &dyn CursorTransport,
    auth: &CursorAuth,
    agent: &str,
    bytes: &mut usize,
    hasher: &mut Sha256,
) -> Result<Vec<RunSpan>> {
    let mut runs = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..MAX_LIST_PAGES {
        let url = page_url(
            &auth.base_url,
            &format!("/v1/agents/{agent}/runs"),
            CURSOR_PAGE_LIMIT,
            cursor.as_deref(),
        );
        let (payload, len) = get_json(transport, auth, &url, "Cursor agent runs")?;
        *bytes = bytes.saturating_add(len);
        hasher.update(payload.to_string().as_bytes());
        let (items, next) = page(&payload, "agent runs")?;
        runs.extend(items.iter().filter_map(|run| {
            let started_ms = iso_ms(run, "createdAt")?;
            Some(RunSpan {
                started_ms,
                ended_ms: iso_ms(run, "updatedAt")
                    .unwrap_or(started_ms)
                    .max(started_ms),
            })
        }));
        cursor = next;
        if cursor.is_none() || items.is_empty() {
            runs.sort_by_key(|run| run.started_ms);
            return Ok(runs);
        }
    }
    anyhow::bail!("EVIDENCE_PARTIAL: Cursor agent runs exceeded the 100-page bound")
}

/// Assigns each conversation message the timestamp of the run its turn
/// belongs to, never earlier than the message before it.
struct TurnClock {
    runs: Vec<RunSpan>,
    agent_created_ms: i64,
    agent_updated_ms: i64,
    turn: Option<usize>,
    last_ms: i64,
}

impl TurnClock {
    fn user(&mut self) -> i64 {
        let turn = self.turn.map_or(0, |turn| turn + 1);
        self.turn = Some(turn);
        let at = self
            .runs
            .get(turn)
            .map(|run| run.started_ms)
            .unwrap_or(self.last_ms.max(self.agent_created_ms));
        self.advance(at)
    }

    fn assistant(&mut self) -> i64 {
        let at = match self.turn.and_then(|turn| self.runs.get(turn)) {
            Some(run) => run.ended_ms,
            None if self.runs.is_empty() => self.agent_updated_ms,
            None => self.last_ms,
        };
        self.advance(at)
    }

    fn advance(&mut self, at: i64) -> i64 {
        self.last_ms = self.last_ms.max(at);
        self.last_ms
    }
}

fn message_records(
    session_id: &str,
    messages: &[Value],
    clock: &mut TurnClock,
) -> Vec<EvidenceRecord> {
    let mut records = Vec::new();
    let mut seen_prompts = HashSet::new();
    for message in messages {
        let role = match string_field(message, "type").as_deref() {
            Some("user_message") => "user",
            Some("assistant_message") => "assistant",
            _ => continue,
        };
        let ts_ms = if role == "user" {
            clock.user()
        } else {
            clock.assistant()
        };
        let Some(id) = string_field(message, "id") else {
            continue;
        };
        let Some(text) = message
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
        else {
            continue;
        };
        let mut event = Map::new();
        event.insert("source".into(), json!("cursor"));
        event.insert("session_id".into(), json!(session_id));
        event.insert("ts_ms".into(), json!(ts_ms));
        event.insert("role".into(), json!(role));
        event.insert("kind".into(), json!("text"));
        event.insert("text".into(), json!(text));
        event.insert("message_id".into(), json!(id));
        event.insert("event_uid".into(), json!(id));
        records.push(EvidenceRecord {
            kind: EvidenceKind::SessionEvent,
            payload: event,
            record_id: None,
            revision_id: None,
        });
        if role == "user" && seen_prompts.insert((ts_ms, text.to_string())) {
            records.push(EvidenceRecord {
                kind: EvidenceKind::History,
                payload: Map::from_iter([
                    ("source".into(), json!("cursor")),
                    ("session_id".into(), json!(session_id)),
                    ("prompt".into(), json!(text)),
                    ("timestamp_ms".into(), json!(ts_ms)),
                ]),
                record_id: None,
                revision_id: None,
            });
        }
    }
    records
}

/// Hydrate one Cloud Agent into a normalized evidence snapshot.
pub(super) fn acquire_cursor_remote_session_at(
    key_path: &Path,
    session_id: &str,
    transport: &dyn CursorTransport,
) -> Result<RemoteSessionEvidence> {
    let agent = cursor_agent_id(session_id)
        .context("INVALID_ARGUMENT: remote Cursor agent id is malformed")?;
    let Ok(auth) = cursor_auth(key_path) else {
        return Ok(RemoteSessionEvidence::CapabilityLimited {
            code: "CONNECTOR_NOT_CONFIGURED",
            message: "cursor-cloud is not configured; set RELAYHISTORY_CURSOR_API_KEY or CURSOR_API_KEY, or write a Cursor API key to the cursor-cloud key file".to_string(),
        });
    };
    let mut bytes = 0usize;
    let mut hasher = Sha256::new();
    let (record, len) = get_json(
        transport,
        &auth,
        &format!("{}/v1/agents/{agent}", auth.base_url),
        "Cursor agent",
    )?;
    bytes += len;
    let runs = agent_runs(transport, &auth, agent, &mut bytes, &mut hasher)?;
    let (conversation, len) = get_json(
        transport,
        &auth,
        &format!("{}/v0/agents/{agent}/conversation", auth.base_url),
        "Cursor agent conversation",
    )?;
    bytes = bytes.saturating_add(len);
    anyhow::ensure!(
        bytes <= MAX_REMOTE_EVIDENCE_BYTES,
        "CONNECTOR_FAILURE: remote Cursor evidence exceeded the 16 MiB response-size limit"
    );
    let messages = conversation
        .get("messages")
        .and_then(Value::as_array)
        .context("CONNECTOR_FAILURE: Cursor agent conversation has no messages array")?;
    hasher.update(conversation.to_string().as_bytes());
    let agent_created_ms = iso_ms(&record, "createdAt")
        .or_else(|| runs.first().map(|run| run.started_ms))
        .context("CONNECTOR_FAILURE: Cursor agent has no createdAt")?;
    let mut clock = TurnClock {
        agent_updated_ms: iso_ms(&record, "updatedAt")
            .unwrap_or(agent_created_ms)
            .max(agent_created_ms),
        runs,
        agent_created_ms,
        turn: None,
        last_ms: agent_created_ms,
    };
    Ok(RemoteSessionEvidence::Normalized(
        ai_hist::sources::NormalizedSourceEvidence {
            source_stamp: format!("conversation:{:x}", hasher.finalize()),
            source_bytes: bytes as i64,
            covered_kinds: vec![EvidenceKind::History, EvidenceKind::SessionEvent],
            records: message_records(session_id, messages, &mut clock),
        },
    ))
}
