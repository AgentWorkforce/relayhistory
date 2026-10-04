// Normalized delivery fixtures: what a machine's uploader sends, built with the same
// identity rules as `ai-hist export` (record_id = SHA-256 of the change-feed key,
// revision_id = SHA-256 of "origin:record:revision"; origin = the store's epoch).
import { createHash } from "node:crypto";

const sha256 = (value) => createHash("sha256").update(value).digest("hex");

function record(originId, kind, key, revision, sessionId, payload) {
  const recordId = sha256(JSON.stringify(key));
  return {
    schema_version: 1,
    origin_id: originId,
    record_id: recordId,
    revision_id: sha256(`${originId}:${recordId}:${revision}`),
    revision,
    kind,
    source: "claude",
    session_id: sessionId,
    operation: payload === null ? "delete" : "upsert",
    payload,
  };
}

function event(sessionId, index, role, text, tsMs, project) {
  return {
    agent_id: null,
    agent_version: "1.0.0",
    call_index: null,
    cwd: project,
    error_signal: "none",
    event_index: index,
    event_source: "provider",
    event_uid: `${sessionId}-message-${index}:0`,
    git_branch: "main",
    id: index + 1,
    is_meta: 0,
    is_sidechain: 0,
    kind: "text",
    message_id: `${sessionId}-message-${index}`,
    model: role === "assistant" ? "claude-fixture" : null,
    parent_id: index === 0 ? null : `${sessionId}-message-${index - 1}`,
    payload_bytes: text.length,
    payload_hash: sha256(text).slice(0, 16),
    payload_truncated: 0,
    project,
    project_key: "example/selfhost",
    project_key_method: "git_remote",
    provider: "anthropic",
    provider_message_id: `${sessionId}-provider-${index}`,
    raw_facts_version: 2,
    raw_kind: role,
    request_id: null,
    request_span: null,
    result_status: null,
    role,
    session_id: sessionId,
    source: "claude",
    stop_reason: role === "assistant" ? "end_turn" : null,
    subagent_session_id: null,
    text,
    token_json: null,
    tool_use_id: null,
    ts_ms: tsMs,
    turn_id: `${sessionId}-turn`,
  };
}

/**
 * One machine's session: its catalog row, a prompt, and a three-message transcript
 * whose texts are `lines`. `startMs` orders sessions across machines.
 */
export function sessionRecords({
  originId,
  sessionId,
  lines,
  startMs,
  project,
}) {
  const records = [];
  let revision = 1;
  const last = startMs + (lines.length - 1) * 1_000;
  records.push(
    record(
      originId,
      "session",
      ["session", "claude", sessionId],
      revision++,
      sessionId,
      {
        agent_version: "1.0.0",
        cwd: project,
        discovery_state: "full",
        first_activity_ms: startMs,
        first_prompt: lines[0],
        git_branch: "main",
        initial_commit: null,
        last_activity_ms: last,
        last_assistant_text: lines[1] ?? null,
        models_json: '["claude-fixture"]',
        originator: null,
        parser_version: 1,
        project_key: "example/selfhost",
        project_key_method: "git_remote",
        raw_path: `${project}/.claude/${sessionId}.jsonl`,
        repo_url: null,
        session_id: sessionId,
        source: "claude",
        source_stamp: null,
        workspace_roots_json: null,
      },
    ),
  );
  records.push(
    record(
      originId,
      "history",
      ["history", "claude", startMs, lines[0]],
      revision++,
      sessionId,
      {
        git_branch: "main",
        id: 1,
        project,
        prompt: lines[0],
        prompt_hash: sha256(lines[0]).slice(0, 16),
        session_id: sessionId,
        source: "claude",
        timestamp_ms: startMs,
      },
    ),
  );
  lines.forEach((text, index) => {
    const role = index % 2 === 0 ? "user" : "assistant";
    records.push(
      record(
        originId,
        "session_event",
        [
          "session_event",
          "claude",
          sessionId,
          `${sessionId}-message-${index}:0`,
        ],
        revision++,
        sessionId,
        event(sessionId, index, role, text, startMs + index * 1_000, project),
      ),
    );
  });
  return records;
}

/** A tombstone for one transcript message at a higher revision. */
export function eventTombstone({ originId, sessionId, index, revision }) {
  return record(
    originId,
    "session_event",
    ["session_event", "claude", sessionId, `${sessionId}-message-${index}:0`],
    revision,
    sessionId,
    null,
  );
}

export function batch({ originId, accountId, batchId, records, instanceId }) {
  return {
    protocolVersion: 1,
    batch: {
      schema_version: 1,
      origin_id: originId,
      batch_id: batchId,
      job_id: `smoke-${originId}`,
      generation: 1,
      destination_id: "relayhistory",
      instance_id: instanceId,
      account_id: accountId,
      mapping_version: "relayhistory-delivery-v1",
      records,
    },
  };
}
