-- Sanitized Devin `sessions.db` exercising the adapter's edge cases:
-- malformed `chat_message` JSON skipped per record, a tool call still
-- running (no completion update), a `tool_call_state` row no message node
-- references, and a `hidden` session that must never be indexed.

CREATE TABLE sessions (
  id TEXT PRIMARY KEY,
  working_directory TEXT NOT NULL,
  backend_type TEXT NOT NULL,
  model TEXT NOT NULL,
  agent_mode TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  last_activity_at INTEGER NOT NULL,
  title TEXT,
  main_chain_id INTEGER,
  shell_last_seen_index INTEGER DEFAULT 0,
  cogs_json TEXT,
  workspace_dirs TEXT,
  hidden INTEGER NOT NULL DEFAULT 0,
  metadata TEXT
);

CREATE TABLE message_nodes (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  node_id INTEGER NOT NULL,
  parent_node_id INTEGER,
  chat_message TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  metadata TEXT,
  FOREIGN KEY (session_id) REFERENCES sessions(id),
  UNIQUE(session_id, node_id)
);

CREATE TABLE tool_call_state (
  session_id TEXT NOT NULL,
  tool_call_id TEXT NOT NULL,
  tool_call_json TEXT,
  tool_call_update_json TEXT,
  PRIMARY KEY (session_id, tool_call_id),
  FOREIGN KEY (session_id) REFERENCES sessions(id)
);

INSERT INTO sessions
  (id, working_directory, backend_type, model, agent_mode, created_at,
   last_activity_at, title, workspace_dirs, hidden, metadata)
VALUES
  ('fixture-devin-partial', '/work/fixture-repo', 'devin', 'fixture-model-1',
   'normal', 1776643300, 1776643310, NULL, '[]', 0, NULL),
  ('fixture-devin-hidden', '/work/fixture-repo', 'devin', 'fixture-model-1',
   'normal', 1776643300, 1776643305, 'Hidden fixture session', '[]', 1, NULL);

INSERT INTO message_nodes
  (session_id, node_id, parent_node_id, chat_message, created_at, metadata)
VALUES
  ('fixture-devin-partial', 0, NULL,
   'not-json{{{',
   1776643300, NULL),
  ('fixture-devin-partial', 1, 0,
   '{"message_id":"p1","role":"user","content":"Fixture partial session prompt.","metadata":{"is_user_input":true},"tool_calls":null,"thinking":null,"tool_call_id":null,"phase":null}',
   1776643301, NULL),
  ('fixture-devin-partial', 2, 1,
   '{"message_id":"p2","role":"assistant","content":null,"thinking":null,"metadata":{"request_id":"req-p1","generation_model":"fixture-model-1"},"tool_calls":[{"id":"call-running-1","kind":"function","name":"execute","arguments":{"command":"fixture-run"},"index":0}],"tool_call_id":null,"phase":null}',
   1776643302, NULL),
  ('fixture-devin-hidden', 0, NULL,
   '{"message_id":"h0","role":"user","content":"Hidden fixture prompt.","metadata":{"is_user_input":true},"tool_calls":null,"thinking":null,"tool_call_id":null,"phase":null}',
   1776643301, NULL);

INSERT INTO tool_call_state
  (session_id, tool_call_id, tool_call_json, tool_call_update_json)
VALUES
  ('fixture-devin-partial', 'call-running-1',
   '{"toolCallId":"call-running-1","title":"execute","kind":"execute","rawInput":{"command":"fixture-run"},"locations":[],"content":[],"_meta":null}',
   NULL),
  ('fixture-devin-partial', 'call-detached-1',
   '{"toolCallId":"call-detached-1","title":"edit","kind":"edit","rawInput":{"file_path":"/work/fixture-repo/detached.rs"},"locations":[{"path":"/work/fixture-repo/detached.rs"}],"content":[],"_meta":null}',
   '{"toolCallId":"call-detached-1","status":"completed","content":[],"_meta":null}'),
  ('fixture-devin-hidden', 'call-hidden-1',
   '{"toolCallId":"call-hidden-1","title":"execute","kind":"execute","rawInput":{"command":"fixture-hidden"},"locations":[],"content":[],"_meta":null}',
   NULL);
