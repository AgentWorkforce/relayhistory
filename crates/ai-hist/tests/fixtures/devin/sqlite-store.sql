-- Sanitized Devin CLI `sessions.db` for the fixture corpus.
--
-- Mirrors the schema `devin` (3000.x) writes at
-- `$XDG_DATA_HOME/devin/cli/sessions.db`. All content is invented for the
-- fixture; no live session data appears here. Timestamps are epoch seconds,
-- as the provider records them.

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
  ('fixture-devin-session', '/work/fixture-repo', 'devin', 'fixture-model-1',
   'normal', 1776643200, 1776643212, 'Fixture session: rename helper',
   '["/work/fixture-repo", "/work/fixture-extras"]', 0,
   '{"total_credit_cost": 0.5}');

INSERT INTO message_nodes
  (session_id, node_id, parent_node_id, chat_message, created_at, metadata)
VALUES
  ('fixture-devin-session', 0, NULL,
   '{"message_id":"m0","role":"system","content":"You are a fixture agent.","metadata":null,"tool_calls":null,"thinking":null,"tool_call_id":null,"phase":null}',
   1776643200, '{"is_system_prefix": true}'),
  ('fixture-devin-session', 1, 0,
   '{"message_id":"m1","role":"user","content":"Rename the helper in the fixture repo.","metadata":{"is_user_input":true},"tool_calls":null,"thinking":null,"tool_call_id":null,"phase":null}',
   1776643201, NULL),
  ('fixture-devin-session', 2, 1,
   '{"message_id":"m2","role":"assistant","content":"I will look at the fixture sources first.","thinking":"Reading the layout.","metadata":{"num_tokens":64,"request_id":"req-1","generation_model":"fixture-model-1"},"tool_calls":[{"id":"call-read-1","kind":"function","name":"read","arguments":{"path":"/work/fixture-repo/README.md"},"index":0}],"tool_call_id":null,"phase":null}',
   1776643202, NULL),
  ('fixture-devin-session', 3, 2,
   '{"message_id":"m3","role":"tool","content":"fixture readme body","metadata":null,"tool_calls":null,"thinking":null,"tool_call_id":"call-read-1","phase":null}',
   1776643203, NULL),
  ('fixture-devin-session', 4, 3,
   '{"message_id":"m4","role":"assistant","content":"Applying the rename now.","thinking":null,"metadata":{"num_tokens":32,"request_id":"req-2","generation_model":"fixture-model-1","finish_reason":"tool_calls"},"tool_calls":[{"id":"call-edit-1","kind":"function","name":"edit","arguments":{"file_path":"/work/fixture-repo/src/lib.rs"},"index":0}],"tool_call_id":null,"phase":null}',
   1776643204, '{"summarized_from": [0]}'),
  ('fixture-devin-session', 5, 4,
   '{"message_id":"m5","role":"tool","content":"edit failed: fixture conflict","metadata":null,"tool_calls":null,"thinking":null,"tool_call_id":"call-edit-1","phase":null}',
   1776643205, NULL),
  ('fixture-devin-session', 6, 5,
   '{"message_id":"m6","role":"user","content":"Fixture synthetic nudge.","metadata":{"is_user_input":false},"tool_calls":null,"thinking":null,"tool_call_id":null,"phase":null}',
   1776643206, NULL),
  ('fixture-devin-session', 7, 6,
   '{"message_id":"m7","role":"final_answer","content":"Done with the fixture rename.","thinking":null,"metadata":{"num_tokens":16,"request_id":"req-3","generation_model":"fixture-model-1","finish_reason":"stop"},"tool_calls":null,"tool_call_id":null,"phase":null}',
   1776643207, NULL);

INSERT INTO tool_call_state
  (session_id, tool_call_id, tool_call_json, tool_call_update_json)
VALUES
  ('fixture-devin-session', 'call-read-1',
   '{"toolCallId":"call-read-1","title":"read","kind":"read","rawInput":{"path":"/work/fixture-repo/README.md"},"locations":[{"path":"/work/fixture-repo/README.md"}],"content":[],"_meta":null}',
   '{"toolCallId":"call-read-1","status":"completed","content":[{"type":"content","content":{"type":"text","text":"fixture readme body"}}],"_meta":null}'),
  ('fixture-devin-session', 'call-edit-1',
   '{"toolCallId":"call-edit-1","title":"edit","kind":"edit","rawInput":{"file_path":"/work/fixture-repo/src/lib.rs"},"locations":[{"path":"/work/fixture-repo/src/lib.rs"}],"content":[{"type":"diff","path":"/work/fixture-repo/src/lib.rs"}],"_meta":null}',
   '{"toolCallId":"call-edit-1","status":"failed","content":[],"_meta":null}'),
  ('fixture-devin-session', 'call-orphan-1',
   '{"toolCallId":"call-orphan-1","title":"execute","kind":"execute","rawInput":{"command":"fixture-ls"},"locations":[],"content":[],"_meta":null}',
   '{"toolCallId":"call-orphan-1","status":"completed","content":[],"_meta":null}');
