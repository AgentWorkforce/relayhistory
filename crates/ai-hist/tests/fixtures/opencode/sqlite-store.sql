-- OpenCode's current on-disk layout: one SQLite store at
-- `~/.local/share/opencode/opencode.db` (see `crates/ai-hist/src/paths.rs`).
-- The harness executes this file into that path for the fixture's HOME.
CREATE TABLE session (
    id TEXT PRIMARY KEY,
    parent_id TEXT,
    directory TEXT,
    time_created INTEGER,
    time_updated INTEGER
);
CREATE TABLE message (
    id TEXT PRIMARY KEY,
    session_id TEXT,
    time_created INTEGER,
    data TEXT
);
CREATE TABLE part (
    id TEXT PRIMARY KEY,
    message_id TEXT,
    session_id TEXT,
    time_created INTEGER,
    data TEXT
);
CREATE INDEX session_time_updated_id_idx ON session (time_updated DESC, id);
CREATE INDEX message_session_time_created_id_idx ON message (session_id, time_created, id);
CREATE INDEX part_session_idx ON part (session_id);
CREATE INDEX part_message_id_id_idx ON part (message_id, id);

INSERT INTO session (id, parent_id, directory, time_created, time_updated)
VALUES ('ses_sqlite_root', NULL, '/tmp/project', 1776643200000, 1776643260000);

-- A child session carrying the provider's parent link, the SQLite equivalent
-- of the `ses_child` sidechain in the legacy JSON corpus. relayhistory's
-- OpenCode adapter does not read this column today (#168).
INSERT INTO session (id, parent_id, directory, time_created, time_updated)
VALUES ('ses_sqlite_child', 'ses_sqlite_root', '/tmp/project', 1776643230000, 1776643240000);

INSERT INTO message (id, session_id, time_created, data) VALUES
  ('msg_sqlite_u1', 'ses_sqlite_root', 1776643200000,
   '{"id":"msg_sqlite_u1","sessionID":"ses_sqlite_root","role":"user","time":{"created":1776643200000}}'),
  ('msg_sqlite_a1', 'ses_sqlite_root', 1776643210000,
   '{"id":"msg_sqlite_a1","sessionID":"ses_sqlite_root","role":"assistant","parentID":"msg_sqlite_u1","providerID":"anthropic","modelID":"claude-sonnet-4-5","time":{"created":1776643210000},"path":{"cwd":"/tmp/project","root":"/tmp/project"},"cost":0,"tokens":{"input":7,"output":120,"reasoning":0,"cache":{"read":0,"write":9000}}}'),
  ('msg_sqlite_u2', 'ses_sqlite_root', 1776643250000,
   '{"id":"msg_sqlite_u2","sessionID":"ses_sqlite_root","role":"user","time":{"created":1776643250000}}'),
  ('msg_sqlite_a2', 'ses_sqlite_root', 1776643260000,
   '{"id":"msg_sqlite_a2","sessionID":"ses_sqlite_root","role":"assistant","parentID":"msg_sqlite_u2","providerID":"anthropic","modelID":"claude-opus-4-5","time":{"created":1776643260000},"path":{"cwd":"/tmp/project","root":"/tmp/project"},"cost":0,"tokens":{"input":9,"output":210,"reasoning":40,"cache":{"read":9000,"write":1200}}}'),
  ('msg_sqlite_child_u1', 'ses_sqlite_child', 1776643230000,
   '{"id":"msg_sqlite_child_u1","sessionID":"ses_sqlite_child","role":"user","time":{"created":1776643230000}}'),
  ('msg_sqlite_child_a1', 'ses_sqlite_child', 1776643240000,
   '{"id":"msg_sqlite_child_a1","sessionID":"ses_sqlite_child","role":"assistant","parentID":"msg_sqlite_child_u1","providerID":"anthropic","modelID":"claude-haiku-4-5","time":{"created":1776643240000},"path":{"cwd":"/tmp/project","root":"/tmp/project"},"cost":0,"tokens":{"input":3,"output":50,"reasoning":0,"cache":{"read":0,"write":2000}}}');

INSERT INTO part (id, message_id, session_id, time_created, data) VALUES
  ('prt_sqlite_u1_text', 'msg_sqlite_u1', 'ses_sqlite_root', 1776643200000,
   '{"id":"prt_sqlite_u1_text","sessionID":"ses_sqlite_root","messageID":"msg_sqlite_u1","type":"text","text":"add a retry to the client"}'),
  ('prt_sqlite_a1_text', 'msg_sqlite_a1', 'ses_sqlite_root', 1776643210000,
   '{"id":"prt_sqlite_a1_text","sessionID":"ses_sqlite_root","messageID":"msg_sqlite_a1","type":"text","text":"Reading the client."}'),
  ('prt_sqlite_a1_tool', 'msg_sqlite_a1', 'ses_sqlite_root', 1776643215000,
   '{"id":"prt_sqlite_a1_tool","sessionID":"ses_sqlite_root","messageID":"msg_sqlite_a1","type":"tool","callID":"toolu_sqlite_edit","tool":"edit","state":{"status":"completed","input":{"filePath":"/tmp/project/src/client.ts"},"output":"edited 1 file","metadata":{"diff":"@@\n+retry\n"}}}'),
  ('prt_sqlite_a1_step', 'msg_sqlite_a1', 'ses_sqlite_root', 1776643218000,
   '{"id":"prt_sqlite_a1_step","sessionID":"ses_sqlite_root","messageID":"msg_sqlite_a1","type":"step-finish","reason":"end_turn","tokens":{"input":7,"output":120,"reasoning":0,"cache":{"read":0,"write":9000}}}'),
  ('prt_sqlite_u2_text', 'msg_sqlite_u2', 'ses_sqlite_root', 1776643250000,
   '{"id":"prt_sqlite_u2_text","sessionID":"ses_sqlite_root","messageID":"msg_sqlite_u2","type":"text","text":"now write the test"}'),
  ('prt_sqlite_a2_text', 'msg_sqlite_a2', 'ses_sqlite_root', 1776643260000,
   '{"id":"prt_sqlite_a2_text","sessionID":"ses_sqlite_root","messageID":"msg_sqlite_a2","type":"text","text":"Added the test."}'),
  ('prt_sqlite_child_u1_text', 'msg_sqlite_child_u1', 'ses_sqlite_child', 1776643230000,
   '{"id":"prt_sqlite_child_u1_text","sessionID":"ses_sqlite_child","messageID":"msg_sqlite_child_u1","type":"text","text":"review the retry change"}'),
  ('prt_sqlite_child_a1_text', 'msg_sqlite_child_a1', 'ses_sqlite_child', 1776643240000,
   '{"id":"prt_sqlite_child_a1_text","sessionID":"ses_sqlite_child","messageID":"msg_sqlite_child_a1","type":"text","text":"The retry looks correct."}');
