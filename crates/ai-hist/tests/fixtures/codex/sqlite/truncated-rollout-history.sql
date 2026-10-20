CREATE TABLE thread_history_projection_state(thread_id TEXT PRIMARY KEY,next_rollout_ordinal INTEGER,next_rollout_byte_offset INTEGER);
CREATE TABLE thread_items(thread_id TEXT,turn_id TEXT,item_id TEXT,rollout_ordinal INTEGER,created_at_ms INTEGER,item_type TEXT,item_json TEXT);
CREATE INDEX items_by_thread_ordinal ON thread_items(thread_id,rollout_ordinal);
CREATE TABLE thread_turns(thread_id TEXT,turn_id TEXT,rollout_ordinal INTEGER,started_at INTEGER,error_json TEXT);
INSERT INTO thread_history_projection_state VALUES ('sqlite-truncated-rollout',20,999999);
INSERT INTO thread_items VALUES ('sqlite-truncated-rollout','turn-1','sqlite-sqlite-truncated-rollout-0',0,1776643200000,'userMessage','{"type":"userMessage","id":"sqlite-sqlite-truncated-rollout-0","content":[{"type":"text","text":"Please inspect the project"}]}');
INSERT INTO thread_items VALUES ('sqlite-truncated-rollout','turn-1','sqlite-sqlite-truncated-rollout-1',1,1776643201000,'agentMessage','{"type":"agentMessage","id":"sqlite-sqlite-truncated-rollout-1","text":"I inspected the project and fixed it.","phase":"final_answer"}');
INSERT INTO thread_turns VALUES ('sqlite-truncated-rollout','turn-1',0,1776643200000,NULL);
