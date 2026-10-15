CREATE TABLE thread_history_projection_state(thread_id TEXT PRIMARY KEY,next_rollout_ordinal INTEGER,next_rollout_byte_offset INTEGER);
CREATE TABLE thread_items(thread_id TEXT,turn_id TEXT,item_id TEXT,rollout_ordinal INTEGER,created_at_ms INTEGER,item_type TEXT,item_json TEXT);
CREATE INDEX items_by_thread_ordinal ON thread_items(thread_id,rollout_ordinal);
CREATE TABLE thread_turns(thread_id TEXT,turn_id TEXT,rollout_ordinal INTEGER,started_at INTEGER,error_json TEXT);
INSERT INTO thread_history_projection_state VALUES ('sqlite-legacy-no-items',20,0);
