CREATE TABLE IF NOT EXISTS sessions.conversation_turns (
  id UUID PRIMARY KEY,
  org_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  session_owner TEXT NOT NULL,
  turn_index INTEGER NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
  content TEXT NOT NULL,
  actor_name TEXT NOT NULL,
  actor_role TEXT NOT NULL CHECK (actor_role IN ('owner', 'steerer')),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  ts TIMESTAMPTZ NOT NULL,
  UNIQUE (org_id, session_id, turn_index)
);

CREATE INDEX IF NOT EXISTS conversation_turns_org_session_ts_idx
  ON sessions.conversation_turns(org_id, session_id, ts);
