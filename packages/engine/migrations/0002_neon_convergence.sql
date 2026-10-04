CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;

CREATE TABLE IF NOT EXISTS sessions.machines (
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  machine_id TEXT NOT NULL,
  hostname TEXT,
  label TEXT,
  os TEXT,
  relayhistory_version TEXT,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  cursors_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (org_id, machine_id)
);

CREATE TABLE IF NOT EXISTS sessions.convergence_events (
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  machine_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  source TEXT NOT NULL,
  lens TEXT,
  session_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  type TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  actor_name TEXT,
  actor_role TEXT,
  subagent_id TEXT,
  trajectory_id TEXT,
  chapter_id TEXT,
  project_id TEXT,
  workflow_id TEXT,
  task_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_title TEXT,
  task_description TEXT,
  task_status TEXT,
  content TEXT,
  significance TEXT,
  confidence_basis_points INTEGER,
  tags JSONB NOT NULL DEFAULT '[]'::jsonb,
  model TEXT,
  provider TEXT,
  input_tokens BIGINT NOT NULL DEFAULT 0,
  output_tokens BIGINT NOT NULL DEFAULT 0,
  reasoning_tokens BIGINT NOT NULL DEFAULT 0,
  cache_read_tokens BIGINT NOT NULL DEFAULT 0,
  cache_create_tokens BIGINT NOT NULL DEFAULT 0,
  cost_usd_micros BIGINT NOT NULL DEFAULT 0,
  tool_name TEXT,
  tool_status TEXT,
  tool_calls JSONB NOT NULL DEFAULT '[]'::jsonb,
  retries INTEGER NOT NULL DEFAULT 0,
  duration_ms INTEGER,
  files_touched JSONB NOT NULL DEFAULT '[]'::jsonb,
  code_churn JSONB NOT NULL DEFAULT '{}'::jsonb,
  embedding public.vector(1536),
  record JSONB NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, machine_id, source, session_id, kind, event_id)
);

CREATE INDEX IF NOT EXISTS convergence_events_org_ts_idx
  ON sessions.convergence_events(org_id, ts);

CREATE INDEX IF NOT EXISTS convergence_events_org_source_ts_idx
  ON sessions.convergence_events(org_id, source, ts);

CREATE INDEX IF NOT EXISTS convergence_events_org_lens_ts_idx
  ON sessions.convergence_events(org_id, lens, ts);

CREATE INDEX IF NOT EXISTS convergence_events_org_type_ts_idx
  ON sessions.convergence_events(org_id, type, ts);

CREATE INDEX IF NOT EXISTS convergence_events_org_session_idx
  ON sessions.convergence_events(org_id, session_id);

CREATE INDEX IF NOT EXISTS convergence_events_org_project_ts_idx
  ON sessions.convergence_events(org_id, project_id, ts);

CREATE INDEX IF NOT EXISTS convergence_events_org_task_status_ts_idx
  ON sessions.convergence_events(org_id, task_status, ts);

CREATE INDEX IF NOT EXISTS convergence_events_embedding_hnsw_idx
  ON sessions.convergence_events USING hnsw (embedding public.vector_cosine_ops)
  WHERE embedding IS NOT NULL;

CREATE TABLE IF NOT EXISTS sessions.sync_batches (
  id TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  machine_id TEXT NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  record_count INTEGER NOT NULL DEFAULT 0,
  accepted_count INTEGER NOT NULL DEFAULT 0,
  cursors_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (org_id, machine_id, id)
);

CREATE INDEX IF NOT EXISTS sync_batches_org_received_idx
  ON sessions.sync_batches(org_id, received_at);

CREATE TABLE IF NOT EXISTS sessions.auth_sessions (
  id TEXT PRIMARY KEY,
  token_family_id TEXT NOT NULL,
  subject_type TEXT NOT NULL DEFAULT 'cli',
  user_id TEXT NOT NULL,
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
  access_token_hash TEXT NOT NULL UNIQUE,
  access_token_expires_at TIMESTAMPTZ NOT NULL,
  refresh_token_hash TEXT NOT NULL UNIQUE,
  refresh_token_expires_at TIMESTAMPTZ NOT NULL,
  label TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at TIMESTAMPTZ,
  last_refreshed_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  revoked_reason TEXT
);

CREATE INDEX IF NOT EXISTS auth_sessions_family_idx
  ON sessions.auth_sessions(token_family_id);

CREATE INDEX IF NOT EXISTS auth_sessions_org_idx
  ON sessions.auth_sessions(org_id);
