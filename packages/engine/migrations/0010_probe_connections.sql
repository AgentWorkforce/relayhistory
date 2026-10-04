CREATE TABLE sessions.probe_connections (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  last_seen_at timestamptz NOT NULL,
  PRIMARY KEY (org_id, workspace_id, user_id)
);
