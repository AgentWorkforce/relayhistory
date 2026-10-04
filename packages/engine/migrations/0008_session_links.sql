-- Polymorphic lifecycle links. Tenancy is assigned by authenticated server writers.
CREATE TABLE IF NOT EXISTS sessions.session_links (
  id BIGSERIAL PRIMARY KEY,
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  source TEXT NOT NULL,
  session_id TEXT NOT NULL,
  link_kind TEXT NOT NULL,
  link_ref TEXT NOT NULL,
  link_url TEXT,
  link_ts TIMESTAMPTZ,
  metadata JSONB,
  provenance_lens TEXT NOT NULL,
  confidence_basis_points INT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (org_id, source, session_id, link_kind, link_ref)
);

CREATE INDEX IF NOT EXISTS session_links_session_idx
  ON sessions.session_links (org_id, source, session_id, link_ts DESC);
CREATE INDEX IF NOT EXISTS session_links_ref_idx
  ON sessions.session_links (org_id, link_kind, link_ref);
