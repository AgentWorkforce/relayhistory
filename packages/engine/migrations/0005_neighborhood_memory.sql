-- Neighborhood memory derived tier + ingest embedding provenance.
-- See docs/specs/2026-09-02-neighborhood-memory.md §4.
-- Does not modify the convergence_events natural key or raw envelope.

-- Provenance columns beside convergence_events.embedding (§4.4).
ALTER TABLE sessions.convergence_events
  ADD COLUMN IF NOT EXISTS embedding_model TEXT,
  ADD COLUMN IF NOT EXISTS embedding_dim INTEGER,
  ADD COLUMN IF NOT EXISTS content_hash TEXT,
  ADD COLUMN IF NOT EXISTS generated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS embedding_skip_reason TEXT;

-- 1. project_aliases: (org_id, alias) → canonical_project, kind ∈ self | sibling | contract.
CREATE TABLE IF NOT EXISTS sessions.project_aliases (
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  alias TEXT NOT NULL,
  canonical_project TEXT NOT NULL,
  kind TEXT NOT NULL,
  version TEXT,
  approved_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, alias)
);

CREATE INDEX IF NOT EXISTS project_aliases_org_canonical_idx
  ON sessions.project_aliases(org_id, canonical_project);

CREATE INDEX IF NOT EXISTS project_aliases_org_kind_idx
  ON sessions.project_aliases(org_id, kind);

-- 2. neighborhood_claims: bi-temporal, content-addressed, cited semantic facts.
CREATE TABLE IF NOT EXISTS sessions.neighborhood_claims (
  id TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  source_project TEXT,
  subject_project TEXT NOT NULL,
  kind TEXT NOT NULL,
  statement TEXT NOT NULL,
  body TEXT,
  impact_on_subject TEXT NOT NULL,
  entities JSONB NOT NULL DEFAULT '[]'::jsonb,
  evidence_class TEXT NOT NULL,
  confidence DOUBLE PRECISION,
  status TEXT NOT NULL DEFAULT 'candidate',
  valid_from TIMESTAMPTZ,
  invalid_at TIMESTAMPTZ,
  observed_from TIMESTAMPTZ,
  last_confirmed_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  supersedes_claim_id TEXT,
  support_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  extractor_version TEXT,
  profile_version TEXT,
  embedding public.vector(1536),
  embedding_model TEXT,
  embedding_dim INTEGER,
  content_hash TEXT,
  generated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS neighborhood_claims_org_subject_status_idx
  ON sessions.neighborhood_claims(org_id, subject_project, status);

CREATE INDEX IF NOT EXISTS neighborhood_claims_org_kind_idx
  ON sessions.neighborhood_claims(org_id, kind);

CREATE INDEX IF NOT EXISTS neighborhood_claims_org_source_idx
  ON sessions.neighborhood_claims(org_id, source_project);

CREATE INDEX IF NOT EXISTS neighborhood_claims_embedding_hnsw_idx
  ON sessions.neighborhood_claims USING hnsw (embedding public.vector_cosine_ops)
  WHERE status IN ('active', 'contested');

-- 3. neighborhood_edges: 1-hop relations only in this slice.
CREATE TABLE IF NOT EXISTS sessions.neighborhood_edges (
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  src_project TEXT NOT NULL,
  dst_project TEXT NOT NULL,
  relation TEXT NOT NULL,
  claim_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, src_project, dst_project, relation, claim_id)
);

CREATE INDEX IF NOT EXISTS neighborhood_edges_org_src_idx
  ON sessions.neighborhood_edges(org_id, src_project);

CREATE INDEX IF NOT EXISTS neighborhood_edges_org_dst_idx
  ON sessions.neighborhood_edges(org_id, dst_project);

CREATE INDEX IF NOT EXISTS neighborhood_edges_org_claim_idx
  ON sessions.neighborhood_edges(org_id, claim_id);

-- 4. memory_jobs: durable extraction/consolidation work. The table is truth.
CREATE TABLE IF NOT EXISTS sessions.memory_jobs (
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  machine_id TEXT NOT NULL,
  source TEXT NOT NULL,
  session_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  event_id TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  profile_version TEXT NOT NULL,
  extractor_version TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  completion_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  PRIMARY KEY (
    org_id,
    machine_id,
    source,
    session_id,
    kind,
    event_id,
    profile_version,
    extractor_version,
    content_hash
  )
);

CREATE INDEX IF NOT EXISTS memory_jobs_org_state_idx
  ON sessions.memory_jobs(org_id, state);

CREATE INDEX IF NOT EXISTS memory_jobs_org_session_idx
  ON sessions.memory_jobs(org_id, session_id);

-- 5. memory_retrieval_log: pack candidates, scores, digest, truncation, later feedback.
CREATE TABLE IF NOT EXISTS sessions.memory_retrieval_log (
  id TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  user_id TEXT,
  subject_project TEXT NOT NULL,
  task TEXT,
  files JSONB NOT NULL DEFAULT '[]'::jsonb,
  budget_tokens INTEGER,
  scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
  as_of TIMESTAMPTZ,
  candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
  scores JSONB NOT NULL DEFAULT '[]'::jsonb,
  selected_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  token_count INTEGER,
  pack_digest TEXT,
  memory_truncated INTEGER NOT NULL DEFAULT 0,
  feedback JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS memory_retrieval_log_org_subject_idx
  ON sessions.memory_retrieval_log(org_id, subject_project, created_at);

CREATE INDEX IF NOT EXISTS memory_retrieval_log_org_digest_idx
  ON sessions.memory_retrieval_log(org_id, pack_digest);
