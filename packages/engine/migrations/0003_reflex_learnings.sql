-- Reflex derived layer: outcomes (intent -> prod) + learnings (individual + team).
-- See docs/decisions/2026-06-27-reflex-learnings-and-outcomes-layer.md
-- Does not modify convergence_events (the raw, lens-discriminated sink).

-- 1. session_outcomes: session -> commit linkage + ship/revert (mistake) signal.
CREATE TABLE IF NOT EXISTS sessions.session_outcomes (
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  machine_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  source TEXT NOT NULL,
  session_id TEXT NOT NULL,
  repo TEXT,
  branch TEXT,
  commit_sha TEXT NOT NULL,
  match_method TEXT,
  confidence_basis_points INTEGER,
  numstat_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  files_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  shipped_at TIMESTAMPTZ,
  reverted BOOLEAN NOT NULL DEFAULT false,
  reverted_by_sha TEXT,
  reverted_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, source, session_id, commit_sha)
);

CREATE INDEX IF NOT EXISTS session_outcomes_org_session_idx
  ON sessions.session_outcomes(org_id, session_id);

CREATE INDEX IF NOT EXISTS session_outcomes_org_commit_idx
  ON sessions.session_outcomes(org_id, commit_sha);

CREATE INDEX IF NOT EXISTS session_outcomes_org_reverted_idx
  ON sessions.session_outcomes(org_id, reverted)
  WHERE reverted = true;

-- 2. patterns: the shared brain. scope='individual' (subject_user_id set) or
--    scope='team' (subject_user_id null, aggregated across the org).
CREATE TABLE IF NOT EXISTS sessions.patterns (
  id TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  scope TEXT NOT NULL,
  subject_user_id TEXT,
  project_id TEXT,
  kind TEXT NOT NULL,
  title TEXT NOT NULL,
  statement TEXT NOT NULL,
  body TEXT,
  confidence_basis_points INTEGER,
  sample_size INTEGER NOT NULL DEFAULT 0,
  support_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'candidate',
  superseded_by TEXT,
  embedding public.vector(1536),
  first_observed_at TIMESTAMPTZ,
  last_observed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS patterns_org_scope_kind_idx
  ON sessions.patterns(org_id, scope, kind);

CREATE INDEX IF NOT EXISTS patterns_org_subject_idx
  ON sessions.patterns(org_id, subject_user_id)
  WHERE subject_user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS patterns_org_project_status_idx
  ON sessions.patterns(org_id, project_id, status);

CREATE INDEX IF NOT EXISTS patterns_embedding_hnsw_idx
  ON sessions.patterns USING hnsw (embedding public.vector_cosine_ops)
  WHERE embedding IS NOT NULL;

-- 3. pattern_hits: Pair feedback loop. 'recurred' is the "made the same mistake
--    twice" signal that re-scores confidence/sample_size.
CREATE TABLE IF NOT EXISTS sessions.pattern_hits (
  id TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  pattern_id TEXT NOT NULL,
  source TEXT,
  session_id TEXT,
  event_id TEXT,
  outcome TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS pattern_hits_org_pattern_ts_idx
  ON sessions.pattern_hits(org_id, pattern_id, ts);

CREATE INDEX IF NOT EXISTS pattern_hits_org_outcome_ts_idx
  ON sessions.pattern_hits(org_id, outcome, ts);

-- 4. github_org_links: GitHub org -> RelayAuth org_id binding for team scoping.
--    A GitHub org resolves to the existing tenancy; it is not a parallel tenant.
CREATE TABLE IF NOT EXISTS sessions.github_org_links (
  github_org_id BIGINT PRIMARY KEY,
  github_org_login TEXT NOT NULL,
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  linked_by_user_id TEXT NOT NULL,
  verified BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS github_org_links_org_idx
  ON sessions.github_org_links(org_id);

CREATE UNIQUE INDEX IF NOT EXISTS github_org_links_login_idx
  ON sessions.github_org_links(github_org_login);
