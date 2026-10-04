CREATE TABLE sessions.session_evaluations (
  org_id text NOT NULL,
  cache_key text NOT NULL,
  status text CHECK (status IN ('active', 'idle', 'finished')),
  confidence_basis_points integer CHECK (confidence_basis_points BETWEEN 0 AND 10000),
  retry_at timestamptz NOT NULL,
  PRIMARY KEY (org_id, cache_key)
);
