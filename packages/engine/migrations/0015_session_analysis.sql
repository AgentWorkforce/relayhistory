CREATE TABLE sessions.session_analysis_jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id text NOT NULL,
  workspace_id text,
  session_id text NOT NULL,
  requested_by_user_id text NOT NULL,
  source_provider text NOT NULL,
  source_fingerprint text NOT NULL,
  analysis_version text NOT NULL,
  schema_version integer NOT NULL,
  summary_version text NOT NULL,
  model_version text NOT NULL,
  summary_model_version text NOT NULL,
  source jsonb NOT NULL,
  status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'completed', 'failed')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3),
  requeues integer NOT NULL DEFAULT 0 CHECK (requeues BETWEEN 0 AND 1),
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_token uuid,
  lease_expires_at timestamptz,
  result jsonb,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);
CREATE UNIQUE INDEX session_analysis_jobs_idempotency_idx ON sessions.session_analysis_jobs
  (org_id, session_id, source_provider, source_fingerprint, analysis_version, schema_version, summary_version, model_version, summary_model_version);
CREATE INDEX session_analysis_jobs_claim_idx ON sessions.session_analysis_jobs (status, available_at, lease_expires_at);
CREATE INDEX session_analysis_jobs_scope_idx ON sessions.session_analysis_jobs (org_id, session_id, created_at);
CREATE TABLE sessions.session_analysis_admissions (
  org_id text NOT NULL,
  day date NOT NULL,
  requests integer NOT NULL DEFAULT 0 CHECK (requests BETWEEN 0 AND 20),
  PRIMARY KEY (org_id, day)
);

-- Neon HTTP executes one statement per request. Keep admission and the job
-- mutation in one transaction, serialized for a tenant, so concurrent identical
-- POSTs cannot each consume quota while the unique index creates one job.
CREATE FUNCTION sessions.enqueue_session_analysis_job(
  p_org_id text,
  p_workspace_id text,
  p_session_id text,
  p_requested_by_user_id text,
  p_source_provider text,
  p_source_fingerprint text,
  p_analysis_version text,
  p_schema_version integer,
  p_summary_version text,
  p_model_version text,
  p_summary_model_version text,
  p_source jsonb
) RETURNS uuid LANGUAGE plpgsql AS $enqueue_analysis$
DECLARE
  existing sessions.session_analysis_jobs%ROWTYPE;
  admitted integer;
  new_id uuid;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis:' || p_org_id, 0));

  SELECT * INTO existing FROM sessions.session_analysis_jobs
   WHERE org_id = p_org_id AND session_id = p_session_id
     AND source_provider = p_source_provider AND source_fingerprint = p_source_fingerprint
     AND analysis_version = p_analysis_version AND schema_version = p_schema_version
     AND summary_version = p_summary_version AND model_version = p_model_version
     AND summary_model_version = p_summary_model_version;
  IF FOUND AND (existing.status <> 'failed' OR existing.requeues >= 1) THEN
    RETURN existing.id;
  END IF;

  INSERT INTO sessions.session_analysis_admissions (org_id, day, requests)
    VALUES (p_org_id, (now() AT TIME ZONE 'UTC')::date, 1)
    ON CONFLICT (org_id, day) DO UPDATE
      SET requests = sessions.session_analysis_admissions.requests + 1
      WHERE sessions.session_analysis_admissions.requests < 20
    RETURNING requests INTO admitted;
  IF admitted IS NULL THEN RETURN NULL; END IF;

  IF existing.id IS NOT NULL THEN
    UPDATE sessions.session_analysis_jobs
       SET status = 'queued', attempts = 0, requeues = requeues + 1,
           available_at = now(), last_error = NULL, lease_token = NULL,
           lease_expires_at = NULL, updated_at = now()
     WHERE id = existing.id AND status = 'failed' AND requeues < 1;
    RETURN existing.id;
  END IF;

  INSERT INTO sessions.session_analysis_jobs (
    org_id, workspace_id, session_id, requested_by_user_id,
    source_provider, source_fingerprint, analysis_version, schema_version,
    summary_version, model_version, summary_model_version, source
  ) VALUES (
    p_org_id, p_workspace_id, p_session_id, p_requested_by_user_id,
    p_source_provider, p_source_fingerprint, p_analysis_version, p_schema_version,
    p_summary_version, p_model_version, p_summary_model_version, p_source
  ) RETURNING id INTO new_id;
  RETURN new_id;
END;
$enqueue_analysis$;
