CREATE TABLE sessions.session_analysis_requests (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  session_id text NOT NULL,
  requested_by_user_id text NOT NULL,
  status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'resolved', 'failed')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3),
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_token uuid,
  lease_expires_at timestamptz,
  analysis_job_id uuid REFERENCES sessions.session_analysis_jobs(id),
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX session_analysis_requests_claim_idx ON sessions.session_analysis_requests (status, available_at, lease_expires_at);
CREATE INDEX session_analysis_requests_scope_idx ON sessions.session_analysis_requests (org_id, session_id, created_at);
CREATE INDEX session_analysis_requests_admission_idx ON sessions.session_analysis_requests (org_id, created_at);
CREATE UNIQUE INDEX session_analysis_requests_active_idx ON sessions.session_analysis_requests (org_id, session_id)
  WHERE status IN ('queued', 'running');

-- Cloud passes only four identities. The function validates a session exists in
-- this organization, serializes admission, and returns only a public projection.
-- The owner has table access; the caller needs only EXECUTE, not table grants.
CREATE FUNCTION sessions.request_session_analysis_v1(
  p_org_id text, p_workspace_id text, p_user_id text, p_session_id text
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $request_analysis$
DECLARE
  existing sessions.session_analysis_requests%ROWTYPE;
  existing_id uuid;
  linked_status text;
  admitted integer;
BEGIN
  IF p_org_id IS NULL OR length(p_org_id) NOT BETWEEN 1 AND 512 OR btrim(p_org_id) = ''
    OR p_workspace_id IS NULL OR length(p_workspace_id) NOT BETWEEN 1 AND 512 OR btrim(p_workspace_id) = ''
    OR p_user_id IS NULL OR length(p_user_id) NOT BETWEEN 1 AND 512 OR btrim(p_user_id) = ''
    OR p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = '' THEN
    RETURN jsonb_build_object('outcome', 'invalid_request');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM sessions.convergence_events WHERE org_id = p_org_id AND session_id = p_session_id)
     AND NOT EXISTS (SELECT 1 FROM sessions.conversation_turns WHERE org_id = p_org_id AND session_id = p_session_id) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || p_org_id, 0));

  SELECT r.id, j.status INTO existing_id, linked_status
    FROM sessions.session_analysis_requests AS r
    LEFT JOIN sessions.session_analysis_jobs AS j ON j.id = r.analysis_job_id
      AND j.org_id = r.org_id AND j.session_id = r.session_id
   WHERE r.org_id = p_org_id AND r.session_id = p_session_id
     AND (r.status IN ('queued', 'running') OR (r.status = 'resolved' AND j.status IN ('queued', 'running')))
   ORDER BY r.created_at DESC, r.id DESC LIMIT 1;
  IF existing_id IS NOT NULL THEN
    SELECT * INTO existing FROM sessions.session_analysis_requests WHERE id = existing_id;
    RETURN jsonb_build_object('outcome', 'accepted', 'request', jsonb_build_object(
      'id', existing.id, 'sessionId', existing.session_id,
      'status', CASE WHEN existing.status = 'resolved' THEN linked_status ELSE existing.status END,
      'analysisId', existing.analysis_job_id, 'error', NULL,
      'createdAt', existing.created_at, 'updatedAt', existing.updated_at));
  END IF;

  -- The org lock above serializes this count with inserts. Request admission is
  -- separate from the canonical model-job admission in 0015: one request that
  -- creates one job must not consume two slots from the same daily counter.
  SELECT count(*)::integer INTO admitted FROM sessions.session_analysis_requests
   WHERE org_id = p_org_id
     AND created_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
  IF admitted >= 20 THEN RETURN jsonb_build_object('outcome', 'rate_limited'); END IF;

  INSERT INTO sessions.session_analysis_requests (org_id, workspace_id, session_id, requested_by_user_id)
    VALUES (p_org_id, p_workspace_id, p_session_id, p_user_id)
    RETURNING * INTO existing;
  RETURN jsonb_build_object('outcome', 'accepted', 'request', jsonb_build_object(
    'id', existing.id, 'sessionId', existing.session_id, 'status', existing.status,
    'analysisId', NULL, 'error', NULL,
    'createdAt', existing.created_at, 'updatedAt', existing.updated_at));
END;
$request_analysis$;

CREATE FUNCTION sessions.read_session_analysis_v1(
  p_org_id text, p_session_id text
) RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $read_analysis$
DECLARE
  public_requests jsonb;
  public_analyses jsonb;
BEGIN
  IF p_org_id IS NULL OR length(p_org_id) NOT BETWEEN 1 AND 512 OR btrim(p_org_id) = ''
    OR p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = '' THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM sessions.convergence_events WHERE org_id = p_org_id AND session_id = p_session_id)
     AND NOT EXISTS (SELECT 1 FROM sessions.conversation_turns WHERE org_id = p_org_id AND session_id = p_session_id)
     AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_requests WHERE org_id = p_org_id AND session_id = p_session_id)
     AND NOT EXISTS (SELECT 1 FROM sessions.session_analysis_jobs WHERE org_id = p_org_id AND session_id = p_session_id) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'id', r.id, 'sessionId', r.session_id,
    'status', CASE WHEN r.status = 'resolved' THEN COALESCE(j.status, 'failed') ELSE r.status END,
    'analysisId', r.analysis_job_id,
    'error', CASE WHEN r.status = 'resolved' AND j.status = 'failed' THEN j.last_error
                  WHEN r.status = 'failed' THEN r.last_error ELSE NULL END,
    'createdAt', r.created_at, 'updatedAt', r.updated_at
  ) ORDER BY r.created_at DESC, r.id DESC), '[]'::jsonb) INTO public_requests
  FROM (SELECT * FROM sessions.session_analysis_requests
         WHERE org_id = p_org_id AND session_id = p_session_id
         ORDER BY created_at DESC, id DESC LIMIT 50) AS r
  LEFT JOIN sessions.session_analysis_jobs AS j ON j.id = r.analysis_job_id AND j.org_id = p_org_id;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'id', j.id, 'sessionId', j.session_id, 'sourceProvider', j.source_provider,
    'sourceFingerprint', j.source_fingerprint, 'status', j.status, 'attempts', j.attempts,
    'analysisVersion', j.analysis_version, 'schemaVersion', j.schema_version,
    'summaryVersion', j.summary_version, 'modelVersion', j.model_version,
    'summaryModelVersion', j.summary_model_version, 'result', j.result,
    'error', CASE WHEN j.status = 'failed' THEN j.last_error ELSE NULL END,
    'createdAt', j.created_at, 'updatedAt', j.updated_at, 'completedAt', j.completed_at
  ) ORDER BY j.created_at DESC, j.id DESC), '[]'::jsonb) INTO public_analyses
  FROM (SELECT id, session_id, source_provider, source_fingerprint, status, attempts,
               analysis_version, schema_version, summary_version, model_version,
               summary_model_version, result, last_error, created_at, updated_at, completed_at
          FROM sessions.session_analysis_jobs
         WHERE org_id = p_org_id AND session_id = p_session_id
         ORDER BY created_at DESC, id DESC LIMIT 50) AS j;

  RETURN jsonb_build_object('outcome', 'ok', 'sessionId', p_session_id,
    'requests', public_requests, 'analyses', public_analyses);
END;
$read_analysis$;

-- The collector must not enqueue a model job after losing its request lease.
-- Lock and fence first, then admit/link in the same database transaction. The
-- caller cannot supply tenant identity; it comes only from the leased request.
CREATE FUNCTION sessions.collect_session_analysis_request(
  p_request_id uuid, p_lease_token uuid,
  p_source_provider text, p_source_fingerprint text,
  p_analysis_version text, p_schema_version integer, p_summary_version text,
  p_model_version text, p_summary_model_version text, p_source jsonb
) RETURNS text LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $collect_analysis$
DECLARE
  request_row sessions.session_analysis_requests%ROWTYPE;
  admitted_job_id uuid;
BEGIN
  SELECT * INTO request_row FROM sessions.session_analysis_requests
   WHERE id = p_request_id FOR UPDATE;
  IF NOT FOUND OR request_row.status <> 'running'
     OR p_lease_token IS NULL OR request_row.lease_token IS NULL
     OR request_row.lease_token IS DISTINCT FROM p_lease_token
     OR request_row.lease_expires_at IS NULL
     OR request_row.lease_expires_at <= clock_timestamp() THEN
    RETURN 'lease-lost';
  END IF;

  admitted_job_id := sessions.enqueue_session_analysis_job(
    request_row.org_id, request_row.workspace_id, request_row.session_id,
    request_row.requested_by_user_id, p_source_provider, p_source_fingerprint,
    p_analysis_version, p_schema_version, p_summary_version,
    p_model_version, p_summary_model_version, p_source
  );
  IF admitted_job_id IS NULL THEN RETURN 'rate-limited'; END IF;

  UPDATE sessions.session_analysis_requests
     SET status = 'resolved', analysis_job_id = admitted_job_id,
         lease_token = NULL, lease_expires_at = NULL, updated_at = now()
   WHERE id = request_row.id;
  RETURN 'collected';
END;
$collect_analysis$;

REVOKE ALL ON FUNCTION sessions.request_session_analysis_v1(text, text, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.read_session_analysis_v1(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.collect_session_analysis_request(uuid, uuid, text, text, text, integer, text, text, text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.enqueue_session_analysis_job(text, text, text, text, text, text, text, integer, text, text, text, jsonb) FROM PUBLIC;
