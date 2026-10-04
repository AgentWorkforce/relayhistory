-- The session revision a request was admitted for. Briefs are cached per
-- revision: a newer revision re-queues analysis while the previous brief stays
-- readable.
ALTER TABLE sessions.session_analysis_requests ADD COLUMN source_revision text;

-- A digest of every stored row for one session, computed inside the database so
-- a read never transfers the transcript. It is captured before evidence
-- collection, so it never claims more data than a brief covered; data arriving
-- mid-collection re-queues once and dedupes onto the same fingerprinted job.
CREATE FUNCTION sessions.session_source_revision(p_org_id text, p_session_id text)
RETURNS text LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $session_revision$
  SELECT CASE WHEN count(*) = 0 THEN NULL
              ELSE md5(string_agg(row_key || '=' || row_hash, ',' ORDER BY row_key)) END
    FROM (
      SELECT 'e|' || machine_id || '|' || source || '|' || kind || '|' || event_id AS row_key,
             md5(record::text || '|' || COALESCE(content, '')) AS row_hash
        FROM sessions.convergence_events
       WHERE org_id = p_org_id AND session_id = p_session_id
      UNION ALL
      SELECT 't|' || lpad(turn_index::text, 10, '0'),
             md5(role || '|' || content || '|' || metadata::text || '|' || extract(epoch FROM ts)::text)
        FROM sessions.conversation_turns
       WHERE org_id = p_org_id AND session_id = p_session_id
    ) AS session_rows;
$session_revision$;

-- Every admission path records the revision it saw, including
-- request_session_analysis_v1, so a POST-produced brief is not re-queued.
CREATE FUNCTION sessions.stamp_session_analysis_request_revision()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $stamp_revision$
BEGIN
  NEW.source_revision := COALESCE(NEW.source_revision,
    sessions.session_source_revision(NEW.org_id, NEW.session_id));
  RETURN NEW;
END;
$stamp_revision$;
CREATE TRIGGER session_analysis_requests_revision
  BEFORE INSERT ON sessions.session_analysis_requests
  FOR EACH ROW EXECUTE FUNCTION sessions.stamp_session_analysis_request_revision();

-- One call reads the cached brief and, when it is missing, its revision is
-- outdated, or it was produced by another engine identity, admits a new request.
-- The previous brief is returned with `stale = true` while the refresh runs.
-- Tenant identity comes only from the authenticated route; `p_engine` is History's
-- active analysis|schema|summary|model|summary-model version identity.
CREATE FUNCTION sessions.session_brief_v1(
  p_org_id text, p_workspace_id text, p_user_id text, p_session_id text, p_engine text
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $session_brief$
DECLARE
  current_revision text;
  cached_revision text;
  cached_id uuid;
  cached_result jsonb;
  cached_at timestamptz;
  cached_engine text;
  latest sessions.session_analysis_requests%ROWTYPE;
  latest_job sessions.session_analysis_jobs%ROWTYPE;
  latest_status text;
  latest_error text;
  fresh boolean;
  enqueue boolean;
  admitted integer;
  brief_status text;
BEGIN
  IF p_org_id IS NULL OR length(p_org_id) NOT BETWEEN 1 AND 512 OR btrim(p_org_id) = ''
    OR p_workspace_id IS NULL OR length(p_workspace_id) NOT BETWEEN 1 AND 512 OR btrim(p_workspace_id) = ''
    OR p_user_id IS NULL OR length(p_user_id) NOT BETWEEN 1 AND 512 OR btrim(p_user_id) = ''
    OR p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = ''
    OR p_engine IS NULL OR btrim(p_engine) = '' THEN
    RETURN jsonb_build_object('outcome', 'invalid_request');
  END IF;
  current_revision := sessions.session_source_revision(p_org_id, p_session_id);
  IF current_revision IS NULL THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  -- Shared with request_session_analysis_v1 so both admission paths serialize.
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || p_org_id, 0));

  SELECT r.source_revision, j.id, j.result, j.completed_at,
         concat_ws('|', j.analysis_version, j.schema_version::text, j.summary_version,
                   j.model_version, j.summary_model_version)
    INTO cached_revision, cached_id, cached_result, cached_at, cached_engine
    FROM sessions.session_analysis_requests AS r
    JOIN sessions.session_analysis_jobs AS j ON j.id = r.analysis_job_id
      AND j.org_id = r.org_id AND j.session_id = r.session_id
   WHERE r.org_id = p_org_id AND r.session_id = p_session_id
     AND r.status = 'resolved' AND j.status = 'completed'
   ORDER BY r.created_at DESC, r.id DESC LIMIT 1;
  fresh := cached_id IS NOT NULL AND cached_revision IS NOT DISTINCT FROM current_revision
    AND cached_engine = p_engine;

  SELECT * INTO latest FROM sessions.session_analysis_requests
   WHERE org_id = p_org_id AND session_id = p_session_id
   ORDER BY created_at DESC, id DESC LIMIT 1;
  IF latest.analysis_job_id IS NOT NULL THEN
    SELECT * INTO latest_job FROM sessions.session_analysis_jobs
     WHERE id = latest.analysis_job_id AND org_id = p_org_id AND session_id = p_session_id;
  END IF;
  latest_status := CASE WHEN latest.status = 'resolved' THEN COALESCE(latest_job.status, 'failed')
                        ELSE latest.status END;
  latest_error := CASE WHEN latest.status = 'resolved' AND latest_job.status = 'failed' THEN latest_job.last_error
                       WHEN latest.status = 'failed' THEN latest.last_error END;

  -- A failure for this exact revision is retried only when another attempt can
  -- change the outcome: a transient request failure after a cooldown, or a
  -- failed model job that still has its one requeue.
  enqueue := NOT fresh AND (
    latest.id IS NULL
    OR latest_status = 'completed'
    OR (latest_status = 'failed' AND (
      latest.source_revision IS DISTINCT FROM current_revision
      OR (latest.status = 'failed'
          AND latest.last_error IN ('session_analysis_rate_limited', 'session_collection_failed', 'request_lease_expired')
          AND latest.updated_at <= now() - interval '1 hour')
      OR (latest.status = 'resolved' AND latest_job.requeues < 1))));

  IF enqueue THEN
    SELECT count(*)::integer INTO admitted FROM sessions.session_analysis_requests
     WHERE org_id = p_org_id
       AND created_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
    IF admitted >= 20 THEN
      brief_status := 'rate_limited';
    ELSE
      INSERT INTO sessions.session_analysis_requests
        (org_id, workspace_id, session_id, requested_by_user_id, source_revision)
        VALUES (p_org_id, p_workspace_id, p_session_id, p_user_id, current_revision)
        RETURNING * INTO latest;
      latest_job := NULL;
      latest_status := latest.status;
      latest_error := NULL;
    END IF;
  END IF;

  brief_status := COALESCE(brief_status, CASE
    WHEN fresh THEN 'ready'
    WHEN latest_status IN ('queued', 'running') THEN
      CASE WHEN cached_id IS NULL THEN 'pending' ELSE 'refreshing' END
    ELSE 'failed' END);

  RETURN jsonb_build_object(
    'outcome', 'ok',
    'sessionId', p_session_id,
    'status', brief_status,
    'stale', cached_id IS NOT NULL AND NOT fresh,
    'analysisId', cached_id,
    'brief', cached_result -> 'brief',
    'analysis', cached_result -> 'analysis',
    'generatedAt', cached_at,
    'request', CASE WHEN latest.id IS NULL THEN NULL ELSE jsonb_build_object(
      'id', latest.id, 'sessionId', latest.session_id, 'status', latest_status,
      'analysisId', latest.analysis_job_id, 'error', latest_error,
      'createdAt', latest.created_at, 'updatedAt', latest.updated_at) END);
END;
$session_brief$;

REVOKE ALL ON FUNCTION sessions.session_source_revision(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.stamp_session_analysis_request_revision() FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.session_brief_v1(text, text, text, text, text) FROM PUBLIC;
