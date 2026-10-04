-- Session briefs are their own cheap model job, separate from the full analysis,
-- and a session's revision is a counter maintained at ingest instead of a digest
-- computed on every read.
--
-- The superseded worker and brief functions (enqueue_session_analysis_job,
-- collect_session_analysis_request, session_brief_v1, session_source_revision)
-- stay until the Worker that no longer calls them is verified in production, so
-- the previous Worker keeps working between this migration and its replacement.

-- Revision: bumped by every stored change to a session's events or turns. Replays
-- that rewrite identical content do not bump it. A session with rows but no counter
-- row (written before this migration) has revision 0.
CREATE TABLE sessions.session_revisions (
  org_id text NOT NULL,
  session_id text NOT NULL,
  revision bigint NOT NULL DEFAULT 1,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, session_id)
);

-- Statement-level: one upsert per touched session per statement, so a bulk ingest
-- costs one counter write per session rather than one per row.
CREATE FUNCTION sessions.bump_session_revisions() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $bump_revisions$
BEGIN
  INSERT INTO sessions.session_revisions AS r (org_id, session_id)
    SELECT DISTINCT org_id, session_id FROM changed_rows
    ON CONFLICT (org_id, session_id)
    DO UPDATE SET revision = r.revision + 1, updated_at = now();
  RETURN NULL;
END;
$bump_revisions$;

-- Updates bump every session with a changed row, comparing whole rows except
-- derived columns (embeddings and their provenance, ingest time). The set
-- difference also catches rows moved between sessions: both sessions bump.
-- Identical ingest replays leave the revision alone.
CREATE FUNCTION sessions.bump_updated_revisions() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $bump_updated$
DECLARE
  derived text[] := ARRAY['embedding', 'embedding_model', 'embedding_dim', 'content_hash',
                          'generated_at', 'embedding_skip_reason', 'ingested_at'];
BEGIN
  INSERT INTO sessions.session_revisions AS r (org_id, session_id)
    SELECT DISTINCT org_id, session_id FROM (
      (SELECT n.org_id, n.session_id, to_jsonb(n) - derived AS body FROM new_rows AS n
       EXCEPT SELECT o.org_id, o.session_id, to_jsonb(o) - derived FROM old_rows AS o)
      UNION ALL
      (SELECT o.org_id, o.session_id, to_jsonb(o) - derived AS body FROM old_rows AS o
       EXCEPT SELECT n.org_id, n.session_id, to_jsonb(n) - derived FROM new_rows AS n)
    ) AS changed
    ON CONFLICT (org_id, session_id)
    DO UPDATE SET revision = r.revision + 1, updated_at = now();
  RETURN NULL;
END;
$bump_updated$;

CREATE TRIGGER convergence_events_revision_insert
  AFTER INSERT ON sessions.convergence_events REFERENCING NEW TABLE AS changed_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.bump_session_revisions();
CREATE TRIGGER convergence_events_revision_delete
  AFTER DELETE ON sessions.convergence_events REFERENCING OLD TABLE AS changed_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.bump_session_revisions();
CREATE TRIGGER convergence_events_revision_update
  AFTER UPDATE ON sessions.convergence_events REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.bump_updated_revisions();
CREATE TRIGGER conversation_turns_revision_insert
  AFTER INSERT ON sessions.conversation_turns REFERENCING NEW TABLE AS changed_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.bump_session_revisions();
CREATE TRIGGER conversation_turns_revision_delete
  AFTER DELETE ON sessions.conversation_turns REFERENCING OLD TABLE AS changed_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.bump_session_revisions();
CREATE TRIGGER conversation_turns_revision_update
  AFTER UPDATE ON sessions.conversation_turns REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.bump_updated_revisions();

CREATE FUNCTION sessions.current_session_revision(p_org_id text, p_session_id text)
RETURNS text LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $current_revision$
  SELECT COALESCE((SELECT revision FROM sessions.session_revisions
                    WHERE org_id = p_org_id AND session_id = p_session_id), 0)::text;
$current_revision$;

-- Requests are stamped at admission; the collector restamps at claim, just before
-- it reads evidence, so a brief never claims less data than it covered.
CREATE OR REPLACE FUNCTION sessions.stamp_session_analysis_request_revision()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $stamp_revision$
BEGIN
  NEW.source_revision := COALESCE(NEW.source_revision,
    sessions.current_session_revision(NEW.org_id, NEW.session_id));
  RETURN NEW;
END;
$stamp_revision$;

-- Jobs: `brief` is the quick title/description; `analysis` is the full cited
-- analysis. Each kind carries only the version columns that define it; the others
-- are empty so a change to one kind's versions never recomputes the other.
ALTER TABLE sessions.session_analysis_jobs
  ADD COLUMN kind text NOT NULL DEFAULT 'analysis' CHECK (kind IN ('brief', 'analysis'));
DROP INDEX sessions.session_analysis_jobs_idempotency_idx;
CREATE UNIQUE INDEX session_analysis_jobs_identity_idx ON sessions.session_analysis_jobs
  (org_id, session_id, kind, source_provider, source_fingerprint, analysis_version,
   schema_version, summary_version, model_version, summary_model_version);
DROP INDEX sessions.session_analysis_jobs_claim_idx;
CREATE INDEX session_analysis_jobs_claim_idx ON sessions.session_analysis_jobs
  (kind, status, available_at, lease_expires_at);

ALTER TABLE sessions.session_analysis_requests
  ADD COLUMN brief_job_id uuid REFERENCES sessions.session_analysis_jobs(id),
  ADD COLUMN engine text;

-- Same idempotency and single-requeue rules as enqueue_session_analysis_job. Only
-- analysis jobs spend the daily model admission; briefs are bounded by request
-- admission.
CREATE FUNCTION sessions.enqueue_session_job_v2(
  p_kind text, p_org_id text, p_workspace_id text, p_session_id text,
  p_requested_by_user_id text, p_source_provider text, p_source_fingerprint text,
  p_analysis_version text, p_schema_version integer, p_summary_version text,
  p_model_version text, p_summary_model_version text, p_source jsonb
) RETURNS uuid LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $enqueue_job$
DECLARE
  existing sessions.session_analysis_jobs%ROWTYPE;
  admitted integer;
  new_id uuid;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis:' || p_org_id, 0));

  SELECT * INTO existing FROM sessions.session_analysis_jobs
   WHERE org_id = p_org_id AND session_id = p_session_id AND kind = p_kind
     AND source_provider = p_source_provider AND source_fingerprint = p_source_fingerprint
     AND analysis_version = p_analysis_version AND schema_version = p_schema_version
     AND summary_version = p_summary_version AND model_version = p_model_version
     AND summary_model_version = p_summary_model_version;
  IF FOUND AND (existing.status <> 'failed' OR existing.requeues >= 1) THEN
    RETURN existing.id;
  END IF;

  IF p_kind = 'analysis' THEN
    INSERT INTO sessions.session_analysis_admissions (org_id, day, requests)
      VALUES (p_org_id, (now() AT TIME ZONE 'UTC')::date, 1)
      ON CONFLICT (org_id, day) DO UPDATE
        SET requests = sessions.session_analysis_admissions.requests + 1
        WHERE sessions.session_analysis_admissions.requests < 20
      RETURNING requests INTO admitted;
    IF admitted IS NULL THEN RETURN NULL; END IF;
  END IF;

  IF existing.id IS NOT NULL THEN
    UPDATE sessions.session_analysis_jobs
       SET status = 'queued', attempts = 0, requeues = requeues + 1,
           available_at = now(), last_error = NULL, lease_token = NULL,
           lease_expires_at = NULL, updated_at = now()
     WHERE id = existing.id AND status = 'failed' AND requeues < 1;
    RETURN existing.id;
  END IF;

  INSERT INTO sessions.session_analysis_jobs (
    kind, org_id, workspace_id, session_id, requested_by_user_id,
    source_provider, source_fingerprint, analysis_version, schema_version,
    summary_version, model_version, summary_model_version, source
  ) VALUES (
    p_kind, p_org_id, p_workspace_id, p_session_id, p_requested_by_user_id,
    p_source_provider, p_source_fingerprint, p_analysis_version, p_schema_version,
    p_summary_version, p_model_version, p_summary_model_version, p_source
  ) RETURNING id INTO new_id;
  RETURN new_id;
END;
$enqueue_job$;

-- True while the session's newest analysis finished (completed or failed) less than
-- 30 minutes ago. Superseded jobs never ran and do not count.
CREATE FUNCTION sessions.session_analysis_cooling(p_org_id text, p_session_id text)
RETURNS boolean LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $analysis_cooling$
  SELECT EXISTS (SELECT 1 FROM sessions.session_analysis_jobs
                  WHERE org_id = p_org_id AND session_id = p_session_id
                    AND kind = 'analysis' AND status IN ('completed', 'failed')
                    AND last_error IS DISTINCT FROM 'superseded'
                    AND COALESCE(completed_at, updated_at) > now() - interval '30 minutes');
$analysis_cooling$;

-- Distinct sessions (other than this one) admitted today. The daily request budget
-- counts sessions, so a session admitted today is not charged again; its refreshes
-- are bounded by the per-session limits in session_brief_v2.
CREATE FUNCTION sessions.sessions_admitted_today(p_org_id text, p_session_id text)
RETURNS integer LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $admitted_today$
  SELECT count(DISTINCT session_id)::integer FROM sessions.session_analysis_requests
   WHERE org_id = p_org_id AND session_id <> p_session_id
     AND created_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
$admitted_today$;

-- Per session, after its first request: at most one request every 2 minutes and 30
-- per UTC day. Returns 'throttled', 'capped' or NULL (admissible).
CREATE FUNCTION sessions.session_request_limit(p_org_id text, p_session_id text)
RETURNS text LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $session_limit$
  SELECT CASE
    WHEN EXISTS (SELECT 1 FROM sessions.session_analysis_requests
                  WHERE org_id = p_org_id AND session_id = p_session_id
                    AND created_at > now() - interval '2 minutes') THEN 'throttled'
    WHEN (SELECT count(*) FROM sessions.session_analysis_requests
           WHERE org_id = p_org_id AND session_id = p_session_id
             AND created_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') >= 30
      THEN 'capped' END;
$session_limit$;

-- Fence the request lease, then admit and link both jobs in one transaction. A
-- brief is linked even when model admission refuses the analysis.
--
-- Analysis spend per session is bounded: while another analysis of the session is
-- running, or one completed within the last 30 minutes, this request defers its
-- analysis (no job, no error); a later read admits a fresh one. Queued jobs for the
-- session's older input are superseded before they reach the model.
CREATE FUNCTION sessions.collect_session_analysis_request_v2(
  p_request_id uuid, p_lease_token uuid, p_source_provider text,
  p_brief_fingerprint text, p_summary_version text, p_summary_model_version text,
  p_brief_source jsonb,
  p_analysis_fingerprint text, p_analysis_version text, p_schema_version integer,
  p_model_version text, p_analysis_source jsonb
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $collect_v2$
DECLARE
  request_row sessions.session_analysis_requests%ROWTYPE;
  brief_id uuid;
  analysis_id uuid;
  same_analysis uuid;
  same_status text;
  deferred boolean;
BEGIN
  SELECT * INTO request_row FROM sessions.session_analysis_requests
   WHERE id = p_request_id FOR UPDATE;
  IF NOT FOUND OR request_row.status <> 'running'
     OR p_lease_token IS NULL OR request_row.lease_token IS NULL
     OR request_row.lease_token IS DISTINCT FROM p_lease_token
     OR request_row.lease_expires_at IS NULL
     OR request_row.lease_expires_at <= clock_timestamp() THEN
    RETURN jsonb_build_object('outcome', 'lease-lost');
  END IF;

  brief_id := sessions.enqueue_session_job_v2('brief',
    request_row.org_id, request_row.workspace_id, request_row.session_id,
    request_row.requested_by_user_id, p_source_provider, p_brief_fingerprint,
    '', 0, p_summary_version, '', p_summary_model_version, p_brief_source);
  UPDATE sessions.session_analysis_jobs
     SET status = 'failed', last_error = 'superseded', updated_at = now()
   WHERE org_id = request_row.org_id AND session_id = request_row.session_id
     AND kind = 'brief' AND status = 'queued' AND id <> brief_id;

  SELECT id, status INTO same_analysis, same_status FROM sessions.session_analysis_jobs
   WHERE org_id = request_row.org_id AND session_id = request_row.session_id
     AND kind = 'analysis' AND source_provider = p_source_provider
     AND source_fingerprint = p_analysis_fingerprint AND analysis_version = p_analysis_version
     AND schema_version = p_schema_version AND summary_version = ''
     AND model_version = p_model_version AND summary_model_version = '';
  -- Reusing a queued, running or completed job spends nothing; a new job or a
  -- failed job's requeue waits for the session's running analysis and cooldown.
  deferred := (same_analysis IS NULL OR same_status = 'failed') AND (
    EXISTS (SELECT 1 FROM sessions.session_analysis_jobs
             WHERE org_id = request_row.org_id AND session_id = request_row.session_id
               AND kind = 'analysis' AND status = 'running')
    OR sessions.session_analysis_cooling(request_row.org_id, request_row.session_id));
  IF NOT deferred THEN
    UPDATE sessions.session_analysis_jobs
       SET status = 'failed', last_error = 'superseded', updated_at = now()
     WHERE org_id = request_row.org_id AND session_id = request_row.session_id
       AND kind = 'analysis' AND status = 'queued' AND id IS DISTINCT FROM same_analysis;
    analysis_id := sessions.enqueue_session_job_v2('analysis',
      request_row.org_id, request_row.workspace_id, request_row.session_id,
      request_row.requested_by_user_id, p_source_provider, p_analysis_fingerprint,
      p_analysis_version, p_schema_version, '', p_model_version, '', p_analysis_source);
  END IF;

  UPDATE sessions.session_analysis_requests
     SET status = 'resolved', brief_job_id = brief_id, analysis_job_id = analysis_id,
         last_error = CASE WHEN NOT deferred AND analysis_id IS NULL
                           THEN 'session_analysis_rate_limited' END,
         lease_token = NULL, lease_expires_at = NULL, updated_at = now()
   WHERE id = request_row.id;
  RETURN jsonb_build_object('outcome', 'collected', 'briefJobId', brief_id, 'analysisJobId', analysis_id);
END;
$collect_v2$;

-- The newest completed job of one kind, via the request that linked it.
CREATE FUNCTION sessions.cached_session_job(p_org_id text, p_session_id text, p_kind text)
RETURNS TABLE (revision text, job_id uuid, result jsonb, completed_at timestamptz, engine text)
LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $cached_job$
  SELECT r.source_revision, j.id, j.result, j.completed_at,
         concat_ws('|', j.analysis_version, j.schema_version::text, j.summary_version,
                   j.model_version, j.summary_model_version)
    FROM sessions.session_analysis_requests AS r
    JOIN sessions.session_analysis_jobs AS j
      ON j.id = CASE p_kind WHEN 'brief' THEN r.brief_job_id ELSE r.analysis_job_id END
     AND j.org_id = r.org_id AND j.session_id = r.session_id AND j.kind = p_kind
   WHERE r.org_id = p_org_id AND r.session_id = p_session_id
     AND r.status = 'resolved' AND j.status = 'completed'
   ORDER BY r.created_at DESC, r.id DESC LIMIT 1;
$cached_job$;

-- Read-through brief. Returns the cached brief and analysis, and admits one
-- request when either is missing or outdated (revision or engine identity).
-- `p_brief_engine` / `p_analysis_engine` are History's active job identities in
-- the `cached_session_job.engine` format. `admittedRequestId` is set only when this
-- call admitted a request, so the caller can start it immediately.
CREATE FUNCTION sessions.session_brief_v2(
  p_org_id text, p_workspace_id text, p_user_id text, p_session_id text,
  p_brief_engine text, p_analysis_engine text
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $session_brief$
DECLARE
  current_revision text;
  engine text;
  brief record;
  analysis record;
  brief_fresh boolean;
  analysis_fresh boolean;
  latest sessions.session_analysis_requests%ROWTYPE;
  brief_job sessions.session_analysis_jobs%ROWTYPE;
  analysis_job sessions.session_analysis_jobs%ROWTYPE;
  brief_busy boolean;
  analysis_busy boolean;
  brief_due boolean;
  analysis_due boolean;
  request_limit text;
  recent_failure text;
  exhausted boolean;
  retryable boolean;
  admitted_id uuid;
  limited boolean := false;
  request_status text;
  brief_error text;
  analysis_error text;
BEGIN
  IF p_org_id IS NULL OR length(p_org_id) NOT BETWEEN 1 AND 512 OR btrim(p_org_id) = ''
    OR p_workspace_id IS NULL OR length(p_workspace_id) NOT BETWEEN 1 AND 512 OR btrim(p_workspace_id) = ''
    OR p_user_id IS NULL OR length(p_user_id) NOT BETWEEN 1 AND 512 OR btrim(p_user_id) = ''
    OR p_session_id IS NULL OR length(p_session_id) NOT BETWEEN 1 AND 512 OR btrim(p_session_id) = ''
    OR p_brief_engine IS NULL OR btrim(p_brief_engine) = ''
    OR p_analysis_engine IS NULL OR btrim(p_analysis_engine) = '' THEN
    RETURN jsonb_build_object('outcome', 'invalid_request');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM sessions.convergence_events WHERE org_id = p_org_id AND session_id = p_session_id)
     AND NOT EXISTS (SELECT 1 FROM sessions.conversation_turns WHERE org_id = p_org_id AND session_id = p_session_id) THEN
    RETURN jsonb_build_object('outcome', 'not_found');
  END IF;
  engine := p_brief_engine || '#' || p_analysis_engine;
  -- Shared with request_session_analysis_v1 so both admission paths serialize.
  PERFORM pg_advisory_xact_lock(hashtextextended('session-analysis-request:' || p_org_id, 0));
  current_revision := sessions.current_session_revision(p_org_id, p_session_id);

  SELECT * INTO brief FROM sessions.cached_session_job(p_org_id, p_session_id, 'brief');
  SELECT * INTO analysis FROM sessions.cached_session_job(p_org_id, p_session_id, 'analysis');
  brief_fresh := brief.job_id IS NOT NULL AND brief.revision IS NOT DISTINCT FROM current_revision
    AND brief.engine = p_brief_engine;
  analysis_fresh := analysis.job_id IS NOT NULL AND analysis.revision IS NOT DISTINCT FROM current_revision
    AND analysis.engine = p_analysis_engine;

  SELECT * INTO latest FROM sessions.session_analysis_requests
   WHERE org_id = p_org_id AND session_id = p_session_id
   ORDER BY created_at DESC, id DESC LIMIT 1;
  SELECT * INTO brief_job FROM sessions.session_analysis_jobs
   WHERE id = latest.brief_job_id AND org_id = p_org_id AND session_id = p_session_id;
  SELECT * INTO analysis_job FROM sessions.session_analysis_jobs
   WHERE id = latest.analysis_job_id AND org_id = p_org_id AND session_id = p_session_id;
  -- Busy means any job of that kind is in flight for the session, not only the
  -- latest request's, so a deferred analysis is not re-admitted on every read.
  brief_busy := COALESCE(latest.status IN ('queued', 'running'), false)
    OR EXISTS (SELECT 1 FROM sessions.session_analysis_jobs
                WHERE org_id = p_org_id AND session_id = p_session_id
                  AND kind = 'brief' AND status IN ('queued', 'running'));
  analysis_busy := COALESCE(latest.status IN ('queued', 'running'), false)
    OR EXISTS (SELECT 1 FROM sessions.session_analysis_jobs
                WHERE org_id = p_org_id AND session_id = p_session_id
                  AND kind = 'analysis' AND status IN ('queued', 'running'));

  exhausted := EXISTS (SELECT 1 FROM sessions.session_analysis_admissions
                        WHERE org_id = p_org_id AND day = (now() AT TIME ZONE 'UTC')::date
                          AND requests >= 20);

  -- Another attempt for the same revision and engine is made only when it can
  -- change the result: a transient failure after a cooldown, a failed job that
  -- still has its single requeue, a deferred analysis, or a refused analysis once
  -- model admission has room again.
  retryable := COALESCE(latest.id IS NULL
    OR latest.source_revision IS DISTINCT FROM current_revision
    -- The claim records the handling worker's engine, so every finished request
    -- has one; a request from before 0024 retries once under the current engine.
    OR latest.engine IS DISTINCT FROM engine
    OR (latest.status = 'failed'
        AND latest.last_error IN ('session_collection_failed', 'request_lease_expired')
        AND latest.updated_at <= now() - interval '1 hour')
    OR (latest.status = 'resolved' AND (
          (brief_job.status = 'failed' AND brief_job.requeues < 1)
       OR (analysis_job.status = 'failed' AND analysis_job.requeues < 1)
       OR (latest.analysis_job_id IS NULL AND latest.last_error IS NULL)
       OR (latest.analysis_job_id IS NULL AND NOT exhausted
           AND latest.updated_at <= now() - interval '1 hour'))), false);

  -- Each kind is judged on its own: a slow analysis never holds back a brief. An
  -- analysis starts at most once per 30 minutes per session and never while the
  -- org's model admission is exhausted.
  brief_due := NOT brief_fresh AND NOT brief_busy;
  analysis_due := NOT analysis_fresh AND NOT analysis_busy AND NOT exhausted
    AND NOT sessions.session_analysis_cooling(p_org_id, p_session_id);
  -- A throttled read returns the stale result with a non-polling status.
  request_limit := sessions.session_request_limit(p_org_id, p_session_id);

  IF (brief_due OR analysis_due) AND retryable AND request_limit IS DISTINCT FROM 'throttled' THEN
    IF request_limit = 'capped' OR sessions.sessions_admitted_today(p_org_id, p_session_id) >= 20 THEN
      limited := true;
    ELSE
      INSERT INTO sessions.session_analysis_requests
        (org_id, workspace_id, session_id, requested_by_user_id, source_revision, engine)
        VALUES (p_org_id, p_workspace_id, p_session_id, p_user_id, current_revision, engine)
        RETURNING * INTO latest;
      admitted_id := latest.id;
      brief_job := NULL;
      analysis_job := NULL;
      -- Only the kinds that were due are now in flight; the other keeps its state.
      brief_busy := brief_busy OR brief_due;
      analysis_busy := analysis_busy OR analysis_due;
    END IF;
  END IF;

  request_status := CASE
    WHEN latest.status <> 'resolved' THEN latest.status
    WHEN brief_job.status = 'failed' OR analysis_job.status = 'failed' THEN 'failed'
    WHEN brief_job.status = 'running' OR analysis_job.status = 'running' THEN 'running'
    WHEN brief_job.status = 'queued' OR analysis_job.status = 'queued' THEN 'queued'
    ELSE 'completed' END;
  brief_error := CASE WHEN latest.status = 'failed' THEN latest.last_error
                      WHEN brief_job.status = 'failed' THEN brief_job.last_error END;
  -- An analysis deferred by a failed run's cooldown reports that failure.
  SELECT last_error INTO recent_failure FROM sessions.session_analysis_jobs
   WHERE org_id = p_org_id AND session_id = p_session_id AND kind = 'analysis'
     AND last_error IS DISTINCT FROM 'superseded' AND status IN ('completed', 'failed')
   ORDER BY COALESCE(completed_at, updated_at) DESC, id DESC LIMIT 1;
  analysis_error := CASE WHEN latest.status = 'failed' THEN latest.last_error
                         WHEN analysis_job.status = 'failed' THEN analysis_job.last_error
                         WHEN latest.status = 'resolved' AND latest.analysis_job_id IS NULL
                           THEN COALESCE(latest.last_error, recent_failure) END;

  RETURN jsonb_build_object(
    'outcome', 'ok',
    'sessionId', p_session_id,
    'admittedRequestId', admitted_id,
    'status', CASE
      WHEN brief_fresh THEN 'ready'
      WHEN brief_busy THEN CASE WHEN brief.job_id IS NULL THEN 'pending' ELSE 'refreshing' END
      WHEN limited THEN 'rate_limited'
      WHEN brief_error IS NOT NULL OR brief.job_id IS NULL THEN 'failed'
      ELSE 'stale' END,
    'stale', brief.job_id IS NOT NULL AND NOT brief_fresh,
    'briefId', brief.job_id,
    'brief', brief.result,
    'generatedAt', brief.completed_at,
    'error', CASE WHEN brief_fresh THEN NULL ELSE brief_error END,
    'analysis', jsonb_build_object(
      'status', CASE
        WHEN analysis_fresh THEN 'ready'
        WHEN analysis_busy THEN CASE WHEN analysis.job_id IS NULL THEN 'pending' ELSE 'refreshing' END
        WHEN limited OR exhausted OR analysis_error = 'session_analysis_rate_limited' THEN 'rate_limited'
        WHEN analysis_error IS NOT NULL THEN 'failed'
        WHEN analysis.job_id IS NULL THEN 'pending'
        ELSE 'stale' END,
      'stale', analysis.job_id IS NOT NULL AND NOT analysis_fresh,
      'id', analysis.job_id,
      'result', analysis.result,
      'generatedAt', analysis.completed_at,
      'error', CASE WHEN analysis_fresh THEN NULL ELSE analysis_error END),
    'request', CASE WHEN latest.id IS NULL THEN NULL ELSE jsonb_build_object(
      'id', latest.id, 'sessionId', latest.session_id, 'status', request_status,
      'analysisId', latest.analysis_job_id,
      'error', COALESCE(brief_error, analysis_error),
      'createdAt', latest.created_at, 'updatedAt', latest.updated_at) END);
END;
$session_brief$;

-- The v1 SQL contract predates separate brief jobs: one job per request whose
-- result holds both `analysis` and `brief`. Each request is projected that way, so
-- v1 readers (Cloud's session summary) keep their contract until they move to v2.
CREATE FUNCTION sessions.session_requests_v1_projection(p_org_id text, p_session_id text)
RETURNS TABLE (
  request_id uuid, session_id text, status text, error text, job_id uuid,
  request_created_at timestamptz, request_updated_at timestamptz,
  source_provider text, source_fingerprint text, attempts integer,
  analysis_version text, schema_version integer, summary_version text,
  model_version text, summary_model_version text, result jsonb,
  job_created_at timestamptz, job_updated_at timestamptz, job_completed_at timestamptz
) LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $v1_projection$
  SELECT r.id, r.session_id,
    CASE WHEN r.status <> 'resolved' THEN r.status
         WHEN b.id IS NULL THEN COALESCE(a.status, 'failed')
         WHEN b.status = 'failed' OR a.status = 'failed' THEN 'failed'
         WHEN b.status = 'running' OR a.status = 'running' THEN 'running'
         WHEN b.status = 'queued' OR a.status = 'queued' THEN 'queued'
         WHEN a.id IS NULL AND r.last_error IS NOT NULL THEN 'failed'
         ELSE 'completed' END,
    CASE WHEN r.status = 'failed' THEN r.last_error
         WHEN r.status = 'resolved' THEN COALESCE(
           CASE WHEN b.status = 'failed' THEN b.last_error END,
           CASE WHEN a.status = 'failed' THEN a.last_error END,
           CASE WHEN a.id IS NULL THEN r.last_error END) END,
    -- Each request is its own v1 job, so no two requests share an ID with
    -- different states. Requests from before 0024 keep their analysis job's ID.
    CASE WHEN b.id IS NULL THEN a.id ELSE md5('v1-job:' || r.id::text)::uuid END,
    r.created_at, r.updated_at,
    COALESCE(a.source_provider, b.source_provider),
    COALESCE(a.source_fingerprint, b.source_fingerprint),
    COALESCE(a.attempts, b.attempts),
    COALESCE(a.analysis_version, ''), COALESCE(a.schema_version, 0),
    COALESCE(NULLIF(b.summary_version, ''), a.summary_version, ''),
    COALESCE(a.model_version, ''),
    COALESCE(NULLIF(b.summary_model_version, ''), a.summary_model_version, ''),
    CASE WHEN b.id IS NULL THEN a.result
         WHEN b.status = 'completed' AND (a.id IS NULL OR a.status = 'completed')
           THEN COALESCE(a.result, '{}'::jsonb) || jsonb_build_object('brief', b.result) END,
    COALESCE(a.created_at, b.created_at),
    GREATEST(a.updated_at, b.updated_at),
    CASE WHEN b.id IS NULL THEN a.completed_at
         WHEN b.status = 'completed' AND (a.id IS NULL OR a.status = 'completed')
           THEN GREATEST(a.completed_at, b.completed_at) END
    FROM sessions.session_analysis_requests AS r
    -- A superseded analysis moved to a newer request; this request keeps its brief.
    LEFT JOIN sessions.session_analysis_jobs AS a ON a.id = r.analysis_job_id
      AND a.org_id = r.org_id AND a.session_id = r.session_id
      AND NOT (a.status = 'failed' AND a.last_error = 'superseded' AND r.brief_job_id IS NOT NULL)
    LEFT JOIN sessions.session_analysis_jobs AS b ON b.id = r.brief_job_id
      AND b.org_id = r.org_id AND b.session_id = r.session_id
   WHERE r.org_id = p_org_id AND r.session_id = p_session_id;
$v1_projection$;

CREATE OR REPLACE FUNCTION sessions.read_session_analysis_v1(
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
    'id', v.request_id, 'sessionId', v.session_id, 'status', v.status,
    'analysisId', v.job_id, 'error', v.error,
    'createdAt', v.request_created_at, 'updatedAt', v.request_updated_at
  ) ORDER BY v.request_created_at DESC, v.request_id DESC), '[]'::jsonb) INTO public_requests
  FROM (SELECT * FROM sessions.session_requests_v1_projection(p_org_id, p_session_id)
         ORDER BY request_created_at DESC, request_id DESC LIMIT 50) AS v;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'id', v.job_id, 'sessionId', v.session_id, 'sourceProvider', v.source_provider,
    'sourceFingerprint', v.source_fingerprint, 'status', v.status, 'attempts', v.attempts,
    'analysisVersion', v.analysis_version, 'schemaVersion', v.schema_version,
    'summaryVersion', v.summary_version, 'modelVersion', v.model_version,
    'summaryModelVersion', v.summary_model_version, 'result', v.result,
    'error', CASE WHEN v.status = 'failed' THEN v.error END,
    'createdAt', v.job_created_at, 'updatedAt', v.job_updated_at, 'completedAt', v.job_completed_at
  ) ORDER BY v.job_created_at DESC, v.job_id DESC), '[]'::jsonb) INTO public_analyses
  FROM (SELECT DISTINCT ON (job_id) *
          FROM (SELECT * FROM sessions.session_requests_v1_projection(p_org_id, p_session_id)
                 ORDER BY request_created_at DESC, request_id DESC LIMIT 50) AS recent
         WHERE job_id IS NOT NULL
         ORDER BY job_id, request_created_at DESC, request_id DESC) AS v;

  RETURN jsonb_build_object('outcome', 'ok', 'sessionId', p_session_id,
    'requests', public_requests, 'analyses', public_analyses);
END;
$read_analysis$;

-- Unchanged v1 contract; the in-flight check follows the projection above and the
-- daily budget counts distinct sessions, shared with session_brief_v2.
CREATE OR REPLACE FUNCTION sessions.request_session_analysis_v1(
  p_org_id text, p_workspace_id text, p_user_id text, p_session_id text
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $request_analysis$
DECLARE
  existing sessions.session_analysis_requests%ROWTYPE;
  active record;
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

  SELECT * INTO active FROM sessions.session_requests_v1_projection(p_org_id, p_session_id)
   WHERE status IN ('queued', 'running')
   ORDER BY request_created_at DESC, request_id DESC LIMIT 1;
  IF active.request_id IS NOT NULL THEN
    RETURN jsonb_build_object('outcome', 'accepted', 'request', jsonb_build_object(
      'id', active.request_id, 'sessionId', active.session_id, 'status', active.status,
      'analysisId', active.job_id, 'error', NULL,
      'createdAt', active.request_created_at, 'updatedAt', active.request_updated_at));
  END IF;

  -- The per-session limits shared with session_brief_v2: a throttled request
  -- returns the session's latest request.
  IF sessions.session_request_limit(p_org_id, p_session_id) = 'throttled' THEN
    SELECT * INTO active FROM sessions.session_requests_v1_projection(p_org_id, p_session_id)
     ORDER BY request_created_at DESC, request_id DESC LIMIT 1;
    RETURN jsonb_build_object('outcome', 'accepted', 'request', jsonb_build_object(
      'id', active.request_id, 'sessionId', active.session_id, 'status', active.status,
      'analysisId', active.job_id, 'error', active.error,
      'createdAt', active.request_created_at, 'updatedAt', active.request_updated_at));
  END IF;
  IF sessions.session_request_limit(p_org_id, p_session_id) = 'capped'
     OR sessions.sessions_admitted_today(p_org_id, p_session_id) >= 20 THEN
    RETURN jsonb_build_object('outcome', 'rate_limited');
  END IF;

  INSERT INTO sessions.session_analysis_requests (org_id, workspace_id, session_id, requested_by_user_id)
    VALUES (p_org_id, p_workspace_id, p_session_id, p_user_id)
    RETURNING * INTO existing;
  RETURN jsonb_build_object('outcome', 'accepted', 'request', jsonb_build_object(
    'id', existing.id, 'sessionId', existing.session_id, 'status', existing.status,
    'analysisId', NULL, 'error', NULL,
    'createdAt', existing.created_at, 'updatedAt', existing.updated_at));
END;
$request_analysis$;

REVOKE ALL ON FUNCTION sessions.session_analysis_cooling(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.sessions_admitted_today(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.session_request_limit(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.session_requests_v1_projection(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.bump_session_revisions() FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.bump_updated_revisions() FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.current_session_revision(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.cached_session_job(text, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.enqueue_session_job_v2(text, text, text, text, text, text, text, text, integer, text, text, text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.collect_session_analysis_request_v2(uuid, uuid, text, text, text, text, jsonb, text, text, integer, text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.session_brief_v2(text, text, text, text, text, text) FROM PUBLIC;
