-- Per-session rollups of convergence_events, so GET /v1/sessions reads one row per
-- session from an index instead of grouping every event in the organization.
--
-- Grain. One row per (org, session, workspace, source, project_id): the columns the
-- session list can filter on without reading events. The list merges a session's
-- rows at read time, so a session spread over several workspaces or projects still
-- reads as one session, and a workspace or project filter reads exactly the rows
-- whose events match it. Filters over event fields (kind, task ref, tag, time
-- window, text search) keep the event aggregate.
--
-- Maintenance. Statement-level triggers on convergence_events keep the rollups in
-- the writer's transaction for every writer: legacy ingest, the delivery projection
-- and any delete. Inserts merge their rows into the stored totals, and updates
-- that only settle a row's usage apply the difference. Deletes and other updates to
-- a rolled-up column (the delivery projection moves tool rows from their delivery
-- time to their parent's) mark the session dirty, and a deferred trigger rebuilds
-- each dirty session from its events once, at commit: one pass over the session
-- per transaction that touches it. Embedding and replay updates change nothing.
--
-- Concurrency. Merges, usage differences, marks and rebuilds take a transaction
-- advisory lock per (org, session), in key order within a statement, before
-- reading or writing that session's rollups. A rebuild
-- therefore reads its events after any concurrent writer to the same session has
-- committed, and a merge applies on top of a rebuild that could not see the
-- merging writer's rows.
--
-- Rollout. This migration is schema only. Sessions written before it are rolled
-- up by scripts/rollout-session-rollups.mjs, which calls
-- session_rollup_backfill_step() in short batches that never wait on an upload's
-- lock. The list reads the rollups once session_rollup_rollout.completed_at is
-- set; until then it keeps the event aggregate.

CREATE TABLE sessions.session_rollups (
  org_id text NOT NULL,
  session_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  project_id text,
  first_ts timestamptz NOT NULL,
  last_ts timestamptz NOT NULL,
  event_count bigint NOT NULL,
  user_ids text[] NOT NULL,
  machine_ids text[] NOT NULL,
  kinds text[] NOT NULL,
  models text[] NOT NULL,
  -- The newest non-null task title, ordered by (ts, title) so merges and rebuilds
  -- agree when two titles share a timestamp.
  task_title text,
  task_title_ts timestamptz,
  -- Distinct non-empty task refs, as a jsonb array.
  task_refs jsonb NOT NULL,
  -- NULL when no event carried a cost; cost_events counts the events that did.
  cost_usd_micros bigint,
  cost_events bigint NOT NULL,
  input_tokens bigint NOT NULL,
  output_tokens bigint NOT NULL,
  reasoning_tokens bigint NOT NULL,
  cache_read_tokens bigint NOT NULL,
  cache_create_tokens bigint NOT NULL,
  cache_create_5m_tokens bigint NOT NULL,
  cache_create_1h_tokens bigint NOT NULL,
  -- Events that wrote cache without a 5m/1h split covering the write. The session's
  -- split is reported only when this is zero.
  cache_split_incomplete_events bigint NOT NULL,
  CONSTRAINT session_rollups_key
    UNIQUE NULLS NOT DISTINCT (org_id, session_id, workspace_id, source, project_id)
);
-- Keyset pages, newest activity first: organization-wide, per workspace, per project.
CREATE INDEX session_rollups_org_recent_idx
  ON sessions.session_rollups (org_id, last_ts DESC, session_id DESC, source DESC);
CREATE INDEX session_rollups_workspace_recent_idx
  ON sessions.session_rollups (org_id, workspace_id, last_ts DESC, session_id DESC, source DESC);
CREATE INDEX session_rollups_project_recent_idx
  ON sessions.session_rollups (org_id, project_id, last_ts DESC, session_id DESC, source DESC);

-- Sorted distinct union, the same order array_agg(DISTINCT …) produces.
CREATE FUNCTION sessions.session_rollup_text_union(a text[], b text[])
RETURNS text[] LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, sessions, pg_temp AS $text_union$
  SELECT coalesce(array_agg(DISTINCT v), '{}') FROM unnest(a || b) AS v
$text_union$;

CREATE FUNCTION sessions.session_rollup_jsonb_union(a jsonb, b jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, sessions, pg_temp AS $jsonb_union$
  SELECT coalesce(jsonb_agg(DISTINCT v), '[]'::jsonb) FROM jsonb_array_elements(a || b) AS v
$jsonb_union$;

-- An event either wrote no cache or reported a 5m/1h split covering its write.
CREATE FUNCTION sessions.session_rollup_split_complete(
  p_create bigint, p_5m bigint, p_1h bigint
) RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $split_complete$
  SELECT p_create = 0
    OR coalesce(p_5m, 0) + coalesce(p_1h, 0) = p_create AND (p_5m IS NOT NULL OR p_1h IS NOT NULL)
$split_complete$;

-- Merge a session's rows at read time.
CREATE AGGREGATE sessions.session_rollup_text_union_agg(text[]) (
  SFUNC = sessions.session_rollup_text_union,
  STYPE = text[],
  INITCOND = '{}'
);
CREATE AGGREGATE sessions.session_rollup_jsonb_union_agg(jsonb) (
  SFUNC = sessions.session_rollup_jsonb_union,
  STYPE = jsonb,
  INITCOND = '[]'
);

CREATE FUNCTION sessions.session_rollup_lock_key(p_org_id text, p_session_id text)
RETURNS bigint LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, sessions, pg_temp AS $lock_key$
  SELECT hashtextextended('session-rollup:' || p_org_id || ':' || p_session_id, 0)
$lock_key$;

-- Rebuild the given sessions from their events. The caller holds their locks. The
-- SELECT is the same aggregate merge_inserted_session_rollups() applies to new rows.
CREATE FUNCTION sessions.rebuild_session_rollups(p_org_ids text[], p_session_ids text[])
RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $rebuild$
BEGIN
  DELETE FROM sessions.session_rollups AS r
   USING unnest(p_org_ids, p_session_ids) AS k(org_id, session_id)
   WHERE r.org_id = k.org_id AND r.session_id = k.session_id;
  INSERT INTO sessions.session_rollups (
    org_id, session_id, workspace_id, source, project_id,
    first_ts, last_ts, event_count, user_ids, machine_ids, kinds, models,
    task_title, task_title_ts, task_refs, cost_usd_micros, cost_events,
    input_tokens, output_tokens, reasoning_tokens, cache_read_tokens,
    cache_create_tokens, cache_create_5m_tokens, cache_create_1h_tokens,
    cache_split_incomplete_events)
  SELECT e.org_id, e.session_id, e.workspace_id, e.source, e.project_id,
         min(e.ts), max(e.ts), count(*),
         array_agg(DISTINCT e.user_id),
         array_agg(DISTINCT e.machine_id),
         array_agg(DISTINCT e.kind),
         coalesce(array_agg(DISTINCT e.model) FILTER (WHERE e.model IS NOT NULL), '{}'),
         (array_agg(e.task_title ORDER BY e.ts DESC, e.task_title DESC)
            FILTER (WHERE e.task_title IS NOT NULL))[1],
         max(e.ts) FILTER (WHERE e.task_title IS NOT NULL),
         coalesce(jsonb_agg(DISTINCT e.task_ref) FILTER (WHERE e.task_ref <> '{}'::jsonb), '[]'::jsonb),
         sum(e.cost_usd_micros),
         count(e.cost_usd_micros),
         coalesce(sum(e.input_tokens), 0),
         coalesce(sum(e.output_tokens), 0),
         coalesce(sum(e.reasoning_tokens), 0),
         coalesce(sum(e.cache_read_tokens), 0),
         coalesce(sum(e.cache_create_tokens), 0),
         coalesce(sum(e.cache_create_5m_tokens), 0),
         coalesce(sum(e.cache_create_1h_tokens), 0),
         count(*) FILTER (WHERE NOT sessions.session_rollup_split_complete(
           e.cache_create_tokens, e.cache_create_5m_tokens, e.cache_create_1h_tokens))
    FROM sessions.convergence_events AS e
    JOIN unnest(p_org_ids, p_session_ids) AS k(org_id, session_id)
      ON e.org_id = k.org_id AND e.session_id = k.session_id
   GROUP BY e.org_id, e.session_id, e.workspace_id, e.source, e.project_id;
END;
$rebuild$;

-- Sessions whose rollups an update or delete invalidated in the current
-- transaction. Each row is rebuilt once at commit and removed, so a committed
-- row never exists; a delivery batch that settles hundreds of rows of one session
-- rebuilds it once, not once per row.
CREATE TABLE sessions.session_rollup_dirty (
  org_id text NOT NULL,
  session_id text NOT NULL,
  PRIMARY KEY (org_id, session_id)
);

CREATE FUNCTION sessions.rebuild_dirty_session_rollup() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $rebuild_dirty$
BEGIN
  PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(NEW.org_id, NEW.session_id));
  PERFORM sessions.rebuild_session_rollups(ARRAY[NEW.org_id], ARRAY[NEW.session_id]);
  DELETE FROM sessions.session_rollup_dirty
   WHERE org_id = NEW.org_id AND session_id = NEW.session_id;
  RETURN NULL;
END;
$rebuild_dirty$;

CREATE CONSTRAINT TRIGGER session_rollup_dirty_rebuild
  AFTER INSERT ON sessions.session_rollup_dirty
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION sessions.rebuild_dirty_session_rollup();

-- Merges inserted rows into the stored totals. Sessions already marked dirty in
-- this transaction are skipped: their rebuild at commit reads these rows too.
CREATE FUNCTION sessions.merge_inserted_session_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $merge_inserted$
DECLARE
  k record;
BEGIN
  FOR k IN SELECT DISTINCT org_id, session_id FROM changed_rows ORDER BY org_id, session_id LOOP
    PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(k.org_id, k.session_id));
  END LOOP;
  INSERT INTO sessions.session_rollups AS r (
    org_id, session_id, workspace_id, source, project_id,
    first_ts, last_ts, event_count, user_ids, machine_ids, kinds, models,
    task_title, task_title_ts, task_refs, cost_usd_micros, cost_events,
    input_tokens, output_tokens, reasoning_tokens, cache_read_tokens,
    cache_create_tokens, cache_create_5m_tokens, cache_create_1h_tokens,
    cache_split_incomplete_events)
  SELECT e.org_id, e.session_id, e.workspace_id, e.source, e.project_id,
         min(e.ts), max(e.ts), count(*),
         array_agg(DISTINCT e.user_id),
         array_agg(DISTINCT e.machine_id),
         array_agg(DISTINCT e.kind),
         coalesce(array_agg(DISTINCT e.model) FILTER (WHERE e.model IS NOT NULL), '{}'),
         (array_agg(e.task_title ORDER BY e.ts DESC, e.task_title DESC)
            FILTER (WHERE e.task_title IS NOT NULL))[1],
         max(e.ts) FILTER (WHERE e.task_title IS NOT NULL),
         coalesce(jsonb_agg(DISTINCT e.task_ref) FILTER (WHERE e.task_ref <> '{}'::jsonb), '[]'::jsonb),
         sum(e.cost_usd_micros),
         count(e.cost_usd_micros),
         coalesce(sum(e.input_tokens), 0),
         coalesce(sum(e.output_tokens), 0),
         coalesce(sum(e.reasoning_tokens), 0),
         coalesce(sum(e.cache_read_tokens), 0),
         coalesce(sum(e.cache_create_tokens), 0),
         coalesce(sum(e.cache_create_5m_tokens), 0),
         coalesce(sum(e.cache_create_1h_tokens), 0),
         count(*) FILTER (WHERE NOT sessions.session_rollup_split_complete(
           e.cache_create_tokens, e.cache_create_5m_tokens, e.cache_create_1h_tokens))
    FROM changed_rows AS e
   WHERE NOT EXISTS (
     SELECT 1 FROM sessions.session_rollup_dirty AS d
      WHERE d.org_id = e.org_id AND d.session_id = e.session_id)
   GROUP BY e.org_id, e.session_id, e.workspace_id, e.source, e.project_id
   ORDER BY e.org_id, e.session_id, e.workspace_id, e.source, e.project_id
  ON CONFLICT ON CONSTRAINT session_rollups_key DO UPDATE SET
    first_ts = least(r.first_ts, excluded.first_ts),
    last_ts = greatest(r.last_ts, excluded.last_ts),
    event_count = r.event_count + excluded.event_count,
    user_ids = sessions.session_rollup_text_union(r.user_ids, excluded.user_ids),
    machine_ids = sessions.session_rollup_text_union(r.machine_ids, excluded.machine_ids),
    kinds = sessions.session_rollup_text_union(r.kinds, excluded.kinds),
    models = sessions.session_rollup_text_union(r.models, excluded.models),
    task_title = CASE
      WHEN excluded.task_title_ts IS NULL THEN r.task_title
      WHEN r.task_title_ts IS NULL
        OR (excluded.task_title_ts, excluded.task_title) > (r.task_title_ts, r.task_title)
        THEN excluded.task_title
      ELSE r.task_title END,
    task_title_ts = greatest(r.task_title_ts, excluded.task_title_ts),
    task_refs = sessions.session_rollup_jsonb_union(r.task_refs, excluded.task_refs),
    cost_usd_micros = CASE
      WHEN r.cost_usd_micros IS NULL THEN excluded.cost_usd_micros
      WHEN excluded.cost_usd_micros IS NULL THEN r.cost_usd_micros
      ELSE r.cost_usd_micros + excluded.cost_usd_micros END,
    cost_events = r.cost_events + excluded.cost_events,
    input_tokens = r.input_tokens + excluded.input_tokens,
    output_tokens = r.output_tokens + excluded.output_tokens,
    reasoning_tokens = r.reasoning_tokens + excluded.reasoning_tokens,
    cache_read_tokens = r.cache_read_tokens + excluded.cache_read_tokens,
    cache_create_tokens = r.cache_create_tokens + excluded.cache_create_tokens,
    cache_create_5m_tokens = r.cache_create_5m_tokens + excluded.cache_create_5m_tokens,
    cache_create_1h_tokens = r.cache_create_1h_tokens + excluded.cache_create_1h_tokens,
    cache_split_incomplete_events = r.cache_split_incomplete_events + excluded.cache_split_incomplete_events;
  RETURN NULL;
END;
$merge_inserted$;

-- Marks sessions dirty, taking their locks first and in key order, so the rebuild
-- at commit and any wait on another writer's mark happen under a lock this
-- transaction already holds.
CREATE FUNCTION sessions.mark_session_rollups_dirty(p_keys jsonb)
RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $mark_dirty$
DECLARE
  k record;
BEGIN
  FOR k IN SELECT DISTINCT o.org_id, o.session_id
             FROM jsonb_to_recordset(p_keys) AS o(org_id text, session_id text)
            ORDER BY o.org_id, o.session_id LOOP
    PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(k.org_id, k.session_id));
    INSERT INTO sessions.session_rollup_dirty (org_id, session_id)
      VALUES (k.org_id, k.session_id)
      ON CONFLICT DO NOTHING;
  END LOOP;
END;
$mark_dirty$;

CREATE FUNCTION sessions.mark_deleted_session_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $mark_deleted$
BEGIN
  PERFORM sessions.mark_session_rollups_dirty(
    (SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object('org_id', org_id, 'session_id', session_id)), '[]')
       FROM changed_rows));
  RETURN NULL;
END;
$mark_deleted$;

-- Updates come in two shapes. The delivery projection settles a row's usage
-- (tokens, cost, cache split) after inserting it; such a row keeps its key and
-- every other rolled-up column, so the difference is applied to the stored totals.
-- Any other change to a rolled-up column (time, title, model, task ref, or a move
-- to another session, workspace or project) marks the sessions on both sides
-- dirty. Embedding writes and identical replays change none of these columns.
CREATE FUNCTION sessions.mark_updated_session_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $mark_updated$
DECLARE
  usage_pairs jsonb;
  dirty jsonb;
  k record;
BEGIN
  WITH changed AS (
    (SELECT org_id, session_id, workspace_id, source, project_id, machine_id, kind, event_id, ts, user_id, model, task_title, task_ref, cost_usd_micros, input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_create_tokens, cache_create_5m_tokens, cache_create_1h_tokens, 'new' AS side FROM new_rows
     EXCEPT SELECT org_id, session_id, workspace_id, source, project_id, machine_id, kind, event_id, ts, user_id, model, task_title, task_ref, cost_usd_micros, input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_create_tokens, cache_create_5m_tokens, cache_create_1h_tokens, 'new' FROM old_rows)
    UNION ALL
    (SELECT org_id, session_id, workspace_id, source, project_id, machine_id, kind, event_id, ts, user_id, model, task_title, task_ref, cost_usd_micros, input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_create_tokens, cache_create_5m_tokens, cache_create_1h_tokens, 'old' AS side FROM old_rows
     EXCEPT SELECT org_id, session_id, workspace_id, source, project_id, machine_id, kind, event_id, ts, user_id, model, task_title, task_ref, cost_usd_micros, input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_create_tokens, cache_create_5m_tokens, cache_create_1h_tokens, 'old' FROM new_rows)
  ),
  -- One row's old and new versions that differ only in usage.
  pairs AS (
    SELECT n.org_id, n.session_id, n.workspace_id, n.source, n.project_id,
           n.machine_id, n.kind, n.event_id,
           coalesce(n.cost_usd_micros, 0) - coalesce(o.cost_usd_micros, 0) AS d_cost,
           (n.cost_usd_micros IS NOT NULL)::int - (o.cost_usd_micros IS NOT NULL)::int AS d_cost_events,
           n.input_tokens - o.input_tokens AS d_input,
           n.output_tokens - o.output_tokens AS d_output,
           n.reasoning_tokens - o.reasoning_tokens AS d_reasoning,
           n.cache_read_tokens - o.cache_read_tokens AS d_cache_read,
           n.cache_create_tokens - o.cache_create_tokens AS d_cache_create,
           coalesce(n.cache_create_5m_tokens, 0) - coalesce(o.cache_create_5m_tokens, 0) AS d_5m,
           coalesce(n.cache_create_1h_tokens, 0) - coalesce(o.cache_create_1h_tokens, 0) AS d_1h,
           (NOT sessions.session_rollup_split_complete(n.cache_create_tokens, n.cache_create_5m_tokens, n.cache_create_1h_tokens))::int
             - (NOT sessions.session_rollup_split_complete(o.cache_create_tokens, o.cache_create_5m_tokens, o.cache_create_1h_tokens))::int
             AS d_incomplete
      FROM changed AS n
      JOIN changed AS o
        ON n.side = 'new' AND o.side = 'old'
       AND n.org_id = o.org_id AND n.session_id = o.session_id AND n.source = o.source
       AND n.machine_id = o.machine_id AND n.kind = o.kind AND n.event_id = o.event_id
       AND n.workspace_id = o.workspace_id
       AND n.project_id IS NOT DISTINCT FROM o.project_id
       AND n.ts = o.ts AND n.user_id = o.user_id
       AND n.model IS NOT DISTINCT FROM o.model
       AND n.task_title IS NOT DISTINCT FROM o.task_title
       AND n.task_ref = o.task_ref
  )
  SELECT (SELECT coalesce(jsonb_agg(to_jsonb(p)), '[]') FROM pairs AS p),
         (SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object('org_id', c.org_id, 'session_id', c.session_id)), '[]')
            FROM changed AS c
           WHERE NOT EXISTS (
             SELECT 1 FROM pairs AS p
              WHERE p.org_id = c.org_id AND p.session_id = c.session_id AND p.source = c.source
                AND p.machine_id = c.machine_id AND p.kind = c.kind AND p.event_id = c.event_id))
    INTO usage_pairs, dirty;

  -- Every other change rebuilds at commit.
  PERFORM sessions.mark_session_rollups_dirty(dirty);
  IF jsonb_array_length(usage_pairs) = 0 THEN RETURN NULL; END IF;

  FOR k IN SELECT DISTINCT p.org_id, p.session_id
             FROM jsonb_to_recordset(usage_pairs) AS p(org_id text, session_id text)
            ORDER BY p.org_id, p.session_id LOOP
    PERFORM pg_advisory_xact_lock(sessions.session_rollup_lock_key(k.org_id, k.session_id));
  END LOOP;
  UPDATE sessions.session_rollups AS r SET
    cost_events = r.cost_events + d.d_cost_events,
    cost_usd_micros = CASE WHEN r.cost_events + d.d_cost_events = 0 THEN NULL
                           ELSE coalesce(r.cost_usd_micros, 0) + d.d_cost END,
    input_tokens = r.input_tokens + d.d_input,
    output_tokens = r.output_tokens + d.d_output,
    reasoning_tokens = r.reasoning_tokens + d.d_reasoning,
    cache_read_tokens = r.cache_read_tokens + d.d_cache_read,
    cache_create_tokens = r.cache_create_tokens + d.d_cache_create,
    cache_create_5m_tokens = r.cache_create_5m_tokens + d.d_5m,
    cache_create_1h_tokens = r.cache_create_1h_tokens + d.d_1h,
    cache_split_incomplete_events = r.cache_split_incomplete_events + d.d_incomplete
    FROM (SELECT p.org_id, p.session_id, p.workspace_id, p.source, p.project_id,
                 sum(p.d_cost) AS d_cost, sum(p.d_cost_events) AS d_cost_events,
                 sum(p.d_input) AS d_input, sum(p.d_output) AS d_output,
                 sum(p.d_reasoning) AS d_reasoning, sum(p.d_cache_read) AS d_cache_read,
                 sum(p.d_cache_create) AS d_cache_create, sum(p.d_5m) AS d_5m,
                 sum(p.d_1h) AS d_1h, sum(p.d_incomplete) AS d_incomplete
            FROM jsonb_to_recordset(usage_pairs) AS p(
                   org_id text, session_id text, workspace_id text, source text, project_id text,
                   d_cost bigint, d_cost_events bigint, d_input bigint, d_output bigint,
                   d_reasoning bigint, d_cache_read bigint, d_cache_create bigint, d_5m bigint,
                   d_1h bigint, d_incomplete bigint)
           GROUP BY p.org_id, p.session_id, p.workspace_id, p.source, p.project_id) AS d
   WHERE r.org_id = d.org_id AND r.session_id = d.session_id
     AND r.workspace_id = d.workspace_id AND r.source = d.source
     AND r.project_id IS NOT DISTINCT FROM d.project_id
     -- A session rebuilt at commit reads the new values itself.
     AND NOT EXISTS (SELECT 1 FROM sessions.session_rollup_dirty AS x
                      WHERE x.org_id = r.org_id AND x.session_id = r.session_id);
  RETURN NULL;
END;
$mark_updated$;

CREATE FUNCTION sessions.truncate_session_rollups() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $truncate_rollups$
BEGIN
  TRUNCATE sessions.session_rollups;
  RETURN NULL;
END;
$truncate_rollups$;

CREATE TRIGGER convergence_events_rollup_insert
  AFTER INSERT ON sessions.convergence_events REFERENCING NEW TABLE AS changed_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.merge_inserted_session_rollups();
CREATE TRIGGER convergence_events_rollup_delete
  AFTER DELETE ON sessions.convergence_events REFERENCING OLD TABLE AS changed_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.mark_deleted_session_rollups();
CREATE TRIGGER convergence_events_rollup_update
  AFTER UPDATE ON sessions.convergence_events REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.mark_updated_session_rollups();
CREATE TRIGGER convergence_events_rollup_truncate
  AFTER TRUNCATE ON sessions.convergence_events
  FOR EACH STATEMENT EXECUTE FUNCTION sessions.truncate_session_rollups();

-- Backfill progress. A database with no events has nothing to backfill.
CREATE TABLE sessions.session_rollup_rollout (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  -- Last (org_id, session_id) rebuilt; exclusive lower bound of the next batch.
  cursor_org_id text,
  cursor_session_id text,
  completed_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO sessions.session_rollup_rollout (completed_at)
  SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM sessions.convergence_events) THEN now() END;

-- Rebuilds the next p_batch sessions in (org_id, session_id) order and returns how
-- many it rebuilt; fewer than p_batch marks the rollout complete. A session an
-- upload is writing raises lock_not_available (55P03) instead of waiting, and the
-- caller retries the batch; once its locks are taken, an upload to one of its
-- sessions waits for the batch to commit (bounded by the caller's statement_timeout).
CREATE FUNCTION sessions.session_rollup_backfill_step(p_batch integer)
RETURNS integer LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $backfill_step$
DECLARE
  state sessions.session_rollup_rollout%ROWTYPE;
  orgs text[];
  ids text[];
  i integer;
  found_count integer;
BEGIN
  SELECT * INTO state FROM sessions.session_rollup_rollout FOR UPDATE;
  IF state.completed_at IS NOT NULL THEN RETURN 0; END IF;

  IF state.cursor_org_id IS NULL THEN
    SELECT array_agg(k.org_id ORDER BY k.org_id, k.session_id),
           array_agg(k.session_id ORDER BY k.org_id, k.session_id)
      INTO orgs, ids
      FROM (SELECT DISTINCT org_id, session_id FROM sessions.convergence_events
             ORDER BY org_id, session_id LIMIT p_batch) AS k;
  ELSE
    SELECT array_agg(k.org_id ORDER BY k.org_id, k.session_id),
           array_agg(k.session_id ORDER BY k.org_id, k.session_id)
      INTO orgs, ids
      FROM (SELECT DISTINCT org_id, session_id FROM sessions.convergence_events
             WHERE (org_id, session_id) > (state.cursor_org_id, state.cursor_session_id)
             ORDER BY org_id, session_id LIMIT p_batch) AS k;
  END IF;

  found_count := coalesce(array_length(orgs, 1), 0);
  FOR i IN 1 .. found_count LOOP
    IF NOT pg_try_advisory_xact_lock(sessions.session_rollup_lock_key(orgs[i], ids[i])) THEN
      RAISE EXCEPTION 'session rollup backfill yielded to a concurrent write'
        USING ERRCODE = '55P03';
    END IF;
  END LOOP;
  IF found_count > 0 THEN
    PERFORM sessions.rebuild_session_rollups(orgs, ids);
  END IF;

  UPDATE sessions.session_rollup_rollout SET
    cursor_org_id = coalesce(orgs[found_count], cursor_org_id),
    cursor_session_id = coalesce(ids[found_count], cursor_session_id),
    completed_at = CASE WHEN found_count < p_batch THEN now() END,
    updated_at = now();
  RETURN found_count;
END;
$backfill_step$;
REVOKE ALL ON FUNCTION sessions.session_rollup_backfill_step(integer) FROM PUBLIC;
