-- Online rollout of the 0022 session catalog and the 0023 activity projection v2.
--
-- 0022 and 0023 used to build their indexes, switch their triggers on and backfill
-- every retained record inside the deploy's migration transaction. That holds the
-- delivery_records write lock until commit, and on 2026-10-02/03 it blocked every
-- upload for over six hours without finishing. A migration is now schema only; the
-- work that grows with stored history runs afterwards, from
-- scripts/rollout-delivery-projection.mjs, in four resumable stages:
--
--   1. Build sessions.delivery_rollout_indexes() with CREATE INDEX CONCURRENTLY.
--   2. activate_delivery_catalog() creates the catalog triggers, then
--      delivery_catalog_backfill_step() projects every retained key in batches.
--   3. activate_delivery_projection_v2() points the activity trigger at v2, then
--      delivery_activity_reproject_step() reprojects retained records in batches.
--
-- Every step takes the workspace projection lock that delivery writes take, so it
-- interleaves with uploads instead of excluding them, and each call is one short
-- transaction that records its cursor before returning.

CREATE TABLE sessions.delivery_rollout (
  -- 'catalog' or 'activity'
  stage text PRIMARY KEY,
  -- Snapshot of the activating transaction: rows it could not see were written by
  -- transactions that committed after the projection went live.
  activated_snapshot pg_snapshot NOT NULL,
  activated_at timestamptz NOT NULL DEFAULT now(),
  -- Last key or record processed, exclusive lower bound of the next batch.
  cursor jsonb,
  completed_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- The indexes the projections read through, each over a table that grows with
-- stored history. Definitions are the ones 0022/0023 used to create in-transaction.
CREATE FUNCTION sessions.delivery_rollout_indexes()
RETURNS TABLE (stage text, index_name text, definition text)
LANGUAGE sql IMMUTABLE AS $indexes$
  VALUES
    ('catalog', 'delivery_records_catalog_key',
     'ON sessions.delivery_records (org_id, workspace_id, sessions.delivery_catalog_key(kind, source, session_id, operation, payload)) WHERE kind IN (''session'', ''relationship'', ''session_marker'', ''commit_link'')'),
    ('activity', 'convergence_events_delivery_record_idx',
     'ON sessions.convergence_events (org_id, workspace_id, delivery_record_id) WHERE delivery_record_id IS NOT NULL'),
    ('activity', 'convergence_events_delivery_request_idx',
     'ON sessions.convergence_events (org_id, workspace_id, source, session_id, request_key) WHERE request_key IS NOT NULL'),
    ('activity', 'convergence_events_delivery_tool_use_idx',
     'ON sessions.convergence_events (org_id, workspace_id, source, session_id, ((record->''payload''->>''tool_use_id''))) WHERE delivery_record_id IS NOT NULL AND (record->''payload''->>''tool_use_id'') IS NOT NULL'),
    ('activity', 'delivery_records_record_idx',
     'ON sessions.delivery_records (org_id, workspace_id, record_id)')
$indexes$;

-- True when every index of the stage exists and is valid. An interrupted
-- CREATE INDEX CONCURRENTLY leaves an invalid index behind; the script rebuilds it.
CREATE FUNCTION sessions.delivery_rollout_indexes_ready(p_stage text)
RETURNS boolean LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $ready$
  SELECT NOT EXISTS (
    SELECT 1 FROM sessions.delivery_rollout_indexes() AS wanted
     WHERE wanted.stage = p_stage
       AND NOT EXISTS (
         SELECT 1 FROM pg_index AS i
           JOIN pg_class AS c ON c.oid = i.indexrelid
           JOIN pg_namespace AS n ON n.oid = c.relnamespace
          WHERE n.nspname = 'sessions' AND c.relname = wanted.index_name
            AND i.indisvalid AND i.indisready))
$ready$;

-- Whether the stage still needs its online indexes. This check intentionally runs
-- before the rollout script reads/builds an index definition: some databases that
-- applied an earlier 0022 have the live catalog triggers but do not have the
-- delivery_catalog_key helper referenced by the current index definition.
CREATE FUNCTION sessions.delivery_rollout_stage_requires_indexes(p_stage text)
RETURNS boolean LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $requires_indexes$
DECLARE
  completed timestamptz;
BEGIN
  SELECT completed_at INTO completed
    FROM sessions.delivery_rollout WHERE stage = p_stage;
  IF FOUND THEN RETURN completed IS NULL; END IF;
  IF p_stage = 'catalog' THEN
    RETURN NOT EXISTS (
      SELECT 1 FROM pg_trigger
       WHERE tgrelid = 'sessions.delivery_records'::regclass
         AND tgname = 'delivery_session_catalog_insert');
  ELSIF p_stage = 'activity' THEN
    RETURN to_regprocedure('sessions.project_delivery_session_v2()') IS NOT NULL;
  END IF;
  RAISE EXCEPTION 'unknown_delivery_rollout_stage' USING ERRCODE = 'P0001';
END;
$requires_indexes$;

-- Was this row version written by a transaction the activation could not see, that
-- is, one that committed after the projection went live? Compared by age, so it
-- assumes fewer than 2^31 transactions since the oldest retained row (this database
-- had used about a million when this was written); xmin of a frozen row keeps its
-- original value.
CREATE FUNCTION sessions.delivery_written_after(p_xmin xid, p_snapshot pg_snapshot)
RETURNS boolean LANGUAGE sql STABLE AS $after$
  SELECT age(p_xmin) <= age(pg_snapshot_xmax(p_snapshot)::xid)
      OR p_xmin = ANY (ARRAY(SELECT x::xid FROM pg_snapshot_xip(p_snapshot) AS x))
$after$;

-- Stage 2a. The catalog triggers 0022 defines, created only once their key index
-- is valid: removing or re-keying a contender looks the key up through it.
CREATE FUNCTION sessions.activate_delivery_catalog()
RETURNS boolean LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $activate_catalog$
BEGIN
  IF EXISTS (SELECT 1 FROM sessions.delivery_rollout WHERE stage = 'catalog') THEN
    RETURN false;
  END IF;
  -- A database that applied an earlier 0022 (see SUPERSEDED_CHECKSUMS in the
  -- migration runner) created these triggers and backfilled in that transaction.
  IF NOT sessions.delivery_rollout_stage_requires_indexes('catalog') THEN
    INSERT INTO sessions.delivery_rollout (stage, activated_snapshot, completed_at)
    VALUES ('catalog', pg_current_snapshot(), now());
    RETURN false;
  END IF;
  IF NOT sessions.delivery_rollout_indexes_ready('catalog') THEN
    RAISE EXCEPTION 'delivery_rollout_index_missing' USING ERRCODE = 'P0001';
  END IF;
  -- WHEN keeps activity kinds from entering PL/pgSQL at all.
  CREATE TRIGGER delivery_session_catalog_insert AFTER INSERT ON sessions.delivery_records
    FOR EACH ROW WHEN (NEW.kind IN ('session', 'relationship', 'session_marker', 'commit_link'))
    EXECUTE FUNCTION sessions.project_delivery_catalog();
  CREATE TRIGGER delivery_session_catalog_update AFTER UPDATE ON sessions.delivery_records
    FOR EACH ROW WHEN (NEW.kind IN ('session', 'relationship', 'session_marker', 'commit_link')
      OR OLD.kind IN ('session', 'relationship', 'session_marker', 'commit_link'))
    EXECUTE FUNCTION sessions.project_delivery_catalog();
  CREATE TRIGGER delivery_session_catalog_delete AFTER DELETE ON sessions.delivery_records
    FOR EACH ROW WHEN (OLD.kind IN ('session', 'relationship', 'session_marker', 'commit_link'))
    EXECUTE FUNCTION sessions.project_delivery_catalog();
  INSERT INTO sessions.delivery_rollout (stage, activated_snapshot)
  VALUES ('catalog', pg_current_snapshot());
  RETURN true;
END;
$activate_catalog$;

-- Stage 2b. Project up to p_limit retained keys after the cursor; returns how many.
-- 0 means every key has been projected. Each key's retained contenders that have no
-- acceptance yet are numbered from 1 in the order 0022 seeded them (received_at,
-- revision, origin, record); live writes rank far above (0022 starts the sequence
-- at 2^40), and project_delivery_catalog_key keeps a live projected contender, so a
-- key an upload already settled is left as it is.
CREATE FUNCTION sessions.delivery_catalog_backfill_step(p_limit integer)
RETURNS integer LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $catalog_step$
DECLARE
  progress sessions.delivery_rollout%ROWTYPE;
  item record;
  winner sessions.delivery_records%ROWTYPE;
  processed integer := 0;
  last_key jsonb;
  locked text[] := '{}';
BEGIN
  SELECT * INTO progress FROM sessions.delivery_rollout WHERE stage = 'catalog' FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'delivery_rollout_not_activated' USING ERRCODE = 'P0001';
  END IF;
  IF progress.completed_at IS NOT NULL THEN RETURN 0; END IF;
  FOR item IN
    SELECT DISTINCT d.org_id, d.workspace_id,
           sessions.delivery_catalog_key(d.kind, d.source, d.session_id, d.operation, d.payload) AS catalog_key
      FROM sessions.delivery_records AS d
     WHERE d.kind IN ('session', 'relationship', 'session_marker', 'commit_link')
       AND sessions.delivery_catalog_key(d.kind, d.source, d.session_id, d.operation, d.payload) IS NOT NULL
       AND (progress.cursor IS NULL
         OR (d.org_id, d.workspace_id,
             sessions.delivery_catalog_key(d.kind, d.source, d.session_id, d.operation, d.payload))
            > (progress.cursor->>0, progress.cursor->>1, progress.cursor->2))
     ORDER BY 1, 2, 3
     LIMIT p_limit
  LOOP
    -- The lock delivery writes take (project_delivery_catalog), once per workspace.
    -- A writer only ever holds one workspace's lock, so taking several here in
    -- sorted order cannot deadlock with it.
    IF NOT (item.org_id || chr(31) || item.workspace_id) = ANY (locked) THEN
      PERFORM pg_advisory_xact_lock(hashtextextended(
        'relayhistory.delivery_projection' || chr(31) || item.org_id || chr(31) || item.workspace_id, 0));
      locked := locked || (item.org_id || chr(31) || item.workspace_id);
    END IF;
    INSERT INTO sessions.delivery_catalog_acceptance (org_id, workspace_id, origin_id, record_id, accepted_seq)
    SELECT d.org_id, d.workspace_id, d.origin_id, d.record_id,
           row_number() OVER (ORDER BY d.received_at, d.revision, d.origin_id, d.record_id)
      FROM sessions.delivery_records AS d
     WHERE d.org_id = item.org_id AND d.workspace_id = item.workspace_id
       AND d.kind IN ('session', 'relationship', 'session_marker', 'commit_link')
       AND sessions.delivery_catalog_key(d.kind, d.source, d.session_id, d.operation, d.payload) = item.catalog_key
    ON CONFLICT (org_id, workspace_id, origin_id, record_id) DO NOTHING;
    -- Every contender now has an acceptance position, so rank by it alone, in
    -- 0022's order. project_delivery_catalog_key would keep whichever contender is
    -- shown, and a live removal before this key was seeded can have shown one
    -- chosen without acceptance (by origin_id, not received_at).
    SELECT d.* INTO winner FROM sessions.delivery_records AS d
      JOIN sessions.delivery_catalog_acceptance AS a
        ON a.org_id = d.org_id AND a.workspace_id = d.workspace_id
       AND a.origin_id = d.origin_id AND a.record_id = d.record_id
     WHERE d.org_id = item.org_id AND d.workspace_id = item.workspace_id
       AND d.kind IN ('session', 'relationship', 'session_marker', 'commit_link')
       AND sessions.delivery_catalog_key(d.kind, d.source, d.session_id, d.operation, d.payload) = item.catalog_key
     ORDER BY a.accepted_seq DESC, d.origin_id DESC, d.record_id DESC
     LIMIT 1;
    IF FOUND THEN
      PERFORM sessions.project_delivery_catalog_row(winner);
    ELSE
      PERFORM sessions.project_delivery_catalog_key(item.org_id, item.workspace_id, item.catalog_key);
    END IF;
    processed := processed + 1;
    last_key := jsonb_build_array(item.org_id, item.workspace_id, item.catalog_key);
  END LOOP;
  UPDATE sessions.delivery_rollout
     SET cursor = COALESCE(last_key, cursor),
         completed_at = CASE WHEN processed < p_limit THEN now() END,
         updated_at = now()
   WHERE stage = 'catalog';
  RETURN processed;
END;
$catalog_step$;

-- Stage 3a. Point the activity trigger at the 0023 projection once its indexes are
-- valid. The trigger keeps the name and timing 0011 gave it.
CREATE FUNCTION sessions.activate_delivery_projection_v2()
RETURNS boolean LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $activate_v2$
BEGIN
  IF EXISTS (SELECT 1 FROM sessions.delivery_rollout WHERE stage = 'activity') THEN
    RETURN false;
  END IF;
  -- A database that applied an earlier 0023 replaced project_delivery_session() with
  -- v2 in place and reprojected in that transaction; v2 is already live there.
  IF NOT sessions.delivery_rollout_stage_requires_indexes('activity') THEN
    INSERT INTO sessions.delivery_rollout (stage, activated_snapshot, completed_at)
    VALUES ('activity', pg_current_snapshot(), now());
    RETURN false;
  END IF;
  IF NOT sessions.delivery_rollout_indexes_ready('activity') THEN
    RAISE EXCEPTION 'delivery_rollout_index_missing' USING ERRCODE = 'P0001';
  END IF;
  -- In place: a weaker lock than DROP + CREATE, and no instant without a trigger.
  CREATE OR REPLACE TRIGGER delivery_session_projection AFTER INSERT OR UPDATE ON sessions.delivery_records
    FOR EACH ROW EXECUTE FUNCTION sessions.project_delivery_session_v2();
  INSERT INTO sessions.delivery_rollout (stage, activated_snapshot)
  VALUES ('activity', pg_current_snapshot());
  RETURN true;
END;
$activate_v2$;

-- Stage 3b. Reproject up to p_limit retained records after the cursor; returns how
-- many. 0 means done. For a record no write has touched since v2 went live, its
-- activity copies are replayed oldest first by (received_at, origin_id), exactly as
-- 0023 used to: the v2 trigger projects each as the newest write, so the newest
-- copy fires last and wins, legacy projections are replaced and request usage is
-- settled. A record written since then already holds the newest write's projection
-- (v2 retracted that origin's legacy row and every v2 row of the record); replaying
-- an older copy over it would be wrong, so only the legacy rows the other origins'
-- copies left behind are removed.
CREATE FUNCTION sessions.delivery_activity_reproject_step(p_limit integer)
RETURNS integer LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $activity_step$
DECLARE
  progress sessions.delivery_rollout%ROWTYPE;
  item record;
  copy record;
  processed integer := 0;
  last_record jsonb;
  locked text[] := '{}';
BEGIN
  SELECT * INTO progress FROM sessions.delivery_rollout WHERE stage = 'activity' FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'delivery_rollout_not_activated' USING ERRCODE = 'P0001';
  END IF;
  IF progress.completed_at IS NOT NULL THEN RETURN 0; END IF;
  FOR item IN
    SELECT DISTINCT d.org_id, d.workspace_id, d.record_id
      FROM sessions.delivery_records AS d
     WHERE d.kind IN ('history', 'session_event', 'tool_call', 'file_edit', 'trajectory')
       AND (progress.cursor IS NULL
         OR (d.org_id, d.workspace_id, d.record_id)
            > (progress.cursor->>0, progress.cursor->>1, progress.cursor->>2))
     ORDER BY 1, 2, 3
     LIMIT p_limit
  LOOP
    -- Take the workspace lock the v2 trigger takes BEFORE deciding how to handle
    -- the record: an upload that committed earlier is then visible to the check
    -- below, and one still waiting projects after this step commits, as the newer
    -- write it is. Several workspaces are taken in sorted order; a writer only
    -- ever holds one, so this cannot deadlock with it.
    IF NOT (item.org_id || chr(31) || item.workspace_id) = ANY (locked) THEN
      PERFORM pg_advisory_xact_lock(hashtextextended(
        'relayhistory.delivery_projection' || chr(31) || item.org_id || chr(31) || item.workspace_id, 0));
      locked := locked || (item.org_id || chr(31) || item.workspace_id);
    END IF;
    IF EXISTS (
      SELECT 1 FROM sessions.delivery_records AS d
       WHERE d.org_id = item.org_id AND d.workspace_id = item.workspace_id
         AND d.record_id = item.record_id
         AND sessions.delivery_written_after(d.xmin, progress.activated_snapshot)
    ) THEN
      FOR copy IN
        SELECT d.origin_id, d.kind, d.source, d.session_id FROM sessions.delivery_records AS d
         WHERE d.org_id = item.org_id AND d.workspace_id = item.workspace_id
           AND d.record_id = item.record_id AND d.session_id IS NOT NULL
           AND d.kind IN ('history', 'session_event', 'tool_call', 'file_edit', 'trajectory')
      LOOP
        DELETE FROM sessions.convergence_events
         WHERE org_id = item.org_id
           AND machine_id = 'delivery:' || md5(jsonb_build_array(item.workspace_id, copy.origin_id)::text)
           AND source = copy.source AND session_id = copy.session_id AND kind = copy.kind
           AND delivery_record_id IS NULL
           AND event_id IN (item.record_id,
             'delivery:' || md5(jsonb_build_array(item.workspace_id, copy.origin_id)::text) || ':' || item.record_id);
      END LOOP;
    ELSE
      -- An upload locks its delivery row before its trigger asks for the workspace
      -- lock held here; waiting on that row would deadlock and Postgres could abort
      -- the upload. NOWAIT makes this batch fail instead (SQLSTATE 55P03), which
      -- releases the lock; the rollout script retries the batch.
      PERFORM 1 FROM sessions.delivery_records AS d
       WHERE d.org_id = item.org_id AND d.workspace_id = item.workspace_id
         AND d.record_id = item.record_id
         FOR UPDATE NOWAIT;
      FOR copy IN
        SELECT d.origin_id FROM sessions.delivery_records AS d
         WHERE d.org_id = item.org_id AND d.workspace_id = item.workspace_id
           AND d.record_id = item.record_id
           AND d.kind IN ('history', 'session_event', 'tool_call', 'file_edit', 'trajectory')
         ORDER BY d.received_at, d.origin_id
      LOOP
        UPDATE sessions.delivery_records SET received_at = received_at
         WHERE org_id = item.org_id AND workspace_id = item.workspace_id
           AND origin_id = copy.origin_id AND record_id = item.record_id;
      END LOOP;
    END IF;
    processed := processed + 1;
    last_record := jsonb_build_array(item.org_id, item.workspace_id, item.record_id);
  END LOOP;
  UPDATE sessions.delivery_rollout
     SET cursor = COALESCE(last_record, cursor),
         completed_at = CASE WHEN processed < p_limit THEN now() END,
         updated_at = now()
   WHERE stage = 'activity';
  RETURN processed;
END;
$activity_step$;
