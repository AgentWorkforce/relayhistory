-- Typed, queryable projections of the delivery kinds that describe sessions rather
-- than activity: `session` (the session catalog), `relationship` (subagent,
-- continuation, fork and resume links), `session_marker` (compactions, resume and
-- fork points) and `commit_link`. Until now these were only retained raw in
-- sessions.delivery_records.
--
-- Identity. A session id is unique only within (org, workspace, source), so every
-- key below starts with (org_id, workspace_id, source) and then mirrors the local
-- store's own uniqueness constraint for that kind:
--   session          (session_id)
--   relationship     (parent_session_id, relationship_uid) -- never the child id:
--                    unlinked evidence has none, and two sidecars of one parent
--                    must not collapse into one row
--   session_marker   (session_id, marker_uid)
--   commit_link      (session_id, commit_sha, match_method)
--
-- Contenders. record_id is origin-independent (a hash of the local key), so the
-- same record can arrive from several origins (machines), and distinct records
-- can still map to one projected key. Every live upsert whose payload maps to a
-- key contends for it. Revisions are per-origin change-feed positions and are not
-- comparable across origins, so each key follows the order in which writes pass
-- the workspace's delivery advisory lock: the newest live upsert wins. Removing a contender (a
-- delete tombstone, a removed row, or a revision that re-kinds or re-keys it)
-- removes the projection only when no contender remains -- one machine pruning
-- its local store does not erase what another machine still reports -- and
-- otherwise leaves or restores the remaining live contender accepted last.
-- received_at cannot rank contenders: it is now() in accept_delivery_batch, the
-- transaction start, so a writer that waited on the lock carries an older stamp
-- than the write it follows. delivery_catalog_acceptance records the order in
-- which live upserts passed the lock instead.
--
-- Commit links get their own table instead of feeding session_outcomes: outcomes
-- are keyed without match_method (two methods for one commit would collapse), are
-- also written by legacy ingest (a delivery tombstone could erase an ingest-owned
-- row), and require a machine id delivery does not have.
--
-- Payloads already passed the delivery service's field allowlist and scrubber;
-- `*_json` columns arrive as canonical JSON strings. This migration does not touch
-- sessions.project_delivery_session() or its trigger.

CREATE TABLE sessions.session_catalog (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  record_id text NOT NULL,
  origin_id text NOT NULL,
  user_id text NOT NULL,
  title text,
  cwd text,
  git_branch text,
  repo_url text,
  initial_commit text,
  first_prompt text,
  models jsonb,
  originator text,
  agent_version text,
  workspace_roots jsonb,
  project_key text,
  project_key_method text,
  discovery_state text,
  first_activity_at timestamptz,
  last_activity_at timestamptz,
  received_at timestamptz NOT NULL,
  PRIMARY KEY (org_id, workspace_id, source, session_id)
);
CREATE INDEX session_catalog_record_idx ON sessions.session_catalog (org_id, workspace_id, record_id);
CREATE INDEX session_catalog_org_session_idx ON sessions.session_catalog (org_id, source, session_id);

CREATE TABLE sessions.session_relationships (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  parent_session_id text NOT NULL,
  relationship_uid text NOT NULL,
  record_id text NOT NULL,
  origin_id text NOT NULL,
  user_id text NOT NULL,
  child_session_id text,
  relationship text,
  identity_status text,
  child_agent_type text,
  child_agent_name text,
  child_model text,
  spawn_depth integer,
  evidence_kind text,
  child_has_events boolean,
  spawned_at timestamptz,
  updated_at timestamptz,
  received_at timestamptz NOT NULL,
  PRIMARY KEY (org_id, workspace_id, source, parent_session_id, relationship_uid)
);
CREATE INDEX session_relationships_record_idx ON sessions.session_relationships (org_id, workspace_id, record_id);
CREATE INDEX session_relationships_child_idx ON sessions.session_relationships (org_id, workspace_id, source, child_session_id)
  WHERE child_session_id IS NOT NULL;

CREATE TABLE sessions.session_markers (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  marker_uid text NOT NULL,
  record_id text NOT NULL,
  origin_id text NOT NULL,
  user_id text NOT NULL,
  marker_kind text,
  subkind text,
  ts timestamptz,
  message_id text,
  parent_id text,
  turn_id text,
  text text,
  payload jsonb,
  received_at timestamptz NOT NULL,
  PRIMARY KEY (org_id, workspace_id, source, session_id, marker_uid)
);
CREATE INDEX session_markers_record_idx ON sessions.session_markers (org_id, workspace_id, record_id);

CREATE TABLE sessions.session_commit_links (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  commit_sha text NOT NULL,
  match_method text NOT NULL,
  record_id text NOT NULL,
  origin_id text NOT NULL,
  user_id text NOT NULL,
  repo text,
  branch text,
  note_ref text,
  confidence_basis_points integer,
  files jsonb,
  numstat jsonb,
  evidence jsonb,
  linked_at timestamptz,
  received_at timestamptz NOT NULL,
  PRIMARY KEY (org_id, workspace_id, source, session_id, commit_sha, match_method)
);
CREATE INDEX session_commit_links_record_idx ON sessions.session_commit_links (org_id, workspace_id, record_id);
CREATE INDEX session_commit_links_commit_idx ON sessions.session_commit_links (org_id, workspace_id, commit_sha);

-- Value readers. PL/pgSQL rather than SQL so a constant argument is never folded
-- into a cast the type check was meant to guard.
CREATE FUNCTION sessions.delivery_catalog_text(value jsonb)
RETURNS text LANGUAGE plpgsql IMMUTABLE
SET search_path = pg_catalog, sessions, pg_temp AS $catalog_text$
BEGIN
  IF jsonb_typeof(value) IN ('string', 'number', 'boolean') THEN
    RETURN NULLIF(value #>> '{}', '');
  END IF;
  RETURN NULL;
END;
$catalog_text$;

CREATE FUNCTION sessions.delivery_catalog_integer(value jsonb)
RETURNS integer LANGUAGE plpgsql IMMUTABLE
SET search_path = pg_catalog, sessions, pg_temp AS $catalog_integer$
DECLARE
  amount numeric;
BEGIN
  IF jsonb_typeof(value) IS DISTINCT FROM 'number' THEN RETURN NULL; END IF;
  amount := (value #>> '{}')::numeric;
  IF amount <> trunc(amount) OR amount NOT BETWEEN -2147483648 AND 2147483647 THEN RETURN NULL; END IF;
  RETURN amount::integer;
END;
$catalog_integer$;

CREATE FUNCTION sessions.delivery_catalog_time(value jsonb)
RETURNS timestamptz LANGUAGE plpgsql IMMUTABLE
SET search_path = pg_catalog, sessions, pg_temp AS $catalog_time$
DECLARE
  milliseconds numeric;
BEGIN
  IF jsonb_typeof(value) IS DISTINCT FROM 'number' THEN RETURN NULL; END IF;
  milliseconds := (value #>> '{}')::numeric;
  IF milliseconds NOT BETWEEN 0 AND 8640000000000000 THEN RETURN NULL; END IF;
  RETURN to_timestamp((milliseconds / 1000)::double precision);
END;
$catalog_time$;

-- `*_json` columns are JSON text; anything unparseable is dropped, never raised.
CREATE FUNCTION sessions.delivery_catalog_json(value jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE
SET search_path = pg_catalog, sessions, pg_temp AS $catalog_json$
BEGIN
  IF jsonb_typeof(value) IN ('object', 'array') THEN RETURN value; END IF;
  IF jsonb_typeof(value) IS DISTINCT FROM 'string' THEN RETURN NULL; END IF;
  BEGIN
    RETURN (value #>> '{}')::jsonb;
  EXCEPTION WHEN others THEN
    RETURN NULL;
  END;
END;
$catalog_json$;

-- The projected key a delivery row contends for, within its (org, workspace):
-- [kind, source, session, ...] mirroring each table's primary key, or NULL when
-- the row is not a live upsert or lacks a key part. The single definition both
-- the projection and the contender lookup use.
CREATE FUNCTION sessions.delivery_catalog_key(
  p_kind text, p_source text, p_session_id text, p_operation text, p_payload jsonb
) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE
SET search_path = pg_catalog, sessions, pg_temp AS $catalog_key$
DECLARE
  target_session text;
  parts text[];
BEGIN
  IF p_operation IS DISTINCT FROM 'upsert' OR jsonb_typeof(p_payload) IS DISTINCT FROM 'object'
    OR p_source IS NULL THEN
    RETURN NULL;
  END IF;
  target_session := COALESCE(p_session_id, sessions.delivery_catalog_text(
    p_payload->(CASE WHEN p_kind = 'relationship' THEN 'parent_session_id' ELSE 'session_id' END)));
  parts := CASE p_kind
    WHEN 'session' THEN ARRAY[target_session]
    WHEN 'relationship' THEN ARRAY[target_session, sessions.delivery_catalog_text(p_payload->'relationship_uid')]
    WHEN 'session_marker' THEN ARRAY[target_session, sessions.delivery_catalog_text(p_payload->'marker_uid')]
    WHEN 'commit_link' THEN ARRAY[target_session, sessions.delivery_catalog_text(p_payload->'commit_sha'),
      sessions.delivery_catalog_text(p_payload->'match_method')]
  END;
  IF parts IS NULL OR array_position(parts, NULL) IS NOT NULL THEN RETURN NULL; END IF;
  RETURN jsonb_build_array(p_kind, p_source) || to_jsonb(parts);
END;
$catalog_key$;

-- Acceptance order of each delivery row's latest live catalog upsert. accepted_seq
-- is drawn after the workspace's advisory lock, which is held until commit, so it
-- follows the order writes pass the serialized point, not transaction start.
CREATE SEQUENCE sessions.delivery_catalog_acceptance_seq;
CREATE TABLE sessions.delivery_catalog_acceptance (
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  origin_id text NOT NULL,
  record_id text NOT NULL,
  accepted_seq bigint NOT NULL,
  PRIMARY KEY (org_id, workspace_id, origin_id, record_id)
);

-- Every live contender for one key is found through delivery_records_catalog_key,
-- an expression index over all of delivery_records. It is built with CREATE INDEX
-- CONCURRENTLY by the rollout (0029), never inside this transaction.

-- Project one delivery row at its key, replacing whichever contender held it.
CREATE FUNCTION sessions.project_delivery_catalog_row(winner sessions.delivery_records)
RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $catalog_row$
DECLARE
  k jsonb := sessions.delivery_catalog_key(winner.kind, winner.source, winner.session_id, winner.operation, winner.payload);
  p jsonb := winner.payload;
  target_session text := k->>2;
  confidence_value numeric;
BEGIN
  IF k IS NULL THEN RETURN; END IF;

  IF winner.kind = 'session' THEN
    INSERT INTO sessions.session_catalog (
      org_id, workspace_id, source, session_id, record_id, origin_id, user_id,
      title, cwd, git_branch, repo_url, initial_commit, first_prompt, models,
      originator, agent_version, workspace_roots, project_key, project_key_method,
      discovery_state, first_activity_at, last_activity_at, received_at
    ) VALUES (
      winner.org_id, winner.workspace_id, winner.source, target_session, winner.record_id, winner.origin_id, winner.user_id,
      sessions.delivery_catalog_text(p->'title'),
      sessions.delivery_catalog_text(p->'cwd'),
      sessions.delivery_catalog_text(p->'git_branch'),
      sessions.delivery_catalog_text(p->'repo_url'),
      sessions.delivery_catalog_text(p->'initial_commit'),
      sessions.delivery_catalog_text(p->'first_prompt'),
      sessions.delivery_catalog_json(p->'models_json'),
      sessions.delivery_catalog_text(p->'originator'),
      sessions.delivery_catalog_text(p->'agent_version'),
      sessions.delivery_catalog_json(p->'workspace_roots_json'),
      sessions.delivery_catalog_text(p->'project_key'),
      sessions.delivery_catalog_text(p->'project_key_method'),
      sessions.delivery_catalog_text(p->'discovery_state'),
      sessions.delivery_catalog_time(p->'first_activity_ms'),
      sessions.delivery_catalog_time(p->'last_activity_ms'),
      winner.received_at
    )
    ON CONFLICT (org_id, workspace_id, source, session_id) DO UPDATE SET
      record_id = excluded.record_id, origin_id = excluded.origin_id, user_id = excluded.user_id,
      title = excluded.title, cwd = excluded.cwd, git_branch = excluded.git_branch,
      repo_url = excluded.repo_url, initial_commit = excluded.initial_commit,
      first_prompt = excluded.first_prompt, models = excluded.models,
      originator = excluded.originator, agent_version = excluded.agent_version,
      workspace_roots = excluded.workspace_roots, project_key = excluded.project_key,
      project_key_method = excluded.project_key_method, discovery_state = excluded.discovery_state,
      first_activity_at = excluded.first_activity_at, last_activity_at = excluded.last_activity_at,
      received_at = excluded.received_at;

  ELSIF winner.kind = 'relationship' THEN
    INSERT INTO sessions.session_relationships (
      org_id, workspace_id, source, parent_session_id, relationship_uid, record_id, origin_id, user_id,
      child_session_id, relationship, identity_status, child_agent_type, child_agent_name,
      child_model, spawn_depth, evidence_kind, child_has_events, spawned_at, updated_at, received_at
    ) VALUES (
      winner.org_id, winner.workspace_id, winner.source, target_session,
      k->>3, winner.record_id, winner.origin_id, winner.user_id,
      sessions.delivery_catalog_text(p->'child_session_id'),
      sessions.delivery_catalog_text(p->'relationship'),
      sessions.delivery_catalog_text(p->'identity_status'),
      sessions.delivery_catalog_text(p->'child_agent_type'),
      sessions.delivery_catalog_text(p->'child_agent_name'),
      sessions.delivery_catalog_text(p->'child_model'),
      sessions.delivery_catalog_integer(p->'spawn_depth'),
      sessions.delivery_catalog_text(p->'evidence_kind'),
      CASE WHEN jsonb_typeof(p->'child_has_events') = 'boolean' THEN (p->>'child_has_events')::boolean
           ELSE sessions.delivery_catalog_integer(p->'child_has_events') <> 0 END,
      sessions.delivery_catalog_time(p->'spawned_at_ms'),
      sessions.delivery_catalog_time(p->'updated_ms'),
      winner.received_at
    )
    ON CONFLICT (org_id, workspace_id, source, parent_session_id, relationship_uid) DO UPDATE SET
      record_id = excluded.record_id, origin_id = excluded.origin_id, user_id = excluded.user_id,
      child_session_id = excluded.child_session_id, relationship = excluded.relationship,
      identity_status = excluded.identity_status, child_agent_type = excluded.child_agent_type,
      child_agent_name = excluded.child_agent_name, child_model = excluded.child_model,
      spawn_depth = excluded.spawn_depth, evidence_kind = excluded.evidence_kind,
      child_has_events = excluded.child_has_events, spawned_at = excluded.spawned_at,
      updated_at = excluded.updated_at, received_at = excluded.received_at;

  ELSIF winner.kind = 'session_marker' THEN
    INSERT INTO sessions.session_markers (
      org_id, workspace_id, source, session_id, marker_uid, record_id, origin_id, user_id,
      marker_kind, subkind, ts, message_id, parent_id, turn_id, text, payload, received_at
    ) VALUES (
      winner.org_id, winner.workspace_id, winner.source, target_session,
      k->>3, winner.record_id, winner.origin_id, winner.user_id,
      sessions.delivery_catalog_text(p->'kind'),
      sessions.delivery_catalog_text(p->'subkind'),
      sessions.delivery_catalog_time(p->'ts_ms'),
      sessions.delivery_catalog_text(p->'message_id'),
      sessions.delivery_catalog_text(p->'parent_id'),
      sessions.delivery_catalog_text(p->'turn_id'),
      sessions.delivery_catalog_text(p->'text'),
      sessions.delivery_catalog_json(p->'payload_json'),
      winner.received_at
    )
    ON CONFLICT (org_id, workspace_id, source, session_id, marker_uid) DO UPDATE SET
      record_id = excluded.record_id, origin_id = excluded.origin_id, user_id = excluded.user_id,
      marker_kind = excluded.marker_kind, subkind = excluded.subkind, ts = excluded.ts,
      message_id = excluded.message_id, parent_id = excluded.parent_id, turn_id = excluded.turn_id,
      text = excluded.text, payload = excluded.payload, received_at = excluded.received_at;

  ELSIF winner.kind = 'commit_link' THEN
    -- The local store keeps confidence as 0..1; the service stores basis points.
    confidence_value := CASE WHEN jsonb_typeof(p->'confidence') = 'number' THEN (p->>'confidence')::numeric END;
    INSERT INTO sessions.session_commit_links (
      org_id, workspace_id, source, session_id, commit_sha, match_method, record_id, origin_id, user_id,
      repo, branch, note_ref, confidence_basis_points, files, numstat, evidence, linked_at, received_at
    ) VALUES (
      winner.org_id, winner.workspace_id, winner.source, target_session,
      k->>3, k->>4,
      winner.record_id, winner.origin_id, winner.user_id,
      sessions.delivery_catalog_text(p->'repo'),
      sessions.delivery_catalog_text(p->'branch'),
      sessions.delivery_catalog_text(p->'note_ref'),
      CASE WHEN confidence_value BETWEEN 0 AND 1 THEN round(confidence_value * 10000)::integer END,
      sessions.delivery_catalog_json(p->'files_json'),
      sessions.delivery_catalog_json(p->'numstat_json'),
      sessions.delivery_catalog_json(p->'evidence_json'),
      sessions.delivery_catalog_time(p->'created_at_ms'),
      winner.received_at
    )
    ON CONFLICT (org_id, workspace_id, source, session_id, commit_sha, match_method) DO UPDATE SET
      record_id = excluded.record_id, origin_id = excluded.origin_id, user_id = excluded.user_id,
      repo = excluded.repo, branch = excluded.branch, note_ref = excluded.note_ref,
      confidence_basis_points = excluded.confidence_basis_points, files = excluded.files,
      numstat = excluded.numstat, evidence = excluded.evidence, linked_at = excluded.linked_at,
      received_at = excluded.received_at;
  END IF;
END;
$catalog_row$;

-- Recompute one key after a contender left it. The projected row is the newest
-- write that passed the key's lock, so while its copy is still a live contender
-- for this key it stays; otherwise the remaining contender with the highest
-- accepted_seq is projected, and with none left the key is removed.
CREATE FUNCTION sessions.project_delivery_catalog_key(p_org_id text, p_workspace_id text, p_key jsonb)
RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $catalog_key_projection$
DECLARE
  key_kind text := p_key->>0;
  key_source text := p_key->>1;
  key_session text := p_key->>2;
  current_origin text;
  current_record text;
  winner sessions.delivery_records%ROWTYPE;
BEGIN
  IF key_kind = 'session' THEN
    SELECT origin_id, record_id INTO current_origin, current_record FROM sessions.session_catalog
     WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND source = key_source AND session_id = key_session;
  ELSIF key_kind = 'relationship' THEN
    SELECT origin_id, record_id INTO current_origin, current_record FROM sessions.session_relationships
     WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND source = key_source
       AND parent_session_id = key_session AND relationship_uid = p_key->>3;
  ELSIF key_kind = 'session_marker' THEN
    SELECT origin_id, record_id INTO current_origin, current_record FROM sessions.session_markers
     WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND source = key_source
       AND session_id = key_session AND marker_uid = p_key->>3;
  ELSIF key_kind = 'commit_link' THEN
    SELECT origin_id, record_id INTO current_origin, current_record FROM sessions.session_commit_links
     WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND source = key_source
       AND session_id = key_session AND commit_sha = p_key->>3 AND match_method = p_key->>4;
  END IF;

  SELECT d.* INTO winner FROM sessions.delivery_records AS d
    LEFT JOIN sessions.delivery_catalog_acceptance AS a
      ON a.org_id = d.org_id AND a.workspace_id = d.workspace_id
     AND a.origin_id = d.origin_id AND a.record_id = d.record_id
   WHERE d.org_id = p_org_id AND d.workspace_id = p_workspace_id
     AND d.kind IN ('session', 'relationship', 'session_marker', 'commit_link')
     AND sessions.delivery_catalog_key(d.kind, d.source, d.session_id, d.operation, d.payload) = p_key
   ORDER BY (d.origin_id, d.record_id) IS NOT DISTINCT FROM (current_origin, current_record) DESC,
     a.accepted_seq DESC NULLS LAST, d.origin_id DESC, d.record_id DESC
   LIMIT 1;
  IF FOUND THEN
    PERFORM sessions.project_delivery_catalog_row(winner);
  ELSIF key_kind = 'session' THEN
    DELETE FROM sessions.session_catalog
     WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND source = key_source AND session_id = key_session;
  ELSIF key_kind = 'relationship' THEN
    DELETE FROM sessions.session_relationships
     WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND source = key_source
       AND parent_session_id = key_session AND relationship_uid = p_key->>3;
  ELSIF key_kind = 'session_marker' THEN
    DELETE FROM sessions.session_markers
     WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND source = key_source
       AND session_id = key_session AND marker_uid = p_key->>3;
  ELSIF key_kind = 'commit_link' THEN
    DELETE FROM sessions.session_commit_links
     WHERE org_id = p_org_id AND workspace_id = p_workspace_id AND source = key_source
       AND session_id = key_session AND commit_sha = p_key->>3 AND match_method = p_key->>4;
  END IF;
END;
$catalog_key_projection$;

-- Row trigger, separate from project_delivery_session(). The workspace lock is held
-- until commit, and each statement below takes a fresh snapshot, so whoever
-- acquires it later really wrote later and sees every earlier writer's committed
-- row. The arriving upsert is therefore projected as the newest write. received_at
-- is not that order: it is now() in accept_delivery_batch, the transaction start,
-- so a writer that started first but waited on the lock carries the older stamp.
CREATE FUNCTION sessions.project_delivery_catalog()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, sessions, pg_temp AS $catalog_projection$
DECLARE
  old_key jsonb;
  new_key jsonb;
  old_lock text;
  new_lock text;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    old_key := sessions.delivery_catalog_key(OLD.kind, OLD.source, OLD.session_id, OLD.operation, OLD.payload);
    IF old_key IS NOT NULL THEN
      old_lock := jsonb_build_array(OLD.org_id, OLD.workspace_id, old_key)::text;
    END IF;
  END IF;
  IF TG_OP <> 'DELETE' THEN
    new_key := sessions.delivery_catalog_key(NEW.kind, NEW.source, NEW.session_id, NEW.operation, NEW.payload);
    IF new_key IS NOT NULL THEN
      new_lock := jsonb_build_array(NEW.org_id, NEW.workspace_id, new_key)::text;
    END IF;
  END IF;
  -- One lock per workspace, not per key: a batch touches many keys in record-id
  -- order, so per-key locks let two origins' batches take the same keys in
  -- opposite orders and deadlock. The lock name is the one the activity projection
  -- (project_delivery_session) takes, so a batch that mixes catalog and activity
  -- rows also needs only this single lock. A row keeps its workspace (it is in the
  -- delivery_records key), so OLD and NEW never need two.
  IF old_lock IS NOT NULL OR new_lock IS NOT NULL THEN
    PERFORM pg_advisory_xact_lock(hashtextextended(
      'relayhistory.delivery_projection' || chr(31) || COALESCE(NEW.org_id, OLD.org_id)
        || chr(31) || COALESCE(NEW.workspace_id, OLD.workspace_id), 0));
  END IF;

  -- A removed or moved row leaves no acceptance behind. A tombstone's entry is
  -- inert: only live contenders are ranked, and a later upsert redraws it.
  IF TG_OP = 'DELETE' OR (TG_OP = 'UPDATE' AND (OLD.org_id, OLD.workspace_id, OLD.origin_id, OLD.record_id)
      IS DISTINCT FROM (NEW.org_id, NEW.workspace_id, NEW.origin_id, NEW.record_id)) THEN
    DELETE FROM sessions.delivery_catalog_acceptance
     WHERE org_id = OLD.org_id AND workspace_id = OLD.workspace_id
       AND origin_id = OLD.origin_id AND record_id = OLD.record_id;
  END IF;
  IF new_lock IS NOT NULL THEN
    INSERT INTO sessions.delivery_catalog_acceptance (org_id, workspace_id, origin_id, record_id, accepted_seq)
    VALUES (NEW.org_id, NEW.workspace_id, NEW.origin_id, NEW.record_id, nextval('sessions.delivery_catalog_acceptance_seq'))
    ON CONFLICT (org_id, workspace_id, origin_id, record_id) DO UPDATE SET accepted_seq = excluded.accepted_seq;
    PERFORM sessions.project_delivery_catalog_row(NEW);
  END IF;
  -- The row no longer contends for its prior key (tombstone, removal, re-kind or
  -- re-key): recompute that key from the contenders left.
  IF old_lock IS NOT NULL AND old_lock IS DISTINCT FROM new_lock THEN
    PERFORM sessions.project_delivery_catalog_key(OLD.org_id, OLD.workspace_id, old_key);
  END IF;
  RETURN NULL;
END;
$catalog_projection$;

-- The triggers that make this projection live (and the backfill of records retained
-- before it) are not created here. Both grow with stored history: building the key
-- index and projecting every retained key held the delivery_records write lock for
-- hours inside the deploy transaction on 2026-10-02/03, blocking every upload.
-- 0029's rollout creates the triggers once the index is valid and then backfills
-- in short batches under the same workspace lock writers take.
--
-- Acceptance order of live writes starts far above any position the backfill seeds
-- for records retained before activation (it numbers each key's contenders from 1),
-- so a write that passes the lock after activation always ranks newest.
SELECT setval('sessions.delivery_catalog_acceptance_seq', 1099511627776, false);
