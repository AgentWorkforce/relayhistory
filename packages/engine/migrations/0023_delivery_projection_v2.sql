-- Delivery projection v2. Replaces the 0014 trigger body; 0011/0014 stay as applied.
-- See docs/durable-delivery.md ("Activity projection") for the contract in prose.
--
-- 1. Usage: `session_events.token_json` is normalized per docs/usage-accounting.md
--    and counted once per request. Claude copies one request's usage onto every
--    content block, so assistant rows sharing a request key form one group and
--    only the group's canonical row (smallest delivery record id) carries tokens.
--    The group is re-settled whenever a member is written or removed, so the
--    result does not depend on arrival order.
-- 2. project_id prefers the canonical `project_key`, host stripped to the
--    `owner/repo` form `?project=` is queried with (see 0006 and the backfill).
-- 3. Tool calls and file edits fill tool/file columns; subagent notifications fill
--    subagent_id from subagent_session_id only (a root event's agent_id is not one).
-- 4. Control rows (slash-command triads, reminders, hook output, isMeta) are not
--    activity. They stay in delivery_records and are not projected.
-- 5. One logical record is projected once per tenant workspace, whatever origin
--    delivered it: the newest delivery write for (workspace, kind, record_id)
--    wins, a tombstone included. "Newest" is the order writes take the
--    workspace's projection lock, not received_at (a transaction-start stamp).
-- 6. A tool row without its own time takes the time of the session event that
--    issued the call.
ALTER TABLE sessions.convergence_events ADD COLUMN IF NOT EXISTS delivery_record_id text;
ALTER TABLE sessions.convergence_events ADD COLUMN IF NOT EXISTS request_key text;

-- The indexes the v2 projection's lookups need (convergence_events_delivery_record_idx,
-- _delivery_request_idx, _delivery_tool_use_idx and delivery_records_record_idx) span
-- convergence_events and delivery_records, so 0029's rollout builds them with CREATE
-- INDEX CONCURRENTLY before v2 goes live, never inside this transaction.

-- The payload's own activity time in epoch milliseconds, or NULL. Same precedence as 0014.
CREATE OR REPLACE FUNCTION sessions.delivery_activity_ms(p_payload jsonb) RETURNS numeric
LANGUAGE sql IMMUTABLE AS $activity$
  SELECT candidate FROM (SELECT COALESCE(
    CASE WHEN jsonb_typeof(p_payload->'updated_ms') = 'number' THEN (p_payload->>'updated_ms')::numeric END,
    CASE WHEN jsonb_typeof(p_payload->'ts_ms') = 'number' THEN (p_payload->>'ts_ms')::numeric END,
    CASE WHEN jsonb_typeof(p_payload->'timestamp_ms') = 'number' THEN (p_payload->>'timestamp_ms')::numeric END,
    CASE WHEN jsonb_typeof(p_payload->'last_activity_ms') = 'number' THEN (p_payload->>'last_activity_ms')::numeric END
  ) AS candidate) AS activity
  WHERE candidate BETWEEN 0 AND 8640000000000000
$activity$;

-- A usage counter: absent/null is "not reported"; anything but a non-negative
-- integer is an error, never a clamp (docs/usage-accounting.md).
CREATE OR REPLACE FUNCTION sessions.delivery_usage_counter(p_value jsonb) RETURNS numeric
LANGUAGE plpgsql IMMUTABLE AS $counter$
DECLARE
  n numeric;
BEGIN
  IF p_value IS NULL OR jsonb_typeof(p_value) = 'null' THEN RETURN NULL; END IF;
  IF jsonb_typeof(p_value) <> 'number' THEN RAISE EXCEPTION 'USAGE_NON_INTEGER_COUNTER'; END IF;
  n := (p_value #>> '{}')::numeric;
  IF n < 0 OR n <> trunc(n) THEN RAISE EXCEPTION 'USAGE_NON_INTEGER_COUNTER'; END IF;
  IF n > 9223372036854775807 THEN RAISE EXCEPTION 'USAGE_COUNT_NOT_REPRESENTABLE'; END IF;
  RETURN n;
END;
$counter$;

-- The first key whose value is present and not JSON null (Grok's spellings).
CREATE OR REPLACE FUNCTION sessions.delivery_usage_first(p_object jsonb, p_keys text[]) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $first$
  SELECT p_object->key FROM unnest(p_keys) WITH ORDINALITY AS k(key, position)
  WHERE jsonb_typeof(p_object->key) IS DISTINCT FROM 'null' AND p_object ? key
  ORDER BY position LIMIT 1
$first$;

-- Mirrors ai_hist::usage::normalize_usage. Returns NULL for "no usage evidence",
-- {"error": CODE} for evidence that cannot be normalized, else the normalized
-- counters: input excludes cache reads for every source; cacheCreate is the total
-- across TTL buckets, with Claude's 5m/1h split kept beside it.
CREATE OR REPLACE FUNCTION sessions.delivery_normalized_usage(p_source text, p_token text) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE AS $usage$
DECLARE
  token jsonb;
  breakdown jsonb;
  input_count numeric;
  output_count numeric;
  reasoning_count numeric;
  total_count numeric;
  cache_read numeric;
  cache_write numeric;
  write_5m numeric;
  write_1h numeric;
  cost jsonb;
  cost_usd numeric;
  reported boolean;
BEGIN
  IF p_token IS NULL THEN RETURN NULL; END IF;
  IF p_source IS NULL OR p_source NOT IN ('claude', 'codex', 'grok', 'muse') THEN
    RETURN jsonb_build_object('error', 'USAGE_UNKNOWN_SOURCE');
  END IF;
  BEGIN
    token := p_token::jsonb;
  EXCEPTION WHEN others THEN
    RETURN jsonb_build_object('error', 'USAGE_MALFORMED');
  END;
  IF jsonb_typeof(token) = 'null' THEN RETURN NULL; END IF;
  IF jsonb_typeof(token) <> 'object' THEN RETURN jsonb_build_object('error', 'USAGE_NOT_AN_OBJECT'); END IF;
  BEGIN
    output_count := sessions.delivery_usage_counter(token->'output_tokens');
    reasoning_count := sessions.delivery_usage_counter(
      token->(CASE WHEN p_source = 'muse' THEN 'reasoning_tokens' ELSE 'reasoning_output_tokens' END));
    total_count := sessions.delivery_usage_counter(token->'total_tokens');
    reported := output_count IS NOT NULL OR reasoning_count IS NOT NULL OR total_count IS NOT NULL;
    IF p_source IN ('codex', 'muse') THEN
      -- Responses-shaped input includes the cached prefix; emit it exclusive.
      input_count := sessions.delivery_usage_counter(token->'input_tokens');
      IF p_source = 'codex' THEN
        cache_read := sessions.delivery_usage_counter(token->'cached_input_tokens');
        cache_write := sessions.delivery_usage_counter(token->'cache_write_input_tokens');
      ELSE
        cache_read := sessions.delivery_usage_counter(token->'cache_read_tokens');
        IF cache_read IS NULL THEN cache_read := sessions.delivery_usage_counter(token->'cached_tokens'); END IF;
        cache_write := sessions.delivery_usage_counter(token->'cache_write_tokens');
      END IF;
      reported := reported OR input_count IS NOT NULL OR cache_read IS NOT NULL OR cache_write IS NOT NULL;
      IF COALESCE(cache_read, 0) > COALESCE(input_count, 0) THEN
        RETURN jsonb_build_object('error', 'USAGE_COUNTER_REGRESSED');
      END IF;
      input_count := COALESCE(input_count, 0) - COALESCE(cache_read, 0);
    ELSIF p_source = 'claude' THEN
      -- Claude's input already excludes cache reads and writes.
      input_count := sessions.delivery_usage_counter(token->'input_tokens');
      cache_read := sessions.delivery_usage_counter(token->'cache_read_input_tokens');
      cache_write := sessions.delivery_usage_counter(token->'cache_creation_input_tokens');
      IF jsonb_typeof(token->'cache_creation') = 'object' THEN
        write_5m := sessions.delivery_usage_counter(token->'cache_creation'->'ephemeral_5m_input_tokens');
        write_1h := sessions.delivery_usage_counter(token->'cache_creation'->'ephemeral_1h_input_tokens');
      END IF;
      reported := reported OR input_count IS NOT NULL OR cache_read IS NOT NULL
        OR cache_write IS NOT NULL OR write_5m IS NOT NULL OR write_1h IS NOT NULL;
      -- Prefer the provider's own total; the split stays beside it.
      cache_write := COALESCE(cache_write, COALESCE(write_5m, 0) + COALESCE(write_1h, 0));
    ELSE
      -- Grok: only the verbatim per-turn `usage` breakdown is usage; a context
      -- snapshot alone is no evidence, and `turn_usage` is never normalized.
      IF NOT token ? 'usage' THEN RETURN NULL; END IF;
      breakdown := token->'usage';
      IF jsonb_typeof(breakdown) <> 'object' THEN RETURN jsonb_build_object('error', 'USAGE_NOT_AN_OBJECT'); END IF;
      input_count := sessions.delivery_usage_counter(sessions.delivery_usage_first(breakdown,
        ARRAY['inputTokens', 'input_tokens', 'promptTokens']));
      output_count := sessions.delivery_usage_counter(sessions.delivery_usage_first(breakdown,
        ARRAY['outputTokens', 'output_tokens', 'completionTokens']));
      cache_read := sessions.delivery_usage_counter(sessions.delivery_usage_first(breakdown,
        ARRAY['cachedReadTokens', 'cacheReadTokens', 'cache_read_input_tokens']));
      cache_write := sessions.delivery_usage_counter(sessions.delivery_usage_first(breakdown,
        ARRAY['cachedWriteTokens', 'cacheWriteTokens', 'cacheCreationTokens', 'cache_creation_input_tokens']));
      reasoning_count := sessions.delivery_usage_counter(sessions.delivery_usage_first(breakdown,
        ARRAY['reasoningTokens', 'thoughtTokens', 'thinkingTokens']));
      total_count := sessions.delivery_usage_counter(sessions.delivery_usage_first(breakdown,
        ARRAY['totalTokens', 'total_tokens']));
      -- Only the nested counters count as reported: top-level context counters
      -- read above are not Grok usage, so an empty `usage` reports nothing.
      reported := input_count IS NOT NULL OR output_count IS NOT NULL OR cache_read IS NOT NULL
        OR cache_write IS NOT NULL OR reasoning_count IS NOT NULL OR total_count IS NOT NULL;
      IF input_count IS NOT NULL THEN
        IF COALESCE(cache_read, 0) > input_count THEN
          RETURN jsonb_build_object('error', 'USAGE_COUNTER_REGRESSED');
        END IF;
        input_count := input_count - COALESCE(cache_read, 0);
      END IF;
    END IF;
  EXCEPTION WHEN raise_exception THEN
    RETURN jsonb_build_object('error', SQLERRM);
  END;
  -- A cost is only read back, never computed.
  cost := sessions.delivery_usage_first(token, ARRAY['cost_usd', 'costUSD', 'total_cost_usd']);
  IF cost IS NOT NULL THEN
    IF jsonb_typeof(cost) <> 'number' THEN RETURN jsonb_build_object('error', 'USAGE_INVALID_COST'); END IF;
    cost_usd := (cost #>> '{}')::numeric;
    IF cost_usd < 0 THEN RETURN jsonb_build_object('error', 'USAGE_INVALID_COST'); END IF;
    reported := true;
  END IF;
  IF NOT reported THEN RETURN NULL; END IF;
  IF GREATEST(COALESCE(input_count, 0), COALESCE(output_count, 0), COALESCE(reasoning_count, 0),
      COALESCE(cache_read, 0), COALESCE(cache_write, 0), COALESCE(cost_usd, 0) * 1000000) > 9223372036854775807 THEN
    RETURN jsonb_build_object('error', 'USAGE_COUNT_NOT_REPRESENTABLE');
  END IF;
  RETURN jsonb_build_object(
    'input', COALESCE(input_count, 0), 'output', COALESCE(output_count, 0),
    'reasoning', COALESCE(reasoning_count, 0), 'cacheRead', COALESCE(cache_read, 0),
    'cacheCreate', COALESCE(cache_write, 0), 'cacheCreate5m', write_5m, 'cacheCreate1h', write_1h,
    'costUsd', cost_usd);
END;
$usage$;

-- The canonical project id: the OSS `project_key` (`host/owner/repo` from the git
-- remote) reduced to `owner/repo`, the form 0006's backfill and `?project=` use.
-- A path-fallback key is machine-local, so the legacy project/cwd value stays.
CREATE OR REPLACE FUNCTION sessions.delivery_project_id(p_payload jsonb) RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $project$
DECLARE
  project_key text := NULLIF(btrim(p_payload->>'project_key'), '');
BEGIN
  IF project_key IS NOT NULL
    AND COALESCE(p_payload->>'project_key_method', '') NOT IN ('path', 'path_fallback')
    AND project_key ~ '^[^/~.\\:[:space:]][^\\:[:space:]]*/[^\\:[:space:]]+$' THEN
    IF project_key ~ '^[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+/[^/]+/.' THEN
      RETURN substring(project_key FROM '^[^/]+/(.+)$');
    END IF;
    RETURN project_key;
  END IF;
  RETURN COALESCE(NULLIF(p_payload->>'project_id', ''), p_payload->>'project', p_payload->>'cwd');
END;
$project$;

-- Count one request's usage exactly once (docs/usage-accounting.md, "The dedup
-- rule"). The group is every live projected assistant row of one request; only
-- its smallest delivery record id carries tokens. Copies that disagree
-- (`ambiguous-usage-copies`), or Claude rows grouped only by their per-block
-- uuid (`unresolved-request-identity`), carry no totals rather than a multiple.
CREATE OR REPLACE FUNCTION sessions.settle_delivery_request(
  p_org text, p_workspace text, p_source text, p_session text, p_key text
) RETURNS void LANGUAGE plpgsql AS $settle$
DECLARE
  variants integer;
  blob text;
  carrier text;
  usage jsonb;
BEGIN
  IF p_key IS NULL THEN RETURN; END IF;
  SELECT count(DISTINCT record->'payload'->>'token_json'), min(record->'payload'->>'token_json'), min(delivery_record_id)
    INTO variants, blob, carrier
    FROM sessions.convergence_events
   WHERE org_id = p_org AND workspace_id = p_workspace AND source = p_source
     AND session_id = p_session AND request_key = p_key;
  IF carrier IS NULL THEN RETURN; END IF;
  IF variants = 1 AND NOT (p_source = 'claude' AND p_key LIKE 'record-id:%') THEN
    usage := sessions.delivery_normalized_usage(p_source, blob);
    IF usage ? 'error' THEN usage := NULL; END IF;
  END IF;
  UPDATE sessions.convergence_events SET
    input_tokens = CASE WHEN delivery_record_id = carrier THEN COALESCE((usage->>'input')::bigint, 0) ELSE 0 END,
    output_tokens = CASE WHEN delivery_record_id = carrier THEN COALESCE((usage->>'output')::bigint, 0) ELSE 0 END,
    reasoning_tokens = CASE WHEN delivery_record_id = carrier THEN COALESCE((usage->>'reasoning')::bigint, 0) ELSE 0 END,
    cache_read_tokens = CASE WHEN delivery_record_id = carrier THEN COALESCE((usage->>'cacheRead')::bigint, 0) ELSE 0 END,
    cache_create_tokens = CASE WHEN delivery_record_id = carrier THEN COALESCE((usage->>'cacheCreate')::bigint, 0) ELSE 0 END,
    -- The 5m/1h split and cost are NULL when unreported (0021's contract), so a
    -- delivered session with no reported cost reads back as unknown, not $0.
    cache_create_5m_tokens = CASE WHEN delivery_record_id = carrier THEN (usage->>'cacheCreate5m')::bigint END,
    cache_create_1h_tokens = CASE WHEN delivery_record_id = carrier THEN (usage->>'cacheCreate1h')::bigint END,
    cost_usd_micros = CASE WHEN delivery_record_id = carrier
      THEN round((usage->>'costUsd')::numeric * 1000000)::bigint END
   WHERE org_id = p_org AND workspace_id = p_workspace AND source = p_source
     AND session_id = p_session AND request_key = p_key;
END;
$settle$;

-- A tool call or file edit with no time of its own takes the earliest projected
-- session event that names its tool_use_id, else its delivery time.
CREATE OR REPLACE FUNCTION sessions.settle_delivery_tool_ts(
  p_org text, p_workspace text, p_source text, p_session text, p_tool_use_id text
) RETURNS void LANGUAGE plpgsql AS $tool_ts$
BEGIN
  IF p_tool_use_id IS NULL THEN RETURN; END IF;
  UPDATE sessions.convergence_events AS tool SET ts = COALESCE((
      SELECT min(parent.ts) FROM sessions.convergence_events AS parent
       WHERE parent.org_id = p_org AND parent.workspace_id = p_workspace AND parent.source = p_source
         AND parent.session_id = p_session AND parent.kind = 'session_event'
         AND parent.delivery_record_id IS NOT NULL
         AND parent.record->'payload'->>'tool_use_id' = p_tool_use_id
    ), tool.ingested_at)
   WHERE tool.org_id = p_org AND tool.workspace_id = p_workspace AND tool.source = p_source
     AND tool.session_id = p_session AND tool.kind IN ('tool_call', 'file_edit')
     AND tool.delivery_record_id IS NOT NULL
     AND tool.record->'payload'->>'tool_use_id' = p_tool_use_id
     AND sessions.delivery_activity_ms(tool.record->'payload') IS NULL;
END;
$tool_ts$;

-- Remove the projection of one logical record (workspace, kind, record_id), from
-- whichever origin it came, plus its pre-0023 projection by this origin, then
-- re-settle the request group and tool times the removed row belonged to (a
-- request key or parent tool_use_id can change between revisions).
CREATE OR REPLACE FUNCTION sessions.retract_delivery_projection(
  p_org text, p_workspace text, p_origin text, p_record_id text, p_kind text, p_source text, p_session text
) RETURNS void LANGUAGE plpgsql AS $retract$
DECLARE
  machine text := 'delivery:' || md5(jsonb_build_array(p_workspace, p_origin)::text);
  prior record;
BEGIN
  -- Projections written before 0023 carry no delivery_record_id (0011/0014 event ids).
  IF p_session IS NOT NULL THEN
    DELETE FROM sessions.convergence_events
     WHERE org_id = p_org AND machine_id = machine AND source = p_source
       AND session_id = p_session AND kind = p_kind AND delivery_record_id IS NULL
       AND event_id IN (p_record_id, machine || ':' || p_record_id);
  END IF;
  FOR prior IN
    DELETE FROM sessions.convergence_events
     WHERE org_id = p_org AND workspace_id = p_workspace
       AND delivery_record_id = p_record_id AND kind = p_kind
    RETURNING source, session_id, request_key, kind AS projected_kind, record->'payload'->>'tool_use_id' AS tool_use_id
  LOOP
    PERFORM sessions.settle_delivery_request(p_org, p_workspace, prior.source, prior.session_id, prior.request_key);
    IF prior.projected_kind = 'session_event' THEN
      PERFORM sessions.settle_delivery_tool_ts(p_org, p_workspace, prior.source, prior.session_id, prior.tool_use_id);
    END IF;
  END LOOP;
END;
$retract$;

-- Payloads have already passed the delivery service's field allowlist and scrubber.
-- Keep session browsing in the same transaction as the durable revision fence.
-- Installed beside the live 0014 function, not over it: replacing it in place would
-- switch every upload to v2 before its indexes exist. 0029's rollout points the
-- delivery_session_projection trigger here once they are valid.
CREATE FUNCTION sessions.project_delivery_session_v2() RETURNS trigger LANGUAGE plpgsql AS $projection$
DECLARE
  activity_kinds text[] := ARRAY['history', 'session_event', 'tool_call', 'file_edit', 'trajectory'];
  winner sessions.delivery_records%ROWTYPE;
  winner_machine text;
  stale_kind text;
  p jsonb;
  activity timestamptz;
  milliseconds numeric;
  content_text text;
  request_identity text;
  tool_use text;
  tool_error boolean;
  tool_state text;
  args_value jsonb;
  tool_call_json jsonb;
BEGIN
  -- A revision can reclassify a record or move it: the write that did so is the
  -- newest statement about its prior identity, so that projection goes, exactly as
  -- a tombstone would remove it, before any early return for the new kind.
  -- An update from babysitter evidence (session_lineage, turn_receipt) to babysitter
  -- evidence cannot change any projection: the row was never projected and, having
  -- been evidence already, any cross-kind retraction ran when it first arrived. It
  -- returns before the lock, so expiry (which row-locks those rows first, across
  -- workspaces, then updates them) never waits on the workspace lock a delivery
  -- batch already holds. A newly inserted evidence row still takes the lock below
  -- and retracts another origin's activity projection of the same record.
  IF TG_OP = 'UPDATE' AND NEW.kind IN ('session_lineage', 'turn_receipt')
    AND OLD.kind IN ('session_lineage', 'turn_receipt') THEN
    RETURN NEW;
  END IF;
  -- Every other write takes the workspace lock before reading any projection, so
  -- the cross-kind check below cannot race another origin's write of the same
  -- record (both would otherwise see nothing and both insert). The lock is the one
  -- the catalog projection takes, held until commit; a batch holds only this one.
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'relayhistory.delivery_projection' || chr(31) || NEW.org_id || chr(31) || NEW.workspace_id, 0));
  IF TG_OP = 'UPDATE' AND OLD.kind = ANY (activity_kinds)
    AND (OLD.org_id, OLD.workspace_id, OLD.record_id, OLD.kind)
      IS DISTINCT FROM (NEW.org_id, NEW.workspace_id, NEW.record_id, NEW.kind) THEN
    PERFORM sessions.retract_delivery_projection(
      OLD.org_id, OLD.workspace_id, OLD.origin_id, OLD.record_id, OLD.kind, OLD.source, OLD.session_id);
  END IF;
  -- The same record_id may arrive from another origin under a new kind. That write
  -- is also the newest statement about the record, so any projection of it under
  -- another kind goes too, whichever origin wrote it (retract settles its group).
  IF EXISTS (
    SELECT 1 FROM sessions.convergence_events
     WHERE org_id = NEW.org_id AND workspace_id = NEW.workspace_id
       AND delivery_record_id = NEW.record_id AND kind <> NEW.kind
  ) THEN
    FOR stale_kind IN
      SELECT DISTINCT kind FROM sessions.convergence_events
       WHERE org_id = NEW.org_id AND workspace_id = NEW.workspace_id
         AND delivery_record_id = NEW.record_id AND kind <> NEW.kind
    LOOP
      PERFORM sessions.retract_delivery_projection(
        NEW.org_id, NEW.workspace_id, NEW.origin_id, NEW.record_id, stale_kind, NEW.source, NEW.session_id);
    END LOOP;
  END IF;
  IF NOT (NEW.kind = ANY (activity_kinds)) THEN RETURN NEW; END IF;
  -- Every origin of one workspace settles shared projections (cross-origin records,
  -- request groups) under the lock taken above, so concurrent origins cannot both win.

  -- The newest delivery write of this logical record, from any origin, wins, and
  -- the arriving row is that write: the lock above is held until commit and each
  -- statement takes a fresh snapshot, so whoever acquires it later wrote later.
  -- record_id is origin-independent. received_at cannot order origins: it is now()
  -- in accept_delivery_batch, the transaction start, so a writer that waited on
  -- the lock carries an older stamp than the write it follows.
  winner := NEW;
  winner_machine := 'delivery:' || md5(jsonb_build_array(NEW.workspace_id, NEW.origin_id)::text);
  PERFORM sessions.retract_delivery_projection(
    NEW.org_id, NEW.workspace_id, NEW.origin_id, NEW.record_id, NEW.kind, NEW.source, NEW.session_id);

  p := winner.payload;
  IF winner.operation = 'upsert' AND winner.session_id IS NOT NULL
    -- Control rows are harness plumbing, not activity (sourcing-contract "Control rows").
    AND NOT (winner.kind = 'session_event' AND (
      NULLIF(p->>'control_kind', '') IS NOT NULL OR COALESCE(p->>'is_meta', '') IN ('1', 'true')))
  THEN
    tool_use := NULLIF(p->>'tool_use_id', '');
    activity := winner.received_at;
    milliseconds := sessions.delivery_activity_ms(p);
    IF milliseconds IS NOT NULL THEN
      activity := to_timestamp((milliseconds / 1000)::double precision);
    ELSIF winner.kind IN ('tool_call', 'file_edit') AND tool_use IS NOT NULL THEN
      SELECT COALESCE(min(parent.ts), activity) INTO activity FROM sessions.convergence_events AS parent
       WHERE parent.org_id = winner.org_id AND parent.workspace_id = winner.workspace_id
         AND parent.source = winner.source AND parent.session_id = winner.session_id
         AND parent.kind = 'session_event' AND parent.delivery_record_id IS NOT NULL
         AND parent.record->'payload'->>'tool_use_id' = tool_use;
    END IF;
    content_text := CASE winner.kind
      WHEN 'history' THEN p->>'prompt'
      WHEN 'session_event' THEN p->>'text'
      WHEN 'tool_call' THEN concat_ws(' ', p->>'name', p->>'target')
      WHEN 'file_edit' THEN p->>'file_path'
      WHEN 'trajectory' THEN COALESCE(p->>'task_description', p->>'search_text')
      ELSE NULL END;
    -- The request identity `session_requests` groups by: namespace-qualified,
    -- verbatim, only for assistant rows with a record id.
    request_identity := CASE WHEN winner.kind = 'session_event' AND p->>'role' = 'assistant' AND NULLIF(p->>'message_id', '') IS NOT NULL THEN
      CASE
        WHEN NULLIF(p->>'request_id', '') IS NOT NULL THEN 'request-id:' || (p->>'request_id')
        WHEN NULLIF(p->>'provider_message_id', '') IS NOT NULL THEN 'provider-message-id:' || (p->>'provider_message_id')
        WHEN NULLIF(p->>'request_span', '') IS NOT NULL THEN 'request-span:' || (p->>'request_span')
        ELSE 'record-id:' || (p->>'message_id')
      END END;
    IF winner.kind = 'tool_call' THEN
      tool_error := CASE WHEN (p->>'is_error') IN ('1', 'true') THEN true WHEN (p->>'is_error') IN ('0', 'false') THEN false END;
      tool_state := CASE tool_error WHEN true THEN 'error' WHEN false THEN 'success' END;
      BEGIN
        args_value := (p->>'args_json')::jsonb;
      EXCEPTION WHEN others THEN
        args_value := NULL;
      END;
      SELECT COALESCE(jsonb_object_agg(field.key, field.value), '{}'::jsonb) INTO tool_call_json
        FROM jsonb_each(jsonb_build_object(
          'toolUseId', tool_use, 'name', p->>'name', 'target', p->>'target',
          'args', args_value, 'isError', tool_error, 'status', tool_state)) AS field
       WHERE field.value <> 'null'::jsonb;
    END IF;
    INSERT INTO sessions.convergence_events (
      org_id, workspace_id, machine_id, user_id, source, session_id, event_id,
      kind, type, ts, actor_role, subagent_id, project_id, content, task_title, model, provider,
      tool_name, tool_status, tool_calls, files_touched, delivery_record_id, request_key,
      cost_usd_micros, record, ingested_at
    ) VALUES (
      winner.org_id, winner.workspace_id, winner_machine, winner.user_id, winner.source, winner.session_id,
      winner_machine || ':' || winner.record_id,
      winner.kind, COALESCE(p->>'role', winner.kind), activity,
      CASE WHEN winner.kind = 'history' THEN 'user' ELSE p->>'role' END,
      -- Only a subagent session id marks subagent activity: root assistant events
      -- can carry their own agent_id.
      NULLIF(p->>'subagent_session_id', ''),
      sessions.delivery_project_id(p), content_text,
      CASE WHEN winner.kind = 'trajectory' THEN p->>'task_title' ELSE NULL END,
      p->>'model', NULLIF(p->>'provider', ''),
      CASE WHEN winner.kind = 'tool_call' THEN p->>'name' END,
      tool_state,
      CASE WHEN winner.kind = 'tool_call' THEN jsonb_build_array(tool_call_json) ELSE '[]'::jsonb END,
      CASE WHEN winner.kind = 'file_edit' AND NULLIF(p->>'file_path', '') IS NOT NULL
        THEN jsonb_build_array(p->>'file_path') ELSE '[]'::jsonb END,
      winner.record_id, request_identity, NULL,
      jsonb_build_object('deliveryRecordId', winner.record_id, 'payload', p), winner.received_at
    );
    PERFORM sessions.settle_delivery_request(winner.org_id, winner.workspace_id, winner.source, winner.session_id, request_identity);
    IF winner.kind = 'session_event' AND tool_use IS NOT NULL THEN
      PERFORM sessions.settle_delivery_tool_ts(winner.org_id, winner.workspace_id, winner.source, winner.session_id, tool_use);
    END IF;
  END IF;
  RETURN NEW;
END;
$projection$;

-- Retained rows are reprojected by 0029's rollout in short batches after v2 is
-- live, not here: replaying every activity record held the delivery_records write
-- lock for hours inside the deploy transaction.
