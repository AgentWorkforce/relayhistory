-- Request settlement writes only the rows whose usage changes. 0023 stays as applied.
--
-- 0023's settle_delivery_request rewrote every member of a request group each
-- time one member was written, so a request copied onto N content blocks cost
-- 1 + 2 + ... + N row versions. In one delivery transaction those dead versions
-- cannot be pruned and every later group lookup walks them: a 500-block request
-- in one batch took 17-32 s under the workspace projection lock (100 blocks:
-- 0.24 s). The result is the same; only the no-op writes are skipped. A newly
-- projected member (inserted with NULL usage) and a carrier that changes are
-- still written, so each settle writes O(1) rows and the 500-block batch takes
-- about 0.2 s. No convergence_events trigger observes the skipped updates.
CREATE OR REPLACE FUNCTION sessions.settle_delivery_request(
  p_org text, p_workspace text, p_source text, p_session text, p_key text
) RETURNS void LANGUAGE plpgsql AS $settle$
DECLARE
  variants integer;
  blob text;
  carrier text;
  usage jsonb;
  c_input bigint;
  c_output bigint;
  c_reasoning bigint;
  c_cache_read bigint;
  c_cache_create bigint;
  c_cache_5m bigint;
  c_cache_1h bigint;
  c_cost bigint;
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
  c_input := COALESCE((usage->>'input')::bigint, 0);
  c_output := COALESCE((usage->>'output')::bigint, 0);
  c_reasoning := COALESCE((usage->>'reasoning')::bigint, 0);
  c_cache_read := COALESCE((usage->>'cacheRead')::bigint, 0);
  c_cache_create := COALESCE((usage->>'cacheCreate')::bigint, 0);
  -- The 5m/1h split and cost are NULL when unreported (0021's contract), so a
  -- delivered session with no reported cost reads back as unknown, not $0.
  c_cache_5m := (usage->>'cacheCreate5m')::bigint;
  c_cache_1h := (usage->>'cacheCreate1h')::bigint;
  c_cost := round((usage->>'costUsd')::numeric * 1000000)::bigint;
  -- The canonical row carries the request's usage.
  UPDATE sessions.convergence_events SET
    (input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_create_tokens,
     cache_create_5m_tokens, cache_create_1h_tokens, cost_usd_micros)
    = (c_input, c_output, c_reasoning, c_cache_read, c_cache_create, c_cache_5m, c_cache_1h, c_cost)
   WHERE org_id = p_org AND workspace_id = p_workspace AND source = p_source
     AND session_id = p_session AND request_key = p_key
     AND delivery_record_id = carrier
     AND (input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_create_tokens,
          cache_create_5m_tokens, cache_create_1h_tokens, cost_usd_micros)
       IS DISTINCT FROM (c_input, c_output, c_reasoning, c_cache_read, c_cache_create, c_cache_5m, c_cache_1h, c_cost);
  -- Every other member carries zero counters and unknown split/cost.
  UPDATE sessions.convergence_events SET
    (input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_create_tokens,
     cache_create_5m_tokens, cache_create_1h_tokens, cost_usd_micros)
    = (0, 0, 0, 0, 0, NULL, NULL, NULL)
   WHERE org_id = p_org AND workspace_id = p_workspace AND source = p_source
     AND session_id = p_session AND request_key = p_key
     AND delivery_record_id IS DISTINCT FROM carrier
     AND (input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_create_tokens,
          cache_create_5m_tokens, cache_create_1h_tokens, cost_usd_micros)
       IS DISTINCT FROM (0::bigint, 0::bigint, 0::bigint, 0::bigint, 0::bigint, NULL::bigint, NULL::bigint, NULL::bigint);
END;
$settle$;
