-- Repair the projection without changing the checksum of applied migration 0011.
-- Metadata stays durable in delivery_records; only actual activity enters replay.
-- Keep session browsing in the same transaction as the durable revision fence.
-- Payloads have already passed the delivery service's field allowlist and scrubber.
CREATE OR REPLACE FUNCTION sessions.project_delivery_session() RETURNS trigger LANGUAGE plpgsql AS $projection$
DECLARE
  machine text;
  activity timestamptz;
  milliseconds numeric;
  content_text text;
BEGIN
  machine := 'delivery:' || md5(jsonb_build_array(NEW.workspace_id, NEW.origin_id)::text);
  DELETE FROM sessions.convergence_events
    WHERE org_id = NEW.org_id AND machine_id = machine AND event_id IN (NEW.record_id, machine || ':' || NEW.record_id);
  IF NEW.operation = 'delete' OR NEW.session_id IS NULL
    OR NEW.kind NOT IN ('history', 'session_event', 'tool_call', 'file_edit', 'trajectory')
    THEN RETURN NEW; END IF;
  activity := NEW.received_at;
  milliseconds := COALESCE(
    CASE WHEN jsonb_typeof(NEW.payload->'updated_ms') = 'number' THEN (NEW.payload->>'updated_ms')::numeric END,
    CASE WHEN jsonb_typeof(NEW.payload->'ts_ms') = 'number' THEN (NEW.payload->>'ts_ms')::numeric END,
    CASE WHEN jsonb_typeof(NEW.payload->'timestamp_ms') = 'number' THEN (NEW.payload->>'timestamp_ms')::numeric END,
    CASE WHEN jsonb_typeof(NEW.payload->'last_activity_ms') = 'number' THEN (NEW.payload->>'last_activity_ms')::numeric END
  );
  IF milliseconds BETWEEN 0 AND 8640000000000000 THEN activity := to_timestamp((milliseconds / 1000)::double precision); END IF;
  content_text := CASE NEW.kind
    WHEN 'history' THEN NEW.payload->>'prompt'
    WHEN 'session_event' THEN NEW.payload->>'text'
    WHEN 'tool_call' THEN concat_ws(' ', NEW.payload->>'name', NEW.payload->>'target')
    WHEN 'file_edit' THEN NEW.payload->>'file_path'
    WHEN 'trajectory' THEN COALESCE(NEW.payload->>'task_description', NEW.payload->>'search_text')
    ELSE NULL END;
  -- Record IDs are origin-scoped; expose a globally unique replay event ID.
  INSERT INTO sessions.convergence_events (
    org_id, workspace_id, machine_id, user_id, source, session_id, event_id,
    kind, type, ts, actor_role, project_id, content, task_title, model, record, ingested_at
  ) VALUES (
    NEW.org_id, NEW.workspace_id, machine, NEW.user_id, NEW.source, NEW.session_id, machine || ':' || NEW.record_id,
    NEW.kind, COALESCE(NEW.payload->>'role', NEW.kind), activity,
    CASE WHEN NEW.kind = 'history' THEN 'user' ELSE NEW.payload->>'role' END,
    COALESCE(NEW.payload->>'project', NEW.payload->>'cwd'), content_text,
    CASE WHEN NEW.kind = 'trajectory' THEN NEW.payload->>'task_title' ELSE NULL END,
    NEW.payload->>'model', jsonb_build_object('deliveryRecordId', NEW.record_id, 'payload', NEW.payload), NEW.received_at
  );
  RETURN NEW;
END;
$projection$;

-- Reproject retained rows atomically, removing old metadata and invented titles.
UPDATE sessions.delivery_records SET received_at = received_at;
