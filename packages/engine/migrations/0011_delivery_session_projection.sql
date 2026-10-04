-- Keep session browsing in the same transaction as the durable revision fence.
-- Payloads have already passed the delivery service's field allowlist and scrubber.
CREATE FUNCTION sessions.project_delivery_session() RETURNS trigger LANGUAGE plpgsql AS $projection$
DECLARE
  machine text;
  activity timestamptz;
  milliseconds numeric;
  content_text text;
BEGIN
  machine := 'delivery:' || md5(jsonb_build_array(NEW.workspace_id, NEW.origin_id)::text);
  DELETE FROM sessions.convergence_events
    WHERE org_id = NEW.org_id AND machine_id = machine AND event_id = NEW.record_id;
  IF NEW.operation = 'delete' OR NEW.session_id IS NULL THEN RETURN NEW; END IF;
  activity := NEW.received_at;
  IF jsonb_typeof(COALESCE(NEW.payload->'ts_ms', NEW.payload->'timestamp_ms', NEW.payload->'last_activity_ms')) = 'number' THEN
    milliseconds := COALESCE(NEW.payload->>'ts_ms', NEW.payload->>'timestamp_ms', NEW.payload->>'last_activity_ms')::numeric;
    IF milliseconds BETWEEN 0 AND 8640000000000000 THEN activity := to_timestamp((milliseconds / 1000)::double precision); END IF;
  END IF;
  content_text := CASE NEW.kind
    WHEN 'history' THEN NEW.payload->>'prompt'
    WHEN 'session_event' THEN NEW.payload->>'text'
    ELSE NULL END;
  INSERT INTO sessions.convergence_events (
    org_id, workspace_id, machine_id, user_id, source, session_id, event_id,
    kind, type, ts, actor_role, project_id, content, task_title, model, record, ingested_at
  ) VALUES (
    NEW.org_id, NEW.workspace_id, machine, NEW.user_id, NEW.source, NEW.session_id, NEW.record_id,
    NEW.kind, COALESCE(NEW.payload->>'role', NEW.kind), activity,
    CASE WHEN NEW.kind = 'history' THEN 'user' ELSE NEW.payload->>'role' END,
    COALESCE(NEW.payload->>'project', NEW.payload->>'cwd'), content_text,
    CASE WHEN NEW.kind = 'history' OR NEW.payload->>'role' = 'user' THEN left(content_text, 160) ELSE NULL END,
    NEW.payload->>'model', jsonb_build_object('deliveryRecordId', NEW.record_id, 'payload', NEW.payload), NEW.received_at
  );
  RETURN NEW;
END;
$projection$;
CREATE TRIGGER delivery_session_projection AFTER INSERT OR UPDATE ON sessions.delivery_records
  FOR EACH ROW EXECUTE FUNCTION sessions.project_delivery_session();
-- Repair existing retained rows when this projection is first introduced.
UPDATE sessions.delivery_records SET received_at = received_at;
