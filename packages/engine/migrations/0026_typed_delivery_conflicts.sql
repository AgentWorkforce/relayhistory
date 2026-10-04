-- Return the exact durable or in-batch identities that caused a delivery
-- conflict. 0009 raised an opaque exception, which made a poison record block
-- every later record in the client queue: the client had no safe authority to
-- quarantine one record or to distinguish it from reuse of a batch ID.
--
-- The tenant/origin row lock still serializes every decision. Conflict
-- preflight finishes before any delivery record is written. Detail is carried
-- on the same exception names understood by the pre-0026 Worker, so applying
-- this migration before deploying the new Worker (or rolling its code back)
-- still produces HTTP 409 rather than a false success. The first
-- 100 conflicts are returned in deterministic order and conflictCount reports
-- the total.
CREATE OR REPLACE FUNCTION sessions.accept_delivery_batch(
  p_org text, p_workspace text, p_user text, p_origin text, p_batch text,
  p_digest text, p_records jsonb, p_receipt jsonb
) RETURNS jsonb LANGUAGE plpgsql AS $delivery$
DECLARE
  prior sessions.delivery_receipts%ROWTYPE;
  current_record sessions.delivery_records%ROWTYPE;
  item jsonb;
  submitted_current jsonb;
  has_current boolean;
  comparison_revision_id text;
  comparison_revision bigint;
  comparison_digest text;
  conflicts jsonb := '[]'::jsonb;
  conflict_count integer := 0;
BEGIN
  INSERT INTO sessions.delivery_origins VALUES (p_org, p_workspace, p_origin)
    ON CONFLICT DO NOTHING;
  PERFORM 1 FROM sessions.delivery_origins WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin FOR UPDATE;
  SELECT * INTO prior FROM sessions.delivery_receipts WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin AND batch_id=p_batch;
  IF FOUND THEN
    IF prior.digest <> p_digest THEN
      RAISE EXCEPTION 'delivery_batch_conflict' USING ERRCODE='P0001', DETAIL=jsonb_build_object(
        'conflict', jsonb_build_object(
          'type', 'batch_id',
          'originId', p_origin,
          'batchId', p_batch,
          'submittedDigest', p_digest,
          'currentDigest', prior.digest
        )
      )::text;
    END IF;
    RETURN prior.receipt;
  END IF;

  -- Compare every submitted identity before writing. A revision equal to the
  -- durable fence compares with that row. A higher/new revision compares with
  -- the first submitted peer at that numeric revision, which preserves 0009's
  -- rejection of two in-batch identities that reuse one revision with
  -- different content.
  FOR item IN
    SELECT value FROM jsonb_array_elements(p_records)
    ORDER BY value->>'record_id', (value->>'revision')::bigint, value->>'revision_id'
  LOOP
    comparison_revision_id := NULL;
    comparison_revision := NULL;
    comparison_digest := NULL;
    SELECT * INTO current_record FROM sessions.delivery_records
      WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin
        AND record_id=item->>'record_id';
    has_current := FOUND;

    IF has_current
      AND current_record.revision=(item->>'revision')::bigint
      AND current_record.digest<>item->>'digest' THEN
      comparison_revision_id := current_record.revision_id;
      comparison_revision := current_record.revision;
      comparison_digest := current_record.digest;
    ELSIF NOT has_current
      OR current_record.revision<(item->>'revision')::bigint THEN
      SELECT value INTO submitted_current
        FROM jsonb_array_elements(p_records)
        WHERE value->>'record_id'=item->>'record_id'
          AND (value->>'revision')::bigint=(item->>'revision')::bigint
        ORDER BY value->>'revision_id'
        LIMIT 1;
      IF submitted_current->>'revision_id'<>item->>'revision_id'
        AND submitted_current->>'digest'<>item->>'digest' THEN
        comparison_revision_id := submitted_current->>'revision_id';
        comparison_revision := (submitted_current->>'revision')::bigint;
        comparison_digest := submitted_current->>'digest';
      END IF;
    END IF;

    IF comparison_revision_id IS NOT NULL THEN
      conflict_count := conflict_count + 1;
      IF conflict_count <= 100 THEN
        conflicts := conflicts || jsonb_build_object(
          'type', 'record_revision',
          'originId', p_origin,
          'recordId', item->>'record_id',
          'submittedRevisionId', item->>'revision_id',
          'submittedRevision', (item->>'revision')::bigint,
          'submittedDigest', item->>'digest',
          'currentRevisionId', comparison_revision_id,
          'currentRevision', comparison_revision,
          'currentDigest', comparison_digest
        );
      END IF;
    END IF;
  END LOOP;

  IF conflict_count > 0 THEN
    RAISE EXCEPTION 'delivery_revision_conflict' USING ERRCODE='P0001', DETAIL=jsonb_build_object(
      'conflict', conflicts->0,
      'conflicts', conflicts,
      'conflictCount', conflict_count
    )::text;
  END IF;

  FOR item IN
    SELECT value FROM jsonb_array_elements(p_records)
    ORDER BY value->>'record_id', (value->>'revision')::bigint, value->>'revision_id'
  LOOP
    INSERT INTO sessions.delivery_records(org_id,workspace_id,origin_id,record_id,revision_id,revision,digest,kind,source,session_id,operation,payload,user_id)
      VALUES(p_org,p_workspace,p_origin,item->>'record_id',item->>'revision_id',(item->>'revision')::bigint,item->>'digest',item->>'kind',item->>'source',item->>'session_id',item->>'operation',NULLIF(item->'payload','null'::jsonb),p_user)
      ON CONFLICT(org_id,workspace_id,origin_id,record_id) DO UPDATE SET
        revision_id=excluded.revision_id,revision=excluded.revision,digest=excluded.digest,kind=excluded.kind,source=excluded.source,session_id=excluded.session_id,
        operation=excluded.operation,payload=excluded.payload,user_id=excluded.user_id,received_at=now()
      WHERE excluded.revision>sessions.delivery_records.revision;
  END LOOP;
  INSERT INTO sessions.delivery_receipts(org_id,workspace_id,origin_id,batch_id,digest,receipt) VALUES(p_org,p_workspace,p_origin,p_batch,p_digest,p_receipt);
  RETURN p_receipt;
END;
$delivery$;
