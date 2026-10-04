-- Receipts and revision fences are permanent protocol state, not a processing queue.
CREATE TABLE IF NOT EXISTS sessions.delivery_origins (
  org_id text NOT NULL, workspace_id text NOT NULL, origin_id text NOT NULL,
  PRIMARY KEY (org_id, workspace_id, origin_id)
);
CREATE TABLE IF NOT EXISTS sessions.delivery_records (
  org_id text NOT NULL, workspace_id text NOT NULL, origin_id text NOT NULL,
  record_id text NOT NULL, revision_id text NOT NULL, revision bigint NOT NULL CHECK (revision > 0),
  digest text NOT NULL, kind text NOT NULL, source text NOT NULL, session_id text,
  operation text NOT NULL CHECK (operation IN ('upsert', 'delete')), payload jsonb,
  user_id text NOT NULL, received_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, workspace_id, origin_id, record_id),
  CHECK ((operation = 'delete' AND payload IS NULL) OR (operation = 'upsert' AND jsonb_typeof(payload) = 'object'))
);
CREATE TABLE IF NOT EXISTS sessions.delivery_receipts (
  org_id text NOT NULL, workspace_id text NOT NULL, origin_id text NOT NULL,
  batch_id text NOT NULL, digest text NOT NULL, receipt jsonb NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, workspace_id, origin_id, batch_id)
);
CREATE INDEX IF NOT EXISTS delivery_records_lookup ON sessions.delivery_records(org_id, workspace_id, kind, source, session_id, origin_id, record_id);

-- A single SELECT is an actual transaction even through Neon's HTTP driver.
-- All writers lock exactly one authenticated tenant/origin before touching records.
CREATE OR REPLACE FUNCTION sessions.accept_delivery_batch(
  p_org text, p_workspace text, p_user text, p_origin text, p_batch text,
  p_digest text, p_records jsonb, p_receipt jsonb
) RETURNS jsonb LANGUAGE plpgsql AS $delivery$
DECLARE
  prior sessions.delivery_receipts%ROWTYPE;
  current_record sessions.delivery_records%ROWTYPE;
  item jsonb;
BEGIN
  INSERT INTO sessions.delivery_origins VALUES (p_org, p_workspace, p_origin) ON CONFLICT DO NOTHING;
  PERFORM 1 FROM sessions.delivery_origins WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin FOR UPDATE;
  SELECT * INTO prior FROM sessions.delivery_receipts WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin AND batch_id=p_batch;
  IF FOUND THEN
    IF prior.digest <> p_digest THEN RAISE EXCEPTION 'delivery_batch_conflict' USING ERRCODE='P0001'; END IF;
    RETURN prior.receipt;
  END IF;
  FOR item IN SELECT value FROM jsonb_array_elements(p_records) ORDER BY value->>'record_id', (value->>'revision')::bigint LOOP
    SELECT * INTO current_record FROM sessions.delivery_records WHERE org_id=p_org AND workspace_id=p_workspace AND origin_id=p_origin AND record_id=item->>'record_id';
    IF FOUND AND current_record.revision=(item->>'revision')::bigint AND current_record.digest<>item->>'digest' THEN
      RAISE EXCEPTION 'delivery_revision_conflict' USING ERRCODE='P0001';
    END IF;
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
