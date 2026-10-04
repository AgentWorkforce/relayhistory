-- The minute retention sweep is global, while the permanent protocol table is
-- tenant-keyed. Index only live Babysitter evidence by its declared expiry so
-- each tick seeks to due rows instead of scanning unrelated permanent history.
CREATE INDEX IF NOT EXISTS delivery_records_babysitter_expiry
ON sessions.delivery_records (
  ((payload->>'expires_at_ms')::numeric),
  received_at,
  org_id,
  workspace_id,
  origin_id,
  record_id
)
WHERE kind IN ('session_lineage','turn_receipt')
  AND operation='upsert'
  AND jsonb_typeof(payload->'expires_at_ms')='number';
