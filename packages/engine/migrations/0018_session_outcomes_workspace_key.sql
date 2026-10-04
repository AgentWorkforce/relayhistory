-- A session outcome belongs to the workspace that recorded it.
--
-- Session ids are unique only per (org, workspace, source): two workspaces in one org
-- can report the same source, session id and commit. Keying outcomes by
-- (org_id, source, session_id, commit_sha) made the second workspace's upsert rewrite
-- the first workspace's row, so a workspace-scoped digest classified the first
-- workspace's session as unknown. Adding workspace_id to the key keeps one row per
-- workspace.
--
-- Existing rows: workspace_id has been NOT NULL since 0003, so every row already has a
-- workspace and needs no backfill. Rows are unique on the old four-column key, so they
-- are unique on the wider key and the new constraint always builds. A collision written
-- before this migration kept only the last writer's row; that overwritten attribution
-- was never stored and cannot be reconstructed here.
--
-- Indexes: the new key leads with (org_id, workspace_id, source, session_id) and serves
-- workspace-scoped outcome lookups. Organization-wide lookups keep
-- session_outcomes_org_session_idx and session_outcomes_org_commit_idx.
--
-- The existing primary key is found by type rather than by name, so the migration does
-- not depend on how the constraint was named when the table was first created.
DO $$
DECLARE
  existing_key text;
BEGIN
  SELECT conname INTO existing_key
    FROM pg_catalog.pg_constraint
   WHERE conrelid = 'sessions.session_outcomes'::regclass
     AND contype = 'p';
  IF existing_key IS NOT NULL THEN
    EXECUTE format(
      'ALTER TABLE sessions.session_outcomes DROP CONSTRAINT %I',
      existing_key
    );
  END IF;
END
$$;

ALTER TABLE sessions.session_outcomes
  ADD CONSTRAINT session_outcomes_pkey
    PRIMARY KEY (org_id, workspace_id, source, session_id, commit_sha);
