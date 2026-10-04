-- A conversation turn belongs to the workspace and source that wrote it.
--
-- Session ids are unique only per (org, workspace, source), like session_outcomes
-- before 0018. Keying turns by (org_id, session_id, turn_index) made a Claude and a
-- Codex session that share an id, or two workspaces in one org reporting the same
-- session id, upsert over each other's turns. This adds `workspace_id` and `source`
-- and widens the key to (org_id, workspace_id, source, session_id, turn_index).
--
-- New writes: workspace_id comes from the authenticated session (`default` when the
-- token carries none, as ingest does). source is metadata.nativeCli, else
-- metadata.source (from the turn, then from any turn in the same request), else
-- session_owner. The retired OSS turns publisher set sessionOwner to the capture source
-- and sent no source metadata. Resolution never reads stored rows, so an untagged
-- upload cannot overwrite a turn another source stored for the same owner. Reads treat
-- a stored source other than session_owner as an explicit tag.
--
-- Existing rows are unique on the old three-column key, so any assignment of source and
-- workspace keeps them unique on the wider key and the new constraint always builds. A
-- collision written before this migration kept only the last writer's row; that
-- overwritten turn was never stored and cannot be reconstructed here.

ALTER TABLE sessions.conversation_turns
  ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS workspace_id text NOT NULL DEFAULT 'default';

-- Source backfill, per row: the row's own metadata, else its session owner.
UPDATE sessions.conversation_turns
   SET source = COALESCE(
         NULLIF(metadata->>'nativeCli', ''),
         NULLIF(metadata->>'source', ''),
         session_owner
       );

-- Untagged turns inherit the one source their owner's tagged turns in the same session
-- name, matching how the write path resolves an untagged turn in a tagged request. An
-- owner whose turns name several sources keeps session_owner.
WITH tagged AS (
  SELECT org_id, session_id, session_owner, min(explicit) AS source
    FROM (
      SELECT org_id, session_id, session_owner,
             COALESCE(NULLIF(metadata->>'nativeCli', ''),
                      NULLIF(metadata->>'source', '')) AS explicit
        FROM sessions.conversation_turns
    ) AS turns
   WHERE explicit IS NOT NULL
   GROUP BY org_id, session_id, session_owner
  HAVING count(DISTINCT explicit) = 1
)
UPDATE sessions.conversation_turns AS t
   SET source = tagged.source
  FROM tagged
 WHERE t.org_id = tagged.org_id
   AND t.session_id = tagged.session_id
   AND t.session_owner = tagged.session_owner
   AND NULLIF(t.metadata->>'nativeCli', '') IS NULL
   AND NULLIF(t.metadata->>'source', '') IS NULL;

-- Workspace backfill. Turns never recorded a workspace, so it is taken from the
-- session's attributed events (the same token pushed both) only where that is
-- unambiguous: first by the turn's own source, then, for a turn whose source matches
-- no event (a human session owner), by the session's single workspace. Everything else
-- keeps `default`, the workspace ingest records for a token that carries none.
WITH attributed AS (
  SELECT e.org_id, e.session_id, e.source, min(e.workspace_id) AS workspace_id
    FROM sessions.convergence_events AS e
   WHERE EXISTS (
     SELECT 1 FROM sessions.conversation_turns AS t
      WHERE t.org_id = e.org_id AND t.session_id = e.session_id
   )
   GROUP BY e.org_id, e.session_id, e.source
  HAVING count(DISTINCT e.workspace_id) = 1
)
UPDATE sessions.conversation_turns AS t
   SET workspace_id = attributed.workspace_id
  FROM attributed
 WHERE t.org_id = attributed.org_id
   AND t.session_id = attributed.session_id
   AND t.source = attributed.source;

WITH attributed AS (
  SELECT e.org_id, e.session_id, min(e.workspace_id) AS workspace_id
    FROM sessions.convergence_events AS e
   WHERE EXISTS (
     SELECT 1 FROM sessions.conversation_turns AS t
      WHERE t.org_id = e.org_id AND t.session_id = e.session_id
   )
   GROUP BY e.org_id, e.session_id
  HAVING count(DISTINCT e.workspace_id) = 1
)
UPDATE sessions.conversation_turns AS t
   SET workspace_id = attributed.workspace_id
  FROM attributed
 WHERE t.org_id = attributed.org_id
   AND t.session_id = attributed.session_id
   AND NOT EXISTS (
     SELECT 1 FROM sessions.convergence_events AS e
      WHERE e.org_id = t.org_id
        AND e.session_id = t.session_id
        AND e.source = t.source
   );

-- Replace the three-column key. 0004 declared it inline, so it is found by its
-- columns rather than by the name Postgres generated for it.
DO $$
DECLARE
  existing_key text;
BEGIN
  FOR existing_key IN
    SELECT c.conname
      FROM pg_catalog.pg_constraint AS c
     WHERE c.conrelid = 'sessions.conversation_turns'::regclass
       AND c.contype = 'u'
       AND (
         SELECT array_agg(a.attname::text ORDER BY a.attname)
           FROM pg_catalog.pg_attribute AS a
          WHERE a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
       ) = ARRAY['org_id', 'session_id', 'turn_index']
  LOOP
    EXECUTE format(
      'ALTER TABLE sessions.conversation_turns DROP CONSTRAINT %I',
      existing_key
    );
  END LOOP;
END
$$;

-- The name schema.ts used for the old key; never created by a migration, dropped in
-- case a hand-built table carries it.
DROP INDEX IF EXISTS sessions.conversation_turns_org_session_index_uidx;

ALTER TABLE sessions.conversation_turns
  ADD CONSTRAINT conversation_turns_scope_turn_key
    UNIQUE (org_id, workspace_id, source, session_id, turn_index);

-- Two stored turns can now share a turn_index, so they share the revision row key
-- below. Ordering by the row hash as well keeps the digest deterministic; for every
-- session without such a pair the digest, and so every cached brief, is unchanged.
CREATE OR REPLACE FUNCTION sessions.session_source_revision(p_org_id text, p_session_id text)
RETURNS text LANGUAGE sql STABLE
SET search_path = pg_catalog, sessions, pg_temp AS $session_revision$
  SELECT CASE WHEN count(*) = 0 THEN NULL
              ELSE md5(string_agg(row_key || '=' || row_hash, ',' ORDER BY row_key, row_hash)) END
    FROM (
      SELECT 'e|' || machine_id || '|' || source || '|' || kind || '|' || event_id AS row_key,
             md5(record::text || '|' || COALESCE(content, '')) AS row_hash
        FROM sessions.convergence_events
       WHERE org_id = p_org_id AND session_id = p_session_id
      UNION ALL
      SELECT 't|' || lpad(turn_index::text, 10, '0'),
             md5(role || '|' || content || '|' || metadata::text || '|' || extract(epoch FROM ts)::text)
        FROM sessions.conversation_turns
       WHERE org_id = p_org_id AND session_id = p_session_id
    ) AS session_rows;
$session_revision$;
