-- Preserve the pre-backfill project_id so canonicalisation is reversible.
--
-- The store held 45,394 distinct project ids for a few dozen repos: `resolve_project_id`
-- preferred the harness cwd over the git remote, and a path was reduced to its last
-- segment, so every worktree and subdirectory of one repo became its own project
-- (`relayfile`, `relayfile-6e8d1a5c`, `relayfile-fork`; `cloud/packages/web` -> `web`).
-- Only 128 of 274,264 events carried the `owner/repo` form that `?project=` is queried
-- with.
--
-- The backfill rewrites project_id in place. Writing the original here first is what
-- makes that safe: the mapping is many-to-one, so the old value cannot be recovered
-- from the new one, and without this column the rewrite would be irreversible.
--
-- NULL means "never rewritten", which is why the backfill is idempotent: it only
-- touches rows where this is still NULL.
ALTER TABLE sessions.convergence_events
  ADD COLUMN IF NOT EXISTS project_id_original TEXT;

-- Partial index: only the rewritten rows are ever looked up by original id (to audit a
-- mapping or to reverse one), and they are a minority of the table.
CREATE INDEX IF NOT EXISTS convergence_events_project_id_original_idx
  ON sessions.convergence_events (project_id_original)
  WHERE project_id_original IS NOT NULL;
