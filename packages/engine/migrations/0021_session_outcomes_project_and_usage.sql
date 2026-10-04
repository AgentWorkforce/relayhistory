-- Stop dropping fields the legacy `/v1/ingest` path already receives.
--
-- session_outcomes.project_id / files_touched
--   The uploader's `session_outcome` envelope carries the session's `projectId` and
--   `filesTouched` (WS-1). The table had no column for either, so both were discarded
--   on every push. Existing rows cannot be backfilled: the values were never stored.
--   project_id stays NULL for them; files_touched reads as an empty array.
--
-- session_outcomes key and match_method
--   The key stays (org_id, workspace_id, source, session_id, commit_sha): one row per
--   session and commit, which is what session-links, digest and reflex learn count.
--   The uploader may report the same commit under several match methods; ingest now
--   keeps the highest-confidence method rather than the last one written (see
--   `upsertSessionOutcome`). No DDL is needed for that.
--
-- convergence_events.cache_create_5m_tokens / cache_create_1h_tokens
--   Anthropic prices 5-minute and 1-hour cache writes differently. Ingest summed both
--   into cache_create_tokens and discarded the split. The summed column is unchanged
--   for compatibility; the split is NULL when the client did not report it.
--
-- convergence_events.cost_usd_micros
--   Was NOT NULL DEFAULT 0, so "no cost reported" and "cost was zero" were the same
--   value and recall reported $0 for sessions whose cost was never sent. Ingest now
--   writes NULL when no cost was sent. Existing rows keep their stored value: a
--   historical 0 cannot be told apart from a real zero, so none are rewritten.
--
-- Every statement is a catalog-only change on Postgres 11+ (nullable columns, a
-- constant default, dropping NOT NULL), so none rewrites the table.
ALTER TABLE sessions.session_outcomes
  ADD COLUMN IF NOT EXISTS project_id TEXT;

ALTER TABLE sessions.session_outcomes
  ADD COLUMN IF NOT EXISTS files_touched JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE sessions.convergence_events
  ADD COLUMN IF NOT EXISTS cache_create_5m_tokens BIGINT;

ALTER TABLE sessions.convergence_events
  ADD COLUMN IF NOT EXISTS cache_create_1h_tokens BIGINT;

ALTER TABLE sessions.convergence_events
  ALTER COLUMN cost_usd_micros DROP NOT NULL;
