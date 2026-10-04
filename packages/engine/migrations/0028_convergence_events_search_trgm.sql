-- Trigram search support for event search (`GET /v1/events?q=`, `GET /v1/sessions?q=`).
--
-- buildWhere matches `q` as `content ILIKE '%q%' OR task_title ILIKE '%q%'`. No
-- btree can serve a leading-wildcard ILIKE, so a term that matches few rows made
-- PostgreSQL walk every row of convergence_events (all organizations) and detoast
-- its content: about 0.8 s per search at 1M rows on local PostgreSQL 18.
--
-- This migration only installs pg_trgm (trusted; Neon and the CI pgvector image
-- ship it). The GIN indexes themselves are CONCURRENT_INDEXES in
-- scripts/apply-neon-migration.mjs: they are built with CREATE INDEX CONCURRENTLY
-- after this transaction commits, so live delivery is never blocked by the build,
-- and an INVALID index left by an interrupted build is dropped and rebuilt on the
-- next run. The guard only skips in-process test databases that do not ship it.
DO $search_trgm$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_available_extensions WHERE name = 'pg_trgm') THEN
    CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;
  END IF;
END
$search_trgm$;
