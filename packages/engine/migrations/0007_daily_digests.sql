-- Daily digest narrative cache.
-- See docs/specs/2026-09-05-reflex-day-view.md §1.
--
-- The stats/projects/epics/sessions rollup is always recomputed from convergence_events
-- on every request; this table caches only the model-written narrative over it, keyed so
-- a narrative is regenerated when the rollup that produced it actually changed (its
-- stats_hash moved), not on a fixed TTL or on every page load.

CREATE TABLE IF NOT EXISTS sessions.daily_digests (
  org_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  -- The calendar day in `tz`, e.g. '2026-09-05' — not a UTC date, and not a timestamp:
  -- the row's identity is the civil day the caller asked about.
  day TEXT NOT NULL,
  tz TEXT NOT NULL,
  -- Exact project_id, or '' for the org-wide digest. Empty string stands in for "no
  -- project" so the primary key never has to treat NULL as a value.
  scope_key TEXT NOT NULL,
  stats_hash TEXT NOT NULL,
  narrative_text TEXT,
  narrative_model TEXT,
  narrative_generated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, day, tz, scope_key)
);

CREATE INDEX IF NOT EXISTS daily_digests_org_day_idx
  ON sessions.daily_digests(org_id, day);
