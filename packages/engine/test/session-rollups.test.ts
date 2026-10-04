/**
 * Migration 0030: per-session rollups maintained by triggers on convergence_events.
 * Everything runs the real migration SQL. GET /v1/sessions must return the same pages
 * from the rollups as from events, and must keep the event aggregate for requests the
 * rollups cannot answer. Rollup maintenance and the backfill are in rollouts.test.ts.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  migrationStatements,
  readMigrations,
  rolloutDeliveryProjection,
  type Migration,
} from "../src/migrate/index.js";
import type { HistoryDb } from "../src/db/database.js";
import type { AuthContext } from "../src/env.js";
import {
  encodeCursor,
  listSessions,
  listSessionsFromEvents,
  type EventFilters,
  type RecallScope,
} from "../src/lib/recall.js";
import {
  createFreshDatabase,
  type FreshDatabase,
} from "./support/fresh-database.js";

// The unmodified packaged files, pgvector included, on PGlite or real PostgreSQL.
const migrations: Migration[] = readMigrations();

const auth = {
  orgId: "org-a",
  workspaceId: "ws-1",
  userId: "user-1",
  tokenSubject: "user-1",
  scopes: ["rth:read", "rth:sync"],
  claims: {},
} as AuthContext;

let client: FreshDatabase;
let db: HistoryDb;

/** Migrations, then the 0029 rollout when the set includes it, as a deploy runs them. */
async function migrate(input: Migration[]) {
  await client.transaction(async (query) => {
    for (const statement of migrationStatements(input)) await query(statement);
  });
  if (input.some((m) => m.name.startsWith("0029_"))) {
    const outcome = await rolloutDeliveryProjection(
      async (sql) => (await client.query(sql)).rows,
    );
    if (!outcome.complete) throw new Error("delivery rollout did not complete");
  }
}

async function rows<T extends Record<string, any> = Record<string, any>>(
  sql: string,
  params?: unknown[],
) {
  return (await client.query<T>(sql, params)).rows;
}

interface Ev {
  org?: string;
  workspace?: string;
  machine?: string;
  user?: string;
  source?: string;
  session: string;
  id: string;
  kind?: string;
  ts: string;
  project?: string | null;
  taskRef?: Record<string, unknown>;
  title?: string | null;
  model?: string | null;
  input?: number;
  cacheCreate?: number;
  cache5m?: number | null;
  cache1h?: number | null;
  cost?: number | null;
}

const COLUMNS = `org_id, workspace_id, machine_id, user_id, source, session_id, event_id, kind,
  type, ts, project_id, task_ref, task_title, content, model, input_tokens, output_tokens,
  cache_create_tokens, cache_create_5m_tokens, cache_create_1h_tokens, cost_usd_micros, record`;

function values(e: Ev): unknown[] {
  return [
    e.org ?? "org-a",
    e.workspace ?? "ws-1",
    e.machine ?? "machine-1",
    e.user ?? "user-1",
    e.source ?? "claude",
    e.session,
    e.id,
    e.kind ?? "assistant",
    "message",
    e.ts,
    e.project === undefined ? "acme/repo" : e.project,
    JSON.stringify(e.taskRef ?? {}),
    e.title ?? null,
    `content ${e.id}`,
    e.model ?? null,
    e.input ?? 0,
    (e.input ?? 0) * 2,
    e.cacheCreate ?? 0,
    e.cache5m ?? null,
    e.cache1h ?? null,
    e.cost === undefined ? null : e.cost,
    "{}",
  ];
}

/** Inserts every event in ONE statement, so statement-level triggers see them together. */
async function insert(events: Ev[], conflict = "") {
  const width = 22;
  const placeholders = events
    .map(
      (_, row) =>
        `(${Array.from({ length: width }, (_, col) => `$${row * width + col + 1}`).join(", ")})`,
    )
    .join(", ");
  await client.query(
    `INSERT INTO sessions.convergence_events (${COLUMNS}) VALUES ${placeholders} ${conflict}`,
    events.flatMap(values),
  );
}

/**
 * An aggregate of the events written independently of the migration's own SQL,
 * compared with the stored rollups in both directions.
 */
async function expectRollupsMatchEvents() {
  const [diff] = await rows<Record<string, number>>(`
    WITH expected AS (
      SELECT org_id, session_id, workspace_id, source, project_id,
             min(ts) AS first_ts, max(ts) AS last_ts, count(*)::bigint AS event_count,
             array_agg(DISTINCT user_id) AS user_ids,
             array_agg(DISTINCT machine_id) AS machine_ids,
             array_agg(DISTINCT kind) AS kinds,
             array_remove(array_agg(DISTINCT model), NULL) AS models,
             (array_remove(array_agg(task_title ORDER BY ts DESC), NULL))[1] AS task_title,
             coalesce(jsonb_agg(DISTINCT task_ref) FILTER (WHERE task_ref <> '{}'::jsonb), '[]'::jsonb) AS task_refs,
             sum(cost_usd_micros)::bigint AS cost_usd_micros,
             sum(input_tokens)::bigint AS input_tokens,
             sum(output_tokens)::bigint AS output_tokens,
             sum(cache_create_tokens)::bigint AS cache_create_tokens,
             coalesce(sum(cache_create_5m_tokens), 0)::bigint AS cache_create_5m_tokens,
             coalesce(sum(cache_create_1h_tokens), 0)::bigint AS cache_create_1h_tokens,
             count(cost_usd_micros)::bigint AS cost_events,
             count(*) FILTER (WHERE NOT (cache_create_tokens = 0 OR (
               coalesce(cache_create_5m_tokens, 0) + coalesce(cache_create_1h_tokens, 0) = cache_create_tokens
               AND (cache_create_5m_tokens IS NOT NULL OR cache_create_1h_tokens IS NOT NULL))))::bigint AS cache_split_incomplete_events
        FROM sessions.convergence_events
       GROUP BY org_id, session_id, workspace_id, source, project_id
    ),
    stored AS (
      SELECT org_id, session_id, workspace_id, source, project_id, first_ts, last_ts,
             event_count, user_ids, machine_ids, kinds, models, task_title, task_refs,
             cost_usd_micros, input_tokens, output_tokens, cache_create_tokens,
             cache_create_5m_tokens, cache_create_1h_tokens, cost_events,
             cache_split_incomplete_events
        FROM sessions.session_rollups
    )
    SELECT (SELECT count(*)::int FROM expected) AS expected,
           (SELECT count(*)::int FROM (SELECT * FROM expected EXCEPT SELECT * FROM stored) AS d) AS missing,
           (SELECT count(*)::int FROM (SELECT * FROM stored EXCEPT SELECT * FROM expected) AS d) AS extra
  `);
  expect(diff).toMatchObject({ missing: 0, extra: 0 });
  return diff!.expected;
}

/** Walks every page of both read paths and requires identical pages and cursors. */
async function expectSamePages(
  filters: EventFilters,
  limit: number,
  scope: RecallScope = {},
) {
  let cursor: string | null = null;
  let seen = 0;
  for (let pageNumber = 0; pageNumber < 200; pageNumber++) {
    const fromRollups = await listSessions(
      db,
      auth,
      filters,
      { limit, cursor },
      scope,
    );
    const fromEvents = await listSessionsFromEvents(
      db,
      auth,
      filters,
      { limit, cursor },
      scope,
    );
    expect(fromRollups).toEqual(fromEvents);
    seen += fromRollups.sessions.length;
    cursor = fromRollups.nextCursor;
    if (!cursor) return seen;
  }
  throw new Error("pagination did not terminate");
}

beforeEach(async () => {
  client = await createFreshDatabase();
  db = client.db;
});
afterEach(async () => {
  await client.close();
});

describe("session rollup cursors", () => {
  beforeEach(async () => {
    await migrate(migrations);
  });

  it("pages sessions whose last activity differs only in microseconds", async () => {
    await insert([
      { session: "a", id: "a1", ts: "2026-09-01T10:00:00.123456Z" },
      { session: "b", id: "b1", ts: "2026-09-01T10:00:00.123100Z" },
    ]);
    // Marks every page as served from the rollups rather than the event aggregate.
    await client.query("UPDATE sessions.session_rollups SET event_count = 999");

    const seen: string[] = [];
    let cursor: string | null = null;
    for (let pageNumber = 0; pageNumber < 5; pageNumber += 1) {
      const page = await listSessions(db, auth, {}, { limit: 1, cursor });
      for (const session of page.sessions) {
        expect(session.eventCount).toBe(999);
        seen.push(session.sessionId);
      }
      cursor = page.nextCursor;
      if (!cursor) break;
    }

    expect(seen).toEqual(["a", "b"]);
  });
});

describe("session list pagination parity", () => {
  beforeEach(async () => {
    await migrate(migrations);
    const events: Ev[] = [];
    const base = Date.parse("2026-09-01T00:00:00Z");
    for (let s = 0; s < 36; s++) {
      const session = `s${String(s % 30).padStart(2, "0")}`;
      // Sessions 30-35 reuse ids 00-05 under another source.
      const source = s < 30 ? (s % 4 === 0 ? "codex" : "claude") : "opencode";
      const count = 1 + (s % 5);
      for (let e = 0; e < count; e++) {
        // Several sessions share their newest timestamp to exercise tie-breaking.
        const minute = s % 6 === 0 ? 1000 : s * 10 + e;
        events.push({
          session,
          source,
          id: `${s}-${e}`,
          ts: new Date(
            base + minute * 60_000 - (count - 1 - e) * 1000,
          ).toISOString(),
          // Spread some sessions over a second workspace and project.
          workspace: s % 7 === 0 && e === 0 ? "ws-2" : "ws-1",
          project:
            s % 3 === 0
              ? null
              : s % 5 === 0 && e === 0
                ? "acme/other"
                : `acme/repo-${s % 2}`,
          title: e === count - 1 && s % 2 === 0 ? `Title ${s}` : null,
          taskRef:
            s % 4 === 1 ? { system: "github", id: `acme/repo#${s}` } : {},
          model: e % 2 ? "claude-opus" : null,
          machine: `machine-${(s + e) % 3}`,
          user: `user-${s % 2}`,
          input: s + e,
          cacheCreate: s % 5 === 2 ? 6 : 0,
          cache5m: s % 5 === 2 && e % 2 === 0 ? 6 : null,
          cost: s % 4 === 3 ? null : s * 10 + e,
        });
      }
    }
    await insert(events);
    await insert([
      {
        org: "org-b",
        session: "s00",
        id: "foreign",
        ts: "2026-09-03T00:00:00Z",
      },
    ]);
    await expectRollupsMatchEvents();
  });

  it.each([1, 4, 7, 50])(
    "returns identical pages organization-wide with limit %i",
    async (limit) => {
      expect(await expectSamePages({}, limit)).toBe(36);
    },
  );

  it.each<[string, EventFilters, RecallScope]>([
    ["a workspace scope", {}, { workspaceId: "ws-2" }],
    [
      "a workspace scope with a project",
      { projectId: "acme/repo-1" },
      { workspaceId: "ws-1" },
    ],
    ["a source", { source: "opencode" }, {}],
    ["a project", { projectId: "acme/other" }, {}],
    ["the null project", { noProject: true }, {}],
    ["a project substring", { projectContains: "repo-" }, {}],
    ["a session id", { sessionId: "s03" }, {}],
  ])("returns identical pages under %s", async (_label, filters, scope) => {
    for (const limit of [1, 3, 50])
      expect(await expectSamePages(filters, limit, scope)).toBeGreaterThan(0);
  });

  it("accepts a legacy cursor without a source", async () => {
    const cursor = encodeCursor("2026-09-01T03:00:00.000Z", "s15");
    expect(await listSessions(db, auth, {}, { limit: 5, cursor })).toEqual(
      await listSessionsFromEvents(db, auth, {}, { limit: 5, cursor }),
    );
  });

  it("keeps the event aggregate for filters over event fields", async () => {
    // Corrupt the rollups: a request the rollups cannot answer must not read them.
    await client.query("UPDATE sessions.session_rollups SET event_count = 999");
    const page = await listSessions(db, auth, { kind: "assistant" });
    expect(page.sessions.length).toBeGreaterThan(0);
    expect(page.sessions.every((session) => session.eventCount < 999)).toBe(
      true,
    );
  });
});
