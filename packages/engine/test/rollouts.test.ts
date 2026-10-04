/**
 * The online rollouts that complete the schema after migrations: 0029's delivery
 * projection (catalog and activity v2) and 0030's session rollups. Every test runs the
 * packaged migration SQL and the engine's own rollout functions.
 *
 * Delivery projection: migrations are schema only. They must not switch a projection on,
 * build an index over a table that grows with stored history, or backfill it, because
 * all of that held the delivery_records write lock inside the deploy transaction for
 * hours on 2026-10-02/03. The rollout does it afterwards in short, resumable batches,
 * and must reach the result the in-transaction migrations reached while uploads keep
 * arriving.
 */
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  MIGRATIONS_DIR,
  SESSION_ROLLUPS_MIGRATION,
  SUPERSEDED_CHECKSUMS,
  migrationStatements,
  readMigrations,
  rolloutDeliveryProjection,
  rolloutSessionRollups,
  type Migration,
} from "../src/migrate/index.js";
import type { HistoryDb } from "../src/db/database.js";
import type { AuthContext } from "../src/env.js";
import { acceptDelivery, deliveryAccount } from "../src/lib/delivery.js";
import type { HistoryExportRecord } from "../src/lib/delivery-contracts.js";
import { applyIngest } from "../src/lib/ingest.js";
import { listSessions, listSessionsFromEvents } from "../src/lib/recall.js";
import {
  createFreshDatabase,
  type FreshDatabase,
} from "./support/fresh-database.js";

// The unmodified packaged files, pgvector included, on PGlite or real PostgreSQL.
const migrations: Migration[] = readMigrations();
const beforeRollups = migrations.filter(
  (m) => m.name < SESSION_ROLLUPS_MIGRATION,
);

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

/** Runs the 0029 rollout to completion, as a deploy does after its migrations. */
async function rollout(database: FreshDatabase, batch?: number) {
  const outcome = await rolloutDeliveryProjection(
    async (sql) => (await database.query(sql)).rows,
    batch ? { batch } : {},
  );
  if (!outcome.complete) throw new Error("delivery rollout did not complete");
}

async function migrate(filter = (_name: string) => true) {
  const input = migrations.filter((m) => filter(m.name));
  await client.transaction(async (query) => {
    for (const statement of migrationStatements(input)) await query(statement);
  });
}

/** Migrations, then the 0029 rollout when the set includes it. */
async function migrateSet(input: Migration[]) {
  await client.transaction(async (query) => {
    for (const statement of migrationStatements(input)) await query(statement);
  });
  if (input.some((m) => m.name.startsWith("0029_"))) await rollout(client);
}

async function rows<T extends Record<string, any> = Record<string, any>>(
  sql: string,
  params?: unknown[],
) {
  return (await client.query<T>(sql, params)).rows;
}

interface Put {
  id: string;
  kind?: string;
  payload?: Record<string, unknown> | null;
  origin?: string;
  session?: string;
  revision?: number;
  receivedAt?: string;
}
/** The upsert accept_delivery_batch performs, with an explicit delivery time. */
async function put({
  id,
  kind = "session_event",
  payload = { role: "user", text: `text ${id}` },
  origin = "origin-a",
  session = "s",
  revision = 1,
  receivedAt = "2026-09-01T00:00:00Z",
}: Put) {
  await client.query(
    `INSERT INTO sessions.delivery_records
      (org_id, workspace_id, origin_id, record_id, revision_id, revision, digest, kind, source, session_id, operation, payload, user_id, received_at)
     VALUES ('org', 'w', $1, $2, $1 || ':' || $2 || ':' || $3::text, $3::bigint, 'digest', $4, 'claude', $5, $6, $7::jsonb, 'u', $8::timestamptz)
     ON CONFLICT (org_id, workspace_id, origin_id, record_id) DO UPDATE SET
       revision = excluded.revision, kind = excluded.kind, session_id = excluded.session_id,
       operation = excluded.operation, payload = excluded.payload, received_at = excluded.received_at
     WHERE excluded.revision > sessions.delivery_records.revision`,
    [
      origin,
      id,
      revision,
      kind,
      session,
      payload ? "upsert" : "delete",
      payload ? JSON.stringify(payload) : null,
      receivedAt,
    ],
  );
}
const session = (branch: string) => ({
  session_id: "s",
  git_branch: branch,
});
async function activity() {
  return rows<{ id: string | null; machine: string; content: string }>(
    `SELECT delivery_record_id AS id, machine_id AS machine, content
       FROM sessions.convergence_events ORDER BY content`,
  );
}
async function catalog() {
  return rows<{ origin_id: string; git_branch: string }>(
    "SELECT origin_id, git_branch FROM sessions.session_catalog",
  );
}
async function triggerFunctions() {
  return rows<{ name: string; fn: string }>(
    `SELECT t.tgname AS name, p.proname AS fn FROM pg_trigger AS t
       JOIN pg_proc AS p ON p.oid = t.tgfoid
      WHERE t.tgrelid = 'sessions.delivery_records'::regclass AND NOT t.tgisinternal
      ORDER BY t.tgname`,
  );
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

async function rollupOf(session: string, extra = "true") {
  return rows(
    `SELECT ctid::text AS ctid, event_count::int AS events, task_title, last_ts
       FROM sessions.session_rollups WHERE session_id = $1 AND ${extra}`,
    [session],
  );
}

beforeEach(async () => {
  client = await createFreshDatabase();
  db = client.db;
});
afterEach(async () => {
  await client.close();
});

describe("migrations are schema only", () => {
  it("leave both projections off and build no index over retained history", async () => {
    await migrate();
    expect(await triggerFunctions()).toEqual([
      // 0011's trigger still runs the 0014 projection; no catalog triggers yet.
      { name: "delivery_session_projection", fn: "project_delivery_session" },
    ]);
    const indexes = (
      await rows<{ name: string }>(
        "SELECT index_name AS name FROM sessions.delivery_rollout_indexes()",
      )
    ).map((row) => row.name);
    expect(
      await rows(
        `SELECT relname FROM pg_class WHERE relkind = 'i' AND relname = ANY (ARRAY['${indexes.join("','")}'])`,
      ),
    ).toEqual([]);
    // A live upload still projects through the pre-0023 path meanwhile.
    await put({ id: "live" });
    expect(await activity()).toEqual([
      expect.objectContaining({ id: null, content: "text live" }),
    ]);
  });

  it("refuses to switch a projection on before its indexes are valid", async () => {
    await migrate();
    await expect(
      client.query("SELECT sessions.activate_delivery_catalog()"),
    ).rejects.toThrow(/delivery_rollout_index_missing/);
    await expect(
      client.query("SELECT sessions.activate_delivery_projection_v2()"),
    ).rejects.toThrow(/delivery_rollout_index_missing/);
    await expect(
      client.query("SELECT sessions.delivery_catalog_backfill_step(10)"),
    ).rejects.toThrow(/delivery_rollout_not_activated/);
  });

  it("never build an index on a table that grows with stored history", () => {
    // Applied migrations before 0020 are history; every later one must leave these
    // to the rollout (CREATE INDEX CONCURRENTLY), which does not block writes.
    const large =
      /CREATE\s+(UNIQUE\s+)?INDEX\b[^;]*?\bON\s+sessions\.(delivery_records|convergence_events|delivery_receipts)\b/i;
    const dir = MIGRATIONS_DIR;
    const offenders = readdirSync(dir)
      .filter((name) => name.endsWith(".sql") && name >= "0020")
      .filter((name) =>
        readFileSync(join(dir, name), "utf8")
          .split("\n")
          .filter((line) => !line.trimStart().startsWith("--"))
          .join("\n")
          .match(large),
      );
    expect(offenders).toEqual([]);
  });
});

describe("rollout", () => {
  async function retained() {
    await migrate((name) => name < "0022");
    // Two copies of one record; the newer one is stored first.
    await put({
      id: "r1",
      origin: "origin-b",
      payload: { role: "user", text: "newer copy" },
      receivedAt: "2026-09-02T00:00:00Z",
    });
    await put({ id: "r1", payload: { role: "user", text: "older copy" } });
    await put({ id: "r2" });
    await put({
      id: "sess",
      kind: "session",
      origin: "origin-a",
      payload: session("older"),
    });
    await put({
      id: "sess",
      kind: "session",
      origin: "origin-b",
      payload: session("newer"),
      receivedAt: "2026-09-02T00:00:00Z",
    });
    await migrate();
  }

  it("reaches the in-transaction result in batches of one, and resumes", async () => {
    await retained();
    await rollout(client, 1);
    expect(await triggerFunctions()).toEqual([
      {
        name: "delivery_session_catalog_delete",
        fn: "project_delivery_catalog",
      },
      {
        name: "delivery_session_catalog_insert",
        fn: "project_delivery_catalog",
      },
      {
        name: "delivery_session_catalog_update",
        fn: "project_delivery_catalog",
      },
      {
        name: "delivery_session_projection",
        fn: "project_delivery_session_v2",
      },
    ]);
    expect((await activity()).map((row) => [row.id, row.content])).toEqual([
      ["r1", "newer copy"],
      ["r2", "text r2"],
    ]);
    expect(await catalog()).toEqual([
      { origin_id: "origin-b", git_branch: "newer" },
    ]);
    // Rerunning is a no-op: completed stages keep their state.
    const before = await rows(
      "SELECT stage, cursor, completed_at IS NOT NULL AS done FROM sessions.delivery_rollout ORDER BY stage",
    );
    expect(before.every((row) => row.done)).toBe(true);
    await rollout(client);
    expect(
      await rows(
        "SELECT stage, cursor, completed_at IS NOT NULL AS done FROM sessions.delivery_rollout ORDER BY stage",
      ),
    ).toEqual(before);
  });

  it("keeps an upload that lands after activation instead of replaying an older copy over it", async () => {
    await retained();
    // Switch v2 on without reprojecting yet, as a deploy would between batches.
    for (const { index_name, definition } of await rows<{
      index_name: string;
      definition: string;
    }>(
      "SELECT index_name, definition FROM sessions.delivery_rollout_indexes()",
    ))
      await client.query(`CREATE INDEX ${index_name} ${definition}`);
    await client.query("SELECT sessions.activate_delivery_projection_v2()");
    // A live upload of r1 from origin-a: it is now the newest write of r1, even
    // though origin-b's retained copy carries the later received_at.
    await put({
      id: "r1",
      revision: 2,
      payload: { role: "user", text: "live rewrite" },
      receivedAt: "2026-09-01T12:00:00Z",
    });
    await rollout(client, 1);
    expect((await activity()).map((row) => [row.id, row.content])).toEqual([
      ["r1", "live rewrite"],
      ["r2", "text r2"],
    ]);
  });

  it("ranks a catalog upload after activation above every retained contender", async () => {
    await retained();
    await client.query(
      `CREATE INDEX delivery_records_catalog_key ${
        (
          await rows<{ definition: string }>(
            "SELECT definition FROM sessions.delivery_rollout_indexes() WHERE index_name = 'delivery_records_catalog_key'",
          )
        )[0]!.definition
      }`,
    );
    await client.query("SELECT sessions.activate_delivery_catalog()");
    // origin-a writes again; its received_at is still older than origin-b's copy.
    await put({
      id: "sess",
      kind: "session",
      origin: "origin-a",
      revision: 2,
      payload: session("live"),
      receivedAt: "2026-09-01T12:00:00Z",
    });
    await rollout(client, 1);
    expect(await catalog()).toEqual([
      { origin_id: "origin-a", git_branch: "live" },
    ]);
    // When the shown contender later leaves, the key is re-ranked by acceptance:
    // origin-a's live write passed the lock after origin-b's copy was retained, so
    // it must still outrank it, whatever their received_at.
    await put({
      id: "sess",
      kind: "session",
      origin: "origin-c",
      payload: session("newest"),
    });
    expect(await catalog()).toEqual([
      { origin_id: "origin-c", git_branch: "newest" },
    ]);
    await put({
      id: "sess",
      kind: "session",
      origin: "origin-c",
      revision: 2,
      payload: null,
    });
    expect(await catalog()).toEqual([
      { origin_id: "origin-a", git_branch: "live" },
    ]);
  });
  it("ranks retained contenders by acceptance even after a live removal showed one early", async () => {
    await migrate((name) => name < "0022");
    // origin-z's copy is older but sorts first by origin_id; origin-a's is newer.
    await put({
      id: "sess",
      kind: "session",
      origin: "origin-z",
      payload: session("older"),
    });
    await put({
      id: "sess",
      kind: "session",
      origin: "origin-a",
      payload: session("newer"),
      receivedAt: "2026-09-02T00:00:00Z",
    });
    await put({
      id: "sess",
      kind: "session",
      origin: "origin-m",
      payload: session("leaving"),
    });
    await migrate();
    await client.query(
      `CREATE INDEX delivery_records_catalog_key ${
        (
          await rows<{ definition: string }>(
            "SELECT definition FROM sessions.delivery_rollout_indexes() WHERE index_name = 'delivery_records_catalog_key'",
          )
        )[0]!.definition
      }`,
    );
    await client.query("SELECT sessions.activate_delivery_catalog()");
    // A live tombstone before the backfill reaches the key: the trigger re-ranks
    // contenders that have no acceptance yet, and shows one by origin_id.
    await put({
      id: "sess",
      kind: "session",
      origin: "origin-m",
      revision: 2,
      payload: null,
    });
    await rollout(client, 1);
    expect(await catalog()).toEqual([
      { origin_id: "origin-a", git_branch: "newer" },
    ]);
  });
});

describe("rollout script", () => {
  it("retries a batch that yielded to an upload's row lock", async () => {
    const calls: string[] = [];
    let busy = 2;
    const query = async (sql: string) => {
      calls.push(sql);
      if (sql.includes("pg_try_advisory_lock(")) return [{ locked: true }];
      if (sql.includes("delivery_rollout_indexes()")) return [];
      if (sql.includes("activate_")) return [{ activated: false }];
      if (sql.includes("_step(")) {
        if (busy > 0) {
          busy -= 1;
          throw Object.assign(new Error("lock"), { code: "55P03" });
        }
        return [{ processed: 0 }];
      }
      return [];
    };
    await expect(
      rolloutDeliveryProjection(query, { batch: 5 }),
    ).resolves.toEqual({
      complete: true,
    });
    expect(calls.filter((sql) => sql === "ROLLBACK")).toHaveLength(2);
    await expect(
      rolloutDeliveryProjection(
        async (sql: string) => {
          if (sql.includes("pg_try_advisory_lock(")) return [{ locked: true }];
          if (sql.includes("_step("))
            throw Object.assign(new Error("x"), { code: "57014" });
          return sql.includes("activate_") ? [{ activated: false }] : [];
        },
        { batch: 5 },
      ),
    ).rejects.toThrow("x");
  });
});

describe("databases that applied the earlier 0022/0023", () => {
  it("accepts their checksums and still rejects any other change", async () => {
    await migrate();
    const name = "0022_delivery_session_catalog.sql";
    await client.query(
      `UPDATE sessions.__migrations SET checksum = '${SUPERSEDED_CHECKSUMS[name][0]}' WHERE name = '${name}'`,
    );
    await migrate();
    await client.query(
      `UPDATE sessions.__migrations SET checksum = 'edited' WHERE name = '${name}'`,
    );
    await expect(migrate()).rejects.toThrow(/checksum changed/);
  });

  it("marks both stages complete where the projections are already live", async () => {
    await migrate();
    // What an earlier 0022/0023 left behind: catalog triggers, no current catalog
    // key helper, and v2 in place. Compatibility detection must happen before the
    // rollout attempts to build the current expression index.
    await client.query(
      `CREATE TRIGGER delivery_session_catalog_insert AFTER INSERT ON sessions.delivery_records
         FOR EACH ROW WHEN (NEW.kind = 'session') EXECUTE FUNCTION sessions.project_delivery_catalog()`,
    );
    await client.query(
      `ALTER FUNCTION sessions.delivery_catalog_key(text, text, text, text, jsonb)
         RENAME TO legacy_delivery_catalog_key`,
    );
    await client.query("DROP FUNCTION sessions.project_delivery_session_v2()");
    await rollout(client);
    expect(
      await rows(
        "SELECT stage, completed_at IS NOT NULL AS done FROM sessions.delivery_rollout ORDER BY stage",
      ),
    ).toEqual([
      { stage: "activity", done: true },
      { stage: "catalog", done: true },
    ]);
    expect(await triggerFunctions()).toContainEqual({
      name: "delivery_session_projection",
      fn: "project_delivery_session",
    });
    expect(
      await rows(
        "SELECT 1 FROM pg_class WHERE relname = 'delivery_records_catalog_key'",
      ),
    ).toEqual([]);
  });
});

/*
 * Migration 0030: per-session rollups maintained by triggers on convergence_events.
 * Everything runs the real migration SQL. The invariant under test is that the rollup
 * rows always equal a fresh aggregate of the events, whichever writer changed them,
 * and that GET /v1/sessions returns the same pages from the rollups as from events.
 */
describe("session rollup maintenance", () => {
  beforeEach(async () => {
    await migrateSet(migrations);
  });

  it("starts complete on a database without events", async () => {
    const [state] = await rows(
      "SELECT completed_at IS NOT NULL AS complete FROM sessions.session_rollup_rollout",
    );
    expect(state).toEqual({ complete: true });
  });

  it("merges inserted rows into the stored totals, across and within statements", async () => {
    await insert([
      {
        session: "s1",
        id: "e1",
        ts: "2026-09-01T10:00:00Z",
        input: 10,
        cost: 5,
      },
      {
        session: "s1",
        id: "e2",
        ts: "2026-09-01T10:01:00Z",
        title: "Old title",
        model: "claude-opus",
        machine: "machine-2",
      },
      {
        session: "s1",
        id: "e3",
        ts: "2026-09-01T10:02:00Z",
        workspace: "ws-2",
        project: null,
      },
      { org: "org-b", session: "s1", id: "e1", ts: "2026-09-01T10:00:00Z" },
    ]);
    await insert([
      {
        session: "s1",
        id: "e4",
        ts: "2026-09-01T09:00:00Z",
        title: "Earlier title",
        taskRef: { system: "github", id: "acme/repo#1" },
        kind: "tool_call",
        user: "user-2",
        input: 3,
        cacheCreate: 8,
        cache5m: 8,
      },
    ]);
    await insert([
      {
        session: "s1",
        id: "e5",
        ts: "2026-09-01T10:05:00Z",
        title: "New title",
        cost: 7,
        cacheCreate: 4,
      },
    ]);
    // A late-arriving event with an older title does not displace the newest one.
    await insert([
      { session: "s1", id: "e6", ts: "2026-09-01T08:00:00Z", title: "Ancient" },
    ]);

    expect(await expectRollupsMatchEvents()).toBe(3);
    const [main] = await rows(
      `SELECT event_count::int AS events, first_ts, last_ts, user_ids, machine_ids, kinds, models,
              task_title, task_refs, cost_usd_micros::int AS cost, input_tokens::int AS input,
              cache_split_incomplete_events::int AS incomplete
         FROM sessions.session_rollups
        WHERE org_id = 'org-a' AND workspace_id = 'ws-1' AND project_id = 'acme/repo'`,
    );
    expect(main).toMatchObject({
      events: 5,
      user_ids: ["user-1", "user-2"],
      machine_ids: ["machine-1", "machine-2"],
      kinds: ["assistant", "tool_call"],
      models: ["claude-opus"],
      task_title: "New title",
      task_refs: [{ system: "github", id: "acme/repo#1" }],
      cost: 12,
      input: 13,
      // e5 wrote cache without reporting a split.
      incomplete: 1,
    });
    expect(new Date(main!.first_ts).toISOString()).toBe(
      "2026-09-01T08:00:00.000Z",
    );
    expect(new Date(main!.last_ts).toISOString()).toBe(
      "2026-09-01T10:05:00.000Z",
    );
  });

  it("ignores duplicate and identical replays and rebuilds on a changed one", async () => {
    const batch = [
      { session: "s1", id: "e1", ts: "2026-09-01T10:00:00Z", input: 10 },
      { session: "s1", id: "e2", ts: "2026-09-01T10:01:00Z", title: "Title" },
    ];
    await insert(batch);
    const [before] = await rollupOf("s1");

    await insert(batch, "ON CONFLICT DO NOTHING");
    // The legacy ingest replay: DO UPDATE with the values already stored.
    await insert(
      batch,
      `ON CONFLICT (org_id, machine_id, source, session_id, kind, event_id)
       DO UPDATE SET input_tokens = excluded.input_tokens, task_title = excluded.task_title,
                     content = excluded.content`,
    );
    const [replayed] = await rollupOf("s1");
    expect(replayed).toEqual(before);

    await insert(
      [{ ...batch[1]!, title: "Renamed", input: 4 }],
      `ON CONFLICT (org_id, machine_id, source, session_id, kind, event_id)
       DO UPDATE SET input_tokens = excluded.input_tokens, task_title = excluded.task_title`,
    );
    await expectRollupsMatchEvents();
    expect((await rollupOf("s1"))[0]).toMatchObject({
      events: 2,
      task_title: "Renamed",
    });
  });

  it("keeps the rollups through repeated legacy ingests", async () => {
    const body = (title: string) => ({
      machine: { id: "machine-1", hostname: "devbox" },
      batchId: `batch-${title}`,
      cursors: { trajectories: 1 },
      records: ["e1", "e2", "e3"].map((eventId, index) => ({
        v: 1,
        source: "trajectories",
        lens: "trajectories",
        machineId: "machine-1",
        sessionId: "legacy-session",
        eventId,
        kind: "decision",
        type: "decision",
        ts: `2026-09-01T10:00:0${index}.000Z`,
        content: `decision ${eventId}`,
        projectId: "acme/repo",
        taskTitle: title,
        usage: { input: 5, output: 1 },
        costUsdMicros: 20,
      })),
    });
    await applyIngest(db, auth, body("First"));
    await applyIngest(db, auth, body("First"));
    expect((await rollupOf("legacy-session"))[0]).toMatchObject({
      events: 3,
      task_title: "First",
    });
    await applyIngest(db, auth, body("Second"));
    await expectRollupsMatchEvents();
    expect((await rollupOf("legacy-session"))[0]).toMatchObject({
      events: 3,
      task_title: "Second",
    });
  });

  it("leaves the rollups untouched by embedding writes", async () => {
    await insert([{ session: "s1", id: "e1", ts: "2026-09-01T10:00:00Z" }]);
    const [before] = await rollupOf("s1");
    await client.query(
      `UPDATE sessions.convergence_events
          SET embedding_skip_reason = 'provider_unavailable', content_hash = 'h', generated_at = now()`,
    );
    // Same physical row: nothing rebuilt it.
    expect((await rollupOf("s1"))[0]).toEqual(before);
  });

  it("rebuilds both sessions when an update moves rows between them", async () => {
    await insert([
      { session: "s1", id: "e1", ts: "2026-09-01T10:00:00Z", title: "One" },
      { session: "s1", id: "e2", ts: "2026-09-01T10:01:00Z", title: "Two" },
      { session: "s2", id: "e3", ts: "2026-09-01T09:00:00Z" },
    ]);
    await client.query(
      "UPDATE sessions.convergence_events SET session_id = 's2' WHERE event_id = 'e2'",
    );
    await expectRollupsMatchEvents();
    expect((await rollupOf("s1"))[0]).toMatchObject({
      events: 1,
      task_title: "One",
    });
    expect((await rollupOf("s2"))[0]).toMatchObject({
      events: 2,
      task_title: "Two",
    });

    // A settled timestamp is a rolled-up column too.
    await client.query(
      "UPDATE sessions.convergence_events SET ts = '2026-09-02T00:00:00Z' WHERE event_id = 'e3'",
    );
    await expectRollupsMatchEvents();
  });

  it("applies a usage settle as a difference, without a rebuild", async () => {
    await insert([
      { session: "s1", id: "e1", ts: "2026-09-01T10:00:00Z", cost: null },
      { session: "s1", id: "e2", ts: "2026-09-01T10:01:00Z", cost: 4 },
    ]);
    const settle = async (sql: string) =>
      client.transaction(async (query) => {
        const tx = { query };
        await tx.query(sql);
        const [marked] = (
          await tx.query<{ n: number }>(
            "SELECT count(*)::int AS n FROM sessions.session_rollup_dirty",
          )
        ).rows;
        expect(marked!.n).toBe(0);
      });
    // The delivery projection's settle: usage arrives on a row inserted without it.
    await settle(
      `UPDATE sessions.convergence_events
          SET input_tokens = 30, output_tokens = 7, cost_usd_micros = 11,
              cache_create_tokens = 9, cache_create_5m_tokens = 9
        WHERE event_id = 'e1'`,
    );
    await expectRollupsMatchEvents();
    // A split that stops covering the write, and a cost withdrawn from every row.
    await settle(
      `UPDATE sessions.convergence_events
          SET cache_create_5m_tokens = NULL, cost_usd_micros = NULL`,
    );
    await expectRollupsMatchEvents();
    const [rollup] = await rows(
      `SELECT cost_usd_micros, cost_events::int AS cost_events,
              cache_split_incomplete_events::int AS incomplete
         FROM sessions.session_rollups`,
    );
    expect(rollup).toEqual({
      cost_usd_micros: null,
      cost_events: 0,
      incomplete: 1,
    });
  });

  it("rebuilds a session changed many times in one transaction once, at commit", async () => {
    await insert([
      { session: "s1", id: "e0", ts: "2026-09-01T10:00:00Z", title: "Start" },
    ]);
    await client.transaction(async (query) => {
      const tx = { query };
      for (let step = 1; step <= 5; step++) {
        await tx.query(
          `INSERT INTO sessions.convergence_events (${COLUMNS})
           VALUES (${Array.from({ length: 22 }, (_, i) => `$${i + 1}`).join(", ")})`,
          values({
            session: "s1",
            id: `e${step}`,
            ts: "2026-09-09T00:00:00Z",
            input: step,
          }),
        );
        // The delivery projection settles a new row's time after inserting it.
        await tx.query(
          `UPDATE sessions.convergence_events SET ts = $1 WHERE event_id = $2`,
          [`2026-09-01T10:0${step}:00Z`, `e${step}`],
        );
      }
      await tx.query(
        "DELETE FROM sessions.convergence_events WHERE event_id = 'e3'",
      );
      // Rebuilds wait for commit; the transaction sees one pending mark.
      const [marked] = (
        await tx.query<{ n: number }>(
          "SELECT count(*)::int AS n FROM sessions.session_rollup_dirty",
        )
      ).rows;
      expect(marked!.n).toBe(1);
    });
    await expectRollupsMatchEvents();
    expect(await rows("SELECT * FROM sessions.session_rollup_dirty")).toEqual(
      [],
    );
    const [rollup] = await rollupOf("s1");
    expect(rollup).toMatchObject({ events: 5, task_title: "Start" });
    expect(new Date(rollup!.last_ts).toISOString()).toBe(
      "2026-09-01T10:05:00.000Z",
    );
  });

  it("rebuilds sessions on delete and drops a session with no events left", async () => {
    await insert([
      { session: "s1", id: "e1", ts: "2026-09-01T10:00:00Z", title: "Kept" },
      { session: "s1", id: "e2", ts: "2026-09-01T10:01:00Z", title: "Gone" },
      { session: "s2", id: "e3", ts: "2026-09-01T10:02:00Z" },
      { org: "org-b", session: "s3", id: "e4", ts: "2026-09-01T10:03:00Z" },
    ]);
    await client.query(
      "DELETE FROM sessions.convergence_events WHERE event_id = 'e2'",
    );
    expect((await rollupOf("s1"))[0]).toMatchObject({
      events: 1,
      task_title: "Kept",
    });
    await client.query(
      "DELETE FROM sessions.convergence_events WHERE session_id = 's2'",
    );
    expect(await rollupOf("s2")).toEqual([]);
    await client.query(
      "DELETE FROM sessions.convergence_events WHERE org_id = 'org-a'",
    );
    expect(await expectRollupsMatchEvents()).toBe(1);

    await client.query("TRUNCATE sessions.convergence_events");
    expect(await rows("SELECT * FROM sessions.session_rollups")).toEqual([]);
  });

  it("follows the delivery projection's inserts, settled times and tombstones", async () => {
    const entry = (
      kind: HistoryExportRecord["kind"],
      id: string,
      payload: Record<string, unknown> | null,
      revision = 1,
    ): HistoryExportRecord => ({
      schema_version: 1,
      origin_id: "origin-a",
      record_id: `d:${id}`,
      revision_id: `d:${id}:${revision}`,
      revision,
      kind,
      source: "claude",
      session_id: "delivered",
      operation: payload ? "upsert" : "delete",
      payload,
    });
    const usage = JSON.stringify({
      input_tokens: 3,
      output_tokens: 43,
      cache_creation_input_tokens: 10,
      cache_creation: { ephemeral_5m_input_tokens: 10 },
    });
    const records = [
      entry("history", "prompt", {
        source: "claude",
        session_id: "delivered",
        prompt: "deliver this",
        timestamp_ms: 1_000_000,
      }),
      ...[0, 1, 2].map((request) =>
        entry("session_event", `r${request}`, {
          role: "assistant",
          kind: "text",
          text: `answer ${request}`,
          message_id: `uuid-${request}`,
          request_id: `req_${request}`,
          provider_message_id: `msg_${request}`,
          provider: "anthropic",
          model: "claude-opus",
          token_json: usage,
          tool_use_id: `toolu_${request}`,
          ts_ms: 1_000_000 + request * 1_000,
        }),
      ),
      // Tool calls carry no time of their own; the projection settles them onto
      // their parent event's, which is an update after the insert.
      ...[0, 1, 2].map((request) =>
        entry("tool_call", `t${request}`, {
          name: "Edit",
          target: `src/file-${request}.ts`,
          tool_use_id: `toolu_${request}`,
          args_json: "{}",
          is_error: 0,
        }),
      ),
    ];
    const deliver = async (batchId: string, batch: HistoryExportRecord[]) =>
      acceptDelivery(db, auth, {
        schema_version: 1,
        origin_id: "origin-a",
        batch_id: batchId,
        job_id: "job",
        generation: 1,
        destination_id: "relayhistory",
        instance_id: "rollup-test",
        account_id: await deliveryAccount(auth),
        mapping_version: "relayhistory-delivery-v1",
        records: batch,
      });

    await deliver("first", records);
    const delivered = await rows(
      "SELECT count(*)::int AS n FROM sessions.convergence_events",
    );
    expect(delivered[0]!.n).toBeGreaterThan(0);
    await expectRollupsMatchEvents();
    await deliver("replay", records);
    await expectRollupsMatchEvents();

    await deliver("tombstones", [
      entry("session_event", "r2", null, 2),
      entry("tool_call", "t2", null, 2),
    ]);
    await expectRollupsMatchEvents();
    const [after] = await rows(
      "SELECT count(*)::int AS n FROM sessions.convergence_events",
    );
    expect(after!.n).toBeLessThan(delivered[0]!.n);
  });
});

describe("session rollup backfill", () => {
  beforeEach(async () => {
    await migrateSet(beforeRollups);
    await insert(
      Array.from({ length: 9 }, (_, index) => ({
        session: `s${index}`,
        id: `e${index}`,
        ts: `2026-09-01T10:0${index}:00Z`,
        title: `Session ${index}`,
        input: index,
      })),
    );
    await migrateSet(migrations);
  });

  it("keeps the event aggregate until the backfill completes, then reads rollups", async () => {
    const [state] = await rows(
      "SELECT completed_at FROM sessions.session_rollup_rollout",
    );
    expect(state!.completed_at).toBeNull();
    expect(await rows("SELECT * FROM sessions.session_rollups")).toEqual([]);
    const pending = await listSessions(db, auth, {}, { limit: 50 });
    expect(pending.sessions).toHaveLength(9);
    expect(pending).toEqual(await listSessionsFromEvents(db, auth, {}));

    // One batch, then writes on both sides of its cursor and into a new session.
    const [first] = await rows<{ n: number }>(
      "SELECT sessions.session_rollup_backfill_step(3) AS n",
    );
    expect(first!.n).toBe(3);
    await insert([
      { session: "s0", id: "late-0", ts: "2026-09-02T00:00:00Z", input: 100 },
      { session: "s7", id: "late-7", ts: "2026-09-02T00:01:00Z", input: 100 },
      { session: "s9", id: "late-9", ts: "2026-09-02T00:02:00Z" },
    ]);
    await client.query(
      "DELETE FROM sessions.convergence_events WHERE event_id = 'e8'",
    );

    const outcome = await rolloutSessionRollups(
      async (sql: string) => (await client.query(sql)).rows,
      { batch: 2 },
    );
    expect(outcome).toEqual({ complete: true, sessions: 6 });
    await expectRollupsMatchEvents();
    const [done] = await rows(
      "SELECT completed_at IS NOT NULL AS complete FROM sessions.session_rollup_rollout",
    );
    expect(done).toEqual({ complete: true });
    expect(await listSessions(db, auth, {})).toEqual(
      await listSessionsFromEvents(db, auth, {}),
    );

    // The list now reads the rollup rows themselves.
    await client.query(
      "UPDATE sessions.session_rollups SET event_count = 999 WHERE session_id = 's9'",
    );
    const served = await listSessions(db, auth, { sessionId: "s9" });
    expect(served.sessions[0]!.eventCount).toBe(999);
    // A rerun finds nothing left to do.
    expect(
      await rolloutSessionRollups(
        async (sql: string) => (await client.query(sql)).rows,
      ),
    ).toEqual({ complete: true, sessions: 0 });
  });
});
