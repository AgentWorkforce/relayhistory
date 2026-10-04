import { createHash } from "node:crypto";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { getTableConfig, type PgTable } from "drizzle-orm/pg-core";
import {
  migrationStatements,
  readMigrations,
  rolloutDeliveryProjection,
  type Migration,
} from "../src/migrate/index.js";
import * as schema from "../src/db/schema.js";
import { getSessionEvents } from "../src/lib/recall.js";
import type { AuthContext } from "../src/env.js";
import { applyIngest } from "../src/lib/ingest.js";
import {
  createFreshDatabase,
  type FreshDatabase,
} from "./support/fresh-database.js";

// The unmodified packaged files, pgvector included, on PGlite or real PostgreSQL.
const migrations: Migration[] = readMigrations();
let database: FreshDatabase;
let client: FreshDatabase;
async function run(input = migrations, runtimeRole?: string) {
  await database.transaction(async (query) => {
    for (const statement of migrationStatements(input, runtimeRole))
      await query(statement);
  });
  // A deploy runs the 0029 rollout after its migrations; the projections go live there.
  if (input.some((m) => m.name.startsWith("0029_"))) {
    const outcome = await rolloutDeliveryProjection(
      async (sql) => (await database.query(sql)).rows,
    );
    if (!outcome.complete) throw new Error("delivery rollout did not complete");
  }
}

beforeEach(async () => {
  database = await createFreshDatabase();
  client = database;
  await client.exec(`CREATE TABLE public.auth_sessions (id text PRIMARY KEY);
    INSERT INTO public.auth_sessions VALUES ('existing-cloud-user');
    CREATE TABLE public.convergence_events (id text PRIMARY KEY);
    INSERT INTO public.convergence_events VALUES ('existing-public-event');
    CREATE SCHEMA drizzle;
    CREATE TABLE drizzle.__drizzle_migrations (id integer PRIMARY KEY);
    INSERT INTO drizzle.__drizzle_migrations VALUES (123);`);
});
afterEach(async () => {
  await database.close();
});

describe("sessions migrations in Cloud's database", () => {
  it("creates all ORM tables, preserves public data and the Cloud ledger, and reruns without replaying DDL", async () => {
    await run();
    const before = (
      await client.query("SELECT * FROM sessions.__migrations ORDER BY name")
    ).rows;
    await run();
    expect(
      (await client.query("SELECT * FROM sessions.__migrations ORDER BY name"))
        .rows,
    ).toEqual(before);
    expect(before).toHaveLength(migrations.length);
    for (const table of Object.values(schema)) {
      const config = getTableConfig(table as PgTable);
      expect(config.schema).toBe("sessions");
      const result = await client.query("SELECT to_regclass($1) AS relation", [
        `sessions.${config.name}`,
      ]);
      expect(result.rows[0]).toEqual({ relation: `sessions.${config.name}` });
    }
    expect(
      (await client.query("SELECT * FROM public.auth_sessions")).rows,
    ).toEqual([{ id: "existing-cloud-user" }]);
    expect(
      (await client.query("SELECT * FROM drizzle.__drizzle_migrations")).rows,
    ).toEqual([{ id: 123 }]);
  });

  it("repairs existing projected prompts and metadata without losing retained records or real activity", async () => {
    await run(migrations.filter((m) => m.name < "0014"));
    const insert = async (id: string, kind: string, payload: object) =>
      client.query(
        `INSERT INTO sessions.delivery_records
        (org_id, workspace_id, origin_id, record_id, revision_id, revision, digest, kind, source, session_id, operation, payload, user_id)
        VALUES ('org', 'w', 'origin', $1, $1, 1, 'digest', $2, 'claude', 's', 'upsert', $3::jsonb, 'u')`,
        [id, kind, JSON.stringify(payload)],
      );
    await insert("opening", "history", {
      prompt: "Build feature",
      timestamp_ms: 1000,
    });
    await insert("followup", "history", {
      prompt: "Try again",
      timestamp_ms: 2000,
    });
    await insert("metadata", "session", { last_activity_ms: 3000 });
    await insert("presence", "source_observation", {});
    await insert("tool", "tool_call", {
      name: "Read",
      target: "src/app.ts",
      ts_ms: 1500,
    });
    await insert("trajectory", "trajectory", {
      task_title: "Actual task title",
      status: "completed",
      timestamp_ms: 1000,
      updated_ms: 5000,
    });
    expect(
      (
        await client.query(
          "SELECT count(*)::int AS n FROM sessions.convergence_events",
        )
      ).rows,
    ).toEqual([{ n: 6 }]);
    await run();
    expect(
      (
        await client.query(
          "SELECT count(*)::int AS n FROM sessions.delivery_records",
        )
      ).rows,
    ).toEqual([{ n: 6 }]);
    expect(
      (
        await client.query(
          "SELECT record->>'deliveryRecordId' AS event_id, task_title, content FROM sessions.convergence_events ORDER BY event_id",
        )
      ).rows,
    ).toEqual([
      { event_id: "followup", task_title: null, content: "Try again" },
      { event_id: "opening", task_title: null, content: "Build feature" },
      { event_id: "tool", task_title: null, content: "Read src/app.ts" },
      {
        event_id: "trajectory",
        task_title: "Actual task title",
        content: null,
      },
    ]);
    expect(
      (
        await client.query(
          "SELECT extract(epoch from ts)::int AS seconds FROM sessions.convergence_events WHERE record->>'deliveryRecordId' = 'trajectory'",
        )
      ).rows,
    ).toEqual([{ seconds: 5 }]);
    await client.exec(`UPDATE sessions.delivery_records
      SET payload = payload || '{"updated_ms": null}'::jsonb
      WHERE record_id = 'trajectory'`);
    expect(
      (
        await client.query(
          "SELECT extract(epoch from ts)::int AS seconds FROM sessions.convergence_events WHERE record->>'deliveryRecordId' = 'trajectory'",
        )
      ).rows,
    ).toEqual([{ seconds: 1 }]);
    await client.exec(`UPDATE sessions.delivery_records
      SET payload = payload || '{"updated_ms": 5000}'::jsonb
      WHERE record_id = 'trajectory'`);
    // Different computers may export the same record ID at the same instant. Record IDs
    // are origin-independent, so since 0023 the logical record is projected once (the
    // tie breaks on origin_id) instead of once per origin.
    await client.exec(`INSERT INTO sessions.delivery_records
      SELECT org_id, workspace_id, 'second-origin', record_id, revision_id, revision, digest, kind, source, session_id, operation, payload, user_id, received_at
      FROM sessions.delivery_records WHERE record_id = 'opening'`);
    const db = database.db;
    const auth = {
      orgId: "org",
      userId: "u",
      scopes: ["rth:read"],
      claims: {},
    } as AuthContext;
    const first = await getSessionEvents(db, auth, "s", { limit: 1 });
    const second = await getSessionEvents(db, auth, "s", {
      limit: 1,
      cursor: first.nextCursor ?? undefined,
    });
    expect(first.events[0].content).toBe("Build feature");
    expect(first.events[0].machineId).toBe(
      "delivery:" +
        createHash("md5")
          .update(JSON.stringify(["w", "second-origin"]).replace(",", ", "))
          .digest("hex"),
    );
    expect(second.events[0].content).toBe("Read src/app.ts");
    expect(
      (
        await client.query(
          "SELECT count(*)::int AS n FROM sessions.convergence_events WHERE record->>'deliveryRecordId' = 'opening'",
        )
      ).rows,
    ).toEqual([{ n: 1 }]);
    await insert("new-metadata", "session", {});
    await insert("new-prompt", "session_event", {
      role: "user",
      text: "Now test it",
      ts_ms: 4000,
    });
    expect(
      (
        await client.query(
          "SELECT record->>'deliveryRecordId' AS event_id, task_title FROM sessions.convergence_events WHERE record->>'deliveryRecordId' LIKE 'new-%'",
        )
      ).rows,
    ).toEqual([{ event_id: "new-prompt", task_title: null }]);
    await client.exec(
      "UPDATE sessions.delivery_records SET operation = 'delete', payload = NULL WHERE record_id = 'new-prompt'",
    );
    expect(
      (
        await client.query(
          "SELECT event_id FROM sessions.convergence_events WHERE record->>'deliveryRecordId' = 'new-prompt'",
        )
      ).rows,
    ).toEqual([]);
  });

  it("grants the existing Cloud runtime role session access without exposing the ledger", async () => {
    const cloudRuntime = await database.createRole("cloud_runtime");
    await client.exec(`GRANT USAGE ON SCHEMA public TO ${cloudRuntime}`);
    await run(migrations, cloudRuntime);
    await client.exec(`SET ROLE ${cloudRuntime}`);
    await client.exec(
      "INSERT INTO sessions.machines (org_id, workspace_id, machine_id) VALUES ('org', 'workspace', 'machine')",
    );
    expect(
      (await client.query("SELECT machine_id FROM sessions.machines")).rows,
    ).toEqual([{ machine_id: "machine" }]);
    await client.exec(
      "INSERT INTO sessions.session_links (org_id, workspace_id, source, session_id, link_kind, link_ref, provenance_lens) VALUES ('org', 'workspace', 'source', 'session', 'pr', 'ref', 'history')",
    );
    await expect(
      client.query("DELETE FROM sessions.__migrations"),
    ).rejects.toThrow("permission denied");
    await expect(
      client.query("DELETE FROM public.auth_sessions"),
    ).rejects.toThrow("permission denied");
    await client.exec("RESET ROLE");
  });

  it("fails on edited applied migrations", async () => {
    await run();
    const changed = migrations.map((m, i) =>
      i === 0 ? { ...m, checksum: "different" } : m,
    );
    await expect(run(changed)).rejects.toThrow("checksum changed");
    expect(
      (
        await client.query(
          "SELECT count(*)::int AS n FROM sessions.__migrations",
        )
      ).rows,
    ).toEqual([{ n: migrations.length }]);
  });

  it("rolls back pending DDL and ledger entries together on failure", async () => {
    await run();
    const invalid = {
      name: "9999_failed.sql",
      checksum: "test",
      statements: [
        "CREATE TABLE sessions.should_rollback (id int)",
        "SELECT missing_column FROM sessions.should_rollback",
      ],
    };
    await expect(run([...migrations, invalid])).rejects.toThrow();
    expect(
      (
        await client.query(
          "SELECT to_regclass('sessions.should_rollback') AS relation",
        )
      ).rows,
    ).toEqual([{ relation: null }]);
    expect(
      (
        await client.query(
          "SELECT count(*)::int AS n FROM sessions.__migrations",
        )
      ).rows,
    ).toEqual([{ n: migrations.length }]);
  });

  it("writes through Drizzle to sessions with the default public search path", async () => {
    await run();
    const db = database.db;
    await applyIngest(
      db,
      {
        userId: "user",
        orgId: "org",
        workspaceId: "workspace",
        tokenSubject: "user",
        scopes: ["rth:sync"],
        claims: {},
      },
      {
        machine: { id: "machine" },
        batchId: "batch",
        records: [
          {
            v: 1,
            kind: "event",
            source: "claude",
            sessionId: "session",
            eventId: "event",
            ts: "2026-09-19T00:00:00Z",
            content: "Synthetic migration test",
          },
        ],
      },
    );
    expect(
      (
        await client.query(
          "SELECT event_id, user_id FROM sessions.convergence_events",
        )
      ).rows,
    ).toEqual([{ event_id: "event:event", user_id: "user" }]);
    expect(
      (await client.query("SELECT * FROM public.convergence_events")).rows,
    ).toEqual([{ id: "existing-public-event" }]);
  });
});
