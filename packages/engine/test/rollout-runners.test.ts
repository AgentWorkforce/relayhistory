/**
 * The rollout runners as a deploy drives them: two replicas starting together, bad
 * options, and a time budget. Concurrency needs two connections, so it runs against a
 * real server only.
 */
import pg from "pg";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  migrationStatements,
  readMigrations,
  rolloutDeliveryProjection,
  rolloutSessionRollups,
  type RolloutQuery,
} from "../src/migrate/index.js";
import { testDatabaseKind } from "./support/database.js";
import {
  createFreshDatabase,
  type FreshDatabase,
} from "./support/fresh-database.js";

let database: FreshDatabase;
const others: pg.Client[] = [];

beforeEach(async () => {
  database = await createFreshDatabase();
  await database.transaction(async (query) => {
    for (const statement of migrationStatements(readMigrations()))
      await query(statement);
  });
});
afterEach(async () => {
  for (const other of others.splice(0)) await other.end();
  await database.close();
});

const queryOf =
  (client: { query(sql: string): Promise<{ rows: any[] }> }): RolloutQuery =>
  async (sql) =>
    (await client.query(sql)).rows;

/** A second session on the same database. */
async function secondConnection(): Promise<pg.Client> {
  const [{ name }] = (
    await database.query<{ name: string }>("SELECT current_database() AS name")
  ).rows as [{ name: string }];
  const url = new URL(process.env.DATABASE_URL!);
  url.pathname = `/${name}`;
  const other = new pg.Client({ connectionString: url.toString() });
  await other.connect();
  others.push(other);
  return other;
}

async function stages() {
  return (
    await database.query(
      `SELECT stage, completed_at IS NOT NULL AS done FROM sessions.delivery_rollout
       UNION ALL
       SELECT 'rollups', completed_at IS NOT NULL FROM sessions.session_rollup_rollout
       ORDER BY 1`,
    )
  ).rows;
}

describe.runIf(testDatabaseKind === "postgres")(
  "two runners on one database",
  () => {
    it("complete the delivery projection rollout once, without colliding", async () => {
      const other = await secondConnection();
      const outcomes = await Promise.all([
        rolloutDeliveryProjection(queryOf(database), {}),
        rolloutDeliveryProjection(queryOf(other), {}),
      ]);
      expect(outcomes).toEqual([{ complete: true }, { complete: true }]);
      expect(await stages()).toEqual([
        { stage: "activity", done: true },
        { stage: "catalog", done: true },
        { stage: "rollups", done: true },
      ]);
    });

    it("complete the session rollup backfill once", async () => {
      await rolloutDeliveryProjection(queryOf(database));
      await database.query(
        "UPDATE sessions.session_rollup_rollout SET completed_at = NULL",
      );
      for (const session of ["s1", "s2", "s3", "s4", "s5"])
        await database.query(
          `INSERT INTO sessions.convergence_events
             (org_id, workspace_id, machine_id, user_id, source, session_id, event_id, kind, type, ts, record)
           VALUES ('org', 'ws', 'machine', 'user', 'claude', $1, $1 || '-e', 'assistant', 'message', now(), '{}')`,
          [session],
        );
      const other = await secondConnection();
      const outcomes = await Promise.all([
        rolloutSessionRollups(queryOf(database), { batch: 2 }),
        rolloutSessionRollups(queryOf(other), { batch: 2 }),
      ]);
      expect(outcomes.map((outcome) => outcome.complete)).toEqual([true, true]);
      // Every session is rebuilt by exactly one of them.
      expect(outcomes.reduce((sum, outcome) => sum + outcome.sessions, 0)).toBe(
        5,
      );
    });
  },
);

async function advisoryLocks() {
  return (
    await database.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM pg_locks
        WHERE locktype = 'advisory'
          AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`,
    )
  ).rows[0]!.n;
}

describe("rollout options", () => {
  it.each([0, -1, 1.5, Number.NaN])(
    "rejects batch %s before any SQL",
    async (batch) => {
      const calls: string[] = [];
      const query: RolloutQuery = async (sql) => {
        calls.push(sql);
        return [];
      };
      await expect(rolloutDeliveryProjection(query, { batch })).rejects.toThrow(
        RangeError,
      );
      await expect(rolloutSessionRollups(query, { batch })).rejects.toThrow(
        RangeError,
      );
      expect(calls).toEqual([]);
    },
  );

  it("builds no index and activates nothing once the deadline has passed", async () => {
    const lines: string[] = [];
    const outcome = await rolloutDeliveryProjection(queryOf(database), {
      deadline: Date.now() - 1,
      report: (line) => lines.push(line),
    });
    expect(outcome).toEqual({ complete: false, stage: "catalog" });
    expect(lines).toEqual([
      "Rollout catalog: paused after 0 keys; rerun to resume",
    ]);
    const indexes = (
      await database.query<{ name: string }>(
        "SELECT index_name AS name FROM sessions.delivery_rollout_indexes()",
      )
    ).rows.map((row) => row.name);
    expect(
      (
        await database.query(
          "SELECT relname FROM pg_class WHERE relkind = 'i' AND relname = ANY ($1)",
          [indexes],
        )
      ).rows,
    ).toEqual([]);
    expect(
      (await database.query("SELECT * FROM sessions.delivery_rollout")).rows,
    ).toEqual([]);
    expect(await advisoryLocks()).toBe(0);
  });

  it("pauses at the deadline while another runner holds the lock", async () => {
    const calls: string[] = [];
    const query: RolloutQuery = async (sql) => {
      calls.push(sql);
      return sql.includes("pg_try_advisory_lock(") ? [{ locked: false }] : [];
    };
    const deadline = Date.now() - 1;
    await expect(
      rolloutDeliveryProjection(query, { deadline }),
    ).resolves.toEqual({
      complete: false,
      stage: "catalog",
    });
    await expect(rolloutSessionRollups(query, { deadline })).resolves.toEqual({
      complete: false,
      sessions: 0,
    });
    expect(calls).toEqual([
      "SELECT pg_try_advisory_lock(1919249529, 3) AS locked, pg_backend_pid() AS pid",
      "SELECT pg_try_advisory_lock(1919249529, 4) AS locked, pg_backend_pid() AS pid",
    ]);
  });

  it("releases its lock when a stage fails", async () => {
    const failing: RolloutQuery = async (sql) => {
      if (sql.includes("activate_delivery_catalog"))
        throw new Error("activation failed");
      return (await database.query(sql)).rows;
    };
    await expect(rolloutDeliveryProjection(failing)).rejects.toThrow(
      "activation failed",
    );
    expect(await advisoryLocks()).toBe(0);
    // A rerun resumes and completes.
    await expect(rolloutDeliveryProjection(queryOf(database))).resolves.toEqual(
      {
        complete: true,
      },
    );
    await expect(rolloutSessionRollups(queryOf(database))).resolves.toEqual({
      complete: true,
      sessions: 0,
    });
    expect(await advisoryLocks()).toBe(0);
  });
});

describe("rollout lock release", () => {
  it("fails when the unlock did not release the lock", async () => {
    const query: RolloutQuery = async (sql) => {
      if (sql.includes("pg_try_advisory_lock("))
        return [{ locked: true, pid: 7 }];
      if (sql.includes("pg_advisory_unlock("))
        return [{ released: false, pid: 7 }];
      if (sql.includes("activate_")) return [{ activated: false }];
      return [];
    };
    await expect(rolloutDeliveryProjection(query)).rejects.toThrow(
      "rollout lock was not released by the connection that took it",
    );
    await expect(rolloutSessionRollups(query)).rejects.toThrow(
      "rollout lock was not released by the connection that took it",
    );
  });

  it.runIf(testDatabaseKind === "postgres")(
    "fails behind a transaction pooler that hands statements to other backends",
    async () => {
      const backends = [await secondConnection(), await secondConnection()];
      const pids = await Promise.all(
        backends.map(
          async (backend) =>
            (await backend.query("SELECT pg_backend_pid() AS pid")).rows[0]
              .pid as number,
        ),
      );
      // A transaction-mode pooler: an explicit transaction stays on the backend that
      // ran its BEGIN until COMMIT or ROLLBACK; each statement outside one goes to the
      // next backend.
      let current = 0;
      let pinned: number | null = null;
      const routed: Array<{ sql: string; backend: number }> = [];
      const pooled: RolloutQuery = async (sql) => {
        let backend: number;
        if (pinned !== null) {
          backend = pinned;
          if (sql === "COMMIT" || sql === "ROLLBACK") pinned = null;
        } else if (sql === "BEGIN") {
          backend = pinned = current;
        } else {
          backend = current;
          current = (current + 1) % backends.length;
        }
        routed.push({ sql, backend });
        return (await backends[backend]!.query(sql)).rows;
      };
      await expect(rolloutSessionRollups(pooled)).rejects.toThrow(
        "rollout lock was not released by the connection that took it",
      );
      // Every backfill transaction ran on one backend, and the unlock reached the
      // other backend from the one that took the lock.
      const begin = routed.findIndex((step) => step.sql === "BEGIN");
      const commit = routed.findIndex((step) => step.sql === "COMMIT");
      expect(begin).toBeGreaterThan(0);
      expect(
        new Set(routed.slice(begin, commit + 1).map((step) => step.backend))
          .size,
      ).toBe(1);
      const lock = routed.find((step) =>
        step.sql.includes("pg_try_advisory_lock("),
      )!;
      const unlock = routed.find((step) =>
        step.sql.includes("pg_advisory_unlock("),
      )!;
      expect(unlock.backend).not.toBe(lock.backend);
      // The backend that took the lock still holds it: the leak the error reports.
      expect(
        (
          await database.query(
            "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
          )
        ).rows,
      ).toEqual([{ pid: pids[lock.backend] }]);
      for (const backend of others.splice(0)) await backend.end();
      expect(await advisoryLocks()).toBe(0);
    },
  );
});
