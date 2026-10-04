import { cpSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import {
  CONCURRENT_INDEXES,
  DELIVERY_PROJECTION_ROLLOUT_MIGRATION,
  MIGRATIONS_DIR,
  SESSION_ROLLUPS_MIGRATION,
  SUPERSEDED_CHECKSUMS,
  applyMigrations,
  migrationStatements,
  readMigrations,
} from "../src/migrate/index.js";
import {
  createFreshDatabase,
  type FreshDatabase,
} from "./support/fresh-database.js";

const directories: string[] = [];
afterAll(() => {
  for (const directory of directories)
    rmSync(directory, { recursive: true, force: true });
});

/** A migrations directory holding `files`, optionally after the packaged set. */
function migrationsDirectory(
  files: Record<string, string>,
  { packaged = false } = {},
): string {
  const directory = mkdtempSync(join(tmpdir(), "rh-engine-migrations-"));
  directories.push(directory);
  if (packaged) cpSync(MIGRATIONS_DIR, directory, { recursive: true });
  for (const [name, source] of Object.entries(files))
    writeFileSync(join(directory, name), source);
  return directory;
}

const FIXTURE = migrationsDirectory({
  "0001.sql": "SELECT first;\nSELECT second;\n",
});
const FIXTURE_STATEMENTS = migrationStatements(readMigrations(FIXTURE));
const MIGRATION = FIXTURE_STATEMENTS.find((s) => s.startsWith("DO "))!;
const LEDGER = FIXTURE_STATEMENTS.at(-1)!;
const TRANSACTION_TIMEOUT_PROBE =
  "SELECT current_setting('transaction_timeout', true) AS v";

/**
 * A recording node-postgres-style client. The extension probe of every concurrent
 * index answers "not installed", so each is skipped after one probe.
 */
function fakeClient(
  options: {
    failQuery?: string;
    failRollback?: boolean;
    notices?: string[];
    /** `current_setting('transaction_timeout', true)`; null on PostgreSQL < 17. */
    transactionTimeout?: string | null;
    /** Migration names the ledger reports after the run. */
    ledger?: string[];
    /** Sessions the first session rollup backfill step reports. */
    rolledUp?: number;
  } = {},
) {
  let rolledUp = options.rolledUp ?? 0;
  const queries: string[] = [];
  const listeners = { added: 0, removed: 0 };
  let onNotice: ((notice: { message?: string }) => void) | undefined;
  const client = {
    async query(statement: string) {
      queries.push(statement);
      if (statement === MIGRATION) {
        for (const message of options.notices ?? []) onNotice?.({ message });
      }
      if (statement === options.failQuery) throw new Error("query failed");
      if (statement === "ROLLBACK" && options.failRollback)
        throw new Error("rollback failed");
      if (statement === TRANSACTION_TIMEOUT_PROBE)
        return {
          rows: [
            {
              v:
                options.transactionTimeout === undefined
                  ? "0"
                  : options.transactionTimeout,
            },
          ],
        };
      if (statement === LEDGER)
        return {
          rows: (options.ledger ?? ["0001.sql"]).map((name) => ({
            name,
            checksum: "checksum",
          })),
        };
      if (statement.includes("session_rollup_backfill_step(")) {
        const processed = rolledUp;
        rolledUp = 0;
        return { rows: [{ processed }] };
      }
      return { rows: [] };
    },
    on(event: "notice", listener: (notice: { message?: string }) => void) {
      if (event === "notice") {
        listeners.added += 1;
        onNotice = listener;
      }
    },
    removeListener(event: "notice") {
      if (event === "notice") {
        listeners.removed += 1;
        onNotice = undefined;
      }
    },
  };
  return { client, queries, listeners };
}

const PREAMBLE = [
  "BEGIN",
  "SET LOCAL lock_timeout = '30s'",
  TRANSACTION_TIMEOUT_PROBE,
  "SET LOCAL transaction_timeout = '10min'",
  "SET LOCAL statement_timeout = '5min'",
];
const INDEX_PROBES = CONCURRENT_INDEXES.map(({ extension }) =>
  expect.stringContaining(`extname = '${extension}'`),
);

describe("migration runner", () => {
  it("marks inner migration statements without changing migration SQL", () => {
    const statements = migrationStatements(
      [
        {
          name: "0001.sql",
          checksum: "checksum",
          statements: ["SELECT first", "SELECT second"],
        },
      ],
      "runtime",
    );
    expect(statements[4]).toContain(
      "RAISE NOTICE 'Sessions migration detail 1/1 statement 2/2: start';",
    );
    expect(statements[4]).toContain("EXECUTE 'SELECT second';");
    expect(statements[4]).toContain(
      "RAISE NOTICE 'Sessions migration detail 1/1 statement 2/2: done';",
    );
  });

  it("checks every migration against its checksum and superseded checksums", () => {
    const statements = migrationStatements(readMigrations()).join("\n");
    for (const [name, superseded] of Object.entries(SUPERSEDED_CHECKSUMS)) {
      const migration = readMigrations().find((m) => m.name === name)!;
      expect(statements).toContain(
        `ARRAY[${[migration.checksum, ...superseded].map((c) => `'${c}'`).join(", ")}]`,
      );
    }
    expect(statements).toContain(
      "RAISE EXCEPTION 'Applied sessions migration checksum changed: %'",
    );
  });

  it("grants the runtime role only when one is named", () => {
    const without = migrationStatements(readMigrations());
    const granted = migrationStatements(readMigrations(), "cloud_runtime");
    expect(without.join("\n")).not.toContain("GRANT");
    expect(granted).toHaveLength(without.length + 1);
    const grants = granted.at(-2)!;
    expect(grants).toContain("$runtime_grants$");
    expect(grants).toContain("tablename <> '__migrations'");
    expect(grants).toContain("'cloud_runtime'");
    // The ledger read stays last, after the grants.
    expect(granted.at(-1)).toBe(without.at(-1));
  });

  it("reports only fixed progress notices from the driver", async () => {
    const fake = fakeClient({
      notices: [
        "Sessions migration detail 21/24 statement 5/12: start",
        "secret URL postgresql://owner:secret@example.test and SQL SELECT *",
        "Sessions migration detail 21/24 statement 5/12: done",
      ],
    });
    const progress: string[] = [];
    await applyMigrations(fake.client, {
      directory: FIXTURE,
      report: (message) => progress.push(message),
    });
    expect(progress).toContain(
      "Sessions migration detail 21/24 statement 5/12: start",
    );
    expect(progress).toContain(
      "Sessions migration detail 21/24 statement 5/12: done",
    );
    expect(progress.join(" ")).not.toMatch(/secret|SELECT/);
    expect(fake.listeners).toEqual({ added: 1, removed: 1 });
  });

  it("runs every statement sequentially on one atomic connection", async () => {
    const fake = fakeClient();
    const progress: string[] = [];

    const result = await applyMigrations(fake.client, {
      directory: FIXTURE,
      report: (message) => progress.push(message),
    });

    // The migration transaction, then the concurrent indexes. The ledger holds
    // neither rollout's migration, so no rollout runs.
    expect(fake.queries).toEqual([
      ...PREAMBLE,
      ...FIXTURE_STATEMENTS,
      "COMMIT",
      ...INDEX_PROBES,
    ]);
    expect(FIXTURE_STATEMENTS[0]).toBe(
      "SELECT pg_advisory_xact_lock(1919249529, 1)",
    );
    expect(result).toEqual({
      applied: [{ name: "0001.sql", checksum: "checksum" }],
      indexes: Object.fromEntries(
        CONCURRENT_INDEXES.map(({ name }) => [name, "skipped"]),
      ),
      rolledUpSessions: 0,
    });
    expect(progress).toContain(
      `Sessions migration statement ${FIXTURE_STATEMENTS.length}/${FIXTURE_STATEMENTS.length}: done`,
    );
    expect(progress.join(" ")).not.toMatch(/secret|SELECT/);
  });

  it("rolls back after a statement failure and builds no indexes", async () => {
    const fake = fakeClient({ failQuery: MIGRATION });

    await expect(
      applyMigrations(fake.client, { directory: FIXTURE }),
    ).rejects.toThrow("query failed");

    const failed = FIXTURE_STATEMENTS.indexOf(MIGRATION);
    expect(fake.queries).toEqual([
      ...PREAMBLE,
      ...FIXTURE_STATEMENTS.slice(0, failed + 1),
      "ROLLBACK",
    ]);
    expect(fake.listeners).toEqual({ added: 1, removed: 1 });
  });

  it("preserves the migration error when rollback also fails", async () => {
    const fake = fakeClient({ failQuery: MIGRATION, failRollback: true });

    await expect(
      applyMigrations(fake.client, { directory: FIXTURE }),
    ).rejects.toThrow("query failed");
    expect(fake.queries.at(-1)).toBe("ROLLBACK");
  });

  it("does not roll back a transaction that never began", async () => {
    const fake = fakeClient({ failQuery: "BEGIN" });

    await expect(
      applyMigrations(fake.client, { directory: FIXTURE }),
    ).rejects.toThrow("query failed");
    expect(fake.queries).toEqual(["BEGIN"]);
  });

  it("bounds how long the migration may hold its locks", async () => {
    const fake = fakeClient();
    await applyMigrations(fake.client, { directory: FIXTURE });
    const queries = fake.queries;
    // Both limits are transaction-scoped and set before any migration SQL, so
    // a pooled connection that drops session settings still enforces them.
    const begin = queries.indexOf("BEGIN");
    for (const limit of [
      "SET LOCAL transaction_timeout = '10min'",
      "SET LOCAL statement_timeout = '5min'",
    ]) {
      expect(queries.indexOf(limit)).toBeGreaterThan(begin);
      expect(queries.indexOf(limit)).toBeLessThan(
        queries.indexOf(FIXTURE_STATEMENTS[0]!),
      );
    }
    expect(
      queries.some((query) => /^SET transaction_timeout/.test(query)),
    ).toBe(false);
  });

  it("skips transaction_timeout on a server without it and keeps the statement bound", async () => {
    const fake = fakeClient({ transactionTimeout: null });
    await applyMigrations(fake.client, { directory: FIXTURE });
    expect(fake.queries.some((q) => q.includes("transaction_timeout ="))).toBe(
      false,
    );
    expect(fake.queries.slice(0, 4)).toEqual([
      "BEGIN",
      "SET LOCAL lock_timeout = '30s'",
      TRANSACTION_TIMEOUT_PROBE,
      "SET LOCAL statement_timeout = '5min'",
    ]);
  });
});

describe("Concurrent index runner", () => {
  it("builds on the same connection outside any transaction, after commit", async () => {
    const fake = fakeClient();
    const { indexes } = await applyMigrations(fake.client, {
      directory: FIXTURE,
    });

    const commit = fake.queries.indexOf("COMMIT");
    const builds = fake.queries.slice(
      commit + 1,
      commit + 1 + CONCURRENT_INDEXES.length,
    );
    expect(Object.values(indexes).every((a) => a === "skipped")).toBe(true);
    expect(builds.some((q) => /\bBEGIN\b/.test(q))).toBe(false);
    expect(builds.every((q) => q.includes("pg_extension"))).toBe(true);
    expect(fake.queries.slice(commit + 1)).toEqual(builds);
  });
});

describe("rollouts after the migration transaction", () => {
  const ROLLOUTS = migrationsDirectory({
    [DELIVERY_PROJECTION_ROLLOUT_MIGRATION]: "SELECT 1;\n",
    [SESSION_ROLLUPS_MIGRATION]: "SELECT 1;\n",
  });
  const rolloutQuery = (q: string) =>
    /delivery_rollout|activate_delivery|_backfill_step|_reproject_step|session_rollup/.test(
      q,
    );

  it("runs no rollout when the ledger holds neither migration", async () => {
    const fake = fakeClient();
    await applyMigrations(fake.client, { directory: FIXTURE });
    expect(fake.queries.filter(rolloutQuery)).toEqual([]);
  });

  it("runs the delivery projection rollout, then the session rollup backfill, after the indexes", async () => {
    const fake = fakeClient({
      ledger: [
        DELIVERY_PROJECTION_ROLLOUT_MIGRATION,
        SESSION_ROLLUPS_MIGRATION,
      ],
      rolledUp: 3,
    });
    const progress: string[] = [];
    const result = await applyMigrations(fake.client, {
      directory: ROLLOUTS,
      report: (line) => progress.push(line),
    });

    expect(result.rolledUpSessions).toBe(3);
    const commit = fake.queries.indexOf("COMMIT");
    const after = fake.queries.slice(commit + 1 + CONCURRENT_INDEXES.length);
    const position = (fragment: string) =>
      after.findIndex((q) => q.includes(fragment));
    expect(position("activate_delivery_catalog()")).toBeGreaterThanOrEqual(0);
    expect(position("activate_delivery_catalog()")).toBeLessThan(
      position("activate_delivery_projection_v2()"),
    );
    expect(position("activate_delivery_projection_v2()")).toBeLessThan(
      position("session_rollup_backfill_step("),
    );
    // The backfill filled the table, so it is analyzed for the planner.
    expect(after.at(-1)).toBe("ANALYZE sessions.session_rollups");
    expect(progress).toContain(
      "Session rollups: complete (3 sessions this run)",
    );
  });

  it("runs only the delivery projection rollout before 0030 is applied", async () => {
    const fake = fakeClient({
      ledger: [DELIVERY_PROJECTION_ROLLOUT_MIGRATION],
    });
    const result = await applyMigrations(fake.client, { directory: ROLLOUTS });
    expect(
      fake.queries.some((q) => q.includes("activate_delivery_projection_v2()")),
    ).toBe(true);
    expect(
      fake.queries.some((q) => q.includes("session_rollup_backfill_step(")),
    ).toBe(false);
    expect(result.rolledUpSessions).toBe(0);
  });

  it("leaves a database migrated short of 0029 without either rollout", async () => {
    const database = await createFreshDatabase();
    try {
      const directory = migrationsDirectory({}, { packaged: true });
      for (const name of readMigrations())
        if (name.name >= DELIVERY_PROJECTION_ROLLOUT_MIGRATION)
          rmSync(join(directory, name.name));

      const result = await applyMigrations(database.client, { directory });

      expect(result.applied.map((m) => m.name)).not.toContain(
        DELIVERY_PROJECTION_ROLLOUT_MIGRATION,
      );
      expect(result.rolledUpSessions).toBe(0);
      expect(
        (
          await database.query(
            "SELECT to_regclass('sessions.delivery_rollout') AS relation",
          )
        ).rows,
      ).toEqual([{ relation: null }]);
    } finally {
      await database.close();
    }
  });

  it("completes both rollouts on a real server and reports the sessions rolled up", async () => {
    const database = await createFreshDatabase();
    try {
      const directory = migrationsDirectory({}, { packaged: true });
      for (const name of readMigrations())
        if (name.name >= SESSION_ROLLUPS_MIGRATION)
          rmSync(join(directory, name.name));
      await applyMigrations(database.client, { directory });
      // Events stored before 0030, as on a database upgraded across it.
      for (const session of ["s1", "s2", "s3"])
        await database.query(
          `INSERT INTO sessions.convergence_events
             (org_id, workspace_id, machine_id, user_id, source, session_id, event_id, kind, type, ts, record)
           VALUES ('org', 'ws', 'machine', 'user', 'claude', $1, $1 || '-e', 'assistant', 'message', now(), '{}')`,
          [session],
        );

      const result = await applyMigrations(database.client);

      expect(result.rolledUpSessions).toBe(3);
      expect(
        (
          await database.query(
            `SELECT stage, completed_at IS NOT NULL AS done FROM sessions.delivery_rollout
             UNION ALL
             SELECT 'rollups', completed_at IS NOT NULL FROM sessions.session_rollup_rollout
             ORDER BY 1`,
          )
        ).rows,
      ).toEqual([
        { stage: "activity", done: true },
        { stage: "catalog", done: true },
        { stage: "rollups", done: true },
      ]);
      expect(
        (
          await database.query(
            "SELECT count(*)::int AS n FROM sessions.session_rollups",
          )
        ).rows,
      ).toEqual([{ n: 3 }]);
      // A rerun has nothing left to roll up.
      expect((await applyMigrations(database.client)).rolledUpSessions).toBe(0);
    } finally {
      await database.close();
    }
  });
});

describe("migration ledger on a real server", () => {
  let database: FreshDatabase;
  afterEach(async () => {
    await database?.close();
  });

  const ledgerCount = async () =>
    (
      await database.query<{ n: number }>(
        "SELECT count(*)::int AS n FROM sessions.__migrations",
      )
    ).rows[0]!.n;

  it("counts a superseded checksum as applied", async () => {
    database = await createFreshDatabase();
    await applyMigrations(database.client);
    const [name, [superseded]] = Object.entries(SUPERSEDED_CHECKSUMS)[0]!;
    await database.query(
      "UPDATE sessions.__migrations SET checksum = $1 WHERE name = $2",
      [superseded, name],
    );

    const again = await applyMigrations(database.client);

    expect(again.applied.find((m) => m.name === name)?.checksum).toBe(
      superseded,
    );
    expect(again.applied).toHaveLength(readMigrations().length);
  });

  it("fails on any other change to an applied migration", async () => {
    database = await createFreshDatabase();
    await applyMigrations(database.client);
    const [first] = readMigrations();
    await database.query(
      "UPDATE sessions.__migrations SET checksum = 'different' WHERE name = $1",
      [first!.name],
    );

    await expect(applyMigrations(database.client)).rejects.toThrow(
      "checksum changed",
    );
    expect(await ledgerCount()).toBe(readMigrations().length);
  });

  it("rolls back a failed pending migration together with its ledger entry", async () => {
    database = await createFreshDatabase();
    await applyMigrations(database.client);
    const directory = migrationsDirectory(
      {
        "9999_failed.sql":
          "CREATE TABLE sessions.should_rollback (id int);\nSELECT missing_column FROM sessions.should_rollback;\n",
      },
      { packaged: true },
    );

    await expect(
      applyMigrations(database.client, { directory }),
    ).rejects.toThrow();
    expect(
      (
        await database.query(
          "SELECT to_regclass('sessions.should_rollback') AS relation",
        )
      ).rows,
    ).toEqual([{ relation: null }]);
    expect(await ledgerCount()).toBe(readMigrations().length);
    // The connection is usable again: the failed transaction was rolled back.
    expect((await applyMigrations(database.client)).applied).toHaveLength(
      readMigrations().length,
    );
  });

  it("grants the runtime role table and function access but not the ledger", async () => {
    database = await createFreshDatabase();
    const runtime = await database.createRole("cloud_runtime");
    await applyMigrations(database.client, { runtimeRole: runtime });

    expect(
      (
        await database.query<{ ok: boolean }>(
          "SELECT has_function_privilege($1, 'sessions.session_brief_v2(text, text, text, text, text, text)', 'EXECUTE') AS ok",
          [runtime],
        )
      ).rows,
    ).toEqual([{ ok: true }]);
    await database.exec(`SET ROLE ${runtime}`);
    try {
      await database.exec(
        "INSERT INTO sessions.machines (org_id, workspace_id, machine_id) VALUES ('org', 'workspace', 'machine')",
      );
      await expect(
        database.query("SELECT * FROM sessions.__migrations"),
      ).rejects.toThrow("permission denied");
    } finally {
      await database.exec("RESET ROLE");
    }
  });
});
