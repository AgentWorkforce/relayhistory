import { cpSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import {
  CONCURRENT_INDEXES,
  MIGRATIONS_DIR,
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
  } = {},
) {
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
        return { rows: [{ name: "0001.sql", checksum: "checksum" }] };
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

    // The migration transaction, then the concurrent indexes; the rollouts follow.
    expect(
      fake.queries.slice(
        0,
        PREAMBLE.length + FIXTURE_STATEMENTS.length + 1 + INDEX_PROBES.length,
      ),
    ).toEqual([...PREAMBLE, ...FIXTURE_STATEMENTS, "COMMIT", ...INDEX_PROBES]);
    expect(fake.queries.filter((q) => q === "BEGIN")).toHaveLength(
      fake.queries.filter((q) => q === "COMMIT").length,
    );
    expect(FIXTURE_STATEMENTS[0]).toBe(
      "SELECT pg_advisory_xact_lock(1919249529, 1)",
    );
    expect(result).toMatchObject({
      applied: [{ name: "0001.sql", checksum: "checksum" }],
      indexes: Object.fromEntries(
        CONCURRENT_INDEXES.map(({ name }) => [name, "skipped"]),
      ),
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
    // Nothing else touches the indexes: what follows is the rollouts.
    expect(
      fake.queries
        .slice(commit + 1 + CONCURRENT_INDEXES.length)
        .some((q) => q.includes("pg_extension")),
    ).toBe(false);
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
