import { describe, expect, it } from "vitest";
import {
  CONCURRENT_INDEXES,
  ensureConcurrentIndexes,
  migrationStatements,
  readMigrations,
} from "../src/migrate/index.js";

type State = "valid" | "invalid" | "building" | "missing";
const [first] = CONCURRENT_INDEXES;

/**
 * A fake connection for the first index (the others report valid). Each catalog
 * check consumes the next scripted state (the last one sticks); DROP makes the
 * index missing and CREATE makes it valid, unless `createError` is set once.
 */
function connection(
  script: State[],
  {
    extensions = ["pg_trgm"],
    createError,
  }: { extensions?: string[]; createError?: unknown } = {},
) {
  const statements: string[] = [];
  let override: State | null = null;
  const query = async (text: string) => {
    statements.push(text);
    const extension = /extname = '([^']+)'/.exec(text);
    if (extension)
      return extensions.includes(extension[1]!) ? [{ v: true }] : [];
    const lookup = /c\.relname = '([^']+)'/.exec(text);
    if (lookup) {
      let state: State = "valid";
      if (lookup[1] === first.name)
        state = override ?? (script.length > 1 ? script.shift()! : script[0]!);
      return state === "missing" ? [] : [{ v: state }];
    }
    if (text.includes(first.name) && text.startsWith("DROP"))
      override = "missing";
    if (text.includes(first.name) && text.startsWith("CREATE")) {
      if (createError) {
        const error = createError;
        createError = undefined;
        override = null;
        throw error;
      }
      override = "valid";
    }
    return [];
  };
  return { query, statements };
}
const clock = () => {
  let t = 0;
  return { now: () => t, sleep: async (ms: number) => void (t += ms) };
};
const writes = (statements: string[]) =>
  statements.filter(
    (s) => /^(CREATE|DROP) INDEX/.test(s) && s.includes(first.name),
  );
const run = (
  query: (text: string) => Promise<Record<string, unknown>[]>,
  timeoutMs = 60_000,
) =>
  ensureConcurrentIndexes(query, CONCURRENT_INDEXES, {
    timeoutMs,
    pollMs: 1_000,
    ...clock(),
  });

describe("concurrent indexes", () => {
  it("builds a missing index with CREATE INDEX CONCURRENTLY", async () => {
    const { query, statements } = connection(["missing"]);
    expect((await run(query))[first.name]).toBe("created");
    expect(writes(statements)).toEqual([
      expect.stringMatching(
        new RegExp(`^CREATE INDEX CONCURRENTLY IF NOT EXISTS ${first.name} `),
      ),
    ]);
  });

  it("leaves a valid index alone", async () => {
    const { query, statements } = connection(["valid"]);
    expect((await run(query))[first.name]).toBe("valid");
    expect(writes(statements)).toEqual([]);
  });

  it("drops and rebuilds an abandoned INVALID index", async () => {
    const { query, statements } = connection(["invalid"]);
    expect((await run(query))[first.name]).toBe("rebuilt");
    expect(writes(statements)).toEqual([
      `DROP INDEX CONCURRENTLY IF EXISTS sessions.${first.name}`,
      expect.stringMatching(/^CREATE INDEX CONCURRENTLY IF NOT EXISTS /),
    ]);
  });

  it("waits for another runner's build and touches nothing when it succeeds", async () => {
    const { query, statements } = connection(["building", "building", "valid"]);
    expect((await run(query))[first.name]).toBe("waited");
    expect(writes(statements)).toEqual([]);
  });

  it("rebuilds when the other runner's build fails while it waits", async () => {
    const { query, statements } = connection(["building", "invalid"]);
    expect((await run(query))[first.name]).toBe("rebuilt");
    expect(writes(statements)).toEqual([
      `DROP INDEX CONCURRENTLY IF EXISTS sessions.${first.name}`,
      expect.stringMatching(/^CREATE INDEX CONCURRENTLY IF NOT EXISTS /),
    ]);
  });

  it("fails the deploy when another build outlasts the timeout", async () => {
    const { query, statements } = connection(["building"]);
    await expect(run(query, 5_000)).rejects.toThrow(/Timed out waiting/);
    expect(writes(statements)).toEqual([]);
  });

  it("re-checks after losing a create race to another runner", async () => {
    const duplicate = Object.assign(new Error("relation already exists"), {
      code: "42P07",
    });
    const { query } = connection(["missing", "building", "valid"], {
      createError: duplicate,
    });
    expect((await run(query))[first.name]).toBe("waited");
  });

  it("propagates other create errors", async () => {
    const { query } = connection(["missing"], {
      createError: new Error("disk full"),
    });
    await expect(run(query)).rejects.toThrow("disk full");
  });

  it("skips indexes whose extension is not installed", async () => {
    const { query, statements } = connection(["missing"], { extensions: [] });
    const actions = await run(query);
    expect(Object.values(actions)).toEqual(
      CONCURRENT_INDEXES.map(() => "skipped"),
    );
    expect(writes(statements)).toEqual([]);
  });

  it("keeps index builds out of the migration transaction", () => {
    const sql = migrationStatements(readMigrations()).join("\n");
    for (const { name } of CONCURRENT_INDEXES) expect(sql).not.toContain(name);
  });
});
